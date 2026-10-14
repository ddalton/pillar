//! Hadoop's DataTransferProtocol (version 28), so stock HDFS clients
//! (`hadoop fs -put/-cat`, Spark, hdfs-native, ...) can write and read
//! blocks on a Pillar DataNode.
//!
//! It shares the data port with Pillar's own protocol ([`super::wire`]): a
//! Hadoop request starts with the 2-byte version `0x001C`, so its first
//! octet is 0, which is never a Pillar op code.
//!
//! ```text
//! request   version(u16=28) op(u8) <varint-delimited Op*Proto>
//!
//! WRITE_BLOCK (80)  reply BlockOpResponseProto (connect ack), then packets
//!                   upstream -> PipelineAckProto per packet downstream
//! READ_BLOCK  (81)  reply BlockOpResponseProto with ReadOpChecksumInfo,
//!                   then packets, then the client's ClientReadStatusProto
//!
//! packet    PLEN(u32) HLEN(u16) PacketHeaderProto[HLEN] checksums data
//!           PLEN = 4 + len(checksums) + len(data)
//! ```
//!
//! Writes forward the pipeline to the next target and verify every
//! chunk's checksum before acknowledging it. The block's `.meta` file is
//! written in HDFS layout (version 1, checksum header, one checksum per
//! chunk). Construction stages:
//!
//! * `PIPELINE_SETUP_CREATE`: a new block (`create`, `put`).
//! * `PIPELINE_SETUP_APPEND`: reopen a finalized replica to extend it; the
//!   first packet may complete a partial chunk, whose stored checksum then
//!   covers the old and the new bytes.
//! * `PIPELINE_SETUP_STREAMING_RECOVERY` / `..._APPEND_RECOVERY`: after a
//!   DataNode in the pipeline failed, the client rebuilds it from the
//!   survivors with a new generation stamp; each reopens its replica being
//!   written, cuts it back to the bytes the client knows were acknowledged
//!   (`minBytesRcvd`) and takes the replayed packets.
//! * `PIPELINE_CLOSE_RECOVERY`: the pipeline broke while closing; the
//!   replica (finalized or not) takes the new generation stamp.
//!
//! When the next DataNode fails mid-stream, the acknowledgement upstream
//! says so (`ERROR` in its position), so the client drops that node and not
//! this one. A replica being written survives a broken pipeline for the
//! recovery; abandoned ones are swept after an hour
//! ([`BlockStorage::sweep_stale_rbw`]).
//!
//! `TRANSFER_BLOCK` (86) adds a replacement DataNode to a pipeline: the
//! client asks a DataNode in it to copy its replica to the new one, which
//! receives it with stage `TRANSFER_RBW` (kept as a replica being written,
//! for the recovery that follows) or `TRANSFER_FINALIZED`.
//!
//! Block access tokens: when the NameNode has sent this DataNode its keys,
//! every op must carry a token for its block, minted by the NameNode, with
//! the access the op needs (READ, WRITE, COPY for a transfer); others are
//! refused with `ERROR_ACCESS_TOKEN`. The DataNode mints its own tokens for
//! the copies it makes. Data transfer encryption (SASL on this port) is not
//! supported: a client must have `dfs.encrypt.data.transfer` off.
//!
//! Not supported (`ERROR_UNSUPPORTED`): block checksums and short-circuit
//! reads.

use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use pillar_proto::hdfs::{
    op_write_block_proto::BlockConstructionStage, BaseHeaderProto, BlockOpResponseProto, ChecksumProto, ChecksumTypeProto,
    ClientReadStatusProto, DatanodeInfoProto, OpReadBlockProto, OpTransferBlockProto, OpWriteBlockProto,
    PacketHeaderProto, PipelineAckProto, ReadOpChecksumInfoProto, Status,
};

use super::{Result, TransportError};
use pillar_common::hdfs::block_token::mode;

use crate::storage::block_storage::{BlockId, BlockStorage, BlockStorageError};

/// `DataTransferProtocol.DATA_TRANSFER_VERSION`.
pub const DATA_TRANSFER_VERSION: u16 = 28;

const OP_WRITE_BLOCK: u8 = 80;
const OP_READ_BLOCK: u8 = 81;
const OP_TRANSFER_BLOCK: u8 = 86;

/// Data per READ packet (HDFS `dfs.client-write-packet-size` is 64 KiB).
const READ_PACKET_DATA: usize = 64 * 1024;
/// Largest packet accepted from a writer.
const MAX_PACKET: usize = 16 * 1024 * 1024;
/// `PacketHeader.HEART_BEAT_SEQNO`.
const HEARTBEAT_SEQNO: i64 = -1;
/// Server-side checksum for reads of blocks written without one.
const DEFAULT_BYTES_PER_CHECKSUM: u32 = 512;
/// `.meta` header: version (u16), checksum type (u8), bytes per checksum (u32).
const META_HEADER_LEN: u64 = 7;
/// How long to wait for the next DataNode's acknowledgement (HDFS
/// `dfs.client.socket-timeout`).
const MIRROR_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

// ------------------------------------------------------------- checksums

/// A DataChecksum: type and chunk size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checksum {
    pub kind: ChecksumTypeProto,
    pub bytes_per_checksum: u32,
}

impl Checksum {
    pub const CRC32C_512: Checksum =
        Checksum { kind: ChecksumTypeProto::ChecksumCrc32c, bytes_per_checksum: DEFAULT_BYTES_PER_CHECKSUM };

    fn from_proto(p: &ChecksumProto) -> Result<Self> {
        let kind = ChecksumTypeProto::try_from(p.r#type)
            .map_err(|_| TransportError::ProtocolError(format!("unknown checksum type {}", p.r#type)))?;
        if p.bytes_per_checksum == 0 || p.bytes_per_checksum > 1 << 20 {
            return Err(TransportError::ProtocolError(format!("bytesPerChecksum {}", p.bytes_per_checksum)));
        }
        Ok(Checksum { kind, bytes_per_checksum: p.bytes_per_checksum })
    }

    fn to_proto(self) -> ChecksumProto {
        ChecksumProto { r#type: self.kind as i32, bytes_per_checksum: self.bytes_per_checksum }
    }

    /// Bytes of checksum per chunk.
    pub fn size(self) -> usize {
        match self.kind {
            ChecksumTypeProto::ChecksumNull => 0,
            _ => 4,
        }
    }

    fn sum(self, chunk: &[u8]) -> Option<u32> {
        match self.kind {
            ChecksumTypeProto::ChecksumNull => None,
            ChecksumTypeProto::ChecksumCrc32 => Some(crc32fast::hash(chunk)),
            ChecksumTypeProto::ChecksumCrc32c => Some(crc32c::crc32c(chunk)),
        }
    }

    /// Checksums for `data`, one per `bytes_per_checksum` chunk, big-endian.
    pub fn compute(self, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len() / self.bytes_per_checksum as usize * 4 + 4);
        for chunk in data.chunks(self.bytes_per_checksum as usize) {
            if let Some(c) = self.sum(chunk) {
                out.extend_from_slice(&c.to_be_bytes());
            }
        }
        out
    }

    /// Verify `data` against `sums`; returns the first bad chunk index.
    pub fn verify(self, data: &[u8], sums: &[u8]) -> std::result::Result<(), usize> {
        if self.size() == 0 {
            return Ok(());
        }
        let chunks = data.len().div_ceil(self.bytes_per_checksum as usize);
        if sums.len() != chunks * 4 {
            return Err(0);
        }
        for (i, chunk) in data.chunks(self.bytes_per_checksum as usize).enumerate() {
            let want = u32::from_be_bytes(sums[i * 4..i * 4 + 4].try_into().unwrap());
            if self.sum(chunk) != Some(want) {
                return Err(i);
            }
        }
        Ok(())
    }

    /// `BlockMetadataHeader`: version 1, then the DataChecksum header
    /// (type octet, bytesPerChecksum).
    fn meta_header(self) -> [u8; 7] {
        let mut h = [0u8; 7];
        h[..2].copy_from_slice(&1u16.to_be_bytes());
        h[2] = self.kind as u8;
        h[3..].copy_from_slice(&self.bytes_per_checksum.to_be_bytes());
        h
    }
}

// --------------------------------------------------------------- framing

async fn read_varint<S: AsyncRead + Unpin>(s: &mut S) -> Result<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let b = s.read_u8().await?;
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(TransportError::ProtocolError("varint too long".into()))
}

/// Read a varint-length-delimited protobuf message.
pub async fn read_delimited<M: Message + Default, S: AsyncRead + Unpin>(s: &mut S) -> Result<M> {
    let len = read_varint(s).await? as usize;
    if len > MAX_PACKET {
        return Err(TransportError::ProtocolError(format!("{} byte message", len)));
    }
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).await?;
    M::decode(&buf[..]).map_err(|e| TransportError::ProtocolError(format!("protobuf: {}", e)))
}

/// Write a varint-length-delimited protobuf message.
pub async fn write_delimited<M: Message, S: AsyncWrite + Unpin>(s: &mut S, m: &M) -> Result<()> {
    s.write_all(&m.encode_length_delimited_to_vec()).await?;
    Ok(())
}

/// One data packet.
#[derive(Debug)]
pub struct Packet {
    pub header: PacketHeaderProto,
    pub checksums: Bytes,
    pub data: Bytes,
}

impl Packet {
    pub fn encode(&self) -> Bytes {
        let header = self.header.encode_to_vec();
        let mut b = BytesMut::with_capacity(6 + header.len() + self.checksums.len() + self.data.len());
        b.put_u32((4 + self.checksums.len() + self.data.len()) as u32);
        b.put_u16(header.len() as u16);
        b.put_slice(&header);
        b.put_slice(&self.checksums);
        b.put_slice(&self.data);
        b.freeze()
    }
}

/// Read one packet; also returns its raw bytes for forwarding.
pub async fn read_packet<S: AsyncRead + Unpin>(s: &mut S) -> Result<(Packet, Bytes)> {
    let plen = s.read_u32().await? as usize;
    let hlen = s.read_u16().await? as usize;
    if plen < 4 || plen > MAX_PACKET {
        return Err(TransportError::ProtocolError(format!("packet length {}", plen)));
    }
    let mut raw = BytesMut::with_capacity(6 + hlen + plen - 4);
    raw.put_u32(plen as u32);
    raw.put_u16(hlen as u16);
    raw.resize(6 + hlen + plen - 4, 0);
    s.read_exact(&mut raw[6..]).await?;
    let raw = raw.freeze();

    let header = PacketHeaderProto::decode(&raw[6..6 + hlen])
        .map_err(|e| TransportError::ProtocolError(format!("packet header: {}", e)))?;
    let body = raw.slice(6 + hlen..);
    let data_len = header.data_len as usize;
    if header.data_len < 0 || data_len > body.len() {
        return Err(TransportError::ProtocolError(format!("dataLen {} in a {} byte body", header.data_len, body.len())));
    }
    let split = body.len() - data_len;
    Ok((Packet { header, checksums: body.slice(..split), data: body.slice(split..) }, raw))
}

fn op_response(status: Status, message: &str) -> BlockOpResponseProto {
    BlockOpResponseProto {
        status: status as i32,
        message: (!message.is_empty()).then(|| message.to_string()),
        ..Default::default()
    }
}

fn ack(seqno: i64, replies: &[Status]) -> PipelineAckProto {
    PipelineAckProto {
        seqno,
        reply: replies.iter().map(|s| *s as i32).collect(),
        downstream_ack_time_nanos: Some(0),
        // Hadoop >= 2.7 readers take the status from `flag` when present;
        // with ECN disabled the flag is the status itself.
        flag: replies.iter().map(|s| *s as u32).collect(),
    }
}

// ---------------------------------------------------------------- server

/// Serve one Hadoop DataTransferProtocol request. The caller has consumed
/// the first version octet (0); this reads the second.
pub async fn serve<S>(socket: &mut S, storage: &Arc<BlockStorage>, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let version = socket.read_u8().await? as u16;
    if version != DATA_TRANSFER_VERSION {
        return Err(TransportError::ProtocolError(format!(
            "DataTransferProtocol version {} (expected {})",
            version, DATA_TRANSFER_VERSION
        )));
    }
    match socket.read_u8().await? {
        OP_WRITE_BLOCK => {
            let op: OpWriteBlockProto = read_delimited(socket).await?;
            if !check_token(socket, storage, &op.header.base_header, mode::WRITE, peer).await? {
                return Ok(());
            }
            write_block(socket, storage, op, peer).await
        }
        OP_READ_BLOCK => {
            let op: OpReadBlockProto = read_delimited(socket).await?;
            if !check_token(socket, storage, &op.header.base_header, mode::READ, peer).await? {
                return Ok(());
            }
            read_block(socket, storage, op, peer).await
        }
        OP_TRANSFER_BLOCK => {
            let op: OpTransferBlockProto = read_delimited(socket).await?;
            if !check_token(socket, storage, &op.header.base_header, mode::COPY, peer).await? {
                return Ok(());
            }
            transfer_block(socket, storage, op, peer).await
        }
        op => {
            // The op's proto has not been read, so the connection is done.
            let r = op_response(Status::ErrorUnsupported, &format!("op {} is not supported by Pillar", op));
            write_delimited(socket, &r).await?;
            socket.flush().await?;
            Ok(())
        }
    }
}

/// Whether the op's block access token grants `access` to its block; if
/// not, the client is told so (`ERROR_ACCESS_TOKEN`, which makes a Hadoop
/// client fetch a fresh token from the NameNode and retry).
async fn check_token<S>(socket: &mut S, storage: &BlockStorage, h: &BaseHeaderProto, access: u8, peer: SocketAddr) -> Result<bool>
where
    S: AsyncWrite + Unpin,
{
    match storage.tokens.verify(h.token.as_ref(), h.block.block_id, access) {
        Ok(()) => Ok(true),
        Err(e) => {
            warn!("Refusing access {} to block {} from {}: {}", access, h.block.block_id, peer, e);
            let msg = format!("block access token for block {}: {}", h.block.block_id, e);
            write_delimited(socket, &op_response(Status::ErrorAccessToken, &msg)).await?;
            socket.flush().await?;
            Ok(false)
        }
    }
}

async fn read_block<S>(socket: &mut S, storage: &Arc<BlockStorage>, op: OpReadBlockProto, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let b = &op.header.base_header.block;
    let checksum = Checksum::CRC32C_512;
    let bpc = checksum.bytes_per_checksum as u64;

    // A finalized replica, or one being written (a file still open), of
    // which readers see what the pipeline acknowledged.
    let (replica, size) = match storage.replica_visible_length(b.block_id) {
        Some((id, len)) if id.generation_stamp >= b.generation_stamp => (id, len),
        Some((id, _)) => {
            let msg = format!("replica {} has genstamp {} < {}", b.block_id, id.generation_stamp, b.generation_stamp);
            write_delimited(socket, &op_response(Status::ErrorInvalid, &msg)).await?;
            return socket.flush().await.map_err(Into::into);
        }
        None => {
            let msg = format!("block {} not found", b.block_id);
            write_delimited(socket, &op_response(Status::ErrorInvalid, &msg)).await?;
            return socket.flush().await.map_err(Into::into);
        }
    };
    if op.offset > size || (op.offset == size && op.len > 0 && size > 0) {
        let msg = format!("offset {} beyond block {} of {} bytes", op.offset, b.block_id, size);
        write_delimited(socket, &op_response(Status::ErrorInvalid, &msg)).await?;
        return socket.flush().await.map_err(Into::into);
    }

    // Send whole chunks: start on a chunk boundary, end at a chunk boundary
    // or the end of the block.
    let start = op.offset - op.offset % bpc;
    let end = (op.offset.saturating_add(op.len)).min(size);
    let end = if end % bpc == 0 { end } else { (end - end % bpc + bpc).min(size) };

    let response = BlockOpResponseProto {
        status: Status::Success as i32,
        read_op_checksum_info: Some(ReadOpChecksumInfoProto { checksum: checksum.to_proto(), chunk_offset: start }),
        ..Default::default()
    };
    write_delimited(socket, &response).await?;

    let finalized = storage.get_block_metadata(b.block_id).is_some_and(|m| m.block_id == replica);
    let (file, len) = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || {
            if finalized {
                storage.open_for_read(&replica, start, end - start)
            } else {
                storage.open_rbw_for_read(&replica, start, end - start)
            }
        })
            .await
            .map_err(|e| TransportError::ProtocolError(e.to_string()))?
            .map_err(|e| TransportError::ProtocolError(e.to_string()))?
    };
    let mut chunks = super::wire::spawn_file_reader(file, len, READ_PACKET_DATA);
    let (mut offset, mut seqno) = (start, 0i64);
    while let Some(data) = chunks.recv().await {
        let data = data?;
        let header = PacketHeaderProto {
            offset_in_block: offset as i64,
            seqno,
            last_packet_in_block: false,
            data_len: data.len() as i32,
            sync_block: Some(false),
        };
        let checksums = Bytes::from(checksum.compute(&data));
        offset += data.len() as u64;
        socket.write_all(&Packet { header, checksums, data }.encode()).await?;
        seqno += 1;
    }
    let last = PacketHeaderProto {
        offset_in_block: offset as i64,
        seqno,
        last_packet_in_block: true,
        data_len: 0,
        sync_block: Some(false),
    };
    socket.write_all(&Packet { header: last, checksums: Bytes::new(), data: Bytes::new() }.encode()).await?;
    socket.flush().await?;

    // The client reports CHECKSUM_OK / SUCCESS; a client that stops early
    // just closes, which is not an error.
    match tokio::time::timeout(std::time::Duration::from_secs(5), read_delimited::<ClientReadStatusProto, _>(socket)).await {
        Ok(Ok(s)) if s.status == Status::ErrorChecksum as i32 => {
            warn!("Client {} reports a checksum error reading block {}", peer, b.block_id)
        }
        _ => {}
    }
    debug!("Served {} bytes of block {} to {} (HDFS protocol)", end - start, b.block_id, peer);
    Ok(())
}

/// Disk work for one received packet.
struct DiskWrite {
    offset: u64,
    data: Bytes,
    /// Checksums for the chunks in `data`, already verified.
    checksums: Bytes,
    /// Bytes of the chunk already on disk before `offset` (a packet that
    /// completes a partial chunk, after append); the stored checksum must
    /// cover them too.
    prefix: u64,
    /// Position in the session's write order; the writer publishes the last
    /// one done, and a packet is acknowledged only once its write is.
    seq: u64,
    /// hsync: make it durable (fsync) before it counts as done.
    sync: bool,
}

/// The replica a write session writes to.
struct Replica {
    /// Its id now (with the session's generation stamp).
    block: BlockId,
    /// Data and `.meta`; `None` for a finalized replica in close recovery,
    /// which takes no more data.
    files: Option<(std::fs::File, std::fs::File)>,
    /// Bytes already in it, after any truncation.
    len: u64,
}

/// Open the replica for a WRITE_BLOCK op according to its stage. Errors
/// are the status and message to answer with.
fn open_replica(
    storage: &BlockStorage,
    op: &OpWriteBlockProto,
    checksum: Checksum,
) -> std::result::Result<Replica, (Status, String)> {
    let b = &op.header.base_header.block;
    let new_gs = if op.latest_generation_stamp > 0 { op.latest_generation_stamp } else { b.generation_stamp };
    let io = |e: std::io::Error| (Status::Error, e.to_string());
    let st = |e: BlockStorageError| match e {
        BlockStorageError::BlockAlreadyExists(_) => (Status::ErrorExists, e.to_string()),
        BlockStorageError::BlockNotFound(_) => (Status::Error, format!("replica not found: {}", e)),
        _ => (Status::Error, e.to_string()),
    };
    let stage = BlockConstructionStage::try_from(op.stage)
        .map_err(|_| (Status::ErrorInvalid, format!("unknown stage {}", op.stage)))?;
    use BlockConstructionStage as S;
    match stage {
        S::PipelineSetupCreate => {
            let block = BlockId::new(b.block_id, b.generation_stamp);
            let data = storage.begin_write(&block).map_err(st)?;
            let mut meta = storage.open_tmp_meta(&block).map_err(st)?;
            meta.write_all(&checksum.meta_header()).map_err(io)?;
            Ok(Replica { block, files: Some((data, meta)), len: 0 })
        }
        S::PipelineSetupStreamingRecovery | S::PipelineSetupAppendRecovery
            if storage.find_rbw(b.block_id).is_some() =>
        {
            let (block, on_disk) = storage.restamp_rbw(b.block_id, new_gs).map_err(st)?;
            if on_disk < op.min_bytes_rcvd {
                return Err((
                    Status::Error,
                    format!("replica of block {} has {} bytes, fewer than the {} acknowledged", b.block_id, on_disk, op.min_bytes_rcvd),
                ));
            }
            let (data, meta) = storage.open_rbw(&block).map_err(st)?;
            check_meta_header(&meta, checksum)?;
            truncate_replica(&data, &meta, op.min_bytes_rcvd, checksum).map_err(io)?;
            storage.reset_visible(&block, op.min_bytes_rcvd);
            info!("Recovering block {} at {} bytes (generation stamp {})", b.block_id, op.min_bytes_rcvd, new_gs);
            Ok(Replica { block, files: Some((data, meta)), len: op.min_bytes_rcvd })
        }
        S::PipelineSetupAppend | S::PipelineSetupAppendRecovery => {
            // The expected length is minBytesRcvd: the Java client puts the
            // file's block size in the header's numBytes for this stage.
            let old = BlockId::new(b.block_id, b.generation_stamp);
            let have = storage.get_block_metadata(b.block_id).map(|m| (m.block_id, m.num_bytes));
            match have {
                Some((id, len)) if id == old && len == op.min_bytes_rcvd => {}
                Some((id, len)) => {
                    return Err((Status::Error, format!("append to {:?} ({} bytes): this replica is {:?} ({} bytes)", old, op.min_bytes_rcvd, id, len)));
                }
                None => return Err((Status::Error, format!("append: no finalized replica of block {}", b.block_id))),
            }
            let (block, len) = storage.unfinalize(&old, new_gs).map_err(st)?;
            storage.reset_visible(&block, len);
            let (data, meta) = storage.open_rbw(&block).map_err(st)?;
            check_meta_header(&meta, checksum)?;
            info!("Appending to block {} at {} bytes (generation stamp {})", b.block_id, len, new_gs);
            Ok(Replica { block, files: Some((data, meta)), len })
        }
        S::PipelineCloseRecovery => {
            if storage.find_rbw(b.block_id).is_some() {
                let (block, len) = storage.restamp_rbw(b.block_id, new_gs).map_err(st)?;
                let (data, meta) = storage.open_rbw(&block).map_err(st)?;
                return Ok(Replica { block, files: Some((data, meta)), len });
            }
            let len = storage.restamp_finalized(b.block_id, new_gs).map_err(st)?;
            Ok(Replica { block: BlockId::new(b.block_id, new_gs), files: None, len })
        }
        S::TransferRbw | S::TransferFinalized => {
            // A copy for a pipeline this DataNode is joining: any stale
            // partial replica of the block here is replaced.
            if let Some((stale, _)) = storage.find_rbw(b.block_id) {
                storage.abort_write(&stale);
            }
            let block = BlockId::new(b.block_id, b.generation_stamp);
            let data = storage.begin_write(&block).map_err(st)?;
            let mut meta = storage.open_tmp_meta(&block).map_err(st)?;
            meta.write_all(&checksum.meta_header()).map_err(io)?;
            Ok(Replica { block, files: Some((data, meta)), len: 0 })
        }
        other => Err((Status::ErrorUnsupported, format!("block construction stage {:?} is not supported by Pillar", other))),
    }
}

/// Block recovery's last step on one replica (HDFS
/// `updateReplicaUnderRecovery`): give it the recovery's generation stamp,
/// cut a replica being written to the agreed length, and finalize it.
/// Returns the finalized length.
pub fn finalize_recovered(storage: &BlockStorage, block_id: u64, recovery_gs: u64, len: u64) -> std::result::Result<u64, String> {
    if storage.find_rbw(block_id).is_some() {
        let (id, on_disk) = storage.restamp_rbw(block_id, recovery_gs).map_err(|e| e.to_string())?;
        if on_disk < len {
            return Err(format!("replica of block {} has {} bytes, recovery needs {}", block_id, on_disk, len));
        }
        let (data, meta) = storage.open_rbw(&id).map_err(|e| e.to_string())?;
        let checksum = replica_checksum(&storage.rbw_meta_path(&id));
        truncate_replica(&data, &meta, len, checksum).map_err(|e| e.to_string())?;
        drop((data, meta));
        return storage.finalize_block(&id).map_err(|e| e.to_string());
    }
    let m = storage.get_block_metadata(block_id).ok_or_else(|| format!("no replica of block {}", block_id))?;
    if m.block_id.generation_stamp > recovery_gs {
        return Err(format!("replica of block {} is newer ({}) than the recovery ({})", block_id, m.block_id.generation_stamp, recovery_gs));
    }
    if m.num_bytes != len {
        return Err(format!("finalized replica of block {} has {} bytes, not {}", block_id, m.num_bytes, len));
    }
    storage.restamp_finalized(block_id, recovery_gs).map_err(|e| e.to_string())
}

/// A reopened replica's `.meta` must use the checksum the writer sends.
fn check_meta_header(meta: &std::fs::File, checksum: Checksum) -> std::result::Result<(), (Status, String)> {
    let mut head = [0u8; META_HEADER_LEN as usize];
    meta.read_exact_at(&mut head, 0).map_err(|e| (Status::Error, e.to_string()))?;
    if head[..] != checksum.meta_header()[..] {
        return Err((Status::ErrorInvalid, "the replica was written with another checksum".to_string()));
    }
    Ok(())
}

/// Cut a replica to `len` bytes, its `.meta` to the chunks covering them,
/// and recompute the checksum of a now partial last chunk.
fn truncate_replica(data: &std::fs::File, meta: &std::fs::File, len: u64, checksum: Checksum) -> std::io::Result<()> {
    let bpc = checksum.bytes_per_checksum as u64;
    let csize = checksum.size() as u64;
    data.set_len(len)?;
    meta.set_len(META_HEADER_LEN + len.div_ceil(bpc) * csize)?;
    let partial = len % bpc;
    if partial > 0 {
        let mut tail = vec![0u8; partial as usize];
        data.read_exact_at(&mut tail, len - partial)?;
        meta.write_all_at(&checksum.compute(&tail), META_HEADER_LEN + (len / bpc) * csize)?;
    }
    data.sync_data()?;
    meta.sync_data()
}

async fn write_block<S>(socket: &mut S, storage: &Arc<BlockStorage>, op: OpWriteBlockProto, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let b = op.header.base_header.block.clone();
    let client = op.header.client_name.clone();

    let checksum = match Checksum::from_proto(&op.requested_checksum) {
        Ok(c) => c,
        Err(e) => {
            write_delimited(socket, &op_response(Status::ErrorInvalid, &e.to_string())).await?;
            return socket.flush().await.map_err(Into::into);
        }
    };
    let create = op.stage == BlockConstructionStage::PipelineSetupCreate as i32;

    // Open the replica before acknowledging the pipeline.
    let opened = {
        let storage = storage.clone();
        let op = op.clone();
        tokio::task::spawn_blocking(move || open_replica(&storage, &op, checksum))
            .await
            .map_err(|e| TransportError::ProtocolError(e.to_string()))?
    };
    let replica = match opened {
        Ok(r) => r,
        Err((status, msg)) => {
            warn!("Write of block {} ({:?}) refused: {}", b.block_id, op.stage, msg);
            write_delimited(socket, &op_response(status, &msg)).await?;
            return socket.flush().await.map_err(Into::into);
        }
    };
    let block = replica.block;
    let transfer = op.stage == BlockConstructionStage::TransferRbw as i32
        || op.stage == BlockConstructionStage::TransferFinalized as i32;
    if transfer {
        return receive_transfer(socket, storage, replica, checksum, op.stage, peer).await;
    }

    // Set up the rest of the pipeline before answering the upstream.
    let mut mirror: Option<super::sasl::DataStream> = None;
    let mut first_bad_link = String::new();
    let mut connect_status = Status::Success;
    if let Some(next) = op.targets.first() {
        let addr = format!("{}:{}", next.id.ip_addr, next.id.xfer_port);
        match connect_mirror(storage, &addr, &op).await {
            Ok((s, Status::Success, _)) => mirror = Some(s),
            Ok((_, status, bad)) => {
                connect_status = status;
                first_bad_link = if bad.is_empty() { addr } else { bad };
            }
            Err(e) => {
                warn!("Pipeline to {} for block {} failed: {}", addr, b.block_id, e);
                connect_status = Status::Error;
                first_bad_link = addr;
            }
        }
    }
    if !client.is_empty() {
        let r = BlockOpResponseProto {
            status: connect_status as i32,
            first_bad_link: Some(first_bad_link.clone()),
            ..Default::default()
        };
        write_delimited(socket, &r).await?;
        socket.flush().await?;
    }
    if connect_status != Status::Success {
        // A new block the client will abandon; a recovering replica stays
        // for the client's next attempt.
        if create {
            storage.abort_write(&block);
        }
        return Ok(());
    }

    // Disk writer thread: data and checksums at their positions, so a
    // re-sent chunk (replay after recovery, or after hflush) overwrites.
    let start_len = replica.len;
    let (disk_tx, mut disk_rx) = mpsc::channel::<DiskWrite>(8);
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0u64);
    let bpc = checksum.bytes_per_checksum as u64;
    let csize = checksum.size() as u64;
    let files = replica.files;
    let has_files = files.is_some();
    let writer = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        let Some((data_file, meta_file)) = files else {
            // A finalized replica in close recovery: nothing to write.
            while let Some(w) = disk_rx.blocking_recv() {
                progress_tx.send_replace(w.seq);
            }
            return Ok(start_len);
        };
        let mut len = start_len;
        while let Some(w) = disk_rx.blocking_recv() {
            // An empty write is an hsync with no new data.
            if !w.data.is_empty() {
                data_file.write_all_at(&w.data, w.offset)?;
                if w.prefix > 0 {
                    // Completing a partial chunk: checksum the whole chunk.
                    let mut chunk = vec![0u8; w.prefix as usize];
                    data_file.read_exact_at(&mut chunk, w.offset - w.prefix)?;
                    chunk.extend_from_slice(&w.data);
                    meta_file.write_all_at(&checksum.compute(&chunk), META_HEADER_LEN + (w.offset - w.prefix) / bpc * csize)?;
                } else {
                    meta_file.write_all_at(&w.checksums, META_HEADER_LEN + w.offset / bpc * csize)?;
                }
                len = len.max(w.offset + w.data.len() as u64);
            }
            if w.sync {
                data_file.sync_data()?;
                meta_file.sync_data()?;
            }
            progress_tx.send_replace(w.seq);
        }
        // No truncation here: writes are positional over a file that was
        // created empty or cut to length when reopened, and the writer of a
        // broken session may still be finishing after a recovery session
        // reopened the same replica.
        data_file.sync_data()?;
        meta_file.sync_data()?;
        Ok(len)
    });

    let (upstream_rx, mut upstream_tx) = tokio::io::split(socket);
    let (mirror_rx, mirror_tx) = match mirror {
        Some(m) => {
            let (r, w) = tokio::io::split(m);
            (Some(r), Some(w))
        }
        None => (None, None),
    };
    // The next DataNode failed: stop forwarding and say so in the acks.
    let mirror_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // (seqno, last packet?, local status, (write seq, end offset) if the
    // packet was written) in arrival order.
    let (ack_tx, mut ack_rx) = mpsc::channel::<(i64, bool, Status, Option<(u64, u64)>)>(64);

    // Receiver: upstream packets -> verify -> mirror + disk.
    let mirror_failed_rx = mirror_failed.clone();
    let receiver = async move {
        let mut upstream_rx = upstream_rx;
        let mut mirror_tx = mirror_tx;
        let mut written_to = start_len;
        let mut seq = 0u64;
        loop {
            let (p, raw) = read_packet(&mut upstream_rx).await?;
            let h = &p.header;
            if let Some(m) = mirror_tx.as_mut() {
                if let Err(e) = m.write_all(&raw).await {
                    warn!("Next DataNode in the pipeline for block {} failed: {}", b.block_id, e);
                    mirror_failed_rx.store(true, std::sync::atomic::Ordering::Release);
                    mirror_tx = None;
                }
            }
            if h.seqno == HEARTBEAT_SEQNO {
                ack_tx.send((h.seqno, false, Status::Success, None)).await.ok();
                continue;
            }
            let offset = h.offset_in_block as u64;
            let in_chunk = offset % bpc;
            let bad_offset = h.offset_in_block < 0
                || offset > written_to
                || (!has_files && !p.data.is_empty())
                // Only a packet continuing exactly where the replica ends may
                // start inside a chunk, and it may only complete that chunk.
                || (in_chunk != 0 && !p.data.is_empty()
                    && (offset != written_to || p.data.len() as u64 > bpc - in_chunk));
            if bad_offset {
                return Err(TransportError::ProtocolError(format!(
                    "packet at offset {} ({} bytes) after {} bytes",
                    offset,
                    p.data.len(),
                    written_to
                )));
            }
            let status = match checksum.verify(&p.data, &p.checksums) {
                Ok(()) => Status::Success,
                Err(chunk) => {
                    warn!("Checksum error in block {} chunk {} at offset {} from {}", b.block_id, chunk, offset, peer);
                    Status::ErrorChecksum
                }
            };
            let sync = h.sync_block == Some(true);
            let mut write = None;
            if status == Status::Success && (!p.data.is_empty() || sync) {
                written_to = written_to.max(offset + p.data.len() as u64);
                seq += 1;
                disk_tx
                    .send(DiskWrite { offset, data: p.data.clone(), checksums: p.checksums.clone(), prefix: in_chunk, seq, sync })
                    .await
                    .map_err(|_| TransportError::ProtocolError("disk writer stopped".into()))?;
                write = Some((seq, written_to));
            }
            let last = h.last_packet_in_block;
            ack_tx.send((h.seqno, last, status, write)).await.ok();
            if status != Status::Success {
                return Err(TransportError::ProtocolError("checksum error".into()));
            }
            if last {
                if let Some(mut m) = mirror_tx.take() {
                    let _ = m.flush().await;
                }
                drop(disk_tx);
                return Ok(written_to);
            }
        }
    };

    // Responder: in packet order, wait for the mirror's ack (if any), then
    // acknowledge upstream with our status followed by the downstream ones.
    // If the next DataNode failed, the ack carries ERROR in its place and
    // the session ends: the client rebuilds the pipeline without it.
    let storage2 = storage.clone();
    let responder = async move {
        let mut mirror_rx = mirror_rx;
        let mut writer = Some(writer);
        let mut progress_rx = progress_rx;
        while let Some((seqno, last, mut status, write)) = ack_rx.recv().await {
            // Acknowledge only what is on this DataNode's disk (and synced,
            // for hsync): what an ack promises a reader can read here.
            if let Some((seq, _)) = write {
                if progress_rx.wait_for(|done| *done >= seq).await.is_err() {
                    status = Status::Error;
                }
            }
            let mut replies = vec![status];
            let mut downstream_failed = mirror_failed.load(std::sync::atomic::Ordering::Acquire);
            if let (Some(m), false, Status::Success) = (mirror_rx.as_mut(), downstream_failed, status) {
                match tokio::time::timeout(MIRROR_ACK_TIMEOUT, read_delimited::<PipelineAckProto, _>(m)).await {
                    Ok(Ok(down)) if down.seqno == seqno => {
                        replies.extend(down.reply.iter().filter_map(|r| Status::try_from(*r).ok()));
                    }
                    Ok(Ok(down)) => {
                        return Err(TransportError::ProtocolError(format!("mirror acked {} for {}", down.seqno, seqno)));
                    }
                    Ok(Err(e)) => {
                        warn!("Next DataNode in the pipeline for block {} stopped acknowledging: {}", block.block_id, e);
                        downstream_failed = true;
                    }
                    Err(_) => {
                        warn!("Next DataNode in the pipeline for block {} timed out", block.block_id);
                        downstream_failed = true;
                    }
                }
            }
            if downstream_failed {
                replies.push(Status::Error);
                write_delimited(&mut upstream_tx, &ack(seqno, &replies)).await?;
                upstream_tx.flush().await?;
                return Ok(());
            }
            if last && status == Status::Success {
                let len = writer
                    .take()
                    .expect("one last packet")
                    .await
                    .map_err(|e| TransportError::ProtocolError(e.to_string()))??;
                if has_files {
                    let s = storage2.clone();
                    tokio::task::spawn_blocking(move || s.finalize_block(&block))
                        .await
                        .map_err(|e| TransportError::ProtocolError(e.to_string()))?
                        .map_err(|e| TransportError::ProtocolError(e.to_string()))?;
                }
                info!("Received block {} ({} bytes, generation stamp {}) from {} via HDFS protocol", block.block_id, len, block.generation_stamp, peer);
            }
            write_delimited(&mut upstream_tx, &ack(seqno, &replies)).await?;
            upstream_tx.flush().await?;
            // Every DataNode from here down has it: readers may see it.
            if let (Some((_, end)), false) = (write, last) {
                if replies.iter().all(|r| *r == Status::Success) {
                    storage2.set_visible(&block, end);
                }
            }
            if last || status != Status::Success {
                return Ok(());
            }
        }
        Err(TransportError::ProtocolError("pipeline closed before the last packet".into()))
    };

    let (received, responded) = tokio::join!(receiver, responder);
    // On failure the replica being written stays, for pipeline recovery
    // (or the sweep, if nobody recovers it).
    received.and(responded).map(|_| ())
}

/// Receive a replica copied from another DataNode (TRANSFER_RBW or
/// TRANSFER_FINALIZED): no pipeline and no per-packet acks; one response
/// once the last packet is in (HDFS DataXceiver does the same).
async fn receive_transfer<S>(
    socket: &mut S,
    storage: &Arc<BlockStorage>,
    replica: Replica,
    checksum: Checksum,
    stage: i32,
    peer: SocketAddr,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let block = replica.block;
    let (data, meta) = replica.files.expect("a new replica");
    let bpc = checksum.bytes_per_checksum as u64;
    let csize = checksum.size() as u64;
    let mut written = 0u64;
    let outcome: Result<()> = async {
        loop {
            let (p, _) = read_packet(socket).await?;
            let offset = p.header.offset_in_block as u64;
            if !p.data.is_empty() {
                if offset != written || offset % bpc != 0 {
                    return Err(TransportError::ProtocolError(format!("transfer packet at {} after {}", offset, written)));
                }
                checksum
                    .verify(&p.data, &p.checksums)
                    .map_err(|c| TransportError::ProtocolError(format!("checksum error in chunk {}", c)))?;
                data.write_all_at(&p.data, offset)?;
                meta.write_all_at(&p.checksums, META_HEADER_LEN + offset / bpc * csize)?;
                written += p.data.len() as u64;
            }
            if p.header.last_packet_in_block {
                data.sync_data()?;
                meta.sync_data()?;
                return Ok(());
            }
        }
    }
    .await;
    let response = match outcome {
        Ok(()) if stage == BlockConstructionStage::TransferFinalized as i32 => {
            let s = storage.clone();
            match tokio::task::spawn_blocking(move || s.finalize_block(&block)).await {
                Ok(Ok(_)) => op_response(Status::Success, ""),
                Ok(Err(e)) => op_response(Status::Error, &e.to_string()),
                Err(e) => op_response(Status::Error, &e.to_string()),
            }
        }
        // TRANSFER_RBW: stays a replica being written, for the recovery.
        Ok(()) => op_response(Status::Success, ""),
        Err(e) => {
            storage.abort_write(&block);
            warn!("Transfer of block {} from {} failed: {}", block.block_id, peer, e);
            op_response(Status::Error, &e.to_string())
        }
    };
    info!("Received a copy of block {} ({} bytes) from {}", block.block_id, written, peer);
    write_delimited(socket, &response).await?;
    socket.flush().await.map_err(Into::into)
}

/// TRANSFER_BLOCK: copy this DataNode's replica of the block (being
/// written, or finalized) to the first target, then answer the client.
async fn transfer_block<S>(socket: &mut S, storage: &Arc<BlockStorage>, op: OpTransferBlockProto, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let b = op.header.base_header.block.clone();
    let result = send_replica(storage, &op).await;
    let response = match &result {
        Ok(len) => {
            info!("Copied block {} ({} bytes) to {:?} for {}", b.block_id, len, op.targets.first().map(|t| &t.id.ip_addr), peer);
            op_response(Status::Success, "")
        }
        Err(e) => {
            warn!("Transfer of block {} requested by {} failed: {}", b.block_id, peer, e);
            op_response(Status::Error, &e.to_string())
        }
    };
    write_delimited(socket, &response).await?;
    socket.flush().await.map_err(Into::into)
}

/// The checksum a replica was written with (its `.meta` header).
fn replica_checksum(meta: &std::path::Path) -> Checksum {
    let mut head = [0u8; META_HEADER_LEN as usize];
    let read = std::fs::File::open(meta).and_then(|f| f.read_exact_at(&mut head, 0));
    let parsed = read.ok().and_then(|_| {
        let kind = ChecksumTypeProto::try_from(head[2] as i32).ok()?;
        let bytes_per_checksum = u32::from_be_bytes(head[3..7].try_into().ok()?);
        (bytes_per_checksum > 0).then_some(Checksum { kind, bytes_per_checksum })
    });
    parsed.unwrap_or(Checksum::CRC32C_512)
}

async fn send_replica(storage: &Arc<BlockStorage>, op: &OpTransferBlockProto) -> Result<u64> {
    let b = &op.header.base_header.block;
    let target = op.targets.first().ok_or_else(|| TransportError::ProtocolError("no target".into()))?;
    // What this DataNode holds: a replica being written, or a finalized one.
    let (id, len, rbw) = match storage.find_rbw(b.block_id) {
        Some((id, len)) => (id, len, true),
        None => match storage.get_block_metadata(b.block_id) {
            Some(m) => (m.block_id, m.num_bytes, false),
            None => return Err(TransportError::BlockNotFound(b.block_id)),
        },
    };
    if id.generation_stamp < b.generation_stamp {
        return Err(TransportError::ProtocolError(format!(
            "replica of block {} has generation stamp {} < {}",
            b.block_id, id.generation_stamp, b.generation_stamp
        )));
    }
    let (file, meta_path) = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || -> std::result::Result<_, BlockStorageError> {
            if rbw {
                let (data, _) = storage.open_rbw(&id)?;
                Ok((data, storage.rbw_meta_path(&id)))
            } else {
                let m = storage.get_block_metadata(id.block_id).ok_or(BlockStorageError::BlockNotFound(id.block_id))?;
                Ok((storage.open_for_read(&id, 0, len)?.0, m.meta_path))
            }
        })
        .await
        .map_err(|e| TransportError::ProtocolError(e.to_string()))?
        .map_err(|e| TransportError::ProtocolError(e.to_string()))?
    };
    let checksum = replica_checksum(&meta_path);

    let mut s = super::wire::connect_secure(&format!("{}:{}", target.id.ip_addr, target.id.xfer_port), storage, b.block_id).await?;
    let mut header = op.header.clone();
    header.base_header.block.generation_stamp = id.generation_stamp;
    header.base_header.block.num_bytes = Some(len);
    // The client's token allows the copy; the target wants one to write.
    if let Some(token) = storage.tokens.mint(b.block_id, mode::WRITE) {
        header.base_header.token = Some(token);
    }
    let stage = if rbw { BlockConstructionStage::TransferRbw } else { BlockConstructionStage::TransferFinalized };
    let write = OpWriteBlockProto {
        header,
        stage: stage as i32,
        pipeline_size: 1,
        latest_generation_stamp: id.generation_stamp,
        requested_checksum: checksum.to_proto(),
        ..Default::default()
    };
    let mut head = Vec::with_capacity(3);
    head.extend_from_slice(&DATA_TRANSFER_VERSION.to_be_bytes());
    head.push(OP_WRITE_BLOCK);
    s.write_all(&head).await?;
    write_delimited(&mut s, &write).await?;

    // Whole chunks per packet; the last chunk may be partial.
    let packet = (READ_PACKET_DATA as u64 / checksum.bytes_per_checksum as u64).max(1) * checksum.bytes_per_checksum as u64;
    let mut chunks = super::wire::spawn_file_reader(file, len, packet as usize);
    let (mut offset, mut seqno) = (0u64, 0i64);
    while let Some(data) = chunks.recv().await {
        let data = data?;
        let header = PacketHeaderProto { offset_in_block: offset as i64, seqno, last_packet_in_block: false, data_len: data.len() as i32, sync_block: Some(false) };
        let checksums = Bytes::from(checksum.compute(&data));
        offset += data.len() as u64;
        s.write_all(&Packet { header, checksums, data }.encode()).await?;
        seqno += 1;
    }
    let last = PacketHeaderProto { offset_in_block: offset as i64, seqno, last_packet_in_block: true, data_len: 0, sync_block: Some(false) };
    s.write_all(&Packet { header: last, checksums: Bytes::new(), data: Bytes::new() }.encode()).await?;
    s.flush().await?;
    let r: BlockOpResponseProto = read_delimited(&mut s).await?;
    if r.status != Status::Success as i32 {
        return Err(TransportError::ProtocolError(format!("target refused the copy: {}", r.message.unwrap_or_default())));
    }
    Ok(len)
}

/// Connect to the next DataNode and forward the WRITE_BLOCK op with the
/// remaining targets; returns its connect-ack status and firstBadLink.
async fn connect_mirror(storage: &BlockStorage, addr: &str, op: &OpWriteBlockProto) -> Result<(super::sasl::DataStream, Status, String)> {
    let mut s = super::wire::connect_secure(addr, storage, op.header.base_header.block.block_id).await?;
    let mut fwd = op.clone();
    fwd.targets = op.targets.iter().skip(1).cloned().collect::<Vec<DatanodeInfoProto>>();
    fwd.target_storage_types = op.target_storage_types.iter().skip(1).copied().collect();
    fwd.target_pinnings = op.target_pinnings.iter().skip(1).copied().collect();
    fwd.storage_id = op.target_storage_ids.first().cloned();
    fwd.target_storage_ids = op.target_storage_ids.iter().skip(1).cloned().collect();

    let mut head = Vec::with_capacity(3);
    head.extend_from_slice(&DATA_TRANSFER_VERSION.to_be_bytes());
    head.push(OP_WRITE_BLOCK);
    s.write_all(&head).await?;
    write_delimited(&mut s, &fwd).await?;
    s.flush().await?;
    let r: BlockOpResponseProto = read_delimited(&mut s).await?;
    let status = Status::try_from(r.status).unwrap_or(Status::Error);
    Ok((s, status, r.first_bad_link.unwrap_or_default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_published_check_value() {
        // CRC-32C("123456789") = 0xE3069283 (RFC 3720 B.4 check value).
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32fast::hash(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn checksums_cover_each_chunk_and_catch_corruption() {
        let c = Checksum::CRC32C_512;
        let data: Vec<u8> = (0..1300u32).map(|i| i as u8).collect();
        let sums = c.compute(&data);
        assert_eq!(sums.len(), 3 * 4);
        assert!(c.verify(&data, &sums).is_ok());
        let mut bad = data.clone();
        bad[600] ^= 1;
        assert_eq!(c.verify(&bad, &sums), Err(1));
        assert!(c.verify(&data, &sums[..8]).is_err());
    }

    #[test]
    fn the_meta_header_is_hdfs_layout() {
        assert_eq!(Checksum::CRC32C_512.meta_header(), [0, 1, 2, 0, 0, 2, 0]);
    }

    #[tokio::test]
    async fn packets_round_trip_and_plen_counts_itself() {
        let p = Packet {
            header: PacketHeaderProto { offset_in_block: 512, seqno: 3, last_packet_in_block: false, data_len: 5, sync_block: None },
            checksums: Bytes::from_static(&[1, 2, 3, 4]),
            data: Bytes::from_static(b"hello"),
        };
        let enc = p.encode();
        assert_eq!(u32::from_be_bytes(enc[..4].try_into().unwrap()), 4 + 4 + 5);
        let (back, raw) = read_packet(&mut &enc[..]).await.unwrap();
        assert_eq!(raw, enc);
        assert_eq!(back.header, p.header);
        assert_eq!(&back.data[..], b"hello");
        assert_eq!(&back.checksums[..], &[1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn delimited_messages_round_trip() {
        let m = op_response(Status::ErrorInvalid, "nope");
        let mut buf = Vec::new();
        write_delimited(&mut buf, &m).await.unwrap();
        let back: BlockOpResponseProto = read_delimited(&mut &buf[..]).await.unwrap();
        assert_eq!(back, m);
    }
}
