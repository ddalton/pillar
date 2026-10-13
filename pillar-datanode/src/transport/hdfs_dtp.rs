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
//! Writes support the `PIPELINE_SETUP_CREATE` stage (a new block, which is
//! what `create`/`put` uses), forward the pipeline to the next target, and
//! verify every chunk's checksum before acknowledging it. The block's
//! `.meta` file is written in HDFS layout (version 1, checksum header,
//! one checksum per chunk). Append, pipeline recovery, block checksums and
//! short-circuit reads are answered with `ERROR_UNSUPPORTED`. There is no
//! SASL / block-token handling: this is for clusters without Kerberos.

use std::io::{Seek, Write};
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use pillar_proto::hdfs::{
    op_write_block_proto::BlockConstructionStage, BlockOpResponseProto, ChecksumProto, ChecksumTypeProto,
    ClientReadStatusProto, DatanodeInfoProto, OpReadBlockProto, OpWriteBlockProto, PacketHeaderProto,
    PipelineAckProto, ReadOpChecksumInfoProto, Status,
};

use super::{Result, TransportError};
use crate::storage::block_storage::{BlockId, BlockStorage, BlockStorageError};

/// `DataTransferProtocol.DATA_TRANSFER_VERSION`.
pub const DATA_TRANSFER_VERSION: u16 = 28;

const OP_WRITE_BLOCK: u8 = 80;
const OP_READ_BLOCK: u8 = 81;

/// Data per READ packet (HDFS `dfs.client-write-packet-size` is 64 KiB).
const READ_PACKET_DATA: usize = 64 * 1024;
/// Largest packet accepted from a writer.
const MAX_PACKET: usize = 16 * 1024 * 1024;
/// `PacketHeader.HEART_BEAT_SEQNO`.
const HEARTBEAT_SEQNO: i64 = -1;
/// Server-side checksum for reads of blocks written without one.
const DEFAULT_BYTES_PER_CHECKSUM: u32 = 512;

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
            let op = read_delimited(socket).await?;
            write_block(socket, storage, op, peer).await
        }
        OP_READ_BLOCK => {
            let op = read_delimited(socket).await?;
            read_block(socket, storage, op, peer).await
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

async fn read_block<S>(socket: &mut S, storage: &Arc<BlockStorage>, op: OpReadBlockProto, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let b = &op.header.base_header.block;
    let block = BlockId::new(b.block_id, b.generation_stamp);
    let checksum = Checksum::CRC32C_512;
    let bpc = checksum.bytes_per_checksum as u64;

    let size = match storage.get_block_metadata(b.block_id) {
        Some(m) if m.block_id.generation_stamp >= b.generation_stamp => m.num_bytes,
        Some(m) => {
            let msg = format!("replica {} has genstamp {} < {}", b.block_id, m.block_id.generation_stamp, b.generation_stamp);
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

    let (file, len) = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || storage.open_for_read(&block, start, end - start))
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
}

async fn write_block<S>(socket: &mut S, storage: &Arc<BlockStorage>, op: OpWriteBlockProto, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let b = op.header.base_header.block.clone();
    let block = BlockId::new(b.block_id, b.generation_stamp);
    let client = op.header.client_name.clone();

    let stage = BlockConstructionStage::try_from(op.stage).ok();
    if stage != Some(BlockConstructionStage::PipelineSetupCreate) {
        let msg = format!("block construction stage {:?} is not supported by Pillar", stage);
        write_delimited(socket, &op_response(Status::ErrorUnsupported, &msg)).await?;
        return socket.flush().await.map_err(Into::into);
    }
    let checksum = match Checksum::from_proto(&op.requested_checksum) {
        Ok(c) => c,
        Err(e) => {
            write_delimited(socket, &op_response(Status::ErrorInvalid, &e.to_string())).await?;
            return socket.flush().await.map_err(Into::into);
        }
    };

    // Open the replica before acknowledging the pipeline.
    let opened = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || {
            let data = storage.begin_write(&block)?;
            let meta = storage.open_tmp_meta(&block)?;
            Ok::<_, BlockStorageError>((data, meta))
        })
        .await
        .map_err(|e| TransportError::ProtocolError(e.to_string()))?
    };
    let (data_file, mut meta_file) = match opened {
        Ok(f) => f,
        Err(e) => {
            let status = if matches!(e, BlockStorageError::BlockAlreadyExists(_)) { Status::ErrorExists } else { Status::Error };
            write_delimited(socket, &op_response(status, &e.to_string())).await?;
            return socket.flush().await.map_err(Into::into);
        }
    };

    // Set up the rest of the pipeline before answering the upstream.
    let mut mirror: Option<TcpStream> = None;
    let mut first_bad_link = String::new();
    let mut connect_status = Status::Success;
    if let Some(next) = op.targets.first() {
        let addr = format!("{}:{}", next.id.ip_addr, next.id.xfer_port);
        match connect_mirror(&addr, &op).await {
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
        storage.abort_write(&block);
        return Ok(());
    }

    // Disk writer thread: data and checksums at their positions, so a
    // re-sent partial chunk (after hflush) simply overwrites.
    meta_file.write_all(&checksum.meta_header())?;
    let header_len = meta_file.stream_position()?;
    let (disk_tx, mut disk_rx) = mpsc::channel::<DiskWrite>(8);
    let bpc = checksum.bytes_per_checksum as u64;
    let csize = checksum.size() as u64;
    let writer = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        let mut len = 0u64;
        while let Some(w) = disk_rx.blocking_recv() {
            data_file.write_all_at(&w.data, w.offset)?;
            meta_file.write_all_at(&w.checksums, header_len + w.offset / bpc * csize)?;
            len = len.max(w.offset + w.data.len() as u64);
        }
        data_file.set_len(len)?;
        meta_file.set_len(header_len + len.div_ceil(bpc) * csize)?;
        data_file.sync_data()?;
        meta_file.sync_data()?;
        Ok(len)
    });

    let (upstream_rx, mut upstream_tx) = tokio::io::split(socket);
    let (mirror_rx, mirror_tx) = match mirror {
        Some(m) => {
            let (r, w) = m.into_split();
            (Some(r), Some(w))
        }
        None => (None, None),
    };
    // (seqno, last packet?, local status) in arrival order.
    let (ack_tx, mut ack_rx) = mpsc::channel::<(i64, bool, Status)>(64);

    // Receiver: upstream packets -> verify -> mirror + disk.
    let receiver = async move {
        let mut upstream_rx = upstream_rx;
        let mut mirror_tx = mirror_tx;
        let mut written_to = 0u64;
        loop {
            let (p, raw) = read_packet(&mut upstream_rx).await?;
            let h = &p.header;
            if let Some(m) = mirror_tx.as_mut() {
                m.write_all(&raw).await?;
            }
            if h.seqno == HEARTBEAT_SEQNO {
                ack_tx.send((h.seqno, false, Status::Success)).await.ok();
                continue;
            }
            let offset = h.offset_in_block as u64;
            if offset > written_to || h.offset_in_block < 0 || (offset % bpc != 0 && !p.data.is_empty()) {
                return Err(TransportError::ProtocolError(format!("packet at offset {} after {} bytes", offset, written_to)));
            }
            let status = match checksum.verify(&p.data, &p.checksums) {
                Ok(()) => Status::Success,
                Err(chunk) => {
                    warn!("Checksum error in block {} chunk {} at offset {} from {}", b.block_id, chunk, offset, peer);
                    Status::ErrorChecksum
                }
            };
            if status == Status::Success && !p.data.is_empty() {
                written_to = written_to.max(offset + p.data.len() as u64);
                disk_tx
                    .send(DiskWrite { offset, data: p.data.clone(), checksums: p.checksums.clone() })
                    .await
                    .map_err(|_| TransportError::ProtocolError("disk writer stopped".into()))?;
            }
            let last = h.last_packet_in_block;
            ack_tx.send((h.seqno, last, status)).await.ok();
            if status != Status::Success {
                return Err(TransportError::ProtocolError("checksum error".into()));
            }
            if last {
                if let Some(mut m) = mirror_tx.take() {
                    m.flush().await?;
                }
                drop(disk_tx);
                return Ok(written_to);
            }
        }
    };

    // Responder: in packet order, wait for the mirror's ack (if any), then
    // acknowledge upstream with our status followed by the downstream ones.
    let storage2 = storage.clone();
    let responder = async move {
        let mut mirror_rx = mirror_rx;
        let mut writer = Some(writer);
        while let Some((seqno, last, status)) = ack_rx.recv().await {
            let mut replies = vec![status];
            if let Some(m) = mirror_rx.as_mut() {
                if status == Status::Success {
                    let down: PipelineAckProto = read_delimited(m).await?;
                    if down.seqno != seqno {
                        return Err(TransportError::ProtocolError(format!("mirror acked {} for {}", down.seqno, seqno)));
                    }
                    replies.extend(down.reply.iter().filter_map(|r| Status::try_from(*r).ok()));
                }
            }
            if last && status == Status::Success {
                let len = writer
                    .take()
                    .expect("one last packet")
                    .await
                    .map_err(|e| TransportError::ProtocolError(e.to_string()))??;
                let s = storage2.clone();
                tokio::task::spawn_blocking(move || s.finalize_block(&block))
                    .await
                    .map_err(|e| TransportError::ProtocolError(e.to_string()))?
                    .map_err(|e| TransportError::ProtocolError(e.to_string()))?;
                info!("Received block {} ({} bytes) from {} via HDFS protocol", block.block_id, len, peer);
            }
            write_delimited(&mut upstream_tx, &ack(seqno, &replies)).await?;
            upstream_tx.flush().await?;
            if last || status != Status::Success {
                return Ok(());
            }
        }
        Err(TransportError::ProtocolError("pipeline closed before the last packet".into()))
    };

    let (received, responded) = tokio::join!(receiver, responder);
    if let Err(e) = received.and(responded) {
        storage.abort_write(&block);
        return Err(e);
    }
    Ok(())
}

/// Connect to the next DataNode and forward the WRITE_BLOCK op with the
/// remaining targets; returns its connect-ack status and firstBadLink.
async fn connect_mirror(addr: &str, op: &OpWriteBlockProto) -> Result<(TcpStream, Status, String)> {
    let mut s = super::wire::connect(addr).await?;
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
