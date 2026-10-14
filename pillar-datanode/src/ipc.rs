//! The DataNode's Hadoop IPC port (`dfs.datanode.ipc.address`), serving
//! `ClientDatanodeProtocol` on the shared IPC engine
//! ([`pillar_common::ipc`]).
//!
//! Only `getReplicaVisibleLength` is implemented: a client reading a file
//! that is still being written asks a DataNode of the last block's pipeline
//! how much of it the pipeline has acknowledged (what hflush made visible).
//! Other methods answer `RpcNoSuchMethodException`.
//!
//! With block access tokens on, callers authenticate with the block's
//! token (SASL DIGEST-MD5, as Hadoop's DFSClient does) and may only ask
//! about that block.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use tokio::net::TcpListener;

use pillar_common::hdfs::block_token::{mode, parse_token_caller};
use pillar_common::ipc::{Authentication, RpcError, RpcHandler, TokenAuthority};
use pillar_common::sasl::Protection;
use pillar_proto::hdfs::{GetReplicaVisibleLengthRequestProto, GetReplicaVisibleLengthResponseProto};

use crate::storage::BlockStorage;

/// The protocol Hadoop clients name in `RequestHeaderProto`.
pub const CLIENT_DATANODE_PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol";

pub struct ClientDatanodeProtocol {
    storage: Arc<BlockStorage>,
    auth: Authentication,
    protection: Protection,
}

impl ClientDatanodeProtocol {
    pub fn new(storage: Arc<BlockStorage>, auth: Authentication, protection: Protection) -> Self {
        Self { storage, auth, protection }
    }

    /// HDFS `getReplicaVisibleLength`: all of a finalized replica, the
    /// acknowledged bytes of one being written.
    fn replica_visible_length(&self, body: &[u8], user: &str) -> Result<Vec<u8>, RpcError> {
        let req = GetReplicaVisibleLengthRequestProto::decode(body)
            .map_err(|e| RpcError::bad_request("getReplicaVisibleLength", e))?;
        let b = &req.block;
        if self.storage.tokens.enabled() {
            match parse_token_caller(user) {
                Some((block_id, modes)) if block_id == b.block_id && modes & mode::READ != 0 => {}
                _ => {
                    return Err(RpcError::application(
                        "org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException",
                        format!("{} has no block access token for block {}", user, b.block_id),
                    ))
                }
            }
        }
        let (id, length) = self.storage.replica_visible_length(b.block_id).ok_or_else(|| {
            RpcError::application(
                "org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException",
                format!("Replica not found for block {}", b.block_id),
            )
        })?;
        if id.generation_stamp < b.generation_stamp {
            return Err(RpcError::application(
                "java.io.IOException",
                format!(
                    "replica of block {} has generation stamp {} < {}",
                    b.block_id, id.generation_stamp, b.generation_stamp
                ),
            ));
        }
        Ok(GetReplicaVisibleLengthResponseProto { length }.encode_to_vec())
    }
}

#[async_trait]
impl RpcHandler for ClientDatanodeProtocol {
    fn protocol(&self) -> &'static str {
        CLIENT_DATANODE_PROTOCOL
    }

    fn authentication(&self) -> &Authentication {
        &self.auth
    }

    fn tokens(&self) -> Option<&dyn TokenAuthority> {
        Some(&self.storage.tokens)
    }

    fn protection(&self) -> Protection {
        self.protection.clone()
    }

    async fn call(&self, method: &str, body: &[u8], user: &str) -> Result<Vec<u8>, RpcError> {
        match method {
            "getReplicaVisibleLength" => self.replica_visible_length(body, user),
            _ => Err(RpcError::no_such_method(method, CLIENT_DATANODE_PROTOCOL)),
        }
    }
}

/// Bind `addr` and serve `ClientDatanodeProtocol` on it in the background,
/// authenticating callers by `auth`.
pub async fn serve(
    addr: SocketAddr,
    storage: Arc<BlockStorage>,
    auth: Authentication,
    protection: Protection,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(pillar_common::ipc::serve(listener, Arc::new(ClientDatanodeProtocol::new(storage, auth, protection))));
    Ok(bound)
}
