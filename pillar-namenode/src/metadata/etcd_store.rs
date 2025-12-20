use etcd_client::{Client, GetOptions, Error as EtcdError, Txn, TxnOp, Compare, CompareOp};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use tracing::info;

use crate::metadata::namespace::INode;
use crate::metadata::block_manager::BlockId;

#[derive(Error, Debug)]
pub enum EtcdStoreError {
    #[error("Etcd client error: {0}")]
    EtcdError(#[from] EtcdError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Key not found: {0}")]
    NotFound(String),

    #[error("Invalid data format")]
    InvalidFormat,
}

pub type Result<T> = std::result::Result<T, EtcdStoreError>;

/// Etcd-backed metadata storage for high availability
/// Stores NameNode metadata in etcd for persistence and HA failover
///
/// The etcd Client is thread-safe and can be cloned cheaply (internally Arc-wrapped),
/// so no Mutex is needed. Uses etcd transactions for atomic operations.
pub struct EtcdMetadataStore {
    client: Client,
    namespace_prefix: String,
}

impl EtcdMetadataStore {
    /// Create a new etcd metadata store
    pub async fn new(endpoints: Vec<String>) -> Result<Self> {
        info!("Connecting to etcd cluster: {:?}", endpoints);

        let client = Client::connect(endpoints, None).await?;

        let store = Self {
            client,
            namespace_prefix: "/pillar/namenode".to_string(),
        };

        info!("Connected to etcd successfully");

        Ok(store)
    }

    /// Initialize the metadata store (create root directory if needed)
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing etcd metadata store");

        // Create root inode if it doesn't exist
        let root_key = self.inode_key("/");

        match self.get_inode("/").await {
            Ok(_) => {
                info!("Root inode already exists");
            }
            Err(EtcdStoreError::NotFound(_)) => {
                info!("Creating root inode");
                let root_inode = INode::new_dir(
                    0, // Root has ID 0
                    "".to_string(),
                    "hdfs".to_string(),
                    "supergroup".to_string(),
                );
                self.put_inode("/", &root_inode).await?;
            }
            Err(e) => return Err(e),
        }

        // Initialize metadata counters
        self.init_counter("next_inode_id", 1).await?;
        self.init_counter("next_block_id", 1).await?;
        self.init_counter("next_generation_stamp", 1000).await?;

        info!("Etcd metadata store initialized successfully");

        Ok(())
    }

    /// Store an inode
    pub async fn put_inode(&self, path: &str, inode: &INode) -> Result<()> {
        let key = self.inode_key(path);
        let value = serde_json::to_string(inode)?;

        self.client.clone().put(key, value, None).await?;

        Ok(())
    }

    /// Retrieve an inode
    pub async fn get_inode(&self, path: &str) -> Result<INode> {
        let key = self.inode_key(path);

        let response = self.client.clone().get(key.clone(), None).await?;

        if let Some(kv) = response.kvs().first() {
            let value = kv.value_str()?;
            let inode: INode = serde_json::from_str(value)?;
            Ok(inode)
        } else {
            Err(EtcdStoreError::NotFound(path.to_string()))
        }
    }

    /// Delete an inode
    pub async fn delete_inode(&self, path: &str) -> Result<()> {
        let key = self.inode_key(path);

        self.client.clone().delete(key, None).await?;

        Ok(())
    }

    /// List children of a directory
    pub async fn list_children(&self, parent_path: &str) -> Result<Vec<String>> {
        let prefix = format!("{}/inodes{}/", self.namespace_prefix, parent_path);

        let options = GetOptions::new().with_prefix();
        let response = self.client.clone()
            .get(prefix.clone(), Some(options))
            .await?;

        let mut children = Vec::new();
        for kv in response.kvs() {
            let key = kv.key_str()?;
            // Extract child name from key
            if let Some(child_path) = key.strip_prefix(&prefix) {
                if !child_path.contains('/') {
                    children.push(child_path.to_string());
                }
            }
        }

        Ok(children)
    }

    /// Store block metadata
    pub async fn put_block(&self, block_id: &BlockId, inode_id: u64, num_bytes: u64) -> Result<()> {
        let key = self.block_key(block_id);
        let value = serde_json::to_string(&BlockMetadata {
            inode_id,
            num_bytes,
            generation_stamp: block_id.generation_stamp,
        })?;

        self.client.clone()
            .put(key, value, None)
            .await?;

        Ok(())
    }

    /// Retrieve block metadata
    pub async fn get_block(&self, block_id: &BlockId) -> Result<BlockMetadata> {
        let key = self.block_key(block_id);

        let response = self.client.clone().get(key.clone(), None).await?;

        if let Some(kv) = response.kvs().first() {
            let value = kv.value_str()?;
            let metadata: BlockMetadata = serde_json::from_str(value)?;
            Ok(metadata)
        } else {
            Err(EtcdStoreError::NotFound(format!("block_{}", block_id.block_id)))
        }
    }

    /// Store block location (DataNode mapping)
    pub async fn add_block_location(&self, block_id: &BlockId, datanode_uuid: &str) -> Result<()> {
        let key = format!(
            "{}/block_locations/{}/{}",
            self.namespace_prefix,
            block_id.block_id,
            datanode_uuid
        );

        self.client.clone()
            .put(key, "", None)
            .await?;

        Ok(())
    }

    /// Get all DataNodes storing a block
    pub async fn get_block_locations(&self, block_id: &BlockId) -> Result<Vec<String>> {
        let prefix = format!(
            "{}/block_locations/{}/",
            self.namespace_prefix,
            block_id.block_id
        );

        let options = GetOptions::new().with_prefix();
        let response = self.client.clone()
            .get(prefix.clone(), Some(options))
            .await?;

        let mut locations = Vec::new();
        for kv in response.kvs() {
            let key = kv.key_str()?;
            if let Some(uuid) = key.strip_prefix(&prefix) {
                locations.push(uuid.to_string());
            }
        }

        Ok(locations)
    }

    /// Atomic counter increment using etcd transactions (for IDs and generation stamps)
    /// Uses optimistic locking with compare-and-swap for true atomicity
    pub async fn increment_counter(&self, counter_name: &str) -> Result<u64> {
        let key = format!("{}/counters/{}", self.namespace_prefix, counter_name);

        // Retry loop for compare-and-swap
        for _ in 0..10 {
            // Read current value
            let response = self.client.clone().get(key.clone(), None).await?;

            let (current_value, mod_revision) = if let Some(kv) = response.kvs().first() {
                let val = kv.value_str()?.parse::<u64>().unwrap_or(0);
                let rev = kv.mod_revision();
                (val, rev)
            } else {
                (0, 0)
            };

            let new_value = current_value + 1;

            // Use transaction to atomically increment only if value hasn't changed
            let txn = if mod_revision > 0 {
                // Key exists - compare mod_revision
                Txn::new()
                    .when(vec![Compare::mod_revision(key.clone(), CompareOp::Equal, mod_revision)])
                    .and_then(vec![TxnOp::put(key.clone(), new_value.to_string(), None)])
            } else {
                // Key doesn't exist - create it
                Txn::new()
                    .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                    .and_then(vec![TxnOp::put(key.clone(), new_value.to_string(), None)])
            };

            let txn_response = self.client.clone().txn(txn).await?;

            if txn_response.succeeded() {
                return Ok(new_value);
            }
            // Transaction failed - retry with new value
        }

        Err(EtcdStoreError::EtcdError(EtcdError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to increment counter after retries",
        ))))
    }

    /// Initialize a counter if it doesn't exist
    async fn init_counter(&self, counter_name: &str, initial_value: u64) -> Result<()> {
        let key = format!("{}/counters/{}", self.namespace_prefix, counter_name);

        let response = self.client.clone().get(key.clone(), None).await?;

        if response.kvs().is_empty() {
            self.client.clone()
                .put(key, initial_value.to_string(), None)
                .await?;
            info!("Initialized counter {} to {}", counter_name, initial_value);
        }

        Ok(())
    }

    /// Store DataNode registration
    pub async fn register_datanode(&self, uuid: &str, info: &DataNodeInfo) -> Result<()> {
        let key = format!("{}/datanodes/{}", self.namespace_prefix, uuid);
        let value = serde_json::to_string(info)?;

        self.client.clone().put(key, value, None).await?;

        info!("Registered DataNode {} in etcd", uuid);

        Ok(())
    }

    /// Update DataNode heartbeat timestamp
    pub async fn update_datanode_heartbeat(&self, uuid: &str, timestamp: u64) -> Result<()> {
        let key = format!("{}/datanodes/{}/last_heartbeat", self.namespace_prefix, uuid);

        self.client.clone().put(key, timestamp.to_string(), None).await?;

        Ok(())
    }

    /// Get all registered DataNodes
    pub async fn list_datanodes(&self) -> Result<Vec<(String, DataNodeInfo)>> {
        let prefix = format!("{}/datanodes/", self.namespace_prefix);

        let options = GetOptions::new().with_prefix();
        let response = self.client.clone().get(prefix.clone(), Some(options)).await?;

        let mut datanodes = Vec::new();
        for kv in response.kvs() {
            let key = kv.key_str()?;
            if key.ends_with("/last_heartbeat") {
                continue;
            }

            if let Some(uuid) = key.strip_prefix(&prefix) {
                if !uuid.contains('/') {
                    let value = kv.value_str()?;
                    let info: DataNodeInfo = serde_json::from_str(value)?;
                    datanodes.push((uuid.to_string(), info));
                }
            }
        }

        Ok(datanodes)
    }

    /// Helper: Generate etcd key for inode
    fn inode_key(&self, path: &str) -> String {
        format!("{}/inodes{}", self.namespace_prefix, path)
    }

    /// Helper: Generate etcd key for block
    fn block_key(&self, block_id: &BlockId) -> String {
        format!("{}/blocks/{}", self.namespace_prefix, block_id.block_id)
    }
}

/// Block metadata stored in etcd
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMetadata {
    pub inode_id: u64,
    pub num_bytes: u64,
    pub generation_stamp: u64,
}

/// DataNode registration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataNodeInfo {
    pub hostname: String,
    pub rpc_port: u16,
    pub data_port: u16,
    pub http_port: u16,
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires running etcd instance
    async fn test_etcd_store() {
        let endpoints = vec!["localhost:2379".to_string()];
        let store = EtcdMetadataStore::new(endpoints).await.unwrap();

        store.initialize().await.unwrap();

        // Test counter increment
        let id1 = store.increment_counter("test_counter").await.unwrap();
        let id2 = store.increment_counter("test_counter").await.unwrap();
        assert_eq!(id2, id1 + 1);
    }
}
