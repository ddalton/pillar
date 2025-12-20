pub mod volume_manager;
pub mod block_storage;

// Note: data_transfer module has been refactored into crate::transport
// The old DataTransferServer is now replaced by TcpTransport + TransportSelector
// This module is kept temporarily for backward compatibility but will be removed
pub mod data_transfer;

pub use volume_manager::{VolumeManager, VolumeInfo};
pub use block_storage::{BlockStorage, BlockMetadata};

#[deprecated(note = "Use crate::transport::tcp::TcpTransport instead")]
pub use data_transfer::DataTransferServer;
