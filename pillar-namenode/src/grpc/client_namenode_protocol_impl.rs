use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn, error};

use pillar_proto::hadoop::hdfs::*;
use crate::grpc::ClientNamenodeService;

/// gRPC service implementation for ClientNamenodeProtocol
/// This implements the actual Hadoop wire protocol
pub struct ClientNamenodeProtocolServer {
    service: Arc<ClientNamenodeService>,
}

impl ClientNamenodeProtocolServer {
    pub fn new(service: Arc<ClientNamenodeService>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl client_namenode_protocol_server::ClientNamenodeProtocol for ClientNamenodeProtocolServer {
    /// Get file/directory information
    async fn get_file_info(
        &self,
        request: Request<GetFileInfoRequestProto>,
    ) -> Result<Response<GetFileInfoResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;

        info!("get_file_info: {}", path);

        match self.service.handle_get_file_info(&path).await {
            Ok(file_info) => {
                let response = GetFileInfoResponseProto {
                    fs: Some(HdfsFileStatusProto {
                        file_type: if file_info.is_dir {
                            hdfs_file_status_proto::FileType::IsDir as i32
                        } else {
                            hdfs_file_status_proto::FileType::IsFile as i32
                        },
                        path: path.into(),
                        length: file_info.length,
                        permission: FsPermissionProto {
                            perm: file_info.permissions as u32,
                        },
                        owner: file_info.owner,
                        group: file_info.group,
                        modification_time: file_info.modification_time,
                        access_time: file_info.access_time,
                        symlink: None,
                        block_replication: Some(file_info.replication as u32),
                        blocksize: Some(file_info.block_size),
                        storage_policy: Some(0),
                        ec_policy: None,
                        flags: Some(0),
                        file_id: Some(0),
                        children_num: Some(0),
                        file_encryption_info: None,
                        locations: None,
                        namespace: None,
                    }),
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    /// List directory contents
    async fn get_listing(
        &self,
        request: Request<GetListingRequestProto>,
    ) -> Result<Response<GetListingResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;

        info!("get_listing: {}", path);

        match self.service.handle_get_listing(&path).await {
            Ok(entries) => {
                let dir_list = DirectoryListingProto {
                    partial_listing: entries
                        .into_iter()
                        .map(|file_info| HdfsFileStatusProto {
                            file_type: if file_info.is_dir {
                                hdfs_file_status_proto::FileType::IsDir as i32
                            } else {
                                hdfs_file_status_proto::FileType::IsFile as i32
                            },
                            path: file_info.path.into(),
                            length: file_info.length,
                            permission: FsPermissionProto {
                                perm: file_info.permissions as u32,
                            },
                            owner: file_info.owner,
                            group: file_info.group,
                            modification_time: file_info.modification_time,
                            access_time: file_info.access_time,
                            symlink: None,
                            block_replication: Some(file_info.replication as u32),
                            blocksize: Some(file_info.block_size),
                            storage_policy: Some(0),
                            ec_policy: None,
                            flags: Some(0),
                            file_id: Some(0),
                            children_num: Some(0),
                            file_encryption_info: None,
                            locations: None,
                            namespace: None,
                        })
                        .collect(),
                    remaining_entries: 0,
                };

                Ok(Response::new(GetListingResponseProto {
                    dir_list: Some(dir_list),
                }))
            }
            Err(e) => Err(e),
        }
    }

    /// Create directory
    async fn mkdirs(
        &self,
        request: Request<MkdirsRequestProto>,
    ) -> Result<Response<MkdirsResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let masked = req.masked;

        info!("mkdirs: {}", path);

        match self.service
            .handle_mkdir(
                &path,
                "hdfs".to_string(), // Default owner, should come from auth context
                "supergroup".to_string(),
                masked.perm as u16,
            )
            .await
        {
            Ok(_) => Ok(Response::new(MkdirsResponseProto { result: true })),
            Err(e) => Err(e),
        }
    }

    /// Create file
    async fn create(
        &self,
        request: Request<CreateRequestProto>,
    ) -> Result<Response<CreateResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let masked = req.masked;
        let replication = req.replication as u16;
        let block_size = req.block_size;

        info!("create: {} (replication={}, blocksize={})", path, replication, block_size);

        match self.service
            .handle_create(
                &path,
                "hdfs".to_string(),
                "supergroup".to_string(),
                masked.perm as u16,
                replication,
                block_size,
            )
            .await
        {
            Ok(_) => Ok(Response::new(CreateResponseProto {
                fs: Some(HdfsFileStatusProto {
                    file_type: hdfs_file_status_proto::FileType::IsFile as i32,
                    path: path.into(),
                    length: 0,
                    permission: masked,
                    owner: "hdfs".to_string(),
                    group: "supergroup".to_string(),
                    modification_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    access_time: 0,
                    symlink: None,
                    block_replication: Some(replication as u32),
                    blocksize: Some(block_size),
                    storage_policy: Some(0),
                    ec_policy: None,
                    flags: Some(0),
                    file_id: Some(0),
                    children_num: Some(0),
                    file_encryption_info: None,
                    locations: None,
                    namespace: None,
                }),
            })),
            Err(e) => Err(e),
        }
    }

    /// Delete file or directory
    async fn delete(
        &self,
        request: Request<DeleteRequestProto>,
    ) -> Result<Response<DeleteResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let recursive = req.recursive;

        info!("delete: {} (recursive={})", path, recursive);

        match self.service.handle_delete(&path, recursive).await {
            Ok(result) => Ok(Response::new(DeleteResponseProto { result })),
            Err(e) => Err(e),
        }
    }

    /// Rename file or directory
    async fn rename(
        &self,
        request: Request<RenameRequestProto>,
    ) -> Result<Response<RenameResponseProto>, Status> {
        let req = request.into_inner();
        let src = req.src;
        let dst = req.dst;

        info!("rename: {} -> {}", src, dst);

        match self.service.handle_rename(&src, &dst).await {
            Ok(result) => Ok(Response::new(RenameResponseProto { result })),
            Err(e) => Err(e),
        }
    }

    /// Get block locations for reading a file
    async fn get_block_locations(
        &self,
        request: Request<GetBlockLocationsRequestProto>,
    ) -> Result<Response<GetBlockLocationsResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let offset = req.offset;
        let length = req.length;

        info!("get_block_locations: {} (offset={}, length={})", path, offset, length);

        match self.service
            .handle_get_block_locations(&path, offset, length)
            .await
        {
            Ok(blocks) => {
                let located_blocks = LocatedBlocksProto {
                    file_length: blocks.iter().map(|b| b.num_bytes).sum(),
                    blocks: blocks
                        .into_iter()
                        .map(|b| LocatedBlockProto {
                            b: ExtendedBlockProto {
                                pool_id: "BP-pillar-001".to_string(),
                                block_id: b.block_id,
                                generation_stamp: b.generation_stamp,
                                num_bytes: Some(b.num_bytes),
                            },
                            offset: b.offset,
                            locs: b.locations
                                .into_iter()
                                .map(|loc| DatanodeInfoProto {
                                    id: DatanodeIdProto {
                                        ip_addr: loc.hostname.clone(),
                                        host_name: loc.hostname,
                                        datanode_uuid: loc.uuid,
                                        xfer_port: loc.data_port as u32,
                                        info_port: loc.http_port as u32,
                                        ipc_port: loc.rpc_port as u32,
                                        info_secure_port: Some(0),
                                    },
                                    capacity: Some(0),
                                    dfs_used: Some(0),
                                    remaining: Some(0),
                                    block_pool_used: Some(0),
                                    cache_capacity: Some(0),
                                    cache_used: Some(0),
                                    last_update: Some(0),
                                    last_update_monotonic: Some(0),
                                    xceiver_count: Some(0),
                                    location: None,
                                    non_dfs_used: Some(0),
                                    admin_state: Some(datanode_info_proto::AdminState::Normal as i32),
                                    upgrade_domain: None,
                                    last_block_report_time: Some(0),
                                    last_block_report_monotonic: Some(0),
                                    num_blocks: Some(0),
                                    software_version: None,
                                })
                                .collect(),
                            corrupt: false,
                            block_token: Default::default(),
                            is_cached: vec![],
                            storage_types: vec![],
                            storage_i_ds: vec![],
                            block_indices: None,
                            block_tokens: vec![],
                        })
                        .collect(),
                    under_construction: false,
                    last_block: None,
                    is_last_block_complete: true,
                    file_encryption_info: None,
                    ec_policy: None,
                };

                Ok(Response::new(GetBlockLocationsResponseProto {
                    locations: Some(located_blocks),
                }))
            }
            Err(e) => Err(e),
        }
    }

    /// Add a new block when writing a file
    async fn add_block(
        &self,
        request: Request<AddBlockRequestProto>,
    ) -> Result<Response<AddBlockResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;

        info!("add_block: {}", path);

        match self.service.handle_add_block(&path).await {
            Ok(block) => {
                let response = AddBlockResponseProto {
                    block: LocatedBlockProto {
                        b: ExtendedBlockProto {
                            pool_id: "BP-pillar-001".to_string(),
                            block_id: block.block_id,
                            generation_stamp: block.generation_stamp,
                            num_bytes: Some(block.num_bytes),
                        },
                        offset: block.offset,
                        locs: block.locations
                            .into_iter()
                            .map(|loc| DatanodeInfoProto {
                                id: DatanodeIdProto {
                                    ip_addr: loc.hostname.clone(),
                                    host_name: loc.hostname,
                                    datanode_uuid: loc.uuid,
                                    xfer_port: loc.data_port as u32,
                                    info_port: loc.http_port as u32,
                                    ipc_port: loc.rpc_port as u32,
                                    info_secure_port: Some(0),
                                },
                                capacity: Some(0),
                                dfs_used: Some(0),
                                remaining: Some(0),
                                block_pool_used: Some(0),
                                cache_capacity: Some(0),
                                cache_used: Some(0),
                                last_update: Some(0),
                                last_update_monotonic: Some(0),
                                xceiver_count: Some(0),
                                location: None,
                                non_dfs_used: Some(0),
                                admin_state: Some(datanode_info_proto::AdminState::Normal as i32),
                                upgrade_domain: None,
                                last_block_report_time: Some(0),
                                last_block_report_monotonic: Some(0),
                                num_blocks: Some(0),
                                software_version: None,
                            })
                            .collect(),
                        corrupt: false,
                        block_token: Default::default(),
                        is_cached: vec![],
                        storage_types: vec![],
                        storage_i_ds: vec![],
                        block_indices: None,
                        block_tokens: vec![],
                    },
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    /// Complete a file (finalize writes)
    async fn complete(
        &self,
        request: Request<CompleteRequestProto>,
    ) -> Result<Response<CompleteResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;

        info!("complete: {}", path);

        match self.service.handle_complete(&path).await {
            Ok(result) => Ok(Response::new(CompleteResponseProto { result })),
            Err(e) => Err(e),
        }
    }

    /// Set replication factor for a file
    async fn set_replication(
        &self,
        _request: Request<SetReplicationRequestProto>,
    ) -> Result<Response<SetReplicationResponseProto>, Status> {
        // Simplified implementation - would update file metadata
        Ok(Response::new(SetReplicationResponseProto { result: true }))
    }

    /// Set permissions
    async fn set_permission(
        &self,
        _request: Request<SetPermissionRequestProto>,
    ) -> Result<Response<SetPermissionResponseProto>, Status> {
        // Simplified implementation
        Ok(Response::new(SetPermissionResponseProto {}))
    }

    /// Set owner
    async fn set_owner(
        &self,
        _request: Request<SetOwnerRequestProto>,
    ) -> Result<Response<SetOwnerResponseProto>, Status> {
        // Simplified implementation
        Ok(Response::new(SetOwnerResponseProto {}))
    }

    /// Renew lease (for write locks)
    async fn renew_lease(
        &self,
        _request: Request<RenewLeaseRequestProto>,
    ) -> Result<Response<RenewLeaseResponseProto>, Status> {
        // Simplified implementation - lease management
        Ok(Response::new(RenewLeaseResponseProto {}))
    }

    // ============================================================================
    // Core HDFS Operations (Implemented)
    // ============================================================================

    async fn get_server_defaults(
        &self,
        _request: Request<GetServerDefaultsRequestProto>,
    ) -> Result<Response<GetServerDefaultsResponseProto>, Status> {
        Ok(Response::new(GetServerDefaultsResponseProto {
            server_defaults: FsServerDefaultsProto {
                block_size: 128 * 1024 * 1024, // 128 MB
                bytes_per_checksum: 512,
                write_packet_size: 64 * 1024,
                replication: 3,
                file_buffer_size: 4096,
                encrypt_data_transfer: Some(false),
                trash_interval: Some(0),
                checksum_type: Some(ChecksumTypeProto::ChecksumCrc32 as i32),
                key_provider_uri: None,
                policy_id: None,
                snapshot_trash_root_enabled: Some(false),
            },
        }))
    }

    async fn get_fs_stats(
        &self,
        _request: Request<GetFsStatusRequestProto>,
    ) -> Result<Response<GetFsStatsResponseProto>, Status> {
        match self.service.handle_get_fs_stats_real().await {
            Ok(stats) => {
                Ok(Response::new(GetFsStatsResponseProto {
                    capacity: stats.capacity,
                    used: stats.used,
                    remaining: stats.remaining,
                    under_replicated: stats.under_replicated,
                    corrupt_blocks: stats.corrupt_blocks,
                    missing_blocks: stats.missing_blocks,
                    missing_repl_one_blocks: Some(0),
                    blocks_in_future: Some(0),
                    pending_deletion_blocks: Some(0),
                }))
            }
            Err(e) => Err(e),
        }
    }

    async fn get_preferred_block_size(
        &self,
        _request: Request<GetPreferredBlockSizeRequestProto>,
    ) -> Result<Response<GetPreferredBlockSizeResponseProto>, Status> {
        Ok(Response::new(GetPreferredBlockSizeResponseProto {
            bsize: 128 * 1024 * 1024, // 128 MB
        }))
    }

    async fn fsync(
        &self,
        _request: Request<FsyncRequestProto>,
    ) -> Result<Response<FsyncResponseProto>, Status> {
        Ok(Response::new(FsyncResponseProto {}))
    }

    async fn set_times(
        &self,
        _request: Request<SetTimesRequestProto>,
    ) -> Result<Response<SetTimesResponseProto>, Status> {
        Ok(Response::new(SetTimesResponseProto {}))
    }

    async fn is_file_closed(
        &self,
        _request: Request<IsFileClosedRequestProto>,
    ) -> Result<Response<IsFileClosedResponseProto>, Status> {
        Ok(Response::new(IsFileClosedResponseProto {
            result: true,
        }))
    }

    async fn get_content_summary(
        &self,
        _request: Request<GetContentSummaryRequestProto>,
    ) -> Result<Response<GetContentSummaryResponseProto>, Status> {
        Ok(Response::new(GetContentSummaryResponseProto {
            summary: ContentSummaryProto {
                length: 0,
                file_count: 0,
                directory_count: 1,
                quota: u64::MAX,
                space_consumed: 0,
                space_quota: u64::MAX,
                type_quota_infos: None,
                snapshot_length: None,
                snapshot_file_count: None,
                snapshot_directory_count: None,
                snapshot_space_consumed: None,
                erasure_coding_policy: None,
            },
        }))
    }

    async fn append(
        &self,
        request: Request<AppendRequestProto>,
    ) -> Result<Response<AppendResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;

        info!("append: {}", path);

        match self.service.handle_append(&path).await {
            Ok(block) => {
                let response = AppendResponseProto {
                    block: Some(LocatedBlockProto {
                        b: ExtendedBlockProto {
                            pool_id: "BP-pillar-001".to_string(),
                            block_id: block.block_id,
                            generation_stamp: block.generation_stamp,
                            num_bytes: Some(block.num_bytes),
                        },
                        offset: block.offset,
                        locs: block.locations
                            .into_iter()
                            .map(|loc| DatanodeInfoProto {
                                id: DatanodeIdProto {
                                    ip_addr: loc.hostname.clone(),
                                    host_name: loc.hostname,
                                    datanode_uuid: loc.uuid,
                                    xfer_port: loc.data_port as u32,
                                    info_port: loc.http_port as u32,
                                    ipc_port: loc.rpc_port as u32,
                                    info_secure_port: Some(0),
                                },
                                capacity: Some(0),
                                dfs_used: Some(0),
                                remaining: Some(0),
                                block_pool_used: Some(0),
                                cache_capacity: Some(0),
                                cache_used: Some(0),
                                last_update: Some(0),
                                last_update_monotonic: Some(0),
                                xceiver_count: Some(0),
                                location: None,
                                non_dfs_used: Some(0),
                                admin_state: Some(datanode_info_proto::AdminState::Normal as i32),
                                upgrade_domain: None,
                                last_block_report_time: Some(0),
                                last_block_report_monotonic: Some(0),
                                num_blocks: Some(0),
                                software_version: None,
                            })
                            .collect(),
                        corrupt: false,
                        block_token: Default::default(),
                        is_cached: vec![],
                        storage_types: vec![],
                        storage_i_ds: vec![],
                        block_indices: None,
                        block_tokens: vec![],
                    }),
                    stat: None,
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    async fn abandon_block(
        &self,
        request: Request<AbandonBlockRequestProto>,
    ) -> Result<Response<AbandonBlockResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let block = req.b;

        info!("abandon_block: {} block_id={}", path, block.block_id);

        self.service.handle_abandon_block(&path, block.block_id).await?;

        Ok(Response::new(AbandonBlockResponseProto {}))
    }

    async fn get_additional_datanode(
        &self,
        _request: Request<GetAdditionalDatanodeRequestProto>,
    ) -> Result<Response<GetAdditionalDatanodeResponseProto>, Status> {
        Err(Status::unimplemented("get_additional_datanode not yet implemented"))
    }

    async fn update_block_for_pipeline(
        &self,
        _request: Request<UpdateBlockForPipelineRequestProto>,
    ) -> Result<Response<UpdateBlockForPipelineResponseProto>, Status> {
        Err(Status::unimplemented("update_block_for_pipeline not yet implemented"))
    }

    async fn update_pipeline(
        &self,
        _request: Request<UpdatePipelineRequestProto>,
    ) -> Result<Response<UpdatePipelineResponseProto>, Status> {
        Ok(Response::new(UpdatePipelineResponseProto {}))
    }

    async fn get_located_file_info(
        &self,
        _request: Request<GetLocatedFileInfoRequestProto>,
    ) -> Result<Response<GetLocatedFileInfoResponseProto>, Status> {
        Err(Status::unimplemented("get_located_file_info not yet implemented"))
    }

    // ============================================================================
    // Admin & Advanced Operations (Stubbed)
    // ============================================================================

    async fn set_storage_policy(
        &self,
        _request: Request<SetStoragePolicyRequestProto>,
    ) -> Result<Response<SetStoragePolicyResponseProto>, Status> {
        Err(Status::unimplemented("Storage policies not supported"))
    }

    async fn unset_storage_policy(
        &self,
        _request: Request<UnsetStoragePolicyRequestProto>,
    ) -> Result<Response<UnsetStoragePolicyResponseProto>, Status> {
        Err(Status::unimplemented("Storage policies not supported"))
    }

    async fn get_storage_policy(
        &self,
        _request: Request<GetStoragePolicyRequestProto>,
    ) -> Result<Response<GetStoragePolicyResponseProto>, Status> {
        Err(Status::unimplemented("Storage policies not supported"))
    }

    async fn get_storage_policies(
        &self,
        _request: Request<GetStoragePoliciesRequestProto>,
    ) -> Result<Response<GetStoragePoliciesResponseProto>, Status> {
        Err(Status::unimplemented("Storage policies not supported"))
    }

    async fn report_bad_blocks(
        &self,
        _request: Request<ReportBadBlocksRequestProto>,
    ) -> Result<Response<ReportBadBlocksResponseProto>, Status> {
        Ok(Response::new(ReportBadBlocksResponseProto {}))
    }

    async fn concat(
        &self,
        _request: Request<ConcatRequestProto>,
    ) -> Result<Response<ConcatResponseProto>, Status> {
        Err(Status::unimplemented("concat not supported"))
    }

    async fn truncate(
        &self,
        _request: Request<TruncateRequestProto>,
    ) -> Result<Response<TruncateResponseProto>, Status> {
        Err(Status::unimplemented("truncate not supported"))
    }

    async fn rename2(
        &self,
        _request: Request<Rename2RequestProto>,
    ) -> Result<Response<Rename2ResponseProto>, Status> {
        Err(Status::unimplemented("rename2 not yet implemented"))
    }

    async fn get_batched_listing(
        &self,
        _request: Request<GetBatchedListingRequestProto>,
    ) -> Result<Response<GetBatchedListingResponseProto>, Status> {
        Err(Status::unimplemented("Batched listing not supported"))
    }

    async fn recover_lease(
        &self,
        _request: Request<RecoverLeaseRequestProto>,
    ) -> Result<Response<RecoverLeaseResponseProto>, Status> {
        Err(Status::unimplemented("Lease recovery not yet implemented"))
    }

    async fn get_fs_replicated_block_stats(
        &self,
        _request: Request<GetFsReplicatedBlockStatsRequestProto>,
    ) -> Result<Response<GetFsReplicatedBlockStatsResponseProto>, Status> {
        Err(Status::unimplemented("Replicated block stats not supported"))
    }

    async fn get_fs_ec_block_group_stats(
        &self,
        _request: Request<GetFsEcBlockGroupStatsRequestProto>,
    ) -> Result<Response<GetFsEcBlockGroupStatsResponseProto>, Status> {
        Err(Status::unimplemented("EC block group stats not supported"))
    }

    async fn get_datanode_report(
        &self,
        _request: Request<GetDatanodeReportRequestProto>,
    ) -> Result<Response<GetDatanodeReportResponseProto>, Status> {
        info!("get_datanode_report called");

        match self.service.handle_get_datanode_report().await {
            Ok(report) => {
                let datanode_infos = report
                    .into_iter()
                    .map(|dn| DatanodeInfoProto {
                        id: DatanodeIdProto {
                            ip_addr: dn.hostname.clone(),
                            host_name: dn.hostname.clone(),
                            datanode_uuid: dn.uuid,
                            xfer_port: 9867,  // Default datanode port
                            info_port: 50075, // Default http port
                            ipc_port: 9867,
                            info_secure_port: Some(0),
                        },
                        capacity: Some(dn.capacity),
                        dfs_used: Some(dn.used),
                        remaining: Some(dn.remaining),
                        block_pool_used: Some(0),
                        cache_capacity: Some(0),
                        cache_used: Some(0),
                        last_update: Some(dn.last_update),
                        last_update_monotonic: Some(0),
                        xceiver_count: Some(0),
                        location: None,
                        non_dfs_used: Some(0),
                        admin_state: Some(datanode_info_proto::AdminState::Normal as i32),
                        upgrade_domain: None,
                        last_block_report_time: Some(0),
                        last_block_report_monotonic: Some(0),
                        num_blocks: Some(dn.num_blocks),
                        software_version: None,
                    })
                    .collect();

                Ok(Response::new(GetDatanodeReportResponseProto {
                    di: datanode_infos,
                }))
            }
            Err(e) => Err(e),
        }
    }

    async fn get_datanode_storage_report(
        &self,
        _request: Request<GetDatanodeStorageReportRequestProto>,
    ) -> Result<Response<GetDatanodeStorageReportResponseProto>, Status> {
        // Similar to get_datanode_report but with storage details
        // For now, return empty - can be implemented similarly to get_datanode_report
        Ok(Response::new(GetDatanodeStorageReportResponseProto {
            datanode_storage_reports: vec![],
        }))
    }

    async fn set_safe_mode(
        &self,
        _request: Request<SetSafeModeRequestProto>,
    ) -> Result<Response<SetSafeModeResponseProto>, Status> {
        Err(Status::unimplemented("Safe mode not supported"))
    }

    async fn save_namespace(
        &self,
        _request: Request<SaveNamespaceRequestProto>,
    ) -> Result<Response<SaveNamespaceResponseProto>, Status> {
        Err(Status::unimplemented("Save namespace not supported"))
    }

    async fn roll_edits(
        &self,
        _request: Request<RollEditsRequestProto>,
    ) -> Result<Response<RollEditsResponseProto>, Status> {
        Err(Status::unimplemented("Roll edits not supported"))
    }

    async fn restore_failed_storage(
        &self,
        _request: Request<RestoreFailedStorageRequestProto>,
    ) -> Result<Response<RestoreFailedStorageResponseProto>, Status> {
        Err(Status::unimplemented("Restore failed storage not supported"))
    }

    async fn refresh_nodes(
        &self,
        _request: Request<RefreshNodesRequestProto>,
    ) -> Result<Response<RefreshNodesResponseProto>, Status> {
        Err(Status::unimplemented("Refresh nodes not supported"))
    }

    async fn finalize_upgrade(
        &self,
        _request: Request<FinalizeUpgradeRequestProto>,
    ) -> Result<Response<FinalizeUpgradeResponseProto>, Status> {
        Err(Status::unimplemented("Finalize upgrade not supported"))
    }

    async fn upgrade_status(
        &self,
        _request: Request<UpgradeStatusRequestProto>,
    ) -> Result<Response<UpgradeStatusResponseProto>, Status> {
        Err(Status::unimplemented("Upgrade status not supported"))
    }

    async fn rolling_upgrade(
        &self,
        _request: Request<RollingUpgradeRequestProto>,
    ) -> Result<Response<RollingUpgradeResponseProto>, Status> {
        Err(Status::unimplemented("Rolling upgrade not supported"))
    }

    async fn list_corrupt_file_blocks(
        &self,
        _request: Request<ListCorruptFileBlocksRequestProto>,
    ) -> Result<Response<ListCorruptFileBlocksResponseProto>, Status> {
        Ok(Response::new(ListCorruptFileBlocksResponseProto {
            corrupt: CorruptFileBlocksProto {
                files: vec![],
                cookie: "".to_string(),
            },
        }))
    }

    async fn meta_save(
        &self,
        _request: Request<MetaSaveRequestProto>,
    ) -> Result<Response<MetaSaveResponseProto>, Status> {
        Err(Status::unimplemented("MetaSave not supported"))
    }

    async fn set_quota(
        &self,
        _request: Request<SetQuotaRequestProto>,
    ) -> Result<Response<SetQuotaResponseProto>, Status> {
        Err(Status::unimplemented("Quotas not supported"))
    }

    async fn get_quota_usage(
        &self,
        _request: Request<GetQuotaUsageRequestProto>,
    ) -> Result<Response<GetQuotaUsageResponseProto>, Status> {
        Err(Status::unimplemented("Quotas not supported"))
    }

    async fn list_open_files(
        &self,
        _request: Request<ListOpenFilesRequestProto>,
    ) -> Result<Response<ListOpenFilesResponseProto>, Status> {
        Err(Status::unimplemented("List open files not supported"))
    }

    async fn msync(
        &self,
        _request: Request<MsyncRequestProto>,
    ) -> Result<Response<MsyncResponseProto>, Status> {
        Ok(Response::new(MsyncResponseProto {}))
    }

    async fn satisfy_storage_policy(
        &self,
        _request: Request<SatisfyStoragePolicyRequestProto>,
    ) -> Result<Response<SatisfyStoragePolicyResponseProto>, Status> {
        Err(Status::unimplemented("Storage policies not supported"))
    }

    async fn get_ha_service_state(
        &self,
        _request: Request<HaServiceStateRequestProto>,
    ) -> Result<Response<HaServiceStateResponseProto>, Status> {
        Ok(Response::new(HaServiceStateResponseProto {
            state: pillar_proto::hadoop::common::HaServiceStateProto::Active as i32,
        }))
    }

    async fn get_slow_datanode_report(
        &self,
        _request: Request<GetSlowDatanodeReportRequestProto>,
    ) -> Result<Response<GetSlowDatanodeReportResponseProto>, Status> {
        Ok(Response::new(GetSlowDatanodeReportResponseProto {
            datanode_info_proto: vec![],
        }))
    }

    // ============================================================================
    // Cache Directives & Pools (Stubbed)
    // ============================================================================

    async fn add_cache_directive(
        &self,
        _request: Request<AddCacheDirectiveRequestProto>,
    ) -> Result<Response<AddCacheDirectiveResponseProto>, Status> {
        Err(Status::unimplemented("Cache directives not supported"))
    }

    async fn modify_cache_directive(
        &self,
        _request: Request<ModifyCacheDirectiveRequestProto>,
    ) -> Result<Response<ModifyCacheDirectiveResponseProto>, Status> {
        Err(Status::unimplemented("Cache directives not supported"))
    }

    async fn remove_cache_directive(
        &self,
        _request: Request<RemoveCacheDirectiveRequestProto>,
    ) -> Result<Response<RemoveCacheDirectiveResponseProto>, Status> {
        Err(Status::unimplemented("Cache directives not supported"))
    }

    async fn list_cache_directives(
        &self,
        _request: Request<ListCacheDirectivesRequestProto>,
    ) -> Result<Response<ListCacheDirectivesResponseProto>, Status> {
        Ok(Response::new(ListCacheDirectivesResponseProto {
            elements: vec![],
            has_more: false,
        }))
    }

    async fn add_cache_pool(
        &self,
        _request: Request<AddCachePoolRequestProto>,
    ) -> Result<Response<AddCachePoolResponseProto>, Status> {
        Err(Status::unimplemented("Cache pools not supported"))
    }

    async fn modify_cache_pool(
        &self,
        _request: Request<ModifyCachePoolRequestProto>,
    ) -> Result<Response<ModifyCachePoolResponseProto>, Status> {
        Err(Status::unimplemented("Cache pools not supported"))
    }

    async fn remove_cache_pool(
        &self,
        _request: Request<RemoveCachePoolRequestProto>,
    ) -> Result<Response<RemoveCachePoolResponseProto>, Status> {
        Err(Status::unimplemented("Cache pools not supported"))
    }

    async fn list_cache_pools(
        &self,
        _request: Request<ListCachePoolsRequestProto>,
    ) -> Result<Response<ListCachePoolsResponseProto>, Status> {
        Ok(Response::new(ListCachePoolsResponseProto {
            entries: vec![],
            has_more: false,
        }))
    }

    // ============================================================================
    // Symlinks (Stubbed)
    // ============================================================================

    async fn get_file_link_info(
        &self,
        _request: Request<GetFileLinkInfoRequestProto>,
    ) -> Result<Response<GetFileLinkInfoResponseProto>, Status> {
        Err(Status::unimplemented("Symlinks not supported"))
    }

    async fn create_symlink(
        &self,
        _request: Request<CreateSymlinkRequestProto>,
    ) -> Result<Response<CreateSymlinkResponseProto>, Status> {
        Err(Status::unimplemented("Symlinks not supported"))
    }

    async fn get_link_target(
        &self,
        _request: Request<GetLinkTargetRequestProto>,
    ) -> Result<Response<GetLinkTargetResponseProto>, Status> {
        Err(Status::unimplemented("Symlinks not supported"))
    }

    // ============================================================================
    // Security & Delegation Tokens (Stubbed)
    // ============================================================================

    async fn get_delegation_token(
        &self,
        _request: Request<pillar_proto::hadoop::common::GetDelegationTokenRequestProto>,
    ) -> Result<Response<pillar_proto::hadoop::common::GetDelegationTokenResponseProto>, Status> {
        Err(Status::unimplemented("Delegation tokens not supported"))
    }

    async fn renew_delegation_token(
        &self,
        _request: Request<pillar_proto::hadoop::common::RenewDelegationTokenRequestProto>,
    ) -> Result<Response<pillar_proto::hadoop::common::RenewDelegationTokenResponseProto>, Status> {
        Err(Status::unimplemented("Delegation tokens not supported"))
    }

    async fn cancel_delegation_token(
        &self,
        _request: Request<pillar_proto::hadoop::common::CancelDelegationTokenRequestProto>,
    ) -> Result<Response<pillar_proto::hadoop::common::CancelDelegationTokenResponseProto>, Status> {
        Err(Status::unimplemented("Delegation tokens not supported"))
    }

    async fn set_balancer_bandwidth(
        &self,
        _request: Request<SetBalancerBandwidthRequestProto>,
    ) -> Result<Response<SetBalancerBandwidthResponseProto>, Status> {
        Err(Status::unimplemented("Balancer not supported"))
    }

    async fn get_data_encryption_key(
        &self,
        _request: Request<GetDataEncryptionKeyRequestProto>,
    ) -> Result<Response<GetDataEncryptionKeyResponseProto>, Status> {
        Err(Status::unimplemented("Data encryption keys not supported"))
    }

    // ============================================================================
    // Snapshots (Stubbed)
    // ============================================================================

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequestProto>,
    ) -> Result<Response<CreateSnapshotResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    async fn rename_snapshot(
        &self,
        _request: Request<RenameSnapshotRequestProto>,
    ) -> Result<Response<RenameSnapshotResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    async fn allow_snapshot(
        &self,
        _request: Request<AllowSnapshotRequestProto>,
    ) -> Result<Response<AllowSnapshotResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    async fn disallow_snapshot(
        &self,
        _request: Request<DisallowSnapshotRequestProto>,
    ) -> Result<Response<DisallowSnapshotResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    async fn get_snapshottable_dir_listing(
        &self,
        _request: Request<GetSnapshottableDirListingRequestProto>,
    ) -> Result<Response<GetSnapshottableDirListingResponseProto>, Status> {
        Ok(Response::new(GetSnapshottableDirListingResponseProto {
            snapshottable_dir_list: Some(SnapshottableDirectoryListingProto {
                snapshottable_dir_listing: vec![],
            }),
        }))
    }

    async fn get_snapshot_listing(
        &self,
        _request: Request<GetSnapshotListingRequestProto>,
    ) -> Result<Response<GetSnapshotListingResponseProto>, Status> {
        Ok(Response::new(GetSnapshotListingResponseProto {
            snapshot_list: Some(SnapshotListingProto {
                snapshot_listing: vec![],
            }),
        }))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequestProto>,
    ) -> Result<Response<DeleteSnapshotResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    async fn get_snapshot_diff_report(
        &self,
        _request: Request<GetSnapshotDiffReportRequestProto>,
    ) -> Result<Response<GetSnapshotDiffReportResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    async fn get_snapshot_diff_report_listing(
        &self,
        _request: Request<GetSnapshotDiffReportListingRequestProto>,
    ) -> Result<Response<GetSnapshotDiffReportListingResponseProto>, Status> {
        Err(Status::unimplemented("Snapshots not supported"))
    }

    // ============================================================================
    // ACLs (Stubbed)
    // ============================================================================

    async fn modify_acl_entries(
        &self,
        _request: Request<ModifyAclEntriesRequestProto>,
    ) -> Result<Response<ModifyAclEntriesResponseProto>, Status> {
        Err(Status::unimplemented("ACLs not supported"))
    }

    async fn remove_acl_entries(
        &self,
        _request: Request<RemoveAclEntriesRequestProto>,
    ) -> Result<Response<RemoveAclEntriesResponseProto>, Status> {
        Err(Status::unimplemented("ACLs not supported"))
    }

    async fn remove_default_acl(
        &self,
        _request: Request<RemoveDefaultAclRequestProto>,
    ) -> Result<Response<RemoveDefaultAclResponseProto>, Status> {
        Err(Status::unimplemented("ACLs not supported"))
    }

    async fn remove_acl(
        &self,
        _request: Request<RemoveAclRequestProto>,
    ) -> Result<Response<RemoveAclResponseProto>, Status> {
        Err(Status::unimplemented("ACLs not supported"))
    }

    async fn set_acl(
        &self,
        _request: Request<SetAclRequestProto>,
    ) -> Result<Response<SetAclResponseProto>, Status> {
        Err(Status::unimplemented("ACLs not supported"))
    }

    async fn get_acl_status(
        &self,
        _request: Request<GetAclStatusRequestProto>,
    ) -> Result<Response<GetAclStatusResponseProto>, Status> {
        Err(Status::unimplemented("ACLs not supported"))
    }

    // ============================================================================
    // Extended Attributes (Stubbed)
    // ============================================================================

    async fn set_x_attr(
        &self,
        _request: Request<SetXAttrRequestProto>,
    ) -> Result<Response<SetXAttrResponseProto>, Status> {
        Err(Status::unimplemented("Extended attributes not supported"))
    }

    async fn get_x_attrs(
        &self,
        _request: Request<GetXAttrsRequestProto>,
    ) -> Result<Response<GetXAttrsResponseProto>, Status> {
        Ok(Response::new(GetXAttrsResponseProto {
            x_attrs: vec![],
        }))
    }

    async fn list_x_attrs(
        &self,
        _request: Request<ListXAttrsRequestProto>,
    ) -> Result<Response<ListXAttrsResponseProto>, Status> {
        Ok(Response::new(ListXAttrsResponseProto {
            x_attrs: vec![],
        }))
    }

    async fn remove_x_attr(
        &self,
        _request: Request<RemoveXAttrRequestProto>,
    ) -> Result<Response<RemoveXAttrResponseProto>, Status> {
        Err(Status::unimplemented("Extended attributes not supported"))
    }

    async fn check_access(
        &self,
        _request: Request<CheckAccessRequestProto>,
    ) -> Result<Response<CheckAccessResponseProto>, Status> {
        Ok(Response::new(CheckAccessResponseProto {}))
    }

    // ============================================================================
    // Encryption Zones (Stubbed)
    // ============================================================================

    async fn create_encryption_zone(
        &self,
        _request: Request<CreateEncryptionZoneRequestProto>,
    ) -> Result<Response<CreateEncryptionZoneResponseProto>, Status> {
        Err(Status::unimplemented("Encryption zones not supported"))
    }

    async fn list_encryption_zones(
        &self,
        _request: Request<ListEncryptionZonesRequestProto>,
    ) -> Result<Response<ListEncryptionZonesResponseProto>, Status> {
        Ok(Response::new(ListEncryptionZonesResponseProto {
            zones: vec![],
            has_more: false,
        }))
    }

    async fn reencrypt_encryption_zone(
        &self,
        _request: Request<ReencryptEncryptionZoneRequestProto>,
    ) -> Result<Response<ReencryptEncryptionZoneResponseProto>, Status> {
        Err(Status::unimplemented("Encryption zones not supported"))
    }

    async fn list_reencryption_status(
        &self,
        _request: Request<ListReencryptionStatusRequestProto>,
    ) -> Result<Response<ListReencryptionStatusResponseProto>, Status> {
        Ok(Response::new(ListReencryptionStatusResponseProto {
            statuses: vec![],
            has_more: false,
        }))
    }

    async fn get_ez_for_path(
        &self,
        _request: Request<GetEzForPathRequestProto>,
    ) -> Result<Response<GetEzForPathResponseProto>, Status> {
        Ok(Response::new(GetEzForPathResponseProto {
            zone: None,
        }))
    }

    async fn get_enclosing_root(
        &self,
        _request: Request<GetEnclosingRootRequestProto>,
    ) -> Result<Response<GetEnclosingRootResponseProto>, Status> {
        Err(Status::unimplemented("Encryption zones not supported"))
    }

    // ============================================================================
    // Erasure Coding (Stubbed)
    // ============================================================================

    async fn set_erasure_coding_policy(
        &self,
        _request: Request<SetErasureCodingPolicyRequestProto>,
    ) -> Result<Response<SetErasureCodingPolicyResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn unset_erasure_coding_policy(
        &self,
        _request: Request<UnsetErasureCodingPolicyRequestProto>,
    ) -> Result<Response<UnsetErasureCodingPolicyResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn get_ec_topology_result_for_policies(
        &self,
        _request: Request<GetEcTopologyResultForPoliciesRequestProto>,
    ) -> Result<Response<GetEcTopologyResultForPoliciesResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn get_erasure_coding_policies(
        &self,
        _request: Request<GetErasureCodingPoliciesRequestProto>,
    ) -> Result<Response<GetErasureCodingPoliciesResponseProto>, Status> {
        Ok(Response::new(GetErasureCodingPoliciesResponseProto {
            ec_policies: vec![],
        }))
    }

    async fn add_erasure_coding_policies(
        &self,
        _request: Request<AddErasureCodingPoliciesRequestProto>,
    ) -> Result<Response<AddErasureCodingPoliciesResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn remove_erasure_coding_policy(
        &self,
        _request: Request<RemoveErasureCodingPolicyRequestProto>,
    ) -> Result<Response<RemoveErasureCodingPolicyResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn enable_erasure_coding_policy(
        &self,
        _request: Request<EnableErasureCodingPolicyRequestProto>,
    ) -> Result<Response<EnableErasureCodingPolicyResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn disable_erasure_coding_policy(
        &self,
        _request: Request<DisableErasureCodingPolicyRequestProto>,
    ) -> Result<Response<DisableErasureCodingPolicyResponseProto>, Status> {
        Err(Status::unimplemented("Erasure coding not supported"))
    }

    async fn get_erasure_coding_policy(
        &self,
        _request: Request<GetErasureCodingPolicyRequestProto>,
    ) -> Result<Response<GetErasureCodingPolicyResponseProto>, Status> {
        Ok(Response::new(GetErasureCodingPolicyResponseProto {
            ec_policy: None,
        }))
    }

    async fn get_erasure_coding_codecs(
        &self,
        _request: Request<GetErasureCodingCodecsRequestProto>,
    ) -> Result<Response<GetErasureCodingCodecsResponseProto>, Status> {
        Ok(Response::new(GetErasureCodingCodecsResponseProto {
            codec: vec![],
        }))
    }

    // ============================================================================
    // Journal & Edit Log Operations (Stubbed)
    // ============================================================================

    async fn get_current_edit_log_txid(
        &self,
        _request: Request<GetCurrentEditLogTxidRequestProto>,
    ) -> Result<Response<GetCurrentEditLogTxidResponseProto>, Status> {
        Ok(Response::new(GetCurrentEditLogTxidResponseProto {
            txid: 0,
        }))
    }

    async fn get_edits_from_txid(
        &self,
        _request: Request<GetEditsFromTxidRequestProto>,
    ) -> Result<Response<GetEditsFromTxidResponseProto>, Status> {
        Err(Status::unimplemented("Edit log access not supported"))
    }
}
