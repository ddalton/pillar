use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn, error};

use pillar_proto::hadoop::hdfs::*;
use crate::grpc::ClientNamenodeService;

/// The user a request runs as: the Hadoop IPC connection's effective user
/// (SIMPLE auth), or `hdfs` for gRPC callers, which carry none.
fn caller<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<crate::ipc::CallerUser>()
        .map(|u| u.0.clone())
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| "hdfs".to_string())
}

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

        let file_info = self.service.handle_get_file_info(&path).await?;
        Ok(Response::new(GetFileInfoResponseProto { fs: Some(file_status_proto(file_info, path)) }))
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
        let owner = caller(&request);
        let req = request.into_inner();
        let path = req.src;
        let masked = req.masked;

        info!("mkdirs: {}", path);

        match self.service
            .handle_mkdirs(
                &path,
                owner,
                "supergroup".to_string(),
                masked.perm as u16,
                req.create_parent,
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
        let owner = caller(&request);
        let req = request.into_inner();
        let path = req.src;
        let masked = req.masked;
        let replication = req.replication as u16;
        let block_size = req.block_size;

        info!("create: {} (replication={}, blocksize={})", path, replication, block_size);

        // CreateFlagProto: CREATE = 0x01, OVERWRITE = 0x02, APPEND = 0x04.
        let overwrite = req.create_flag & 0x02 != 0;
        match self.service
            .handle_create_with(
                &path,
                owner.clone(),
                "supergroup".to_string(),
                masked.perm as u16,
                replication,
                block_size,
                req.create_parent,
                overwrite,
            )
            .await
        {
            Ok(_) => Ok(Response::new(CreateResponseProto {
                fs: Some(HdfsFileStatusProto {
                    file_type: hdfs_file_status_proto::FileType::IsFile as i32,
                    path: path.into(),
                    length: 0,
                    permission: masked,
                    owner,
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

        let located = self.service
            .handle_get_located_blocks(&path, offset, length)
            .await?;

        let located_blocks = LocatedBlocksProto {
            file_length: located.file_length,
            blocks: located.blocks.into_iter().map(|b| located_block_to_proto(b, self.service.block_pool_id())).collect(),
            under_construction: located.under_construction,
            last_block: located.last_block.map(|b| located_block_to_proto(b, self.service.block_pool_id())),
            is_last_block_complete: located.is_last_block_complete,
            file_encryption_info: None,
            ec_policy: None,
        };

        Ok(Response::new(GetBlockLocationsResponseProto {
            locations: Some(located_blocks),
        }))
    }

    /// Add a new block when writing a file
    async fn add_block(
        &self,
        request: Request<AddBlockRequestProto>,
    ) -> Result<Response<AddBlockResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;

        info!("add_block: {}", path);

        if let Some(prev) = &req.previous {
            self.service.handle_commit_block(&path, prev.block_id, prev.num_bytes.unwrap_or(0)).await?;
        }

        match self.service.handle_add_block(&path).await {
            Ok(block) => {
                let response = AddBlockResponseProto {
                    block: fill_storages(LocatedBlockProto {
                        b: ExtendedBlockProto {
                            pool_id: self.service.block_pool_id().to_string(),
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
                                    supported_transports: loc.supported_transports,
                                    rdma_port: Some(loc.rdma_port as u32),
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

        if let Some(last) = &req.last {
            self.service.handle_commit_block(&path, last.block_id, last.num_bytes.unwrap_or(0)).await?;
        }

        match self.service.handle_complete(&path).await {
            Ok(result) => Ok(Response::new(CompleteResponseProto { result })),
            Err(e) => Err(e),
        }
    }

    /// Set replication factor for a file
    async fn set_replication(
        &self,
        request: Request<SetReplicationRequestProto>,
    ) -> Result<Response<SetReplicationResponseProto>, Status> {
        let req = request.into_inner();
        let result = self.service.handle_set_replication(&req.src, req.replication).await?;
        Ok(Response::new(SetReplicationResponseProto { result }))
    }

    /// Set permissions
    async fn set_permission(
        &self,
        request: Request<SetPermissionRequestProto>,
    ) -> Result<Response<SetPermissionResponseProto>, Status> {
        let req = request.into_inner();
        let attrs = crate::metadata::edit_log::InodeAttributes {
            permission: Some(req.permission.perm as u16),
            ..Default::default()
        };
        self.service.handle_set_attributes(&req.src, attrs).await?;
        Ok(Response::new(SetPermissionResponseProto {}))
    }

    /// Set owner
    async fn set_owner(
        &self,
        request: Request<SetOwnerRequestProto>,
    ) -> Result<Response<SetOwnerResponseProto>, Status> {
        let req = request.into_inner();
        if req.username.is_none() && req.groupname.is_none() {
            return Err(Status::invalid_argument("setOwner: neither user nor group given"));
        }
        let attrs = crate::metadata::edit_log::InodeAttributes {
            owner: req.username,
            group: req.groupname,
            ..Default::default()
        };
        self.service.handle_set_attributes(&req.src, attrs).await?;
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
                block_size: self.service.default_block_size(),
                bytes_per_checksum: 512,
                write_packet_size: 64 * 1024,
                replication: self.service.default_replication() as u32,
                file_buffer_size: 4096,
                encrypt_data_transfer: Some(false),
                trash_interval: Some(0),
                // HDFS's default (dfs.checksum.type); DataNodes accept CRC32 too.
                checksum_type: Some(ChecksumTypeProto::ChecksumCrc32c as i32),
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
        request: Request<GetPreferredBlockSizeRequestProto>,
    ) -> Result<Response<GetPreferredBlockSizeResponseProto>, Status> {
        let bsize = self.service.handle_preferred_block_size(&request.into_inner().filename).await?;
        Ok(Response::new(GetPreferredBlockSizeResponseProto { bsize }))
    }

    async fn fsync(
        &self,
        _request: Request<FsyncRequestProto>,
    ) -> Result<Response<FsyncResponseProto>, Status> {
        Ok(Response::new(FsyncResponseProto {}))
    }

    async fn set_times(
        &self,
        request: Request<SetTimesRequestProto>,
    ) -> Result<Response<SetTimesResponseProto>, Status> {
        let req = request.into_inner();
        // -1 (all bits set on the wire) leaves a time unchanged.
        let time = |t: u64| (t != u64::MAX).then_some(t);
        let attrs = crate::metadata::edit_log::InodeAttributes {
            mtime: time(req.mtime),
            atime: time(req.atime),
            ..Default::default()
        };
        self.service.handle_set_attributes(&req.src, attrs).await?;
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
        request: Request<GetContentSummaryRequestProto>,
    ) -> Result<Response<GetContentSummaryResponseProto>, Status> {
        let s = self.service.handle_get_content_summary(&request.into_inner().path).await?;
        Ok(Response::new(GetContentSummaryResponseProto {
            summary: ContentSummaryProto {
                length: s.length,
                file_count: s.file_count,
                directory_count: s.directory_count,
                // -1 (no quota) as the uint64 the proto carries.
                quota: u64::MAX,
                space_consumed: s.space_consumed,
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
        // CreateFlagProto NEW_BLOCK = 0x20.
        let new_block = req.flag.unwrap_or(0) & 0x20 != 0;
        info!("append: {} (new_block={})", req.src, new_block);
        // Always a new block: no last block to extend, so the client's next
        // call is addBlock.
        self.service.handle_append(&req.src, new_block).await?;
        let stat = file_status_proto(self.service.handle_get_file_info(&req.src).await?, req.src);
        Ok(Response::new(AppendResponseProto { block: None, stat: Some(stat) }))
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
        request: Request<GetAdditionalDatanodeRequestProto>,
    ) -> Result<Response<GetAdditionalDatanodeResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let block = req.blk;

        info!(
            "get_additional_datanode: {} block_id={} num_additional={}",
            path, block.block_id, req.num_additional_nodes
        );

        // Convert existing and excluded datanodes from proto
        use crate::metadata::block_manager::DatanodeId;
        let existing_datanodes: Vec<DatanodeId> = req.existings
            .iter()
            .map(|dn| DatanodeId::from_proto(&dn.id))
            .collect();

        let excluded_datanodes: Vec<DatanodeId> = req.excludes
            .iter()
            .map(|dn| DatanodeId::from_proto(&dn.id))
            .collect();

        // Get additional datanode(s)
        let located_block = self.service.handle_get_additional_datanode(
            block.block_id,
            block.generation_stamp,
            &existing_datanodes,
            &excluded_datanodes,
            req.num_additional_nodes,
        ).await?;

        // Convert to proto
        let locs = located_block.locations
            .into_iter()
            .map(|dn| DatanodeInfoProto {
                id: DatanodeIdProto {
                    datanode_uuid: dn.uuid,
                    ip_addr: dn.hostname.clone(),
                    host_name: dn.hostname,
                    xfer_port: dn.data_port as u32,
                    info_port: dn.http_port as u32,
                    ipc_port: dn.rpc_port as u32,
                    info_secure_port: Some(0),
                    supported_transports: dn.supported_transports,
                    rdma_port: Some(dn.rdma_port as u32),
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
            .collect();

        let response = GetAdditionalDatanodeResponseProto {
            block: fill_storages(LocatedBlockProto {
                b: ExtendedBlockProto {
                    pool_id: self.service.block_pool_id().to_string(),
                    block_id: located_block.block_id,
                    generation_stamp: located_block.generation_stamp,
                    num_bytes: Some(located_block.num_bytes),
                },
                offset: located_block.offset,
                locs,
                corrupt: false,
                block_token: Default::default(),
                is_cached: vec![],
                storage_types: vec![],
                storage_i_ds: vec![],
                block_indices: None,
                block_tokens: vec![],
            }),
        };

        Ok(Response::new(response))
    }

    async fn update_block_for_pipeline(
        &self,
        request: Request<UpdateBlockForPipelineRequestProto>,
    ) -> Result<Response<UpdateBlockForPipelineResponseProto>, Status> {
        let req = request.into_inner();
        let block = req.block;

        info!(
            "update_block_for_pipeline: block_id={} client={}",
            block.block_id, req.client_name
        );

        // Update block with new generation stamp
        let located_block = self.service.handle_update_block_for_pipeline(
            block.block_id,
            block.generation_stamp,
        ).await?;

        // Convert to proto
        let locs = located_block.locations
            .into_iter()
            .map(|dn| DatanodeInfoProto {
                id: DatanodeIdProto {
                    datanode_uuid: dn.uuid,
                    ip_addr: dn.hostname.clone(),
                    host_name: dn.hostname,
                    xfer_port: dn.data_port as u32,
                    info_port: dn.http_port as u32,
                    ipc_port: dn.rpc_port as u32,
                    info_secure_port: Some(0),
                    supported_transports: dn.supported_transports,
                    rdma_port: Some(dn.rdma_port as u32),
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
            .collect();

        let response = UpdateBlockForPipelineResponseProto {
            block: fill_storages(LocatedBlockProto {
                b: ExtendedBlockProto {
                    pool_id: self.service.block_pool_id().to_string(),
                    block_id: located_block.block_id,
                    generation_stamp: located_block.generation_stamp,
                    num_bytes: Some(located_block.num_bytes),
                },
                offset: located_block.offset,
                locs,
                corrupt: false,
                block_token: Default::default(),
                is_cached: vec![],
                storage_types: vec![],
                storage_i_ds: vec![],
                block_indices: None,
                block_tokens: vec![],
            }),
        };

        Ok(Response::new(response))
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
        request: Request<ReportBadBlocksRequestProto>,
    ) -> Result<Response<ReportBadBlocksResponseProto>, Status> {
        let replicas: Vec<_> = request
            .into_inner()
            .blocks
            .into_iter()
            .flat_map(|lb| {
                let block = crate::metadata::block_manager::BlockId::new(lb.b.block_id, lb.b.generation_stamp);
                lb.locs.into_iter().map(move |dn| (block, dn.id.datanode_uuid))
            })
            .collect();
        self.service.handle_report_bad_replicas(&replicas);
        Ok(Response::new(ReportBadBlocksResponseProto {}))
    }

    async fn concat(
        &self,
        request: Request<ConcatRequestProto>,
    ) -> Result<Response<ConcatResponseProto>, Status> {
        let req = request.into_inner();

        info!(
            "concat: target={} sources={:?}",
            req.trg, req.srcs
        );

        self.service.handle_concat(&req.trg, &req.srcs).await?;

        Ok(Response::new(ConcatResponseProto {}))
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequestProto>,
    ) -> Result<Response<TruncateResponseProto>, Status> {
        let req = request.into_inner();

        info!(
            "truncate: src={} new_length={} client={}",
            req.src, req.new_length, req.client_name
        );

        let result = self.service.handle_truncate(
            &req.src,
            req.new_length,
        ).await?;

        Ok(Response::new(TruncateResponseProto { result }))
    }

    async fn rename2(
        &self,
        request: Request<Rename2RequestProto>,
    ) -> Result<Response<Rename2ResponseProto>, Status> {
        let req = request.into_inner();
        info!("rename2: {} -> {} (overwrite={})", req.src, req.dst, req.overwrite_dest);
        self.service.handle_rename2(&req.src, &req.dst, req.overwrite_dest).await?;
        Ok(Response::new(Rename2ResponseProto {}))
    }

    async fn get_batched_listing(
        &self,
        _request: Request<GetBatchedListingRequestProto>,
    ) -> Result<Response<GetBatchedListingResponseProto>, Status> {
        Err(Status::unimplemented("Batched listing not supported"))
    }

    async fn recover_lease(
        &self,
        request: Request<RecoverLeaseRequestProto>,
    ) -> Result<Response<RecoverLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let path = req.src;
        let client_name = req.client_name;

        info!("recover_lease: {} client={}", path, client_name);

        let result = self.service.handle_recover_lease(&path, &client_name).await?;

        Ok(Response::new(RecoverLeaseResponseProto { result }))
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
        let di = self.service.handle_get_datanode_report().await?.into_iter().map(datanode_report_proto).collect();
        Ok(Response::new(GetDatanodeReportResponseProto { di }))
    }

    async fn get_datanode_storage_report(
        &self,
        _request: Request<GetDatanodeStorageReportRequestProto>,
    ) -> Result<Response<GetDatanodeStorageReportResponseProto>, Status> {
        // One storage per DataNode (its volume), keyed by the DataNode uuid.
        let reports = self
            .service
            .handle_get_datanode_report()
            .await?
            .into_iter()
            .map(|dn| {
                let storage = StorageReportProto {
                    #[allow(deprecated)]
                    storage_uuid: dn.uuid.clone(),
                    failed: Some(false),
                    capacity: Some(dn.capacity),
                    dfs_used: Some(dn.block_pool_used),
                    remaining: Some(dn.remaining),
                    block_pool_used: Some(dn.block_pool_used),
                    storage: Some(DatanodeStorageProto {
                        storage_uuid: dn.uuid.clone(),
                        state: Some(0),        // NORMAL
                        storage_type: Some(1), // DISK
                    }),
                    non_dfs_used: Some(dn.used.saturating_sub(dn.block_pool_used)),
                    mount: None,
                };
                DatanodeStorageReportProto { datanode_info: datanode_report_proto(dn), storage_reports: vec![storage] }
            })
            .collect();
        Ok(Response::new(GetDatanodeStorageReportResponseProto { datanode_storage_reports: reports }))
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
        request: Request<CheckAccessRequestProto>,
    ) -> Result<Response<CheckAccessResponseProto>, Status> {
        self.service.handle_check_access(&request.into_inner().path).await?;
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
        Ok(Response::new(GetCurrentEditLogTxidResponseProto { txid: self.service.last_txid() as i64 }))
    }

    async fn get_edits_from_txid(
        &self,
        _request: Request<GetEditsFromTxidRequestProto>,
    ) -> Result<Response<GetEditsFromTxidResponseProto>, Status> {
        Err(Status::unimplemented("Edit log access not supported"))
    }
}

/// A file or directory's status as getFileInfo (and append) return it.
fn file_status_proto(info: crate::grpc::client_namenode_service::FileInfo, path: String) -> HdfsFileStatusProto {
    HdfsFileStatusProto {
        file_type: if info.is_dir {
            hdfs_file_status_proto::FileType::IsDir as i32
        } else {
            hdfs_file_status_proto::FileType::IsFile as i32
        },
        path: path.into(),
        length: info.length,
        permission: FsPermissionProto { perm: info.permissions as u32 },
        owner: info.owner,
        group: info.group,
        modification_time: info.modification_time,
        access_time: info.access_time,
        symlink: None,
        block_replication: Some(info.replication as u32),
        blocksize: Some(info.block_size),
        storage_policy: Some(0),
        ec_policy: None,
        flags: Some(0),
        file_id: Some(0),
        children_num: Some(0),
        file_encryption_info: None,
        locations: None,
        namespace: None,
    }
}

/// A live DataNode as getDatanodeReport describes it.
fn datanode_report_proto(dn: crate::grpc::client_namenode_service::DatanodeReportInfo) -> DatanodeInfoProto {
    DatanodeInfoProto {
        id: DatanodeIdProto {
            ip_addr: dn.hostname.clone(),
            host_name: dn.hostname,
            datanode_uuid: dn.uuid,
            xfer_port: dn.data_port as u32,
            info_port: dn.http_port as u32,
            ipc_port: dn.rpc_port as u32,
            info_secure_port: Some(0),
            supported_transports: dn.supported_transports,
            rdma_port: Some(dn.rdma_port as u32),
        },
        capacity: Some(dn.capacity),
        dfs_used: Some(dn.block_pool_used),
        remaining: Some(dn.remaining),
        block_pool_used: Some(dn.block_pool_used),
        cache_capacity: Some(0),
        cache_used: Some(0),
        last_update: Some(dn.last_update),
        last_update_monotonic: Some(0),
        xceiver_count: Some(0),
        location: None,
        non_dfs_used: Some(dn.used.saturating_sub(dn.block_pool_used)),
        admin_state: Some(datanode_info_proto::AdminState::Normal as i32),
        upgrade_domain: None,
        last_block_report_time: Some(0),
        last_block_report_monotonic: Some(0),
        num_blocks: Some(dn.num_blocks),
        software_version: None,
    }
}

/// Convert a service-level LocatedBlock into the wire LocatedBlockProto.
fn located_block_to_proto(b: crate::grpc::client_namenode_service::LocatedBlock, pool_id: &str) -> LocatedBlockProto {
    fill_storages(LocatedBlockProto {
        b: ExtendedBlockProto {
            pool_id: pool_id.to_string(),
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
                    supported_transports: loc.supported_transports,
                    rdma_port: Some(loc.rdma_port as u32),
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
}

/// Hadoop clients expect one storage id, storage type and cached flag per
/// location (they index them alongside `locs` to build the write pipeline).
/// Pillar DataNodes report a single DISK storage whose id is their uuid.
fn fill_storages(mut b: LocatedBlockProto) -> LocatedBlockProto {
    let n = b.locs.len();
    if b.storage_i_ds.len() != n {
        b.storage_i_ds = b.locs.iter().map(|l| l.id.datanode_uuid.clone()).collect();
    }
    if b.storage_types.len() != n {
        b.storage_types = vec![pillar_proto::hadoop::hdfs::StorageTypeProto::Disk as i32; n];
    }
    if b.is_cached.len() != n {
        b.is_cached = vec![false; n];
    }
    b
}
