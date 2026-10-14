//! Method table: Hadoop `ClientProtocol` method names to the tonic
//! `ClientNamenodeProtocol` trait. Generated from the `rpc` declarations in
//! `ClientNamenodeProtocol.proto` against tonic's generated trait, in
//! declaration order; regenerate if the proto changes.

use prost::Message;
use pillar_proto::hadoop::hdfs::client_namenode_protocol_server::ClientNamenodeProtocol;
use pillar_proto::hadoop::hdfs::*;

use super::RpcError;

macro_rules! methods {
    ($($name:literal => $method:ident($req:ty),)*) => {
        /// Decode `body` as the request for `method`, call it, and return
        /// the encoded response (not length-delimited).
        pub async fn dispatch<T: ClientNamenodeProtocol>(svc: &T, method: &str, body: &[u8]) -> Result<Vec<u8>, RpcError> {
            match method {
                $($name => {
                    let req = <$req>::decode(body).map_err(|e| RpcError::bad_request(method, e))?;
                    let resp = svc.$method(tonic::Request::new(req)).await.map_err(RpcError::from_status)?;
                    Ok(resp.into_inner().encode_to_vec())
                })*
                _ => Err(RpcError::no_such_method(method)),
            }
        }

        /// Every method name the table knows.
        #[cfg(test)]
        pub const METHODS: &[&str] = &[$($name),*];
    };
}

methods! {
    "getBlockLocations" => get_block_locations(GetBlockLocationsRequestProto),
    "getServerDefaults" => get_server_defaults(GetServerDefaultsRequestProto),
    "create" => create(CreateRequestProto),
    "append" => append(AppendRequestProto),
    "setReplication" => set_replication(SetReplicationRequestProto),
    "setStoragePolicy" => set_storage_policy(SetStoragePolicyRequestProto),
    "unsetStoragePolicy" => unset_storage_policy(UnsetStoragePolicyRequestProto),
    "getStoragePolicy" => get_storage_policy(GetStoragePolicyRequestProto),
    "getStoragePolicies" => get_storage_policies(GetStoragePoliciesRequestProto),
    "setPermission" => set_permission(SetPermissionRequestProto),
    "setOwner" => set_owner(SetOwnerRequestProto),
    "abandonBlock" => abandon_block(AbandonBlockRequestProto),
    "addBlock" => add_block(AddBlockRequestProto),
    "getAdditionalDatanode" => get_additional_datanode(GetAdditionalDatanodeRequestProto),
    "complete" => complete(CompleteRequestProto),
    "reportBadBlocks" => report_bad_blocks(ReportBadBlocksRequestProto),
    "concat" => concat(ConcatRequestProto),
    "truncate" => truncate(TruncateRequestProto),
    "rename" => rename(RenameRequestProto),
    "rename2" => rename2(Rename2RequestProto),
    "delete" => delete(DeleteRequestProto),
    "mkdirs" => mkdirs(MkdirsRequestProto),
    "getListing" => get_listing(GetListingRequestProto),
    "getBatchedListing" => get_batched_listing(GetBatchedListingRequestProto),
    "renewLease" => renew_lease(RenewLeaseRequestProto),
    "recoverLease" => recover_lease(RecoverLeaseRequestProto),
    "getFsStats" => get_fs_stats(GetFsStatusRequestProto),
    "getFsReplicatedBlockStats" => get_fs_replicated_block_stats(GetFsReplicatedBlockStatsRequestProto),
    "getFsECBlockGroupStats" => get_fs_ec_block_group_stats(GetFsEcBlockGroupStatsRequestProto),
    "getDatanodeReport" => get_datanode_report(GetDatanodeReportRequestProto),
    "getDatanodeStorageReport" => get_datanode_storage_report(GetDatanodeStorageReportRequestProto),
    "getPreferredBlockSize" => get_preferred_block_size(GetPreferredBlockSizeRequestProto),
    "setSafeMode" => set_safe_mode(SetSafeModeRequestProto),
    "saveNamespace" => save_namespace(SaveNamespaceRequestProto),
    "rollEdits" => roll_edits(RollEditsRequestProto),
    "restoreFailedStorage" => restore_failed_storage(RestoreFailedStorageRequestProto),
    "refreshNodes" => refresh_nodes(RefreshNodesRequestProto),
    "finalizeUpgrade" => finalize_upgrade(FinalizeUpgradeRequestProto),
    "upgradeStatus" => upgrade_status(UpgradeStatusRequestProto),
    "rollingUpgrade" => rolling_upgrade(RollingUpgradeRequestProto),
    "listCorruptFileBlocks" => list_corrupt_file_blocks(ListCorruptFileBlocksRequestProto),
    "metaSave" => meta_save(MetaSaveRequestProto),
    "getFileInfo" => get_file_info(GetFileInfoRequestProto),
    "getLocatedFileInfo" => get_located_file_info(GetLocatedFileInfoRequestProto),
    "addCacheDirective" => add_cache_directive(AddCacheDirectiveRequestProto),
    "modifyCacheDirective" => modify_cache_directive(ModifyCacheDirectiveRequestProto),
    "removeCacheDirective" => remove_cache_directive(RemoveCacheDirectiveRequestProto),
    "listCacheDirectives" => list_cache_directives(ListCacheDirectivesRequestProto),
    "addCachePool" => add_cache_pool(AddCachePoolRequestProto),
    "modifyCachePool" => modify_cache_pool(ModifyCachePoolRequestProto),
    "removeCachePool" => remove_cache_pool(RemoveCachePoolRequestProto),
    "listCachePools" => list_cache_pools(ListCachePoolsRequestProto),
    "getFileLinkInfo" => get_file_link_info(GetFileLinkInfoRequestProto),
    "getContentSummary" => get_content_summary(GetContentSummaryRequestProto),
    "setQuota" => set_quota(SetQuotaRequestProto),
    "fsync" => fsync(FsyncRequestProto),
    "setTimes" => set_times(SetTimesRequestProto),
    "createSymlink" => create_symlink(CreateSymlinkRequestProto),
    "getLinkTarget" => get_link_target(GetLinkTargetRequestProto),
    "updateBlockForPipeline" => update_block_for_pipeline(UpdateBlockForPipelineRequestProto),
    "updatePipeline" => update_pipeline(UpdatePipelineRequestProto),
    "setBalancerBandwidth" => set_balancer_bandwidth(SetBalancerBandwidthRequestProto),
    "getDataEncryptionKey" => get_data_encryption_key(GetDataEncryptionKeyRequestProto),
    "createSnapshot" => create_snapshot(CreateSnapshotRequestProto),
    "renameSnapshot" => rename_snapshot(RenameSnapshotRequestProto),
    "allowSnapshot" => allow_snapshot(AllowSnapshotRequestProto),
    "disallowSnapshot" => disallow_snapshot(DisallowSnapshotRequestProto),
    "getSnapshottableDirListing" => get_snapshottable_dir_listing(GetSnapshottableDirListingRequestProto),
    "getSnapshotListing" => get_snapshot_listing(GetSnapshotListingRequestProto),
    "deleteSnapshot" => delete_snapshot(DeleteSnapshotRequestProto),
    "getSnapshotDiffReport" => get_snapshot_diff_report(GetSnapshotDiffReportRequestProto),
    "getSnapshotDiffReportListing" => get_snapshot_diff_report_listing(GetSnapshotDiffReportListingRequestProto),
    "isFileClosed" => is_file_closed(IsFileClosedRequestProto),
    "modifyAclEntries" => modify_acl_entries(ModifyAclEntriesRequestProto),
    "removeAclEntries" => remove_acl_entries(RemoveAclEntriesRequestProto),
    "removeDefaultAcl" => remove_default_acl(RemoveDefaultAclRequestProto),
    "removeAcl" => remove_acl(RemoveAclRequestProto),
    "setAcl" => set_acl(SetAclRequestProto),
    "getAclStatus" => get_acl_status(GetAclStatusRequestProto),
    "setXAttr" => set_x_attr(SetXAttrRequestProto),
    "getXAttrs" => get_x_attrs(GetXAttrsRequestProto),
    "listXAttrs" => list_x_attrs(ListXAttrsRequestProto),
    "removeXAttr" => remove_x_attr(RemoveXAttrRequestProto),
    "checkAccess" => check_access(CheckAccessRequestProto),
    "createEncryptionZone" => create_encryption_zone(CreateEncryptionZoneRequestProto),
    "listEncryptionZones" => list_encryption_zones(ListEncryptionZonesRequestProto),
    "reencryptEncryptionZone" => reencrypt_encryption_zone(ReencryptEncryptionZoneRequestProto),
    "listReencryptionStatus" => list_reencryption_status(ListReencryptionStatusRequestProto),
    "getEZForPath" => get_ez_for_path(GetEzForPathRequestProto),
    "setErasureCodingPolicy" => set_erasure_coding_policy(SetErasureCodingPolicyRequestProto),
    "unsetErasureCodingPolicy" => unset_erasure_coding_policy(UnsetErasureCodingPolicyRequestProto),
    "getECTopologyResultForPolicies" => get_ec_topology_result_for_policies(GetEcTopologyResultForPoliciesRequestProto),
    "getCurrentEditLogTxid" => get_current_edit_log_txid(GetCurrentEditLogTxidRequestProto),
    "getEditsFromTxid" => get_edits_from_txid(GetEditsFromTxidRequestProto),
    "getErasureCodingPolicies" => get_erasure_coding_policies(GetErasureCodingPoliciesRequestProto),
    "addErasureCodingPolicies" => add_erasure_coding_policies(AddErasureCodingPoliciesRequestProto),
    "removeErasureCodingPolicy" => remove_erasure_coding_policy(RemoveErasureCodingPolicyRequestProto),
    "enableErasureCodingPolicy" => enable_erasure_coding_policy(EnableErasureCodingPolicyRequestProto),
    "disableErasureCodingPolicy" => disable_erasure_coding_policy(DisableErasureCodingPolicyRequestProto),
    "getErasureCodingPolicy" => get_erasure_coding_policy(GetErasureCodingPolicyRequestProto),
    "getErasureCodingCodecs" => get_erasure_coding_codecs(GetErasureCodingCodecsRequestProto),
    "getQuotaUsage" => get_quota_usage(GetQuotaUsageRequestProto),
    "listOpenFiles" => list_open_files(ListOpenFilesRequestProto),
    "msync" => msync(MsyncRequestProto),
    "satisfyStoragePolicy" => satisfy_storage_policy(SatisfyStoragePolicyRequestProto),
    "getHAServiceState" => get_ha_service_state(HaServiceStateRequestProto),
    "getSlowDatanodeReport" => get_slow_datanode_report(GetSlowDatanodeReportRequestProto),
    "getEnclosingRoot" => get_enclosing_root(GetEnclosingRootRequestProto),
}
