// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/lake/replication_txn_manager.h"

#include <fmt/format.h>
#include <sys/stat.h>

#include <filesystem>
#include <set>

#include "agent/agent_server.h"
#include "agent/master_info.h"
#include "agent/task_signatures_manager.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "fs/fs_starlet.h"
#include "fs/key_cache.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/Types_constants.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/delete_handler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/protobuf_file.h"
#include "storage/replication_utils.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment_stream_converter.h"
#include "storage/snapshot_manager.h"
#include "storage/tablet_updates.h"
#include "util/defer_op.h"
#include "util/string_parser.hpp"
#include "util/thrift_rpc_helper.h"

namespace starrocks::lake {

Status ReplicationTxnManager::remote_snapshot(const TRemoteSnapshotRequest& request, TSnapshotInfo* src_snapshot_info) {
    if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
        return Status::InternalError("Process is going to quit. The remote snapshot will stop");
    }

    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(request.tablet_id));

    auto status_or_txn_log = tablet.get_txn_slog(request.transaction_id);
    if (status_or_txn_log.ok()) {
        const ReplicationTxnMetaPB& txn_meta = status_or_txn_log.value()->op_replication().txn_meta();
        if (txn_meta.txn_state() == ReplicationTxnStatePB::TXN_SNAPSHOTED &&
            txn_meta.snapshot_version() == request.src_visible_version) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already made remote snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
                      << ", snapshot_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    std::vector<Version> missed_versions;
    for (auto v = request.data_version + 1; v <= request.src_visible_version; ++v) {
        missed_versions.emplace_back(v, v);
    }
    if (UNLIKELY(missed_versions.empty())) {
        LOG(WARNING) << "Remote snapshot tablet skipped, no missing version"
                     << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                     << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
                     << ", data_version: " << request.data_version
                     << ", snapshot_version: " << request.src_visible_version;
        return Status::Corruption("No missing version");
    }

    LOG(INFO) << "Start make remote snapshot, txn_id: " << request.transaction_id
              << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
              << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
              << ", snapshot_version: " << request.src_visible_version << ", missed_versions: ["
              << (request.data_version + 1) << " ... " << request.src_visible_version << "]";

    Status status;
    if (request.data_version <= 1) { // Make full snapshot
        src_snapshot_info->incremental_snapshot = false;
        status = make_remote_snapshot(request, nullptr, nullptr, &src_snapshot_info->backend,
                                      &src_snapshot_info->snapshot_path);
    } else { // Try to make incremental snapshot first, if failed, make full snapshot
        src_snapshot_info->incremental_snapshot = true;
        status = make_remote_snapshot(request, &missed_versions, nullptr, &src_snapshot_info->backend,
                                      &src_snapshot_info->snapshot_path);
        if (!status.ok()) {
            LOG(INFO) << "Failed to make incremental snapshot: " << status << ". switch to fully snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
                      << ", snapshot_version: " << request.src_visible_version;
            src_snapshot_info->incremental_snapshot = false;
            status = make_remote_snapshot(request, nullptr, nullptr, &src_snapshot_info->backend,
                                          &src_snapshot_info->snapshot_path);
        }
    }

    if (!status.ok()) {
        LOG(WARNING) << "Failed to make remote snapshot: " << status << ", txn_id: " << request.transaction_id
                     << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                     << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
                     << ", snapshot_version: " << request.src_visible_version;
        return status;
    }

    src_snapshot_info->__isset.backend = true;
    src_snapshot_info->__isset.snapshot_path = true;
    src_snapshot_info->__isset.incremental_snapshot = true;

    LOG(INFO) << "Made remote snapshot from " << src_snapshot_info->backend.host << ":"
              << src_snapshot_info->backend.be_port << ":" << src_snapshot_info->snapshot_path
              << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
              << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
              << ", data_version: " << request.data_version << ", snapshot_version: " << request.src_visible_version
              << ", incremental_snapshot: " << src_snapshot_info->incremental_snapshot;

    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(request.tablet_id);
    txn_log->set_txn_id(request.transaction_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_txn_id(request.transaction_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_SNAPSHOTED);
    txn_meta->set_tablet_id(request.tablet_id);
    txn_meta->set_visible_version(request.visible_version);
    txn_meta->set_data_version(request.data_version);
    txn_meta->set_src_backend_host(src_snapshot_info->backend.host);
    txn_meta->set_src_backend_port(src_snapshot_info->backend.be_port);
    txn_meta->set_src_snapshot_path(src_snapshot_info->snapshot_path);
    txn_meta->set_snapshot_version(request.src_visible_version);
    txn_meta->set_incremental_snapshot(src_snapshot_info->incremental_snapshot);

    return tablet.put_txn_slog(txn_log);
}

Status ReplicationTxnManager::replicate_snapshot(const TReplicateSnapshotRequest& request) {
    if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
        return Status::InternalError("Process is going to quit. The replicate snapshot will stop");
    }
    if (!request.encryption_meta.empty()) {
        RETURN_IF_ERROR_WITH_WARN(KeyCache::instance().refresh_keys(request.encryption_meta),
                                  "refresh keys using encryption_meta in TReplicateSnapshotRequest failed");
    }

    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(request.tablet_id));

    auto status_or_txn_log = tablet.get_txn_log(request.transaction_id);
    if (status_or_txn_log.ok()) {
        const ReplicationTxnMetaPB& txn_meta = status_or_txn_log.value()->op_replication().txn_meta();
        if (txn_meta.txn_state() >= ReplicationTxnStatePB::TXN_REPLICATED &&
            txn_meta.snapshot_version() == request.src_visible_version) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already replicated remote snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
                      << ", snapshot_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    ASSIGN_OR_RETURN(auto tablet_metadata, tablet.get_metadata(request.visible_version));

    Status status;
    for (const auto& src_snapshot_info : request.src_snapshot_infos) {
        status = replicate_remote_snapshot(request, src_snapshot_info, tablet_metadata);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to download snapshot from " << src_snapshot_info.backend.host << ":"
                         << src_snapshot_info.backend.http_port << ":" << src_snapshot_info.snapshot_path << ", "
                         << status << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                         << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", data_version: " << request.data_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        LOG(INFO) << "Replicated remote snapshot from " << src_snapshot_info.backend.host << ":"
                  << src_snapshot_info.backend.http_port << ":" << src_snapshot_info.snapshot_path << " to "
                  << _tablet_manager->location_provider()->segment_root_location(request.tablet_id)
                  << ", keys_type: " << KeysType_Name(tablet_metadata->schema().keys_type())
                  << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                  << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
                  << ", data_version: " << request.data_version
                  << ", snapshot_version: " << request.src_visible_version;

        return status;
    }

    return status;
}

Status ReplicationTxnManager::replicate_lake_remote_storage(const TReplicateLakeRemoteStorageRequest& request) {
    LOG(INFO) << "Start to replicate lake remote storage, txn_id: " << request.transaction_id
              << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
              << ", src_db_id: " << request.src_db_id << ", src_table_id: " << request.src_table_id
              << ", src_partition_id: " << request.src_partition_id << ", visible_version: " << request.visible_version
              << ", data_version: " << request.data_version << ", faked_shard_id: " << request.faked_shard_id
              << ", src_visible_version: " << request.src_visible_version;
    if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
        return Status::InternalError("Process is going to quit. The replicate lake remote storage task will stop");
    }
    if (!request.encryption_meta.empty()) {
        RETURN_IF_ERROR_WITH_WARN(KeyCache::instance().refresh_keys(request.encryption_meta),
                                  "refresh keys using encryption_meta in TReplicateLakeRemoteStorageRequest failed");
    }

    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(request.tablet_id));
    auto status_or_txn_log = tablet.get_txn_log(request.transaction_id);
    if (status_or_txn_log.ok()) {
        const ReplicationTxnMetaPB& txn_meta = status_or_txn_log.value()->op_replication().txn_meta();
        if (txn_meta.txn_state() >= ReplicationTxnStatePB::TXN_REPLICATED) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already replicated lake remote storage"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id << ", src_db_id: " << request.src_db_id
                      << ", src_table_id: " << request.src_table_id
                      << ", src_partition_id: " << request.src_partition_id
                      << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
                      << ", faked_shard_id: " << request.faked_shard_id
                      << ", src_visible_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    auto txn_id = request.transaction_id;
    auto src_tablet_id = request.src_tablet_id;
    auto target_tablet_id = request.tablet_id;
    auto last_version = request.data_version;
    auto current_version = request.src_visible_version;
    auto partition_path = fmt::format("db{}/{}/{}", request.src_db_id, request.src_table_id, request.src_partition_id);
    auto src_root_loc = _tablet_manager->tablet_root_location(src_tablet_id);
    auto src_root_full_path = join_path(src_root_loc, partition_path);
    auto src_meta_dir = join_path(src_root_full_path, kMetadataDirectoryName);
    auto src_data_dir = join_path(src_root_full_path, kSegmentDirectoryName);

    LOG(INFO) << "Lake replicate storage task, built source meta and data dir, meta dir: " << src_meta_dir
              << ", data dir: " << src_data_dir << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
              << ", target_tablet_id: " << target_tablet_id;

    // use faked_shard_id here to distinguish src starlet fs
    auto shared_src_fs = new_fs_starlet_with_shard_fs(request.faked_shard_id);
    if (shared_src_fs == nullptr) {
        return Status::InternalError(fmt::format("Failed to create file system for faked shard id {}, tablet_id: {} ",
                                                 request.faked_shard_id, target_tablet_id));
    }

    // step 1: read source tablet meta file
    std::unordered_set<std::string> added_segments;
    std::unordered_set<std::string> added_del_files;
    ASSIGN_OR_RETURN(auto last_src_tablet_meta,
                     build_source_tablet_meta(src_tablet_id, last_version, src_meta_dir, shared_src_fs));
    ASSIGN_OR_RETURN(auto current_src_tablet_meta,
                     build_source_tablet_meta(src_tablet_id, current_version, src_meta_dir, shared_src_fs));
    RETURN_IF_ERROR(find_files_diff_between_rowset_metas(last_src_tablet_meta, current_src_tablet_meta, added_segments,
                                                         added_del_files));
    LOG(INFO) << "Lake replicate storage task, found new added segments and del files between versions " << last_version
              << " and " << current_version << ", added_segments size: " << added_segments.size()
              << ", added_del_files size: " << added_del_files.size() << ", txn_id: " << txn_id
              << ", src_tablet_id: " << src_tablet_id << ", target_tablet_id: " << target_tablet_id;

    // filename_map mappings between source and target file names
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>> filename_map;
    ASSIGN_OR_RETURN(auto last_target_tablet_meta,
                     _tablet_manager->get_tablet_metadata(target_tablet_id, last_version, true, 0, nullptr));
    // build file mappings for last tablet meta, in order to skipp generating new file names for new version
    for (int i = 0; i < last_target_tablet_meta->rowsets_size(); ++i) {
        const auto& source_rowset_meta = last_src_tablet_meta->rowsets(i);
        const auto& target_rowset_meta = last_target_tablet_meta->rowsets(i);
        for (int i = 0; i < target_rowset_meta.segments_size(); ++i) {
            auto old_segment_filename = source_rowset_meta.segments(i);
            auto new_segment_filename = target_rowset_meta.segments(i);
            // use an empty encryption_info since we just need to maintain the mappings here
            FileEncryptionInfo encryption_info;
            filename_map.emplace(std::move(old_segment_filename),
                                 std::make_pair(new_segment_filename, encryption_info));
        }

        for (int i = 0; i < target_rowset_meta.del_files_size(); ++i) {
            auto old_del_filename = source_rowset_meta.del_files(i).name();
            auto new_del_filename = target_rowset_meta.del_files(i).name();
            // use nullptr as encryption info since we just need to maintain the mappings here
            FileEncryptionInfo encryption_info;
            filename_map.emplace(std::move(old_del_filename), std::make_pair(new_del_filename, encryption_info));
        }
    }

    // step 2: build txn log and maintain rowset meta and path mappings between src & target file
    auto txn_log = std::make_shared<TxnLog>();
    for (const auto& src_rowset : current_src_tablet_meta->rowsets()) {
        auto* op_write = txn_log->mutable_op_replication()->add_op_writes();
        auto st = convert_lake_replicate_rowset_meta(request, src_rowset, op_write, &filename_map);
        if (!st.ok()) {
            LOG(WARNING) << "Lake replicate storage task, failed to convert rowset meta, src_tablet_id: "
                         << src_tablet_id << ", version: " << current_version << ", error: " << st;
            return st;
        }
    }

    // step 3: find diff and transfer files
    std::map<std::string, std::string> file_locations;
    for (const auto& src_segment_filename : added_segments) {
        auto iter = filename_map.find(src_segment_filename);
        if (UNLIKELY(iter == filename_map.end())) {
            return Status::Corruption("Failed to find target file name for source segment file: " +
                                      src_segment_filename);
        }
        auto target_segment_path = _tablet_manager->segment_location(request.tablet_id, iter->second.first);
        file_locations.emplace(join_path(src_data_dir, src_segment_filename), target_segment_path);
    }

    for (const auto& src_del_filename : added_del_files) {
        auto iter = filename_map.find(src_del_filename);
        if (UNLIKELY(iter == filename_map.end())) {
            return Status::Corruption("Failed to find target file name for source del file: " + src_del_filename);
        }
        auto target_del_file_path = _tablet_manager->del_location(request.tablet_id, iter->second.first);
        file_locations.emplace(join_path(src_meta_dir, src_del_filename), target_del_file_path);
    }

    LOG(INFO) << "Lake replicate storage task, need transfer file count: " << file_locations.size()
              << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
              << ", target_tablet_id: " << target_tablet_id;

    // set to nullptr, will create one in function fs::copy_file
    auto dest_fs = nullptr;
    for (auto& [src_file_location, target_file_location] : file_locations) {
        auto res = fs::copy_file(src_file_location, shared_src_fs, target_file_location, dest_fs, 1024 * 1024);
        if (!res.ok()) {
            LOG(WARNING) << "Failed to replicate lake remote file, src file: " << src_file_location
                         << ", target: " << target_file_location << ", error: " << res;
            return res;
        }
        LOG(INFO) << "Finished to replicate lake remote file, src file: " << src_file_location
                  << ", target: " << target_file_location;
    }

    // step 4: convert schema
    txn_log->mutable_op_replication()->mutable_source_schema()->CopyFrom(current_src_tablet_meta->schema());
    const TabletSchemaPB* source_schema_pb = &txn_log->op_replication().source_schema();
    if (source_schema_pb == nullptr) {
        LOG(WARNING) << "Failed to get source schema, tablet source tablet: " << src_tablet_id
                     << ", target tablet: " << target_tablet_id;
        return Status::Corruption("Failed to get source schema");
    }

    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ASSIGN_OR_RETURN(auto target_tablet_metadata, tablet.get_metadata(request.visible_version));
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb->column(), target_tablet_metadata->schema().column(),
                                                &column_unique_id_map);

    for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
        if (op_write.has_txn_meta()) {
            RETURN_IF_ERROR(
                    ReplicationUtils::convert_rowset_txn_meta(op_write.mutable_txn_meta(), column_unique_id_map));
        }
    }
    // step 5: write txn log
    txn_log->set_tablet_id(target_tablet_id);
    txn_log->set_txn_id(txn_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_tablet_id(target_tablet_id);
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_visible_version(request.visible_version);
    txn_meta->set_data_version(request.data_version);
    txn_meta->set_shared_data_source(true);

    RETURN_IF_ERROR(_tablet_manager->put_txn_log(txn_log));

    LOG(INFO) << "Replicated lake remote files, txn_id: " << request.transaction_id
              << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
              << ", src_db_id: " << request.src_db_id << ", src_table_id: " << request.src_table_id
              << ", src_partition_id: " << request.src_partition_id << ", visible_version: " << request.visible_version
              << ", data_version: " << request.data_version << ", faked_shard_id: " << request.faked_shard_id
              << ", src_visible_version: " << request.src_visible_version;

    return Status::OK();
}

Status ReplicationTxnManager::clear_snapshots(const TxnLogPtr& txn_slog) {
    const auto& txn_meta = txn_slog->op_replication().txn_meta();
    return ReplicationUtils::release_remote_snapshot(txn_meta.src_backend_host(), txn_meta.src_backend_port(),
                                                     txn_meta.src_snapshot_path());
}

Status ReplicationTxnManager::make_remote_snapshot(const TRemoteSnapshotRequest& request,
                                                   const std::vector<Version>* missed_versions,
                                                   const std::vector<int64_t>* missing_version_ranges,
                                                   TBackend* src_backend, std::string* src_snapshot_path) {
    int timeout_s = 0;
    if (request.__isset.timeout_sec) {
        timeout_s = request.timeout_sec;
    }

    Status status;
    for (const auto& src_be : request.src_backends) {
        // Make snapshot in remote olap engine
        status = ReplicationUtils::make_remote_snapshot(src_be.host, src_be.be_port, request.src_tablet_id,
                                                        request.src_schema_hash, request.src_visible_version, timeout_s,
                                                        missed_versions, missing_version_ranges, src_snapshot_path);
        if (!status.ok()) {
            continue;
        }

        *src_backend = src_be;
        break;
    }

    return status;
}

Status ReplicationTxnManager::replicate_remote_snapshot(const TReplicateSnapshotRequest& request,
                                                        const TSnapshotInfo& src_snapshot_info,
                                                        const TabletMetadataPtr& tablet_metadata) {
    auto txn_log = std::make_shared<TxnLog>();
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>> filename_map;
    const TabletSchemaPB* source_schema_pb = nullptr;

    if (!is_primary_key(*tablet_metadata)) { // None-pk table
        std::string remote_header_file_name = std::to_string(request.src_tablet_id) + ".hdr";
        ASSIGN_OR_RETURN(auto header_file_content,
                         ReplicationUtils::download_remote_snapshot_file(
                                 src_snapshot_info.backend.host, src_snapshot_info.backend.http_port, request.src_token,
                                 src_snapshot_info.snapshot_path, request.src_tablet_id, request.src_schema_hash,
                                 remote_header_file_name, config::download_low_speed_time));
        TabletMeta tablet_meta;
        auto status = tablet_meta.create_from_memory(header_file_content);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to parse remote snapshot header file: " << remote_header_file_name
                         << ", content: " << header_file_content << ", status: " << status;
            return status.clone_and_prepend("Failed to parse remote snapshot header file: " + remote_header_file_name +
                                            ", content: " + header_file_content + ", status");
        }

        const auto& rowset_metas =
                tablet_meta.all_rs_metas().empty() ? tablet_meta.all_inc_rs_metas() : tablet_meta.all_rs_metas();
        for (const auto& rowset_meta : rowset_metas) {
            auto* op_write = txn_log->mutable_op_replication()->add_op_writes();
            RETURN_IF_ERROR(convert_rowset_meta(*rowset_meta, request.transaction_id, op_write, &filename_map));
        }
        // None-pk table always has tablet schema in tablet meta
        tablet_meta.tablet_schema_ptr()->to_schema_pb(txn_log->mutable_op_replication()->mutable_source_schema());
        source_schema_pb = &txn_log->op_replication().source_schema();
    } else { // Pk table
        std::string snapshot_meta_file_name = "meta";
        ASSIGN_OR_RETURN(auto snapshot_meta_content,
                         ReplicationUtils::download_remote_snapshot_file(
                                 src_snapshot_info.backend.host, src_snapshot_info.backend.http_port, request.src_token,
                                 src_snapshot_info.snapshot_path, request.src_tablet_id, request.src_schema_hash,
                                 snapshot_meta_file_name, config::download_low_speed_time));

        auto memory_file = new_random_access_file_from_memory(snapshot_meta_file_name, snapshot_meta_content);
        SnapshotMeta snapshot_meta;
        auto status = snapshot_meta.parse_from_file(memory_file.get());
        if (!status.ok()) {
            LOG(WARNING) << "Failed to parse remote snapshot meta file: " << snapshot_meta_file_name
                         << ", content: " << snapshot_meta_content << ", status: " << status;
            return status.clone_and_prepend("Failed to parse remote snapshot meta file: " + snapshot_meta_file_name +
                                            ", content: " + snapshot_meta_content + ", status");
        }

        DCHECK(((src_snapshot_info.incremental_snapshot &&
                 snapshot_meta.snapshot_type() == SnapshotTypePB::SNAPSHOT_TYPE_INCREMENTAL) ||
                (!src_snapshot_info.incremental_snapshot &&
                 snapshot_meta.snapshot_type() == SnapshotTypePB::SNAPSHOT_TYPE_FULL)))
                << ", incremental_snapshot: " << src_snapshot_info.incremental_snapshot
                << ", snapshot_type: " << SnapshotTypePB_Name(snapshot_meta.snapshot_type());

        const auto& rowset_metas = snapshot_meta.rowset_metas();
        for (const auto& rowset_meta_pb : rowset_metas) {
            RowsetMeta rowset_meta(rowset_meta_pb);
            auto* op_write = txn_log->mutable_op_replication()->add_op_writes();
            RETURN_IF_ERROR(convert_rowset_meta(rowset_meta, request.transaction_id, op_write, &filename_map));
        }

        for (const auto& [segment_id, delvec] : snapshot_meta.delete_vectors()) {
            auto* delvecs = txn_log->mutable_op_replication()->mutable_delvecs();
            auto& delvec_data = (*delvecs)[segment_id];
            delvec_data.set_version(delvec.version());
            delvec.save_to(delvec_data.mutable_data());
        }

        if (snapshot_meta.tablet_meta().has_schema()) {
            // Try to get source schema from tablet meta, only full snapshot has tablet meta
            txn_log->mutable_op_replication()->mutable_source_schema()->CopyFrom(snapshot_meta.tablet_meta().schema());
            source_schema_pb = &txn_log->op_replication().source_schema();
        } else if (!rowset_metas.empty() && rowset_metas.front().has_tablet_schema()) {
            // Try to get source schema from rowset meta, rowset meta has schema if light schema change enabled in source cluster
            txn_log->mutable_op_replication()->mutable_source_schema()->CopyFrom(
                    TabletMeta::rowset_meta_pb_with_max_rowset_version(rowset_metas).tablet_schema());
            source_schema_pb = &txn_log->op_replication().source_schema();
        } else {
            // Get source schema from previous saved in tablet meta
            source_schema_pb = &tablet_metadata->source_schema();
        }

        if (source_schema_pb == nullptr) {
            LOG(WARNING) << "Failed to get source schema, tablet meta has schema: "
                         << snapshot_meta.tablet_meta().has_schema() << ", rowset meta has schema: "
                         << (!rowset_metas.empty() && rowset_metas.front().has_tablet_schema());
            return Status::Corruption("Failed to get source schema");
        }
    }

    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb->column(), tablet_metadata->schema().column(),
                                                &column_unique_id_map);

    std::vector<std::string> files_to_delete;
    CancelableDefer clean_files([&files_to_delete]() { lake::delete_files_async(std::move(files_to_delete)); });

    auto file_converters = [&](const std::string& file_name,
                               uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
        if (request.transaction_id < get_master_info().min_active_txn_id) {
            LOG(WARNING) << "Transaction is aborted, txn_id: " << request.transaction_id
                         << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", data_version: " << request.data_version
                         << ", snapshot_version: " << request.src_visible_version;
            return Status::InternalError("Transaction is aborted");
        }

        auto iter = filename_map.find(file_name);
        if (iter == filename_map.end()) {
            return nullptr;
        }

        auto segment_location = _tablet_manager->segment_location(request.tablet_id, iter->second.first);
        WritableFileOptions opts{.sync_on_close = true,
                                 .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                 .encryption_info = iter->second.second};
        ASSIGN_OR_RETURN(auto output_file, fs::new_writable_file(opts, segment_location));

        files_to_delete.push_back(std::move(segment_location));

        if (is_segment(file_name) && !column_unique_id_map.empty()) {
            return std::make_unique<SegmentStreamConverter>(file_name, file_size, std::move(output_file),
                                                            &column_unique_id_map);
        }
        return std::make_unique<FileStreamConverter>(file_name, file_size, std::move(output_file));
    };

    RETURN_IF_ERROR(ReplicationUtils::download_remote_snapshot(
            src_snapshot_info.backend.host, src_snapshot_info.backend.http_port, request.src_token,
            src_snapshot_info.snapshot_path, request.src_tablet_id, request.src_schema_hash, file_converters));

    for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
        if (op_write.has_txn_meta()) {
            RETURN_IF_ERROR(
                    ReplicationUtils::convert_rowset_txn_meta(op_write.mutable_txn_meta(), column_unique_id_map));
        }
    }

    txn_log->set_tablet_id(request.tablet_id);
    txn_log->set_txn_id(request.transaction_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_txn_id(request.transaction_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_tablet_id(request.tablet_id);
    txn_meta->set_visible_version(request.visible_version);
    txn_meta->set_data_version(request.data_version);
    txn_meta->set_src_backend_host(src_snapshot_info.backend.host);
    txn_meta->set_src_backend_port(src_snapshot_info.backend.be_port);
    txn_meta->set_src_snapshot_path(src_snapshot_info.snapshot_path);
    txn_meta->set_snapshot_version(request.src_visible_version);
    txn_meta->set_incremental_snapshot(src_snapshot_info.incremental_snapshot);

    RETURN_IF_ERROR(_tablet_manager->put_txn_log(txn_log));

    clean_files.cancel();
    return Status::OK();
}

Status ReplicationTxnManager::convert_rowset_meta(
        const RowsetMeta& rowset_meta, TTransactionId transaction_id, TxnLogPB::OpWrite* op_write,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>* filename_map) {
    if (rowset_meta.is_column_mode_partial_update()) {
        return Status::NotSupported("Column mode partial update is not supported in shared-data mode");
    }

    // Convert rowset metadata
    auto* rowset_metadata = op_write->mutable_rowset();
    rowset_metadata->set_id(rowset_meta.get_rowset_seg_id());
    rowset_metadata->set_overlapped(rowset_meta.is_segments_overlapping());
    rowset_metadata->set_num_rows(rowset_meta.num_rows());
    rowset_metadata->set_data_size(rowset_meta.data_disk_size());
    rowset_metadata->set_num_dels(rowset_meta.get_num_delete_files());
    if (rowset_meta.has_delete_predicate()) {
        auto* delete_predicate_pb = rowset_metadata->mutable_delete_predicate();
        delete_predicate_pb->CopyFrom(rowset_meta.delete_predicate());
        RETURN_IF_ERROR(convert_delete_predicate_pb(delete_predicate_pb));
    }

    std::string rowset_id = rowset_meta.rowset_id().to_string();
    for (int64_t segment_id = 0; segment_id < rowset_meta.num_segments(); ++segment_id) {
        std::string old_segment_filename = rowset_id + '_' + std::to_string(segment_id) + ".dat";
        std::string new_segment_filename = gen_segment_filename(transaction_id);

        rowset_metadata->add_segments(new_segment_filename);
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            rowset_metadata->add_segment_encryption_metas(pair.encryption_meta);
            encryption_info = std::move(pair.info);
        }
        auto pair = filename_map->emplace(std::move(old_segment_filename),
                                          std::pair(std::move(new_segment_filename), std::move(encryption_info)));
        if (!pair.second) {
            return Status::Corruption("Duplicated segment file: " + pair.first->first);
        }
    }

    // Convert rowset txn meta
    if (rowset_meta.has_txn_meta()) {
        auto* rowset_txn_meta = op_write->mutable_txn_meta();
        rowset_txn_meta->CopyFrom(rowset_meta.txn_meta());
    }

    // Convert dels
    for (int64_t del_id = 0; del_id < rowset_meta.get_num_delete_files(); ++del_id) {
        std::string old_del_filename = rowset_id + '_' + std::to_string(del_id) + ".del";
        std::string new_del_filename = gen_del_filename(transaction_id);

        op_write->add_dels(new_del_filename);
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            op_write->add_del_encryption_metas(pair.encryption_meta);
            encryption_info = std::move(pair.info);
        }
        auto pair = filename_map->emplace(std::move(old_del_filename),
                                          std::pair(std::move(new_del_filename), std::move(encryption_info)));
        if (!pair.second) {
            return Status::Corruption("Duplicated del file: " + pair.first->first);
        }
    }

    return Status::OK();
}

Status ReplicationTxnManager::convert_delete_predicate_pb(DeletePredicatePB* delete_predicate) {
    for (const auto& sub_predicate : delete_predicate->sub_predicates()) {
        TCondition condition;
        if (!DeleteHandler::parse_condition(sub_predicate, &condition)) {
            LOG(WARNING) << "Invalid delete condition: " << sub_predicate;
            return Status::InternalError("Invalid delete condition: " + sub_predicate);
        }
        if (condition.condition_op == "IS") {
            auto* is_null_predicate = delete_predicate->add_is_null_predicates();
            is_null_predicate->set_column_name(condition.column_name);
            is_null_predicate->set_is_not_null(condition.condition_values[0].starts_with("NOT"));
        } else if (condition.condition_op == "*=" || condition.condition_op == "!*=") {
            auto* in_predicate = delete_predicate->add_in_predicates();
            in_predicate->set_column_name(condition.column_name);
            in_predicate->set_is_not_in(condition.condition_op.starts_with('!'));
            for (const auto& value : condition.condition_values) {
                in_predicate->add_values()->assign(value);
            }
        } else {
            auto* binary_predicate = delete_predicate->add_binary_predicates();
            binary_predicate->set_column_name(condition.column_name);
            if (condition.condition_op == "<<") {
                binary_predicate->set_op("<");
            } else if (condition.condition_op == ">>") {
                binary_predicate->set_op(">");
            } else {
                binary_predicate->set_op(condition.condition_op);
            }
            binary_predicate->set_value(condition.condition_values[0]);
        }
    }
    return Status::OK();
}

Status ReplicationTxnManager::find_files_diff_between_rowset_metas(TabletMetadataPtr last_src_tablet_meta,
                                                                   TabletMetadataPtr current_src_tablet_meta,
                                                                   std::unordered_set<std::string>& added_segments,
                                                                   std::unordered_set<std::string>& added_del_files) {
    std::unordered_set<std::string> start_segments;
    std::unordered_set<std::string> start_del_files;

    auto collect_start_assets = [&](const RowsetMetadataPB& rowset) {
        for (const auto& seg : rowset.segments()) {
            start_segments.insert(seg);
        }
        for (const auto& del : rowset.del_files()) {
            start_del_files.insert(del.name());
        }
    };

    for (const auto& rowset_meta : last_src_tablet_meta->rowsets()) {
        collect_start_assets(rowset_meta);
    }

    std::unordered_set<std::string> result_segments;
    std::unordered_set<std::string> result_del_files;

    auto check_end_assets = [&](const RowsetMetadataPB& rowset) {
        for (const auto& seg : rowset.segments()) {
            if (!start_segments.count(seg) && !result_segments.count(seg)) {
                LOG(INFO) << "Lake replicate storage task, found new segment file: " << seg;
                added_segments.insert(seg);
                result_segments.insert(seg);
            }
        }
        for (const auto& del : rowset.del_files()) {
            auto name = del.name();
            if (!start_del_files.count(name) && !result_del_files.count(name)) {
                LOG(INFO) << "Lake replicate storage task, found new del file: " << name;
                added_del_files.insert(name);
                result_del_files.insert(name);
            }
        }
    };

    for (const auto& rowset_meta : current_src_tablet_meta->rowsets()) {
        check_end_assets(rowset_meta);
    }

    return Status::OK();
}

StatusOr<TabletMetadataPtr> ReplicationTxnManager::build_source_tablet_meta(int64_t src_tablet_id, int64_t version,
                                                                            const std::string& meta_dir,
                                                                            std::shared_ptr<FileSystem> shared_src_fs) {
    LOG(INFO) << "Lake replicate storage task, building source tablet meta for tablet: " << src_tablet_id
              << ", version: " << version;
    auto src_metadata_file_name = tablet_metadata_filename(src_tablet_id, version);
    auto src_tablet_meta_path = join_path(meta_dir, src_metadata_file_name);
    auto src_tablet_meta_or = _tablet_manager->get_tablet_metadata(src_tablet_meta_path, false, 0, shared_src_fs);
    if (!src_tablet_meta_or.ok()) {
        LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                     << ", src_tablet_id: " << src_tablet_id << ", error: " << src_tablet_meta_or;
        return src_tablet_meta_or;
    }
    return src_tablet_meta_or.value();
}

Status ReplicationTxnManager::convert_lake_replicate_rowset_meta(
        const TReplicateLakeRemoteStorageRequest& request, const RowsetMetadataPB& src_rowset_meta,
        TxnLogPB::OpWrite* op_write,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>* filename_map) {
    LOG(INFO) << "Converting rowset meta for tablet: " << request.tablet_id
              << ", src_tablet_id: " << request.src_tablet_id << ", src_rowset_meta_id: " << src_rowset_meta.id()
              << ", segment size: " << src_rowset_meta.segments_size();
    // Convert rowset metadata
    auto* rowset_metadata = op_write->mutable_rowset();
    rowset_metadata->set_id(src_rowset_meta.id());
    rowset_metadata->set_overlapped(src_rowset_meta.overlapped());
    rowset_metadata->set_num_rows(src_rowset_meta.num_rows());
    rowset_metadata->set_data_size(src_rowset_meta.data_size());
    rowset_metadata->set_num_dels(src_rowset_meta.num_dels());

    // optional DeletePredicatePB delete_predicate
    if (src_rowset_meta.has_delete_predicate()) {
        auto* delete_predicate_pb = rowset_metadata->mutable_delete_predicate();
        delete_predicate_pb->CopyFrom(src_rowset_meta.delete_predicate());
        RETURN_IF_ERROR(convert_delete_predicate_pb(delete_predicate_pb));
    }

    // repeated string segments
    for (int i = 0; i < src_rowset_meta.segments_size(); ++i) {
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            rowset_metadata->add_segment_encryption_metas(pair.encryption_meta);
            encryption_info = std::move(pair.info);
        }
        std::string new_segment_filename;
        auto old_segment_filename = src_rowset_meta.segments(i);
        const auto it = filename_map->find(old_segment_filename);
        if (it != filename_map->end()) {
            // Reuse existing mapping
            LOG(INFO) << "Lake replicate storage task, reuse existing mapping, new segment name: " << it->second.first
                      << ", old segment name: " << old_segment_filename << ", tablet: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", src_rowset_meta_id: " << src_rowset_meta.id();
            new_segment_filename = it->second.first;
            rowset_metadata->add_segments(new_segment_filename);
        } else {
            // Generate new segment filename
            new_segment_filename = starrocks::lake::gen_segment_filename(request.transaction_id);
            rowset_metadata->add_segments(new_segment_filename);
            auto pair = filename_map->emplace(std::move(old_segment_filename),
                                              std::pair(std::move(new_segment_filename), std::move(encryption_info)));
            if (!pair.second) {
                return Status::Corruption("Duplicated segment file: " + pair.first->first);
            }
        }
        rowset_metadata->add_segment_size(src_rowset_meta.segment_size(i));
    }

    // repeated DelfileWithRowsetId del_files
    for (const auto& src_del_file : src_rowset_meta.del_files()) {
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            op_write->add_del_encryption_metas(pair.encryption_meta);
            encryption_info = std::move(pair.info);
        }
        std::string new_del_filename;
        const auto it = filename_map->find(src_del_file.name());
        if (it != filename_map->end()) {
            // Reuse existing mapping
            new_del_filename = it->second.first;
            op_write->add_dels(new_del_filename);
        } else {
            // Generate new del filename
            new_del_filename = starrocks::lake::gen_delvec_filename(request.transaction_id);
            op_write->add_dels(new_del_filename);
            auto pair = filename_map->emplace(std::move(src_del_file.name()),
                                              std::pair(std::move(new_del_filename), std::move(encryption_info)));
            if (!pair.second) {
                return Status::Corruption("Duplicated del file: " + pair.first->first);
            }
        }
    }

    return Status::OK();
}
} // namespace starrocks::lake
