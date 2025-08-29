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

    if (request.src_tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        auto status = replicate_lake_remote_storage(request);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to replicate lake remote file, tablet_id: " << request.tablet_id
                         << ", txn_id: " << request.transaction_id << ", src_tablet_id: " << request.src_tablet_id
                         << ", error: " << status.to_string();
        }
        return status;
    } else {
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
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version << ", data_version: " << request.data_version
                      << ", snapshot_version: " << request.src_visible_version;

            return status;
        }

        return status;
    }
}

Status ReplicationTxnManager::replicate_lake_remote_storage(const TReplicateSnapshotRequest& request) {
    auto src_tablet_id = request.src_tablet_id;
    auto src_visible_version = request.src_visible_version;
    auto src_db_id = request.src_db_id;
    auto src_table_id = request.src_table_id;
    auto src_partition_id = request.src_partition_id;

    auto data_version = request.data_version;
    auto target_visible_version = request.visible_version;
    auto target_tablet_id = request.tablet_id;

    auto txn_id = request.transaction_id;
    auto virtual_tablet_id = request.virtual_tablet_id;

    LOG(INFO) << "Start to replicate lake remote storage, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", src_db_id: " << src_db_id
              << ", src_table_id: " << src_table_id << ", src_partition_id: " << src_partition_id
              << ", visible_version: " << target_visible_version << ", data_version: " << data_version
              << ", virtual_tablet_id: " << virtual_tablet_id << ", src_visible_version: " << src_visible_version;

    std::vector<Version> missed_versions;
    for (auto v = data_version + 1; v <= src_visible_version; ++v) {
        missed_versions.emplace_back(v, v);
    }
    if (UNLIKELY(missed_versions.empty())) {
        LOG(WARNING) << "Replicate lake remote storage skipped, no missing version"
                     << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
                     << ", src_tablet_id: " << src_tablet_id << ", visible_version: " << target_visible_version
                     << ", data_version: " << data_version << ", src_visible_version: " << src_visible_version;
        return Status::Corruption("No missing version");
    }

    auto src_meta_dir =
            _remote_location_provider->metadata_root_location(src_tablet_id, src_db_id, src_table_id, src_partition_id);
    auto src_data_dir =
            _remote_location_provider->segment_root_location(src_tablet_id, src_db_id, src_table_id, src_partition_id);

    // `shared_src_fs` is used to access storage of src cluster
    auto shared_src_fs = new_fs_starlet(virtual_tablet_id);

    VLOG(3) << "Lake replicate storage task, built source meta and data dir, meta dir: " << src_meta_dir
            << ", data dir: " << src_data_dir << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
            << ", tablet_id: " << target_tablet_id;

    // mak full replication for shared-data cluster migration
    bool incremental = false;

    // collect all rowsets for replication
    // `new_rowsets` indicates all rowsets that will be covered to replicate files to target storage
    std::vector<RowsetMetadataPB> new_rowsets;
    ASSIGN_OR_RETURN(auto src_current_tablet_meta,
                     build_source_tablet_meta(src_tablet_id, src_visible_version, src_meta_dir, shared_src_fs));

    VLOG(3) << "Start to convert rowset metas for tablet_id: " << target_tablet_id
            << ", src_tablet_id: " << src_tablet_id << ", rowsets size: " << src_current_tablet_meta->rowsets().size();

    std::unordered_set<std::string> existed_filename_uuids;
    ASSIGN_OR_RETURN(auto target_data_version_tablet_meta,
                     _tablet_manager->get_tablet_metadata(target_tablet_id, data_version, false, 0, nullptr));
    for (const auto& rowset : target_data_version_tablet_meta->rowsets()) {
        for (int i = 0; i < rowset.segments_size(); ++i) {
            auto segment_name = rowset.segments(i);
            existed_filename_uuids.emplace(extract_uuid_from(segment_name));
        }
    }

    // build file_locations mappings between source and target file locations,
    // it contains all files that need to replicate from source to target storage
    std::map<std::string, std::string> file_locations;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>> filename_map;
    // build the mapping between segment file name to its file size
    // we use the `segment_size` field in rowset metadata to get the file size.
    // for history reasons, the `segment_size` field is not always present, so the resulting map is not guaranteed to
    // have all segment file sizes.
    std::unordered_map<std::string, size_t> segment_name_to_size_map;

    // build txn log and rowsets
    auto txn_log = std::make_shared<TxnLog>();
    std::shared_ptr<TabletMetadataPB> new_metadata = std::make_shared<TabletMetadataPB>(*src_current_tablet_meta);
    new_metadata->mutable_rowsets()->Clear();
    new_metadata->mutable_sstable_meta()->Clear();

    for (const auto& src_rowset_meta : src_current_tablet_meta->rowsets()) {
        auto new_rowset_meta = new_metadata->add_rowsets();
        new_rowset_meta->CopyFrom(src_rowset_meta);
        new_rowset_meta->mutable_segments()->Clear();
        RETURN_IF_ERROR(convert_lake_replicate_rowset_meta(src_rowset_meta, new_rowset_meta, txn_id, src_tablet_id,
                                                           target_tablet_id, src_data_dir, existed_filename_uuids,
                                                           segment_name_to_size_map, file_locations, filename_map));
    }

    RETURN_IF_ERROR(convert_lake_replicate_sstable_meta(new_metadata, src_current_tablet_meta, txn_id, target_tablet_id,
                                                        src_data_dir, existed_filename_uuids, file_locations,
                                                        filename_map));

    txn_log->mutable_op_replication()->mutable_tablet_metadata()->CopyFrom(*new_metadata);

    VLOG(3) << "Lake replicate storage task, found new added segments files between versions " << data_version
            << " and " << src_visible_version << ", added segment size: " << file_locations.size()
            << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
            << ", target_tablet_id: " << target_tablet_id << ", filename_map size: " << filename_map.size();

    // copy source schema
    txn_log->mutable_op_replication()->mutable_source_schema()->CopyFrom(src_current_tablet_meta->schema());
    const TabletSchemaPB* source_schema_pb = &txn_log->op_replication().source_schema();
    if (source_schema_pb == nullptr) {
        LOG(WARNING) << "Failed to get source schema, tablet source tablet: " << src_tablet_id
                     << ", target tablet: " << target_tablet_id;
        return Status::Corruption("Failed to get source schema");
    }

    // calc column unique id to adapt for fast schema change
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ASSIGN_OR_RETURN(auto target_tablet, _tablet_manager->get_tablet(target_tablet_id));
    ASSIGN_OR_RETURN(auto target_tablet_metadata, target_tablet.get_metadata(target_visible_version));
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb->column(), target_tablet_metadata->schema().column(),
                                                &column_unique_id_map);

    std::vector<std::string> files_to_delete;
    CancelableDefer clean_files([&files_to_delete]() { lake::delete_files_async(std::move(files_to_delete)); });
    auto file_converters = build_file_converters(request, filename_map, column_unique_id_map, files_to_delete);
    for (const auto& pair : filename_map) {
        const auto& src_file_name = pair.first;
        auto src_file_location = join_path(src_data_dir, src_file_name);
        auto it = file_locations.find(src_file_location);
        if (it == file_locations.end()) {
            LOG(WARNING) << "Found invalid file location, src file location: " << src_file_location;
            continue;
        }
        const auto& target_file_location = it->second;
        // check if need to convert segment file while downloading
        if (is_segment(src_file_name) && !column_unique_id_map.empty()) {
            // file_size might be empty, in that case we'll get it while downloading the file
            auto file_size = segment_name_to_size_map[src_file_name];
            RETURN_IF_ERROR(ReplicationUtils::download_lake_segment_file(src_file_location, src_file_name, file_size,
                                                                         shared_src_fs, file_converters));
        } else {
            // since we can't easily get the file size for non-segment files, here we just use fs::copy operation
            RETURN_IF_ERROR(
                    fs::copy_file(src_file_location, shared_src_fs, target_file_location, nullptr, 1024 * 1024));
        }
        LOG(INFO) << "Finished to replicate lake remote file, src file: " << src_file_location
                  << ", target: " << target_file_location;
    }

    // write txn log
    txn_log->set_tablet_id(target_tablet_id);
    txn_log->set_txn_id(txn_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_tablet_id(target_tablet_id);
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_visible_version(target_visible_version);
    txn_meta->set_data_version(data_version);
    txn_meta->set_snapshot_version(src_visible_version);
    txn_meta->set_incremental_snapshot(incremental);

    RETURN_IF_ERROR(_tablet_manager->put_txn_log(txn_log));

    LOG(INFO) << "Replicated lake remote files, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", src_db_id: " << src_db_id
              << ", src_table_id: " << src_table_id << ", src_partition_id: " << src_partition_id
              << ", visible_version: " << target_visible_version << ", data_version: " << data_version
              << ", virtual_tablet_id: " << virtual_tablet_id << ", src_visible_version: " << src_visible_version;

    clean_files.cancel();
    return Status::OK();
}

StatusOr<TabletMetadataPtr> ReplicationTxnManager::build_source_tablet_meta(
        int64_t src_tablet_id, int64_t version, const std::string& meta_dir,
        const std::shared_ptr<FileSystem>& shared_src_fs) {
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
        const RowsetMetadataPB& src_rowset_meta, RowsetMetadataPB* new_rowset_meta, TTransactionId txn_id,
        int64_t src_tablet_id, int64_t target_tablet_id, const std::string& src_data_dir,
        const std::unordered_set<std::string>& existed_filename_uuids,
        std::unordered_map<std::string, size_t>& segment_name_to_size_map,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>& filename_map) {
    // check if segment size is valid
    auto segment_size_size = src_rowset_meta.segment_size_size();
    if (segment_size_size > 0) {
        auto segment_file_size = src_rowset_meta.segments_size();
        // `segment_size_size` and `segment_file_size` should always be equal
        if (UNLIKELY(segment_size_size > 0 && segment_size_size != segment_file_size)) {
            return Status::Corruption(
                    fmt::format("found invalidate segment_size, src_tablet_id: {}, "
                                "rowse_id: {}, segment_size_size: {}, segment_file_size: {}",
                                src_tablet_id, src_rowset_meta.id(), segment_size_size, segment_file_size));
        }
    }

    LOG(INFO) << "Converting rowset meta for tablet: " << target_tablet_id << ", src_tablet_id: " << src_tablet_id
              << ", src_rowset_meta_id: " << src_rowset_meta.id()
              << ", segments size: " << src_rowset_meta.segments_size();

    // Convert rowset metadata
    for (int i = 0; i < src_rowset_meta.segments_size(); ++i) {
        auto src_segment_filename = src_rowset_meta.segments(i);
        auto new_segment_filename = gen_segment_filename_from(txn_id, src_segment_filename);
        if (UNLIKELY(new_segment_filename.empty())) {
            return Status::Corruption("Failed to generate new segment filename from: " + src_segment_filename);
        }
        new_rowset_meta->add_segments(new_segment_filename);

        // build file_locations map
        if (existed_filename_uuids.count(extract_uuid_from(src_segment_filename)) == 0) {
            // not existed, means need to replicate from source to target, add it to file_locations map
            auto target_segment_path = _tablet_manager->segment_location(target_tablet_id, new_segment_filename);
            file_locations.emplace(join_path(src_data_dir, src_segment_filename), target_segment_path);
        }

        // build filename_map
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            new_rowset_meta->add_segment_encryption_metas(pair.encryption_meta);
            encryption_info = std::move(pair.info);
        }
        // reuse filename map structure
        auto pair =
                filename_map.emplace(src_segment_filename, std::pair(new_segment_filename, std::move(encryption_info)));
        if (!pair.second) {
            return Status::Corruption("Duplicated segment file: " + pair.first->first);
        }

        // build segment_name_to_size_map
        if (segment_size_size > 0) {
            auto segment_size = src_rowset_meta.segment_size(i);
            segment_name_to_size_map.emplace(new_segment_filename, segment_size);
        }
    }

    // Convert dels
    for (int i = 0; i < src_rowset_meta.del_files_size(); ++i) {
        DelfileWithRowsetId src_del = src_rowset_meta.del_files(i);

        auto src_del_filename = src_del.name();
        auto new_del_filename = gen_del_filename_from(txn_id, src_del_filename);
        if (UNLIKELY(new_del_filename.empty())) {
            return Status::Corruption("Failed to generate new del filename from: " + src_del_filename);
        }

        auto* new_del = new_rowset_meta->add_del_files();
        new_del->CopyFrom(src_del);
        new_del->set_name(new_del_filename);

        // build file_locations map
        if (existed_filename_uuids.count(extract_uuid_from(src_del_filename)) == 0) {
            // not existed, means need to replicate from source to target, add it to file_locations map
            auto target_del_path = _tablet_manager->segment_location(target_tablet_id, new_del_filename);
            file_locations.emplace(join_path(src_data_dir, src_del_filename), target_del_path);
        }

        // build filename_map
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            encryption_info = std::move(pair.info);
        }
        // reuse filename map structure
        auto pair = filename_map.emplace(src_del_filename, std::pair(new_del_filename, std::move(encryption_info)));
        if (!pair.second) {
            return Status::Corruption("Duplicated del file: " + pair.first->first);
        }
    }

    return Status::OK();
}

Status ReplicationTxnManager::convert_lake_replicate_sstable_meta(
        const std::shared_ptr<TabletMetadataPB>& new_metadata, const TabletMetadataPtr& src_current_tablet_meta,
        int64_t txn_id, int64_t target_tablet_id, const std::string& src_data_dir,
        const std::unordered_set<std::string>& existed_filename_uuids,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>& filename_map) {
    if (!src_current_tablet_meta->has_sstable_meta()) {
        return Status::OK();
    }

    PersistentIndexSstableMetaPB* dest_meta = new_metadata->mutable_sstable_meta();
    dest_meta->CopyFrom(src_current_tablet_meta->sstable_meta());

    for (int i = 0; i < dest_meta->sstables_size(); ++i) {
        PersistentIndexSstablePB* sst = dest_meta->mutable_sstables(i);
        const auto& src_sst_filename = sst->filename();
        auto new_sst_filename = gen_sst_filename_from(txn_id, src_sst_filename);
        sst->set_filename(new_sst_filename);

        // build file_locations map
        if (existed_filename_uuids.count(extract_uuid_from(src_sst_filename)) == 0) {
            // not existed, means need to replicate from source to target, add it to file_locations map
            auto target_sst_path = _tablet_manager->segment_location(target_tablet_id, new_sst_filename);
            file_locations.emplace(join_path(src_data_dir, src_sst_filename), target_sst_path);
        }

        // build filename_map
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            encryption_info = std::move(pair.info);
        }
        // reuse filename map structure
        auto pair = filename_map.emplace(src_sst_filename, std::pair(new_sst_filename, std::move(encryption_info)));
        if (!pair.second) {
            return Status::Corruption("Duplicated sst file: " + pair.first->first);
        }
    }
    return Status::OK();
}

FileConverterCreatorFunc ReplicationTxnManager::build_file_converters(
        const TReplicateSnapshotRequest& request,
        const std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>& filename_map,
        std::unordered_map<uint32_t, uint32_t>& column_unique_id_map, std::vector<std::string>& files_to_delete) const {
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
    return file_converters;
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

    auto file_converters = build_file_converters(request, filename_map, column_unique_id_map, files_to_delete);
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
} // namespace starrocks::lake
