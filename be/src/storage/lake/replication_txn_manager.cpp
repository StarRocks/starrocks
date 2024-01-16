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
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
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

Status ReplicationTxnManager::remote_snapshot(const TRemoteSnapshotRequest& request, std::string* src_snapshot_path,
                                              bool* incremental_snapshot) {
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(request.tablet_id));

    auto status_or_txn_log = tablet.get_txn_slog(request.transaction_id);
    if (status_or_txn_log.ok()) {
        const ReplicationTxnMetaPB& txn_meta = status_or_txn_log.value()->op_replication().txn_meta();
        if (txn_meta.txn_state() == ReplicationTxnStatePB::TXN_SNAPSHOTED &&
            txn_meta.snapshot_version() == request.src_visible_version) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already made remote snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version
                      << ", snapshot_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    std::vector<Version> missed_versions;
    for (auto v = request.visible_version + 1; v <= request.src_visible_version; ++v) {
        missed_versions.emplace_back(Version(v, v));
    }
    if (missed_versions.empty()) {
        LOG(WARNING) << "Remote snapshot tablet skipped, no missing version"
                     << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                     << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
                     << ", snapshot_version: " << request.src_visible_version;
        return Status::Corruption("No missing version");
    }

    LOG(INFO) << "Start make remote snapshot tablet"
              << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
              << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
              << ", snapshot_version: " << request.src_visible_version << ", missed_versions: ["
              << (request.visible_version + 1) << " ... " << request.src_visible_version << "]";

    Status status;
    TBackend src_backend;
    if (request.visible_version <= 1) { // Make full snapshot
        *incremental_snapshot = false;
        status = make_remote_snapshot(request, nullptr, nullptr, &src_backend, src_snapshot_path);
    } else { // Try to make incremental snapshot first, if failed, make full snapshot
        *incremental_snapshot = true;
        status = make_remote_snapshot(request, &missed_versions, nullptr, &src_backend, src_snapshot_path);
        if (!status.ok()) {
            LOG(INFO) << "Fail to make incremental snapshot: " << status << ". switch to fully snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version
                      << ", snapshot_version: " << request.src_visible_version;
            *incremental_snapshot = false;
            status = make_remote_snapshot(request, nullptr, nullptr, &src_backend, src_snapshot_path);
        }
    }

    if (!status.ok()) {
        LOG(WARNING) << "Fail to make remote snapshot: " << status << ", txn_id: " << request.transaction_id
                     << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                     << ", visible_version: " << request.visible_version
                     << ", snapshot_version: " << request.src_visible_version;
        return status;
    }

    LOG(INFO) << "Made snapshot from " << src_backend.host << ":" << src_backend.be_port << ":" << *src_snapshot_path
              << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
              << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
              << ", snapshot_version: " << request.src_visible_version << ", is_incremental: " << *incremental_snapshot;

    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(request.tablet_id);
    txn_log->set_txn_id(request.transaction_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_txn_id(request.transaction_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_SNAPSHOTED);
    txn_meta->set_tablet_id(request.tablet_id);
    txn_meta->set_visible_version(request.visible_version);
    txn_meta->set_src_backend_host(src_backend.host);
    txn_meta->set_src_backend_port(src_backend.be_port);
    txn_meta->set_src_snapshot_path(*src_snapshot_path);
    txn_meta->set_snapshot_version(request.src_visible_version);
    txn_meta->set_incremental_snapshot(*incremental_snapshot);

    return tablet.put_txn_slog(std::move(txn_log));
}

Status ReplicationTxnManager::replicate_snapshot(const TReplicateSnapshotRequest& request) {
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(request.tablet_id));

    auto status_or_txn_log = tablet.get_txn_log(request.transaction_id);
    if (status_or_txn_log.ok()) {
        const ReplicationTxnMetaPB& txn_meta = status_or_txn_log.value()->op_replication().txn_meta();
        if (txn_meta.txn_state() >= ReplicationTxnStatePB::TXN_REPLICATED &&
            txn_meta.snapshot_version() == request.src_visible_version) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already replicated remote snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version
                      << ", snapshot_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    ASSIGN_OR_RETURN(auto tablet_metadata, tablet.get_metadata(request.visible_version));

    Status status;
    for (const auto& src_snapshot_info : request.src_snapshot_infos) {
        auto status_or = replicate_remote_snapshot(request, src_snapshot_info, tablet_metadata);

        if (!status_or.ok()) {
            status = status_or.status();
            LOG(WARNING) << "Fail to download snapshot from " << src_snapshot_info.backend.host << ":"
                         << src_snapshot_info.backend.http_port << ":" << src_snapshot_info.snapshot_path << ", "
                         << status << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                         << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        LOG(INFO) << "Replicated snapshot from " << src_snapshot_info.backend.host << ":"
                  << src_snapshot_info.backend.http_port << ":" << src_snapshot_info.snapshot_path << " to "
                  << _tablet_manager->location_provider()->segment_root_location(request.tablet_id)
                  << ", keys_type: " << KeysType_Name(tablet_metadata->schema().keys_type())
                  << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                  << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
                  << ", snapshot_version: " << request.src_visible_version;

        return tablet.put_txn_log(std::move(status_or.value()));
    }

    return status;
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

StatusOr<TxnLogPtr> ReplicationTxnManager::replicate_remote_snapshot(const TReplicateSnapshotRequest& request,
                                                                     const TRemoteSnapshotInfo& src_snapshot_info,
                                                                     const TabletMetadataPtr& tablet_metadata) {
    auto txn_log = std::make_shared<TxnLog>();
    std::unordered_map<std::string, std::string> filename_map;
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
            LOG(WARNING) << "Fail to parse remote snapshot header file: " << remote_header_file_name
                         << ", content: " << header_file_content << ", " << status;
            return status;
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
            LOG(WARNING) << "Fail to parse remote snapshot meta file: " << snapshot_meta_file_name
                         << ", content: " << snapshot_meta_content << ", " << status;
            return status;
        }

        CHECK(((src_snapshot_info.incremental_snapshot &&
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
    }

    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb->column(), tablet_metadata->schema().column(),
                                                &column_unique_id_map);

    auto file_converters = [&](const std::string& file_name,
                               uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
        auto iter = filename_map.find(file_name);
        if (iter == filename_map.end()) {
            return nullptr;
        }

        auto segment_location = _tablet_manager->segment_location(request.tablet_id, iter->second);
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto output_file, fs::new_writable_file(opts, segment_location));

        if (is_segment(file_name) && !column_unique_id_map.empty()) {
            return std::make_unique<SegmentStreamConverter>(file_name, file_size, std::move(output_file),
                                                            &column_unique_id_map);
        }
        return std::make_unique<FileStreamConverter>(file_name, file_size, std::move(output_file));
    };

    RETURN_IF_ERROR(ReplicationUtils::download_remote_snapshot(
            src_snapshot_info.backend.host, src_snapshot_info.backend.http_port, request.src_token,
            src_snapshot_info.snapshot_path, request.src_tablet_id, request.src_schema_hash, file_converters));

    txn_log->set_tablet_id(request.tablet_id);
    txn_log->set_txn_id(request.transaction_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_txn_id(request.transaction_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_tablet_id(request.tablet_id);
    txn_meta->set_visible_version(request.visible_version);
    txn_meta->set_src_backend_host(src_snapshot_info.backend.host);
    txn_meta->set_src_backend_port(src_snapshot_info.backend.be_port);
    txn_meta->set_src_snapshot_path(src_snapshot_info.snapshot_path);
    txn_meta->set_snapshot_version(request.src_visible_version);
    txn_meta->set_incremental_snapshot(src_snapshot_info.incremental_snapshot);

    return txn_log;
}

Status ReplicationTxnManager::convert_rowset_meta(const RowsetMeta& rowset_meta, TTransactionId transaction_id,
                                                  TxnLogPB::OpWrite* op_write,
                                                  std::unordered_map<std::string, std::string>* filename_map) {
    // Convert rowset metadata
    auto* rowset_metadata = op_write->mutable_rowset();
    rowset_metadata->set_id(rowset_meta.get_rowset_seg_id());
    rowset_metadata->set_overlapped(rowset_meta.is_segments_overlapping());
    rowset_metadata->set_num_rows(rowset_meta.num_rows());
    rowset_metadata->set_data_size(rowset_meta.data_disk_size());
    rowset_metadata->set_num_dels(rowset_meta.get_num_delete_files());
    if (rowset_meta.has_delete_predicate()) {
        rowset_metadata->mutable_delete_predicate()->CopyFrom(rowset_meta.delete_predicate());
    }

    std::string rowset_id = rowset_meta.rowset_id().to_string();
    for (int64_t segment_id = 0; segment_id < rowset_meta.num_segments(); ++segment_id) {
        std::string old_segment_filename = rowset_id + '_' + std::to_string(segment_id) + ".dat";
        std::string new_segment_filename = gen_segment_filename(transaction_id);

        rowset_metadata->add_segments(new_segment_filename);
        auto pair = filename_map->emplace(std::move(old_segment_filename), std::move(new_segment_filename));
        if (!pair.second) {
            return Status::Corruption("Duplicated segment file: " + pair.first->first);
        }
    }

    // Convert rowset txn meta
    auto* rowset_txn_meta = op_write->mutable_txn_meta();
    rowset_txn_meta->CopyFrom(rowset_meta.txn_meta());

    // Convert dels
    for (int64_t del_id = 0; del_id < rowset_meta.get_num_delete_files(); ++del_id) {
        std::string old_del_filename = rowset_id + '_' + std::to_string(del_id) + ".del";
        std::string new_del_filename = gen_del_filename(transaction_id);

        op_write->add_dels(new_del_filename);
        auto pair = filename_map->emplace(std::move(old_del_filename), std::move(new_del_filename));
        if (!pair.second) {
            return Status::Corruption("Duplicated del file: " + pair.first->first);
        }
    }

    return Status::OK();
}

} // namespace starrocks::lake
