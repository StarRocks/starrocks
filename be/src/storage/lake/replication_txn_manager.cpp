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
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/protobuf_file.h"
#include "storage/replication_utils.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
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

    TBackend src_backend;
    *incremental_snapshot = true;
    Status status = make_remote_snapshot(request, &missed_versions, nullptr, &src_backend, src_snapshot_path);
    if (!status.ok()) {
        LOG(INFO) << "Fail to make incremental snapshot: " << status << ". switch to fully snapshot"
                  << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                  << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
                  << ", snapshot_version: " << request.src_visible_version;
        *incremental_snapshot = false;
        status = make_remote_snapshot(request, nullptr, nullptr, &src_backend, src_snapshot_path);
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
              << ", snapshot_version: " << request.src_visible_version;

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

    Status status;
    for (const auto& src_snapshot_info : request.src_snapshot_infos) {
        auto status_or = replicate_remote_snapshot(request, src_snapshot_info);

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
            LOG(WARNING) << "Fail to make snapshot from " << src_be.host << ":" << src_be.be_port << ", " << status
                         << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                         << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        *src_backend = src_be;
        break;
    }

    return status;
}

StatusOr<TxnLogPtr> ReplicationTxnManager::replicate_remote_snapshot(const TReplicateSnapshotRequest& request,
                                                                     const TRemoteSnapshotInfo& src_snapshot_info) {
    std::string remote_header_file_name = std::to_string(request.src_tablet_id) + ".hdr";
    ASSIGN_OR_RETURN(auto header_file_content,
                     ReplicationUtils::download_remote_snapshot_file(
                             src_snapshot_info.backend.host, src_snapshot_info.backend.http_port, request.src_token,
                             src_snapshot_info.snapshot_path, request.src_tablet_id, request.src_schema_hash,
                             remote_header_file_name, config::download_low_speed_time));
    TabletMeta tablet_meta;
    RETURN_IF_ERROR(tablet_meta.create_from_memory(header_file_content));

    auto txn_log = std::make_shared<TxnLog>();
    std::unordered_map<std::string, std::string> segment_filename_map;
    const auto& rowset_metas =
            tablet_meta.all_rs_metas().empty() ? tablet_meta.all_inc_rs_metas() : tablet_meta.all_rs_metas();
    for (const auto& rowset_meta : rowset_metas) {
        auto* rowset_metadata = txn_log->mutable_op_replication()->add_op_writes()->mutable_rowset();
        RETURN_IF_ERROR(
                convert_rowset_meta(*rowset_meta, request.transaction_id, rowset_metadata, &segment_filename_map));
    }

    RETURN_IF_ERROR(ReplicationUtils::download_remote_snapshot(
            src_snapshot_info.backend.host, src_snapshot_info.backend.http_port, request.src_token,
            src_snapshot_info.snapshot_path, request.src_tablet_id, request.src_schema_hash, nullptr,
            _tablet_manager->location_provider()->segment_root_location(request.tablet_id) + '/',
            [segment_filename_map = std::move(segment_filename_map)](const std::string& filename) {
                auto iter = segment_filename_map.find(filename);
                if (iter == segment_filename_map.end()) {
                    return std::string();
                }
                return iter->second;
            }));

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
                                                  RowsetMetadata* rowset_metadata,
                                                  std::unordered_map<std::string, std::string>* segment_filename_map) {
    rowset_metadata->set_overlapped(rowset_meta.is_segments_overlapping());
    rowset_metadata->set_num_rows(rowset_meta.num_rows());
    rowset_metadata->set_data_size(rowset_meta.data_disk_size());
    if (rowset_meta.has_delete_predicate()) {
        rowset_metadata->mutable_delete_predicate()->CopyFrom(rowset_meta.delete_predicate());
    }

    std::string rowset_id = rowset_meta.rowset_id().to_string();
    for (int64_t segment_id = 0; segment_id < rowset_meta.num_segments(); ++segment_id) {
        std::string old_segment_filename = rowset_id + '_' + std::to_string(segment_id) + ".dat";
        std::string new_segment_filename = gen_segment_filename(transaction_id);

        rowset_metadata->add_segments(new_segment_filename);
        auto pair = segment_filename_map->emplace(std::move(old_segment_filename), std::move(new_segment_filename));
        if (!pair.second) {
            return Status::Corruption("Duplicated segment file: " + pair.first->first);
        }
    }

    return Status::OK();
}

} // namespace starrocks::lake
