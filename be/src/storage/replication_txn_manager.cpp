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

#include "storage/replication_txn_manager.h"

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
#include "storage/protobuf_file.h"
#include "storage/replication_utils.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/snapshot_manager.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "util/defer_op.h"
#include "util/string_parser.hpp"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

static string version_list_to_string(const std::vector<Version>& versions) {
    std::ostringstream str;
    size_t last = 0;
    for (size_t i = last + 1; i <= versions.size(); i++) {
        if (i == versions.size() || versions[last].second + 1 != versions[i].first) {
            if (versions[last].first == versions[i - 1].second) {
                str << versions[last].first << ",";
            } else {
                str << versions[last].first << "-" << versions[i - 1].second << ",";
            }
            last = i;
        }
    }
    return str.str();
}

static std::string get_txn_dir_path(DataDir* data_dir, TTransactionId transaction_id) {
    return fmt::format("{}/{}/", data_dir->get_replication_path(), transaction_id);
}

static std::string get_tablet_txn_dir_path(DataDir* data_dir, TTransactionId transaction_id, TPartitionId partition_id,
                                           TTabletId tablet_id) {
    return fmt::format("{}/{}/{}/{}/", data_dir->get_replication_path(), transaction_id, partition_id, tablet_id);
}

static std::string get_tablet_snapshot_dir_path(DataDir* data_dir, TTransactionId transaction_id,
                                                TPartitionId partition_id, TTabletId tablet_id) {
    return fmt::format("{}/{}/{}/{}/snapshot/", data_dir->get_replication_path(), transaction_id, partition_id,
                       tablet_id);
}

static std::string get_tablet_txn_meta_file_path(DataDir* data_dir, TTransactionId transaction_id,
                                                 TPartitionId partition_id, TTabletId tablet_id) {
    return fmt::format("{}/{}/{}/{}/txn_meta", data_dir->get_replication_path(), transaction_id, partition_id,
                       tablet_id);
}

static std::string get_tablet_txn_meta_file_path(const std::string& tablet_txn_dir_path) {
    return tablet_txn_dir_path + "txn_meta";
}

Status ReplicationTxnManager::remote_snapshot(const TRemoteSnapshotRequest& request, std::string* src_snapshot_path,
                                              bool* incremental_snapshot) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(request.tablet_id));

    ReplicationTxnMetaPB txn_meta_pb;
    Status status = load_tablet_txn_meta(tablet->data_dir(), request.transaction_id, request.partition_id,
                                         request.tablet_id, txn_meta_pb);
    if (status.ok()) {
        if (txn_meta_pb.txn_state() >= ReplicationTxnStatePB::TXN_SNAPSHOTED &&
            txn_meta_pb.snapshot_version() == request.src_visible_version) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already made remote snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version
                      << ", snapshot_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    // TODO: Primary key
    std::vector<Version> missed_versions;
    tablet->calc_missed_versions(request.src_visible_version, &missed_versions);
    if (missed_versions.empty()) {
        LOG(WARNING) << "Remote snapshot tablet skipped, no missing version"
                     << ", type: " << KeysType_Name(tablet->keys_type()) << ", txn_id: " << request.transaction_id
                     << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                     << ", src_tablet_id: " << request.src_tablet_id << ", visible_version: " << request.visible_version
                     << ", snapshot_version: " << request.src_visible_version;
        return Status::Corruption("No missing version");
    }

    LOG(INFO) << "Remote snapshot tablet. "
              << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
              << ", type: " << KeysType_Name(tablet->keys_type()) << ", src_tablet_id: " << request.src_tablet_id
              << ", visible version: " << request.visible_version
              << ", snapshot version: " << request.src_visible_version
              << ", missed_versions=" << version_list_to_string(missed_versions);

    TBackend src_backend;
    *incremental_snapshot = true;
    status = make_remote_snapshot(request, &missed_versions, nullptr, &src_backend, src_snapshot_path);
    if (!status.ok()) {
        LOG(INFO) << "Fail to make incremental snapshot: " << status << ", txn_id: " << request.transaction_id
                  << ", switch to fully snapshot. tablet_id: " << request.tablet_id
                  << ", src_tablet_id: " << request.src_tablet_id << ", visible version: " << request.visible_version
                  << ", snapshot version: " << request.src_visible_version;
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

    txn_meta_pb.set_txn_id(request.transaction_id);
    txn_meta_pb.set_txn_state(ReplicationTxnStatePB::TXN_SNAPSHOTED);
    txn_meta_pb.set_tablet_id(request.tablet_id);
    txn_meta_pb.set_visible_version(request.visible_version);
    txn_meta_pb.set_src_backend_host(src_backend.host);
    txn_meta_pb.set_src_backend_port(src_backend.be_port);
    txn_meta_pb.set_src_snapshot_path(*src_snapshot_path);
    txn_meta_pb.set_snapshot_version(request.src_visible_version);
    txn_meta_pb.set_incremental_snapshot(*incremental_snapshot);

    return save_tablet_txn_meta(tablet->data_dir(), request.transaction_id, request.partition_id, request.tablet_id,
                                txn_meta_pb);
}

Status ReplicationTxnManager::replicate_snapshot(const TReplicateSnapshotRequest& request) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(request.tablet_id));

    ReplicationTxnMetaPB txn_meta_pb;
    Status status = load_tablet_txn_meta(tablet->data_dir(), request.transaction_id, request.partition_id,
                                         request.tablet_id, txn_meta_pb);
    if (status.ok()) {
        if (txn_meta_pb.txn_state() >= ReplicationTxnStatePB::TXN_REPLICATED &&
            txn_meta_pb.snapshot_version() == request.src_visible_version) {
            LOG(INFO) << "Tablet " << request.tablet_id << " already replicated remote snapshot"
                      << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                      << ", src_tablet_id: " << request.src_tablet_id
                      << ", visible_version: " << request.visible_version
                      << ", snapshot_version: " << request.src_visible_version;
            return Status::OK();
        }
    }

    std::string tablet_snapshot_dir_path = get_tablet_snapshot_dir_path(tablet->data_dir(), request.transaction_id,
                                                                        request.partition_id, request.tablet_id);
    for (const auto& src_snapshot_info : request.src_snapshot_infos) {
        // Check local path exist, if exist, remove it, then create the dir
        RETURN_IF_ERROR(fs::remove_all(tablet_snapshot_dir_path));
        RETURN_IF_ERROR(fs::create_directories(tablet_snapshot_dir_path));

        const TBackend& src_be = src_snapshot_info.backend;
        const std::string& src_snapshot_path = src_snapshot_info.snapshot_path;

        status = ReplicationUtils::download_remote_snapshot(
                src_be.host, src_be.http_port, request.src_token, src_snapshot_path, request.src_tablet_id,
                request.src_schema_hash, tablet->data_dir(), tablet_snapshot_dir_path);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to download snapshot from " << src_be.host << ":" << src_be.http_port << ":"
                         << src_snapshot_path << ", " << status << ", txn_id: " << request.transaction_id
                         << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        status = convert_tablet_meta_file(tablet_snapshot_dir_path, request);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to convert tablet meta file: " << status << ", txn_id: " << request.transaction_id
                         << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        status = SnapshotManager::instance()->convert_rowset_ids(tablet_snapshot_dir_path, request.tablet_id,
                                                                 request.schema_hash);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to convert rowset ids: " << status << ", txn_id: " << request.transaction_id
                         << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        txn_meta_pb.set_txn_id(request.transaction_id);
        txn_meta_pb.set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
        txn_meta_pb.set_tablet_id(request.tablet_id);
        txn_meta_pb.set_visible_version(request.visible_version);
        txn_meta_pb.set_snapshot_version(request.src_visible_version);
        txn_meta_pb.set_incremental_snapshot(src_snapshot_info.incremental_snapshot);
        status = save_tablet_txn_meta(tablet->data_dir(), request.transaction_id, request.partition_id,
                                      request.tablet_id, txn_meta_pb);
        RETURN_IF_ERROR(status);

        LOG(INFO) << "Replicated snapshot from " << src_be.host << ":" << src_be.http_port << ":" << src_snapshot_path
                  << " to " << tablet_snapshot_dir_path << ", txn_id: " << request.transaction_id
                  << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                  << ", visible_version: " << request.visible_version
                  << ", snapshot_version: " << request.src_visible_version;
        break;
    }

    return status;
}

Status ReplicationTxnManager::get_txn_related_tablets(const TTransactionId transaction_id, TPartitionId partition_id,
                                                      std::vector<TTabletId>* tablet_ids) {
    for (DataDir* data_dir : StorageEngine::instance()->get_stores()) {
        std::string txn_dir_path = get_txn_dir_path(data_dir, transaction_id);
        std::string partition_dir_path = txn_dir_path + std::to_string(partition_id) + '/';
        if (!fs::path_exist(partition_dir_path)) {
            continue;
        }

        std::set<std::string> tablet_dirs;
        Status status = fs::list_dirs_files(partition_dir_path, &tablet_dirs, nullptr);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to list partition dir: " << partition_dir_path << ", " << status
                         << ", txn_id: " << transaction_id;
            return status;
        }

        for (const std::string& tablet_dir : tablet_dirs) {
            TTabletId tablet_id = ::atoll(tablet_dir.c_str());
            if (tablet_id == 0) {
                LOG(WARNING) << "Invalid tablet dir name: " << tablet_dir << " in partition dir: " << partition_dir_path
                             << ", txn_id: " << transaction_id;
                return Status::InternalError("Invalid tablet dir name: " + tablet_dir);
            }
            tablet_ids->push_back(tablet_id);
        }
    }
    return Status::OK();
}

Status ReplicationTxnManager::publish_txn(TTransactionId transaction_id, TPartitionId partition_id,
                                          const TabletSharedPtr& tablet, int64_t version) {
    ReplicationTxnMetaPB txn_meta_pb;
    RETURN_IF_ERROR(
            load_tablet_txn_meta(tablet->data_dir(), transaction_id, partition_id, tablet->tablet_id(), txn_meta_pb));
    if (txn_meta_pb.txn_state() == ReplicationTxnStatePB::TXN_PUBLISHED) {
        return Status::OK();
    }

    if (txn_meta_pb.txn_state() != ReplicationTxnStatePB::TXN_REPLICATED) {
        LOG(WARNING) << "Fail to publish snapshot, invalid txn meta state, tablet_id: " << tablet->tablet_id()
                     << ", partition_id: " << partition_id << ", txn_id: " << transaction_id
                     << ", txn state: " << ReplicationTxnStatePB_Name(txn_meta_pb.txn_state());
        return Status::Corruption("Invalid txn meta state: " + ReplicationTxnStatePB_Name(txn_meta_pb.txn_state()));
    }

    if (txn_meta_pb.snapshot_version() != version) {
        LOG(WARNING) << "Fail to publish snapshot, missmatched version, tablet_id: " << tablet->tablet_id()
                     << ", partition_id: " << partition_id << ", txn_id: " << transaction_id << ", version: " << version
                     << ", snapshot version: " << txn_meta_pb.snapshot_version();
        return Status::Corruption("Missmatched version");
    }

    std::string snapshot_dir_path =
            get_tablet_snapshot_dir_path(tablet->data_dir(), transaction_id, partition_id, tablet->tablet_id());

    return publish_snapshot(tablet.get(), snapshot_dir_path, version, txn_meta_pb.incremental_snapshot());
}

void ReplicationTxnManager::clear_expired_snapshots() {
    int64_t min_active_txn_id = get_master_info().min_active_txn_id;

    for (DataDir* data_dir : StorageEngine::instance()->get_stores()) {
        std::string replication_path = data_dir->get_replication_path();
        std::set<std::string> txn_dirs;
        Status status = fs::list_dirs_files(replication_path, &txn_dirs, nullptr);
        if (!status.ok()) {
            continue;
        }

        for (const std::string& txn_dir : txn_dirs) {
            int64_t txn_id = ::atoll(txn_dir.c_str());
            if (txn_id != 0 && txn_id < min_active_txn_id) {
                clear_txn_snapshots(txn_id);
            }
        }
    }
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
            LOG(WARNING) << "Fail to make snapshot from " << src_be.host << ", " << status
                         << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                         << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            continue;
        }

        *src_backend = src_be;
        LOG(INFO) << "Made snapshot from " << src_be.host << ", txn_id: " << request.transaction_id
                  << ", tablet_id: " << request.tablet_id << ", src_tablet_id: " << request.src_tablet_id
                  << ", visible_version: " << request.visible_version
                  << ", snapshot_version: " << request.src_visible_version;
        break;
    }

    return status;
}

Status ReplicationTxnManager::convert_tablet_meta_file(const std::string& tablet_snapshot_path,
                                                       const TReplicateSnapshotRequest& request) {
    std::string src_header_file_path = tablet_snapshot_path + std::to_string(request.src_tablet_id) + ".hdr";
    if (fs::path_exist(src_header_file_path)) {
        TabletMeta tablet_meta;
        Status status = tablet_meta.create_from_file(src_header_file_path);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to load tablet meta from " << src_header_file_path << ", " << status
                         << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                         << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            return status;
        }

        TabletMetaPB tablet_meta_pb;
        tablet_meta.to_meta_pb(&tablet_meta_pb);
        tablet_meta_pb.set_table_id(request.table_id);
        tablet_meta_pb.set_partition_id(request.partition_id);
        tablet_meta_pb.set_tablet_id(request.tablet_id);

        std::string header_file_path = tablet_snapshot_path + std::to_string(request.tablet_id) + ".hdr";
        status = TabletMeta::save(header_file_path, tablet_meta_pb);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to save tablet meta pb to " << header_file_path << ", " << status
                         << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                         << ", src_tablet_id: " << request.src_tablet_id
                         << ", visible_version: " << request.visible_version
                         << ", snapshot_version: " << request.src_visible_version;
            return status;
        }

        if (request.tablet_id != request.src_tablet_id) {
            status = fs::remove(src_header_file_path);
            if (!status.ok()) {
                LOG(WARNING) << "Fail to remove tablet meta " << src_header_file_path << ", " << status
                             << ", txn_id: " << request.transaction_id << ", tablet_id: " << request.tablet_id
                             << ", src_tablet_id: " << request.src_tablet_id
                             << ", visible_version: " << request.visible_version
                             << ", snapshot_version: " << request.src_visible_version;
            }
        }

        return status;
    }

    // TODO: convert meta file and dcgs_snapshot file
    return Status::OK();
}

Status ReplicationTxnManager::publish_snapshot(Tablet* tablet, const string& snapshot_dir, int64_t snapshot_version,
                                               bool incremental_snapshot) {
    if (tablet->updates() != nullptr) {
        return Status::NotSupported("Pk table not supported");
    }

    Status res;
    std::vector<std::string> linked_success_files;

    // clone and compaction operation should be performed sequentially
    tablet->obtain_base_compaction_lock();
    DeferOp base_compaction_lock_release_guard([&tablet]() { tablet->release_base_compaction_lock(); });

    tablet->obtain_cumulative_lock();
    DeferOp cumulative_lock_release_guard([&tablet]() { tablet->release_cumulative_lock(); });

    tablet->obtain_push_lock();
    DeferOp push_lock_release_guard([&tablet]() { tablet->release_push_lock(); });

    tablet->obtain_header_wrlock();
    DeferOp header_wrlock_release_guard([&tablet]() { tablet->release_header_lock(); });

    do {
        // load src header
        std::string header_file = strings::Substitute("$0/$1.hdr", snapshot_dir, tablet->tablet_id());
        std::string dcgs_snapshot_file = strings::Substitute("$0/$1.dcgs_snapshot", snapshot_dir, tablet->tablet_id());
        TabletMeta cloned_tablet_meta;
        res = cloned_tablet_meta.create_from_file(header_file);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to load load tablet meta from " << header_file;
            break;
        }

        DeltaColumnGroupSnapshotPB dcg_snapshot_pb;
        bool has_dcgs_snapshot_file = fs::path_exist(dcgs_snapshot_file);
        if (has_dcgs_snapshot_file) {
            res = DeltaColumnGroupListHelper::parse_snapshot(dcgs_snapshot_file, dcg_snapshot_pb);
            if (!res.ok()) {
                LOG(WARNING) << "Fail to load load dcg snapshot from " << dcgs_snapshot_file;
                break;
            }
        }

        std::set<std::string> clone_files;
        res = fs::list_dirs_files(snapshot_dir, nullptr, &clone_files);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to list directory " << snapshot_dir << ": " << res;
            break;
        }

        clone_files.erase(strings::Substitute("$0.hdr", tablet->tablet_id()));
        if (has_dcgs_snapshot_file) {
            clone_files.erase(strings::Substitute("$0.dcgs_snapshot", tablet->tablet_id()));
        }

        std::set<string> local_files;
        std::string tablet_dir = tablet->schema_hash_path();
        res = fs::list_dirs_files(tablet_dir, nullptr, &local_files);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to list tablet directory " << tablet_dir << ": " << res;
            break;
        }

        // link files from clone dir, if file exists, skip it
        for (const string& clone_file : clone_files) {
            if (local_files.find(clone_file) != local_files.end()) {
                VLOG(3) << "find same file when clone, skip it. "
                        << "tablet=" << tablet->full_name() << ", clone_file=" << clone_file;
                continue;
            }

            std::string from = strings::Substitute("$0/$1", snapshot_dir, clone_file);
            std::string to = strings::Substitute("$0/$1", tablet_dir, clone_file);
            res = FileSystem::Default()->link_file(from, to);
            if (!res.ok()) {
                LOG(WARNING) << "Fail to link " << from << " to " << to << ": " << res;
                break;
            }
            linked_success_files.emplace_back(std::move(to));
        }
        if (!res.ok()) {
            break;
        }
        LOG(INFO) << "Linked " << clone_files.size() << " files from " << snapshot_dir << " to " << tablet_dir;

        std::vector<RowsetMetaSharedPtr> rs_to_clone;
        if (incremental_snapshot) {
            res = publish_incremental_meta(tablet, cloned_tablet_meta, snapshot_version);
        } else {
            res = publish_full_meta(tablet, &cloned_tablet_meta, rs_to_clone);
        }

        // if full clone success, need to update cumulative layer point
        if (!incremental_snapshot && res.ok()) {
            tablet->set_cumulative_layer_point(-1);
        }

        // recover dcg meta
        if (has_dcgs_snapshot_file && rs_to_clone.size() != 0) {
            auto data_dir = tablet->data_dir();
            rocksdb::WriteBatch wb;
            for (const auto& rs_meta : rs_to_clone) {
                int idx = 0;
                for (const auto& rowset_id : dcg_snapshot_pb.rowset_id()) {
                    if (rowset_id != rs_meta->rowset_id().to_string()) {
                        ++idx;
                        continue;
                    }
                    // dcgs for each segment
                    auto& dcg_list_pb = dcg_snapshot_pb.dcg_lists(idx);
                    DeltaColumnGroupList dcgs;
                    RETURN_IF_ERROR(
                            DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(dcg_list_pb, &dcgs));

                    if (dcgs.size() == 0) {
                        continue;
                    }

                    RETURN_IF_ERROR(TabletMetaManager::put_delta_column_group(
                            data_dir, &wb, dcg_snapshot_pb.tablet_id(idx), dcg_snapshot_pb.rowset_id(idx),
                            dcg_snapshot_pb.segment_id(idx), dcgs));
                    ++idx;
                }
            }
            res = data_dir->get_meta()->write_batch(&wb);
            if (!res.ok()) {
                std::stringstream ss;
                ss << "save dcgs meta failed, tablet id: " << tablet->tablet_id();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
        }
    } while (false);

    // clear linked files if errors happen
    if (!res.ok()) {
        (void)fs::remove(linked_success_files);
    }

    return res;
}

Status ReplicationTxnManager::publish_incremental_meta(Tablet* tablet, const TabletMeta& cloned_tablet_meta,
                                                       int64_t snapshot_version) {
    LOG(INFO) << "begin to publish incremental meta. tablet=" << tablet->full_name()
              << ", snapshot_version=" << snapshot_version;

    std::vector<Version> missed_versions;
    tablet->calc_missed_versions_unlocked(snapshot_version, &missed_versions);

    std::vector<Version> versions_to_delete;
    std::vector<RowsetMetaSharedPtr> rowsets_to_clone;

    VLOG(3) << "get missed versions again when publish incremental meta. "
            << "tablet=" << tablet->full_name() << ", snapshot_version=" << snapshot_version
            << ", missed_versions_size=" << missed_versions.size();

    // check missing versions exist in clone src
    for (Version version : missed_versions) {
        RowsetMetaSharedPtr inc_rs_meta = cloned_tablet_meta.acquire_inc_rs_meta_by_version(version);
        if (inc_rs_meta == nullptr) {
            LOG(WARNING) << "missed version is not found in cloned tablet meta."
                         << ", missed_version=" << version.first << "-" << version.second;
            return Status::NotFound(strings::Substitute("version not found"));
        }

        rowsets_to_clone.push_back(inc_rs_meta);
    }

    // clone_data to tablet
    Status st = tablet->revise_tablet_meta(rowsets_to_clone, versions_to_delete);
    LOG(INFO) << "finish to publish incremental meta. [tablet=" << tablet->full_name() << ", status=" << st << "]";
    return st;
}

Status ReplicationTxnManager::publish_full_meta(Tablet* tablet, TabletMeta* cloned_tablet_meta,
                                                std::vector<RowsetMetaSharedPtr>& rs_to_clone) {
    Version cloned_max_version = cloned_tablet_meta->max_version();
    LOG(INFO) << "begin to publish full meta. tablet=" << tablet->full_name()
              << ", cloned_max_version=" << cloned_max_version.first << "-" << cloned_max_version.second;
    std::vector<Version> versions_to_delete;
    std::vector<RowsetMetaSharedPtr> rs_metas_found_in_src;
    // check local versions
    for (auto& rs_meta : tablet->tablet_meta()->all_rs_metas()) {
        Version local_version(rs_meta->start_version(), rs_meta->end_version());
        LOG(INFO) << "check local delta when publish full snapshot."
                  << "tablet=" << tablet->full_name() << ", local_version=" << local_version.first << "-"
                  << local_version.second;

        // if local version cross src latest, clone failed
        // if local version is : 0-0, 1-1, 2-10, 12-14, 15-15,16-16
        // cloned max version is 13-13, this clone is failed, because could not
        // fill local data by using cloned data.
        // It should not happen because if there is a hole, the following delta will not
        // do compaction.
        if (local_version.first <= cloned_max_version.second && local_version.second > cloned_max_version.second) {
            LOG(WARNING) << "stop to publish full snapshot, version cross src latest."
                         << "tablet=" << tablet->full_name() << ", local_version=" << local_version.first << "-"
                         << local_version.second;
            return Status::InternalError("clone version conflict with local version");

        } else if (local_version.second <= cloned_max_version.second) {
            // if local version smaller than src, check if existed in src, will not clone it
            bool existed_in_src = false;

            // if delta labeled with local_version is same with the specified version in clone header,
            // there is no necessity to clone it.
            for (auto& rs_meta : cloned_tablet_meta->all_rs_metas()) {
                if (rs_meta->version().first == local_version.first &&
                    rs_meta->version().second == local_version.second) {
                    existed_in_src = true;
                    break;
                }
            }

            if (existed_in_src) {
                cloned_tablet_meta->delete_rs_meta_by_version(local_version, &rs_metas_found_in_src);
                LOG(INFO) << "Delta has already existed in local header, no need to clone."
                          << "tablet=" << tablet->full_name() << ", version='" << local_version.first << "-"
                          << local_version.second;
            } else {
                // Delta labeled in local_version is not existed in clone header,
                // some overlapping delta will be cloned to replace it.
                // And also, the specified delta should deleted from local header.
                versions_to_delete.push_back(local_version);
                LOG(INFO) << "Delete delta not included by the clone header, should delete it from "
                             "local header."
                          << "tablet=" << tablet->full_name() << ","
                          << ", version=" << local_version.first << "-" << local_version.second;
            }
        }
    }

    std::vector<RowsetMetaSharedPtr> rowsets_to_clone;
    for (auto& rs_meta : cloned_tablet_meta->all_rs_metas()) {
        rowsets_to_clone.push_back(rs_meta);
        LOG(INFO) << "Delta to clone."
                  << "tablet=" << tablet->full_name() << ", version=" << rs_meta->version().first << "-"
                  << rs_meta->version().second;
    }
    rs_to_clone = rowsets_to_clone;

    // clone_data to tablet
    Status st = tablet->revise_tablet_meta(rowsets_to_clone, versions_to_delete);
    LOG(INFO) << "finish to full clone. tablet=" << tablet->full_name() << ", res=" << st;
    // in previous step, copy all files from CLONE_DIR to tablet dir
    // but some rowset is useless, so that remove them here
    for (auto& rs_meta_ptr : rs_metas_found_in_src) {
        RowsetSharedPtr rowset_to_remove;
        if (auto s = RowsetFactory::create_rowset(cloned_tablet_meta->tablet_schema_ptr(), tablet->schema_hash_path(),
                                                  rs_meta_ptr, &rowset_to_remove);
            !s.ok()) {
            LOG(WARNING) << "failed to init rowset to remove: " << rs_meta_ptr->rowset_id().to_string();
            continue;
        }
        if (auto ost = rowset_to_remove->remove(); !ost.ok()) {
            LOG(WARNING) << "failed to remove rowset " << rs_meta_ptr->rowset_id().to_string() << ", res=" << ost;
        }
    }
    return st;
}

void ReplicationTxnManager::clear_txn_snapshots(TTransactionId transaction_id) {
    for (DataDir* data_dir : StorageEngine::instance()->get_stores()) {
        std::string txn_dir_path = get_txn_dir_path(data_dir, transaction_id);
        if (!fs::path_exist(txn_dir_path)) {
            continue;
        }

        std::set<std::string> partition_dirs;
        Status status = fs::list_dirs_files(txn_dir_path, &partition_dirs, nullptr);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to list txn dir: " << txn_dir_path << ", " << status
                         << ", txn_id: " << transaction_id;
            continue;
        }

        for (const std::string& partition_dir : partition_dirs) {
            std::string partition_dir_path = txn_dir_path + partition_dir + '/';
            std::set<std::string> tablet_dirs;
            status = fs::list_dirs_files(partition_dir_path, &tablet_dirs, nullptr);
            if (!status.ok()) {
                LOG(WARNING) << "Fail to list partition dir: " << partition_dir_path << ", " << status
                             << ", txn_id: " << transaction_id;
                continue;
            }

            for (const std::string& tablet_dir : tablet_dirs) {
                std::string tablet_dir_path = partition_dir_path + tablet_dir + '/';
                ReplicationTxnMetaPB txn_meta_pb;
                status = load_tablet_txn_meta(tablet_dir_path, txn_meta_pb);
                if (!status.ok()) {
                    continue;
                }

                const std::string& src_backend_host = txn_meta_pb.src_backend_host();
                int32_t src_backend_port = txn_meta_pb.src_backend_port();
                const std::string& src_snapshot_path = txn_meta_pb.src_snapshot_path();
                if (src_backend_host.empty() || src_backend_port == 0 || src_snapshot_path.empty()) {
                    continue;
                }

                (void)ReplicationUtils::release_remote_snapshot(src_backend_host, src_backend_port, src_snapshot_path);
            }
        }

        status = fs::remove_all(txn_dir_path);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to remove txn dir: " << txn_dir_path << ", " << status
                         << ", txn_id: " << transaction_id;
        } else {
            LOG(INFO) << "Removed txn dir: " << txn_dir_path << ", txn_id: " << transaction_id;
        }
    }
}

Status ReplicationTxnManager::save_tablet_txn_meta(DataDir* data_dir, TTransactionId transaction_id,
                                                   TPartitionId partition_id, TTabletId tablet_id,
                                                   const ReplicationTxnMetaPB& txn_meta) {
    std::string tablet_txn_dir_path = get_tablet_txn_dir_path(data_dir, transaction_id, partition_id, tablet_id);
    if (!fs::path_exist(tablet_txn_dir_path)) {
        Status status = fs::create_directories(tablet_txn_dir_path);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to create directory " << tablet_txn_dir_path << ", " << status
                         << ", txn_id: " << transaction_id;
            return status;
        }
    }

    return save_tablet_txn_meta(tablet_txn_dir_path, txn_meta);
}

Status ReplicationTxnManager::save_tablet_txn_meta(const std::string& tablet_txn_dir_path,
                                                   const ReplicationTxnMetaPB& txn_meta) {
    std::string tablet_txn_meta_file_path = get_tablet_txn_meta_file_path(tablet_txn_dir_path);
    std::string tmp_tablet_txn_meta_file_path = tablet_txn_meta_file_path + ".temp";
    ProtobufFileWithHeader file(tmp_tablet_txn_meta_file_path);
    Status status = file.save(txn_meta, true);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to save txn meta to " << tmp_tablet_txn_meta_file_path << ", " << status;
        return status;
    }

    if (0 != rename(tmp_tablet_txn_meta_file_path.c_str(), tablet_txn_meta_file_path.c_str())) {
        LOG(WARNING) << "Fail to rename txn meta file from " << tmp_tablet_txn_meta_file_path << " to "
                     << tablet_txn_meta_file_path << ", " << strerror(errno);
        return Status::IOError(strerror(errno));
    }

    return Status::OK();
}

Status ReplicationTxnManager::load_tablet_txn_meta(DataDir* data_dir, TTransactionId transaction_id,
                                                   TPartitionId partition_id, TTabletId tablet_id,
                                                   ReplicationTxnMetaPB& txn_meta) {
    std::string tablet_txn_meta_file_path =
            get_tablet_txn_meta_file_path(data_dir, transaction_id, partition_id, tablet_id);
    ProtobufFileWithHeader file(tablet_txn_meta_file_path);
    return file.load(&txn_meta);
}

Status ReplicationTxnManager::load_tablet_txn_meta(const std::string& tablet_txn_dir_path,
                                                   ReplicationTxnMetaPB& txn_meta) {
    std::string tablet_txn_meta_file_path = get_tablet_txn_meta_file_path(tablet_txn_dir_path);
    ProtobufFileWithHeader file(tablet_txn_meta_file_path);
    Status status = file.load(&txn_meta);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to load txn meta from " << tablet_txn_meta_file_path << ", " << status;
    }
    return status;
}

StatusOr<TabletSharedPtr> ReplicationTxnManager::get_tablet(TTabletId tablet_id) {
    auto tablet_manager = StorageEngine::instance()->tablet_manager();
    std::string error_msg;
    auto tablet = tablet_manager->get_tablet(tablet_id, false, &error_msg);
    if (tablet == nullptr) {
        LOG(WARNING) << "Cannot get tablet " << tablet_id << ", " << error_msg;
        return Status::NotFound(error_msg);
    }
    return tablet;
}

} // namespace starrocks
