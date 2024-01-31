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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_storage_migration_task.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/task/engine_storage_migration_task.h"

#include <fmt/format.h>

#include "runtime/exec_env.h"
#include "storage/replication_txn_manager.h"
#include "storage/snapshot_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/update_manager.h"
#include "util/defer_op.h"

namespace starrocks {

EngineStorageMigrationTask::EngineStorageMigrationTask(TTabletId tablet_id, TSchemaHash schema_hash,
                                                       DataDir* dest_store)
        : _tablet_id(tablet_id), _schema_hash(schema_hash), _dest_store(dest_store) {}

Status EngineStorageMigrationTask::execute() {
    StarRocksMetrics::instance()->storage_migrate_requests_total.increment(1);
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "Not found tablet: " << _tablet_id;
        return Status::NotFound(fmt::format("Not found tablet: {}", _tablet_id));
    }

    // check tablet data dir
    if (tablet->data_dir() == _dest_store) {
        LOG(INFO) << "Already existed path. tablet_id=" << _tablet_id << ", dest_store=" << _dest_store->path();
        return Status::OK();
    }

    // check disk capacity
    int64_t tablet_size = tablet->tablet_footprint();
    if (_dest_store->capacity_limit_reached(tablet_size)) {
        LOG(WARNING) << "No space left to migration. tablet_id: " << _tablet_id
                     << ", dest_path: " << _dest_store->path();
        return Status::IOError(fmt::format("No space left to migration. tablet_id: {}, dest_path: {}", _tablet_id,
                                           _dest_store->path()));
    }

    return _storage_migrate(tablet);
}

Status EngineStorageMigrationTask::_storage_migrate(TabletSharedPtr tablet) {
    bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    if (bg_worker_stopped) {
        LOG(WARNING) << "Process is going to quit. The migration will stop.";
        return Status::InternalError("Process is going to quit.");
    }

    Status res = Status::OK();
    LOG(INFO) << "begin to process storage migrate. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
              << ", tablet=" << tablet->full_name() << ", dest_store=" << _dest_store->path();

    // 1. lock and check
    int64_t end_version = -1;
    std::vector<RowsetSharedPtr> consistent_rowsets;
    uint64_t shard = 0;
    std::string schema_hash_path;
    {
        // lock
        std::unique_lock migration_wlock(tablet->get_migration_lock(), std::try_to_lock);
        if (!migration_wlock.owns_lock()) {
            return Status::InternalError("Fail to get lock");
        }
        if (tablet->is_migrating()) {
            LOG(WARNING) << "tablet is already migrating.";
            return Status::InternalError("tablet is already migrating.");
        }

        std::lock_guard push_lock(tablet->get_push_lock());

        // check load txns
        int64_t partition_id;
        std::set<int64_t> transaction_ids;
        StorageEngine::instance()->txn_manager()->get_tablet_related_txns(
                _tablet_id, _schema_hash, tablet->tablet_uid(), &partition_id, &transaction_ids);
        if (transaction_ids.empty()) {
            StorageEngine::instance()->replication_txn_manager()->get_tablet_related_txns(_tablet_id, &transaction_ids);
        }
        if (!transaction_ids.empty()) {
            LOG(WARNING) << "could not migration because has unfinished txns.";
            return Status::InternalError("could not migration because has unfinished txns.");
        }

        if (tablet->updates() != nullptr && tablet->updates()->num_pending() != 0) {
            std::stringstream ss;
            ss << "could not migration because has pending txns, tablet_id: " << tablet->tablet_id();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // get all versions to be migrate
        {
            std::shared_lock header_rdlock(tablet->get_header_lock());
            if (tablet->updates() == nullptr) {
                RowsetSharedPtr max_version = tablet->rowset_with_max_version();
                if (max_version == nullptr) {
                    std::stringstream ss;
                    ss << "Not found version in tablet. tablet: " << tablet->tablet_id();
                    LOG(WARNING) << ss.str();
                    return Status::NotFound(ss.str());
                }

                end_version = max_version->end_version();
            } else {
                end_version = tablet->updates()->max_version();
            }
            res = tablet->capture_consistent_rowsets(Version(0, end_version), &consistent_rowsets);
            if (!res.ok() || consistent_rowsets.empty()) {
                LOG(WARNING) << "Fail to capture consistent rowsets. version=" << end_version;
                return Status::InternalError(
                        fmt::format("Fail to capture consistent rowsets. version: {}", end_version));
            }
        }

        // get shard
        res = _dest_store->get_shard(&shard);
        if (!res.ok()) {
            LOG(WARNING) << "fail to get root path shard. res=" << res;
            return res;
        }

        TabletMetaSharedPtr stale_tablet_meta(new (std::nothrow) TabletMeta());
        Status st = TabletMetaManager::get_tablet_meta(_dest_store, _tablet_id, _schema_hash, stale_tablet_meta.get());
        if (st.ok() && stale_tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
            // When tablet state is TABLET_SHUTDOWN, it means it's a tablet to be GCed
            // Try to delete it right now.
            // If delete success, go on the process, or return failure.
            // If there is queries running on the tablet, should return failure
            Status st = StorageEngine::instance()->tablet_manager()->delete_shutdown_tablet(_tablet_id);
            if (st.ok() || st.is_not_found()) {
                // delete tablet from shutdown tablets successully, and continue the migration
                LOG(INFO) << "Successfully delete stale TABLET_SHUTDOWN tablet:" << _tablet_id
                          << " from path:" << _dest_store->path();
            } else {
                LOG(WARNING) << "delete shutdown tablet failed. st:" << st;
                return st;
            }
        } else if (st.ok()) {
            LOG(WARNING) << "tablet_meta already exist. tablet:" << tablet->full_name()
                         << ", tablet state:" << stale_tablet_meta->tablet_state()
                         << ", dest path:" << _dest_store->path() << ", source path:" << tablet->data_dir()->path();
            return Status::AlreadyExist(fmt::format("tablet_meta already exist. tablet: {}", tablet->full_name()));
        } else if (!st.is_not_found()) {
            LOG(WARNING) << "get tablet_meta failed. tablet: " << tablet->full_name();
            return Status::NotFound(fmt::format("get tablet_meta failed. tablet: {}", tablet->full_name()));
        }

        std::stringstream root_path_stream;
        root_path_stream << _dest_store->path() << DATA_PREFIX << "/" << shard;
        schema_hash_path = SnapshotManager::instance()->get_schema_hash_full_path(tablet, root_path_stream.str());

        // if dir already exist then return err, it should not happen
        // should not remove the dir directly
        if (fs::path_exist(schema_hash_path)) {
            LOG(INFO) << "Path already exist. "
                      << "schema_hash_path: " << schema_hash_path;
            return Status::AlreadyExist(fmt::format("Path already exist. schema_hash_path: {}", schema_hash_path));
        }

        st = fs::create_directories(schema_hash_path);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to create dir. path: " << schema_hash_path << ", error: " << st.to_string();
            return Status::IOError(
                    fmt::format("Fail to create dir. path: {}, error: {}", schema_hash_path, st.to_string()));
        }

        // set tablet is_migrating
        tablet->set_is_migrating(true);
    }

    std::string new_meta_file;
    std::string dcgs_snapshot_path;
    bool need_remove_new_path = false;
    do {
        // 2. copy all index and data files without lock
        res = _copy_index_and_data_files(schema_hash_path, tablet, consistent_rowsets);
        if (!res.ok()) {
            LOG(WARNING) << "fail to copy index and data files when migrate. res=" << res;
            need_remove_new_path = true;
            std::unique_lock migration_wlock(tablet->get_migration_lock());
            tablet->set_is_migrating(false);
            break;
        }

        // 3. lock and check again, and load new tablet from dest dir
        std::unique_lock migration_wlock(tablet->get_migration_lock());
        DeferOp reset_tablet_is_migrating([&tablet] { return tablet->set_is_migrating(false); });

        std::lock_guard push_lock(tablet->get_push_lock());

        int64_t partition_id;
        std::set<int64_t> transaction_ids;
        StorageEngine::instance()->txn_manager()->get_tablet_related_txns(
                _tablet_id, _schema_hash, tablet->tablet_uid(), &partition_id, &transaction_ids);
        if (transaction_ids.empty()) {
            StorageEngine::instance()->replication_txn_manager()->get_tablet_related_txns(_tablet_id, &transaction_ids);
        }
        if (!transaction_ids.empty()) {
            LOG(WARNING) << "could not migration because has unfinished txns.";
            need_remove_new_path = true;
            res = Status::InternalError("could not migration because has unfinished txns.");
            break;
        }

        if (tablet->updates() != nullptr && tablet->updates()->num_pending() != 0) {
            std::stringstream ss;
            ss << "could not migration because has pending txns. tablet id: " << tablet->tablet_id();
            LOG(WARNING) << ss.str();
            need_remove_new_path = true;
            res = Status::InternalError(ss.str());
            break;
        }

        // can guarantee there are no new load txns in the following execution flow.

        auto new_tablet_meta = std::make_shared<TabletMeta>();
        Status st = TabletMetaManager::get_tablet_meta(_dest_store, _tablet_id, _schema_hash, new_tablet_meta.get());
        if (st.ok()) {
            LOG(WARNING) << "tablet_meta already exist. tablet:" << tablet->full_name();
            need_remove_new_path = true;
            res = Status::AlreadyExist(fmt::format("tablet_meta already exist. tablet: {}", tablet->full_name()));
            break;
        } else if (!st.is_not_found()) {
            LOG(WARNING) << "tablet_meta not found. tablet: " << tablet->full_name();
            need_remove_new_path = true;
            res = Status::NotFound(fmt::format("tablet_meta not found. tablet: {}", tablet->full_name()));
            break;
        }

        if (tablet->updates() == nullptr) {
            res = _finish_migration(tablet, end_version, shard, consistent_rowsets, new_tablet_meta, schema_hash_path,
                                    new_meta_file, dcgs_snapshot_path, need_remove_new_path);
        } else {
            res = _finish_primary_key_migration(tablet, end_version, shard, consistent_rowsets, schema_hash_path,
                                                new_meta_file, need_remove_new_path);
        }
    } while (false);

    // 4. clear
    if (!new_meta_file.empty()) {
        // remove meta file
        Status st = fs::remove(new_meta_file);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove meta file. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
                         << ", path=" << schema_hash_path << ", error=" << st.message();
        }
    }
    if (!dcgs_snapshot_path.empty()) {
        // remove dcg snapshot file
        Status st = fs::remove(dcgs_snapshot_path);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove dcg file. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
                         << ", path=" << schema_hash_path << ", error=" << st.message();
        }
    }
    if (!res.ok() && need_remove_new_path) {
        // remove all index and data files if migration failed
        Status st = fs::remove_all(schema_hash_path);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove storage migration path, tablet_id: " << _tablet_id
                         << ". schema_hash_path=" << schema_hash_path << ", error=" << st.message();
        }
    }

    return res;
}

Status EngineStorageMigrationTask::_finish_migration(const TabletSharedPtr& tablet, int64_t end_version, uint64_t shard,
                                                     const std::vector<RowsetSharedPtr>& consistent_rowsets,
                                                     const TabletMetaSharedPtr& new_tablet_meta,
                                                     const string& schema_hash_path, std::string& new_meta_file,
                                                     std::string& dcgs_snapshot_path, bool& need_remove_new_path) {
    Status res = Status::OK();
    do {
        {
            // check version
            std::shared_lock header_rdlock(tablet->get_header_lock());
            RowsetSharedPtr max_version = tablet->rowset_with_max_version();
            if (max_version == nullptr) {
                LOG(WARNING) << "Not found version in tablet. tablet: " << tablet->tablet_id();
                need_remove_new_path = true;
                res = Status::NotFound(fmt::format("Not found version in tablet. tablet: {}", tablet->tablet_id()));
                break;
            }
            int64_t new_end_version = max_version->end_version();
            if (end_version != new_end_version) {
                LOG(WARNING) << "Version does not match. src_version: " << end_version
                             << ", dst_version: " << new_end_version;
                need_remove_new_path = true;
                res = Status::InternalError(fmt::format("Version does not match. src_version: {}, dst_version: {}",
                                                        end_version, new_end_version));
                break;
            }

            // generate new header
            _generate_new_header(_dest_store, shard, tablet, consistent_rowsets, new_tablet_meta);
        }

        // snapshot dcgs for non-PrimaryKey tablet from source data dir
        auto meta_store = tablet->data_dir()->get_meta();
        DeltaColumnGroupSnapshotPB dcg_snapshot_pb;
        for (const auto& rs : consistent_rowsets) {
            for (int i = 0; i < rs->num_segments(); ++i) {
                int64_t tablet_id = tablet->tablet_id();
                RowsetId rowsetid = rs->rowset_meta()->rowset_id();

                DeltaColumnGroupList dcgs;
                res = TabletMetaManager::get_delta_column_group(meta_store, tablet_id, rowsetid, i, INT64_MAX, &dcgs);
                if (!res.ok()) {
                    break;
                }

                DeltaColumnGroupListPB dcg_list_pb;
                DeltaColumnGroupListSerializer::serialize_delta_column_group_list(dcgs, &dcg_list_pb);

                dcg_snapshot_pb.add_tablet_id(tablet_id);
                dcg_snapshot_pb.add_rowset_id(rowsetid.to_string());
                dcg_snapshot_pb.add_segment_id(i);

                auto add_dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
                add_dcg_list_pb->CopyFrom(dcg_list_pb);
            }
            if (!res.ok()) {
                break;
            }
        }

        if (!res.ok()) {
            LOG(WARNING) << "snapshot dcgs failed, " << res.message() << " tablet id: " << _tablet_id;
            need_remove_new_path = true;
            break;
        }

        dcgs_snapshot_path = schema_hash_path + "/" + std::to_string(_tablet_id) + ".dcgs_snapshot";
        res = DeltaColumnGroupListHelper::save_snapshot(dcgs_snapshot_path, dcg_snapshot_pb);
        if (!res.ok()) {
            LOG(WARNING) << "save dcg snapshot failed, " << res.message() << " tablet id: " << _tablet_id;
            need_remove_new_path = true;
            break;
        }

        new_meta_file = schema_hash_path + "/" + std::to_string(_tablet_id) + ".hdr";
        res = new_tablet_meta->save(new_meta_file);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to save meta to path. file=" << new_meta_file;
            need_remove_new_path = true;
            break;
        }

        res = TabletMeta::reset_tablet_uid(new_meta_file);
        if (!res.ok()) {
            LOG(WARNING) << "errors while set tablet uid. file=" << new_meta_file;
            need_remove_new_path = true;
            break;
        }

        // it will change rowset id and its create time
        // rowset create time is useful when load tablet from meta to check which tablet is the tablet to load
        Status st = SnapshotManager::instance()->convert_rowset_ids(schema_hash_path, _tablet_id, _schema_hash);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to convert rowset id. path=" << schema_hash_path;
            need_remove_new_path = true;
            res = Status::InternalError(fmt::format("Fail to convert rowset id. path: {}", schema_hash_path));
            break;
        }

        // recover dcg meta
        DeltaColumnGroupSnapshotPB modified_dcg_snapshot_pb;
        // the dcgs meta has been reset by new rowsetid
        st = DeltaColumnGroupListHelper::parse_snapshot(dcgs_snapshot_path, modified_dcg_snapshot_pb);
        if (!st.ok()) {
            LOG(WARNING) << "failed to parse dcgs meta";
            need_remove_new_path = true;
            break;
        }

        if (modified_dcg_snapshot_pb.dcg_lists().size() != 0) {
            int idx = 0;
            rocksdb::WriteBatch wb;

            for (const auto& dcg_list_pb : modified_dcg_snapshot_pb.dcg_lists()) {
                // dcgs for each segment
                DeltaColumnGroupList dcgs;
                res = DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(dcg_list_pb, &dcgs);
                if (!res.ok()) {
                    break;
                }

                if (dcgs.size() == 0) {
                    ++idx;
                    continue;
                }

                res = TabletMetaManager::put_delta_column_group(
                        _dest_store, &wb, modified_dcg_snapshot_pb.tablet_id(idx),
                        modified_dcg_snapshot_pb.rowset_id(idx), modified_dcg_snapshot_pb.segment_id(idx), dcgs);
                if (!res.ok()) {
                    break;
                }
                ++idx;
            }

            if (!res.ok()) {
                LOG(WARNING) << "recover dcgs meta failed, tablet id: " << _tablet_id;
                need_remove_new_path = true;
                break;
            }

            res = _dest_store->get_meta()->write_batch(&wb);
            if (!res.ok()) {
                LOG(WARNING) << "recover dcgs meta failed, tablet id: " << _tablet_id;
                need_remove_new_path = true;
                break;
            }
        }

        st = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(_dest_store, _tablet_id, _schema_hash,
                                                                               schema_hash_path, false);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to load tablet from new path. path: " << schema_hash_path;
            need_remove_new_path = true;
            res = Status::InternalError(fmt::format("Fail to load tablet from new path. path: {}", schema_hash_path));
            break;
        }

        // if old tablet finished schema change, then the schema change status of the new tablet is DONE
        // else the schema change status of the new tablet is FAILED
        TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
        if (new_tablet == nullptr) {
            // tablet already loaded success.
            // just log, and not set need_remove_new_path.
            LOG(WARNING) << "Not found tablet: " << _tablet_id;
            res = Status::NotFound(fmt::format("Not found tablet: {}", _tablet_id));
            break;
        }
    } while (false);

    return res;
}

/*
    The key idea is that, when we migrate the Primary Key tablet, it need the following steps:
    1. copy the data file into the dest store path (done before this function)
    2. snapshot the meta data
    3. create a NEW tablet using meta data in step 2 with the same tablet id. And FORCE REPLACE
       the old one. This is the same as non Primary Key tablet.
    4. clear primary index cache
*/
Status EngineStorageMigrationTask::_finish_primary_key_migration(const TabletSharedPtr& tablet, int64_t end_version,
                                                                 uint64_t shard,
                                                                 const std::vector<RowsetSharedPtr>& consistent_rowsets,
                                                                 const string& schema_hash_path,
                                                                 std::string& new_meta_file,
                                                                 bool& need_remove_new_path) {
    Status res = Status::OK();
    do {
        {
            // check version
            std::shared_lock header_rdlock(tablet->get_header_lock());
            /*
                Just make sure the major version is consistent.

                There is no need to pay attention to minor here,
                because when making the snapshot on tablet meta, the
                corresponding delvector will be obtained according
                to the segment file in consistent_rowsets, so
                consistency can be maintained.
            */
            int64_t new_end_version = tablet->updates()->max_version();
            if (end_version != new_end_version) {
                std::stringstream ss;
                ss << "Version does not match. src_version: " << end_version << ", dst_version: " << new_end_version
                   << "tablet_id: " << tablet->tablet_id();
                LOG(WARNING) << ss.str();
                need_remove_new_path = true;
                res = Status::InternalError(ss.str());
                break;
            }

            std::vector<RowsetMetaSharedPtr> rowset_metas;
            rowset_metas.reserve(consistent_rowsets.size());
            for (const auto& rowset : consistent_rowsets) {
                rowset_metas.emplace_back(rowset->rowset_meta());
            }
            res = SnapshotManager::instance()->make_snapshot_on_tablet_meta(SNAPSHOT_TYPE_FULL, schema_hash_path,
                                                                            tablet, rowset_metas, new_end_version,
                                                                            g_Types_constants.TSNAPSHOT_REQ_VERSION2);
            if (!res.ok()) {
                need_remove_new_path = true;
                break;
            }
        }

        auto tablet_manager = StorageEngine::instance()->tablet_manager();
        res = tablet_manager->create_tablet_from_meta_snapshot(_dest_store, _tablet_id, tablet->schema_hash(),
                                                               schema_hash_path, false);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to create tablet from meta snapshot. tablet_id: " << _tablet_id;
            WriteBatch wb;
            RETURN_IF_ERROR(TabletMetaManager::clear_del_vector(_dest_store, &wb, _tablet_id));
            RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(_dest_store, &wb, _tablet_id));
            RETURN_IF_ERROR(TabletMetaManager::clear_rowset(_dest_store, &wb, _tablet_id));
            RETURN_IF_ERROR(TabletMetaManager::clear_log(_dest_store, &wb, _tablet_id));
            RETURN_IF_ERROR(TabletMetaManager::remove_tablet_meta(_dest_store, &wb, _tablet_id, tablet->schema_hash()));
            auto st = _dest_store->get_meta()->write_batch(&wb);
            LOG_IF(WARNING, !st.ok()) << "Fail to clear meta store: " << st << " tablet_id: " << _tablet_id;
            need_remove_new_path = true;
            break;
        }

        // clear index cache
        auto manager = StorageEngine::instance()->update_manager();
        auto& index_cache = manager->index_cache();
        auto index_entry = index_cache.get_or_create(_tablet_id);
        index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
        index_entry->value().unload();
        index_cache.release(index_entry);

        // if old tablet finished schema change, then the schema change status of the new tablet is DONE
        // else the schema change status of the new tablet is FAILED
        TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
        if (new_tablet == nullptr) {
            // tablet already loaded success.
            // just log, and not set need_remove_new_path.
            LOG(WARNING) << "Not found tablet: " << _tablet_id;
            res = Status::NotFound(fmt::format("Not found tablet: {}", _tablet_id));
            break;
        }
    } while (false);

    return res;
}

// TODO(ygl): lost some infomation here, such as cumulative layer point
void EngineStorageMigrationTask::_generate_new_header(DataDir* store, const uint64_t new_shard,
                                                      const TabletSharedPtr& tablet,
                                                      const std::vector<RowsetSharedPtr>& consistent_rowsets,
                                                      const TabletMetaSharedPtr& new_tablet_meta) {
    DCHECK(store != nullptr);
    tablet->generate_tablet_meta_copy_unlocked(new_tablet_meta);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    rs_metas.reserve(consistent_rowsets.size());
    for (auto& rs : consistent_rowsets) {
        rs_metas.push_back(rs->rowset_meta());
    }
    new_tablet_meta->revise_inc_rs_metas(std::vector<RowsetMetaSharedPtr>());
    new_tablet_meta->revise_rs_metas(std::move(rs_metas));
    new_tablet_meta->set_shard_id(new_shard);
    // should not save new meta here, because new tablet may failed
    // should not remove the old meta here, because the new header maybe not valid
    // remove old meta after the new tablet is loaded successfully
}

Status EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& schema_hash_path, const TabletSharedPtr& ref_tablet,
        const std::vector<RowsetSharedPtr>& consistent_rowsets) const {
    Status status = Status::OK();
    for (const auto& rs : consistent_rowsets) {
        bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
        if (bg_worker_stopped) {
            status = Status::InternalError("Process is going to quit.");
            break;
        }
        status = rs->copy_files_to(ref_tablet->data_dir()->get_meta(), schema_hash_path);
        if (!status.ok()) {
            break;
        }
    }
    return status;
}

} // namespace starrocks
