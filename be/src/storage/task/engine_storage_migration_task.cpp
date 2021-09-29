// This file is made available under Elastic License 2.0.
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

#include "storage/snapshot_manager.h"
#include "storage/tablet_meta_manager.h"
#include "util/defer_op.h"

namespace starrocks {

EngineStorageMigrationTask::EngineStorageMigrationTask(TTabletId tablet_id, TSchemaHash schema_hash,
                                                       DataDir* dest_store)
        : _tablet_id(tablet_id), _schema_hash(schema_hash), _dest_store(dest_store) {}

OLAPStatus EngineStorageMigrationTask::execute() {
    StarRocksMetrics::instance()->storage_migrate_requests_total.increment(1);

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id, _schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "failed to find tablet. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }
    if (tablet->updates() != nullptr) {
        LOG(WARNING) << "migration task does not support updatable tablet yet";
        return OLAP_ERR_OTHER_ERROR;
    }

    // check tablet data dir
    if (tablet->data_dir() == _dest_store) {
        LOG(INFO) << "tablet is already in the dest path. tablet_id=" << _tablet_id
                  << ", dest_store=" << _dest_store->path();
        return OLAP_SUCCESS;
    }

    // check disk capacity
    int64_t tablet_size = tablet->tablet_footprint();
    if (_dest_store->reach_capacity_limit(tablet_size)) {
        LOG(WARNING) << "disk reaches capacity limit. tablet_id=" << _tablet_id
                     << ", dest_store=" << _dest_store->path();
        return OLAP_ERR_DISK_REACH_CAPACITY_LIMIT;
    }

    return _storage_migrate(tablet);
}

OLAPStatus EngineStorageMigrationTask::_storage_migrate(TabletSharedPtr tablet) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to process storage migrate. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
              << ", tablet=" << tablet->full_name() << ", dest_store=" << _dest_store->path();

    // 1. lock and check
    int32_t end_version = -1;
    std::vector<RowsetSharedPtr> consistent_rowsets;
    uint64_t shard = 0;
    std::string schema_hash_path;
    {
        // lock
        std::unique_lock migration_wlock(tablet->get_migration_lock(), std::try_to_lock);
        if (!migration_wlock.owns_lock()) {
            return OLAP_ERR_RWLOCK_ERROR;
        }
        if (tablet->is_migrating()) {
            LOG(WARNING) << "tablet is already migrating.";
            return OLAP_ERR_OTHER_ERROR;
        }

        std::lock_guard push_lock(tablet->get_push_lock());

        // check load txns
        int64_t partition_id;
        std::set<int64_t> transaction_ids;
        StorageEngine::instance()->txn_manager()->get_tablet_related_txns(
                _tablet_id, _schema_hash, tablet->tablet_uid(), &partition_id, &transaction_ids);
        if (!transaction_ids.empty()) {
            LOG(WARNING) << "could not migration because has unfinished txns.";
            return OLAP_ERR_HEADER_HAS_PENDING_DATA;
        }

        // get all versions to be migrate
        {
            std::shared_lock header_rdlock(tablet->get_header_lock());
            const RowsetSharedPtr lastest_version = tablet->rowset_with_max_version();
            if (lastest_version == nullptr) {
                LOG(WARNING) << "tablet has no any version.";
                return OLAP_ERR_VERSION_NOT_EXIST;
            }

            end_version = lastest_version->end_version();
            res = tablet->capture_consistent_rowsets(Version(0, end_version), &consistent_rowsets);
            if (res != OLAP_SUCCESS || consistent_rowsets.empty()) {
                LOG(WARNING) << "fail to capture consistent rowsets. version=" << end_version;
                return OLAP_ERR_VERSION_NOT_EXIST;
            }
        }

        // get shard
        res = _dest_store->get_shard(&shard);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to get root path shard. res=" << res;
            return res;
        }

        std::stringstream root_path_stream;
        root_path_stream << _dest_store->path() << DATA_PREFIX << "/" << shard;
        schema_hash_path = SnapshotManager::instance()->get_schema_hash_full_path(tablet, root_path_stream.str());
        // if dir already exist then return err, it should not happen
        // should not remove the dir directly
        if (FileUtils::check_exist(schema_hash_path)) {
            LOG(INFO) << "schema hash path already exist, skip this path. "
                      << "schema_hash_path=" << schema_hash_path;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }

        auto mem_tracker = std::make_unique<MemTracker>();
        TabletMetaSharedPtr new_tablet_meta(new (std::nothrow) TabletMeta(mem_tracker.get()));
        Status st = TabletMetaManager::get_tablet_meta(_dest_store, _tablet_id, _schema_hash, new_tablet_meta);
        if (st.ok()) {
            LOG(WARNING) << "tablet_meta already exists. data_dir:" << _dest_store->path()
                         << "tablet:" << tablet->full_name();
            return OLAP_ERR_META_ALREADY_EXIST;
        } else if (!st.is_not_found()) {
            LOG(WARNING) << "fail to get tablet meta: " << st;
            return OLAP_ERR_IO_ERROR;
        }

        st = FileUtils::create_dir(schema_hash_path);
        if (!st.ok()) {
            LOG(WARNING) << "fail to create path. path=" << schema_hash_path << ", error=" << st.to_string();
            return OLAP_ERR_CANNOT_CREATE_DIR;
        }

        // set tablet is_migrating
        tablet->set_is_migrating(true);
    }

    std::string new_meta_file;
    bool need_remove_new_path = false;
    do {
        // 2. copy all index and data files without lock
        res = _copy_index_and_data_files(schema_hash_path, tablet, consistent_rowsets);
        if (res != OLAP_SUCCESS) {
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
        if (!transaction_ids.empty()) {
            LOG(WARNING) << "could not migration because has unfinished txns.";
            need_remove_new_path = true;
            res = OLAP_ERR_HEADER_HAS_PENDING_DATA;
            break;
        }

        auto mem_tracker = std::make_unique<MemTracker>();
        TabletMetaSharedPtr new_tablet_meta(new (std::nothrow) TabletMeta(mem_tracker.get()));
        Status st = TabletMetaManager::get_tablet_meta(_dest_store, _tablet_id, _schema_hash, new_tablet_meta);
        if (st.ok()) {
            LOG(WARNING) << "tablet_meta already exists. data_dir:" << _dest_store->path()
                         << "tablet:" << tablet->full_name();
            need_remove_new_path = true;
            res = OLAP_ERR_META_ALREADY_EXIST;
            break;
        } else if (!st.is_not_found()) {
            LOG(WARNING) << "fail to get tablet meta: " << st;
            need_remove_new_path = true;
            res = OLAP_ERR_IO_ERROR;
            break;
        }

        {
            // check version
            std::shared_lock header_rdlock(tablet->get_header_lock());
            const RowsetSharedPtr lastest_version = tablet->rowset_with_max_version();
            if (lastest_version == nullptr) {
                LOG(WARNING) << "tablet has no any version.";
                need_remove_new_path = true;
                res = OLAP_ERR_VERSION_NOT_EXIST;
                break;
            }
            int32_t new_end_version = lastest_version->end_version();
            if (end_version != new_end_version) {
                LOG(WARNING) << "copied version=" << end_version
                             << " is different from new version=" << new_end_version;
                need_remove_new_path = true;
                res = OLAP_ERR_VERSION_NOT_EXIST;
                break;
            }

            // generate new header
            _generate_new_header(_dest_store, shard, tablet, consistent_rowsets, new_tablet_meta);
        }

        new_meta_file = schema_hash_path + "/" + std::to_string(_tablet_id) + ".hdr";
        res = new_tablet_meta->save(new_meta_file);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save meta to path. file=" << new_meta_file;
            need_remove_new_path = true;
            break;
        }

        res = TabletMeta::reset_tablet_uid(new_meta_file);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "errors while set tablet uid. file=" << new_meta_file;
            need_remove_new_path = true;
            break;
        }

        // it will change rowset id and its create time
        // rowset create time is useful when load tablet from meta to check which tablet is the tablet to load
        st = SnapshotManager::instance()->convert_rowset_ids(schema_hash_path, _tablet_id, _schema_hash);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to convert rowset id. path=" << schema_hash_path;
            need_remove_new_path = true;
            res = OLAP_ERR_OTHER_ERROR;
            break;
        }

        st = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(_dest_store, _tablet_id, _schema_hash,
                                                                               schema_hash_path, false);
        if (!st.ok()) {
            LOG(WARNING) << "failed to load tablet from new path. path=" << schema_hash_path;
            need_remove_new_path = true;
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }

        // if old tablet finished schema change, then the schema change status of the new tablet is DONE
        // else the schema change status of the new tablet is FAILED
        TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id, _schema_hash);
        if (new_tablet == nullptr) {
            // tablet already loaded success.
            // just log, and not set need_remove_new_path.
            LOG(WARNING) << "get null tablet. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash;
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }
        AlterTabletTaskSharedPtr alter_task = tablet->alter_task();
        if (alter_task != nullptr) {
            if (alter_task->alter_state() == ALTER_FINISHED) {
                new_tablet->set_alter_state(ALTER_FINISHED);
            } else {
                new_tablet->delete_alter_task();
            }
        }
    } while (false);

    // 4. clear
    if (!new_meta_file.empty()) {
        // remove hdr meta file
        Status st = FileUtils::remove(new_meta_file);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove meta file. tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
                         << ", path=" << schema_hash_path << ", error=" << st.to_string();
        }
    }
    if (res != OLAP_SUCCESS && need_remove_new_path) {
        // remove all index and data files if migration failed
        Status st = FileUtils::remove_all(schema_hash_path);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove storage migration path"
                         << ". schema_hash_path=" << schema_hash_path << ", error=" << st.to_string();
        }
    }

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

OLAPStatus EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& schema_hash_path, const TabletSharedPtr& ref_tablet,
        const std::vector<RowsetSharedPtr>& consistent_rowsets) const {
    OLAPStatus status = OLAP_SUCCESS;
    for (const auto& rs : consistent_rowsets) {
        status = rs->copy_files_to(schema_hash_path);
        if (status != OLAP_SUCCESS) {
            break;
        }
    }
    return status;
}

} // namespace starrocks
