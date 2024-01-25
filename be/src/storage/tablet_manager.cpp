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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_manager.cpp

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

#include "storage/tablet_manager.h"

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <re2/re2.h>

#include <ctime>
#include <memory>

#include "common/config.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "storage/compaction_manager.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_meta_manager.h"
#include "storage/txn_manager.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "util/path_util.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

namespace {
constexpr int RETRY_TIMES_ON_SHUTDOWN_TABLET_OCCUPIED = 5;
constexpr int RETRY_INTERVAL_ON_SHUTDOWN_TABLET_OCCUPIED = 1; // in second
} // namespace

static void get_shutdown_tablets(std::ostream& os, void*) {
    auto mgr = StorageEngine::instance() ? StorageEngine::instance()->tablet_manager() : nullptr;
    if (mgr != nullptr) {
        os << mgr->shutdown_tablets();
    } else {
        os << "unknown";
    }
}

bvar::PassiveStatus<std::string> g_shutdown_tablets("starrocks_shutdown_tablets", get_shutdown_tablets, nullptr);

TabletManager::TabletManager(int64_t tablet_map_lock_shard_size)
        : _tablets_shards(tablet_map_lock_shard_size),
          _tablets_shards_mask(tablet_map_lock_shard_size - 1),
          _last_update_stat_ms(0) {
    CHECK_GT(_tablets_shards.size(), 0) << "tablets shard count greater than 0";
    CHECK_EQ(_tablets_shards.size() & _tablets_shards_mask, 0) << "tablets shard count must be power of two";
}

Status TabletManager::_add_tablet_unlocked(const TabletSharedPtr& new_tablet, bool update_meta, bool force) {
    TabletSharedPtr old_tablet = _get_tablet_unlocked(new_tablet->tablet_id());
    if (old_tablet == nullptr) {
        RETURN_IF_ERROR(_update_tablet_map_and_partition_info(new_tablet));
    } else if (force) {
        RETURN_IF_ERROR(_drop_tablet_unlocked(old_tablet->tablet_id(), kKeepMetaAndFiles));
        RETURN_IF_ERROR(_update_tablet_map_and_partition_info(new_tablet));
    } else {
        if (old_tablet->schema_hash_path() == new_tablet->schema_hash_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id=" << new_tablet->tablet_id()
                         << " schema_hash_path=" << new_tablet->schema_hash_path();
            return Status::InternalError(fmt::format("tablet already exists, tablet_id: {}", old_tablet->tablet_id()));
        }
        if (old_tablet->data_dir() == new_tablet->data_dir()) {
            LOG(WARNING) << "add tablet with same data dir twice! tablet_id=" << new_tablet->tablet_id();
            return Status::InternalError(fmt::format("tablet already exists, tablet_id: {}", old_tablet->tablet_id()));
        }

        int64_t old_time = 0;
        int64_t new_time = 0;
        int64_t old_version = 0;
        int64_t new_version = 0;
        if (new_tablet->updates() != nullptr) {
            old_time = old_tablet->updates()->max_rowset_creation_time();
            new_time = new_tablet->updates()->max_rowset_creation_time();
            old_version = old_tablet->updates()->max_version();
            new_version = new_tablet->updates()->max_version();
        } else {
            old_tablet->obtain_header_rdlock();
            auto old_rowset = old_tablet->rowset_with_max_version();
            auto new_rowset = new_tablet->rowset_with_max_version();
            old_time = (old_rowset == nullptr) ? -1 : old_rowset->creation_time();
            new_time = (new_rowset == nullptr) ? -1 : new_rowset->creation_time();
            old_version = (old_rowset == nullptr) ? -1 : old_rowset->end_version();
            new_version = (new_rowset == nullptr) ? -1 : new_rowset->end_version();
            old_tablet->release_header_lock();
        }
        bool replace_old = (new_version > old_version) || (new_version == old_version && new_time > old_time);

        if (replace_old) {
            RETURN_IF_ERROR(_drop_tablet_unlocked(old_tablet->tablet_id(), kMoveFilesToTrash));
            RETURN_IF_ERROR(_update_tablet_map_and_partition_info(new_tablet));
            LOG(INFO) << "Added duplicated tablet. tablet_id=" << new_tablet->tablet_id()
                      << " old_tablet_path=" << old_tablet->schema_hash_path()
                      << " new_tablet_path=" << new_tablet->schema_hash_path();
        } else {
            return Status::AlreadyExist(fmt::format("tablet already exists, tablet_id: {}", old_tablet->tablet_id()));
        }
    }
    if (update_meta) {
        new_tablet->save_meta();
    }
    return Status::OK();
}

Status TabletManager::_update_tablet_map_and_partition_info(const TabletSharedPtr& tablet) {
    // Register tablet into DataDir, so that we can manage tablet from
    // the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    tablet->register_tablet_into_dir();
    TabletMap& tablet_map = _get_tablet_map(tablet->tablet_id());
    auto [it, inserted] = tablet_map.emplace(tablet->tablet_id(), tablet);
    if (!inserted) {
        return Status::InternalError(fmt::format("tablet {} already exist in map", tablet->tablet_id()));
    }
    _add_tablet_to_partition(*tablet);
    return Status::OK();
}

Status TabletManager::create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores) {
    StarRocksMetrics::instance()->create_tablet_requests_total.increment(1);

    int64_t tablet_id = request.tablet_id;
    int32_t schema_hash = request.tablet_schema.schema_hash;
    LOG(INFO) << "Creating tablet " << tablet_id;

    std::unique_lock wlock(_get_tablets_shard_lock(tablet_id), std::defer_lock);
    std::shared_lock<std::shared_mutex> base_rlock;

    // If do schema change, both the shard where the source tablet is located and
    // the shard where the target tablet is located need to be locked.
    // In order to prevent deadlock, the order of locking needs to be fixed.
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        int64_t shard_idx = _get_tablets_shard_idx(tablet_id);
        int64_t base_shard_idx = _get_tablets_shard_idx(request.base_tablet_id);

        if (shard_idx == base_shard_idx) {
            wlock.lock();
        } else {
            std::shared_lock tmp_rlock(_get_tablets_shard_lock(request.base_tablet_id), std::defer_lock);
            base_rlock = std::move(tmp_rlock);

            if (shard_idx < base_shard_idx) {
                wlock.lock();
                base_rlock.lock();
            } else {
                base_rlock.lock();
                wlock.lock();
            }
        }
    } else {
        wlock.lock();
    }

    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, true, nullptr);
    if (tablet != nullptr && tablet->tablet_state() != TABLET_SHUTDOWN) {
        return Status::OK();
    } else if (tablet != nullptr) {
        StarRocksMetrics::instance()->create_tablet_requests_failed.increment(1);
        DCHECK_EQ(TABLET_SHUTDOWN, tablet->tablet_state());
        return Status::InternalError("tablet still resident in shutdown queue");
    }

    TabletSharedPtr base_tablet = nullptr;
    bool is_schema_change = false;
    // If the CreateTabletReq has base_tablet_id then it is a alter-tablet request
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        is_schema_change = true;
        base_tablet = _get_tablet_unlocked(request.base_tablet_id);
        if (base_tablet == nullptr) {
            LOG(WARNING) << "Fail to create tablet(change schema), base tablet does not exist. "
                         << "new_tablet_id=" << tablet_id << " new_schema_hash=" << schema_hash
                         << " base_tablet_id=" << request.base_tablet_id
                         << " base_schema_hash=" << request.base_schema_hash;
            StarRocksMetrics::instance()->create_tablet_requests_failed.increment(1);
            return Status::InternalError("base tablet not exist");
        }
        // If we are doing schema-change, we should use the same data dir
        // TODO(lingbin): A litter trick here, the directory should be determined before
        // entering this method
        stores.clear();
        stores.push_back(base_tablet->data_dir());
    }

    // set alter type to schema-change. it is useless
    tablet = _internal_create_tablet_unlocked(AlterTabletType::SCHEMA_CHANGE, request, is_schema_change,
                                              base_tablet.get(), stores);
    if (tablet == nullptr) {
        LOG(WARNING) << "Fail to create tablet " << request.tablet_id;
        StarRocksMetrics::instance()->create_tablet_requests_failed.increment(1);
        return Status::InternalError("fail to create tablet");
    }

    LOG(INFO) << "Created tablet " << tablet_id;
    return Status::OK();
}

TabletSharedPtr TabletManager::_internal_create_tablet_unlocked(AlterTabletType alter_type,
                                                                const TCreateTabletReq& request, bool is_schema_change,
                                                                const Tablet* base_tablet,
                                                                const std::vector<DataDir*>& data_dirs) {
    // If in schema-change state, base_tablet must also be provided.
    // i.e., is_schema_change and base_tablet are either assigned or not assigned
    DCHECK((is_schema_change && base_tablet) || (!is_schema_change && !base_tablet));

    // NOTE: The existence of tablet_id and schema_hash has already been checked,
    // no need check again here.

    auto tablet = _create_tablet_meta_and_dir_unlocked(request, is_schema_change, base_tablet, data_dirs);
    if (tablet == nullptr) {
        return nullptr;
    }
    CHECK_EQ(request.tablet_id, tablet->tablet_id());
    CHECK_EQ(request.tablet_schema.schema_hash, tablet->schema_hash());

    // should remove the tablet's pending_id no matter create-tablet success or not
    DataDir* data_dir = tablet->data_dir();

    Status status;
    bool is_tablet_added = false;
    do {
        if (tablet->keys_type() == KeysType::PRIMARY_KEYS) {
            tablet->tablet_meta()->create_inital_updates_meta();
        }
        if (Status st = tablet->init(); !st.ok()) {
            LOG(WARNING) << "Fail to init tablet " << tablet->full_name() << ": " << st;
            status = Status::InternalError("tablet init failed: " + st.to_string());
            break;
        }
        // TODO(lingbin): is it needed? because all type of create_tablet will be true.
        // 1. !is_schema_change: not in schema-change state;
        // 2. request.base_tablet_id > 0: in schema-change state;
        // updatable tablet already have initial version, do not need to call _create_inital_rowset_unlocked
        if (tablet->keys_type() != KeysType::PRIMARY_KEYS &&
            (!is_schema_change || (request.__isset.base_tablet_id && request.base_tablet_id > 0))) {
            // Create init version if this is not a restore mode replica and request.version is set
            // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
            // if (!in_restore_mode && request.__isset.version) {
            // create inital rowset before add it to storage engine could omit many locks
            status = _create_inital_rowset_unlocked(request, tablet.get());
            if (!status.ok()) {
                LOG(WARNING) << "Fail to create initial version for tablet: " << status.to_string();
                break;
            }
        }
        if (is_schema_change) {
            if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
                // if this is a new alter tablet, has to set its state to not ready
                // because schema change hanlder depends on it to check whether history data
                // convert finished
                DCHECK(tablet->tablet_state() != TabletState::TABLET_SHUTDOWN);
                (void)tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
            }
            // Possible cases:
            // 1. Because system time may rollback, creation_time of new table will be earlier
            // than old table's creation_time
            // 2. Because the olap engine code is unified in seconds, if the time interval between
            // two operations (e.g., create a table, then immediately alter the table) is less
            // than 1s, the creation_time of the new table and the old table obtained by alter
            // will be the same
            //
            // When the above two cases occur, in order to distinguish the new table and the
            // old table obtained by alter, the creation_time of the new table is set to the
            // creation_time of the old table plus 1
            if (tablet->creation_time() <= base_tablet->creation_time()) {
                LOG(WARNING) << "new tablet's create time is less than or equal to old tablet,"
                             << " new_tablet_create_time=" << tablet->creation_time()
                             << " base_tablet_create_time=" << base_tablet->creation_time();
                int64_t new_creation_time = base_tablet->creation_time() + 1;
                tablet->set_creation_time(new_creation_time);
            }
        }
        // Add tablet to StorageEngine will make it visiable to user
        status = _add_tablet_unlocked(tablet, true, false);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to add tablet to StorageEngine: " << status.to_string();
            break;
        }
        is_tablet_added = true;
    } while (false);

    if (status.ok()) {
        return tablet;
    }
    // something is wrong, we need clear environment
    if (is_tablet_added) {
        status = _drop_tablet_unlocked(tablet->tablet_id(), kMoveFilesToTrash);
        LOG_IF(WARNING, !status.ok()) << "Fail to drop tablet when create tablet failed: " << status.to_string();
    } else {
        tablet->delete_all_files();
        (void)TabletMetaManager::remove(data_dir, tablet->tablet_id(), tablet->schema_hash());
    }
    return nullptr;
}

TabletSharedPtr TabletManager::_create_tablet_meta_and_dir_unlocked(const TCreateTabletReq& request,
                                                                    bool is_schema_change, const Tablet* base_tablet,
                                                                    const std::vector<DataDir*>& data_dirs) {
    for (const auto& data_dir : data_dirs) {
        TabletMetaSharedPtr tablet_meta;
        Status st = _create_tablet_meta_unlocked(request, data_dir, is_schema_change, base_tablet, &tablet_meta);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to create tablet meta. root=" << data_dir->path() << " status=" << st.to_string();
            continue;
        }

        TabletSharedPtr new_tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
        st = fs::create_directories(new_tablet->schema_hash_path());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to create " << new_tablet->schema_hash_path() << ": " << st.to_string();
            continue;
        }

        if (config::sync_tablet_meta) {
            std::filesystem::path schema_hash_path(new_tablet->schema_hash_path());
            std::filesystem::path tablet_id_path = schema_hash_path.parent_path();
            Status st = fs::sync_dir(tablet_id_path.string());
            if (!st.ok()) {
                LOG(WARNING) << "Fail to sync " << tablet_id_path.string() << ": " << st.to_string();
                continue;
            }
            std::filesystem::path shard_id_path = tablet_id_path.parent_path();
            st = fs::sync_dir(shard_id_path.string());
            if (!st.ok()) {
                LOG(WARNING) << "Fail to sync " << shard_id_path.string() << ": " << st.to_string();
                continue;
            }
        }
        return new_tablet;
    }
    return nullptr;
}

Status TabletManager::drop_tablet(TTabletId tablet_id, TabletDropFlag flag) {
    TabletSharedPtr dropped_tablet = nullptr;
    {
        std::unique_lock wlock(_get_tablets_shard_lock(tablet_id));
        StarRocksMetrics::instance()->drop_tablet_requests_total.increment(1);

        if (flag != kDeleteFiles && flag != kMoveFilesToTrash && flag != kKeepMetaAndFiles) {
            return Status::InvalidArgument(fmt::format("invalid TabletDropFlag {}", (int)flag));
        }

        TabletMap& tablet_map = _get_tablet_map(tablet_id);
        auto it = tablet_map.find(tablet_id);
        if (it == tablet_map.end()) {
            LOG(WARNING) << "Fail to drop nonexistent tablet " << tablet_id;
            return Status::NotFound(strings::Substitute("tablet $0 not fount", tablet_id));
        }

        LOG(INFO) << "Start to drop tablet " << tablet_id;
        dropped_tablet = it->second;
        tablet_map.erase(it);
        _remove_tablet_from_partition(*dropped_tablet);
    }
    if (config::enable_event_based_compaction_framework) {
        dropped_tablet->stop_compaction();
        StorageEngine::instance()->compaction_manager()->remove_candidate(dropped_tablet->tablet_id());
    }

    DroppedTabletInfo drop_info{.tablet = dropped_tablet, .flag = flag};

    if (flag == kDeleteFiles) {
        {
            // NOTE: Other threads may save the tablet meta back to storage again after we
            // have deleted it here, and the tablet will reappear after restarted.
            // To prevent this, set the tablet state to `SHUTDOWN` first before removing tablet
            // meta from storage, and assuming that no thread will change the tablet state back
            // to 'RUNNING' from 'SHUTDOWN'.
            std::unique_lock l(dropped_tablet->get_header_lock());
            (void)dropped_tablet->set_tablet_state(TABLET_SHUTDOWN);
        }

        // Remove tablet meta from storage, crash the program if failed.
        if (auto st = _remove_tablet_meta(dropped_tablet); !st.ok()) {
            LOG(FATAL) << "Fail to remove tablet meta: " << st;
        }

        // Remove the tablet directory in background to avoid holding the lock of tablet map shard for long.
        std::unique_lock l(_shutdown_tablets_lock);
        _shutdown_tablets.emplace(tablet_id, std::move(drop_info));
    } else if (flag == kMoveFilesToTrash) {
        {
            // See comments above
            std::unique_lock l(dropped_tablet->get_header_lock());
            (void)dropped_tablet->set_tablet_state(TABLET_SHUTDOWN);
            dropped_tablet->save_meta();
        }

        std::unique_lock l(_shutdown_tablets_lock);
        _shutdown_tablets.emplace(tablet_id, std::move(drop_info));
    } else {
        DCHECK_EQ(kKeepMetaAndFiles, flag);
    }
    dropped_tablet->deregister_tablet_from_dir();
    LOG(INFO) << "Succeed to drop tablet " << tablet_id;
    return Status::OK();
}

Status TabletManager::drop_tablets_on_error_root_path(const std::vector<TabletInfo>& tablet_info_vec) {
    if (tablet_info_vec.empty()) {
        return Status::OK();
    }
    auto num_shards = _tablets_shards.size();
    for (int i = 0; i < num_shards; i++) {
        std::unique_lock wlock(_tablets_shards[i].lock);
        for (const TabletInfo& tablet_info : tablet_info_vec) {
            TTabletId tablet_id = tablet_info.tablet_id;
            if ((tablet_id & _tablets_shards_mask) != i) {
                continue;
            }
            TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id);
            if (dropped_tablet == nullptr) {
                LOG(WARNING) << "dropping tablet not exist. tablet_id=" << tablet_id;
                continue;
            } else {
                TabletMap& tablet_map = _get_tablet_map(tablet_id);
                _remove_tablet_from_partition(*dropped_tablet);
                tablet_map.erase(tablet_id);
            }
        }
    }
    return Status::OK();
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, bool include_deleted, std::string* err) {
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));
    return _get_tablet_unlocked(tablet_id, include_deleted, err);
}

StatusOr<TabletAndRowsets> TabletManager::capture_tablet_and_rowsets(TTabletId tablet_id, int64_t from_version,
                                                                     int64_t to_version) {
    std::string err;
    auto tablet = get_tablet(tablet_id, true, &err);
    if (!tablet) {
        std::string errmsg = strings::Substitute("Failed to get tablet(tablet_id=$0),reason=$1", tablet_id, err);
        return Status::InternalError(errmsg);
    }
    std::shared_lock rlock(tablet->get_header_lock());
    std::vector<RowsetSharedPtr> rowsets;
    RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version{from_version, to_version}, &rowsets));
    return std::make_tuple(std::move(tablet), std::move(rowsets), std::make_shared<RowsetsAcqRel>(rowsets));
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, bool include_deleted, std::string* err) {
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id);
    if (tablet == nullptr && include_deleted) {
        std::shared_lock rlock(_shutdown_tablets_lock);
        if (auto it = _shutdown_tablets.find(tablet_id); it != _shutdown_tablets.end()) {
            tablet = it->second.tablet;
        }
    }

    if (tablet == nullptr) {
        if (err != nullptr) {
            *err = "tablet does not exist";
        }
        return nullptr;
    }

    if (!tablet->is_used()) {
        LOG(WARNING) << "tablet_id=" << tablet_id << " cannot be used";
        if (err != nullptr) {
            *err = "tablet cannot be used";
        }
        return nullptr;
    }

    return tablet;
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, const TabletUid& tablet_uid, bool include_deleted,
                                          std::string* err) {
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
}

bool TabletManager::get_tablet_id_and_schema_hash_from_path(const std::string& path, TTabletId* tablet_id,
                                                            TSchemaHash* schema_hash) {
    static re2::RE2 normal_re(R"(/data/\d+/(\d+)/(\d+)($|/))", re2::RE2::Quiet);
    // match tablet schema hash data path, for example, the path is /data/1/16791/29998
    // 1 is shard id , 16791 is tablet id, 29998 is schema hash
    if (RE2::PartialMatch(path, normal_re, tablet_id, schema_hash)) {
        return true;
    }

    // If we can't match normal path pattern, this may be a path which is a empty tablet
    // directory. Use this pattern to match empty tablet directory. In this case schema_hash
    // will be set to zero.
    static re2::RE2 empty_tablet_re("/data/\\d+/(\\d+)($|/$)", re2::RE2::Quiet);
    if (!RE2::PartialMatch(path, empty_tablet_re, tablet_id)) {
        return false;
    }
    *schema_hash = 0;
    return true;
}

bool TabletManager::get_rowset_id_from_path(const std::string& path, RowsetId* rowset_id) {
    static re2::RE2 re(R"(/data/\d+/\d+/\d+/([A-Fa-f0-9]+)_.*)", re2::RE2::Quiet);
    std::string id_str;
    bool ret = RE2::PartialMatch(path, re, &id_str);
    if (ret) {
        rowset_id->init(id_str);
        return true;
    }
    return false;
}

void TabletManager::get_tablet_stat(TTabletStatResult* result) {
    int64_t curr_ms = UnixMillis();
    // Update cache if it is too old
    {
        int interval_sec = config::tablet_stat_cache_update_interval_second;
        std::lock_guard<std::mutex> l(_tablet_stat_mutex);
        if (curr_ms - _last_update_stat_ms > interval_sec * 1000) {
            VLOG(3) << "update tablet stat.";
            _build_tablet_stat();
            _last_update_stat_ms = UnixMillis();
        }
    }

    result->__set_tablets_stats(_tablet_stat_cache);
}

// return true if all tablets have been visited
bool TabletManager::get_next_batch_tablets(size_t batch_size, std::vector<TabletSharedPtr>* tablets) {
    DCHECK(tablets);
    if (batch_size == 0) {
        return false;
    }
    size_t size = 0;
    const auto& tablets_shard = _tablets_shards[_cur_shard];
    std::shared_lock rlock(tablets_shard.lock);
    for (auto [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
        if (_shard_visited_tablet_ids.find(tablet_id) == _shard_visited_tablet_ids.end()) {
            tablets->push_back(tablet_ptr);
            _shard_visited_tablet_ids.insert(tablet_id);
            ++size;
            if (size >= batch_size) {
                // has enough unvisited tablets
                // also means that not all tablets has been visited
                return false;
            }
        }
    }
    // reach here means do not has enough tablets to pick in current shard
    _shard_visited_tablet_ids.clear();
    DCHECK(!_tablets_shards.empty());
    _cur_shard = (_cur_shard + 1) % _tablets_shards.size();
    if (_cur_shard == 0) {
        // the next shard is 0, then all tablets has been visited
        return true;
    } else {
        return false;
    }
}

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(CompactionType compaction_type, DataDir* data_dir,
                                                              std::pair<int32_t, int32_t> tablet_shards_range) {
    int64_t now_ms = UnixMillis();
    const std::string& compaction_type_str = compaction_type == CompactionType::BASE_COMPACTION ? "base" : "cumulative";
    // only do compaction if compaction #rowset > 1
    uint32_t highest_score = 1;
    TabletSharedPtr best_tablet;
    for (int32_t i = tablet_shards_range.first; i < tablet_shards_range.second; i++) {
        std::shared_lock rlock(_tablets_shards[i].lock);
        for (auto [tablet_id, tablet_ptr] : _tablets_shards[i].tablet_map) {
            if (tablet_ptr->keys_type() == PRIMARY_KEYS) {
                continue;
            }
            // A not-ready tablet maybe a newly created tablet under schema-change, skip it
            if (tablet_ptr->tablet_state() == TABLET_NOTREADY) {
                continue;
            }

            if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() || !tablet_ptr->is_used() ||
                !tablet_ptr->init_succeeded()) {
                continue;
            }

            int64_t last_failure_ts = 0;
            if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                if (tablet_ptr->last_cumu_compaction_failure_status() == TStatusCode::NOT_FOUND) {
                    if (now_ms - last_failure_ts <= 5 * config::cumulative_compaction_check_interval_seconds * 1000) {
                        VLOG(1) << "Too often to schedule no suitable compaction, skip it."
                                << "compaction_type=" << compaction_type_str
                                << ", last_failure_timestamp=" << last_failure_ts / 1000
                                << ", tablet_id=" << tablet_ptr->tablet_id();
                        continue;
                    }
                } else {
                    last_failure_ts = tablet_ptr->last_cumu_compaction_failure_time();
                    if (now_ms - last_failure_ts <= config::min_cumulative_compaction_failure_interval_sec * 1000) {
                        VLOG(1) << "Too often to schedule failure compaction, skip it."
                                << "compaction_type=" << compaction_type_str
                                << ", min_cumulative_compaction_failure_interval_sec="
                                << config::min_cumulative_compaction_failure_interval_sec
                                << ", last_failure_timestamp=" << last_failure_ts / 1000
                                << ", tablet_id=" << tablet_ptr->tablet_id();
                        continue;
                    }
                }
            } else if (compaction_type == CompactionType::BASE_COMPACTION) {
                last_failure_ts = tablet_ptr->last_base_compaction_failure_time();
                if (now_ms - last_failure_ts <= config::min_compaction_failure_interval_sec * 1000) {
                    VLOG(1) << "Too often to schedule failure compaction, skip it."
                            << "compaction_type=" << compaction_type_str
                            << ", min_compaction_failure_interval_sec=" << config::min_compaction_failure_interval_sec
                            << ", last_failure_timestamp=" << last_failure_ts / 1000
                            << ", tablet_id=" << tablet_ptr->tablet_id();
                    continue;
                }
            }

            if (compaction_type == CompactionType::BASE_COMPACTION) {
                if (!tablet_ptr->get_base_lock().try_lock()) {
                    continue;
                }
                tablet_ptr->get_base_lock().unlock();
            } else {
                if (!tablet_ptr->get_cumulative_lock().try_lock()) {
                    continue;
                }
                tablet_ptr->get_cumulative_lock().unlock();
            }

            uint32_t table_score = 0;
            {
                std::shared_lock rdlock(tablet_ptr->get_header_lock());
                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    table_score = tablet_ptr->calc_base_compaction_score();
                } else if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                    table_score = tablet_ptr->calc_cumulative_compaction_score();
                }
            }
            if (table_score > highest_score) {
                highest_score = table_score;
                best_tablet = tablet_ptr;
            }
        }
    }

    if (best_tablet != nullptr) {
        VLOG(1) << "Found the best tablet to compact. "
                << "compaction_type=" << compaction_type_str << " tablet_id=" << best_tablet->tablet_id()
                << " highest_score=" << highest_score;
        // TODO(lingbin): Remove 'max' from metric name, it would be misunderstood as the
        // biggest in history(like peak), but it is really just the value at current moment.
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            StarRocksMetrics::instance()->tablet_base_max_compaction_score.set_value(highest_score);
        } else {
            StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.set_value(highest_score);
        }
    }
    return best_tablet;
}

Status TabletManager::generate_pk_dump() {
    std::vector<TabletAndScore> pick_tablets;
    // 1. pick primary key tablet
    std::vector<TabletSharedPtr> tablet_ptr_list;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
            if (tablet_ptr->keys_type() != PRIMARY_KEYS) {
                continue;
            }

            tablet_ptr_list.push_back(tablet_ptr);
        }
    }
    // 2. generate pk dump if need
    for (const auto& tablet_ptr : tablet_ptr_list) {
        RETURN_IF_ERROR(tablet_ptr->updates()->generate_pk_dump_if_in_error_state());
    }
    return Status::OK();
}

// pick tablets to do primary index compaction
std::vector<TabletAndScore> TabletManager::pick_tablets_to_do_pk_index_major_compaction() {
    std::vector<TabletAndScore> pick_tablets;
    // 1. pick valid tablet, which score is larger than 0
    std::vector<TabletSharedPtr> tablet_ptr_list;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
            if (tablet_ptr->keys_type() != PRIMARY_KEYS) {
                continue;
            }
            // A not-ready tablet maybe a newly created tablet under schema-change, skip it
            if (tablet_ptr->tablet_state() == TABLET_NOTREADY) {
                continue;
            }

            tablet_ptr_list.push_back(tablet_ptr);
        }
    }
    for (const auto& tablet_ptr : tablet_ptr_list) {
        double score = tablet_ptr->updates()->get_pk_index_write_amp_score();
        if (score <= 0) {
            // score == 0 means this tablet's pk index doesn't need major compaction
            continue;
        }

        pick_tablets.emplace_back(tablet_ptr, score);
    }
    // 2. sort tablet by score, by ascending order.
    std::sort(pick_tablets.begin(), pick_tablets.end(), [](TabletAndScore& a, TabletAndScore& b) {
        // We try to compact tablet with small write amplification score first,
        // to improve the total write IO amplification
        return a.second < b.second;
    });
    if (!pick_tablets.empty()) {
        LOG(INFO) << fmt::format("found {} tablets to do pk index major compaction", pick_tablets.size());
    }
    return pick_tablets;
}

TabletSharedPtr TabletManager::find_best_tablet_to_do_update_compaction(DataDir* data_dir) {
    int64_t highest_score = 0;
    TabletSharedPtr best_tablet;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
            if (tablet_ptr->keys_type() != PRIMARY_KEYS) {
                continue;
            }
            // A not-ready tablet maybe a newly created tablet under schema-change, skip it
            if (tablet_ptr->tablet_state() == TABLET_NOTREADY) {
                continue;
            }

            if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() || !tablet_ptr->is_used() ||
                !tablet_ptr->init_succeeded()) {
                continue;
            }

            int64_t table_score = tablet_ptr->updates()->get_compaction_score();
            // tablet_score maybe negative, which means it's not worthy to do compaction
            if (table_score <= 0) {
                continue;
            }

            if (table_score > highest_score) {
                highest_score = table_score;
                best_tablet = tablet_ptr;
            }
        }
    }

    if (best_tablet != nullptr) {
        LOG(INFO) << "Found the best tablet to compact. "
                  << "compaction_type=update"
                  << " tablet_id=" << best_tablet->tablet_id() << " highest_score=" << highest_score;
        StarRocksMetrics::instance()->tablet_update_max_compaction_score.set_value(highest_score);
    }
    return best_tablet;
}

Status TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id, TSchemaHash schema_hash,
                                            std::string_view meta_binary, bool update_meta, bool force, bool restore,
                                            bool check_path) {
    std::unique_lock wlock(_get_tablets_shard_lock(tablet_id));
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    if (Status st = tablet_meta->deserialize(meta_binary); !st.ok()) {
        LOG(WARNING) << "Fail to load tablet because can not parse meta_binary string. "
                     << "tablet_id=" << tablet_id << " path=" << data_dir->path();
        return Status::InternalError("Invalid serialized tablet meta");
    }

    if (tablet_meta->tablet_id() != tablet_id) {
        LOG(WARNING) << "Mismatched tablet_id. expect=" << tablet_id << " real=" << tablet_meta->tablet_id();
        return Status::InternalError("mismatched tablet_id");
    }
    if (tablet_meta->schema_hash() != schema_hash) {
        LOG(WARNING) << "Mismatched schema_hash. expect=" << schema_hash << " real=" << tablet_meta->schema_hash()
                     << " tablet_id=" << tablet_id;
        return Status::InternalError("mismatched schema_hash");
    }
    if (tablet_meta->tablet_uid().hi == 0 && tablet_meta->tablet_uid().lo == 0) {
        LOG(WARNING) << "Invalid tablet uid."
                     << " tablet_id=" << tablet_meta->tablet_id() << " path=" << data_dir->path();
        return Status::InternalError("invalid tablet uid");
    }

    if (restore) {
        // we're restoring tablet from trash, tablet state should be changed from shutdown back to running
        tablet_meta->set_tablet_state(TABLET_RUNNING);
    }

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "Fail to load tablet_id=" << tablet_id;
        return Status::InternalError("Fail to create tablet");
    }

    // NOTE: method load_tablet_from_meta could be called by two cases as below
    // case 1: BE start;
    // case 2: Clone Task/Restore
    // For case 1 doesn't need path check because BE is just starting and not ready,
    // just check tablet meta status to judge whether tablet is delete is enough.
    // For case 2, If a tablet has just been copied to local BE,
    // it may be cleared by gc-thread(see perform_path_gc_by_tablet) because the tablet meta may not be loaded to memory.
    // So clone task should check path and then failed and retry in this case.
    if (check_path && !FileSystem::Default()->path_exists(tablet->schema_hash_path()).ok()) {
        LOG(WARNING) << "Fail to create table, tablet path not exists, path=" << tablet->schema_hash_path();
        return Status::NotFound("tablet path not exists");
    }
    Status init_st = tablet->init();
    if (tablet->tablet_state() == TABLET_SHUTDOWN) {
        if (init_st.ok()) {
            LOG(INFO) << "Loaded shutdown tablet " << tablet_id;
        } else {
            LOG(WARNING) << "Loaded shutdown tablet " << tablet_id << " with ignored failure: " << init_st.to_string();
        }
        std::unique_lock shutdown_tablets_wlock(_shutdown_tablets_lock);
        DroppedTabletInfo info{.tablet = tablet, .flag = kMoveFilesToTrash};
        _shutdown_tablets.emplace(tablet->tablet_id(), std::move(info));
        return Status::NotFound("tablet state is shutdown");
    }
    if (!init_st.ok()) {
        LOG(WARNING) << "Fail to init tablet " << tablet->full_name() << ": " << init_st.message();
        return Status::InternalError("tablet init failed: " + init_st.to_string());
    }
    // NOTE: We do not check tablet's initial version here, because if BE restarts when
    // one tablet is doing schema-change, we may meet empty tablet.
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "Fail to load tablet. it is in running state but without delta. "
                     << "tablet=" << tablet->full_name() << " path=" << data_dir->path();
        // tablet state is invalid, drop tablet
        return Status::InternalError("tablet in running state but without delta");
    }
    auto st = _add_tablet_unlocked(tablet, update_meta, force);
    LOG_IF(WARNING, !st.ok()) << "Fail to add tablet " << tablet->full_name();

    return st;
}

Status TabletManager::load_tablet_from_dir(DataDir* store, TTabletId tablet_id, SchemaHash schema_hash,
                                           const string& schema_hash_path, bool force, bool restore) {
    LOG(INFO) << "Loading tablet " << tablet_id << " from " << schema_hash_path << ". force=" << force
              << " restore=" << restore;
    // not add lock here, because load_tablet_from_meta already add lock
    std::string meta_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);
    // should change shard id before load tablet
    std::string shard_path = path_util::dir_name(path_util::dir_name(path_util::dir_name(meta_path)));
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    if (!TabletMeta::reset_tablet_uid(meta_path).ok()) {
        LOG(WARNING) << "Fail to set tablet uid when copied meta file. meta_path=" << meta_path;
        return Status::InternalError("reset tablet uid failed");
    }

    if (!FileSystem::Default()->path_exists(meta_path).ok()) {
        LOG(WARNING) << "Fail to find header file. meta_path=" << meta_path;
        return Status::NotFound("header file not exist");
    }

    auto tablet_meta = TabletMeta::create();
    if (!tablet_meta->create_from_file(meta_path).ok()) {
        LOG(WARNING) << "Fail to load tablet_meta. file_path=" << meta_path;
        return Status::InternalError("fail to create tablet meta from file");
    }
    // has to change shard id here, because meta file maybe copied from other source
    // its shard is different from local shard
    tablet_meta->set_shard_id(shard);
    std::string meta_binary;
    RETURN_IF_ERROR(tablet_meta->serialize(&meta_binary));
    auto st = load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force, restore, true);
    LOG_IF(WARNING, !st.ok()) << "fail to load tablet. meta_path=" << meta_path;
    return st;
}

Status TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    StarRocksMetrics::instance()->report_tablet_requests_total.increment(1);

    TabletSharedPtr tablet = get_tablet(tablet_info->tablet_id, false);
    if (tablet == nullptr) {
        LOG(WARNING) << "Fail to report tablet info: can't find tablet."
                     << " tablet=" << tablet_info->tablet_id;
        return Status::NotFound("tablet not found");
    }
    LOG(INFO) << "Reporting tablet info. tablet_id=" << tablet_info->tablet_id;

    tablet->build_tablet_report_info(tablet_info);
    VLOG(10) << "Reported tablet info.";
    return Status::OK();
}

Status TabletManager::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    DCHECK(tablets_info != nullptr);

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::vector<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);
    if (expire_txn_map.size() > 0) {
        LOG(INFO) << "Found " << expire_txn_map.size() << " expired tablet transactions";
    }

    StarRocksMetrics::instance()->report_all_tablets_requests_total.increment(1);

    size_t max_tablet_rowset_num = 0;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
            TTablet t_tablet;
            TTabletInfo tablet_info;
            tablet_ptr->build_tablet_report_info(&tablet_info);
            max_tablet_rowset_num = std::max(max_tablet_rowset_num, tablet_ptr->version_count());
            // find expired transaction corresponding to this tablet
            TabletInfo tinfo(tablet_id, tablet_ptr->schema_hash(), tablet_ptr->tablet_uid());
            auto find = expire_txn_map.find(tinfo);
            if (find != expire_txn_map.end()) {
                tablet_info.__set_transaction_ids(find->second);
                expire_txn_map.erase(find);
            }
            t_tablet.tablet_infos.push_back(tablet_info);

            if (!t_tablet.tablet_infos.empty()) {
                tablets_info->emplace(tablet_id, t_tablet);
            }
        }
    }
    LOG(INFO) << "Report all " << tablets_info->size()
              << " tablets info. max_tablet_rowset_num:" << max_tablet_rowset_num;
    StarRocksMetrics::instance()->max_tablet_rowset_num.set_value(max_tablet_rowset_num);
    return Status::OK();
}

Status TabletManager::start_trash_sweep() {
    {
        // we use this vector to save all tablet ptr for saving lock time.
        std::vector<TabletSharedPtr> all_tablets;
        for (auto& tablets_shard : _tablets_shards) {
            TabletMap& tablet_map = tablets_shard.tablet_map;
            {
                std::shared_lock rlock(tablets_shard.lock);
                for (auto& [tablet_id, tablet] : tablet_map) {
                    all_tablets.push_back(tablet);
                }
            }

            for (const auto& tablet : all_tablets) {
                tablet->delete_expired_inc_rowsets();
                tablet->delete_expired_stale_rowset();
            }
            all_tablets.clear();
        }
    }

    std::vector<DroppedTabletInfo> tablets_to_check;
    {
        std::shared_lock l(_shutdown_tablets_lock);
        tablets_to_check.reserve(_shutdown_tablets.size());
        for (auto& [tablet_id, info] : _shutdown_tablets) {
            tablets_to_check.emplace_back(info);
        }
    }

    std::vector<TTabletId> finished_tablets;
    finished_tablets.reserve(tablets_to_check.size());

    for (const auto& info : tablets_to_check) {
        auto& tablet = info.tablet;
        // The current thread has two references: _shutdown_tablets[i] and tablets_to_checks[i], check
        // if there is another thread has reference to the tablet, and if so, does not remove the tablet for now.
        if (tablet.use_count() > 2) {
            continue;
        }

        if (tablet->updates() != nullptr) {
            Status st = tablet->updates()->check_and_remove_rowset();
            if (!st.ok()) {
                LOG(WARNING) << "there are some rowsets still been referenced, drop tablet: " << tablet->tablet_id()
                             << " later";
                continue;
            }
        }

        bool remove_meta = false;
        TabletMeta tablet_meta;
        Status st = TabletMetaManager::get_tablet_meta(tablet->data_dir(), tablet->tablet_id(), tablet->schema_hash(),
                                                       &tablet_meta);
        if (st.ok()) {
            if (tablet_meta.tablet_uid() != tablet->tablet_uid()) {
                finished_tablets.push_back(tablet->tablet_id());
                LOG(INFO) << "Skipped remove tablet " << tablet_meta.tablet_id() << ": uid mismatch";
                continue;
            }
            if (tablet_meta.tablet_state() != TABLET_SHUTDOWN) { // This should not happen.
                finished_tablets.push_back(tablet->tablet_id());
                LOG(ERROR) << "Cannot remove normal state tablet " << tablet_meta.tablet_id();
                continue;
            }
            remove_meta = true;
        } else if (!st.is_not_found()) {
            LOG(ERROR) << "Fail to get tablet meta: " << st;
            break;
        }

        if (info.flag == kMoveFilesToTrash) {
            st = _move_tablet_directories_to_trash(tablet);
        } else if (info.flag == kDeleteFiles) {
            st = _remove_tablet_directories(tablet);
        } else {
            finished_tablets.push_back(tablet->tablet_id());
            LOG(WARNING) << "Invalid flag " << info.flag;
            continue;
        }

        if (st.ok() || st.is_not_found()) {
            finished_tablets.push_back(tablet->tablet_id());
            LOG(INFO) << ((info.flag == kMoveFilesToTrash) ? "Moved " : " Removed ") << tablet->tablet_id_path();
        } else {
            remove_meta = false;
            LOG(WARNING) << "Fail to remove or move " << tablet->tablet_id_path() << " :" << st;
        }

        if (remove_meta) {
            st = _remove_tablet_meta(tablet);
            LOG_IF(ERROR, !st.ok()) << "Fail to remove tablet meta of tablet " << tablet->tablet_id() << ": " << st;
        }
    }

    if (!finished_tablets.empty()) {
        std::unique_lock l(_shutdown_tablets_lock);
        for (auto tablet_id : finished_tablets) {
            _shutdown_tablets.erase(tablet_id);
        }
    }
    return Status::OK();
}

Status TabletManager::delete_shutdown_tablet(int64_t tablet_id) {
    DroppedTabletInfo to_delete;
    {
        std::shared_lock l(_shutdown_tablets_lock);
        size_t num = _shutdown_tablets.count(tablet_id);
        if (num == 0) {
            return Status::NotFound(fmt::format("tablet: {} not found in shutdown tables", tablet_id));
        }
        DroppedTabletInfo& info = _shutdown_tablets[tablet_id];
        if (info.tablet.use_count() != 1) {
            // there is usage, can not delete it
            return Status::ResourceBusy(fmt::format("used in somewhere, use count:{}", info.tablet.use_count()));
        }
        to_delete = info;
    }
    // We release the `_shutdown_tablets_lock` prematurely here, because we're gonna do some io operations.
    // This is safe because after a tablet is put into `_shutdown_tablets`, it'll never be used again(except those
    // in-flight usage which is temporal).
    auto& tablet = to_delete.tablet;
    Status st = Status::OK();
    if (to_delete.flag == kMoveFilesToTrash) {
        st = _move_tablet_directories_to_trash(tablet);
    } else if (to_delete.flag == kDeleteFiles) {
        st = _remove_tablet_directories(tablet);
    } else {
        LOG(WARNING) << "Invalid flag " << to_delete.flag;
        return Status::NotFound(fmt::format("invalid flag: {}", to_delete.flag));
    }

    if (st.ok() || st.is_not_found()) {
        LOG(INFO) << ((to_delete.flag == kMoveFilesToTrash) ? "Moved " : " Removed ") << tablet->tablet_id_path();
    } else {
        LOG(WARNING) << "Fail to remove or move " << tablet->tablet_id_path() << " :" << st;
        return st;
    }
    st = _remove_tablet_meta(tablet);
    LOG_IF(ERROR, !st.ok()) << "Fail to remove tablet meta of tablet " << tablet->tablet_id() << ", status:" << st;
    std::unique_lock l(_shutdown_tablets_lock);
    _shutdown_tablets.erase(tablet_id);
    return Status::OK();
}

Status TabletManager::delete_shutdown_tablet_before_clone(int64_t tablet_id) {
    std::unique_lock l(_get_tablets_shard_lock(tablet_id));
    auto old_tablet_ptr = _get_tablet_unlocked(tablet_id, true, nullptr);
    if (old_tablet_ptr != nullptr) {
        if (old_tablet_ptr->tablet_state() == TabletState::TABLET_SHUTDOWN) {
            // Must reset old_tablet_ptr, otherwise `delete_shutdown_tablet()` will never success.
            old_tablet_ptr.reset();
            int retry = RETRY_TIMES_ON_SHUTDOWN_TABLET_OCCUPIED;
            Status st = Status::OK();
            do {
                st = delete_shutdown_tablet(tablet_id);
                if (st.ok() || st.is_not_found()) {
                    LOG(INFO) << "before adding new cloned tablet, delete stale TABLET_SHUTDOWN tablet:" << tablet_id
                              << " successfully, retried " << RETRY_TIMES_ON_SHUTDOWN_TABLET_OCCUPIED - retry
                              << " times";
                    break;
                } else if (st.code() == TStatusCode::RESOURCE_BUSY) {
                    // The SHUTDOWN tablet being referenced by other thread is just a temporal state, so we retry
                    // a few times before mark this clone task failed to avoid too much wasted work.
                    retry--;
                    if (retry > 0) {
                        SleepFor(MonoDelta::FromSeconds(RETRY_INTERVAL_ON_SHUTDOWN_TABLET_OCCUPIED));
                    }
                } else {
                    break;
                }
            } while (retry > 0);
            if (!st.ok() && !st.is_not_found()) {
                LOG(WARNING) << "before adding new cloned tablet, delete stale TABLET_SHUTDOWN"
                             << " tablet failed after " << RETRY_TIMES_ON_SHUTDOWN_TABLET_OCCUPIED - retry
                             << " times retry, tablet:" << tablet_id << " st:" << st;
                return st;
            }
        } else {
            // normally this should not happen, unless in when doing clone, another create tablet task is performed
            return Status::AlreadyExist(fmt::format(
                    "delete_shutdown_tablet_before_clone error: tablet already exists tablet: {}", tablet_id));
        }
    }
    return Status::OK();
}

void TabletManager::register_clone_tablet(int64_t tablet_id) {
    TabletsShard& shard = _get_tablets_shard(tablet_id);
    std::unique_lock wlock(shard.lock);
    shard.tablets_under_clone.insert(tablet_id);
}

void TabletManager::unregister_clone_tablet(int64_t tablet_id) {
    TabletsShard& shard = _get_tablets_shard(tablet_id);
    std::unique_lock wlock(shard.lock);
    shard.tablets_under_clone.erase(tablet_id);
}

bool TabletManager::check_clone_tablet(int64_t tablet_id) {
    TabletsShard& shard = _get_tablets_shard(tablet_id);
    std::unique_lock wlock(shard.lock);
    return shard.tablets_under_clone.count(tablet_id) > 0;
}

void TabletManager::try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id, SchemaHash schema_hash,
                                                  const std::string& tablet_id_path) {
    // acquire the read lock, so that there is no creating tablet or load tablet from meta tasks
    // create tablet and load tablet task should check whether the dir exists
    TabletsShard& shard = _get_tablets_shard(tablet_id);
    std::shared_lock rlock(shard.lock);

    // check if meta already exists
    TabletMeta tablet_meta;
    Status st = TabletMetaManager::get_tablet_meta(data_dir, tablet_id, schema_hash, &tablet_meta);
    if (st.ok()) {
        LOG(INFO) << "Cannot remove schema_hash_path=" << tablet_id_path << ", tablet meta exist in meta store";
    } else if (st.is_not_found()) {
        if (shard.tablets_under_clone.count(tablet_id) > 0) {
            LOG(INFO) << "Cannot move schema_hash_path=" << tablet_id_path << " to trash, tablet is under clone";
            return;
        }

        if (st = move_to_trash(tablet_id_path); st.ok()) {
            LOG(INFO) << "Moved " << tablet_id_path << " to trash";
        } else {
            LOG(WARNING) << "Fail to move " << tablet_id_path << " to trash: " << st;
        }
    } else {
        LOG(WARNING) << "Fail to get tablet meta: " << st;
    }
}

bool TabletManager::try_schema_change_lock(TTabletId tablet_id) {
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));
    TabletMap& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "Fail to lock nonexistent tablet_id=" << tablet_id;
        return false;
    } else {
        return _schema_change_lock_tbl.try_lock(it->first);
    }
}

void TabletManager::release_schema_change_lock(TTabletId tablet_id) {
    (void)_schema_change_lock_tbl.unlock(tablet_id);
}

void TabletManager::update_root_path_info(std::map<std::string, DataDirInfo>* path_map, size_t* tablet_count) {
    DCHECK(tablet_count != nullptr);
    *tablet_count = 0;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet] : tablets_shard.tablet_map) {
            ++(*tablet_count);
            int64_t data_size = tablet->tablet_footprint();
            auto iter = path_map->find(tablet->data_dir()->path());
            if (iter == path_map->end()) {
                continue;
            }
            if (iter->second.is_used) {
                iter->second.data_used_capacity += data_size;
            }
        }
    }
}

void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    std::vector<TabletSharedPtr> related_tablets;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
            if (tablet_ptr->tablet_state() != TABLET_RUNNING) {
                continue;
            }

            if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() || !tablet_ptr->is_used() ||
                !tablet_ptr->init_succeeded()) {
                continue;
            }
            related_tablets.push_back(tablet_ptr);
        }
    }
    for (const TabletSharedPtr& tablet : related_tablets) {
        tablet->do_tablet_meta_checkpoint();
    }
}

void TabletManager::_build_tablet_stat() {
    _tablet_stat_cache.clear();
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (const auto& [tablet_id, tablet] : tablets_shard.tablet_map) {
            TTabletStat stat;
            stat.tablet_id = tablet_id;
            if (tablet->updates()) {
                auto [row_num, data_size] = tablet->updates()->num_rows_and_data_size();
                stat.__set_row_num(row_num);
                stat.__set_data_size(data_size);
            } else {
                stat.__set_data_size(tablet->tablet_footprint());
                stat.__set_row_num(tablet->num_rows());
            }
            stat.__set_version_count(tablet->version_count());
            _tablet_stat_cache.emplace(tablet_id, stat);
        }
    }
}

Status TabletManager::_create_inital_rowset_unlocked(const TCreateTabletReq& request, Tablet* tablet) {
    Status st;
    if (request.version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1. request.version=" << request.version;
        return Status::InvalidArgument("invalid version");
    } else {
        Version version(0, request.version);
        VLOG(3) << "begin to create init version. version=" << version;
        RowsetSharedPtr new_rowset;
        do {
            RowsetWriterContext context;
            context.rowset_id = StorageEngine::instance()->next_rowset_id();
            context.tablet_uid = tablet->tablet_uid();
            context.tablet_id = tablet->tablet_id();
            context.partition_id = tablet->partition_id();
            context.tablet_schema_hash = tablet->schema_hash();
            context.rowset_path_prefix = tablet->schema_hash_path();
            context.tablet_schema = tablet->tablet_schema();
            context.rowset_state = VISIBLE;
            context.version = version;
            // there is no data in init rowset, so overlapping info is unknown.
            context.segments_overlap = OVERLAP_UNKNOWN;

            std::unique_ptr<RowsetWriter> rowset_writer;
            st = RowsetFactory::create_rowset_writer(context, &rowset_writer);
            if (!st.ok()) {
                LOG(WARNING) << "failed to init rowset writer for tablet " << tablet->full_name() << ": " << st;
                break;
            }
            st = rowset_writer->flush();
            if (!st.ok()) {
                LOG(WARNING) << "failed to flush rowset writer for tablet " << tablet->full_name() << ": " << st;
                break;
            }
            auto ret = rowset_writer->build();
            if (!ret.ok()) return ret.status();
            new_rowset = std::move(ret.value());
            st = tablet->add_rowset(new_rowset, false);
            if (!st.ok()) {
                LOG(WARNING) << "failed to add rowset for tablet " << tablet->full_name() << ": " << st;
                break;
            }
        } while (false);

        // Unregister index and delete files(index and data) if failed
        if (!st.ok()) {
            LOG(WARNING) << "fail to create initial rowset: " << st << " version=" << version;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            return Status::InternalError(fmt::format("fail to create initial rowset: {}", st.to_string()));
        }
    }
    tablet->set_cumulative_layer_point(request.version + 1);
    // NOTE: should not save tablet meta here, because it will be saved if add to map successfully

    return st;
}

Status TabletManager::_create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store,
                                                   bool is_schema_change, const Tablet* base_tablet,
                                                   TabletMetaSharedPtr* tablet_meta) {
    uint32_t next_unique_id = 0;
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    TCreateTabletReq normal_request = request;
    if (request.tablet_schema.storage_type == TStorageType::COLUMN_WITH_ROW) {
        // TODO: support schemachange
        if (is_schema_change) {
            return Status::NotSupported("column with row store does not support schema change");
        }
        normal_request.tablet_schema.columns.emplace_back();
        TColumn& column = normal_request.tablet_schema.columns.back();
        column.__set_column_name(Schema::FULL_ROW_COLUMN);
        TColumnType ctype;
        ctype.__set_type(TPrimitiveType::VARCHAR);
        ctype.__set_len(TypeDescriptor::MAX_VARCHAR_LENGTH);
        column.__set_column_type(ctype);
        column.__set_aggregation_type(TAggregationType::REPLACE);
        column.__set_is_allow_null(false);
        column.__set_default_value("");
    }
    if (!is_schema_change) {
        next_unique_id = normal_request.tablet_schema.columns.size();
        for (uint32_t col_idx = 0; col_idx < next_unique_id; ++col_idx) {
            col_idx_to_unique_id[col_idx] = col_idx;
        }
    } else {
        auto base_tablet_schema = base_tablet->tablet_schema();
        next_unique_id = base_tablet_schema->next_column_unique_id();
        size_t old_num_columns = base_tablet_schema->num_columns();
        const auto& new_columns = request.tablet_schema.columns;
        for (uint32_t new_col_idx = 0; new_col_idx < new_columns.size(); ++new_col_idx) {
            const TColumn& column = new_columns[new_col_idx];
            // For schema change, compare old_tablet and new_tablet:
            // 1. if column exist in both new_tablet and old_tablet, choose the column's
            //    unique_id in old_tablet to be the column's ordinal number in new_tablet
            // 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
            //    to the new column
            size_t old_col_idx = 0;
            for (old_col_idx = 0; old_col_idx < old_num_columns; ++old_col_idx) {
                auto old_name = base_tablet_schema->column(old_col_idx).name();
                if (old_name == column.column_name) {
                    uint32_t old_unique_id = base_tablet_schema->column(old_col_idx).unique_id();
                    if (normal_request.tablet_schema.schema_version <= base_tablet_schema->schema_version() + 1) {
                        if (column.col_unique_id > 0) {
                            DCHECK(column.col_unique_id == old_unique_id);
                            if (column.col_unique_id != old_unique_id) {
                                std::string msg = strings::Substitute(
                                        "Tablet[$0] column[$1] has different column unique id during schema change. "
                                        "$2(FE) "
                                        "vs $3(BE)",
                                        base_tablet->tablet_id(), old_col_idx, column.col_unique_id, old_unique_id);
                                return Status::InternalError(msg);
                            }
                        }
                    }

                    col_idx_to_unique_id[new_col_idx] = old_unique_id;
                    // During linked schema change, the now() default value is stored in TabletMeta.
                    // When receiving a new schema change request, the last default value stored should be
                    // remained instead of changing.
                    if (base_tablet_schema->column(old_col_idx).has_default_value()) {
                        normal_request.tablet_schema.columns[new_col_idx].__set_default_value(
                                base_tablet_schema->column(old_col_idx).default_value());
                    }
                    break;
                }
            }
            // Not exist in old tablet, it is a new added column
            if (old_col_idx == old_num_columns) {
                col_idx_to_unique_id[new_col_idx] = next_unique_id++;
            }
        }
    }
    LOG(INFO) << "creating tablet meta. next_unique_id=" << next_unique_id;

    uint64_t shard_id = 0;
    if (!store->get_shard(&shard_id).ok()) {
        LOG(WARNING) << "Fail to get root path shard";
        return Status::InternalError("fail to get root path shard");
    }
    // We generate a new tablet_uid for this new tablet.
    return TabletMeta::create(normal_request, TabletUid::gen_uid(), shard_id, next_unique_id, col_idx_to_unique_id,
                              tablet_meta);
}

Status TabletManager::_drop_tablet_unlocked(TTabletId tablet_id, TabletDropFlag flag) {
    StarRocksMetrics::instance()->drop_tablet_requests_total.increment(1);

    if (flag != kDeleteFiles && flag != kMoveFilesToTrash && flag != kKeepMetaAndFiles) {
        return Status::InvalidArgument(fmt::format("invalid TabletDropFlag {}", (int)flag));
    }

    TabletMap& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "Fail to drop nonexistent tablet " << tablet_id;
        return Status::NotFound(strings::Substitute("tablet $0 not fount", tablet_id));
    }

    LOG(INFO) << "Start to drop tablet " << tablet_id;
    TabletSharedPtr dropped_tablet = it->second;
    tablet_map.erase(it);
    _remove_tablet_from_partition(*dropped_tablet);
    if (config::enable_event_based_compaction_framework) {
        dropped_tablet->stop_compaction();
        StorageEngine::instance()->compaction_manager()->remove_candidate(dropped_tablet->tablet_id());
    }

    DroppedTabletInfo drop_info{.tablet = dropped_tablet, .flag = flag};

    if (flag == kDeleteFiles) {
        {
            // NOTE: Other threads may save the tablet meta back to storage again after we
            // have deleted it here, and the tablet will reappear after restarted.
            // To prevent this, set the tablet state to `SHUTDOWN` first before removing tablet
            // meta from storage, and assuming that no thread will change the tablet state back
            // to 'RUNNING' from 'SHUTDOWN'.
            std::unique_lock l(dropped_tablet->get_header_lock());
            CHECK(dropped_tablet->set_tablet_state(TABLET_SHUTDOWN).ok());
        }

        // Remove tablet meta from storage, crash the program if failed.
        if (auto st = _remove_tablet_meta(dropped_tablet); !st.ok()) {
            LOG(FATAL) << "Fail to remove tablet meta: " << st;
        }

        // Remove the tablet directory in background to avoid holding the lock of tablet map shard for long.
        std::unique_lock l(_shutdown_tablets_lock);
        _shutdown_tablets.emplace(tablet_id, std::move(drop_info));
    } else if (flag == kMoveFilesToTrash) {
        {
            // See comments above
            std::unique_lock l(dropped_tablet->get_header_lock());
            CHECK(dropped_tablet->set_tablet_state(TABLET_SHUTDOWN).ok());
            dropped_tablet->save_meta();
        }

        std::unique_lock l(_shutdown_tablets_lock);
        _shutdown_tablets.emplace(tablet_id, std::move(drop_info));
    } else {
        DCHECK_EQ(kKeepMetaAndFiles, flag);
    }
    dropped_tablet->deregister_tablet_from_dir();
    LOG(INFO) << "Succeed to drop tablet " << tablet_id;
    return Status::OK();
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id) {
    TabletMap& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    return it != tablet_map.end() ? it->second : nullptr;
}

void TabletManager::_add_tablet_to_partition(const Tablet& tablet) {
    std::unique_lock wlock(_partition_tablet_map_lock);
    _partition_tablet_map[tablet.partition_id()].insert(tablet.get_tablet_info());
}

void TabletManager::_remove_tablet_from_partition(const Tablet& tablet) {
    std::unique_lock wlock(_partition_tablet_map_lock);
    _partition_tablet_map[tablet.partition_id()].erase(tablet.get_tablet_info());
    if (_partition_tablet_map[tablet.partition_id()].empty()) {
        _partition_tablet_map.erase(tablet.partition_id());
    }
}

void TabletManager::get_tablets_by_partition(int64_t partition_id, std::vector<TabletInfo>& tablet_infos) {
    std::shared_lock rlock(_partition_tablet_map_lock);
    auto search = _partition_tablet_map.find(partition_id);
    if (search != _partition_tablet_map.end()) {
        tablet_infos.reserve(search->second.size());
        tablet_infos.assign(search->second.begin(), search->second.end());
    }
}

void TabletManager::get_tablets_basic_infos(int64_t table_id, int64_t partition_id, int64_t tablet_id,
                                            std::vector<TabletBasicInfo>& tablet_infos) {
    if (tablet_id != -1) {
        auto tablet = get_tablet(tablet_id, true, nullptr);
        if (tablet) {
            auto& info = tablet_infos.emplace_back();
            tablet->get_basic_info(info);
        }
    } else if (partition_id != -1) {
        vector<int64_t> tablet_ids;
        {
            std::shared_lock rlock(_partition_tablet_map_lock);
            auto search = _partition_tablet_map.find(partition_id);
            if (search != _partition_tablet_map.end()) {
                for (auto tablet_id : search->second) {
                    tablet_ids.push_back(tablet_id.tablet_id);
                }
            }
        }
        for (int64_t tablet_id : tablet_ids) {
            auto tablet = get_tablet(tablet_id, true, nullptr);
            if (tablet) {
                auto& info = tablet_infos.emplace_back();
                tablet->get_basic_info(info);
            }
        }
    } else {
        for (auto& shard : _tablets_shards) {
            std::shared_lock rlock(shard.lock);
            for (auto& itr : shard.tablet_map) {
                auto& tablet = itr.second;
                if (table_id == -1 || tablet->tablet_meta()->table_id() == table_id) {
                    auto& info = tablet_infos.emplace_back();
                    tablet->get_basic_info(info);
                }
            }
        }
        // order by table_id, partition_id, tablet_id by default
        std::sort(tablet_infos.begin(), tablet_infos.end(), [](const TabletBasicInfo& a, const TabletBasicInfo& b) {
            if (a.partition_id == b.partition_id) {
                return a.tablet_id < b.tablet_id;
            }
            if (a.table_id == b.table_id) {
                return a.partition_id < b.partition_id;
            }
            return a.table_id < b.table_id;
        });
    }
}

std::shared_mutex& TabletManager::_get_tablets_shard_lock(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).lock;
}

TabletManager::TabletMap& TabletManager::_get_tablet_map(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).tablet_map;
}

TabletManager::TabletsShard& TabletManager::_get_tablets_shard(TTabletId tabletId) {
    return _tablets_shards[tabletId & _tablets_shards_mask];
}

Status TabletManager::create_tablet_from_meta_snapshot(DataDir* store, TTabletId tablet_id, SchemaHash schema_hash,
                                                       const string& schema_hash_path, bool restore) {
    auto meta_path = strings::Substitute("$0/meta", schema_hash_path);
    auto shard_path = path_util::dir_name(path_util::dir_name(path_util::dir_name(meta_path)));
    auto shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    auto shard = stol(shard_str);

    auto meta_file = SnapshotManager::instance()->parse_snapshot_meta(meta_path);
    if (!meta_file.ok()) {
        LOG(WARNING) << "Fail to parse " << meta_path << ": " << meta_file.status();
        return meta_file.status();
    }
    auto val = std::move(meta_file).value();
    auto snapshot_meta = &val;

    if (snapshot_meta->snapshot_type() != SNAPSHOT_TYPE_FULL) {
        return Status::InternalError("not full snapshot");
    }
    if (snapshot_meta->tablet_meta().tablet_id() != tablet_id) {
        return Status::InternalError("mismatched tablet id");
    }
    if (snapshot_meta->tablet_meta().schema_hash() != schema_hash) {
        return Status::InternalError("mismatched schema hash");
    }
    if (snapshot_meta->tablet_meta().updates().next_log_id() != 0) {
        return Status::InternalError("non-zero log id in tablet meta");
    }
    LOG(INFO) << strings::Substitute("create tablet from snapshot tablet:$0 version:$1 path:$2", tablet_id,
                                     snapshot_meta->snapshot_version(), schema_hash_path);

    RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(snapshot_meta, schema_hash_path));

    // Set of rowset id collected from rowset meta.
    std::set<uint32_t> set1;
    for (const auto& rowset_meta_pb : snapshot_meta->rowset_metas()) {
        set1.insert(rowset_meta_pb.rowset_seg_id());
    }
    if (set1.size() != snapshot_meta->rowset_metas().size()) {
        return Status::InternalError("has duplicate rowset id");
    }
    // Set of rowset id collected from tablet meta.
    std::set<uint32_t> set2;
    auto& v = snapshot_meta->tablet_meta().updates().versions(0).rowsets();
    set2.insert(v.begin(), v.end());
    if (!(set1 == set2)) {
        return Status::InternalError("mismatched rowset list");
    }

    // Assign a new tablet uid and shard id.
    auto uid = TabletUid::gen_uid();
    snapshot_meta->tablet_meta().mutable_tablet_uid()->set_hi(uid.hi);
    snapshot_meta->tablet_meta().mutable_tablet_uid()->set_lo(uid.lo);
    snapshot_meta->tablet_meta().set_shard_id(shard);

    WriteBatch wb;
    auto meta_store = store->get_meta();
    for (const RowsetMetaPB& rowset_meta_pb : snapshot_meta->rowset_metas()) {
        RETURN_IF_ERROR(TabletMetaManager::put_rowset_meta(store, &wb, tablet_id, rowset_meta_pb));
    }
    for (const auto& [segid, dv] : snapshot_meta->delete_vectors()) {
        RETURN_IF_ERROR(TabletMetaManager::put_del_vector(store, &wb, tablet_id, segid, dv));
    }
    for (const auto& [segid, dcg] : snapshot_meta->delta_column_groups()) {
        RETURN_IF_ERROR(TabletMetaManager::put_delta_column_group(store, &wb, tablet_id, segid, dcg));
    }
    RETURN_IF_ERROR(TabletMetaManager::put_tablet_meta(store, &wb, snapshot_meta->tablet_meta()));

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(&snapshot_meta->tablet_meta());
    if (restore && tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
        // we're restoring tablet from trash, tablet state should be changed from shutdown back to running
        tablet_meta->set_tablet_state(TABLET_RUNNING);
    }
    if (tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "Fail to load snapshot from " << schema_hash_path << ": tablet has been shutdown";
        return Status::InternalError("tablet state is shutdown");
    }
    // DO NOT access tablet->updates() until tablet has been init()-ed.
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, store);
    if (tablet == nullptr) {
        LOG(WARNING) << "Fail to load tablet " << tablet_id;
        return Status::InternalError("Fail to create tablet");
    }
    // NOTE: do NOT touch snapshot_meta->tablet_meta since here, it has been modified by
    // `Tablet::create_tablet_from_meta`.

    if (!FileSystem::Default()->path_exists(tablet->schema_hash_path()).ok()) {
        return Status::NotFound("tablet path not exists");
    }

    std::unique_lock l(_get_tablets_shard_lock(tablet_id));
    RETURN_IF_ERROR(meta_store->write_batch(&wb));

    if (!tablet->init().ok()) {
        LOG(WARNING) << "Fail to init cloned tablet " << tablet_id << ", try to clear meta store";
        wb.Clear();
        RETURN_IF_ERROR(TabletMetaManager::clear_del_vector(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::clear_rowset(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::clear_log(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::remove_tablet_meta(store, &wb, tablet_id, schema_hash));
        auto st = meta_store->write_batch(&wb);
        LOG_IF(WARNING, !st.ok()) << "Fail to clear meta store: " << st;
        return Status::InternalError("tablet init failed");
    }
    auto st = _add_tablet_unlocked(tablet, true, false);
    LOG_IF(WARNING, !st.ok()) << "Fail to add cloned tablet " << tablet_id << ": " << st;
    return st;
}

Status TabletManager::_remove_tablet_meta(const TabletSharedPtr& tablet) {
    if (tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        return tablet->updates()->clear_meta();
    } else {
        return TabletMetaManager::remove(tablet->data_dir(), tablet->tablet_id(), tablet->schema_hash());
    }
}

Status TabletManager::_remove_tablet_directories(const TabletSharedPtr& tablet) {
    return fs::remove_all(tablet->tablet_id_path());
}

Status TabletManager::_move_tablet_directories_to_trash(const TabletSharedPtr& tablet) {
    if (config::trash_file_expire_time_sec == 0) {
        return _remove_tablet_directories(tablet);
    }
    // take snapshot of tablet meta for recovery
    if (tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        RETURN_IF_ERROR(SnapshotManager::instance()->make_snapshot_on_tablet_meta(tablet));
    } else {
        auto meta_file_path = fmt::format("{}/{}.hdr", tablet->schema_hash_path(), tablet->tablet_id());
        RETURN_IF_ERROR(tablet->tablet_meta()->save(meta_file_path));
    }
    // move tablet directories to ${storage_root_path}/trash
    return move_to_trash(tablet->tablet_id_path());
}

std::unordered_map<TTabletId, vector<pair<uint32_t, uint32_t>>> TabletManager::get_tablets_need_repair_compaction() {
    std::unordered_map<TTabletId, std::vector<std::pair<uint32_t, uint32_t>>> ret;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(tablets_shard.lock);
        for (auto [tablet_id, tablet_ptr] : tablets_shard.tablet_map) {
            if (tablet_ptr->updates() == nullptr) {
                continue;
            }
            auto st = tablet_ptr->updates()->list_rowsets_need_repair_compaction();
            if (!st.ok()) {
                continue;
            }
            if (st.value().empty()) {
                continue;
            }
            ret.emplace(tablet_id, std::move(st.value()));
        }
    }
    return ret;
}

} // end namespace starrocks
