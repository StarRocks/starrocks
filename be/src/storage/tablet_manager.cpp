// This file is made available under Elastic License 2.0.
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

#include <re2/re2.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <cstdlib>
#include <ctime>
#include <memory>

#include "env/env.h"
#include "gutil/strings/strcat.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/reader.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/schema_change.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "util/file_utils.h"
#include "util/path_util.h"
#include "util/scoped_cleanup.h"
#include "util/starrocks_metrics.h"

using std::list;
using std::map;
using std::set;
using strings::Substitute;

namespace starrocks {

static bool _cmp_tablet_by_create_time(const TabletSharedPtr& a, const TabletSharedPtr& b) {
    return a->creation_time() < b->creation_time();
}

TabletManager::TabletManager(MemTracker* mem_tracker, int32_t tablet_map_lock_shard_size)
        : _mem_tracker(mem_tracker),
          _tablets_shards_size(tablet_map_lock_shard_size),
          _tablets_shards_mask(tablet_map_lock_shard_size - 1),
          _last_update_stat_ms(0) {
    CHECK_GT(_tablets_shards_size, 0);
    CHECK_EQ(_tablets_shards_size & _tablets_shards_mask, 0);
    _tablets_shards.resize(_tablets_shards_size);
    for (auto& tablets_shard : _tablets_shards) {
        tablets_shard.lock = std::make_unique<std::shared_mutex>();
    }
}

Status TabletManager::_add_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, const TabletSharedPtr& tablet,
                                           bool update_meta, bool force) {
    Status res = Status::OK();
    TabletSharedPtr existed_tablet = nullptr;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    for (const TabletSharedPtr& item : tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            existed_tablet = item;
            break;
        }
    }

    if (existed_tablet == nullptr) {
        return _add_tablet_to_map_unlocked(tablet_id, schema_hash, tablet, update_meta, false /*keep_state*/,
                                           false /*drop_old*/);
    }

    if (!force) {
        if (existed_tablet->tablet_path() == tablet->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id=" << tablet_id
                         << " tablet_path=" << tablet->tablet_path();
            return Status::InternalError("tablet already exists");
        }
        if (existed_tablet->data_dir() == tablet->data_dir()) {
            LOG(WARNING) << "add tablet with same data dir twice! tablet_id=" << tablet_id;
            return Status::InternalError("tablet already exists");
        }
    }

    existed_tablet->obtain_header_rdlock();
    const RowsetSharedPtr old_rowset = existed_tablet->rowset_with_max_version();
    const RowsetSharedPtr new_rowset = tablet->rowset_with_max_version();

    // If new tablet is empty, it is a newly created schema change tablet.
    // the old tablet is dropped before add tablet. it should not exist old tablet
    if (new_rowset == nullptr) {
        existed_tablet->release_header_lock();
        // it seems useless to call unlock and return here.
        // it could prevent error when log level is changed in the future.
        LOG(FATAL) << "new tablet is empty and old tablet exists. it should not happen."
                   << " tablet_id=" << tablet_id;
        return Status::InternalError("tablet already exists");
    }
    int64_t old_time = old_rowset == nullptr ? -1 : old_rowset->creation_time();
    int64_t new_time = new_rowset->creation_time();
    int32_t old_version = old_rowset == nullptr ? -1 : old_rowset->end_version();
    int32_t new_version = new_rowset->end_version();
    existed_tablet->release_header_lock();

    // In restore process, we replace all origin files in tablet dir with
    // the downloaded snapshot files. Then we try to reload tablet header.
    // force == true means we forcibly replace the Tablet in tablet_map
    // with the new one.
    bool keep_state = force;
    if (force || (new_version > old_version || (new_version == old_version && new_time > old_time))) {
        // check if new tablet's meta is in store and add new tablet's meta to meta store
        res = _add_tablet_to_map_unlocked(tablet_id, schema_hash, tablet, update_meta, keep_state, true /*drop_old*/);
    } else {
        res = Status::InternalError("tablet already exists");
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << " res=" << res.to_string() << " tablet_id=" << tablet_id
                 << " schema_hash=" << schema_hash << " old_version=" << old_version << " new_version=" << new_version
                 << " old_time=" << old_time << " new_time=" << new_time
                 << " old_tablet_path=" << existed_tablet->tablet_path()
                 << " new_tablet_path=" << tablet->tablet_path();

    return res;
}

Status TabletManager::_add_tablet_to_map_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                                  const TabletSharedPtr& tablet, bool update_meta, bool keep_state,
                                                  bool drop_old) {
    // check if new tablet's meta is in store and add new tablet's meta to meta store
    if (update_meta) {
        // call tablet save meta in order to valid the meta
        tablet->save_meta();
    }
    if (drop_old) {
        // If the new tablet is fresher than the existing one, then replace
        // the existing tablet with the new one.
        Status st = _drop_tablet_unlocked(tablet_id, schema_hash, keep_state);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to drop old tablet when add new tablet. tablet_id=" << tablet_id;
            return st;
        }
    }
    // Register tablet into DataDir, so that we can manage tablet from
    // the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    tablet->register_tablet_into_dir();
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map[tablet_id].table_arr.push_back(tablet);
    tablet_map[tablet_id].table_arr.sort(_cmp_tablet_by_create_time);
    _add_tablet_to_partition(*tablet);

    return Status::OK();
}

bool TabletManager::_check_tablet_id_exist_unlocked(TTabletId tablet_id) {
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    return it != tablet_map.end() && !it->second.table_arr.empty();
}

Status TabletManager::create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores) {
    StarRocksMetrics::instance()->create_tablet_requests_total.increment(1);

    int64_t tablet_id = request.tablet_id;
    int32_t schema_hash = request.tablet_schema.schema_hash;
    LOG(INFO) << "Creating tablet " << tablet_id;

    std::unique_lock wlock(_get_tablets_shard_lock(tablet_id));
    // Make create_tablet operation to be idempotent:
    // 1. Return true if tablet with same tablet_id and schema_hash exist;
    //           false if tablet with same tablet_id but different schema_hash exist.
    // 2. When this is an alter task, if the tablet(both tablet_id and schema_hash are
    // same) already exist, then just return true(an duplicate request). But if
    // tablet_id exist but with different schema_hash, return an error(report task will
    // eventually trigger its deletion).
    if (request.tablet_schema.keys_type == TKeysType::PRIMARY_KEYS) {
        TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash, true, nullptr);
        if (tablet != nullptr && tablet->tablet_state() != TABLET_SHUTDOWN) {
            return Status::OK();
        } else if (tablet != nullptr) {
            return Status::InternalError("primary key tablet still resident in shutdown queue");
        }
    } else if (_check_tablet_id_exist_unlocked(tablet_id)) {
        TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash);
        if (tablet != nullptr) {
            LOG(INFO) << "Created tablet " << tablet_id << ": already exist";
            return Status::OK();
        } else {
            LOG(WARNING) << "Fail to create tablet " << tablet_id << ": tablet exist but with different schema_hash";
            StarRocksMetrics::instance()->create_tablet_requests_failed.increment(1);
            return Status::InternalError("tablet exist but with different schema hash");
        }
    }

    TabletSharedPtr base_tablet = nullptr;
    bool is_schema_change = false;
    // If the CreateTabletReq has base_tablet_id then it is a alter-tablet request
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        is_schema_change = true;
        base_tablet = _get_tablet_unlocked(request.base_tablet_id, request.base_schema_hash);
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
    TabletSharedPtr tablet = _internal_create_tablet_unlocked(AlterTabletType::SCHEMA_CHANGE, request, is_schema_change,
                                                              base_tablet.get(), stores);
    if (tablet == nullptr) {
        LOG(WARNING) << "Fail to create tablet " << request.tablet_id;
        StarRocksMetrics::instance()->create_tablet_requests_failed.increment(1);
        return Status::InternalError("fail to create tablet");
    }

    LOG(INFO) << "Created tablet " << tablet_id;
    return Status::OK();
}

TabletSharedPtr TabletManager::_internal_create_tablet_unlocked(const AlterTabletType alter_type,
                                                                const TCreateTabletReq& request,
                                                                const bool is_schema_change, const Tablet* base_tablet,
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

    int64_t new_tablet_id = request.tablet_id;
    int32_t new_schema_hash = request.tablet_schema.schema_hash;

    // should remove the tablet's pending_id no matter create-tablet success or not
    DataDir* data_dir = tablet->data_dir();

    // TODO(yiguolei)
    // the following code is very difficult to understand because it mixed alter tablet v2
    // and alter tablet v1 should remove alter tablet v1 code after v0.12
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
                LOG(INFO) << "request for alter-tablet v2, do not add alter task to tablet";
                // if this is a new alter tablet, has to set its state to not ready
                // because schema change hanlder depends on it to check whether history data
                // convert finished
                tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
            } else {
                // add alter task to new tablet if it is a new tablet during schema change
                tablet->add_alter_task(base_tablet->tablet_id(), base_tablet->schema_hash(), std::vector<Version>(),
                                       alter_type);
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
        status = _add_tablet_unlocked(new_tablet_id, new_schema_hash, tablet, true, false);
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
        status = _drop_tablet_unlocked(new_tablet_id, new_schema_hash, false);
        LOG_IF(WARNING, !status.ok()) << "Fail to drop tablet when create tablet failed: " << status.to_string();
    } else {
        tablet->delete_all_files();
        (void)TabletMetaManager::remove(data_dir, new_tablet_id, new_schema_hash);
    }
    return nullptr;
}

static std::string _gen_tablet_dir(const std::string& dir, int16_t shard_id, int64_t tablet_id) {
    std::string path = dir;
    path = path_util::join_path_segments(path, DATA_PREFIX);
    path = path_util::join_path_segments(path, std::to_string(shard_id));
    path = path_util::join_path_segments(path, std::to_string(tablet_id));
    return path;
}

TabletSharedPtr TabletManager::_create_tablet_meta_and_dir_unlocked(const TCreateTabletReq& request,
                                                                    const bool is_schema_change,
                                                                    const Tablet* base_tablet,
                                                                    const std::vector<DataDir*>& data_dirs) {
    for (const auto& data_dir : data_dirs) {
        TabletMetaSharedPtr tablet_meta;
        Status st = _create_tablet_meta_unlocked(request, data_dir, is_schema_change, base_tablet, &tablet_meta);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to create tablet meta. root=" << data_dir->path() << " status=" << st.to_string();
            continue;
        }

        std::string tablet_dir = _gen_tablet_dir(data_dir->path(), tablet_meta->shard_id(), request.tablet_id);
        std::string schema_hash_dir =
                path_util::join_path_segments(tablet_dir, std::to_string(request.tablet_schema.schema_hash));

        st = FileUtils::create_dir(schema_hash_dir);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to create " << schema_hash_dir << ": " << st.to_string();
            continue;
        }

        TabletSharedPtr new_tablet = Tablet::create_tablet_from_meta(_mem_tracker, tablet_meta, data_dir);
        DCHECK(new_tablet != nullptr);
        return new_tablet;
    }
    return nullptr;
}

Status TabletManager::drop_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool keep_state) {
    std::unique_lock wlock(_get_tablets_shard_lock(tablet_id));
    return _drop_tablet_unlocked(tablet_id, schema_hash, keep_state);
}

// Drop specified tablet, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && the dropping tablet is a base-tablet:
//          base-tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet directly and clear schema change info.
Status TabletManager::_drop_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool keep_state) {
    StarRocksMetrics::instance()->drop_tablet_requests_total.increment(1);

    // Fetch tablet which need to be dropped
    TabletSharedPtr tablet_to_drop = _get_tablet_unlocked(tablet_id, schema_hash);
    if (tablet_to_drop == nullptr) {
        LOG(WARNING) << "Fail to drop tablet " << tablet_id << ": not exist";
        return Status::NotFound(strings::Substitute("tablet $0 not fount", tablet_id));
    }
    LOG(INFO) << "Dropping tablet " << tablet_id;

    // Try to get schema change info, we can drop tablet directly if it is not
    // in schema-change state.
    AlterTabletTaskSharedPtr alter_task = tablet_to_drop->alter_task();
    if (alter_task == nullptr) {
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_state);
    }

    AlterTabletState alter_state = alter_task->alter_state();
    TTabletId related_tablet_id = alter_task->related_tablet_id();
    TSchemaHash related_schema_hash = alter_task->related_schema_hash();

    TabletSharedPtr related_tablet = _get_tablet_unlocked(related_tablet_id, related_schema_hash);
    if (related_tablet == nullptr) {
        // TODO(lingbin): in what case, can this happen?
        LOG(WARNING) << "Dropping tablet directly when related tablet not found. "
                     << " tablet_id=" << related_tablet_id;
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_state);
    }

    // Check whether the tablet we want to delete is in schema-change state
    bool is_schema_change_finished = (alter_state == ALTER_FINISHED || alter_state == ALTER_FAILED);

    // Check whether the tablet we want to delete is base-tablet
    bool is_dropping_base_tablet = false;
    if (tablet_to_drop->creation_time() < related_tablet->creation_time()) {
        is_dropping_base_tablet = true;
    }

    if (is_dropping_base_tablet && !is_schema_change_finished) {
        LOG(WARNING) << "Fail to drop tablet. it is in schema-change state. tablet=" << tablet_to_drop->full_name();
        return Status::InternalError("doing schema-change");
    }

    // When the code gets here, there are two possibilities:
    // 1. The tablet currently being deleted is a base-tablet, and the corresponding
    //    schema-change process has finished;
    // 2. The tablet we are currently trying to drop is not base-tablet(i.e. a tablet
    //    generated from its base-tablet due to schema-change). For example, the current
    //    request is triggered by cancel alter). In this scenario, the corresponding
    //    schema-change task may still in process.

    // Drop specified tablet and clear schema-change info
    // NOTE: must first break the hard-link and then drop the tablet.
    // Otherwise, if first drop tablet, then break link. If BE restarts during execution,
    // after BE restarts, the tablet is no longer in metadata, but because the hard-link
    // is still there, the corresponding file may never be deleted from disk.
    related_tablet->obtain_header_wrlock();
    // should check the related tablet_id in alter task is current tablet to be dropped
    // For example: A related to B, BUT B related to C.
    // If drop A, should not clear B's alter task
    AlterTabletTaskSharedPtr related_alter_task = related_tablet->alter_task();
    if (related_alter_task != nullptr && related_alter_task->related_tablet_id() == tablet_id &&
        related_alter_task->related_schema_hash() == schema_hash) {
        related_tablet->delete_alter_task();
        related_tablet->save_meta();
    }
    related_tablet->release_header_lock();
    Status res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_state);
    if (!res.ok()) {
        LOG(WARNING) << "Fail to drop tablet which in schema change. tablet=" << tablet_to_drop->full_name();
        return res;
    }

    LOG(INFO) << "Dropped tablet " << tablet_id;
    return res;
}

Status TabletManager::drop_tablets_on_error_root_path(const std::vector<TabletInfo>& tablet_info_vec) {
    if (tablet_info_vec.empty()) {
        return Status::OK();
    }
    for (int32 i = 0; i < _tablets_shards_size; i++) {
        std::unique_lock wlock(*_tablets_shards[i].lock);
        for (const TabletInfo& tablet_info : tablet_info_vec) {
            TTabletId tablet_id = tablet_info.tablet_id;
            if ((tablet_id & _tablets_shards_mask) != i) {
                continue;
            }
            TSchemaHash schema_hash = tablet_info.schema_hash;
            TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id, schema_hash);
            if (dropped_tablet == nullptr) {
                LOG(WARNING) << "dropping tablet not exist. tablet_id=" << tablet_id;
                continue;
            } else {
                tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
                for (list<TabletSharedPtr>::iterator it = tablet_map[tablet_id].table_arr.begin();
                     it != tablet_map[tablet_id].table_arr.end();) {
                    if ((*it)->equal(tablet_id, schema_hash)) {
                        // We should first remove tablet from partition_map to avoid iterator
                        // becoming invalid.
                        _remove_tablet_from_partition(*(*it));
                        it = tablet_map[tablet_id].table_arr.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
        }
    }
    return Status::OK();
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool include_deleted,
                                          std::string* err) {
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));
    return _get_tablet_unlocked(tablet_id, schema_hash, include_deleted, err);
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool include_deleted,
                                                    std::string* err) {
    TabletSharedPtr tablet;
    tablet = _get_tablet_unlocked(tablet_id, schema_hash);
    if (tablet == nullptr && include_deleted) {
        std::shared_lock rlock(_shutdown_tablets_lock);
        for (auto& deleted_tablet : _shutdown_tablets) {
            CHECK(deleted_tablet != nullptr) << "deleted tablet is nullptr";
            if (deleted_tablet->tablet_id() == tablet_id && deleted_tablet->schema_hash() == schema_hash) {
                tablet = deleted_tablet;
                break;
            }
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

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid,
                                          bool include_deleted, std::string* err) {
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
}

bool TabletManager::get_tablet_id_and_schema_hash_from_path(const std::string& path, TTabletId* tablet_id,
                                                            TSchemaHash* schema_hash) {
    static re2::RE2 normal_re(R"(/data/\d+/(\d+)/(\d+)($|/))");
    // match tablet schema hash data path, for example, the path is /data/1/16791/29998
    // 1 is shard id , 16791 is tablet id, 29998 is schema hash
    if (RE2::PartialMatch(path, normal_re, tablet_id, schema_hash)) {
        return true;
    }

    // If we can't match normal path pattern, this may be a path which is a empty tablet
    // directory. Use this pattern to match empty tablet directory. In this case schema_hash
    // will be set to zero.
    static re2::RE2 empty_tablet_re("/data/\\d+/(\\d+)($|/$)");
    if (!RE2::PartialMatch(path, empty_tablet_re, tablet_id)) {
        return false;
    }
    *schema_hash = 0;
    return true;
}

bool TabletManager::get_rowset_id_from_path(const std::string& path, RowsetId* rowset_id) {
    static re2::RE2 re(R"(/data/\d+/\d+/\d+/([A-Fa-f0-9]+)_.*)");
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

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(CompactionType compaction_type, DataDir* data_dir) {
    int64_t now_ms = UnixMillis();
    const std::string& compaction_type_str = compaction_type == CompactionType::BASE_COMPACTION ? "base" : "cumulative";
    // only do compaction if compaction #rowset > 1
    uint32_t highest_score = 1;
    TabletSharedPtr best_tablet;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(*tablets_shard.lock);
        for (const auto& tablet_map : tablets_shard.tablet_map) {
            for (const TabletSharedPtr& tablet_ptr : tablet_map.second.table_arr) {
                if (tablet_ptr->keys_type() == PRIMARY_KEYS) {
                    continue;
                }
                AlterTabletTaskSharedPtr cur_alter_task = tablet_ptr->alter_task();
                if (cur_alter_task != nullptr && cur_alter_task->alter_state() != ALTER_FINISHED &&
                    cur_alter_task->alter_state() != ALTER_FAILED) {
                    TabletSharedPtr related_tablet = _get_tablet_unlocked(cur_alter_task->related_tablet_id(),
                                                                          cur_alter_task->related_schema_hash());
                    if (related_tablet != nullptr && tablet_ptr->creation_time() > related_tablet->creation_time()) {
                        // Current tablet is newly created during schema-change or rollup, skip it
                        continue;
                    }
                }
                // A not-ready tablet maybe a newly created tablet under schema-change, skip it
                if (tablet_ptr->tablet_state() == TABLET_NOTREADY) {
                    continue;
                }

                if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() || !tablet_ptr->is_used() ||
                    !tablet_ptr->init_succeeded() || !tablet_ptr->can_do_compaction()) {
                    continue;
                }

                int64_t last_failure_ms = tablet_ptr->last_cumu_compaction_failure_time();
                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    last_failure_ms = tablet_ptr->last_base_compaction_failure_time();
                }
                if (now_ms - last_failure_ms <= config::min_compaction_failure_interval_sec * 1000) {
                    VLOG(1) << "Too often to check compaction, skip it."
                            << "compaction_type=" << compaction_type_str << ", last_failure_time_ms=" << last_failure_ms
                            << ", tablet_id=" << tablet_ptr->tablet_id();
                    continue;
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
    }

    if (best_tablet != nullptr) {
        LOG(INFO) << "Found the best tablet to compact. "
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

TabletSharedPtr TabletManager::find_best_tablet_to_do_update_compaction(DataDir* data_dir) {
    int64_t highest_score = 0;
    TabletSharedPtr best_tablet;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(*tablets_shard.lock);
        for (const auto& tablet_map : tablets_shard.tablet_map) {
            for (const TabletSharedPtr& tablet_ptr : tablet_map.second.table_arr) {
                if (tablet_ptr->keys_type() != PRIMARY_KEYS) {
                    continue;
                }
                AlterTabletTaskSharedPtr cur_alter_task = tablet_ptr->alter_task();
                if (cur_alter_task != nullptr && cur_alter_task->alter_state() != ALTER_FINISHED &&
                    cur_alter_task->alter_state() != ALTER_FAILED) {
                    TabletSharedPtr related_tablet = _get_tablet_unlocked(cur_alter_task->related_tablet_id(),
                                                                          cur_alter_task->related_schema_hash());
                    if (related_tablet != nullptr && tablet_ptr->creation_time() > related_tablet->creation_time()) {
                        // Current tablet is newly created during schema-change or rollup, skip it
                        continue;
                    }
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
                                            const std::string& meta_binary, bool update_meta, bool force, bool restore,
                                            bool check_path) {
    std::unique_lock wlock(_get_tablets_shard_lock(tablet_id));
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_mem_tracker));
    if (Status st = tablet_meta->deserialize(meta_binary); !st.ok()) {
        LOG(WARNING) << "Fail to load tablet because can not parse meta_binary string. "
                     << "tablet_id=" << tablet_id << " path=" << data_dir->path();
        return Status::InternalError("Invalid serialized tablet meta");
    }

    for (const auto& rs : tablet_meta->all_rs_metas()) {
        LOG_IF(FATAL, rs->rowset_type() != BETA_ROWSET)
                << "Unsupported rowset type " << rs->rowset_type() << " tablet_id=" << tablet_meta->tablet_id()
                << " tablet_uid=" << rs->tablet_uid() << " schema_hash=" << tablet_meta->schema_hash()
                << " rowset_id=" << rs->rowset_id();
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

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(_mem_tracker, tablet_meta, data_dir);
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
    if (check_path && !Env::Default()->path_exists(tablet->tablet_path()).ok()) {
        LOG(WARNING) << "Fail to create table, tablet path not exists, path=" << tablet->tablet_path();
        return Status::NotFound("tablet path not exists");
    }
    if (Status st = tablet->init(); !st.ok()) {
        LOG(WARNING) << "Fail to init tablet " << tablet->full_name() << ": " << st;
        return Status::InternalError("tablet init failed: " + st.to_string());
    }
    if (tablet->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "Loaded shutdown tablet " << tablet_id;
        std::unique_lock shutdown_tablets_wlock(_shutdown_tablets_lock);
        _shutdown_tablets.push_back(tablet);
        return Status::NotFound("tablet state is shutdown");
    }
    // NOTE: We do not check tablet's initial version here, because if BE restarts when
    // one tablet is doing schema-change, we may meet empty tablet.
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "Fail to load tablet. it is in running state but without delta. "
                     << "tablet=" << tablet->full_name() << " path=" << data_dir->path();
        // tablet state is invalid, drop tablet
        return Status::InternalError("tablet in running state but without delta");
    }
    auto st = _add_tablet_unlocked(tablet_id, schema_hash, tablet, update_meta, force);
    LOG_IF(WARNING, !st.ok()) << "Fail to add tablet " << tablet->full_name();
    return st;
}

Status TabletManager::load_tablet_from_dir(DataDir* store, TTabletId tablet_id, SchemaHash schema_hash,
                                           const string& schema_hash_path, bool force, bool restore) {
    LOG(INFO) << "Loading tablet " << tablet_id << " from " << schema_hash_path << ". force=" << force
              << " restore=" << restore;
    // not add lock here, because load_tablet_from_meta already add lock
    std::string header_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);
    // should change shard id before load tablet
    std::string shard_path = path_util::dir_name(path_util::dir_name(path_util::dir_name(header_path)));
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    if (Status st = TabletMeta::reset_tablet_uid(header_path); !st.ok()) {
        LOG(WARNING) << "Fail to set tablet uid when copied meta file. header_path=" << header_path;
        return Status::InternalError("reset tablet uid failed");
    }

    if (!Env::Default()->path_exists(header_path).ok()) {
        LOG(WARNING) << "Fail to find header file. header_path=" << header_path;
        return Status::NotFound("header file not exist");
    }

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_mem_tracker));
    if (Status st = tablet_meta->create_from_file(header_path); !st.ok()) {
        LOG(WARNING) << "Fail to load tablet_meta. file_path=" << header_path;
        return Status::InternalError("fail to create tablet meta from file");
    }
    // has to change shard id here, because meta file maybe copyed from other source
    // its shard is different from local shard
    tablet_meta->set_shard_id(shard);
    std::string meta_binary;
    tablet_meta->serialize(&meta_binary);
    auto st = load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force, restore, true);
    LOG_IF(WARNING, !st.ok()) << "fail to load tablet. header_path=" << header_path;
    return st;
}

void TabletManager::release_schema_change_lock(TTabletId tablet_id) {
    VLOG(3) << "release_schema_change_lock begin. tablet_id=" << tablet_id;
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));

    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet=" << tablet_id;
    } else {
        it->second.schema_change_lock.unlock();
    }
    VLOG(3) << "release_schema_change_lock end. tablet_id=" << tablet_id;
}

Status TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    StarRocksMetrics::instance()->report_tablet_requests_total.increment(1);

    TabletSharedPtr tablet = get_tablet(tablet_info->tablet_id, tablet_info->schema_hash);
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
    LOG(INFO) << "Reporting all tablets info";

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::vector<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);
    LOG(INFO) << "Found " << expire_txn_map.size() << " expired tablet transactions";

    StarRocksMetrics::instance()->report_all_tablets_requests_total.increment(1);

    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(*tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            if (item.second.table_arr.empty()) {
                continue;
            }

            uint64_t tablet_id = item.first;
            TTablet t_tablet;
            for (const TabletSharedPtr& tablet_ptr : item.second.table_arr) {
                TTabletInfo tablet_info;
                tablet_ptr->build_tablet_report_info(&tablet_info);

                // find expired transaction corresponding to this tablet
                TabletInfo tinfo(tablet_id, tablet_ptr->schema_hash(), tablet_ptr->tablet_uid());
                auto find = expire_txn_map.find(tinfo);
                if (find != expire_txn_map.end()) {
                    tablet_info.__set_transaction_ids(find->second);
                    expire_txn_map.erase(find);
                }
                t_tablet.tablet_infos.push_back(tablet_info);
            }

            if (!t_tablet.tablet_infos.empty()) {
                tablets_info->emplace(tablet_id, t_tablet);
            }
        }
    }
    LOG(INFO) << "Reported all " << tablets_info->size() << " tablets info";
    return Status::OK();
}

Status TabletManager::start_trash_sweep() {
    {
        std::vector<int64_t> tablets_to_clean;
        // we use this vector to save all tablet ptr for saving lock time.
        std::vector<TabletSharedPtr> all_tablets;
        for (auto& tablets_shard : _tablets_shards) {
            tablet_map_t& tablet_map = tablets_shard.tablet_map;
            {
                std::shared_lock rlock(*tablets_shard.lock);
                for (auto& item : tablet_map) {
                    // try to clean empty item
                    if (item.second.table_arr.empty()) {
                        tablets_to_clean.push_back(item.first);
                    }
                    for (const TabletSharedPtr& tablet : item.second.table_arr) {
                        all_tablets.push_back(tablet);
                    }
                }
            }

            for (const auto& tablet : all_tablets) {
                tablet->delete_expired_inc_rowsets();
                tablet->delete_expired_stale_rowset();
            }
            all_tablets.clear();

            if (!tablets_to_clean.empty()) {
                std::unique_lock wlock(*tablets_shard.lock);
                // clean empty tablet id item
                for (const auto& tablet_id_to_clean : tablets_to_clean) {
                    auto& item = tablet_map[tablet_id_to_clean];
                    if (item.table_arr.empty()) {
                        // try to get schema change lock if could get schema change lock, then nobody
                        // own the lock could remove the item
                        // it will core if schema change thread may hold the lock and this thread will deconstruct lock
                        if (item.schema_change_lock.try_lock()) {
                            item.schema_change_lock.unlock();
                            tablet_map.erase(tablet_id_to_clean);
                        }
                    }
                }
                tablets_to_clean.clear(); // We should clear the vector before next loop
            }
        }
    }

    int32_t clean_num = 0;
    do {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        clean_num = 0;
        // should get write lock here, because it will remove tablet from shut_down_tablets
        // and get tablet will access shut_down_tablets
        std::unique_lock wlock(_shutdown_tablets_lock);
        auto it = _shutdown_tablets.begin();
        while (it != _shutdown_tablets.end()) {
            // check if the meta has the tablet info and its state is shutdown
            if (it->use_count() > 1) {
                // it means current tablet is referenced by other thread
                ++it;
                continue;
            }
            TabletMetaSharedPtr tablet_meta(new TabletMeta(_mem_tracker));
            Status st = TabletMetaManager::get_tablet_meta((*it)->data_dir(), (*it)->tablet_id(), (*it)->schema_hash(),
                                                           tablet_meta);
            if (st.ok()) {
                if (tablet_meta->tablet_uid() != (*it)->tablet_uid()) {
                    LOG(INFO) << "Skipped remove tablet " << tablet_meta->tablet_id() << ": uid mismatch";
                    it = _shutdown_tablets.erase(it);
                    continue;
                }
                if (tablet_meta->tablet_state() != TABLET_SHUTDOWN) {
                    LOG(WARNING) << "Cannot remove normal state tablet " << tablet_meta->tablet_id()
                                 << ". tablet_uid=" << (*it)->tablet_uid();
                    it = _shutdown_tablets.erase(it);
                    continue;
                }
                // move data to trash
                TabletSharedPtr& tablet = *it;
                std::string tablet_path = tablet->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    // take snapshot of tablet meta
                    std::string meta_file_path = path_util::join_path_segments(
                            tablet->tablet_path(), std::to_string(tablet->tablet_id()) + ".hdr");
                    tablet->tablet_meta()->save(meta_file_path);
                    OLAPStatus rm_st = move_to_trash(tablet_path, tablet_path);
                    if (rm_st == OLAP_SUCCESS) {
                        LOG(INFO) << "Moved " << tablet_path << " to trash";
                    } else {
                        LOG(WARNING) << "Fail to move " << tablet_path << " to trash";
                        ++it;
                        continue;
                    }
                }
                Status st;
                if (tablet->keys_type() == KeysType::PRIMARY_KEYS) {
                    st = tablet->updates()->clear_meta();
                } else {
                    st = TabletMetaManager::remove(tablet->data_dir(), tablet->tablet_id(), tablet->schema_hash());
                }
                if (!st.ok()) {
                    LOG(WARNING) << "Fail to clear tablet meta: " << st;
                    ++it;
                } else {
                    it = _shutdown_tablets.erase(it);
                    ++clean_num;
                }
            } else if (st.is_not_found()) {
                // if could not find tablet info in meta store, then check if dir existed
                st = Env::Default()->path_exists((*it)->tablet_path());
                if (st.ok()) {
                    LOG(WARNING) << "Cannot remove tablet " << (*it)->tablet_id() << ": tablet meta not found";
                    ++it;
                } else if (st.is_not_found()) {
                    LOG(INFO) << "Removed tablet " << (*it)->tablet_id();
                    it = _shutdown_tablets.erase(it);
                } else {
                    LOG(WARNING) << "Fail to check path " << (*it)->tablet_path() << ": " << st;
                    ++it;
                }
            } else {
                LOG(WARNING) << "Fail to get tablet meta: " << st;
                return st;
            }

            // yield to avoid hoding _tablet_map_lock for too long
            if (clean_num >= 200) {
                break;
            }
        }
    } while (clean_num >= 200);
    return Status::OK();
} // start_trash_sweep

void TabletManager::register_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::unique_lock wlock(*shard.lock);
    shard.tablets_under_clone.insert(tablet_id);
}

void TabletManager::unregister_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::unique_lock wlock(*shard.lock);
    shard.tablets_under_clone.erase(tablet_id);
}

void TabletManager::try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id, SchemaHash schema_hash,
                                                  const std::string& schema_hash_path) {
    // acquire the read lock, so that there is no creating tablet or load tablet from meta tasks
    // create tablet and load tablet task should check whether the dir exists
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::shared_lock rlock(*shard.lock);

    // check if meta already exists
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_mem_tracker));
    Status st = TabletMetaManager::get_tablet_meta(data_dir, tablet_id, schema_hash, tablet_meta);
    if (st.ok()) {
        LOG(INFO) << "Cannot remove tablet_path=" << schema_hash_path << ", tablet meta exist in meta store";
    } else if (st.is_not_found()) {
        if (shard.tablets_under_clone.count(tablet_id) > 0) {
            LOG(INFO) << "Cannot move tablet_path=" << schema_hash_path << " to trash, tablet is under clone";
            return;
        }

        // TODO(ygl): may do other checks in the future
        if (Env::Default()->path_exists(schema_hash_path).ok()) {
            OLAPStatus rm_st = move_to_trash(schema_hash_path, schema_hash_path);
            if (rm_st != OLAP_SUCCESS) {
                LOG(WARNING) << "Fail to move tablet_path=" << schema_hash_path << " to trash";
            } else {
                LOG(INFO) << "Moved tablet_path=" << schema_hash_path << " to trash";
            }
        }
    } else {
        LOG(WARNING) << "Fail to get tablet meta: " << st;
    }
}

bool TabletManager::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    VLOG(3) << "Locking tablet_id=" << tablet_id;
    std::shared_lock rlock(_get_tablets_shard_lock(tablet_id));
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "Fail to lock nonexistent tablet_id=" << tablet_id;
    } else {
        res = (it->second.schema_change_lock.try_lock());
    }
    VLOG(3) << "Locked tablet_id=" << tablet_id;
    return res;
}

void TabletManager::update_root_path_info(std::map<std::string, DataDirInfo>* path_map, size_t* tablet_count) {
    DCHECK(tablet_count != nullptr);
    *tablet_count = 0;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(*tablets_shard.lock);
        for (const auto& entry : tablets_shard.tablet_map) {
            const TableInstances& instance = entry.second;
            for (const auto& tablet : instance.table_arr) {
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
}

void TabletManager::get_partition_related_tablets(int64_t partition_id, std::set<TabletInfo>* tablet_infos) {
    std::shared_lock rlock(_partition_tablet_map_lock);
    if (_partition_tablet_map.find(partition_id) != _partition_tablet_map.end()) {
        *tablet_infos = _partition_tablet_map[partition_id];
    }
}

void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    std::vector<TabletSharedPtr> related_tablets;
    {
        for (const auto& tablets_shard : _tablets_shards) {
            std::shared_lock rlock(*tablets_shard.lock);
            for (const auto& tablet_map : tablets_shard.tablet_map) {
                for (const TabletSharedPtr& tablet_ptr : tablet_map.second.table_arr) {
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
        }
    }
    for (const TabletSharedPtr& tablet : related_tablets) {
        tablet->do_tablet_meta_checkpoint();
    }
}

void TabletManager::_build_tablet_stat() {
    _tablet_stat_cache.clear();
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rlock(*tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            if (item.second.table_arr.empty()) {
                continue;
            }

            TTabletStat stat;
            stat.tablet_id = item.first;
            for (const TabletSharedPtr& tablet : item.second.table_arr) {
                // TODO(lingbin): if it is nullptr, why is it not deleted?
                if (tablet == nullptr) {
                    continue;
                }
                // TODO(cbl): get row num and data size together is faster
                stat.__set_data_size(tablet->tablet_footprint());
                stat.__set_row_num(tablet->num_rows());
                VLOG(3) << "building tablet stat. tablet_id=" << item.first
                        << ", data_size=" << tablet->tablet_footprint() << ", row_num=" << tablet->num_rows();
                break;
            }

            _tablet_stat_cache.emplace(item.first, stat);
        }
    }
}

Status TabletManager::_create_inital_rowset_unlocked(const TCreateTabletReq& request, Tablet* tablet) {
    Status st;
    if (request.version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1. req.ver=" << request.version;
        return Status::InvalidArgument("invalid version");
    } else {
        Version version(0, request.version);
        VLOG(3) << "begin to create init version. version=" << version;
        RowsetSharedPtr new_rowset;
        do {
            RowsetWriterContext context(kDataFormatUnknown, config::storage_format_version);
            context.mem_tracker = _mem_tracker;
            context.rowset_id = StorageEngine::instance()->next_rowset_id();
            context.tablet_uid = tablet->tablet_uid();
            context.tablet_id = tablet->tablet_id();
            context.partition_id = tablet->partition_id();
            context.tablet_schema_hash = tablet->schema_hash();
            context.rowset_type = RowsetTypePB::BETA_ROWSET;

            context.rowset_path_prefix = tablet->tablet_path();
            context.tablet_schema = &(tablet->tablet_schema());
            context.rowset_state = VISIBLE;
            context.version = version;
            context.version_hash = request.version_hash;
            // there is no data in init rowset, so overlapping info is unknown.
            context.segments_overlap = OVERLAP_UNKNOWN;

            std::unique_ptr<RowsetWriter> builder;
            st = RowsetFactory::create_rowset_writer(context, &builder);
            if (!st.ok()) {
                LOG(WARNING) << "failed to init rowset writer for tablet " << tablet->full_name() << ": " << st;
                break;
            }
            if (builder->flush() != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to flush rowset writer for tablet " << tablet->full_name();
                st = Status::InternalError("flush rowset failed");
                break;
            }

            new_rowset = builder->build();
            st = tablet->add_rowset(new_rowset, false);
            if (!st.ok()) {
                LOG(WARNING) << "failed to add rowset for tablet " << tablet->full_name();
                break;
            }
        } while (false);

        // Unregister index and delete files(index and data) if failed
        if (!st.ok()) {
            LOG(WARNING) << "fail to create initial rowset: " << st << " version=" << version;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            return Status::InternalError("fail to create initial rowset");
        }
    }
    tablet->set_cumulative_layer_point(request.version + 1);
    // NOTE: should not save tablet meta here, because it will be saved if add to map successfully

    return st;
}

Status TabletManager::_create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store,
                                                   const bool is_schema_change, const Tablet* base_tablet,
                                                   TabletMetaSharedPtr* tablet_meta) {
    uint32_t next_unique_id = 0;
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    if (!is_schema_change) {
        for (uint32_t col_idx = 0; col_idx < request.tablet_schema.columns.size(); ++col_idx) {
            col_idx_to_unique_id[col_idx] = col_idx;
        }
        next_unique_id = request.tablet_schema.columns.size();
    } else {
        next_unique_id = base_tablet->next_unique_id();
        size_t old_num_columns = base_tablet->num_columns();
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
                const std::string& old_name = base_tablet->tablet_schema().column(old_col_idx).name();
                if (old_name == column.column_name) {
                    uint32_t old_unique_id = base_tablet->tablet_schema().column(old_col_idx).unique_id();
                    col_idx_to_unique_id[new_col_idx] = old_unique_id;
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

    // We generate a new tablet_uid for this new tablet.
    uint64_t shard_id = 0;
    if (store->get_shard(&shard_id) != OLAP_SUCCESS) {
        LOG(WARNING) << "Fail to get root path shard";
        return Status::InternalError("fail to get root path shard");
    }

    RowsetTypePB rowset_type = RowsetTypePB::BETA_ROWSET;
    return TabletMeta::create(_mem_tracker, request, TabletUid::gen_uid(), shard_id, next_unique_id,
                              col_idx_to_unique_id, rowset_type, tablet_meta);
}

Status TabletManager::_drop_tablet_directly_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool keep_state) {
    TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id, schema_hash);
    if (dropped_tablet == nullptr) {
        LOG(WARNING) << "Fail to drop nonexistent tablet " << tablet_id;
        return Status::NotFound("");
    }
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    list<TabletSharedPtr>& candidate_tablets = tablet_map[tablet_id].table_arr;
    auto it = candidate_tablets.begin();
    while (it != candidate_tablets.end()) {
        if (!(*it)->equal(tablet_id, schema_hash)) {
            ++it;
            continue;
        }

        TabletSharedPtr tablet = *it;
        _remove_tablet_from_partition(*(*it));
        it = candidate_tablets.erase(it);
        if (!keep_state) {
            // drop tablet will update tablet meta, should lock
            std::unique_lock wrlock(tablet->get_header_lock());
            LOG(INFO) << "Shutting down tablet " << tablet_id;
            // NOTE: has to update tablet here, but must not update tablet meta directly.
            // because other thread may hold the tablet object, they may save meta too.
            // If update meta directly here, other thread may override the meta
            // and the tablet will be loaded at restart time.
            // To avoid this exception, we first set the state of the tablet to `SHUTDOWN`.
            tablet->set_tablet_state(TABLET_SHUTDOWN);
            tablet->save_meta();
            {
                std::unique_lock wlock(_shutdown_tablets_lock);
                _shutdown_tablets.push_back(tablet);
            }
        }
    }

    dropped_tablet->deregister_tablet_from_dir();
    return Status::OK();
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash) {
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    auto it = tablet_map.find(tablet_id);
    if (it != tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {
            CHECK(tablet != nullptr) << "tablet is nullptr. tablet_id=" << tablet_id;
            if (tablet->equal(tablet_id, schema_hash)) {
                return tablet;
            }
        }
    }
    return nullptr;
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

std::shared_mutex& TabletManager::_get_tablets_shard_lock(TTabletId tabletId) {
    return *_get_tablets_shard(tabletId).lock;
}

TabletManager::tablet_map_t& TabletManager::_get_tablet_map(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).tablet_map;
}

TabletManager::tablets_shard& TabletManager::_get_tablets_shard(TTabletId tabletId) {
    return _tablets_shards[tabletId & _tablets_shards_mask];
}

Status TabletManager::create_tablet_from_snapshot(DataDir* store, TTabletId tablet_id, SchemaHash schema_hash,
                                                  const string& schema_hash_path) {
    LOG(INFO) << "Loading tablet " << tablet_id << " from snapshot " << schema_hash_path;
    auto meta_path = strings::Substitute("$0/meta", schema_hash_path);
    auto shard_path = path_util::dir_name(path_util::dir_name(path_util::dir_name(meta_path)));
    auto shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    auto shard = stol(shard_str);

    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_path);
    if (!snapshot_meta.ok()) {
        LOG(WARNING) << "Fail to parse " << meta_path << ": " << snapshot_meta.status();
        return snapshot_meta.status();
    }
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

    // Set of rowset id collected from rowset meta.
    std::set<uint32_t> set1;
    for (const auto& rm : snapshot_meta->rowset_metas()) {
        set1.insert(rm.rowset_seg_id());
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
    for (const RowsetMetaPB& rm : snapshot_meta->rowset_metas()) {
        RETURN_IF_ERROR(TabletMetaManager::put_rowset_meta(store, &wb, tablet_id, rm));
    }
    for (const auto& [segid, dv] : snapshot_meta->delete_vectors()) {
        RETURN_IF_ERROR(TabletMetaManager::put_del_vector(store, &wb, tablet_id, segid, dv));
    }
    RETURN_IF_ERROR(TabletMetaManager::put_tablet_meta(store, &wb, snapshot_meta->tablet_meta()));

    auto tablet_meta = std::make_shared<TabletMeta>(_mem_tracker);
    tablet_meta->init_from_pb(&snapshot_meta->tablet_meta());
    // DO NOT access tablet->updates() until tablet has been init()-ed.
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(_mem_tracker, tablet_meta, store);
    if (tablet == nullptr) {
        LOG(WARNING) << "Fail to load tablet " << tablet_id;
        return Status::InternalError("Fail to create tablet");
    }
    // NOTE: do NOT touch snapshot_meta->tablet_meta since here, it has been modified by
    // `Tablet::create_tablet_from_meta`.

    if (!Env::Default()->path_exists(tablet->tablet_path()).ok()) {
        return Status::NotFound("tablet path not exists");
    }

    if (tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "Fail to load snapshot from " << schema_hash_path << ": tablet has been shutdown";
        return Status::InternalError("tablet state is shutdown");
    }

    std::unique_lock l(_get_tablets_shard_lock(tablet_id));
    if (_get_tablet_unlocked(tablet_id, schema_hash, true, nullptr) != nullptr) {
        return Status::InternalError("tablet already exist");
    }

    RETURN_IF_ERROR(meta_store->write_batch(&wb));

    if (!tablet->init().ok()) {
        LOG(WARNING) << "Fail to init cloned tablet " << tablet_id << ", try to clear meta store";
        wb.Clear();
        RETURN_IF_ERROR(TabletMetaManager::clear_del_vector(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::clear_rowset(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::clear_log(store, &wb, tablet_id));
        RETURN_IF_ERROR(TabletMetaManager::remove_tablet_meta(store, &wb, tablet_id, schema_hash));
        auto st = meta_store->write_batch(&wb);
        LOG_IF(WARNING, !st.ok()) << "Fail to clear meta store: " << st;
        return Status::InternalError("tablet init failed");
    }
    auto st = _add_tablet_unlocked(tablet_id, schema_hash, tablet, true, false);
    LOG_IF(WARNING, !st.ok()) << "Fail to add cloned tablet " << tablet_id << ": " << st;
    return st;
}

} // end namespace starrocks
