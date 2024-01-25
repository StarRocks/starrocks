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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_manager.h

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

#pragma once

#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gutil/macros.h"
#include "storage/kv_store.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/tablet.h"
#include "util/spinlock.h"

namespace starrocks {

class Tablet;
class DataDir;
struct TabletBasicInfo;

// RowsetsAcqRel is a RAII wrapper for invocation of Rowset::acquire_readers and Rowset::release_readers
class RowsetsAcqRel;
using RowsetsAcqRelPtr = std::shared_ptr<RowsetsAcqRel>;
class RowsetsAcqRel {
private:
public:
    explicit RowsetsAcqRel(const std::vector<RowsetSharedPtr>& rowsets) : _rowsets(rowsets) {
        Rowset::acquire_readers(_rowsets);
    }
    ~RowsetsAcqRel() { Rowset::release_readers(_rowsets); }

private:
    DISALLOW_COPY_AND_MOVE(RowsetsAcqRel);
    std::vector<RowsetSharedPtr> _rowsets;
};

using TabletAndRowsets = std::tuple<TabletSharedPtr, std::vector<RowsetSharedPtr>, RowsetsAcqRelPtr>;
using TabletAndScore = std::pair<TabletSharedPtr, double>;

enum TabletDropFlag {
    kMoveFilesToTrash = 0,
    kDeleteFiles = 1,
    kKeepMetaAndFiles = 2,
};

// TabletManager provides get, add, delete tablet method for storage engine
// NOTE: If you want to add a method that needs to hold meta-lock before you can call it,
// please uniformly name the method in "xxx_unlocked()" mode
class TabletManager {
public:
    explicit TabletManager(int64_t tablet_map_lock_shard_size);
    ~TabletManager() = default;

    // The param stores holds all candidate data_dirs for this tablet.
    // NOTE: If the request is from a schema-changing tablet, The directory selected by the
    // new tablet should be the same as the directory of origin tablet. Because the
    // linked-schema-change type requires Linux hard-link, which does not support cross disk.
    // TODO(lingbin): Other schema-change type do not need to be on the same disk. Because
    // there may be insufficient space on the current disk, which will lead the schema-change
    // task to be fail, even if there is enough space on other disks
    Status create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores);

    Status drop_tablet(TTabletId tablet_id, TabletDropFlag flag = kMoveFilesToTrash);

    Status drop_tablets_on_error_root_path(const std::vector<TabletInfo>& tablet_info_vec);

    TabletSharedPtr find_best_tablet_to_compaction(CompactionType compaction_type, DataDir* data_dir,
                                                   std::pair<int32_t, int32_t> tablet_shards_range);

    TabletSharedPtr find_best_tablet_to_do_update_compaction(DataDir* data_dir);

    // TODO: pass |include_deleted| as an enum instead of boolean to avoid unexpected implicit cast.
    TabletSharedPtr get_tablet(TTabletId tablet_id, bool include_deleted = false, std::string* err = nullptr);

    StatusOr<TabletAndRowsets> capture_tablet_and_rowsets(TTabletId tablet_id, int64_t from_version,
                                                          int64_t to_version);

    TabletSharedPtr get_tablet(TTabletId tablet_id, const TabletUid& tablet_uid, bool include_deleted = false,
                               std::string* err = nullptr);

    // Extract tablet_id and schema_hash from given path.
    //
    // The normal path pattern is like "/data/{shard_id}/{tablet_id}/{schema_hash}/xxx.data".
    // Besides that, this also support empty tablet path, which path looks like
    // "/data/{shard_id}/{tablet_id}"
    //
    // Return true when the path matches the path pattern, and tablet_id and schema_hash is
    // saved in input params. When input path is an empty tablet directory, schema_hash will
    // be set to 0. Return false if the path don't match valid pattern.
    static bool get_tablet_id_and_schema_hash_from_path(const std::string& path, TTabletId* tablet_id,
                                                        TSchemaHash* schema_hash);

    static bool get_rowset_id_from_path(const std::string& path, RowsetId* rowset_id);

    void get_tablet_stat(TTabletStatResult* result);

    // parse tablet header msg to generate tablet object
    // - restore: whether the request is from restore tablet action,
    //   where we should change tablet status from shutdown back to running
    //
    // return NotFound if the tablet path has been deleted or the tablet statue is SHUTDOWN.
    Status load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id, TSchemaHash schema_hash, std::string_view meta,
                                 bool update_meta, bool force = false, bool restore = false, bool check_path = true);

    Status load_tablet_from_dir(DataDir* data_dir, TTabletId tablet_id, SchemaHash schema_hash,
                                const std::string& schema_hash_path, bool force = false, bool restore = false);

    Status create_tablet_from_meta_snapshot(DataDir* data_dir, TTabletId tablet_id, SchemaHash schema_hash,
                                            const std::string& schema_hash_path, bool restore = false);

    void release_schema_change_lock(TTabletId tablet_id);

    // Returns NotFound if the corresponding tablet does not exist.
    Status report_tablet_info(TTabletInfo* tablet_info);

    Status report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    Status start_trash_sweep();
    // Prevent schema change executed concurrently.
    bool try_schema_change_lock(TTabletId tablet_id);

    void try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id, SchemaHash schema_hash,
                                       const string& tablet_id_path);

    void update_root_path_info(std::map<std::string, DataDirInfo>* path_map, size_t* tablet_counter);

    void do_tablet_meta_checkpoint(DataDir* data_dir);

    void register_clone_tablet(int64_t tablet_id);
    void unregister_clone_tablet(int64_t tablet_id);
    bool check_clone_tablet(int64_t tablet_id);

    size_t shutdown_tablets() const {
        std::shared_lock l(_shutdown_tablets_lock);
        return _shutdown_tablets.size();
    }

    Status delete_shutdown_tablet(int64_t tablet_id);

    Status delete_shutdown_tablet_before_clone(int64_t tablet_id);

    // return true if all tablets visited
    bool get_next_batch_tablets(size_t batch_size, std::vector<TabletSharedPtr>* tablets);

    // return map<TabletId, vector<pair<rowsetid, segment file num>>>
    std::unordered_map<TTabletId, std::vector<std::pair<uint32_t, uint32_t>>> get_tablets_need_repair_compaction();

    void get_tablets_by_partition(int64_t partition_id, std::vector<TabletInfo>& tablet_infos);

    void get_tablets_basic_infos(int64_t table_id, int64_t partition_id, int64_t tablet_id,
                                 std::vector<TabletBasicInfo>& tablet_infos);

    std::vector<TabletAndScore> pick_tablets_to_do_pk_index_major_compaction();

    Status generate_pk_dump_in_error_state();

private:
    using TabletMap = std::unordered_map<int64_t, TabletSharedPtr>;
    using TabletSet = std::unordered_set<int64_t>;

    struct TabletsShard {
        mutable std::shared_mutex lock;
        TabletMap tablet_map;
        TabletSet tablets_under_clone;
    };

    struct DroppedTabletInfo {
        TabletSharedPtr tablet;
        TabletDropFlag flag;
    };

    class LockTable {
    public:
        bool is_locked(int64_t tablet_id);
        bool try_lock(int64_t tablet_id);
        bool unlock(int64_t tablet_id);

    private:
        constexpr static int kNumShard = 128;

        int64_t _shard(int64_t tablet_id) { return tablet_id % kNumShard; }

        SpinLock _latches[kNumShard];
        std::unordered_set<int64_t> _locks[kNumShard];
    };

    TabletManager(const TabletManager&) = delete;
    const TabletManager& operator=(const TabletManager&) = delete;

    // Add a tablet pointer to StorageEngine
    // If force, drop the existing tablet add this new one
    Status _add_tablet_unlocked(const TabletSharedPtr& tablet, bool update_meta, bool force);

    Status _update_tablet_map_and_partition_info(const TabletSharedPtr& tablet);

    static Status _create_inital_rowset_unlocked(const TCreateTabletReq& request, Tablet* tablet);

    Status _drop_tablet_unlocked(TTabletId tablet_id, TabletDropFlag flag);

    TabletSharedPtr _get_tablet_unlocked(TTabletId tablet_id);
    TabletSharedPtr _get_tablet_unlocked(TTabletId tablet_id, bool include_deleted, std::string* err);

    TabletSharedPtr _internal_create_tablet_unlocked(AlterTabletType alter_type, const TCreateTabletReq& request,
                                                     bool is_schema_change, const Tablet* base_tablet,
                                                     const std::vector<DataDir*>& data_dirs);
    TabletSharedPtr _create_tablet_meta_and_dir_unlocked(const TCreateTabletReq& request, bool is_schema_change,
                                                         const Tablet* base_tablet,
                                                         const std::vector<DataDir*>& data_dirs);
    Status _create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store, bool is_schema_change_tablet,
                                        const Tablet* base_tablet, TabletMetaSharedPtr* tablet_meta);

    void _build_tablet_stat();

    void _add_tablet_to_partition(const Tablet& tablet);

    void _remove_tablet_from_partition(const Tablet& tablet);

    std::shared_mutex& _get_tablets_shard_lock(TTabletId tabletId);

    TabletMap& _get_tablet_map(TTabletId tablet_id);

    TabletsShard& _get_tablets_shard(TTabletId tabletId);

    int64_t _get_tablets_shard_idx(TTabletId tabletId) const { return tabletId & _tablets_shards_mask; }

    static Status _remove_tablet_meta(const TabletSharedPtr& tablet);
    static Status _remove_tablet_directories(const TabletSharedPtr& tablet);
    static Status _move_tablet_directories_to_trash(const TabletSharedPtr& tablet);

    std::vector<TabletsShard> _tablets_shards;
    const int64_t _tablets_shards_mask;
    LockTable _schema_change_lock_tbl;

    // Protect _partition_tablet_map, should not be obtained before _tablet_map_lock to avoid deadlock
    mutable std::shared_mutex _partition_tablet_map_lock;
    // Protect _shutdown_tablets, should not be obtained before _tablet_map_lock to avoid deadlock
    mutable std::shared_mutex _shutdown_tablets_lock;
    // partition_id => tablet_info
    std::map<int64_t, std::set<TabletInfo>> _partition_tablet_map;
    std::map<int64_t, DroppedTabletInfo> _shutdown_tablets;

    std::mutex _tablet_stat_mutex;
    // cache to save tablets' statistics, such as data-size and row-count
    // TODO(cmy): for now, this is a naive implementation
    std::map<int64_t, TTabletStat> _tablet_stat_cache;
    // last update time of tablet stat cache
    int64_t _last_update_stat_ms;

    // context for compaction checker
    size_t _cur_shard = 0;
    std::unordered_set<int64_t> _shard_visited_tablet_ids;
};

inline bool TabletManager::LockTable::is_locked(int64_t tablet_id) {
    auto s = _shard(tablet_id);
    std::lock_guard l(_latches[s]);
    return _locks[s].count(tablet_id) > 0;
}

inline bool TabletManager::LockTable::try_lock(int64_t tablet_id) {
    auto s = _shard(tablet_id);
    std::lock_guard l(_latches[s]);
    return _locks[s].insert(tablet_id).second;
}

inline bool TabletManager::LockTable::unlock(int64_t tablet_id) {
    auto s = _shard(tablet_id);
    std::lock_guard l(_latches[s]);
    return _locks[s].erase(tablet_id) > 0;
}

} // namespace starrocks
