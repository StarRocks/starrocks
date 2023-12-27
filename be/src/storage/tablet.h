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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet.h

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

#include <boost/algorithm/string.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/base_tablet.h"
#include "storage/data_dir.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet_meta.h"
#include "storage/tuple.h"
#include "storage/utils.h"
#include "storage/version_graph.h"
#include "util/once.h"

namespace starrocks {

class DataDir;
class RowsetReadOptions;
class Tablet;
class TabletMeta;
class TabletUpdates;
class CompactionTask;
struct CompactionCandidate;
struct CompactionContext;
struct TabletBasicInfo;

using TabletSharedPtr = std::shared_ptr<Tablet>;

class ChunkIterator;

using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;

class Tablet : public BaseTablet {
public:
    static TabletSharedPtr create_tablet_from_meta(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir = nullptr);

    Tablet(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir);

    Tablet(const Tablet&) = delete;
    const Tablet& operator=(const Tablet&) = delete;

    // for ut
    Tablet();

    ~Tablet() override;

    [[nodiscard]] Status init();

    bool init_succeeded();

    bool is_used();

    void register_tablet_into_dir();
    void deregister_tablet_from_dir();

    void save_meta();
    // Used in clone task, to update local meta when finishing a clone job
    [[nodiscard]] Status revise_tablet_meta(const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                                            const std::vector<Version>& versions_to_delete);

    const int64_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int64_t new_point);

    size_t tablet_footprint(); // disk space occupied by tablet
    size_t num_rows();
    size_t version_count() const;
    Version max_version() const;

    // propreties encapsulated in TabletSchema
    KeysType keys_type() const;
    size_t num_columns_with_max_version() const;
    size_t num_key_columns_with_max_version() const;
    size_t num_rows_per_row_block_with_max_version() const;
    size_t next_unique_id() const;
    size_t field_index_with_max_version(const string& field_name) const;
    std::string schema_debug_string() const;
    std::string debug_string() const;
    bool enable_shortcut_compaction() const;

    // Load incremental rowsets to the tablet in DataDir#load.
    [[nodiscard]] Status load_rowset(const RowsetSharedPtr& rowset);
    // finish loading rowsets
    [[nodiscard]] Status finish_load_rowsets();

    // operation in rowsets
    [[nodiscard]] Status add_rowset(const RowsetSharedPtr& rowset, bool need_persist = true);
    void modify_rowsets(const vector<RowsetSharedPtr>& to_add, const vector<RowsetSharedPtr>& to_delete,
                        std::vector<RowsetSharedPtr>* to_replace);

    // _rs_version_map and _inc_rs_version_map should be protected by _meta_lock
    // The caller must call hold _meta_lock when call this two function.
    RowsetSharedPtr get_rowset_by_version(const Version& version) const;
    RowsetSharedPtr get_inc_rowset_by_version(const Version& version) const;

    RowsetSharedPtr rowset_with_max_version() const;

    [[nodiscard]] Status add_inc_rowset(const RowsetSharedPtr& rowset, int64_t version);
    void delete_expired_inc_rowsets();

    /// Delete stale rowset by timing. This delete policy uses now() munis
    /// config::tablet_rowset_expired_stale_sweep_time_sec to compute the deadline of expired rowset
    /// to delete.  When rowset is deleted, it will be added to StorageEngine unused map and record
    /// need to delete flag.
    void delete_expired_stale_rowset();

    [[nodiscard]] Status capture_consistent_versions(const Version& spec_version, vector<Version>* version_path) const;
    [[nodiscard]] Status check_version_integrity(const Version& version);
    void list_versions(std::vector<Version>* versions) const;

    // REQUIRE: `obtain_header_rdlock()`ed
    [[nodiscard]] Status capture_consistent_rowsets(const Version& spec_version,
                                                    vector<RowsetSharedPtr>* rowsets) const;

    const DelPredicateArray& delete_predicates() const { return _tablet_meta->delete_predicates(); }
    [[nodiscard]] bool version_for_delete_predicate(const Version& version);
    [[nodiscard]] bool has_delete_predicates(const Version& version);

    // meta lock
    void obtain_header_rdlock() { _meta_lock.lock_shared(); }
    void obtain_header_wrlock() { _meta_lock.lock(); }
    void release_header_lock() { _meta_lock.unlock(); }
    std::shared_mutex& get_header_lock() { return _meta_lock; }

    // ingest lock
    void obtain_push_lock() { _ingest_lock.lock(); }
    void release_push_lock() { _ingest_lock.unlock(); }
    std::mutex& get_push_lock() { return _ingest_lock; }

    // base lock
    void obtain_base_compaction_lock() { _base_lock.lock(); }
    void release_base_compaction_lock() { _base_lock.unlock(); }
    std::shared_mutex& get_base_lock() { return _base_lock; }

    // cumulative lock
    void obtain_cumulative_lock() { _cumulative_lock.lock(); }
    void release_cumulative_lock() { _cumulative_lock.unlock(); }
    std::shared_mutex& get_cumulative_lock() { return _cumulative_lock; }

    std::shared_mutex& get_migration_lock() { return _migration_lock; }
    // should use with migration lock.
    [[nodiscard]] bool is_migrating() const { return _is_migrating; }
    // should use with migration lock.
    void set_is_migrating(bool is_migrating) { _is_migrating = is_migrating; }

    std::shared_mutex& get_meta_store_lock() { return _meta_store_lock; }

    // check tablet is migrating or has been migrated.
    // if tablet is migrating or has been migrated, return true.
    // should use with migration lock.
    static bool check_migrate(const TabletSharedPtr& tablet);

    // operation for compaction
    const uint32_t calc_cumulative_compaction_score() const;
    const uint32_t calc_base_compaction_score() const;

    // operation for clone
    void calc_missed_versions(int64_t spec_version, vector<Version>* missed_versions);
    void calc_missed_versions_unlocked(int64_t spec_version, vector<Version>* missed_versions) const;

    // Same as max_continuous_version_from_beginning, only return end version, using a more efficient implementation
    int64_t max_continuous_version() const;
    int64_t max_readable_version() const;

    int64_t last_cumu_compaction_failure_time() { return _last_cumu_compaction_failure_millis; }
    void set_last_cumu_compaction_failure_time(int64_t millis) { _last_cumu_compaction_failure_millis = millis; }

    TStatusCode::type last_cumu_compaction_failure_status() { return _last_cumu_compaction_failure_status.load(); }
    void set_last_cumu_compaction_failure_status(TStatusCode::type st) {
        _last_cumu_compaction_failure_status.store(st);
    }

    int64_t last_base_compaction_failure_time() { return _last_base_compaction_failure_millis; }
    void set_last_base_compaction_failure_time(int64_t millis) { _last_base_compaction_failure_millis = millis; }

    int64_t last_cumu_compaction_success_time() { return _last_cumu_compaction_success_millis; }
    void set_last_cumu_compaction_success_time(int64_t millis) { _last_cumu_compaction_success_millis = millis; }

    int64_t last_base_compaction_success_time() { return _last_base_compaction_success_millis; }
    void set_last_base_compaction_success_time(int64_t millis) { _last_base_compaction_success_millis = millis; }

    void delete_all_files();

    bool check_rowset_id(const RowsetId& rowset_id);

    void set_partition_id(int64_t partition_id);

    TabletInfo get_tablet_info() const;

    void pick_candicate_rowsets_to_cumulative_compaction(std::vector<RowsetSharedPtr>* candidate_rowsets);
    void pick_candicate_rowsets_to_base_compaction(std::vector<RowsetSharedPtr>* candidate_rowsets);
    void pick_all_candicate_rowsets(std::vector<RowsetSharedPtr>* candidate_rowsets);

    void calculate_cumulative_point();

    void do_tablet_meta_checkpoint();

    bool rowset_meta_is_useful(const RowsetMetaSharedPtr& rowset_meta);

    void build_tablet_report_info(TTabletInfo* tablet_info);

    void generate_tablet_meta_copy(const TabletMetaSharedPtr& new_tablet_meta) const;
    // caller should hold the _meta_lock before calling this method
    void generate_tablet_meta_copy_unlocked(const TabletMetaSharedPtr& new_tablet_meta) const;

    // return a json string to show the compaction status of this tablet
    void get_compaction_status(std::string* json_result);

    // updatable tablet specific operations
    TabletUpdates* updates() { return _updates.get(); }
    [[nodiscard]] Status rowset_commit(int64_t version, const RowsetSharedPtr& rowset, uint32_t wait_time = 0);

    // if there is _compaction_task running
    // do not do compaction
    bool need_compaction();

    bool force_base_compaction();

    double compaction_score();
    CompactionType compaction_type();

    void set_compaction_context(std::unique_ptr<CompactionContext>& context);

    std::shared_ptr<CompactionTask> create_compaction_task();

    bool has_compaction_task();

    void stop_compaction();

    void reset_compaction_status();

    bool enable_compaction();

    std::string get_storage_type() const { return _tablet_meta->get_storage_type(); }

    const bool is_column_with_row_store() const {
        return boost::algorithm::to_lower_copy(get_storage_type()) == "column_with_row";
    }

    [[nodiscard]] bool get_enable_persistent_index() { return _tablet_meta->get_enable_persistent_index(); }

    void set_enable_persistent_index(bool enable_persistent_index) {
        return _tablet_meta->set_enable_persistent_index(enable_persistent_index);
    }

    [[nodiscard]] Status support_binlog();

    // This will modify the TabletMeta, and save_meta() will be called outside
    // to persist it. See run_update_meta_info_task() in agent_task.cpp
    void update_binlog_config(const BinlogConfig& binlog_config);

    BinlogManager* binlog_manager() { return _binlog_manager == nullptr ? nullptr : _binlog_manager.get(); }

    [[nodiscard]] Status contains_version(const Version& version);

    void get_basic_info(TabletBasicInfo& info);

    const TabletSchemaCSPtr tablet_schema() const override;

    const TabletSchema& unsafe_tablet_schema_ref() const override;

    const TabletSchemaCSPtr thread_safe_get_tablet_schema() const;

    void update_max_version_schema(const TabletSchemaCSPtr& tablet_schema);

    int64_t data_size();

    int64_t in_writing_data_size();

    void add_in_writing_data_size(int64_t txn_id, int64_t delta);

    void remove_in_writing_data_size(int64_t txn_id);

    // verify all rowsets of current(max) version in this tablet
    [[nodiscard]] Status verify();

    void update_max_continuous_version() { _timestamped_version_tracker.update_max_continuous_version(); }

    void set_will_be_force_replaced() { _will_be_force_replaced = true; }

protected:
    void on_shutdown() override;

private:
    int64_t _mem_usage() { return sizeof(Tablet); }

    Status _init_once_action();
    void _print_missed_versions(const std::vector<Version>& missed_versions) const;
    bool _contains_rowset(const RowsetId rowset_id);
    Status _contains_version(const Version& version);
    Version _max_continuous_version_from_beginning_unlocked() const;
    void _delete_inc_rowset_by_version(const Version& version);
    /// Delete stale rowset by version. This method not only delete the version in expired rowset map,
    /// but also delete the version in rowset meta vector.
    void _delete_stale_rowset_by_version(const Version& version);
    Status _capture_consistent_rowsets_unlocked(const vector<Version>& version_path,
                                                vector<RowsetSharedPtr>* rowsets) const;

    // The process to generate binlog when publishing a rowset. These methods are protected by _meta_lock
    // _prepare_binlog_if_needed: persist the binlog file before saving the rowset meta in add_inc_rowset()
    //              but the in-memory binlog meta in BinlogManager is not modified, so the binlog
    //              is not visible
    // _commit_binlog: if successful to save rowset meta in add_inc_rowset(), make the newly binlog
    //              file visible. _commit_binlog is expected to be always successful because it just
    //              modifies the in-memory binlog metas
    // _abort_binlog: if failed to save rowset meta, clean up the binlog file generated in
    //              prepare_binlog

    // Prepare the binlog if needed. Return false if no need to prepare binlog, such as the binlog
    // is disabled, otherwise will prepare the binlog. true will be returned if prepare successfully,
    // other status if error happens during preparation.
    StatusOr<bool> _prepare_binlog_if_needed(const RowsetSharedPtr& rowset, int64_t version);
    void _commit_binlog(int64_t version);
    void _abort_binlog(const RowsetSharedPtr& rowset, int64_t version);
    // check whether there is useless binlog, and update the in-memory TabletMeta to the state after
    // those binlog is deleted. Return true the meta has been changed, and needs to be persisted
    bool _check_useless_binlog_and_update_meta(int64_t current_second);

    friend class TabletUpdates;
    static const int64_t kInvalidCumulativePoint = -1;

    TimestampedVersionTracker _timestamped_version_tracker;

    // Max schema_version schema from Rowset or FE
    TabletSchemaCSPtr _max_version_schema;

    OnceFlag _init_once;
    // meta store lock is used for prevent 2 threads do checkpoint concurrently
    // it will be used in econ-mode in the future
    // This lock will be also used for prevent SnapshotLoader::move and checkpoint
    // concurrently for restoring the tablet.
    std::shared_mutex _meta_store_lock;
    std::mutex _ingest_lock;
    std::shared_mutex _base_lock;
    std::shared_mutex _cumulative_lock;

    std::shared_mutex _migration_lock;
    // should use with migration lock.
    std::atomic<bool> _is_migrating{false};

    // explain how these two locks work together.
    mutable std::shared_mutex _meta_lock;
    mutable std::shared_mutex _schema_lock;
    // A new load job will produce a new rowset, which will be inserted into both _rs_version_map
    // and _inc_rs_version_map. Only the most recent rowsets are kept in _inc_rs_version_map to
    // reduce the amount of data that needs to be copied during the clone task.
    // NOTE: Not all incremental-rowsets are in _rs_version_map. Because after some rowsets
    // are compacted, they will be remove from _rs_version_map, but it may not be deleted from
    // _inc_rs_version_map.
    // Which rowsets should be deleted from _inc_rs_version_map is affected by
    // inc_rowset_expired_sec conf. In addition, the deletion is triggered periodically,
    // So at a certain time point (such as just after a base compaction), some rowsets in
    // _inc_rs_version_map may do not exist in _rs_version_map.
    std::map<Version, RowsetSharedPtr> _rs_version_map;
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _inc_rs_version_map;
    // This variable _stale_rs_version_map is used to record these rowsets which are be compacted.
    // These _stale rowsets are been removed when rowsets' pathVersion is expired,
    // this policy is judged and computed by TimestampedVersionTracker.
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _stale_rs_version_map;

    // States used for updatable tablets only
    std::unique_ptr<TabletUpdates> _updates;

    // compaction related
    std::unique_ptr<CompactionContext> _compaction_context;
    bool _enable_compaction = true;

    std::mutex _compaction_task_lock;

    // used for default base cumulative compaction strategy to control the
    bool _has_running_compaction = false;

    // if this tablet is broken, set to true. default is false
    // timestamp of last cumu compaction failure
    std::atomic<int64_t> _last_cumu_compaction_failure_millis{0};
    // timestamp of last base compaction failure
    std::atomic<int64_t> _last_base_compaction_failure_millis{0};
    // timestamp of last cumu compaction success
    std::atomic<int64_t> _last_cumu_compaction_success_millis{0};
    // timestamp of last base compaction success
    std::atomic<int64_t> _last_base_compaction_success_millis{0};

    std::atomic<TStatusCode::type> _last_cumu_compaction_failure_status = TStatusCode::OK;

    std::atomic<int64_t> _cumulative_point{0};
    std::atomic<int32_t> _newly_created_rowset_num{0};
    std::atomic<int64_t> _last_checkpoint_time{0};

    std::unique_ptr<BinlogManager> _binlog_manager;

    std::unordered_map<int64_t, int64_t> _in_writing_txn_size;

    // this variable indicate tablet will be replaced in TabletManger by
    // another tablet with the same tablet id
    // currently, it will be used in Restore process
    bool _will_be_force_replaced = false;
};

inline bool Tablet::init_succeeded() {
    return invoked(_init_once);
}

inline bool Tablet::is_used() {
    return _data_dir->is_used();
}

inline void Tablet::register_tablet_into_dir() {
    _data_dir->register_tablet(this);
}

inline void Tablet::deregister_tablet_from_dir() {
    _data_dir->deregister_tablet(this);
}

inline const int64_t Tablet::cumulative_layer_point() const {
    return _cumulative_point;
}

inline void Tablet::set_cumulative_layer_point(int64_t new_point) {
    _cumulative_point = new_point;
}

inline KeysType Tablet::keys_type() const {
    return tablet_schema()->keys_type();
}

inline size_t Tablet::num_columns_with_max_version() const {
    return tablet_schema()->num_columns();
}

inline size_t Tablet::num_key_columns_with_max_version() const {
    return tablet_schema()->num_key_columns();
}

inline size_t Tablet::num_rows_per_row_block_with_max_version() const {
    return tablet_schema()->num_rows_per_row_block();
}

inline size_t Tablet::next_unique_id() const {
    return tablet_schema()->next_column_unique_id();
}

inline size_t Tablet::field_index_with_max_version(const string& field_name) const {
    return tablet_schema()->field_index(field_name);
}

inline bool Tablet::enable_shortcut_compaction() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->enable_shortcut_compaction();
}

} // namespace starrocks
