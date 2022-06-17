// This file is made available under Elastic License 2.0.
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
class Tablet;
class TabletMeta;
class TabletUpdates;
class CompactionTask;
class CompactionContext;
class CompactionCandidate;
class TabletBasicInfo;

using TabletSharedPtr = std::shared_ptr<Tablet>;

namespace vectorized {
class ChunkIterator;
class RowsetReadOptions;
} // namespace vectorized

using ChunkIteratorPtr = std::shared_ptr<vectorized::ChunkIterator>;

class Tablet : public BaseTablet {
public:
    static TabletSharedPtr create_tablet_from_meta(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir = nullptr);

    Tablet(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir);

    Tablet(const Tablet&) = delete;
    const Tablet& operator=(const Tablet&) = delete;

    // for ut
    Tablet();

    ~Tablet() override;

    Status init();
    bool init_succeeded();

    bool is_used();

    void register_tablet_into_dir();
    void deregister_tablet_from_dir();

    void save_meta();
    // Used in clone task, to update local meta when finishing a clone job
    Status revise_tablet_meta(const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                              const std::vector<Version>& versions_to_delete);

    const int64_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int64_t new_point);

    size_t tablet_footprint(); // disk space occupied by tablet
    size_t num_rows();
    int version_count() const;
    Version max_version() const;

    // propreties encapsulated in TabletSchema
    KeysType keys_type() const;
    size_t num_columns() const;
    size_t num_key_columns() const;
    size_t num_short_key_columns() const;
    size_t num_rows_per_row_block() const;
    size_t next_unique_id() const;
    size_t field_index(const string& field_name) const;
    std::string schema_debug_string() const;
    std::string debug_string() const;

    // operation in rowsets
    Status add_rowset(const RowsetSharedPtr& rowset, bool need_persist = true);
    void modify_rowsets(const vector<RowsetSharedPtr>& to_add, const vector<RowsetSharedPtr>& to_delete,
                        std::vector<RowsetSharedPtr>* to_replace);

    // _rs_version_map and _inc_rs_version_map should be protected by _meta_lock
    // The caller must call hold _meta_lock when call this two function.
    RowsetSharedPtr get_rowset_by_version(const Version& version) const;
    RowsetSharedPtr get_inc_rowset_by_version(const Version& version) const;

    RowsetSharedPtr rowset_with_max_version() const;

    Status add_inc_rowset(const RowsetSharedPtr& rowset, int64_t version);
    void delete_expired_inc_rowsets();

    /// Delete stale rowset by timing. This delete policy uses now() munis
    /// config::tablet_rowset_expired_stale_sweep_time_sec to compute the deadline of expired rowset
    /// to delete.  When rowset is deleted, it will be added to StorageEngine unused map and record
    /// need to delete flag.
    void delete_expired_stale_rowset();

    Status capture_consistent_versions(const Version& spec_version, vector<Version>* version_path) const;
    Status check_version_integrity(const Version& version);
    void list_versions(std::vector<Version>* versions) const;

    // REQUIRE: `obtain_header_rdlock()`ed
    Status capture_consistent_rowsets(const Version& spec_version, vector<RowsetSharedPtr>* rowsets) const;

    const DelPredicateArray& delete_predicates() const { return _tablet_meta->delete_predicates(); }
    bool version_for_delete_predicate(const Version& version);

    // message for alter task
    AlterTabletTaskSharedPtr alter_task();
    void add_alter_task(int64_t related_tablet_id, int32_t related_schema_hash,
                        const vector<Version>& versions_to_alter, const AlterTabletType alter_type);
    void delete_alter_task();
    Status set_alter_state(AlterTabletState state);

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
    std::mutex& get_base_lock() { return _base_lock; }

    // cumulative lock
    void obtain_cumulative_lock() { _cumulative_lock.lock(); }
    void release_cumulative_lock() { _cumulative_lock.unlock(); }
    std::mutex& get_cumulative_lock() { return _cumulative_lock; }

    std::shared_mutex& get_migration_lock() { return _migration_lock; }
    // should use with migration lock.
    bool is_migrating() const { return _is_migrating; }
    // should use with migration lock.
    void set_is_migrating(bool is_migrating) { _is_migrating = is_migrating; }

    // check tablet is migrating or has been migrated.
    // if tablet is migrating or has been migrated, return true.
    // should use with migration lock.
    static bool check_migrate(const TabletSharedPtr& tablet);

    // operation for compaction
    bool can_do_compaction();
    const uint32_t calc_cumulative_compaction_score() const;
    const uint32_t calc_base_compaction_score() const;

    // operation for clone
    void calc_missed_versions(int64_t spec_version, vector<Version>* missed_versions);
    void calc_missed_versions_unlocked(int64_t spec_version, vector<Version>* missed_versions) const;

    // This function to find max continuous version from the beginning.
    // For example: If there are 1, 2, 3, 5, 6, 7 versions belongs tablet, then 3 is target.
    Version max_continuous_version_from_beginning() const;

    int64_t last_cumu_compaction_failure_time() { return _last_cumu_compaction_failure_millis; }
    void set_last_cumu_compaction_failure_time(int64_t millis) { _last_cumu_compaction_failure_millis = millis; }

    int64_t last_base_compaction_failure_time() { return _last_base_compaction_failure_millis; }
    void set_last_base_compaction_failure_time(int64_t millis) { _last_base_compaction_failure_millis = millis; }

    int64_t last_cumu_compaction_success_time() { return _last_cumu_compaction_success_millis; }
    void set_last_cumu_compaction_success_time(int64_t millis) { _last_cumu_compaction_success_millis = millis; }

    int64_t last_base_compaction_success_time() { return _last_base_compaction_success_millis; }
    void set_last_base_compaction_success_time(int64_t millis) { _last_base_compaction_success_millis = millis; }

    void delete_all_files();

    bool check_rowset_id(const RowsetId& rowset_id);

    Status set_partition_id(int64_t partition_id);

    TabletInfo get_tablet_info() const;

    void pick_candicate_rowsets_to_cumulative_compaction(int64_t skip_window_sec,
                                                         std::vector<RowsetSharedPtr>* candidate_rowsets);
    void pick_candicate_rowsets_to_base_compaction(std::vector<RowsetSharedPtr>* candidate_rowsets);

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
    Status rowset_commit(int64_t version, const RowsetSharedPtr& rowset);

    // if there is _compaction_task running
    // do not do compaction
    bool need_compaction(CompactionType type) const {
        std::unique_lock wrlock(_meta_lock);
        return _need_compaction_unlock(type);
    }

    // for ut
    void set_compaction_context(std::unique_ptr<CompactionContext>& compaction_context);

    std::vector<CompactionCandidate> get_compaction_candidates(bool need_update_context);

    double compaction_score(CompactionType type) const;

    std::shared_ptr<CompactionTask> get_compaction(CompactionType type, bool create_if_not_exist);

    void stop_compaction();

    void reset_compaction(CompactionType type);

    bool get_enable_persistent_index() { return _tablet_meta->get_enable_persistent_index(); }

    void set_enable_persistent_index(bool enable_persistent_index) {
        return _tablet_meta->set_enable_persistent_index(enable_persistent_index);
    }

    void get_basic_info(TabletBasicInfo& info);

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

    bool _check_versions_completeness();

    std::unique_ptr<CompactionContext> _get_compaction_context();

    // protected by _meta_lock
    void _update_tablet_compaction_context();
    std::vector<CompactionCandidate> _get_compaction_candidates();
    bool _need_compaction_unlock() const;
    bool _need_compaction_unlock(CompactionType type) const;

    friend class TabletUpdates;
    static const int64_t kInvalidCumulativePoint = -1;

    TimestampedVersionTracker _timestamped_version_tracker;

    OnceFlag _init_once;
    // meta store lock is used for prevent 2 threads do checkpoint concurrently
    // it will be used in econ-mode in the future
    std::shared_mutex _meta_store_lock;
    std::mutex _ingest_lock;
    std::mutex _base_lock;
    std::mutex _cumulative_lock;

    std::shared_mutex _migration_lock;
    // should use with migration lock.
    std::atomic<bool> _is_migrating{false};

    // explain how these two locks work together.
    mutable std::shared_mutex _meta_lock;
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
    std::shared_ptr<CompactionTask> _base_compaction_task;
    std::shared_ptr<CompactionTask> _cumulative_compaction_task;

    // if this tablet is broken, set to true. default is false
    // timestamp of last cumu compaction failure
    std::atomic<int64_t> _last_cumu_compaction_failure_millis{0};
    // timestamp of last base compaction failure
    std::atomic<int64_t> _last_base_compaction_failure_millis{0};
    // timestamp of last cumu compaction success
    std::atomic<int64_t> _last_cumu_compaction_success_millis{0};
    // timestamp of last base compaction success
    std::atomic<int64_t> _last_base_compaction_success_millis{0};

    std::atomic<int64_t> _cumulative_point{0};
    std::atomic<int32_t> _newly_created_rowset_num{0};
    std::atomic<int64_t> _last_checkpoint_time{0};
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
    return _tablet_meta->tablet_schema().keys_type();
}

inline size_t Tablet::num_columns() const {
    return _tablet_meta->tablet_schema().num_columns();
}

inline size_t Tablet::num_key_columns() const {
    return _tablet_meta->tablet_schema().num_key_columns();
}

inline size_t Tablet::num_short_key_columns() const {
    return _tablet_meta->tablet_schema().num_short_key_columns();
}

inline size_t Tablet::num_rows_per_row_block() const {
    return _tablet_meta->tablet_schema().num_rows_per_row_block();
}

inline size_t Tablet::next_unique_id() const {
    return _tablet_meta->tablet_schema().next_column_unique_id();
}

inline size_t Tablet::field_index(const string& field_name) const {
    return _tablet_meta->tablet_schema().field_index(field_name);
}

} // namespace starrocks
