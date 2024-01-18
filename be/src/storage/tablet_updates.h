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

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/delta_column_group.h"
#include "storage/edit_version.h"
#include "storage/olap_common.h"
#include "storage/row_store_encoder_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "util/blocking_queue.hpp"

namespace starrocks {

class PrimaryIndex;
class PersistentIndex;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class DelVector;
using DelVectorPtr = std::shared_ptr<DelVector>;
class MemTracker;
class RowsetReadOptions;
class SnapshotMeta;
class Tablet;
struct TabletBasicInfo;
class TTabletInfo;

class ChunkIterator;
class CompactionState;
class Schema;
class TabletReader;
class ChunkChanger;
class SegmentIterator;
class PrimaryKeyDump;

// save the context when reading from delta column files
struct GetDeltaColumnContext {
    DeltaColumnGroupList dcgs;
    // from delta column filename to segment
    std::unordered_map<std::string, std::shared_ptr<Segment>> dcg_segments;
    // from delta column filename to read file
    std::unordered_map<std::string, std::unique_ptr<RandomAccessFile>> dcg_read_files;
    // main segment
    std::shared_ptr<Segment> segment;

    Status prepareGetDeltaColumnContext(std::shared_ptr<Segment> segment, KVStore* kvstore, const TabletSegmentId& tsid,
                                        int64_t read_version);
};

struct CompactionInfo {
    EditVersion start_version;
    std::vector<uint32_t> inputs;
    uint32_t output = UINT32_MAX;
};

struct EditVersionInfo {
    EditVersion version;
    int64_t creation_time;
    std::vector<uint32_t> rowsets;
    // used for rowset commit
    std::vector<uint32_t> deltas;
    // used for compaction commit
    std::unique_ptr<CompactionInfo> compaction;
    EditVersionInfo() = default;
    // add a copy constructor to better expose to scripting engine
    EditVersionInfo(const EditVersionInfo& rhs) {
        version = rhs.version;
        creation_time = rhs.creation_time;
        rowsets = rhs.rowsets;
        deltas = rhs.deltas;
        if (rhs.compaction) {
            compaction = std::make_unique<CompactionInfo>();
            *compaction = *rhs.compaction;
        }
    }
    // add method to better expose to scripting engine
    CompactionInfo* get_compaction() { return compaction.get(); }
};

// maintain all states for updatable tablets
class TabletUpdates {
public:
    using ColumnUniquePtr = std::unique_ptr<Column>;
    using segment_rowid_t = uint32_t;
    using DeletesMap = std::unordered_map<uint32_t, vector<segment_rowid_t>>;

    TabletUpdates(const TabletUpdates&) = delete;
    const TabletUpdates& operator=(const TabletUpdates&) = delete;

    explicit TabletUpdates(Tablet& tablet);
    ~TabletUpdates();

    Status init();

    bool is_error() const { return _error; }

    std::string get_error_msg() const { return _error_msg; }

    using IteratorList = std::vector<std::shared_ptr<ChunkIterator>>;

    // get latest version's number of rows
    size_t num_rows() const;

    // get total size of latest version's rowset files
    size_t data_size() const;

    // get number of rows and total size of latest version's rowset files together
    std::pair<int64_t, int64_t> num_rows_and_data_size() const;

    // get latest version's number of rowsets
    size_t num_rowsets() const;

    // get latest version's version
    int64_t max_version() const;

    int64_t max_readable_version() const;

    // get total number of committed and pending rowsets
    size_t version_count() const;

    // if need do apply
    bool need_apply() const;

    // get num of pending rowsets
    size_t num_pending() const;

    Status get_rowsets_total_stats(const std::vector<uint32_t>& rowsets, size_t* total_rows, size_t* total_dels);

    Status rowset_commit(int64_t version, const RowsetSharedPtr& rowset, uint32_t wait_time);

    // should only called by UpdateManager's apply thread
    void do_apply();

    // get compaction score for this tablet, should only be called by compaction thread
    //
    // compaction score is an approximation of IO benefits that a compaction can bring
    // assuming for each compaction rowset Rowset_i, with num of rows: r_i, num of deletes: d_i
    // read cost:
    //        C_before = sum(Rf * r_i + C_seek)
    //            Rf: normalized read frequency
    // compaction cost:
    //        C_compaction = sum(Wf * (r_i - d_i))
    //            Wf: normalized write cost
    // after compaction read cost:
    //        C_after = sum(Rf * (r_i - d_i)) + C_seek
    // so total benefit/gain:
    //        G = C_before - C_after - C_compation
    //          = sum((Rf+Wf) * d_i - Wf * r_i + Rf * C_seek) - C_seek
    // so a compaction has a constant part of gain: -C_seek
    // and each input rowset has a gain: (Rf+Wf) * d_i - Wf * r_i + C_seek
    // (if this is negative just skip this rowset)
    //
    // about C_seek, Rf & Wf, these variables are related to multiple factors, like:
    //   * rowset query/read frequency
    //   * storage write performance
    //   * table schema, number of columns
    //   * rowset delvec update frequency
    // so currently we use fixed values
    // TODO(cbl): estimate more suitable values
    int64_t get_compaction_score();

    // perform compaction, should only be called by compaction thread
    Status compaction(MemTracker* mem_tracker);

    // perform compaction with specified rowsets, this may be a manual compaction invoked by tools or data fixing jobs
    Status compaction(MemTracker* mem_tracker, const vector<uint32_t>& input_rowset_ids);
    // picks rowsets whose size is below rowset_size_threshold for compaction
    Status get_rowsets_for_compaction(int64_t rowset_size_threshold, std::vector<uint32_t>& rowset_ids,
                                      size_t& total_bytes);

    // vertical compaction introduced a bug that may generate rowset with lots of small segment files
    // this method go through all rowsets and identify them for further repair
    // return list of <rowsetid, segment file num> pair
    StatusOr<std::vector<std::pair<uint32_t, uint32_t>>> list_rowsets_need_repair_compaction();

    void get_compaction_status(std::string* json_result);

    // Remove version whose creation time is less than |expire_time|.
    // [thread-safe]
    void remove_expired_versions(int64_t expire_time);

    bool check_rowset_id(const RowsetId& rowset_id) const;

    // Note: EditVersion history count, not like talet.version_count()
    size_t version_history_count() const { return _edit_version_infos.size(); }

    // get info's version, version_count, row_count, data_size
    void get_tablet_info_extra(TTabletInfo* info);

    // get average row size
    int64_t get_average_row_size();

    std::string debug_string() const;

    // Return nullptr if the delta rowset does not exist.
    // |version| does not need to be applied.
    RowsetSharedPtr get_delta_rowset(int64_t version) const;

    // Wait until |version| been applied.
    Status get_applied_rowsets(int64_t version, std::vector<RowsetSharedPtr>* rowsets,
                               EditVersion* full_version = nullptr);

    void to_updates_pb(TabletUpdatesPB* updates_pb) const;

    // Used for schema change, migrate another tablet's version&rowsets to this tablet
    Status link_from(Tablet* base_tablet, int64_t request_version, ChunkChanger* chunk_changer,
                     const TabletSchemaCSPtr& base_tablet_schema, const std::string& err_msg_header = "");

    Status convert_from(const std::shared_ptr<Tablet>& base_tablet, int64_t request_version,
                        ChunkChanger* chunk_changer, const TabletSchemaCSPtr& base_tablet_schema,
                        const std::string& err_msg_header = "");

    Status reorder_from(const std::shared_ptr<Tablet>& base_tablet, int64_t request_version,
                        ChunkChanger* chunk_changer, const TabletSchemaCSPtr& base_tablet_schema,
                        const std::string& err_msg_header = "");

    Status load_snapshot(const SnapshotMeta& snapshot_meta, bool restore_from_backup = false,
                         bool save_source_schema = false);

    Status get_latest_applied_version(EditVersion* latest_applied_version);

    // Clear both in-memory cached and permanently stored meta data:
    //  - primary index
    //  - delete vectors
    //  - tablet meta
    //  - rowset meta
    //  - update state
    //  - logs
    Status clear_meta();

    // get column values by rssids and rowids, at currently applied version
    // for example:
    // get_column_values with
    //    column:          {1,3}
    //    with_default:    true
    //    rowids_by_rssid: {4:[1,3], 6:[2,4]}
    // will return:
    // [
    //   [
    //              default_value_for_column 1,
    //          column 1 value@rssid:4 rowid:1,
    //          column 1 value@rssid:4 rowid:3,
    //          column 1 value@rssid:6 rowid:2,
    //          column 1 value@rssid:6 rowid:4,
    //   ],
    //   [
    //              default_value_for_column 2,
    //          column 2 value@rssid:4 rowid:1,
    //          column 2 value@rssid:4 rowid:3,
    //          column 2 value@rssid:6 rowid:2,
    //          column 2 value@rssid:6 rowid:4,
    //   ]
    // ]
    // get_column_values with
    //    column:          {1,3}
    //    with_default:    false
    //    rowids_by_rssid: {4:[1,3], 6:[2,4]}
    // will return:
    // [
    //   [
    //          column 1 value@rssid:4 rowid:1,
    //          column 1 value@rssid:4 rowid:3,
    //          column 1 value@rssid:6 rowid:2,
    //          column 1 value@rssid:6 rowid:4,
    //   ],
    //   [
    //          column 2 value@rssid:4 rowid:1,
    //          column 2 value@rssid:4 rowid:3,
    //          column 2 value@rssid:6 rowid:2,
    //          column 2 value@rssid:6 rowid:4,
    //   ]
    // ]
    Status get_column_values(const std::vector<uint32_t>& column_ids, int64_t read_version, bool with_default,
                             std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                             vector<std::unique_ptr<Column>>* columns, void* state,
                             const TabletSchemaCSPtr& tablet_schema);

    Status get_rss_rowids_by_pk(Tablet* tablet, const Column& keys, EditVersion* read_version,
                                std::vector<uint64_t>* rss_rowids, int64_t timeout_ms = 0);

    Status get_rss_rowids_by_pk_unlock(Tablet* tablet, const Column& keys, EditVersion* read_version,
                                       std::vector<uint64_t>* rss_rowids);

    Status get_missing_version_ranges(std::vector<int64_t>& missing_version_ranges);

    Status get_rowsets_for_incremental_snapshot(const std::vector<int64_t>& missing_version_ranges,
                                                std::vector<RowsetSharedPtr>& rowsets);

    void to_rowset_meta_pb(const std::vector<RowsetMetaSharedPtr>& rowset_metas,
                           std::vector<RowsetMetaPB>& rowset_metas_pb);

    Status check_and_remove_rowset();

    void get_basic_info_extra(TabletBasicInfo& info);

    // methods used by scripting engine
    std::vector<std::string> get_version_list() const;

    std::shared_ptr<EditVersionInfo> get_edit_version(const string& version) const;

    std::shared_ptr<std::unordered_map<uint32_t, RowsetSharedPtr>> get_rowset_map() const;

    Status get_apply_version_and_rowsets(int64_t* version, std::vector<RowsetSharedPtr>* rowsets,
                                         std::vector<uint32_t>* rowset_ids);

    bool check_delta_column_generate_from_version(EditVersion begin_version);

    Status get_rowset_and_segment_idx_by_rssid(uint32_t rssid, RowsetSharedPtr* rowset, uint32_t* segment_idx);

    double get_pk_index_write_amp_score();

    Status pk_index_major_compaction();

    // get the max rowset creation time for largest major version
    int64_t max_rowset_creation_time();

    Status get_rowset_stats(std::map<uint32_t, std::string>* output_rowset_stats);

    Status primary_index_dump(PrimaryKeyDump* dump, PrimaryIndexMultiLevelPB* dump_pb);

private:
    friend class Tablet;
    friend class PrimaryIndex;
    friend class PersistentIndex;
    friend class UpdateManager;
    friend class RowsetUpdateState;

    template <typename K, typename V>
    using OrderedMap = std::map<K, V>;

    struct RowsetStats {
        size_t num_segments = 0;
        size_t num_rows = 0;
        size_t num_dels = 0;
        size_t byte_size = 0;
        size_t row_size = 0;
        int64_t compaction_score = 0;
        bool partial_update_by_column = false;
        std::string to_string() const;
    };

    void _redo_edit_version_log(const EditVersionMetaPB& v);

    // check if needs submit an async apply task
    // assuming _lock is already hold
    void _check_for_apply();

    void _sync_apply_version_idx(const EditVersion& v);

    Status _rowset_commit_unlocked(int64_t version, const RowsetSharedPtr& rowset);

    void _try_commit_pendings_unlocked();

    void _ignore_rowset_commit(int64_t version, const RowsetSharedPtr& rowset);

    void _apply_rowset_commit(const EditVersionInfo& version_info);

    // used for normal update or row-mode partial update
    void _apply_normal_rowset_commit(const EditVersionInfo& version_info, const RowsetSharedPtr& rowset);
    // used for column-mode partial update
    void _apply_column_partial_update_commit(const EditVersionInfo& version_info, const RowsetSharedPtr& rowset);

    void _apply_compaction_commit(const EditVersionInfo& version_info);

    RowsetSharedPtr _get_rowset(uint32_t rowset_id);

    // wait a version to be applied, so reader can read this version
    // assuming _lock already hold
    Status _wait_for_version(const EditVersion& version, int64_t timeout_ms, std::unique_lock<std::mutex>& lock);

    Status _commit_compaction(std::unique_ptr<CompactionInfo>* info, const RowsetSharedPtr& rowset,
                              EditVersion* commit_version);

    void _stop_and_wait_apply_done();

    Status _do_compaction(std::unique_ptr<CompactionInfo>* pinfo);

    void _calc_compaction_score(RowsetStats* stats);

    Status _do_update(uint32_t rowset_id, int32_t upsert_idx, int32_t condition_column, int64_t read_version,
                      const std::vector<ColumnUniquePtr>& upserts, PrimaryIndex& index, int64_t tablet_id,
                      DeletesMap* new_deletes, const TabletSchemaCSPtr& tablet_schema);

    // This method will acquire |_lock|.
    size_t _get_rowset_num_deletes(uint32_t rowsetid);

    // This method will NOT acquire any lock.
    size_t _get_rowset_num_deletes(const Rowset& rowset);

    std::string _debug_string(bool lock, bool abbr = false) const;

    std::string _debug_version_info(bool lock) const;

    std::string _debug_compaction_stats(const std::vector<uint32_t>& input_rowsets, const uint32_t output_rowset);

    void _print_rowsets(std::vector<uint32_t>& rowsets, std::string* dst, bool abbr) const;

    void _set_error(const string& msg);

    Status _load_meta_and_log(const TabletUpdatesPB& tablet_updates_pb);

    Status _load_pending_rowsets();

    Status _load_rowsets_and_check_consistency(std::set<uint32_t>& unapplied_rowsets);

    Status _purge_versions_to_fix_rowset_missing_inconsistency();

    Status _load_from_pb(const TabletUpdatesPB& tablet_updates_pb);

    // thread-safe
    void _remove_unused_rowsets(bool drop_tablet = false);

    // REQUIRE: |_lock| is held.
    void _to_updates_pb_unlocked(TabletUpdatesPB* updates_pb) const;

    void _clear_rowset_del_vec_cache(const Rowset& rowset);

    void _clear_rowset_delta_column_group_cache(const Rowset& rowset);

    void _update_total_stats(const std::vector<uint32_t>& rowsets, size_t* row_count_before, size_t* row_count_after);

    Status _convert_from_base_rowset(const Schema& base_schema, const ChunkIteratorPtr& seg_iterator,
                                     ChunkChanger* chunk_changer, const std::unique_ptr<RowsetWriter>& rowset_writer);

    void _check_creation_time_increasing();

    Status _check_conflict_with_partial_update(CompactionInfo* info);

    // these functions is only used in ut
    void stop_apply(bool apply_stopped) { _apply_stopped = apply_stopped; }

    void check_for_apply() { _check_for_apply(); }

    std::timed_mutex* get_index_lock() { return &_index_lock; }

    Status _get_extra_file_size(int64_t* pindex_size, int64_t* col_size) const;

private:
    Tablet& _tablet;

    // |_lock| protects |_edit_version_infos|, |_next_rowset_id|, |_next_log_id|, |_apply_version_idx|, |_pending_commits|.
    mutable std::mutex _lock;
    std::vector<std::unique_ptr<EditVersionInfo>> _edit_version_infos;
    uint32_t _next_rowset_id = 0;
    uint64_t _next_log_id = 0;
    size_t _apply_version_idx = 0;
    std::condition_variable _apply_version_changed;
    // stores non-continous pending rowset commits, so temporary non-contious
    // commits can success when doing schema-change, currently it's not persistent meta yet,
    // so after BE restart those "committed" will be lost.
    std::map<int64_t, RowsetSharedPtr> _pending_commits;

    mutable std::mutex _rowsets_lock;
    std::unordered_map<uint32_t, RowsetSharedPtr> _rowsets;

    // used for async apply, make sure at most 1 thread is doing applying
    mutable std::mutex _apply_running_lock;
    // make sure at most 1 thread is read or write primary index
    mutable std::timed_mutex _index_lock;
    // apply process is running currently
    bool _apply_running = false;

    // used to stop apply thread when shutting-down this tablet
    std::atomic<bool> _apply_stopped = false;
    std::condition_variable _apply_stopped_cond;

    BlockingQueue<RowsetSharedPtr> _unused_rowsets;

    std::atomic<bool> _compaction_running{false};
    int64_t _last_compaction_time_ms = 0;
    std::atomic<int64_t> _last_compaction_success_millis{0};
    std::atomic<int64_t> _last_compaction_failure_millis{0};

    mutable std::mutex _rowset_stats_lock;
    // maintain current version(applied version) rowsets' stats
    // used to for compaction policy.
    // protected by |_rowset_stats_lock|.
    OrderedMap<uint32_t, std::unique_ptr<RowsetStats>> _rowset_stats;
    // stats for current applied version's total rows/total deletes
    // it will be updated after new EditVersion applied
    // currently just for debug/logging purpose
    size_t _cur_total_rows = 0;
    size_t _cur_total_dels = 0;

    // state used in compaction process
    std::unique_ptr<CompactionState> _compaction_state;

    // if tablet is in error state, it means some fatal error occurred and we want to
    // keep the scene(internal state) unchanged for further investigation, and don't crash
    // the whole BE, and more more operation on this tablet is allowed
    std::atomic<bool> _error{false};
    std::string _error_msg;
};

} // namespace starrocks
