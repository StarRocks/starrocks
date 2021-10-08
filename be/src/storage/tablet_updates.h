// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/olap_common.h"
#include "util/blocking_queue.hpp"

namespace starrocks {

class PrimaryIndex;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class DelVector;
using DelVectorPtr = std::shared_ptr<DelVector>;
class MemTracker;
class SnapshotMeta;
class Tablet;
class TTabletInfo;

namespace vectorized {
class ChunkIterator;
class CompactionState;
class RowsetReadOptions;
class Schema;
} // namespace vectorized

struct EditVersion {
    uint128_t value = 0;
    EditVersion() = default;
    EditVersion(int64_t major, int64_t minor) { value = (((uint128_t)major) << 64) | minor; }
    int64_t major() const { return value >> 64; }
    int64_t minor() const { return (int64_t)(value & 0xffffffffUL); }
    std::string to_string() const;
    bool operator<(const EditVersion& rhs) const { return value < rhs.value; }
    bool operator==(const EditVersion& rhs) const { return value == rhs.value; }
};

inline std::ostream& operator<<(std::ostream& os, const EditVersion& v) {
    return os << v.to_string();
}

struct CompactionInfo {
    EditVersion start_version;
    std::vector<uint32_t> inputs;
    uint32_t output = UINT32_MAX;
};

// maintain all states for updatable tablets
class TabletUpdates {
public:
    explicit TabletUpdates(Tablet& tablet);
    ~TabletUpdates();

    Status init();

    bool is_error() const { return _error; }

    using IteratorList = std::vector<std::shared_ptr<vectorized::ChunkIterator>>;

    // Return NotFound if the |version| does not exist.
    StatusOr<IteratorList> read(int64_t version, const vectorized::Schema& schema,
                                const vectorized::RowsetReadOptions& options);

    // get latest version's number of rows
    size_t num_rows() const;

    // get total size of latest version's rowset files
    size_t data_size() const;

    // get latest version's number of rowsets
    size_t num_rowsets() const;

    // get latest version's version
    int64_t max_version() const;

    // get total number of committed and pending rowsets
    size_t version_count() const;

    // get num of pending rowsets
    size_t num_pending() const;

    Status get_rowsets_total_stats(const std::vector<uint32_t>& rowsets, size_t* total_rows, size_t* total_dels);

    Status rowset_commit(int64_t version, const RowsetSharedPtr& rowset);

    Status save_meta();

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

    // Remove version whose creation time is less than |expire_time|.
    // [thread-safe]
    void remove_expired_versions(int64_t expire_time);

    bool check_rowset_id(const RowsetId& rowset_id) const;

    // Note: EditVersion history count, not like talet.version_count()
    size_t version_history_count() const { return _versions.size(); }

    // get info's version, version_hash, version_count, row_count, data_size
    void get_tablet_info_extra(TTabletInfo* info);

    std::string debug_string() const;

    // Return nullptr if the delta rowset does not exist.
    // |version| does not need to be applied.
    RowsetSharedPtr get_delta_rowset(int64_t version) const;

    // Wait until |version| been applied.
    Status get_applied_rowsets(int64_t version, std::vector<RowsetSharedPtr>* rowsets,
                               EditVersion* full_version = nullptr);

    void to_updates_pb(TabletUpdatesPB* updates_pb) const;

    // Used for schema change, migrate another tablet's version&rowsets to this tablet
    Status load_from_base_tablet(int64_t version, Tablet* base_tablet);

    Status load_snapshot(const SnapshotMeta& snapshot_meta);

    // Clear both in-memory cached and permanently stored meta data:
    //  - primary index
    //  - delete vectors
    //  - tablet meta
    //  - rowset meta
    //  - update state
    //  - logs
    Status clear_meta();

private:
    friend class Tablet;
    friend class PrimaryIndex;

    template <typename K, typename V>
    using OrderedMap = std::map<K, V>;

    struct EditVersionInfo {
        EditVersion version;
        int64_t creation_time;
        std::vector<uint32_t> rowsets;
        // used for rowset commit
        std::vector<uint32_t> deltas;
        // used for compaction commit
        std::unique_ptr<CompactionInfo> compaction;
    };

    struct RowsetStats {
        size_t num_segments = 0;
        size_t num_rows = 0;
        size_t num_dels = 0;
        size_t byte_size = 0;
        int64_t compaction_score = 0;
        std::string to_string() const;
    };

    Status _get_rowsets(int64_t version, std::vector<RowsetSharedPtr>* rowsets, EditVersion* full_version);

    // used for PrimaryIndex load
    Status _get_apply_version_and_rowsets(int64_t* version, std::vector<RowsetSharedPtr>* rowsets,
                                          std::vector<uint32_t>* rowset_ids);

    void _redo_edit_version_log(const EditVersionMetaPB& v);

    // check if needs submit an async apply task
    // assuming _lock is already hold
    void _check_for_apply();

    void _sync_apply_version_idx(const EditVersion& v);

    Status _rowset_commit_unlocked(int64_t version, const RowsetSharedPtr& rowset);

    void _try_commit_pendings_unlocked();

    void _ignore_rowset_commit(int64_t version, const RowsetSharedPtr& rowset);

    void _apply_rowset_commit(const EditVersionInfo& version_info);

    void _apply_compaction_commit(const EditVersionInfo& version_info);

    RowsetSharedPtr _get_rowset(uint32_t rowset_id);

    // wait a version to be applied, so reader can read this version
    // assuming _lock already hold
    Status _wait_for_version(const EditVersion& version, int64_t timeout_ms);

    Status _commit_compaction(std::unique_ptr<CompactionInfo>* info, const RowsetSharedPtr& rowset,
                              EditVersion* commit_version);

    // Find all but the latest already-applied versions whose creation time is less than or
    // equal to |expire_time|, then append them into |expire_list| and erase them from the
    // in-memory version list.
    void _erase_expired_versions(int64_t expire_time, std::vector<std::unique_ptr<EditVersionInfo>>* expire_list);

    std::set<uint32_t> _active_rowsets();

    void _stop_and_wait_apply_done();

    StatusOr<std::unique_ptr<CompactionInfo>> _get_compaction();

    Status _do_compaction(std::unique_ptr<CompactionInfo>* pinfo, MemTracker* mem_tracker, bool wait_apply);

    void _calc_compaction_score(RowsetStats* stats);

    // This method will acquire |_lock|.
    size_t _get_rowset_num_deletes(uint32_t rowsetid);

    // This method will NOT acquire any lock.
    size_t _get_rowset_num_deletes(const Rowset& rowset);

    std::string _debug_string(bool lock, bool abbr = false) const;

    void _print_rowsets(std::vector<uint32_t>& rowsets, std::string* dst, bool abbr) const;

    void _set_error();

    Status _load_from_pb(const TabletUpdatesPB& updates);

    // thread-safe
    void _remove_unused_rowsets();

    // REQUIRE: |_lock| is held.
    void _to_updates_pb_unlocked(TabletUpdatesPB* updates_pb) const;

    void _clear_rowset_del_vec_cache(const Rowset& rowset);

    void _update_total_stats(const std::vector<uint32_t>& rowsets);

private:
    Tablet& _tablet;

    // |_lock| protects |_versions|, |_next_rowset_id|, |_next_log_id|, |_apply_version_idx|, |_pending_commits|.
    mutable std::mutex _lock;
    std::vector<std::unique_ptr<EditVersionInfo>> _versions;
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
    // apply process is running currently
    bool _apply_running = false;

    // used to stop apply thread when shutting-down this tablet
    std::atomic<bool> _apply_stopped = false;
    std::condition_variable _apply_stopped_cond;

    BlockingQueue<RowsetSharedPtr> _unused_rowsets;

    std::atomic<bool> _compaction_running{false};
    int64_t _last_compaction_time_ms = 0;
    int64_t _compaction_cost_seek = 32 * 1024 * 1024; // 32MB

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
    std::unique_ptr<vectorized::CompactionState> _compaction_state;

    // if tablet is in error state, it means some fatal error occurred and we want to
    // keep the scene(internal state) unchanged for further investigation, and don't crash
    // the whole BE, and more more operation on this tablet is allowed
    std::atomic<bool> _error{false};

    TabletUpdates(const TabletUpdates&) = delete;
    const TabletUpdates& operator=(const TabletUpdates&) = delete;
};

} // namespace starrocks
