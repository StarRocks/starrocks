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

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/tracer_fwd.h"
#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/olap_common.pb.h"
#include "gutil/macros.h"
#include "storage/memtable_flush_executor.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/segment_flush_executor.h"
#include "storage/tablet.h"

namespace starrocks {

class FlushToken;
class ReplicateToken;
class MemTracker;
class Schema;
class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;

class MemTable;
class MemTableSink;

enum ReplicaState {
    // peer storage engine
    Peer,
    // replicated storage engine
    Primary,
    Secondary,
};

struct DeltaWriterOptions {
    int64_t tablet_id;
    int32_t schema_hash;
    int64_t txn_id;
    int64_t partition_id;
    int64_t sink_id;
    PUniqueId load_id;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots;
    GlobalDictByNameMaps* global_dicts = nullptr;
    Span parent_span;
    int64_t index_id;
    int64_t node_id;
    std::vector<PNetworkAddress> replicas;
    int64_t timeout_ms;
    WriteQuorumTypePB write_quorum;
    std::string merge_condition;
    ReplicaState replica_state;
    bool miss_auto_increment_column = false;
    PartialUpdateMode partial_update_mode = PartialUpdateMode::UNKNOWN_MODE;
    // `ptable_schema_param` is valid during initialization.
    // And it will be set to nullptr because we only need to access it during intialization.
    // If you need to access it after intialization, please make sure the pointer is valid.
    const POlapTableSchemaParam* ptable_schema_param = nullptr;
    int64_t immutable_tablet_size = 0;
    std::map<string, string>* column_to_expr_value = nullptr;
};

enum State {
    kUninitialized,
    kWriting,
    kClosed,
    kAborted,
    kCommitted, // committed state can transfer to kAborted state
};

// Statistics for DeltaWriter
struct DeltaWriterStat {
    std::atomic_int32_t task_count = 0;
    std::atomic_int64_t pending_time_ns = 0;

    // ====== statistics for write()

    // The number of write()
    std::atomic_int32_t write_count = 0;
    // The number of rows to write
    std::atomic_int32_t row_count = 0;
    // Accumulated time for write()
    std::atomic_int64_t write_time_ns = 0;
    // The number that memtable is full
    std::atomic_int32_t memtable_full_count = 0;
    // The number that reach memory limit, and each will
    // trigger memtable flush, and wait for it to finish
    std::atomic_int32_t memory_exceed_count = 0;
    // Accumulated time to wait for flush because of reaching memory limit
    std::atomic_int64_t write_wait_flush_time_ns = 0;

    // ====== statistics for add_segment()

    // The number of add_segment()
    std::atomic_int32_t add_segment_count = 0;
    // Accumulated time for add_segment()
    std::atomic_int32_t add_segment_time_ns = 0;
    // Accumulated io time for add_segment()
    std::atomic_int64_t add_segment_io_time_ns = 0;
    std::atomic_int64_t add_segment_data_size = 0;

    // ====== statistics for close()

    // Time for close()
    std::atomic_int64_t close_time_ns = 0;

    // ====== statistics for commit()

    // Time for commit()
    std::atomic_int64_t commit_time_ns = 0;
    // Time to wait for memtable flush in commit()
    std::atomic_int64_t commit_wait_flush_time_ns = 0;
    // Time to build rowset in commit()
    std::atomic_int64_t commit_rowset_build_time_ns = 0;
    // Time to deal with primary key in commit() which may load data from disk
    std::atomic_int64_t commit_pk_preload_time_ns = 0;
    // Time to wait for replica sync in commit()
    std::atomic_int64_t commit_wait_replica_time_ns = 0;
    // Time to commit txn in commit()
    std::atomic_int64_t commit_txn_commit_time_ns = 0;
};

// Writer for a particular (load, index, tablet).
class DeltaWriter {
public:
    // Create a new DeltaWriter and call `TxnManager::prepare_txn` to register a new trasaction associated with
    // this DeltaWriter.
    static StatusOr<std::unique_ptr<DeltaWriter>> open(const DeltaWriterOptions& opt, MemTracker* mem_tracker);
    ~DeltaWriter();

    DISALLOW_COPY(DeltaWriter);

    // [NOT thread-safe]
    Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    // [thread-safe]
    Status write_segment(const SegmentPB& segment_pb, butil::IOBuf& data);

    // Flush all in-memory data to disk, without waiting.
    // Subsequent `write()`s to this DeltaWriter will fail after this method returned.
    // [NOT thread-safe]
    Status close();

    void cancel(const Status& st);

    // Wait until all data have been flushed to disk, then create a new Rowset.
    // Prerequite: the DeltaWriter has been successfully `close()`d.
    // [NOT thread-safe]
    Status commit();

    // Manual flush used by stale memtable flush
    Status manual_flush();

    Status flush_memtable_async(bool eos = false);

    // Rollback all writes and delete the Rowset created by 'commit()', if any.
    // [thread-safe]
    //
    // with_log is used to control whether to print the log when rollback txn.
    // with_log is false when there is no data load into one partition and abort
    // the related txn.
    void abort(bool with_log = true);

    int64_t txn_id() const { return _opt.txn_id; }

    const PUniqueId& load_id() const { return _opt.load_id; }

    int64_t index_id() const { return _opt.index_id; }

    int64_t partition_id() const;

    int64_t node_id() const { return _opt.node_id; }

    const std::vector<PNetworkAddress>& replicas() const { return _opt.replicas; }

    const Tablet* tablet() const { return _tablet.get(); }

    MemTracker* mem_tracker() { return _mem_tracker; };

    // Return the rowset created by `commit()`, or nullptr if `commit()` not been called or failed.
    const Rowset* committed_rowset() const { return _cur_rowset.get(); }

    // REQUIRE: has successfully `commit()`ed
    const RowsetWriter* committed_rowset_writer() const { return _rowset_writer.get(); }

    const ReplicateToken* replicate_token() const { return _replicate_token.get(); }

    SegmentFlushToken* segment_flush_token() const { return _segment_flush_token.get(); }

    // REQUIRE: has successfully `commit()`ed
    const DictColumnsValidMap& global_dict_columns_valid_info() const {
        CHECK_EQ(kCommitted, _state);
        CHECK(_rowset_writer != nullptr);
        return _rowset_writer->global_dict_columns_valid_info();
    }

    ReplicaState replica_state() const { return _replica_state; }

    State get_state() const;

    Status get_err_status() const;

    const FlushStatistic& get_flush_stats() const { return _flush_token->get_stats(); }

    bool is_immutable() const { return _is_immutable.load(std::memory_order_relaxed); }

    int64_t last_write_ts() const { return _last_write_ts; }

    int64_t write_buffer_size() const { return _write_buffer_size; }

    void update_task_stat(int32_t num_tasks, int64_t pending_time_ns) {
        _stats.task_count.fetch_add(num_tasks, std::memory_order_relaxed);
        _stats.pending_time_ns.fetch_add(pending_time_ns, std::memory_order_relaxed);
    }
    const DeltaWriterStat& get_writer_stat() const { return _stats; }

    static bool is_partial_update_with_sort_key_conflict(const PartialUpdateMode& partial_update_mode,
                                                         const std::vector<int32_t>& referenced_column_ids,
                                                         const std::vector<ColumnId>& sort_key_idxes,
                                                         size_t num_key_columns);

private:
    DeltaWriter(DeltaWriterOptions opt, MemTracker* parent, StorageEngine* storage_engine);

    Status _init();
    Status _flush_memtable();
    Status _build_current_tablet_schema(int64_t index_id, const POlapTableSchemaParam* table_schema_param,
                                        const TabletSchemaCSPtr& ori_tablet_schema);

    const char* _state_name(State state) const;
    const char* _replica_state_name(ReplicaState state) const;
    Status _fill_auto_increment_id(const Chunk& chunk);
    Status _check_partial_update_with_sort_key(const Chunk& chunk);

    void _garbage_collection();

    void _reset_mem_table();

    void _set_state(State state, const Status& st);

    State _state;
    Status _err_status;
    mutable std::mutex _state_lock;

    ReplicaState _replica_state;
    DeltaWriterOptions _opt;
    MemTracker* _mem_tracker;
    StorageEngine* _storage_engine;

    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    bool _schema_initialized;
    Schema _vectorized_schema;
    std::unique_ptr<MemTable> _mem_table;
    std::unique_ptr<MemTableSink> _mem_table_sink;
    // tablet schema owned by delta writer, all write will use this tablet schema
    // it's build from unsafe_tablet_schema_ref（stored when create tablet） and OlapTableSchema
    // every request will have it's own tablet schema so simple schema change can work
    TabletSchemaCSPtr _tablet_schema;

    std::unique_ptr<FlushToken> _flush_token;
    std::unique_ptr<ReplicateToken> _replicate_token;
    std::unique_ptr<SegmentFlushToken> _segment_flush_token;
    bool _with_rollback_log;
    // initial value is max value
    size_t _memtable_buffer_row = std::numeric_limits<size_t>::max();
    bool _partial_schema_with_sort_key_conflict = false;
    std::atomic<bool> _is_immutable = false;

    int64_t _last_write_ts = 0;
    // for concurrency issue, we can't get write_buffer_size from memtable directly
    int64_t _write_buffer_size = 0;

    DeltaWriterStat _stats;
};

} // namespace starrocks
