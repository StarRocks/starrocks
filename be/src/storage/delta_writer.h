// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/tracer.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/macros.h"
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

namespace vectorized {

class MemTable;
class MemTableSink;

struct DeltaWriterOptions {
    int64_t tablet_id;
    int32_t schema_hash;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots;
    vectorized::GlobalDictByNameMaps* global_dicts = nullptr;
    Span parent_span;
    int64_t index_id;
    int64_t node_id;
    bool is_replicated_storage = false;
    std::vector<PNetworkAddress> replicas;
    int64_t timeout_ms;
    WriteQuorumTypePB write_quorum;
};

enum ReplicaState {
    // peer storage engine
    Peer,
    // replicated storage engine
    Primary,
    Secondary,
};

enum State {
    kUninitialized,
    kWriting,
    kClosed,
    kAborted,
    kCommitted, // committed state can transfer to kAborted state
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
    [[nodiscard]] Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    // [thread-safe]
    [[nodiscard]] Status write_segment(const SegmentPB& segment_pb, butil::IOBuf& data);

    // Flush all in-memory data to disk, without waiting.
    // Subsequent `write()`s to this DeltaWriter will fail after this method returned.
    // [NOT thread-safe]
    [[nodiscard]] Status close();

    // Wait until all data have been flushed to disk, then create a new Rowset.
    // Prerequite: the DeltaWriter has been successfully `close()`d.
    // [NOT thread-safe]
    [[nodiscard]] Status commit();

    // Rollback all writes and delete the Rowset created by 'commit()', if any.
    // [thread-safe]
    //
    // with_log is used to control whether to print the log when rollback txn.
    // with_log is false when there is no data load into one partition and abort
    // the related txn.
    void abort(bool with_log = true);

    int64_t txn_id() const { return _opt.txn_id; }

    const PUniqueId& load_id() const { return _opt.load_id; }

    int64_t partition_id() const;

    int64_t node_id() const { return _opt.node_id; }

    const Tablet* tablet() const { return _tablet.get(); }

    MemTracker* mem_tracker() { return _mem_tracker; };

    // Return the rowset created by `commit()`, or nullptr if `commit()` not been called or failed.
    const Rowset* committed_rowset() const { return _cur_rowset.get(); }

    // REQUIRE: has successfully `commit()`ed
    const RowsetWriter* committed_rowset_writer() const { return _rowset_writer.get(); }

    const ReplicateToken* replicate_token() const { return _replicate_token.get(); }

    // REQUIRE: has successfully `commit()`ed
    const vectorized::DictColumnsValidMap& global_dict_columns_valid_info() const {
        CHECK_EQ(kCommitted, _state);
        CHECK(_rowset_writer != nullptr);
        return _rowset_writer->global_dict_columns_valid_info();
    }

    ReplicaState replica_state() const { return _replica_state; }

    State get_state() const;

    Status get_err_status() const;

private:
    DeltaWriter(DeltaWriterOptions opt, MemTracker* parent, StorageEngine* storage_engine);

    Status _init();
    Status _flush_memtable_async(bool eos = false);
    Status _flush_memtable();
    const char* _state_name(State state) const;
    const char* _replica_state_name(ReplicaState state) const;

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
    const TabletSchema* _tablet_schema;

    std::unique_ptr<FlushToken> _flush_token;
    std::unique_ptr<ReplicateToken> _replicate_token;
    bool _with_rollback_log;
};

} // namespace vectorized

} // namespace starrocks
