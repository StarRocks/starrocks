// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/tracer.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/macros.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"

namespace starrocks {

class FlushToken;
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

    // Flush all in-memory data to disk, without waiting.
    // Subsequent `write()`s to this DeltaWriter will fail after this method returned.
    // [NOT thread-safe]
    [[nodiscard]] Status close();

    void cancel(const Status& st);

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

    const Tablet* tablet() const { return _tablet.get(); }

    MemTracker* mem_tracker() { return _mem_tracker; };

    // Return the rowset created by `commit()`, or nullptr if `commit()` not been called or failed.
    const Rowset* committed_rowset() const { return _cur_rowset.get(); }

    // REQUIRE: has successfully `commit()`ed
    const RowsetWriter* committed_rowset_writer() const { return _rowset_writer.get(); }

    // REQUIRE: has successfully `commit()`ed
    const vectorized::DictColumnsValidMap& global_dict_columns_valid_info() const {
        CHECK_EQ(kCommitted, _state);
        CHECK(_rowset_writer != nullptr);
        return _rowset_writer->global_dict_columns_valid_info();
    }

private:
    enum State {
        kUninitialized,
        kWriting,
        kClosed,
        kAborted,
        kCommitted, // committed state can transfer to kAborted state
    };

    DeltaWriter(const DeltaWriterOptions& opt, MemTracker* parent, StorageEngine* storage_engine);

    Status _init();
    Status _flush_memtable_async();
    Status _flush_memtable();
    const char* _state_name(State state) const;

    void _garbage_collection();

    void _reset_mem_table();

    State _get_state() { return _state.load(std::memory_order_acquire); }
    void _set_state(State state) { _state.store(state, std::memory_order_release); }

    std::atomic<State> _state;
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
    bool _with_rollback_log;
};

} // namespace vectorized

} // namespace starrocks
