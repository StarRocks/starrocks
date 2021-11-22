// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <bthread/execution_queue.h>

#include "column/vectorized_fwd.h"
#include "gen_cpp/internal_service.pb.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"
#include "storage/vectorized/delta_writer_options.h"

namespace starrocks {

class FlushToken;
class MemTracker;
class Schema;
class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;

namespace vectorized {

class MemTable;

// Writer for a particular (load, index, tablet).
class DeltaWriter {
public:
    static StatusOr<std::unique_ptr<DeltaWriter>> open(DeltaWriterOptions* req, MemTracker* mem_tracker);

    ~DeltaWriter();

    // Disable copy/copy-assign/move/move-assign
    DeltaWriter(const DeltaWriter&) = delete;
    void operator=(const DeltaWriter&) = delete;
    DeltaWriter(DeltaWriter&&) = delete;
    void operator=(DeltaWriter&&) = delete;

    Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    Status commit();

    // NOTE: you can abort the DeltaWriter at any time, even after `commit()` succeeded.
    void abort();

    // REQUIRED: has successfully `commit()`ed
    const vectorized::DictColumnsValidMap& global_dict_columns_valid_info() const {
        CHECK_EQ(kCommitted, _state);
        CHECK(_rowset_writer != nullptr);
        return _rowset_writer->global_dict_columns_valid_info();
    }

    int64_t txn_id() const { return _req.txn_id; }

    const PUniqueId& load_id() const { return _req.load_id; }

    int64_t partition_id() const;

    const Tablet* tablet() const { return _tablet.get(); }

    const Rowset* committed_rowset() const { return _cur_rowset.get(); }

    const RowsetWriter* committed_rowset_writer() const { return _rowset_writer.get(); }

    MemTracker* mem_tracker() { return _mem_tracker; };

private:
    enum State {
        kUninitialized,
        kWriting,
        kAborted,
        kCommitted, // committed state can transfer to cancelled state
    };

    DeltaWriter(DeltaWriterOptions* req, MemTracker* parent, StorageEngine* storage_engine);

    Status _init();

    // push a full memtable to flush executor
    Status _flush_memtable_async();
    Status _flush_memtable_sync();

    void _garbage_collection();

    void _reset_mem_table();

    State _get_state() { return _state.load(std::memory_order_acquire); }
    void _set_state(State state) { _state.store(state, std::memory_order_release); }

    std::atomic<State> _state;
    DeltaWriterOptions _req;
    MemTracker* _mem_tracker;
    StorageEngine* _storage_engine;

    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::unique_ptr<MemTable> _mem_table;
    const TabletSchema* _tablet_schema;

    std::unique_ptr<FlushToken> _flush_token;
};

} // namespace vectorized

} // namespace starrocks
