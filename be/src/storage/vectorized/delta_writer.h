// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "gen_cpp/internal_service.pb.h"
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

enum WriteType { LOAD = 1, LOAD_DELETE = 2, DELETE = 3 };

struct WriteRequest {
    int64_t tablet_id;
    int32_t schema_hash;
    WriteType write_type;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    TupleDescriptor* tuple_desc;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots;
    vectorized::GlobalDictByNameMaps* global_dicts = nullptr;
};

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class DeltaWriter {
public:
    static Status open(WriteRequest* req, MemTracker* mem_tracker, std::shared_ptr<DeltaWriter>* writer);

    ~DeltaWriter();

    Status write(Chunk* chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    // flush the last memtable to flush queue, must call it before close_wait()
    Status close();
    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    Status close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    Status cancel();

    // submit current memtable to flush queue.
    // this is currently for reducing mem consumption of this delta writer.
    Status flush_memtable_async();
    // wait all memtables in flush queue to be flushed.
    Status wait_memtable_flushed();

    int64_t partition_id() const;

    int64_t mem_consumption() const;

private:
    DeltaWriter(WriteRequest* req, MemTracker* parent, StorageEngine* storage_engine);

    Status _init();

    // push a full memtable to flush executor
    Status _flush_memtable_async();

    void _garbage_collection();

    void _reset_mem_table();

    bool _is_init = false;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::shared_ptr<MemTable> _mem_table;
    const TabletSchema* _tablet_schema;
    bool _delta_written_success;

    StorageEngine* _storage_engine;
    std::unique_ptr<FlushToken> _flush_token;
    std::unique_ptr<MemTracker> _mem_tracker;
    bool _is_cancelled = false;
};

} // namespace vectorized

} // namespace starrocks
