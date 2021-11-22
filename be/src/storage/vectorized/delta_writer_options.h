
#pragma once

namespace starrocks::vectorized {

enum WriteType { LOAD = 1, LOAD_DELETE = 2, DELETE = 3 };

struct DeltaWriterOptions {
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

} // namespace starrocks::vectorized
