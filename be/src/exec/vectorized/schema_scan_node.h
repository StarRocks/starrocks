// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/scan_node.h"
#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"

namespace starrocks {
class TupleDescriptor;
class RuntimeState;
class Status;
} // namespace starrocks

namespace starrocks::vectorized {

class SchemaScanNode final : public ScanNode {
public:
    SchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~SchemaScanNode() override;

    // Prepare conjuncts, create Schema columns to slots mapping
    // initialize _schema_scanner
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    // Prepare conjuncts, create Schema columns to slots mapping
    // initialize _schema_scanner
    Status prepare(RuntimeState* state) override;

    // Start Schema scan using _schema_scanner.
    Status open(RuntimeState* state) override;

    // Fill the next chunk by calling next() on the _schema_scanner,
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    // Close the _schema_scanner, and report errors.
    Status close(RuntimeState* state) override;

    // this is no use in this class
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    // Write debug string of this into out.
    void debug_string(int indentation_level, std::stringstream* out) const override;

    bool _is_init;
    bool _is_finished = false;
    const std::string _table_name;
    SchemaScannerParam _scanner_param;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of dest tuples
    const TupleDescriptor* _dest_tuple_desc;
    // Jni helper for scanning an schema table.
    std::unique_ptr<SchemaScanner> _schema_scanner;

    // TODO(xueli): remove this when fe and be version both >= 1.19
    // Map from index in desc slots to column of src schema table.
    std::vector<int> _index_map;

    RuntimeProfile::Counter* _filter_timer = nullptr;
};

} // namespace starrocks::vectorized
