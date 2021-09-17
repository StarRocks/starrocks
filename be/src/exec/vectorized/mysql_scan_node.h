// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/mysql_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace starrocks {

class TextConverter;
class Tuple;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

namespace vectorized {

class MysqlScanNode : public ScanNode {
public:
    MysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MysqlScanNode() override = default;

    // initialize _mysql_scanner, and create _text_converter.
    Status prepare(RuntimeState* state) override;

    // Start MySQL scan using _mysql_scanner.
    Status open(RuntimeState* state) override;

    // Fill the next chunk by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    // Fill the next row batch by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

    // Close the _mysql_scanner, and report errors.
    Status close(RuntimeState* state) override;

    // No use
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    // Write debug string of this into out.
    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    Status append_text_to_column(const char* value, const int& value_length, const SlotDescriptor* slot_desc,
                                 Column* column);

    template <PrimitiveType PT, typename CppType = RunTimeCppType<PT>>
    void append_value_to_column(Column* column, CppType& value);

    bool _is_init;
    bool _is_finished = false;

    MysqlScannerParam _my_param;
    // Name of Mysql table
    std::string _table_name;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // select columns
    std::vector<std::string> _columns;
    // where clause
    std::vector<std::string> _filters;

    // Descriptor of tuples read from MySQL table.
    const TupleDescriptor* _tuple_desc;
    // Tuple index in tuple row.
    size_t _slot_num = 0;
    // Pool for allocating tuple data, including all varying-length slots.
    std::unique_ptr<MemPool> _tuple_pool;
    // Jni helper for scanning an HBase table.
    std::unique_ptr<MysqlScanner> _mysql_scanner;
    // Current tuple.
    Tuple* _tuple = nullptr;
};
} // namespace vectorized
} // namespace starrocks
