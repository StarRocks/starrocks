// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/mysql_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace starrocks {

class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

namespace vectorized {
#define APPLY_FOR_NUMERICAL_TYPE(M, APPEND_TO_SQL) \
    M(TYPE_TINYINT, APPEND_TO_SQL)                 \
    M(TYPE_BOOLEAN, APPEND_TO_SQL)                 \
    M(TYPE_SMALLINT, APPEND_TO_SQL)                \
    M(TYPE_INT, APPEND_TO_SQL)                     \
    M(TYPE_BIGINT, APPEND_TO_SQL)

#define APPLY_FOR_VARCHAR_DATE_TYPE(M, APPEND_TO_SQL) \
    M(TYPE_DATE, APPEND_TO_SQL)                       \
    M(TYPE_DATETIME, APPEND_TO_SQL)                   \
    M(TYPE_CHAR, APPEND_TO_SQL)                       \
    M(TYPE_VARCHAR, APPEND_TO_SQL)

class MysqlScanNode final : public ScanNode {
public:
    MysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MysqlScanNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    // initialize _mysql_scanner, and create _text_converter.
    Status prepare(RuntimeState* state) override;

    // Start MySQL scan using _mysql_scanner.
    Status open(RuntimeState* state) override;

    // Fill the next chunk by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

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
    // limit for query with external table.
    int64_t _limit;

    // Descriptor of tuples read from MySQL table.
    const TupleDescriptor* _tuple_desc;
    // Tuple index in tuple row.
    size_t _slot_num = 0;
    // Pool for allocating tuple data, including all varying-length slots.
    std::unique_ptr<MemPool> _tuple_pool;
    std::unique_ptr<MysqlScanner> _mysql_scanner;
};
} // namespace vectorized
} // namespace starrocks
