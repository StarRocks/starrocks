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

#include "formats/parquet/file_reader.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <random>
#include <set>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "exec/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "testutil/assert.h"

namespace starrocks::parquet {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class FileReaderTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }
    void TearDown() override {}

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);

    HdfsScannerContext* _create_scan_context();

    HdfsScannerContext* _create_file1_base_context();
    HdfsScannerContext* _create_context_for_partition();
    HdfsScannerContext* _create_context_for_not_exist();

    HdfsScannerContext* _create_file2_base_context();
    HdfsScannerContext* _create_context_for_min_max();
    HdfsScannerContext* _create_context_for_filter_file();
    HdfsScannerContext* _create_context_for_dict_filter();
    HdfsScannerContext* _create_context_for_other_filter();
    HdfsScannerContext* _create_context_for_skip_group();

    HdfsScannerContext* _create_file3_base_context();
    HdfsScannerContext* _create_context_for_multi_filter();
    HdfsScannerContext* _create_context_for_late_materialization();

    HdfsScannerContext* _create_file4_base_context();
    HdfsScannerContext* _create_context_for_struct_column();
    HdfsScannerContext* _create_context_for_upper_pred();

    HdfsScannerContext* _create_file5_base_context();
    HdfsScannerContext* _create_file6_base_context();
    HdfsScannerContext* _create_file_map_char_key_context();
    HdfsScannerContext* _create_file_map_base_context();

    HdfsScannerContext* _create_file_random_read_context(const std::string& file_path);

    HdfsScannerContext* _create_file_struct_in_struct_read_context(const std::string& file_path);

    HdfsScannerContext* _create_file_struct_in_struct_prune_and_no_output_read_context(const std::string& file_path);

    void _create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                   std::vector<ExprContext*>* conjunct_ctxs);
    void _create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                      std::vector<ExprContext*>* conjunct_ctxs);

    void _create_in_predicate_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, std::set<int32_t>& values,
                                            std::vector<ExprContext*>* conjunct_ctxs);

    void _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                         const TypeDescriptor& type,
                                                         const std::vector<std::string>& subfiled_path,
                                                         const std::string& value,
                                                         std::vector<ExprContext*>* conjunct_ctxs);

    static ChunkPtr _create_chunk();
    static ChunkPtr _create_multi_page_chunk();
    static ChunkPtr _create_struct_chunk();
    static ChunkPtr _create_required_array_chunk();
    static ChunkPtr _create_chunk_for_partition();
    static ChunkPtr _create_chunk_for_not_exist();
    static void _append_column_for_chunk(LogicalType column_type, ChunkPtr* chunk);

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0);

    // Description: A simple parquet file that all columns are null
    //
    // c1      c2      c3       c4
    // -------------------------------------------
    // NULL    NULL    NULL    NULL
    // NULL    NULL    NULL    NULL
    // NULL    NULL    NULL    NULL
    // NULL    NULL    NULL    NULL
    std::string _file1_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet1";

    // Description: A simple parquet file contains single page
    //
    // c1      c2      c3       c4
    // -------------------------------------------
    // 0       10      a       2021-01-01 00:00:00
    // 1       11      b       2021-01-02 00:00:00
    // 2       12      b       2021-01-03 00:00:00
    // 3       13      c       2021-01-04 00:00:00
    // 4       14      c       2021-01-05 00:00:00
    // 5       15      c       2021-01-06 00:00:00
    // 6       16      d       2021-01-07 00:00:00
    // 7       17      d       2021-01-08 00:00:00
    // 8       18      d       2021-01-09 00:00:00
    // 9       19      d       2021-01-10 00:00:00
    // NULL    NULL    NULL    NULL
    std::string _file2_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet2";

    // Description: A parquet file contains multiple pages
    //
    // c1      c2      c3      c4                       c5
    // -------------------------------------------------------------------------
    // 0       10      a       2022-07-12 03:52:14  {"e": 0, "f": 10, "g": 20}
    // 1       11      a       2022-07-12 03:52:14  {"e": 1, "f": 11, "g": 21}
    // 2       12      a       2022-07-12 03:52:14  {"e": 2, "f": 12, "g": 22}
    // 3       13      c       2022-07-12 03:52:14  {"e": 3, "f": 13, "g": 23}
    // 4       14      c       2022-07-12 03:52:14  {"e": 4, "f": 14, "g": 24}
    // 5       15      c       2022-07-12 03:52:14  {"e": 5, "f": 15, "g": 25}
    // 6       16      a       2022-07-12 03:52:14  {"e": 6, "f": 16, "g": 26}
    // ...    ...     ...      ...
    // 4092   4102     a       2022-07-12 03:52:14  {"e": 4092, "f": 4102, "g": 4112}
    // 4093   4103     a       2022-07-12 03:52:14  {"e": 4093, "f": 4103, "g": 4113}
    // 4094   4104     a       2022-07-12 03:52:14  {"e": 4094, "f": 4104, "g": 4114}
    // 4095   4105     a       2022-07-12 03:52:14  {"e": 4095, "f": 4105, "g": 4115}
    std::string _file3_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet3";

    // Description: A complex parquet file contains contains struct, array and uppercase columns
    //
    //c1    | c2                                       | c3   |                      c4                      | B1
    // --------------------------------- ------------------------------------------------------------------------
    // 0    | {'f1': 0, 'f2': 'a', 'f3': [0, 1, 2]}   |  a   |  [{'e1': 0, 'e2': 'a'} {'e1': 1, 'e2': 'a'}]  | A
    // 1    | {'f1': 1, 'f2': 'a', 'f3': [1, 2, 3]}   |  a   |  [{'e1': 1, 'e2': 'a'} {'e1': 2, 'e2': 'a'}]  | A
    // 2    | {'f1': 2, 'f2': 'a', 'f3': [2, 3, 4]}   |  a   |  [{'e1': 2, 'e2': 'a'} {'e1': 3, 'e2': 'a'}]  | A
    // 3    | {'f1': 3, 'f2': 'c', 'f3': [3, 4, 5]}   |  c   |  [{'e1': 3, 'e2': 'c'} {'e1': 4, 'e2': 'c'}]  | C
    // 4    | {'f1': 4, 'f2': 'c', 'f3': [4, 5, 6]}   |  c   |  [{'e1': 4, 'e2': 'c'} {'e1': 5, 'e2': 'c'}]  | C
    // 5    | {'f1': 5, 'f2': 'c', 'f3': [5, 6, 7]}   |  c   |  [{'e1': 5, 'e2': 'c'} {'e1': 6, 'e2': 'c'}]  | C
    // 6    | {'f1': 6, 'f2': 'a', 'f3': [6, 7, 8]}   |  a   |  [{'e1': 6, 'e2': 'a'} {'e1': 7, 'e2': 'a'}]  | A
    // 7    | {'f1': 7, 'f2': 'a', 'f3': [7, 8, 9]}   |  a   |  [{'e1': 7, 'e2': 'a'} {'e1': 8, 'e2': 'a'}]  | A
    // ...... more rows
    // 1023
    std::string _file4_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet4";

    // Description: Column of Array<Array<int>>
    //
    //c1    | c2
    // --------------------------------- ------------------------------------------------------------------------
    // 1    | [[1,2]]
    // 2    | [[1,2],[3,4]]
    // 3    | [[1,2,3],[4]]
    // 4    | [[1,2,3],[4],[5]]
    // 5    | [[1,2,3],[4,5]]
    std::string _file5_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet5";

    // Description: A parquet file contains required array columns
    // col_int  |   col_array
    // 8        |   [8, 80, 800]
    std::string _file6_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet6";

    // Description: A complex parquet file contains contains map
    //
    //c1    | c2                            |  c3                                                   | c4
    // 1    | {'k1': 0, 'k2': 1}            |  {'e1': {'f1': 1, 'f2': 2}}                           | {'g1': [1, 2]}
    // 2    | {'k1': 1, 'k3': 2, 'k4': 3}   |  {'e1': {'f1': 1, 'f2': 2}, 'e2': {'f1': 1, 'f2': 2}} | {'g2': [1], 'g3': [2]}
    // 3    | {'k2': 2, 'k3': 3, 'k5': 4}   |  {'e1': {'f1': 1, 'f2': 2, 'f3': 3}}                  | {'g1': [1, 2, 3]}
    // 4    | {'k1': 3, 'k2': 4, 'k3': 5}   |  {'e2': {'f2': 2}}                                    | {'g1': [1]}
    // 5    | {'k3': 4}                     |  {'e2': {'f2': 2}}                                    | {'g1': [null]}
    // 6    | {'k1': null}                  |  {'e2': {'f2': null}}                                 | {'g1': [1]}
    // 7    | {'k1': 1, 'k2': 2}            |  {'e2': {'f2': 2}}                                    | {'g1': [1, 2, 3]}
    // 8    | {'k3': 4}                     |  {'e1': {'f1': 1, 'f2': 2, 'f3': 3}}                  | {'g2': [1], 'g3': [2]}
    std::string _file_map_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test_map.parquet";

    // Description: A parquet file contains map which key is char
    // It's key_schema is BYTE_ARRAY and optional
    //
    // col_int   | col_map
    // 7         | {"abc-123":-327}
    std::string _file_map_char_key_path =
            "./be/test/exec/test_data/parquet_scanner/file_reader_test_map_char_key.parquet";

    // c0                      int
    // c1                      struct<c1_0:int,c1_1:array<int>>
    // c2                      array<struct<c2_0:int,c2_1:int>>
    // Data:
    // 1    {"c1_0":1,"c1_1":[1,2,3]}       [{"c2_0":1,"c2_1":1},{"c2_0":2,"c2_1":2},{"c2_0":3,"c2_1":3}]
    // 2    {"c1_0":null,"c1_1":[2,3,4]}    [{"c2_0":null,"c2_1":2},{"c2_0":null,"c2_1":3}]
    // 3    {"c1_0":3,"c1_1":[null]}        [{"c2_0":null,"c2_1":null},{"c2_0":null,"c2_1":null},{"c2_0":null,"c2_1":null}]
    // 4    {"c1_0":4,"c1_1":[4,5,6]}       [{"c2_0":4,"c2_1":null},{"c2_0":5,"c2_1":null},{"c2_0":6,"c2_1":4}]
    std::string _file_struct_null_path =
            "./be/test/exec/test_data/parquet_scanner/file_reader_test_struct_null.parquet";

    std::string _file_col_not_null_path = "./be/test/exec/test_data/parquet_scanner/col_not_null.parquet";

    std::string _file_map_null_path = "./be/test/exec/test_data/parquet_scanner/map_null.parquet";

    std::string _file_array_map_path = "./be/test/exec/test_data/parquet_scanner/hudi_array_map.parquet";

    std::string _file_binary_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test_binary.parquet";

    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
};

std::unique_ptr<RandomAccessFile> FileReaderTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

HdfsScannerContext* FileReaderTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file1_base_context() {
    auto ctx = _create_scan_context();

    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file1_path, 1024));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_partition() {
    auto ctx = _create_scan_context();

    Utils::SlotDesc slot_descs[] = {
            // {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            // {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file1_path, 1024));
    auto column = ColumnHelper::create_const_column<LogicalType::TYPE_INT>(1, 1);
    ctx->partition_values.emplace_back(column);

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_not_exist() {
    auto ctx = _create_scan_context();

    Utils::SlotDesc slot_descs[] = {
            // {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            // {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file1_path, 1024));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file2_base_context() {
    auto ctx = _create_scan_context();

    // tuple desc and conjuncts
    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file2_path, 850));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_min_max() {
    auto* ctx = _create_file2_base_context();

    Utils::SlotDesc min_max_slots[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    // create min max conjuncts
    // c1 >= 1
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 1, &ctx->min_max_conjunct_ctxs);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_filter_file() {
    auto* ctx = _create_file2_base_context();

    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);

    // create conjuncts
    // c5 >= 1
    _create_int_conjunct_ctxs(TExprOpcode::GE, 4, 1, &ctx->conjunct_ctxs_by_slot[4]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_dict_filter() {
    auto* ctx = _create_file2_base_context();
    // create conjuncts
    // c3 = "c"
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 2, "c", &ctx->conjunct_ctxs_by_slot[2]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_other_filter() {
    auto* ctx = _create_file2_base_context();
    // create conjuncts
    // c1 >= 4
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 4, &ctx->conjunct_ctxs_by_slot[0]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_skip_group() {
    auto* ctx = _create_file2_base_context();
    // create conjuncts
    // c1 > 10000
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 10000, &ctx->conjunct_ctxs_by_slot[0]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file3_base_context() {
    auto ctx = _create_scan_context();

    // tuple desc and conjuncts
    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file3_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_multi_filter() {
    auto ctx = _create_file3_base_context();
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 2, "c", &ctx->conjunct_ctxs_by_slot[2]);
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 4, &ctx->conjunct_ctxs_by_slot[0]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_late_materialization() {
    auto ctx = _create_file3_base_context();
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 4080, &ctx->conjunct_ctxs_by_slot[0]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file4_base_context() {
    auto ctx = _create_scan_context();

    // tuple desc and conjuncts
    // struct columns are not supported now, so we skip reading them
    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"B1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file5_base_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_inner(LogicalType::TYPE_ARRAY);
    type_inner.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor type_outer(LogicalType::TYPE_ARRAY);
    type_outer.children.emplace_back(type_inner);

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", type_outer},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file5_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_struct_column() {
    auto* ctx = _create_file4_base_context();
    // create conjuncts
    // c3 = "c", c2 is not in slots, so the slot_id=1
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 1, "c", &ctx->conjunct_ctxs_by_slot[1]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_upper_pred() {
    auto* ctx = _create_file4_base_context();
    // create conjuncts
    // B1 = "C", c2,c4 is not in slots, so the slot_id=2
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 2, "C", &ctx->conjunct_ctxs_by_slot[2]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file6_base_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor array_column(LogicalType::TYPE_ARRAY);
    array_column.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    // tuple desc and conjuncts
    // struct columns are not supported now, so we skip reading them
    Utils::SlotDesc slot_descs[] = {
            {"col_int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"col_array", array_column},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file6_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_map_char_key_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_map_char(LogicalType::TYPE_MAP);
    type_map_char.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR));
    type_map_char.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor type_map_varchar(LogicalType::TYPE_MAP);
    type_map_varchar.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map_varchar.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", type_map_char},
            {"c3", type_map_varchar},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_char_key_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_map_base_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_map(LogicalType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor type_map_map(LogicalType::TYPE_MAP);
    type_map_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor type_map_array(LogicalType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map_array.children.emplace_back(type_array);

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", type_map},
            {"c3", type_map_map},
            {"c4", type_map_array},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_random_read_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c3", type_array},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_struct_in_struct_read_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("c0");

    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("c1");

    TypeDescriptor type_struct_in_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct_in_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct_in_struct.field_names.emplace_back("c0");

    type_struct_in_struct.children.emplace_back(type_struct);
    type_struct_in_struct.field_names.emplace_back("c_struct");

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c_struct", type_struct},
            {"c_struct_struct", type_struct_in_struct},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_struct_in_struct_prune_and_no_output_read_context(
        const std::string& file_path) {
    auto ctx = _create_scan_context();

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("c0");

    TypeDescriptor type_struct_in_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct_in_struct.children.emplace_back(type_struct);
    type_struct_in_struct.field_names.emplace_back("c_struct");

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c_struct_struct", type_struct_in_struct},
            {""},
    };
    TupleDescriptor* tupleDescriptor = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    SlotDescriptor* slot = tupleDescriptor->slots()[1];
    TSlotDescriptorBuilder builder;
    builder.column_name(slot->col_name())
            .type(slot->type())
            .id(slot->id())
            .nullable(slot->is_nullable())
            .is_output_column(false);
    TSlotDescriptor tslot = builder.build();
    SlotDescriptor* new_slot = _pool.add(new SlotDescriptor(tslot));
    (tupleDescriptor->slots())[1] = new_slot;
    ctx->tuple_desc = tupleDescriptor;
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->materialized_columns[1].decode_needed = false;
    ctx->scan_ranges.emplace_back(_create_scan_range(file_path));

    return ctx;
}

void FileReaderTest::_create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                               std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = opcode;
    node0.child_type = TPrimitiveType::INT;
    node0.num_children = 2;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::INT_LITERAL;
    node2.type = gen_type_desc(TPrimitiveType::INT);
    node2.num_children = 0;
    TIntLiteral int_literal;
    int_literal.value = value;
    node2.__set_int_literal(int_literal);
    node2.is_nullable = false;
    nodes.emplace_back(node2);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    ASSERT_OK(Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, _runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, _runtime_state));
}

void FileReaderTest::_create_in_predicate_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                        std::set<int32_t>& values,
                                                        std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::IN_PRED;
    node0.opcode = opcode;
    node0.child_type = TPrimitiveType::INT;
    node0.num_children = values.size() + 1;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    for (int32_t value : values) {
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TIntLiteral int_literal;
        int_literal.value = value;
        node.__set_int_literal(int_literal);
        node.is_nullable = false;
        nodes.emplace_back(node);
    }

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    ASSERT_OK(Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, _runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, _runtime_state));
}

void FileReaderTest::_create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                                  std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = opcode;
    node0.child_type = TPrimitiveType::VARCHAR;
    node0.num_children = 2;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::STRING_LITERAL;
    node2.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node2.num_children = 0;
    TStringLiteral string_literal;
    string_literal.value = value;
    node2.__set_string_literal(string_literal);
    node2.is_nullable = false;
    nodes.emplace_back(node2);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    ASSERT_OK(Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, _runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, _runtime_state));
}

void FileReaderTest::_create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                                     const TypeDescriptor& type,
                                                                     const std::vector<std::string>& subfiled_path,
                                                                     const std::string& value,
                                                                     std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = opcode;
    node0.child_type = TPrimitiveType::VARCHAR;
    node0.num_children = 2;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SUBFIELD_EXPR;
    node1.is_nullable = true;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 1;
    node1.used_subfield_names = subfiled_path;
    node1.__isset.used_subfield_names = true;
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::SLOT_REF;
    node2.type = type.to_thrift();
    node2.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node2.__set_slot_ref(t_slot_ref);
    node2.is_nullable = true;
    nodes.emplace_back(node2);

    TExprNode node3;
    node3.node_type = TExprNodeType::STRING_LITERAL;
    node3.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node3.num_children = 0;
    TStringLiteral string_literal;
    string_literal.value = value;
    node3.__set_string_literal(string_literal);
    node3.is_nullable = false;
    nodes.emplace_back(node3);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    ASSERT_OK(Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, _runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, _runtime_state));
}

THdfsScanRange* FileReaderTest::_create_scan_range(const std::string& file_path, size_t scan_length) {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = file_path;
    scan_range->file_length = std::filesystem::file_size(file_path);
    scan_range->offset = 4;
    scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;

    return scan_range;
}

void FileReaderTest::_append_column_for_chunk(LogicalType column_type, ChunkPtr* chunk) {
    auto c = ColumnHelper::create_column(TypeDescriptor::from_logical_type(column_type), true);
    (*chunk)->append_column(c, (*chunk)->num_columns());
}

ChunkPtr FileReaderTest::_create_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_BIGINT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_DATETIME, &chunk);
    return chunk;
}

ChunkPtr FileReaderTest::_create_multi_page_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_BIGINT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_DATETIME, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_VARCHAR, &chunk);
    return chunk;
}

ChunkPtr FileReaderTest::_create_struct_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_VARCHAR, &chunk);
    return chunk;
}

ChunkPtr FileReaderTest::_create_required_array_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);

    TypeDescriptor array_column(LogicalType::TYPE_ARRAY);
    array_column.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    auto c = ColumnHelper::create_column(array_column, true);
    chunk->append_column(c, chunk->num_columns());
    return chunk;
}

ChunkPtr FileReaderTest::_create_chunk_for_partition() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    return chunk;
}

ChunkPtr FileReaderTest::_create_chunk_for_not_exist() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    return chunk;
}

TEST_F(FileReaderTest, TestInit) {
    auto file = _create_file(_file1_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file1_path));
    // init
    auto* ctx = _create_file1_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
}

TEST_F(FileReaderTest, TestGetNext) {
    auto file = _create_file(_file1_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file1_path));
    // init
    auto* ctx = _create_file1_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(4, chunk->num_rows());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestGetNextWithSkipID) {
    int64_t ids[] = {1};
    std::set<int64_t> need_skip_rowids(ids, ids + 1);
    auto file = _create_file(_file1_path);
    auto file_reader = std::make_shared<FileReader>(
            config::vector_chunk_size, file.get(), std::filesystem::file_size(_file1_path), nullptr, &need_skip_rowids);
    // init
    auto* ctx = _create_file1_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestGetNextPartition) {
    auto file = _create_file(_file1_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file1_path));
    // init
    auto* ctx = _create_context_for_partition();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk_for_partition();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(4, chunk->num_rows());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestGetNextEmpty) {
    auto file = _create_file(_file1_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file1_path));
    // init
    auto* ctx = _create_context_for_not_exist();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk_for_not_exist();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(4, chunk->num_rows());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestMinMaxConjunct) {
    auto file = _create_file(_file2_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file2_path), nullptr);
    // init
    auto* ctx = _create_context_for_min_max();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(11, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestFilterFile) {
    auto file = _create_file(_file2_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file2_path));
    // init
    auto* ctx = _create_context_for_filter_file();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // check file is filtered
    ASSERT_TRUE(file_reader->_is_file_filtered);

    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestGetNextDictFilter) {
    auto file = _create_file(_file2_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file2_path));
    // init
    auto* ctx = _create_context_for_dict_filter();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    {
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_column_indices[0];
        ASSERT_EQ(2, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id);
    }

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestGetNextOtherFilter) {
    auto file = _create_file(_file2_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file2_path));
    // init
    auto* ctx = _create_context_for_other_filter();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c1 is other conjunct filter column
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(6, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestSkipRowGroup) {
    auto file = _create_file(_file2_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file2_path));
    // c1 > 10000
    auto* ctx = _create_context_for_skip_group();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, chunk->num_rows());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestMultiFilterWithMultiPage) {
    auto file = _create_file(_file3_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file3_path));
    // c3 = "c", c1 >= 4
    auto* ctx = _create_context_for_multi_filter();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    {
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_column_indices[0];
        ASSERT_EQ(2, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id);
    }

    // c0 is conjunct filter column
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    // get next
    auto chunk = _create_multi_page_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(2, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }

    status = file_reader->get_next(&chunk);
    // for both group_reader.cpp _do_get_next && _do_get_next_new
    ASSERT_TRUE(status.is_end_of_file() || status.ok());
}

TEST_F(FileReaderTest, TestOtherFilterWithMultiPage) {
    auto file = _create_file(_file3_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file3_path));
    // c1 >= 4080
    auto* ctx = _create_context_for_late_materialization();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c0 is conjunct filter column
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    // get next
    while (!status.is_end_of_file()) {
        auto chunk = _create_multi_page_chunk();
        status = file_reader->get_next(&chunk);
        if (!status.ok()) {
            break;
        }
    }
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestReadStructUpperColumns) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));
    ;

    // init
    auto* ctx = _create_context_for_struct_column();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    {
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_column_indices[0];
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id);
    }

    // get next
    auto chunk = _create_struct_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());

    ColumnPtr int_col = chunk->get_column_by_slot_id(0);
    int i = int_col->get(0).get_int32();
    EXPECT_EQ(i, 3);

    ColumnPtr char_col = chunk->get_column_by_slot_id(2);
    Slice s = char_col->get(0).get_slice();
    std::string res(s.data, s.size);
    EXPECT_EQ(res, "C");
}

TEST_F(FileReaderTest, TestReadWithUpperPred) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // init
    auto* ctx = _create_context_for_upper_pred();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_struct_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    LOG(ERROR) << "status: " << status.get_error_msg();
    ASSERT_EQ(3, chunk->num_rows());

    ColumnPtr int_col = chunk->get_column_by_slot_id(0);
    int i = int_col->get(0).get_int32();
    EXPECT_EQ(i, 3);

    ColumnPtr char_col = chunk->get_column_by_slot_id(2);
    Slice s = char_col->get(0).get_slice();
    std::string res(s.data, s.size);
    EXPECT_EQ(res, "C");
}

TEST_F(FileReaderTest, TestReadArray2dColumn) {
    auto file = _create_file(_file5_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file5_path));

    //init
    auto* ctx = _create_file5_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 5);

    TypeDescriptor type_inner(LogicalType::TYPE_ARRAY);
    type_inner.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor type_outer(LogicalType::TYPE_ARRAY);
    type_outer.children.emplace_back(type_inner);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(type_outer, true);
    chunk->append_column(c, chunk->num_columns());
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 5);
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }
    EXPECT_EQ(chunk->debug_row(0), "[1, [[1,2]]]");
    EXPECT_EQ(chunk->debug_row(1), "[2, [[1,2],[3,4]]]");
    EXPECT_EQ(chunk->debug_row(2), "[3, [[1,2,3],[4]]]");
    EXPECT_EQ(chunk->debug_row(3), "[4, [[1,2,3],[4],[5]]]");
    EXPECT_EQ(chunk->debug_row(4), "[5, [[1,2,3],[4,5]]]");
}

TEST_F(FileReaderTest, TestReadRequiredArrayColumns) {
    auto file = _create_file(_file6_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file6_path));

    // init
    auto* ctx = _create_file6_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_required_array_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }
}

// when key type is char or varchar, not string
// the real type is BYTE_ARRAY which is OPTIONAL
TEST_F(FileReaderTest, TestReadMapCharKeyColumn) {
    auto file = _create_file(_file_map_char_key_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_map_char_key_path));

    //init
    auto* ctx = _create_file_map_char_key_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok()) << status.get_error_msg();

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c2.value, c3.key, c3.value
    EXPECT_EQ(ranges.size(), 5);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 1);
    TypeDescriptor type_map_char(LogicalType::TYPE_MAP);
    type_map_char.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR));
    type_map_char.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor type_map_varchar(LogicalType::TYPE_MAP);
    type_map_varchar.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map_varchar.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(type_map_char, true);
    chunk->append_column(c, chunk->num_columns());
    auto c_map1 = ColumnHelper::create_column(type_map_varchar, true);
    chunk->append_column(c_map1, chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok()) << status.get_error_msg();
    EXPECT_EQ(chunk->num_rows(), 1);
    EXPECT_EQ(chunk->debug_row(0), "[0, {'abc':123}, {'def':456}]");
}

TEST_F(FileReaderTest, TestReadMapColumn) {
    auto file = _create_file(_file_map_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_map_path));

    //init
    auto* ctx = _create_file_map_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c2.value, c3.key, c3.value.key, c3.value.value, c4.key. c4.value
    EXPECT_EQ(ranges.size(), 8);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 8);
    TypeDescriptor type_map(LogicalType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor type_map_map(LogicalType::TYPE_MAP);
    type_map_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor type_map_array(LogicalType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map_array.children.emplace_back(type_array);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(type_map, true);
    chunk->append_column(c, chunk->num_columns());
    auto c_map_map = ColumnHelper::create_column(type_map_map, true);
    chunk->append_column(c_map_map, chunk->num_columns());
    auto c_map_array = ColumnHelper::create_column(type_map_array, true);
    chunk->append_column(c_map_array, chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 8);
    EXPECT_EQ(chunk->debug_row(0), "[1, {'k1':0,'k2':1}, {'e1':{'f1':1,'f2':2}}, {'g1':[1,2]}]");
    EXPECT_EQ(chunk->debug_row(1),
              "[2, {'k1':1,'k3':2,'k4':3}, {'e1':{'f1':1,'f2':2},'e2':{'f1':1,'f2':2}}, {'g1':[1],'g3':[2]}]");
    EXPECT_EQ(chunk->debug_row(2), "[3, {'k2':2,'k3':3,'k5':4}, {'e1':{'f1':1,'f2':2,'f3':3}}, {'g1':[1,2,3]}]");
    EXPECT_EQ(chunk->debug_row(3), "[4, {'k1':3,'k2':4,'k3':5}, {'e2':{'f2':2}}, {'g1':[1]}]");
    EXPECT_EQ(chunk->debug_row(4), "[5, {'k3':4}, {'e2':{'f2':2}}, {'g1':[NULL]}]");
    EXPECT_EQ(chunk->debug_row(5), "[6, {'k1':NULL}, {'e2':{'f2':NULL}}, {'g1':[1]}]");
    EXPECT_EQ(chunk->debug_row(6), "[7, {'k1':1,'k2':2}, {'e2':{'f2':2}}, {'g1':[1,2,3]}]");
    EXPECT_EQ(chunk->debug_row(7), "[8, {'k3':4}, {'e1':{'f1':1,'f2':2,'f3':3}}, {'g2':[1],'g3':[2]}]");
}

TEST_F(FileReaderTest, TestReadStruct) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    // Test unordered field name
    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c2.field_names.emplace_back("f2");

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2.field_names.emplace_back("f1");

    TypeDescriptor f3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("f3");

    TypeDescriptor c3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    TypeDescriptor c4 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

    // start to build inner struct
    TypeDescriptor c4_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    c4_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c4_struct.field_names.emplace_back("e1");

    c4_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c4_struct.field_names.emplace_back("e2");
    // end to build inner struct

    c4.children.emplace_back(c4_struct);

    TypeDescriptor B1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    Utils::SlotDesc slot_descs[] = {
            {"c1", c1}, {"c2", c2}, {"c3", c3}, {"c4", c4}, {"B1", B1}, {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.f1, c2.f2, c2.f3, c3, c4.e1, c4.e2, B1
    EXPECT_EQ(ranges.size(), 8);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c3, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c4, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(B1, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024, chunk->num_rows());

    EXPECT_EQ("[0, {f2:'a',f1:0,f3:[0,1,2]}, 'a', [{e1:0,e2:'a'},{e1:1,e2:'a'}], 'A']", chunk->debug_row(0));
    EXPECT_EQ("[1, {f2:'a',f1:1,f3:[1,2,3]}, 'a', [{e1:1,e2:'a'},{e1:2,e2:'a'}], 'A']", chunk->debug_row(1));
    EXPECT_EQ("[2, {f2:'a',f1:2,f3:[2,3,4]}, 'a', [{e1:2,e2:'a'},{e1:3,e2:'a'}], 'A']", chunk->debug_row(2));
    EXPECT_EQ("[3, {f2:'c',f1:3,f3:[3,4,5]}, 'c', [{e1:3,e2:'c'},{e1:4,e2:'c'}], 'C']", chunk->debug_row(3));
    EXPECT_EQ("[4, {f2:'c',f1:4,f3:[4,5,6]}, 'c', [{e1:4,e2:'c'},{e1:5,e2:'c'}], 'C']", chunk->debug_row(4));
    EXPECT_EQ("[5, {f2:'c',f1:5,f3:[5,6,7]}, 'c', [{e1:5,e2:'c'},{e1:6,e2:'c'}], 'C']", chunk->debug_row(5));
    EXPECT_EQ("[6, {f2:'a',f1:6,f3:[6,7,8]}, 'a', [{e1:6,e2:'a'},{e1:7,e2:'a'}], 'A']", chunk->debug_row(6));
    EXPECT_EQ("[7, {f2:'a',f1:7,f3:[7,8,9]}, 'a', [{e1:7,e2:'a'},{e1:8,e2:'a'}], 'A']", chunk->debug_row(7));
    EXPECT_EQ("[8, {f2:'a',f1:8,f3:[8,9,10]}, 'a', [{e1:8,e2:'a'},{e1:9,e2:'a'}], 'A']", chunk->debug_row(8));
    EXPECT_EQ("[9, {f2:'a',f1:9,f3:[9,10,11]}, 'a', [{e1:9,e2:'a'},{e1:10,e2:'a'}], 'A']", chunk->debug_row(9));

    //    for (int i = 0; i < 10; ++i) {
    //        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    //    }
}

TEST_F(FileReaderTest, TestReadStructSubField) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2.field_names.emplace_back("f1");

    TypeDescriptor f3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("f3");

    TypeDescriptor c3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    TypeDescriptor c4 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

    // start to build inner struct
    // dont't load subfield e1
    TypeDescriptor c4_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    c4_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c4_struct.field_names.emplace_back("e2");
    // end to build inner struct

    c4.children.emplace_back(c4_struct);

    TypeDescriptor B1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    Utils::SlotDesc slot_descs[] = {
            {"c1", c1}, {"c2", c2}, {"c3", c3}, {"c4", c4}, {"B1", B1}, {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.f1, c2.f3, c3, c4.e2, B1
    EXPECT_EQ(ranges.size(), 6);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c3, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c4, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(B1, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024, chunk->num_rows());

    EXPECT_EQ("[0, {f1:0,f3:[0,1,2]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(0));
    EXPECT_EQ("[1, {f1:1,f3:[1,2,3]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(1));
    EXPECT_EQ("[2, {f1:2,f3:[2,3,4]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(2));
    EXPECT_EQ("[3, {f1:3,f3:[3,4,5]}, 'c', [{e2:'c'},{e2:'c'}], 'C']", chunk->debug_row(3));
    EXPECT_EQ("[4, {f1:4,f3:[4,5,6]}, 'c', [{e2:'c'},{e2:'c'}], 'C']", chunk->debug_row(4));
    EXPECT_EQ("[5, {f1:5,f3:[5,6,7]}, 'c', [{e2:'c'},{e2:'c'}], 'C']", chunk->debug_row(5));
    EXPECT_EQ("[6, {f1:6,f3:[6,7,8]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(6));
    EXPECT_EQ("[7, {f1:7,f3:[7,8,9]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(7));
    EXPECT_EQ("[8, {f1:8,f3:[8,9,10]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(8));
    EXPECT_EQ("[9, {f1:9,f3:[9,10,11]}, 'a', [{e2:'a'},{e2:'a'}], 'A']", chunk->debug_row(9));

    //    for (int i = 0; i < 10; ++i) {
    //        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    //    }
}

TEST_F(FileReaderTest, TestReadStructAbsentSubField) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2.field_names.emplace_back("f1");

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c2.field_names.emplace_back("f2");

    TypeDescriptor f3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("f3");

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c2.field_names.emplace_back("not_existed");

    Utils::SlotDesc slot_descs[] = {
            {"c1", c1},
            {"c2", c2},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024, chunk->num_rows());

    EXPECT_EQ("[0, {f1:0,f2:'a',f3:[0,1,2],not_existed:CONST: NULL}]", chunk->debug_row(0));
    EXPECT_EQ("[1, {f1:1,f2:'a',f3:[1,2,3],not_existed:CONST: NULL}]", chunk->debug_row(1));
    EXPECT_EQ("[2, {f1:2,f2:'a',f3:[2,3,4],not_existed:CONST: NULL}]", chunk->debug_row(2));
    EXPECT_EQ("[3, {f1:3,f2:'c',f3:[3,4,5],not_existed:CONST: NULL}]", chunk->debug_row(3));
    EXPECT_EQ("[4, {f1:4,f2:'c',f3:[4,5,6],not_existed:CONST: NULL}]", chunk->debug_row(4));
}

TEST_F(FileReaderTest, TestReadStructCaseSensitive) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2.field_names.emplace_back("F1");

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c2.field_names.emplace_back("F2");

    TypeDescriptor f3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("F3");

    Utils::SlotDesc slot_descs[] = {{"c1", c1}, {"c2", c2}, {""}};
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.get_error_msg() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024, chunk->num_rows());

    EXPECT_EQ("[0, {F1:0,F2:'a',F3:[0,1,2]}]", chunk->debug_row(0));

    //    for (int i = 0; i < 1; ++i) {
    //        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    //    }
}

TEST_F(FileReaderTest, TestReadStructCaseSensitiveError) {
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    ctx->case_sensitive = true;

    TypeDescriptor c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2.field_names.emplace_back("F1");

    c2.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c2.field_names.emplace_back("F2");

    TypeDescriptor f3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("F3");

    Utils::SlotDesc slot_descs[] = {{"c1", c1}, {"c2", c2}, {""}};
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    EXPECT_TRUE(!status.ok());
    if (!status.ok()) {
        std::cout << status.get_error_msg() << std::endl;
    }
}

TEST_F(FileReaderTest, TestReadStructNull) {
    auto file = _create_file(_file_struct_null_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_struct_null_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    ctx->case_sensitive = false;

    TypeDescriptor c0 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    c1.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c1.field_names.emplace_back("c1_0");

    c1.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY));
    c1.field_names.emplace_back("c1_1");

    c1.children.at(1).children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    TypeDescriptor c2_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    c2_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c2_struct.field_names.emplace_back("c2_0");
    c2_struct.field_names.emplace_back("c2_1");
    c2.children.emplace_back(c2_struct);

    Utils::SlotDesc slot_descs[] = {{"c0", c0}, {"c1", c1}, {"c2", c2}, {""}};

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_struct_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.get_error_msg() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(c0, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(4, chunk->num_rows());

    EXPECT_EQ("[1, {c1_0:1,c1_1:[1,2,3]}, [{c2_0:1,c2_1:1},{c2_0:2,c2_1:2},{c2_0:3,c2_1:3}]]", chunk->debug_row(0));
    EXPECT_EQ("[2, {c1_0:NULL,c1_1:[2,3,4]}, [{c2_0:NULL,c2_1:2},{c2_0:NULL,c2_1:3}]]", chunk->debug_row(1));
    EXPECT_EQ("[3, {c1_0:3,c1_1:[NULL]}, [{c2_0:NULL,c2_1:NULL},{c2_0:NULL,c2_1:NULL},{c2_0:NULL,c2_1:NULL}]]",
              chunk->debug_row(2));
    EXPECT_EQ("[4, {c1_0:4,c1_1:[4,5,6]}, [{c2_0:4,c2_1:NULL},{c2_0:5,c2_1:NULL},{c2_0:6,c2_1:4}]]",
              chunk->debug_row(3));
}

TEST_F(FileReaderTest, TestReadBinary) {
    auto file = _create_file(_file_binary_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_binary_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    ctx->case_sensitive = false;

    TypeDescriptor k1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    TypeDescriptor k2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY);

    Utils::SlotDesc slot_descs[] = {{"k1", k1}, {"k2", k2}, {""}};

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_binary_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.get_error_msg() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(k1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(k2, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    std::string s = chunk->debug_row(0);
    EXPECT_EQ("[6, '\017']", chunk->debug_row(0));
}

TEST_F(FileReaderTest, TestReadMapColumnWithPartialMaterialize) {
    auto file = _create_file(_file_map_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_map_path));

    //init
    auto ctx = _create_scan_context();
    {
        TypeDescriptor type_map(LogicalType::TYPE_MAP);
        // only key will be materialized
        type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
        type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));

        TypeDescriptor type_map_map(LogicalType::TYPE_MAP);
        // the first level value will be materialized, and the second level key will be materialized
        type_map_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));
        type_map_map.children.emplace_back(type_map);

        TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
        type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        // only value will be materialized
        TypeDescriptor type_map_array(LogicalType::TYPE_MAP);
        type_map_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));
        type_map_array.children.emplace_back(type_array);

        // tuple desc
        Utils::SlotDesc slot_descs[] = {
                {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {"c2", type_map},
                {"c3", type_map_map},
                {"c4", type_map_array},
                {""},
        };
        ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_path));
    }

    ColumnAccessPathPtr c2_path = ColumnAccessPathUtil::create(TAccessPathType::ROOT, "c2").value();
    c2_path->put_child_path(ColumnAccessPathUtil::create(TAccessPathType::KEY, "P").value());

    ColumnAccessPathPtr c3_path = ColumnAccessPathUtil::create(TAccessPathType::ROOT, "c3").value();
    ColumnAccessPathPtr c3_map_path = ColumnAccessPathUtil::create(TAccessPathType::VALUE, "P").value();
    ColumnAccessPathPtr c3_map_index_path = ColumnAccessPathUtil::create(TAccessPathType::INDEX, "P").value();
    ColumnAccessPathPtr c3_map_index_map_path = ColumnAccessPathUtil::create(TAccessPathType::KEY, "P").value();
    c3_map_index_path->put_child_path(std::move(c3_map_index_map_path));
    c3_map_path->put_child_path(std::move(c3_map_index_path));
    c3_path->put_child_path(std::move(c3_map_path));

    ColumnAccessPathPtr c4_path = ColumnAccessPathUtil::create(TAccessPathType::ROOT, "c4").value();
    c4_path->put_child_path(ColumnAccessPathUtil::create(TAccessPathType::VALUE, "P").value());

    std::unordered_map<std::string, ColumnAccessPathPtr> path_mapping{};
    path_mapping.emplace("c2", std::move(c2_path));
    path_mapping.emplace("c3", std::move(c3_path));
    path_mapping.emplace("c4", std::move(c4_path));

    ctx->column_name_2_column_access_path_mapping = &path_mapping;

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok()) << status.get_error_msg();

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c3.value.key, c4.value
    EXPECT_EQ(ranges.size(), 4);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 8);
    TypeDescriptor type_map(LogicalType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));

    TypeDescriptor type_map_map(LogicalType::TYPE_MAP);
    type_map_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor type_map_array(LogicalType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));
    type_map_array.children.emplace_back(type_array);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(type_map, true);
    chunk->append_column(c, chunk->num_columns());
    auto c_map_map = ColumnHelper::create_column(type_map_map, true);
    chunk->append_column(c_map_map, chunk->num_columns());
    auto c_map_array = ColumnHelper::create_column(type_map_array, true);
    chunk->append_column(c_map_array, chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 8);
    EXPECT_EQ(chunk->debug_row(1),
              "[2, {'k1':CONST: NULL,'k3':CONST: NULL,'k4':CONST: NULL}, {CONST: NULL:{'f1':CONST: NULL,'f2':CONST: "
              "NULL},CONST: NULL:{'f1':CONST: NULL,'f2':CONST: NULL}}, "
              "{CONST: NULL:[1],CONST: NULL:[2]}]");
    EXPECT_EQ(chunk->debug_row(2),
              "[3, {'k2':CONST: NULL,'k3':CONST: NULL,'k5':CONST: NULL}, {CONST: NULL:{'f1':CONST: NULL,'f2':CONST: "
              "NULL,'f3':CONST: NULL}}, {CONST: NULL:[1,2,3]}]");
    EXPECT_EQ(chunk->debug_row(3),
              "[4, {'k1':CONST: NULL,'k2':CONST: NULL,'k3':CONST: NULL}, {CONST: NULL:{'f2':CONST: NULL}}, {CONST: "
              "NULL:[1]}]");
    EXPECT_EQ(chunk->debug_row(4), "[5, {'k3':CONST: NULL}, {CONST: NULL:{'f2':CONST: NULL}}, {CONST: NULL:[NULL]}]");
    EXPECT_EQ(chunk->debug_row(5), "[6, {'k1':CONST: NULL}, {CONST: NULL:{'f2':CONST: NULL}}, {CONST: NULL:[1]}]");
    EXPECT_EQ(chunk->debug_row(6),
              "[7, {'k1':CONST: NULL,'k2':CONST: NULL}, {CONST: NULL:{'f2':CONST: NULL}}, {CONST: NULL:[1,2,3]}]");
    EXPECT_EQ(chunk->debug_row(7),
              "[8, {'k3':CONST: NULL}, {CONST: NULL:{'f1':CONST: NULL,'f2':CONST: NULL,'f3':CONST: NULL}}, {CONST: "
              "NULL:[1],CONST: NULL:[2]}]");
}

TEST_F(FileReaderTest, TestReadNotNull) {
    auto file = _create_file(_file_col_not_null_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_col_not_null_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor type_map(LogicalType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("a");

    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_struct.field_names.emplace_back("b");

    Utils::SlotDesc slot_descs[] = {
            {"col_int", type_int},
            {"col_map", type_map},
            {"col_struct", type_struct},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_map, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());

    EXPECT_EQ("[1, {'a':1}, {a:'a',b:1}]", chunk->debug_row(0));
    EXPECT_EQ("[2, {'b':2,'c':3}, {a:'b',b:2}]", chunk->debug_row(1));
    EXPECT_EQ("[3, {'c':3}, {a:'c',b:3}]", chunk->debug_row(2));
}

TEST_F(FileReaderTest, TestTwoNestedLevelArray) {
    // format:
    // id: INT, b: ARRAY<ARRAY<INT>>
    const std::string filepath = "./be/test/exec/test_data/parquet_data/two_level_nested_array.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor type_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

    TypeDescriptor type_array_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    type_array_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    type_array.children.emplace_back(type_array_array);

    Utils::SlotDesc slot_descs[] = {
            {"id", type_int},
            {"b", type_array},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());

    chunk->check_or_die();

    EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
    EXPECT_EQ("[2, [NULL]]", chunk->debug_row(1));
    EXPECT_EQ("[3, [NULL,NULL,NULL]]", chunk->debug_row(2));
    EXPECT_EQ("[4, [[1,2,3,4],NULL,[1,2,3,4]]]", chunk->debug_row(3));
    EXPECT_EQ("[5, [[1,2,3,4,5]]]", chunk->debug_row(4));

    size_t total_row_nums = 0;
    total_row_nums += chunk->num_rows();

    {
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            total_row_nums += chunk->num_rows();
        }
    }

    EXPECT_EQ(163840, total_row_nums);
}

TEST_F(FileReaderTest, TestReadMapNull) {
    auto file = _create_file(_file_map_null_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_map_null_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor type_map(LogicalType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    Utils::SlotDesc slot_descs[] = {
            {"uuid", type_int},
            {"c1", type_map},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_map, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());

    EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
    EXPECT_EQ("[2, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[3, NULL]", chunk->debug_row(2));
}

// Illegal parquet files, not support it anymore
TEST_F(FileReaderTest, TestReadArrayMap) {
    // optional group col_array_map (LIST) {
    //     repeated group array (MAP) {
    //         repeated group map (MAP_KEY_VALUE) {
    //             required binary key (UTF8);
    //             optional int32 value;
    //         }
    //     }
    // }

    auto file = _create_file(_file_array_map_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_array_map_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_string = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    TypeDescriptor type_map(LogicalType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY));
    type_map.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor type_array_map(LogicalType::TYPE_ARRAY);
    type_array_map.children.emplace_back(type_map);

    Utils::SlotDesc slot_descs[] = {
            {"uuid", type_string},
            {"col_array_map", type_array_map},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_array_map_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);

    // Illegal parquet files, not support it anymore
    ASSERT_FALSE(status.ok());

    //  ASSERT_TRUE(status.ok()) << status.get_error_msg();
    //  EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    //
    //  auto chunk = std::make_shared<Chunk>();
    //  chunk->append_column(ColumnHelper::create_column(type_string, true), chunk->num_columns());
    //  chunk->append_column(ColumnHelper::create_column(type_array_map, true), chunk->num_columns());
    //
    //  status = file_reader->get_next(&chunk);
    //  ASSERT_TRUE(status.ok());
    //  ASSERT_EQ(2, chunk->num_rows());
    //
    //  EXPECT_EQ("['0', [{'def':11,'abc':10},{'ghi':12},{'jkl':13}]]", chunk->debug_row(0));
    //  EXPECT_EQ("['1', [{'happy new year':11,'hello world':10},{'vary happy':12},{'ok':13}]]", chunk->debug_row(1));
}

TEST_F(FileReaderTest, TestStructArrayNull) {
    // message table {
    //  optional int32 id = 1;
    //  optional group col = 2 {
    //    optional int32 a = 3;
    //    optional group b (LIST) = 4 {
    //      repeated group list {
    //        optional group element = 5 {
    //          optional int32 c = 6;
    //          optional binary d (STRING) = 7;
    //        }
    //      }
    //    }
    //  }
    // }
    std::string filepath = "./be/test/exec/test_data/parquet_data/struct_array_null.parquet";

    // With config's vector chunk size
    {
        auto file = _create_file(filepath);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(filepath));

        // --------------init context---------------
        auto ctx = _create_scan_context();

        TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

        TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        type_struct.field_names.emplace_back("a");

        TypeDescriptor type_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

        TypeDescriptor type_array_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        type_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        type_array_struct.field_names.emplace_back("c");
        type_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY));
        type_array_struct.field_names.emplace_back("d");

        type_array.children.emplace_back(type_array_struct);

        type_struct.children.emplace_back(type_array);
        type_struct.field_names.emplace_back("b");

        Utils::SlotDesc slot_descs[] = {
                {"id", type_int},
                {"col", type_struct},
                {""},
        };

        ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
        EXPECT_EQ("[2, {a:2,b:NULL}]", chunk->debug_row(1));
        EXPECT_EQ("[3, {a:NULL,b:[NULL]}]", chunk->debug_row(2));
        EXPECT_EQ("[4, {a:4,b:[NULL]}]", chunk->debug_row(3));
        EXPECT_EQ("[5, {a:5,b:[NULL,{c:5,d:'hello'},{c:5,d:'world'},NULL,{c:5,d:NULL}]}]", chunk->debug_row(4));
        EXPECT_EQ("[6, {a:NULL,b:[{c:6,d:'hello'},NULL,{c:NULL,d:'world'}]}]", chunk->debug_row(5));
        EXPECT_EQ("[7, {a:7,b:[NULL,NULL,NULL,NULL]}]", chunk->debug_row(6));
        EXPECT_EQ("[8, {a:8,b:[{c:8,d:'hello'},{c:NULL,d:'danny'},{c:8,d:'world'}]}]", chunk->debug_row(7));

        size_t total_row_nums = 0;
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();

        {
            while (!status.is_end_of_file()) {
                chunk->reset();
                status = file_reader->get_next(&chunk);
                chunk->check_or_die();
                total_row_nums += chunk->num_rows();
            }
        }
        EXPECT_EQ(524288, total_row_nums);
    }

    // With 1024 chunk size
    {
        auto file = _create_file(filepath);
        auto file_reader = std::make_shared<FileReader>(1024, file.get(), std::filesystem::file_size(filepath));

        // --------------init context---------------
        auto ctx = _create_scan_context();

        TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

        TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        type_struct.field_names.emplace_back("a");

        TypeDescriptor type_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

        TypeDescriptor type_array_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        type_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        type_array_struct.field_names.emplace_back("c");
        type_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
        type_array_struct.field_names.emplace_back("d");

        type_array.children.emplace_back(type_array_struct);

        type_struct.children.emplace_back(type_array);
        type_struct.field_names.emplace_back("b");

        Utils::SlotDesc slot_descs[] = {
                {"id", type_int},
                {"col", type_struct},
                {""},
        };

        ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
        EXPECT_EQ("[2, {a:2,b:NULL}]", chunk->debug_row(1));
        EXPECT_EQ("[3, {a:NULL,b:[NULL]}]", chunk->debug_row(2));
        EXPECT_EQ("[4, {a:4,b:[NULL]}]", chunk->debug_row(3));
        EXPECT_EQ("[5, {a:5,b:[NULL,{c:5,d:'hello'},{c:5,d:'world'},NULL,{c:5,d:NULL}]}]", chunk->debug_row(4));
        EXPECT_EQ("[6, {a:NULL,b:[{c:6,d:'hello'},NULL,{c:NULL,d:'world'}]}]", chunk->debug_row(5));
        EXPECT_EQ("[7, {a:7,b:[NULL,NULL,NULL,NULL]}]", chunk->debug_row(6));
        EXPECT_EQ("[8, {a:8,b:[{c:8,d:'hello'},{c:NULL,d:'danny'},{c:8,d:'world'}]}]", chunk->debug_row(7));

        size_t total_row_nums = 0;
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();

        {
            while (!status.is_end_of_file()) {
                chunk->reset();
                status = file_reader->get_next(&chunk);
                chunk->check_or_die();
                total_row_nums += chunk->num_rows();
            }
        }
        EXPECT_EQ(524288, total_row_nums);
    }
}

TEST_F(FileReaderTest, TestComplexTypeNotNull) {
    // message table {
    //  optional int32 id = 1;
    //  optional group col = 2 {
    //    optional int32 a = 3;
    //    optional group b (LIST) = 4 {
    //      repeated group list {
    //        optional group element = 5 {
    //          optional int32 c = 6;
    //          optional binary d (STRING) = 7;
    //        }
    //      }
    //    }
    //  }
    // }

    std::string filepath = "./be/test/exec/test_data/parquet_data/complex_subfield_not_null.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_struct.field_names.emplace_back("a");

    TypeDescriptor type_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

    TypeDescriptor type_array_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_array_struct.field_names.emplace_back("c");
    type_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY));
    type_array_struct.field_names.emplace_back("d");

    type_array.children.emplace_back(type_array_struct);

    type_struct.children.emplace_back(type_array);
    type_struct.field_names.emplace_back("b");

    Utils::SlotDesc slot_descs[] = {
            {"id", type_int},
            {"col", type_struct},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ("[1, {a:1,b:[{c:1,d:'hello'}]}]", chunk->debug_row(0));
    EXPECT_EQ("[2, {a:2,b:[{c:2,d:'hello'},NULL,{c:2,d:NULL}]}]", chunk->debug_row(1));
    EXPECT_EQ("[3, {a:3,b:[NULL,NULL]}]", chunk->debug_row(2));
    EXPECT_EQ("[4, {a:4,b:[NULL,{c:4,d:NULL}]}]", chunk->debug_row(3));

    size_t total_row_nums = 0;
    chunk->check_or_die();
    total_row_nums += chunk->num_rows();

    {
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            total_row_nums += chunk->num_rows();
        }
    }

    EXPECT_EQ(262144, total_row_nums);
}

// Illegal parquet files, not support it anymore
TEST_F(FileReaderTest, TestHudiMORTwoNestedLevelArray) {
    // format:
    // b: varchar
    // c: ARRAY<ARRAY<INT>>
    const std::string filepath = "./be/test/exec/test_data/parquet_data/hudi_mor_two_level_nested_array.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_string = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    TypeDescriptor type_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    TypeDescriptor type_array_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    type_array_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    type_array.children.emplace_back(type_array_array);

    Utils::SlotDesc slot_descs[] = {
            {"b", type_string},
            {"c", type_array},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);

    // Illegal parquet files, not support it anymore
    ASSERT_FALSE(status.ok()) << status.get_error_msg();
    // ASSERT_TRUE(status.ok()) << status.get_error_msg();

    //  EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    //
    //  auto chunk = std::make_shared<Chunk>();
    //  chunk->append_column(ColumnHelper::create_column(type_string, true), chunk->num_columns());
    //  chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());
    //
    //  status = file_reader->get_next(&chunk);
    //  ASSERT_TRUE(status.ok());
    //
    //  chunk->check_or_die();
    //
    //  EXPECT_EQ("['hello', [[10,20,30],[40,50,60,70]]]", chunk->debug_row(0));
    //  EXPECT_EQ("[NULL, [[30,40],[10,20,30]]]", chunk->debug_row(1));
    //  EXPECT_EQ("['hello', NULL]", chunk->debug_row(2));
    //
    //  size_t total_row_nums = 0;
    //  total_row_nums += chunk->num_rows();
    //
    //  {
    //      while (!status.is_end_of_file()) {
    //          chunk->reset();
    //          status = file_reader->get_next(&chunk);
    //          chunk->check_or_die();
    //          total_row_nums += chunk->num_rows();
    //      }
    //  }
    //
    //  EXPECT_EQ(3, total_row_nums);
}

TEST_F(FileReaderTest, TestLateMaterializationAboutRequiredComplexType) {
    // Schema:
    //  message schema {
    //    required int64 a (INTEGER(64,true));
    //    required group b {
    //      required int64 b1 (INTEGER(64,true));
    //      required int64 b2 (INTEGER(64,true));
    //  }
    //    required group c (MAP) {
    //      repeated group key_value {
    //        required int64 key (INTEGER(64,true));
    //        required int64 value (INTEGER(64,true));
    //      }
    //    }
    //  }
    const std::string filepath = "./be/test/formats/parquet/test_data/map_struct_subfield_required.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_a = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor type_b = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_b.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_b.field_names.emplace_back("b1");
    type_b.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_b.field_names.emplace_back("b2");

    TypeDescriptor type_c = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    type_c.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_c.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    Utils::SlotDesc slot_descs[] = {
            {"a", type_a},
            {"b", type_b},
            {"c", type_c},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    _create_int_conjunct_ctxs(TExprOpcode::EQ, 0, 8000, &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 3);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_a, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_b, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_c, true), chunk->num_columns());

    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        if (!status.ok() && !status.is_end_of_file()) {
            std::cout << status.get_error_msg() << std::endl;
            break;
        }
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        if (chunk->num_rows() == 1) {
            EXPECT_EQ("[8000, {b1:8000,b2:8000}, {8000:8000}]", chunk->debug_row(0));
        }
    }

    EXPECT_EQ(1, total_row_nums);
}

TEST_F(FileReaderTest, TestLateMaterializationAboutOptionalComplexType) {
    // Schema:
    // message schema {
    //  optional int64 a (INTEGER(64,true));
    //  optional group b {
    //    optional int64 b1 (INTEGER(64,true));
    //    optional int64 b2 (INTEGER(64,true));
    //  }
    //  optional group c (MAP) {
    //    repeated group key_value {
    //      required int64 key (INTEGER(64,true));
    //      optional int64 value (INTEGER(64,true));
    //    }
    //  }
    // }
    const std::string filepath = "./be/test/formats/parquet/test_data/map_struct_subfield_optional.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_a = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    TypeDescriptor type_b = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_b.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_b.field_names.emplace_back("b1");
    type_b.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_b.field_names.emplace_back("b2");

    TypeDescriptor type_c = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    type_c.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    type_c.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    Utils::SlotDesc slot_descs[] = {
            {"a", type_a},
            {"b", type_b},
            {"c", type_c},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    _create_int_conjunct_ctxs(TExprOpcode::EQ, 0, 8000, &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 3);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_a, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_b, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_c, true), chunk->num_columns());

    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        if (chunk->num_rows() == 1) {
            EXPECT_EQ("[8000, {b1:8000,b2:8000}, {8000:8000}]", chunk->debug_row(0));
        }
    }

    EXPECT_EQ(1, total_row_nums);
}

TEST_F(FileReaderTest, CheckDictOutofBouds) {
    const std::string filepath = "./be/test/exec/test_data/parquet_scanner/type_mismatch_decode_min_max.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_vin = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_log_domain = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_file_name = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_is_collection = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    TypeDescriptor type_is_center = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    TypeDescriptor type_is_cloud = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    TypeDescriptor type_collection_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_center_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_cloud_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_error_collection_tips = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_error_center_tips = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_error_cloud_tips = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_error_collection_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_error_center_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_error_cloud_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_original_time = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
    TypeDescriptor type_is_original = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    Utils::SlotDesc slot_descs[] = {
            {"vin", type_vin},
            {"type_log_domain", type_log_domain},
            {"file_name", type_file_name},
            {"is_collection", type_is_collection},
            {"is_center", type_is_center},
            {"is_cloud", type_is_cloud},
            {"collection_time", type_collection_time},
            {"center_time", type_center_time},
            {"cloud_time", type_cloud_time},
            {"error_collection_tips", type_error_collection_tips},
            {"error_center_tips", type_error_center_tips},
            {"error_cloud_tips", type_error_cloud_tips},
            {"error_collection_time", type_error_collection_time},
            {"error_center_time", type_error_center_time},
            {"error_cloud_time", type_error_cloud_time},
            {"original_time", type_original_time},
            {"is_original", type_is_original},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_vin, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_log_domain, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_file_name, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_is_collection, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_is_center, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_is_cloud, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_collection_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_center_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_cloud_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_error_collection_tips, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_error_center_tips, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_error_cloud_tips, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_error_collection_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_error_center_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_error_cloud_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_original_time, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_is_original, true), chunk->num_columns());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        if (!status.ok()) {
            break;
        }
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(0, total_row_nums);
}

TEST_F(FileReaderTest, CheckLargeParquetHeader) {
    const std::string filepath = "./be/test/formats/parquet/test_data/large_page_header.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    TypeDescriptor type_string = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    Utils::SlotDesc slot_descs[] = {
            {"myString", type_string},
            {"myInteger", type_int},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_string, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        if (!status.ok()) {
            std::cout << status.get_error_msg() << std::endl;
            break;
        }
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(5, total_row_nums);
}

TEST_F(FileReaderTest, TestMinMaxForIcebergTable) {
    // Schema:
    // message table {
    //  required binary data (STRING) = 1;
    //  required group struct = 2 {
    //    optional binary x (STRING) = 4;
    //    optional binary y (STRING) = 5;
    //  }
    //  required int32 int = 3;
    // }
    const std::string filepath =
            "./be/test/formats/parquet/test_data/iceberg_schema_evolution/iceberg_string_map_string.parquet";
    auto file = _create_file(filepath);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(filepath));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_data{};
    field_data.__set_field_id(1);
    field_data.__set_name("data");

    TIcebergSchemaField field_struct{};
    field_struct.__set_field_id(2);
    field_struct.__set_name("struct");

    TIcebergSchemaField field_struct_a{};
    field_struct_a.__set_field_id(4);
    field_struct_a.__set_name("x");

    TIcebergSchemaField field_struct_b{};
    field_struct_b.__set_field_id(5);
    field_struct_b.__set_name("y");

    std::vector<TIcebergSchemaField> subfields{field_struct_a, field_struct_b};
    field_struct.__set_children(subfields);

    TIcebergSchemaField field_int{};
    field_int.__set_field_id(3);
    field_int.__set_name("int");

    std::vector<TIcebergSchemaField> fields{field_data, field_struct, field_int};
    schema.__set_fields(fields);
    ctx->iceberg_schema = &schema;

    TypeDescriptor type_data = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("x");
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("y");

    TypeDescriptor type_int = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    Utils::SlotDesc slot_descs[] = {
            {"data", type_data},
            {"struct", type_struct},
            {"int", type_int},
            {""},
    };

    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    Utils::SlotDesc min_max_slots[] = {
            {"int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 5, &ctx->min_max_conjunct_ctxs);
    _create_int_conjunct_ctxs(TExprOpcode::LE, 0, 5, &ctx->min_max_conjunct_ctxs);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type_data, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_int, true), chunk->num_columns());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        if (chunk->num_rows() == 1) {
            EXPECT_EQ("['hello', {x:'world',y:'danny'}, 5]", chunk->debug_row(0));
        }
    }

    EXPECT_EQ(1, total_row_nums);
}

TEST_F(FileReaderTest, TestRandomReadWith2PageSize) {
    std::random_device rd;
    std::mt19937 rng(rd());

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());

    // c0 = np.arange(1, 20001)
    // c1 = np.arange(20000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string small_page_file = "./be/test/formats/parquet/test_data/read_range_test.parquet";

    // c0 = np.arange(1, 100001)
    // c1 = np.arange(100000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string big_page_file = "./be/test/formats/parquet/test_data/read_range_big_page_test.parquet";

    // for small page 1000 values / page
    // for big page 10000 values / page
    for (size_t index = 0; index < 2; index++) {
        const std::string& file_path = index == 0 ? small_page_file : big_page_file;
        std::cout << "file_path: " << file_path << std::endl;

        std::uniform_int_distribution<int> dist_small(1, 20000);
        std::uniform_int_distribution<int> dist_big(1, 100000);

        std::set<int32_t> in_oprands;
        auto _print_in_predicate = [&]() {
            std::stringstream ss;
            ss << "predicate: in ( ";
            for (int32_t op : in_oprands) {
                ss << op << " ";
            }
            ss << ")";
            return ss.str();
        };
        std::vector<int32_t> in_oprand_sizes{5, 50};
        for (int32_t in_oprand_size : in_oprand_sizes) {
            // use 20 to save ci's time, change bigger to test more case
            for (int32_t i = 0; i < 20; i++) {
                in_oprands.clear();
                for (int32_t j = 0; j < in_oprand_size; j++) {
                    int32_t num = index == 0 ? dist_small(rng) : dist_big(rng);
                    in_oprands.emplace(num);
                }
                auto ctx = _create_file_random_read_context(file_path);
                auto file = _create_file(file_path);
                ctx->conjunct_ctxs_by_slot[0].clear();
                _create_in_predicate_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands,
                                                   &ctx->conjunct_ctxs_by_slot[0]);
                auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                                std::filesystem::file_size(file_path));

                Status status = file_reader->init(ctx);
                ASSERT_TRUE(status.ok());
                size_t total_row_nums = 0;
                while (!status.is_end_of_file()) {
                    chunk->reset();
                    status = file_reader->get_next(&chunk);
                    chunk->check_or_die();
                    total_row_nums += chunk->num_rows();
                    if (!status.ok() && !status.is_end_of_file()) {
                        std::cout << status.get_error_msg() << std::endl;
                        DCHECK(false) << "file path: " << file_path << ", " << _print_in_predicate();
                    }
                    // check row value
                    if (chunk->num_rows() > 0) {
                        ColumnPtr c0 = chunk->get_column_by_index(0);
                        ColumnPtr c1 = chunk->get_column_by_index(1);
                        ColumnPtr c2 = chunk->get_column_by_index(2);
                        ColumnPtr c3 = chunk->get_column_by_index(3);
                        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
                            int32_t c0_value = c0->get(row_index).get_int32();
                            bool flag = (in_oprands.find(c0_value) != in_oprands.end());
                            int32_t c1_value = c1->get(row_index).get_int32();
                            flag &= index == 0 ? c0_value + c1_value == 20001 : c0_value + c1_value == 100001;
                            if (c0_value % 10 == 0) {
                                flag &= c2->is_null(row_index);
                                flag &= c3->is_null(row_index);
                            } else {
                                flag &= (!c2->is_null(row_index));
                                flag &= (!c3->is_null(row_index));
                                if (!flag) {
                                    std::cout << "file path: " << file_path << ", " << _print_in_predicate();
                                }
                                EXPECT_TRUE(flag);
                                std::string expected_string = std::to_string(c0_value % 100);
                                Slice expected_value = Slice(expected_string);
                                Slice c2_value = c2->get(row_index).get_slice();
                                flag &= (c2_value == expected_value);
                                DatumArray c3_value = c3->get(row_index).get_array();
                                flag &= (c3_value.size() == 3) && (!c3_value[0].is_null()) &&
                                        (c3_value[0].get_int32() == (c0_value % 1000)) && (c3_value[1].is_null()) &&
                                        (!c3_value[2].is_null()) && (c3_value[2].get_int32() == (c1_value % 1000));
                            }
                            if (!flag) {
                                std::cout << "file path: " << file_path << ", " << _print_in_predicate();
                            }
                            EXPECT_TRUE(flag);
                        }
                    }
                }
                EXPECT_EQ(total_row_nums, in_oprands.size());
            }
        }
    }
}

TEST_F(FileReaderTest, TestStructSubfieldDictFilter) {
    /*
    c1 = np.arange(10000, 0, -1)
    data = {
        'c1': c1
    }
    data['c0'] = np.arange(1, 10001)
    data['c1'] = np.arange(10000, 0, -1)
    df = pd.DataFrame(data)
    df_dict = pd.DataFrame({
        "c_struct": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else {"c1": str(x["c1"] % 100), "c0": str(x["c0"] % 100)}, axis=1),
        "c0": df["c0"],
        "c1": df["c1"]
    })
    df_with_dict = pd.DataFrame({
        "c0": df_dict["c0"],
        "c1": df_dict["c1"],
        "c_struct" : df_dict["c_struct"],
        "c_struct_struct" : df_dict.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else {"c0" : str(x["c0"] % 100), "c_struct" : x["c_struct"]}, axis = 1)
    })
     */
    const std::string struct_in_struct_file_path =
            "./be/test/formats/parquet/test_data/test_parquet_struct_in_struct.parquet";
    auto ctx = _create_file_struct_in_struct_read_context(struct_in_struct_file_path);

    auto file = _create_file(struct_in_struct_file_path);

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("c0");

    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("c1");

    TypeDescriptor type_struct_in_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct_in_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct_in_struct.field_names.emplace_back("c0");

    type_struct_in_struct.children.emplace_back(type_struct);
    type_struct_in_struct.field_names.emplace_back("c_struct");

    std::vector<std::string> subfield_path({"c_struct", "c0"});

    _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::EQ, 3, type_struct_in_struct, subfield_path, "55",
                                                    &ctx->conjunct_ctxs_by_slot[3]);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(struct_in_struct_file_path));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct_in_struct, true), chunk->num_columns());

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
    ASSERT_EQ(3, file_reader->_row_group_readers[0]->_dict_column_indices[0]);
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_sub_field_paths.size());
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_sub_field_paths[3].size());
    ASSERT_EQ(subfield_path, file_reader->_row_group_readers[0]->_dict_column_sub_field_paths[3][0]);
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        if (chunk->num_rows() != 0) {
            ASSERT_EQ("{c0:'55',c_struct:{c0:'55',c1:'46'}}", chunk->get_column_by_slot_id(3)->debug_item(0));
        }
    }
    EXPECT_EQ(100, total_row_nums);
}

TEST_F(FileReaderTest, TestReadRoundByRound) {
    // c0 = np.arange(1, 100001)
    // c1 = np.arange(100000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string file_path = "./be/test/formats/parquet/test_data/read_range_big_page_test.parquet";
    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());

    auto ctx = _create_file_random_read_context(file_path);
    auto file = _create_file(file_path);
    // c0 >= 100
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 100, &ctx->conjunct_ctxs_by_slot[0]);
    // c1 <= 100
    _create_int_conjunct_ctxs(TExprOpcode::LE, 1, 100, &ctx->conjunct_ctxs_by_slot[1]);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(file_path));
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(100, total_row_nums);
    EXPECT_EQ(g_hdfs_scan_stats.group_min_round_cost, 1);
}

TEST_F(FileReaderTest, TestStructSubfieldNoDecodeNotOutput) {
    const std::string struct_in_struct_file_path =
            "./be/test/formats/parquet/test_data/test_parquet_struct_in_struct.parquet";
    auto ctx = _create_file_struct_in_struct_prune_and_no_output_read_context(struct_in_struct_file_path);

    auto file = _create_file(struct_in_struct_file_path);

    TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("c0");

    TypeDescriptor type_struct_in_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    type_struct_in_struct.children.emplace_back(type_struct);
    type_struct_in_struct.field_names.emplace_back("c_struct");

    std::vector<std::string> subfield_path({"c_struct", "c0"});

    _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::EQ, 1, type_struct_in_struct, subfield_path, "55",
                                                    &ctx->conjunct_ctxs_by_slot[1]);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(struct_in_struct_file_path));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct_in_struct, true), chunk->num_columns());

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices[0]);
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_sub_field_paths.size());
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_sub_field_paths[1].size());
    ASSERT_EQ(std::vector<std::string>({"c_struct", "c0"}),
              file_reader->_row_group_readers[0]->_dict_column_sub_field_paths[1][0]);
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        if (chunk->num_rows() != 0) {
            ASSERT_EQ("{c_struct:{c0:NULL}}", chunk->get_column_by_slot_id(1)->debug_item(0));
        }
    }
    EXPECT_EQ(100, total_row_nums);
}

} // namespace starrocks::parquet
