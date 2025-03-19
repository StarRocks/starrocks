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

#include "cache/block_cache/block_cache.h"
#include "cache/object_cache/starcache_module.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "exec/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/runtime_filter.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class FileReaderTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _rf_probe_collector = _pool.add(new RuntimeFilterProbeCollector());
    }
    void TearDown() override {}

protected:
    using Int32RF = ComposedRuntimeFilter<TYPE_INT>;

    StatusOr<RuntimeFilterProbeDescriptor*> gen_runtime_filter_desc(SlotId slot_id);

    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);
    DataCacheOptions _mock_datacache_options();

    std::shared_ptr<FileReader> _create_file_reader(const std::string& file_path, int64_t chunk_size = 4096);

    HdfsScannerContext* _create_scan_context();
    HdfsScannerContext* _create_scan_context(Utils::SlotDesc* slot_descs, const std::string& file_path,
                                             int64_t scan_length = 0);
    HdfsScannerContext* _create_scan_context(Utils::SlotDesc* slot_descs, Utils::SlotDesc* min_max_slot_descs,
                                             const std::string& file_path, int64_t scan_length = 0);

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
    HdfsScannerContext* _create_file_map_partial_materialize_context();

    StatusOr<HdfsScannerContext*> _create_context_for_in_filter(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_in_filter_normal(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_min_max_all_null_group(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_bool(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_smallint(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_int32(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_int64(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_datetime(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_string(SlotId slot_id);
    StatusOr<HdfsScannerContext*> _create_context_for_has_null_page_decimal(SlotId slot_id);

    StatusOr<ExprContext*> _create_in_const_pred(SlotId slot_id, const std::vector<int32_t>& values, bool has_null,
                                                 bool is_runtime_filter);

    StatusOr<HdfsScannerContext*> _create_context_for_filter_row_group_1(SlotId slot_id, int32_t start, int32_t end,
                                                                         bool has_null);

    StatusOr<HdfsScannerContext*> _create_context_for_filter_row_group_update_rf(SlotId slot_id);

    StatusOr<HdfsScannerContext*> _create_context_for_filter_page_index(SlotId slot_id, int32_t start, int32_t end,
                                                                        bool has_null);

    HdfsScannerContext* _create_file_random_read_context(const std::string& file_path, Utils::SlotDesc* slot_descs);

    HdfsScannerContext* _create_file_struct_in_struct_read_context(const std::string& file_path);

    HdfsScannerContext* _create_file_struct_in_struct_prune_and_no_output_read_context(const std::string& file_path);

    void _create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                   std::vector<ExprContext*>* conjunct_ctxs);
    void _create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                      std::vector<ExprContext*>* conjunct_ctxs);

    void _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                         const TypeDescriptor& type,
                                                         const std::vector<std::string>& subfiled_path,
                                                         const std::string& value,
                                                         std::vector<ExprContext*>* conjunct_ctxs);

    static ChunkPtr _create_chunk();
    static ChunkPtr _create_int_chunk();
    static ChunkPtr _create_multi_page_chunk();
    static ChunkPtr _create_struct_chunk();
    static ChunkPtr _create_required_array_chunk();
    static ChunkPtr _create_chunk_for_partition();
    static ChunkPtr _create_chunk_for_not_exist();
    static void _append_column_for_chunk(LogicalType column_type, ChunkPtr* chunk);

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0);

    // Description: A simple parquet file that all columns are null
    // one row group
    //
    // col1    col2    col3    col4
    // int    bigint  varchar  datetime
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

    // The length of binary type is greater than 4k, and there is no min max statistics
    std::string _file_no_min_max_stats_path = "./be/test/exec/test_data/parquet_scanner/no_min_max_statistics.parquet";

    // 2 row group, 3 row per row group
    //        +--------+--------+
    //        |   col1 |   col2 |
    //        |--------+--------|
    //        |      1 |     11 |
    //        |      2 |     22 |
    //        |      3 |     33 |
    //        |      4 |     44 |
    //        |      5 |     55 |
    //        |      6 |     66 |
    //        +--------+--------+
    std::string _filter_row_group_path_1 =
            "./be/test/formats/parquet/test_data/file_read_test_filter_row_group_1.parquet";

    // 2 row group, 3 rows per group
    //      +--------+--------+
    //      |   col1 |   col2 |
    //      |--------+--------|
    //      |    nan |     11 |
    //      |      2 |     22 |
    //      |      3 |     33 |
    //      |      4 |     44 |
    //      |      5 |     55 |
    //      |      6 |     66 |
    //      +--------+--------+
    std::string _filter_row_group_path_2 =
            "./be/test/formats/parquet/test_data/file_read_test_filter_row_group_2.parquet";
    // 3 row group, 3 rows per group
    //      +--------+--------+
    //      |   col1 |   col2 |
    //      |--------+--------|
    //      |      1 |     11 |
    //      |      2 |     22 |
    //      |      3 |     33 |
    //      |      4 |     44 |
    //      |      5 |     55 |
    //      |      6 |     66 |
    //      |      7 |     77 |
    //      |      8 |     88 |
    //      |      9 |     99 |
    //      +--------+--------+
    std::string _filter_row_group_path_3 =
            "./be/test/formats/parquet/test_data/file_read_test_filter_row_group_update_rf.parquet";

    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;

    const size_t _chunk_size = 4096;

    std::string _filter_page_index_with_rf_has_null =
            "./be/test/formats/parquet/test_data/filter_page_index_with_rf_has_null.parquet";

    // c1        c2      c3
    // (int32)  (int64) (int32, no group stats)
    // ======================
    // null      null     null
    // null      null     null
    // null      null     null
    // null      null     null
    // null      null     null
    // null      null     null
    std::string _all_null_parquet_file = "./be/test/formats/parquet/test_data/all_null.parquet";
    std::string _has_null_page_file = "./be/test/formats/parquet/test_data/has_null_page.parquet";

    RuntimeFilterProbeCollector* _rf_probe_collector;
    const TypeDescriptor TYPE_DECIMAL128_DESC = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9);
    const TypeDescriptor TYPE_INT_ARRAY_ARRAY_DESC = TypeDescriptor::create_array_type(TYPE_INT_ARRAY_DESC);
    const TypeDescriptor TYPE_INT_INT_MAP_DESC = TypeDescriptor::create_map_type(TYPE_INT_DESC, TYPE_INT_DESC);
    const TypeDescriptor TYPE_CHAR_INT_MAP_DESC = TypeDescriptor::create_map_type(TYPE_CHAR_DESC, TYPE_INT_DESC);
    const TypeDescriptor TYPE_VARCHAR_INT_MAP_DESC = TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_INT_DESC);
    const TypeDescriptor TYPE_VARBINARY_INT_MAP_DESC =
            TypeDescriptor::create_map_type(TYPE_VARBINARY_DESC, TYPE_INT_DESC);
    const TypeDescriptor TYPE_VARCHAR_INTARRAY_MAP_DESC =
            TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_INT_ARRAY_DESC);
    const TypeDescriptor TYPE_VARCHAR_UNKNOWN_MAP_DESC =
            TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_UNKNOWN_DESC);
    const TypeDescriptor TYPE_UNKNOWN_INTARRAY_MAP_DESC =
            TypeDescriptor::create_map_type(TYPE_UNKNOWN_DESC, TYPE_INT_ARRAY_DESC);
    const TypeDescriptor TYPE_VARCHAR_ARRAY_DESC = TypeDescriptor::create_array_type(TYPE_VARCHAR_DESC);
};

StatusOr<RuntimeFilterProbeDescriptor*> FileReaderTest::gen_runtime_filter_desc(SlotId slot_id) {
    TRuntimeFilterDescription tRuntimeFilterDescription;
    tRuntimeFilterDescription.__set_filter_id(1);
    tRuntimeFilterDescription.__set_has_remote_targets(false);
    tRuntimeFilterDescription.__set_build_plan_node_id(1);
    tRuntimeFilterDescription.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BORADCAST);
    tRuntimeFilterDescription.__set_filter_type(TRuntimeFilterBuildType::TOPN_FILTER);

    TExpr col_ref = ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(slot_id, true);
    tRuntimeFilterDescription.__isset.plan_node_id_to_target_expr = true;
    tRuntimeFilterDescription.plan_node_id_to_target_expr.emplace(1, col_ref);

    auto* runtime_filter_desc = _pool.add(new RuntimeFilterProbeDescriptor());
    RETURN_IF_ERROR(runtime_filter_desc->init(&_pool, tRuntimeFilterDescription, 1, _runtime_state));

    return runtime_filter_desc;
}

std::unique_ptr<RandomAccessFile> FileReaderTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

DataCacheOptions FileReaderTest::_mock_datacache_options() {
    return DataCacheOptions{.enable_datacache = true,
                            .enable_cache_select = false,
                            .enable_populate_datacache = true,
                            .enable_datacache_async_populate_mode = true,
                            .enable_datacache_io_adaptor = true,
                            .modification_time = 100000,
                            .datacache_evict_probability = 0,
                            .datacache_priority = 0,
                            .datacache_ttl_seconds = 0};
}

HdfsScannerContext* FileReaderTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;
    ctx->runtime_filter_collector = _rf_probe_collector;
    return ctx;
}

std::shared_ptr<FileReader> FileReaderTest::_create_file_reader(const std::string& file_path, int64_t chunk_size) {
    auto file = _create_file(file_path);
    auto* file_ptr = _pool.add(file.release());
    uint64_t file_size = std::filesystem::file_size(file_path);
    return std::make_shared<FileReader>(chunk_size, file_ptr, file_size, _mock_datacache_options());
}

HdfsScannerContext* FileReaderTest::_create_scan_context(Utils::SlotDesc* slot_descs, const std::string& file_path,
                                                         int64_t scan_length) {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;
    ctx->runtime_filter_collector = _rf_probe_collector;

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = _create_scan_range(file_path, scan_length);
    ctx->parquet_bloom_filter_enable = true;
    ctx->parquet_page_index_enable = true;
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_scan_context(Utils::SlotDesc* slot_descs,
                                                         Utils::SlotDesc* min_max_slot_descs,
                                                         const std::string& file_path, int64_t scan_length) {
    auto* ctx = _create_scan_context(slot_descs, file_path, scan_length);
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slot_descs);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file1_base_context() {
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_BIGINT_DESC},
            {"c3", TYPE_VARCHAR_DESC},
            {"c4", TYPE_DATETIME_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file1_path, 1024);
}

HdfsScannerContext* FileReaderTest::_create_context_for_partition() {
    Utils::SlotDesc slot_descs[] = {
            {"c5", TYPE_INT_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file1_path, 1024);

    auto column = ColumnHelper::create_const_column<LogicalType::TYPE_INT>(1, 1);
    ctx->partition_values.emplace_back(column);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_not_exist() {
    Utils::SlotDesc slot_descs[] = {
            {"c5", TYPE_INT_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file1_path, 1024);
}

HdfsScannerContext* FileReaderTest::_create_file2_base_context() {
    // tuple desc and conjuncts
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_BIGINT_DESC},
            {"c3", TYPE_VARCHAR_DESC},
            {"c4", TYPE_DATETIME_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file2_path, 850);
}

HdfsScannerContext* FileReaderTest::_create_context_for_min_max() {
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_BIGINT_DESC},
            {"c3", TYPE_VARCHAR_DESC},
            {"c4", TYPE_DATETIME_DESC},
            {""},
    };
    Utils::SlotDesc min_max_slots[] = {
            {"c1", TYPE_INT_DESC},
            {""},
    };

    auto* ctx = _create_scan_context(slot_descs, min_max_slots, _file2_path, 850);

    // create min max conjuncts
    // c1 >= 1
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 1, &ctx->min_max_conjunct_ctxs);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_filter_file() {
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},      {"c2", TYPE_BIGINT_DESC}, {"c3", TYPE_VARCHAR_DESC},
            {"c4", TYPE_DATETIME_DESC}, {"c5", TYPE_INT_DESC},    {""},
    };
    auto* ctx = _create_scan_context(slot_descs, _file2_path, 850);
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
    // tuple desc and conjuncts
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},      {"c2", TYPE_BIGINT_DESC},  {"c3", TYPE_VARCHAR_DESC},
            {"c4", TYPE_DATETIME_DESC}, {"c5", TYPE_VARCHAR_DESC}, {""},
    };
    return _create_scan_context(slot_descs, _file3_path);
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
    // tuple desc and conjuncts
    // struct columns are not supported now, so we skip reading them
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c3", TYPE_VARCHAR_DESC},
            {"B1", TYPE_VARCHAR_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file4_path);
}

HdfsScannerContext* FileReaderTest::_create_file5_base_context() {
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_INT_ARRAY_ARRAY_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file5_path);
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
    // tuple desc and conjuncts
    // struct columns are not supported now, so we skip reading them
    Utils::SlotDesc slot_descs[] = {
            {"col_int", TYPE_INT_DESC},
            {"col_array", TYPE_INT_ARRAY_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file6_path);
}

StatusOr<ExprContext*> FileReaderTest::_create_in_const_pred(SlotId slot_id, const std::vector<int32_t>& values,
                                                             bool has_null, bool is_runtime_filter) {
    ColumnRef* col_ref = _pool.add(new ColumnRef(TYPE_INT_DESC, slot_id));
    VectorizedInConstPredicateBuilder builder(_runtime_state, &_pool, col_ref);
    RETURN_IF_ERROR(builder.create());

    ExprContext* expr_ctx = builder.get_in_const_predicate();
    RETURN_IF_ERROR(expr_ctx->prepare(_runtime_state));

    auto* in_pred = reinterpret_cast<VectorizedInConstPredicate<TYPE_INT>*>(expr_ctx->root());
    for (auto& v : values) {
        in_pred->insert(v);
    }
    if (has_null) {
        in_pred->insert_null();
    }
    in_pred->set_is_join_runtime_filter(is_runtime_filter);
    RETURN_IF_ERROR(expr_ctx->open(_runtime_state));
    return expr_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_in_filter(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC, 1}, {"c2", TYPE_BIGINT_DESC, 2}, {"c3", TYPE_VARCHAR_DESC, 3}, {""}};

    std::vector<int32_t> values{1, 3, 5};
    ASSIGN_OR_RETURN(auto* expr_ctx, _create_in_const_pred(slot_id, values, true, false));

    std::vector<ExprContext*> expr_ctxs{expr_ctx};
    auto scan_ctx = _create_scan_context(slot_descs, _all_null_parquet_file);
    scan_ctx->conjunct_ctxs_by_slot.insert({slot_id, expr_ctxs});
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->conjunct_ctxs_by_slot[slot_id], nullptr, tuple_desc,
                                           _runtime_state, scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_in_filter_normal(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"col1", TYPE_INT_DESC, 1}, {"col2", TYPE_INT_DESC, 2}, {"col3", TYPE_INT_DESC, 3},
                                    {"col4", TYPE_INT_DESC, 4}, {"col5", TYPE_INT_DESC, 5}, {""}};

    std::vector<int32_t> values{5, 6};
    ASSIGN_OR_RETURN(auto* expr_ctx, _create_in_const_pred(slot_id, values, false, false));

    std::vector<ExprContext*> expr_ctxs{expr_ctx};
    auto scan_ctx = _create_scan_context(slot_descs, _filter_row_group_path_1);
    scan_ctx->conjunct_ctxs_by_slot.insert({slot_id, expr_ctxs});
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->conjunct_ctxs_by_slot[slot_id], nullptr, tuple_desc,
                                           _runtime_state, scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_min_max_all_null_group(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c1", TYPE_INT_DESC}, {"c2", TYPE_BIGINT_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::GE, slot_id, 4, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _all_null_parquet_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_bool(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_bool", TYPE_BOOLEAN_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::GE, slot_id, false, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_smallint(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_smallint", TYPE_SMALLINT_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_smallint_conjunct(TExprOpcode::GT, slot_id, 3, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_int32(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_int32", TYPE_INT_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, slot_id, 33, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_int64(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_int64", TYPE_BIGINT_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_bigint_conjunct(TExprOpcode::GT, slot_id, 333, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_string(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_string", TYPE_VARCHAR_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_string_conjunct(TExprOpcode::GT, slot_id, "33333", &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_decimal(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_decimal", TYPE_DECIMAL128_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_decimal_conjunct(TExprOpcode::GT, slot_id, "333.300000000", &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_has_null_page_datetime(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"c_datetime", TYPE_DATETIME_DESC}, {""}};

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_datetime_conjunct(TExprOpcode::GT, slot_id, "2024-01-10 00:00:00", &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto scan_ctx = _create_scan_context(slot_descs, slot_descs, _has_null_page_file);
    scan_ctx->min_max_conjunct_ctxs.insert(scan_ctx->min_max_conjunct_ctxs.end(), expr_ctxs.begin(), expr_ctxs.end());
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(scan_ctx->min_max_conjunct_ctxs, nullptr, tuple_desc, _runtime_state,
                                           scan_ctx);

    return scan_ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_filter_row_group_update_rf(SlotId slot_id) {
    Utils::SlotDesc slot_descs[] = {{"col1", TYPE_INT_DESC, 0}, {"col2", TYPE_INT_DESC, 1}, {""}};
    auto ctx = _create_scan_context(slot_descs, _filter_row_group_path_3);
    ASSIGN_OR_RETURN(auto* rf_desc, gen_runtime_filter_desc(slot_id));

    _rf_probe_collector->add_descriptor(rf_desc);

    auto* pred_parser = _pool.add(new ConnectorPredicateParser(&ctx->slot_descs));
    auto* rf_list = _pool.add(new UnarrivedRuntimeFilterList());
    rf_list->driver_sequence = 1;
    rf_list->unarrived_runtime_filters.emplace_back(rf_desc);
    rf_list->slot_descs.emplace_back(ctx->slot_descs[0]);
    ctx->rf_scan_range_pruner = _pool.add(new RuntimeScanRangePruner(pred_parser, *rf_list));

    return ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_filter_row_group_1(SlotId slot_id, int32_t start,
                                                                                     int32_t end, bool has_null) {
    Utils::SlotDesc slot_descs[] = {{"col1", TYPE_INT_DESC, 1}, {"col2", TYPE_INT_DESC, 2}, {"col3", TYPE_INT_DESC, 3},
                                    {"col4", TYPE_INT_DESC, 4}, {"col5", TYPE_INT_DESC, 5}, {""}};
    auto ctx = _create_scan_context(slot_descs, _filter_row_group_path_1);

    auto* rf = _pool.add(new Int32RF());

    ASSIGN_OR_RETURN(auto* rf_desc, gen_runtime_filter_desc(slot_id));

    rf->get_bloom_filter()->init(10);
    rf->insert(start);
    rf->insert(end);
    if (has_null) {
        rf->insert_null();
    }

    rf_desc->set_runtime_filter(rf);
    _rf_probe_collector->add_descriptor(rf_desc);

    ColumnPtr partition_col3 = ColumnHelper::create_const_column<TYPE_INT>(5, 1);
    ColumnPtr partition_col4 = ColumnHelper::create_const_column<TYPE_INT>(2, 1);
    ColumnPtr partition_col5 = ColumnHelper::create_const_null_column(1);
    ctx->partition_columns.emplace_back(HdfsScannerContext::ColumnInfo{3, ctx->slot_descs[2], false});
    ctx->partition_columns.emplace_back(HdfsScannerContext::ColumnInfo{4, ctx->slot_descs[3], false});
    ctx->partition_columns.emplace_back(HdfsScannerContext::ColumnInfo{5, ctx->slot_descs[4], false});
    ctx->partition_values.emplace_back(partition_col3);
    ctx->partition_values.emplace_back(partition_col4);
    ctx->partition_values.emplace_back(partition_col5);

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[slot_id], _rf_probe_collector, tuple_desc,
                                           _runtime_state, ctx);

    return ctx;
}

StatusOr<HdfsScannerContext*> FileReaderTest::_create_context_for_filter_page_index(SlotId slot_id, int32_t start,
                                                                                    int32_t end, bool has_null) {
    Utils::SlotDesc slot_descs[] = {{"lo_orderkey", TYPE_INT_DESC, 1}, {"col2", TYPE_INT_DESC, 2},
                                    {"col3", TYPE_INT_DESC, 3},        {"col4", TYPE_INT_DESC, 4},
                                    {"col5", TYPE_INT_DESC, 5},        {""}};

    auto* rf = _pool.add(new ComposedRuntimeFilter<TYPE_INT>());
    ASSIGN_OR_RETURN(auto* rf_desc, gen_runtime_filter_desc(slot_id));

    rf->bloom_filter().init(10);
    rf->insert(start);
    rf->insert(end);
    if (has_null) {
        rf->insert_null();
    }

    rf_desc->set_runtime_filter(rf);
    _rf_probe_collector->add_descriptor(rf_desc);

    Expr* min_max_predicate = nullptr;
    RuntimeFilterHelper::create_min_max_value_predicate(&_pool, slot_id, TYPE_INT, rf, &min_max_predicate);
    ExprContext* expr_ctx = _pool.add(new ExprContext(min_max_predicate));
    RETURN_IF_ERROR(expr_ctx->prepare(_runtime_state));
    RETURN_IF_ERROR(expr_ctx->open(_runtime_state));
    std::vector<ExprContext*> expr_ctxs{expr_ctx};
    auto ctx = _create_scan_context(slot_descs, _filter_row_group_path_1);
    ctx->conjunct_ctxs_by_slot.insert({slot_id, expr_ctxs});

    ColumnPtr partition_col3 = ColumnHelper::create_const_column<TYPE_INT>(5, 1);
    ColumnPtr partition_col4 = ColumnHelper::create_const_column<TYPE_INT>(2, 1);
    ColumnPtr partition_col5 = ColumnHelper::create_const_null_column(1);
    ctx->partition_columns.emplace_back(HdfsScannerContext::ColumnInfo{3, ctx->slot_descs[2], false});
    ctx->partition_columns.emplace_back(HdfsScannerContext::ColumnInfo{4, ctx->slot_descs[3], false});
    ctx->partition_columns.emplace_back(HdfsScannerContext::ColumnInfo{5, ctx->slot_descs[4], false});
    ctx->partition_values.emplace_back(partition_col3);
    ctx->partition_values.emplace_back(partition_col4);
    ctx->partition_values.emplace_back(partition_col5);

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[slot_id], _rf_probe_collector, tuple_desc,
                                           _runtime_state, ctx);

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_map_char_key_context() {
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_CHAR_INT_MAP_DESC},
            {"c3", TYPE_VARCHAR_INT_MAP_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file_map_char_key_path);
}

HdfsScannerContext* FileReaderTest::_create_file_map_base_context() {
    const TypeDescriptor type_map_map = TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_VARCHAR_INT_MAP_DESC);
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_VARCHAR_INT_MAP_DESC},
            {"c3", type_map_map},
            {"c4", TYPE_VARCHAR_INTARRAY_MAP_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file_map_path);
}

HdfsScannerContext* FileReaderTest::_create_file_map_partial_materialize_context() {
    TypeDescriptor type_map_map = TypeDescriptor::create_map_type(TYPE_UNKNOWN_DESC, TYPE_VARCHAR_UNKNOWN_MAP_DESC);

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_VARCHAR_UNKNOWN_MAP_DESC},
            {"c3", type_map_map},
            {"c4", TYPE_UNKNOWN_INTARRAY_MAP_DESC},
            {""},
    };
    return _create_scan_context(slot_descs, _file_map_path);
}

HdfsScannerContext* FileReaderTest::_create_file_random_read_context(const std::string& file_path,
                                                                     Utils::SlotDesc* slot_descs) {
    return _create_scan_context(slot_descs, file_path);
}

HdfsScannerContext* FileReaderTest::_create_file_struct_in_struct_read_context(const std::string& file_path) {
    TypeDescriptor type_struct =
            TypeDescriptor::create_struct_type({"c0", "c1"}, {TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC});
    TypeDescriptor type_struct_in_struct =
            TypeDescriptor::create_struct_type({"c0", "c_struct"}, {TYPE_VARCHAR_DESC, type_struct});

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC},
            {"c1", TYPE_INT_DESC},
            {"c_struct", type_struct},
            {"c_struct_struct", type_struct_in_struct},
            {""},
    };
    return _create_scan_context(slot_descs, file_path);
}

HdfsScannerContext* FileReaderTest::_create_file_struct_in_struct_prune_and_no_output_read_context(
        const std::string& file_path) {
    TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"c0"}, {TYPE_VARCHAR_DESC});
    TypeDescriptor type_struct_in_struct = TypeDescriptor::create_struct_type({"c_struct"}, {type_struct});

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC},
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
    auto ctx = _create_scan_context(slot_descs, file_path);
    ctx->materialized_columns[1].decode_needed = false;

    return ctx;
}

void FileReaderTest::_create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                               std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(opcode, slot_id, value, &t_conjuncts);

    ASSERT_OK(Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, _runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, _runtime_state));
}

void FileReaderTest::_create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                                  std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0 = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, opcode);
    TExprNode node1 = ExprsTestHelper::create_slot_expr_node_t<TYPE_VARCHAR>(0, slot_id, true);
    TExprNode node2 = ExprsTestHelper::create_literal<TYPE_VARCHAR, std::string>(value, false);

    nodes.emplace_back(node0);
    nodes.emplace_back(node1);
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

    TExprNode node0 = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, opcode);
    node0.__set_is_monotonic(true);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SUBFIELD_EXPR;
    node1.is_nullable = true;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 1;
    node1.used_subfield_names = subfiled_path;
    node1.__isset.used_subfield_names = true;
    node1.__set_is_monotonic(true);
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
    node2.__set_is_monotonic(true);
    nodes.emplace_back(node2);

    TExprNode node3;
    node3.node_type = TExprNodeType::STRING_LITERAL;
    node3.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node3.num_children = 0;
    TStringLiteral string_literal;
    string_literal.value = value;
    node3.__set_string_literal(string_literal);
    node3.is_nullable = false;
    node3.__set_is_monotonic(true);
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
    (*chunk)->append_column(std::move(c), (*chunk)->num_columns());
}

ChunkPtr FileReaderTest::_create_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_BIGINT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_DATETIME, &chunk);
    return chunk;
}

ChunkPtr FileReaderTest::_create_int_chunk() {
    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
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

    auto c = ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, true);
    chunk->append_column(std::move(c), chunk->num_columns());
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
    auto file_reader = _create_file_reader(_file1_path);

    // init
    auto* ctx = _create_file1_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
}

TEST_F(FileReaderTest, TestGetNext) {
    auto file_reader = _create_file_reader(_file1_path);

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
    roaring64_bitmap_t* bitmap = roaring64_bitmap_create();
    roaring64_bitmap_add(bitmap, 1);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(bitmap);
    auto file = _create_file(_file1_path);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(_file1_path),
                                         _mock_datacache_options(), nullptr, skip_rows_ctx);

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
    auto file_reader = _create_file_reader(_file1_path);
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
    auto file_reader = _create_file_reader(_file1_path);
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
    auto file_reader = _create_file_reader(_file2_path);
    // init
    auto* ctx = _create_context_for_min_max();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // get next
    auto chunk = _create_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(11, chunk->num_rows());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestFilterFile) {
    auto file_reader = _create_file_reader(_file2_path);
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
    std::shared_ptr<io::SeekableInputStream> input_stream = file->stream();
    std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream =
            std::make_shared<io::SharedBufferedInputStream>(input_stream, file->filename(),
                                                            std::filesystem::file_size(_file2_path));

    auto wrap_file = std::make_unique<RandomAccessFile>(shared_buffered_input_stream, file->filename());

    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, wrap_file.get(),
                                                    std::filesystem::file_size(_file2_path), _mock_datacache_options(),
                                                    shared_buffered_input_stream.get());
    // init
    auto* ctx = _create_context_for_dict_filter();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    {
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_column_indices[0];
        ASSERT_EQ(2, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id());
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
    ASSERT_EQ(1, shared_buffered_input_stream->direct_io_count());
    ASSERT_EQ(1, shared_buffered_input_stream->shared_io_count());
}

TEST_F(FileReaderTest, TestGetNextOtherFilter) {
    auto file_reader = _create_file_reader(_file2_path);
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

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestSkipRowGroup) {
    auto file_reader = _create_file_reader(_file2_path);
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
    auto file_reader = _create_file_reader(_file3_path);
    // c3 = "c", c1 >= 4
    auto* ctx = _create_context_for_multi_filter();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    {
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_column_indices[0];
        ASSERT_EQ(2, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id());
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
    auto file_reader = _create_file_reader(_file3_path);

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
    auto file_reader = _create_file_reader(_file4_path);

    // init
    auto* ctx = _create_context_for_struct_column();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    {
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_column_indices[0];
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id());
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
    auto file_reader = _create_file_reader(_file4_path);

    // init
    auto* ctx = _create_context_for_upper_pred();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

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

TEST_F(FileReaderTest, TestReadArray2dColumn) {
    auto file_reader = _create_file_reader(_file5_path);

    //init
    auto* ctx = _create_file5_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 5);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(TYPE_INT_ARRAY_ARRAY_DESC, true);
    chunk->append_column(std::move(c), chunk->num_columns());
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 5);
    EXPECT_EQ(chunk->debug_row(0), "[1, [[1,2]]]");
    EXPECT_EQ(chunk->debug_row(1), "[2, [[1,2],[3,4]]]");
    EXPECT_EQ(chunk->debug_row(2), "[3, [[1,2,3],[4]]]");
    EXPECT_EQ(chunk->debug_row(3), "[4, [[1,2,3],[4],[5]]]");
    EXPECT_EQ(chunk->debug_row(4), "[5, [[1,2,3],[4,5]]]");
}

TEST_F(FileReaderTest, TestReadRequiredArrayColumns) {
    auto file_reader = _create_file_reader(_file6_path);

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
    auto file_reader = _create_file_reader(_file_map_char_key_path);

    //init
    auto* ctx = _create_file_map_char_key_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok()) << status.message();

    EXPECT_EQ(file_reader->row_group_size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c2.value, c3.key, c3.value
    EXPECT_EQ(ranges.size(), 5);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 1);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(TYPE_CHAR_INT_MAP_DESC, true);
    chunk->append_column(std::move(c), chunk->num_columns());
    auto c_map1 = ColumnHelper::create_column(TYPE_VARCHAR_INT_MAP_DESC, true);
    chunk->append_column(std::move(c_map1), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok()) << status.message();
    EXPECT_EQ(chunk->num_rows(), 1);
    EXPECT_EQ(chunk->debug_row(0), "[0, {'abc':123}, {'def':456}]");
}

TEST_F(FileReaderTest, TestReadMapColumn) {
    auto file_reader = _create_file_reader(_file_map_path);

    //init
    auto* ctx = _create_file_map_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c2.value, c3.key, c3.value.key, c3.value.value, c4.key. c4.value
    EXPECT_EQ(ranges.size(), 8);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 8);

    TypeDescriptor type_map_map = TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_VARCHAR_INT_MAP_DESC);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(TYPE_VARCHAR_INT_MAP_DESC, true);
    chunk->append_column(std::move(c), chunk->num_columns());
    auto c_map_map = ColumnHelper::create_column(type_map_map, true);
    chunk->append_column(std::move(c_map_map), chunk->num_columns());
    auto c_map_array = ColumnHelper::create_column(TYPE_VARCHAR_INTARRAY_MAP_DESC, true);
    chunk->append_column(std::move(c_map_array), chunk->num_columns());

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
    auto file_reader = _create_file_reader(_file4_path);

    // --------------init context---------------
    TypeDescriptor c2 = TypeDescriptor::create_struct_type({"f2", "f1", "f3"},
                                                           {TYPE_VARCHAR_DESC, TYPE_INT_DESC, TYPE_INT_ARRAY_DESC});
    TypeDescriptor c4_struct = TypeDescriptor::create_struct_type({"e1", "e2"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    TypeDescriptor c4 = TypeDescriptor::create_array_type(c4_struct);

    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC}, {"c2", c2}, {"c3", TYPE_VARCHAR_DESC}, {"c4", c4}, {"B1", TYPE_VARCHAR_DESC}, {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file4_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.f1, c2.f2, c2.f3, c3, c4.e1, c4.e2, B1
    EXPECT_EQ(ranges.size(), 8);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c4, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());

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
}

TEST_F(FileReaderTest, TestReadStructSubField) {
    auto file_reader = _create_file_reader(_file4_path);

    // --------------init context---------------
    TypeDescriptor c2 = TypeDescriptor::create_struct_type({"f1", "f3"}, {TYPE_INT_DESC, TYPE_INT_ARRAY_DESC});
    TypeDescriptor c4_struct = TypeDescriptor::create_struct_type({"e2"}, {TYPE_VARCHAR_DESC});
    TypeDescriptor c4 = TypeDescriptor::create_array_type(c4_struct);

    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC}, {"c2", c2}, {"c3", TYPE_VARCHAR_DESC}, {"c4", c4}, {"B1", TYPE_VARCHAR_DESC}, {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file4_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.f1, c2.f3, c3, c4.e2, B1
    EXPECT_EQ(ranges.size(), 6);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c4, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());

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
    auto file_reader = _create_file_reader(_file4_path);

    // --------------init context---------------
    TypeDescriptor c2 = TypeDescriptor::create_struct_type(
            {"f1", "f2", "f3", "not_existed"},
            {TYPE_INT_DESC, TYPE_VARCHAR_DESC, TYPE_INT_ARRAY_DESC, TYPE_VARCHAR_DESC});
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", c2},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file4_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024, chunk->num_rows());

    EXPECT_EQ("[0, {f1:0,f2:'a',f3:[0,1,2],not_existed:NULL}]", chunk->debug_row(0));
    EXPECT_EQ("[1, {f1:1,f2:'a',f3:[1,2,3],not_existed:NULL}]", chunk->debug_row(1));
    EXPECT_EQ("[2, {f1:2,f2:'a',f3:[2,3,4],not_existed:NULL}]", chunk->debug_row(2));
    EXPECT_EQ("[3, {f1:3,f2:'c',f3:[3,4,5],not_existed:NULL}]", chunk->debug_row(3));
    EXPECT_EQ("[4, {f1:4,f2:'c',f3:[4,5,6],not_existed:NULL}]", chunk->debug_row(4));
}

TEST_F(FileReaderTest, TestReadStructCaseSensitive) {
    auto file_reader = _create_file_reader(_file4_path);

    // --------------init context---------------
    TypeDescriptor c2 = TypeDescriptor::create_struct_type({"F1", "F2", "F3"},
                                                           {TYPE_INT_DESC, TYPE_VARCHAR_DESC, TYPE_INT_ARRAY_DESC});

    Utils::SlotDesc slot_descs[] = {{"c1", TYPE_INT_DESC}, {"c2", c2}, {""}};
    auto ctx = _create_scan_context(slot_descs, _file4_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024, chunk->num_rows());

    EXPECT_EQ("[0, {F1:0,F2:'a',F3:[0,1,2]}]", chunk->debug_row(0));
}

TEST_F(FileReaderTest, TestReadStructCaseSensitiveError) {
    auto file_reader = _create_file_reader(_file4_path);

    // --------------init context---------------
    TypeDescriptor c2 = TypeDescriptor::create_struct_type({"F1", "F2", "F3"},
                                                           {TYPE_INT_DESC, TYPE_VARCHAR_DESC, TYPE_INT_ARRAY_DESC});

    Utils::SlotDesc slot_descs[] = {{"c1", TYPE_INT_DESC}, {"c2", c2}, {""}};
    auto ctx = _create_scan_context(slot_descs, _file4_path);
    ctx->case_sensitive = true;
    // --------------finish init context---------------

    ASSERT_OK(file_reader->init(ctx));
    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    ASSERT_OK(file_reader->get_next(&chunk));
    ASSERT_EQ(1024, chunk->num_rows());
    EXPECT_EQ("[0, NULL]", chunk->debug_row(0));
}

TEST_F(FileReaderTest, TestReadStructNull) {
    auto file_reader = _create_file_reader(_file_struct_null_path);

    // --------------init context---------------
    TypeDescriptor c1 = TypeDescriptor::create_struct_type({"c1_0", "c1_1"}, {TYPE_INT_DESC, TYPE_INT_ARRAY_DESC});
    TypeDescriptor c2_struct = TypeDescriptor::create_struct_type({"c2_0", "c2_1"}, {TYPE_INT_DESC, TYPE_INT_DESC});
    TypeDescriptor c2 = TypeDescriptor::create_array_type(c2_struct);

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC}, {"c1", c1}, {"c2", c2}, {""}};

    auto ctx = _create_scan_context(slot_descs, _file_struct_null_path);
    // --------------finish init context---------------

    ASSERT_OK(file_reader->init(ctx));
    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());

    ASSERT_OK(file_reader->get_next(&chunk));
    ASSERT_EQ(4, chunk->num_rows());

    EXPECT_EQ("[1, {c1_0:1,c1_1:[1,2,3]}, [{c2_0:1,c2_1:1},{c2_0:2,c2_1:2},{c2_0:3,c2_1:3}]]", chunk->debug_row(0));
    EXPECT_EQ("[2, {c1_0:NULL,c1_1:[2,3,4]}, [{c2_0:NULL,c2_1:2},{c2_0:NULL,c2_1:3}]]", chunk->debug_row(1));
    EXPECT_EQ("[3, {c1_0:3,c1_1:[NULL]}, [{c2_0:NULL,c2_1:NULL},{c2_0:NULL,c2_1:NULL},{c2_0:NULL,c2_1:NULL}]]",
              chunk->debug_row(2));
    EXPECT_EQ("[4, {c1_0:4,c1_1:[4,5,6]}, [{c2_0:4,c2_1:NULL},{c2_0:5,c2_1:NULL},{c2_0:6,c2_1:4}]]",
              chunk->debug_row(3));
}

TEST_F(FileReaderTest, TestReadBinary) {
    auto file_reader = _create_file_reader(_file_binary_path);

    // --------------init context---------------
    Utils::SlotDesc slot_descs[] = {{"k1", TYPE_INT_DESC}, {"k2", TYPE_VARBINARY_DESC}, {""}};

    auto ctx = _create_scan_context(slot_descs, _file_binary_path);
    // --------------finish init context---------------

    ASSERT_OK(file_reader->init(ctx));
    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARBINARY_DESC, true), chunk->num_columns());

    ASSERT_OK(file_reader->get_next(&chunk));
    ASSERT_EQ(1, chunk->num_rows());

    std::string s = chunk->debug_row(0);
    EXPECT_EQ("[6, '\017']", chunk->debug_row(0));
}

TEST_F(FileReaderTest, TestReadMapColumnWithPartialMaterialize) {
    auto file_reader = _create_file_reader(_file_map_path);

    //init
    auto* ctx = _create_file_map_partial_materialize_context();

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c3.value.key, c4.value
    EXPECT_EQ(ranges.size(), 4);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 8);

    TypeDescriptor type_map_map = TypeDescriptor::create_map_type(TYPE_UNKNOWN_DESC, TYPE_VARCHAR_UNKNOWN_MAP_DESC);
    TypeDescriptor type_map_array = TypeDescriptor::create_map_type(TYPE_UNKNOWN_DESC, TYPE_INT_ARRAY_DESC);

    ChunkPtr chunk = std::make_shared<Chunk>();
    _append_column_for_chunk(LogicalType::TYPE_INT, &chunk);
    auto c = ColumnHelper::create_column(TYPE_VARCHAR_UNKNOWN_MAP_DESC, true);
    chunk->append_column(std::move(c), chunk->num_columns());
    auto c_map_map = ColumnHelper::create_column(type_map_map, true);
    chunk->append_column(std::move(c_map_map), chunk->num_columns());
    auto c_map_array = ColumnHelper::create_column(type_map_array, true);
    chunk->append_column(std::move(c_map_array), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 8);
    EXPECT_EQ(chunk->debug_row(0), "[1, {'k1':NULL,'k2':NULL}, {NULL:{'f1':NULL,'f2':NULL}}, {NULL:[1,2]}]");
    EXPECT_EQ(chunk->debug_row(1),
              "[2, {'k1':NULL,'k3':NULL,'k4':NULL}, {NULL:{'f1':NULL,'f2':NULL},NULL:{'f1':NULL,'f2':NULL}}, "
              "{NULL:[1],NULL:[2]}]");
    EXPECT_EQ(chunk->debug_row(2),
              "[3, {'k2':NULL,'k3':NULL,'k5':NULL}, {NULL:{'f1':NULL,'f2':NULL,'f3':NULL}}, {NULL:[1,2,3]}]");
    EXPECT_EQ(chunk->debug_row(3), "[4, {'k1':NULL,'k2':NULL,'k3':NULL}, {NULL:{'f2':NULL}}, {NULL:[1]}]");
    EXPECT_EQ(chunk->debug_row(4), "[5, {'k3':NULL}, {NULL:{'f2':NULL}}, {NULL:[NULL]}]");
    EXPECT_EQ(chunk->debug_row(5), "[6, {'k1':NULL}, {NULL:{'f2':NULL}}, {NULL:[1]}]");
    EXPECT_EQ(chunk->debug_row(6), "[7, {'k1':NULL,'k2':NULL}, {NULL:{'f2':NULL}}, {NULL:[1,2,3]}]");
    EXPECT_EQ(chunk->debug_row(7), "[8, {'k3':NULL}, {NULL:{'f1':NULL,'f2':NULL,'f3':NULL}}, {NULL:[1],NULL:[2]}]");
}

TEST_F(FileReaderTest, TestReadNotNull) {
    auto file_reader = _create_file_reader(_file_col_not_null_path);

    // --------------init context---------------
    TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_VARCHAR_DESC, TYPE_INT_DESC});
    Utils::SlotDesc slot_descs[] = {
            {"col_int", TYPE_INT_DESC},
            {"col_map", TYPE_VARCHAR_INT_MAP_DESC},
            {"col_struct", type_struct},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file_col_not_null_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_INT_MAP_DESC, true), chunk->num_columns());
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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    TypeDescriptor type_array = TypeDescriptor::create_array_type(TYPE_INT_ARRAY_DESC);
    Utils::SlotDesc slot_descs[] = {
            {"id", TYPE_INT_DESC},
            {"b", type_array},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, filepath);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
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
    auto file_reader = _create_file_reader(_file_map_null_path);

    // --------------init context---------------
    Utils::SlotDesc slot_descs[] = {
            {"uuid", TYPE_INT_DESC},
            {"c1", TYPE_VARCHAR_INT_MAP_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file_map_null_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_INT_MAP_DESC, true), chunk->num_columns());

    ASSERT_OK(file_reader->get_next(&chunk));
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

    auto file_reader = _create_file_reader(_file_array_map_path);

    // --------------init context---------------
    TypeDescriptor type_array_map = TypeDescriptor::create_array_type(TYPE_VARBINARY_INT_MAP_DESC);
    Utils::SlotDesc slot_descs[] = {
            {"uuid", TYPE_VARCHAR_DESC},
            {"col_array_map", type_array_map},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, _file_array_map_path);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);

    // Illegal parquet files, not support it anymore
    ASSERT_FALSE(status.ok());

    //  ASSERT_TRUE(status.ok()) << status.message();
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
        auto file_reader = _create_file_reader(filepath);

        // --------------init context---------------
        TypeDescriptor type_array_struct =
                TypeDescriptor::create_struct_type({"c", "d"}, {TYPE_INT_DESC, TYPE_VARBINARY_DESC});
        TypeDescriptor type_array = TypeDescriptor::create_array_type(type_array_struct);
        TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, type_array});

        Utils::SlotDesc slot_descs[] = {
                {"id", TYPE_INT_DESC},
                {"col", type_struct},
                {""},
        };
        auto ctx = _create_scan_context(slot_descs, filepath);
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->row_group_size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
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
        auto file_reader = _create_file_reader(filepath, 1024);

        // --------------init context---------------
        TypeDescriptor type_array_struct =
                TypeDescriptor::create_struct_type({"c", "d"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
        TypeDescriptor type_array = TypeDescriptor::create_array_type(type_array_struct);
        TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, type_array});

        Utils::SlotDesc slot_descs[] = {
                {"id", TYPE_INT_DESC},
                {"col", type_struct},
                {""},
        };
        auto ctx = _create_scan_context(slot_descs, filepath);
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->row_group_size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    TypeDescriptor type_array_struct =
            TypeDescriptor::create_struct_type({"c", "d"}, {TYPE_INT_DESC, TYPE_VARBINARY_DESC});
    TypeDescriptor type_array = TypeDescriptor::create_array_type(type_array_struct);
    TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, type_array});

    Utils::SlotDesc slot_descs[] = {
            {"id", TYPE_INT_DESC},
            {"col", type_struct},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, filepath);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    Utils::SlotDesc slot_descs[] = {
            {"b", TYPE_VARCHAR_DESC},
            {"c", TYPE_INT_ARRAY_ARRAY_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, filepath);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);

    // Illegal parquet files, will treat illegal column as null
    ASSERT_TRUE(status.ok()) << status.message();

    EXPECT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_ARRAY_ARRAY_DESC, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());

    chunk->check_or_die();

    EXPECT_EQ("['hello', NULL]", chunk->debug_row(0));
    EXPECT_EQ("[NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("['hello', NULL]", chunk->debug_row(2));
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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    TypeDescriptor type_b = TypeDescriptor::create_struct_type({"b1", "b2"}, {TYPE_INT_DESC, TYPE_INT_DESC});

    Utils::SlotDesc slot_descs[] = {
            {"a", TYPE_INT_DESC},
            {"b", type_b},
            {"c", TYPE_INT_INT_MAP_DESC},
            {""},
    };

    auto ctx = _create_scan_context(slot_descs, filepath);

    _create_int_conjunct_ctxs(TExprOpcode::EQ, 0, 8000, &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 3);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_b, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_INT_MAP_DESC, true), chunk->num_columns());

    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        if (!status.ok() && !status.is_end_of_file()) {
            std::cout << status.message() << std::endl;
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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    TypeDescriptor type_b = TypeDescriptor::create_struct_type({"b1", "b2"}, {TYPE_INT_DESC, TYPE_INT_DESC});

    Utils::SlotDesc slot_descs[] = {
            {"a", TYPE_INT_DESC},
            {"b", type_b},
            {"c", TYPE_INT_INT_MAP_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, filepath);

    _create_int_conjunct_ctxs(TExprOpcode::EQ, 0, 8000, &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->row_group_size(), 3);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_b, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_INT_MAP_DESC, true), chunk->num_columns());

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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    TypeDescriptor type_vin = TYPE_VARCHAR_DESC;
    TypeDescriptor type_log_domain = TYPE_VARCHAR_DESC;
    TypeDescriptor type_file_name = TYPE_VARCHAR_DESC;
    TypeDescriptor type_is_collection = TYPE_INT_DESC;
    TypeDescriptor type_is_center = TYPE_INT_DESC;
    TypeDescriptor type_is_cloud = TYPE_INT_DESC;
    TypeDescriptor type_collection_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_center_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_cloud_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_error_collection_tips = TYPE_VARCHAR_DESC;
    TypeDescriptor type_error_center_tips = TYPE_VARCHAR_DESC;
    TypeDescriptor type_error_cloud_tips = TYPE_VARCHAR_DESC;
    TypeDescriptor type_error_collection_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_error_center_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_error_cloud_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_original_time = TYPE_VARCHAR_DESC;
    TypeDescriptor type_is_original = TYPE_INT_DESC;

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

    auto ctx = _create_scan_context(slot_descs, filepath);

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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    Utils::SlotDesc slot_descs[] = {
            {"myString", TYPE_VARCHAR_DESC},
            {"myInteger", TYPE_INT_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, filepath);

    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
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
    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
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

    TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"x", "y"}, {TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC});

    Utils::SlotDesc slot_descs[] = {
            {"data", TYPE_VARCHAR_DESC, 0},
            {"struct", type_struct, 1},
            {"int", TYPE_INT_DESC, 2},
            {""},
    };
    Utils::SlotDesc min_max_slots[] = {
            {"int", TYPE_INT_DESC, 2},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, min_max_slots, filepath);
    ctx->lake_schema = &schema;

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::GE, 2, 5, &t_conjuncts);
    ParquetUTBase::append_int_conjunct(TExprOpcode::LE, 2, 5, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

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

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, true), chunk->num_columns());

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

    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_VARCHAR_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""}};

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
                auto ctx = _create_file_random_read_context(file_path, slot_descs);
                ctx->conjunct_ctxs_by_slot[0].clear();
                std::vector<TExpr> t_conjuncts;
                ParquetUTBase::create_in_predicate_int_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands,
                                                                     &t_conjuncts);
                ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                    &ctx->conjunct_ctxs_by_slot[0]);

                auto file_reader = _create_file_reader(file_path);

                Status status = file_reader->init(ctx);
                ASSERT_TRUE(status.ok());
                size_t total_row_nums = 0;
                while (!status.is_end_of_file()) {
                    chunk->reset();
                    status = file_reader->get_next(&chunk);
                    chunk->check_or_die();
                    total_row_nums += chunk->num_rows();
                    if (!status.ok() && !status.is_end_of_file()) {
                        std::cout << status.message() << std::endl;
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

    TypeDescriptor type_struct =
            TypeDescriptor::create_struct_type({"c0", "c1"}, {TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC});
    TypeDescriptor type_struct_in_struct =
            TypeDescriptor::create_struct_type({"c0", "c_struct"}, {TYPE_VARCHAR_DESC, type_struct});

    std::vector<std::string> subfield_path({"c_struct", "c0"});
    _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::EQ, 3, type_struct_in_struct, subfield_path, "55",
                                                    &ctx->conjunct_ctxs_by_slot[3]);
    auto file_reader = _create_file_reader(struct_in_struct_file_path);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
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

TEST_F(FileReaderTest, TestStructSubfieldZonemap) {
    const std::string struct_in_struct_file_path =
            "./be/test/formats/parquet/test_data/test_parquet_struct_in_struct.parquet";

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
    std::vector<ExprContext*> expr_ctxs;
    _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::EQ, 3, type_struct_in_struct, subfield_path, "0",
                                                    &expr_ctxs);
    auto ctx = _create_file_struct_in_struct_read_context(struct_in_struct_file_path);
    ctx->conjunct_ctxs_by_slot.insert({3, expr_ctxs});

    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(struct_in_struct_file_path));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_struct_in_struct, true), chunk->num_columns());

    // setup OlapScanConjunctsManager
    // TypeDescriptor type_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    // type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    // type_struct.field_names.emplace_back("c0");
    //
    // type_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    // type_struct.field_names.emplace_back("c1");
    //
    // TypeDescriptor type_struct_in_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    // type_struct_in_struct.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    // type_struct_in_struct.field_names.emplace_back("c0");
    //
    // type_struct_in_struct.children.emplace_back(type_struct);
    // type_struct_in_struct.field_names.emplace_back("c_struct");

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c_struct", type_struct},
            {"c_struct_struct", type_struct_in_struct},
            {""},
    };
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    // RETURN_IF_ERROR(Expr::clone_if_not_exists(state, &_pool, _min_max_conjunct_ctxs, &cloned_conjunct_ctxs));
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[3], nullptr, tuple_desc, _runtime_state, ctx);
    for (const auto& [cid, col_children] : ctx->predicate_tree.root().col_children_map()) {
        for (const auto& child : col_children) {
            std::cout << "pred type" << child.col_pred()->type() << "pred" << child.debug_string() << std::endl;
        }
    }
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(1, file_reader->_group_reader_param.stats->parquet_filtered_row_groups);
    EXPECT_EQ(0, file_reader->_row_group_readers.size());
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(0, total_row_nums);
}

TEST_F(FileReaderTest, bloom_filter_reader) {
    const std::string bloom_filter_file = "./be/test/formats/parquet/test_data/sample.parquet";
    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_BOOLEAN_DESC}, {"c1", TYPE_VARCHAR_DESC}, {""}};
    auto file_reader = _create_file_reader(bloom_filter_file);
    auto ctx = _create_file_random_read_context(bloom_filter_file, slot_descs);
    Status status = file_reader->init(ctx);
    //auto chunk = std::make_shared<Chunk>();
    // chunk->append_column(ColumnHelper::create_column(TYPE_BOOLEAN_DESC, true), chunk->num_columns());
    // chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    // status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(file_reader->get_file_metadata() != nullptr);
    std::cout << "bloom filter meta info,"
              << " offset is set:"
              << file_reader->get_file_metadata()
                         ->t_metadata()
                         .row_groups[0]
                         .columns[1]
                         .meta_data.__isset.bloom_filter_offset
              << " offset:"
              << file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset
              << " length is set:"
              << file_reader->get_file_metadata()
                         ->t_metadata()
                         .row_groups[0]
                         .columns[1]
                         .meta_data.__isset.bloom_filter_length
              << " length:"
              << file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_length
              << std::endl;
    std::vector<char> buffer;
    buffer.resize(256);
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset,
            buffer.data(), 256);
    uint32_t header_len = 256;
    tparquet::BloomFilterHeader header;
    deserialize_thrift_msg(reinterpret_cast<const uint8*>(buffer.data()), &header_len, TProtocolType::COMPACT, &header);
    //uint64_t value = 1ULL * buffer[0] + (buffer[1] >> 8) + (buffer[2] >> 16) + (buffer[3] >> 24);
    std::cout << "bloom filter header info, header length:" << header_len;
    EXPECT_EQ(header_len, 18);
    header.printTo(std::cout);
    std::cout << std::endl;

    buffer.resize(header.numBytes + 1);
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset +
                    header_len,
            buffer.data(), header.numBytes);

    ParquetBlockSplitBloomFilter bloom_filter;

    bloom_filter.init(buffer.data(), header.numBytes + 1, Hasher::HashStrategy::XXHASH64, 0);

    ASSERT_TRUE(bloom_filter.test_bytes("A", 1));
    ASSERT_TRUE(bloom_filter.test_bytes("D", 1));
    ASSERT_FALSE(bloom_filter.test_bytes("AB", 2));
}

TEST_F(FileReaderTest, bloom_filter_reader_test_not_hit) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

    const std::string file = "./be/test/formats/parquet/test_data/sample.parquet";

    Utils::SlotDesc slot_descs[] = {{"a_bool", TYPE_BOOLEAN_DESC}, {"a_str", TYPE_VARCHAR_DESC}, {""}};
    //auto ctx = _create_file_random_read_context(file, slot_descs);

    auto ctx = _create_scan_context(slot_descs, slot_descs, file);

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, 1, "2", &t_conjuncts);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);

    // attr_value = '2'
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 1, "2", &ctx->conjunct_ctxs_by_slot[1]);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);
    // --------------finish init context---------------
    auto file_reader = _create_file_reader(file);
    ASSERT_OK(file_reader->init(ctx));

    EXPECT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, bloom_filter_reader_test_hit) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

    const std::string file = "./be/test/formats/parquet/test_data/sample.parquet";

    Utils::SlotDesc slot_descs[] = {{"a_bool", TYPE_BOOLEAN_DESC}, {"a_str", TYPE_VARCHAR_DESC}, {""}};
    //auto ctx = _create_file_random_read_context(file, slot_descs);

    auto ctx = _create_scan_context(slot_descs, slot_descs, file);

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, 1, "A", &t_conjuncts);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);

    // attr_value = '2'
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 1, "A", &ctx->conjunct_ctxs_by_slot[1]);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);
    // --------------finish init context---------------
    auto file_reader = _create_file_reader(file);
    ASSERT_OK(file_reader->init(ctx));

    EXPECT_EQ(file_reader->row_group_size(), 1);
}

TEST_F(FileReaderTest, read_parquet_bloom_filter_by_parquet_hadoop) {
    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_VARCHAR_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_DATETIME_DESC}, {""}};
    const std::string bloom_filter_file = "./be/test/formats/parquet/test_data/data_20200601.parquet";
    auto file_reader = _create_file_reader(bloom_filter_file);
    auto ctx = _create_file_random_read_context(bloom_filter_file, slot_descs);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(file_reader->get_file_metadata() != nullptr);
    std::vector<char> buffer;
    buffer.resize(256);
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset,
            buffer.data(), 256);
    uint32_t header_len = 256;
    tparquet::BloomFilterHeader header;
    deserialize_thrift_msg(reinterpret_cast<const uint8*>(buffer.data()), &header_len, TProtocolType::COMPACT, &header);
    //uint64_t value = 1ULL * buffer[0] + (buffer[1] >> 8) + (buffer[2] >> 16) + (buffer[3] >> 24);
    std::cout << "bloom filter header info, header length:" << header_len;
    EXPECT_EQ(header_len, 18);
    header.printTo(std::cout);
    std::cout << std::endl;

    buffer.resize(header.numBytes + 1);
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset +
                    header_len,
            buffer.data(), header.numBytes);

    ParquetBlockSplitBloomFilter bloom_filter;
    bloom_filter.init(buffer.data(), header.numBytes + 1, Hasher::HashStrategy::XXHASH64, 0);
    int32_t v = 1;
    char buf[4];
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_TRUE(bloom_filter.test_bytes(buf, 4));
    v = 10000;
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_TRUE(bloom_filter.test_bytes(buf, 4));
    v = 1000000000;
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_FALSE(bloom_filter.test_bytes(buf, 4));
}

TEST_F(FileReaderTest, read_parquet_bloom_filter_by_parquet_hadoop2) {
    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_VARCHAR_DESC}, {"myInteger", TYPE_INT_DESC}, {"c2", TYPE_DATETIME_DESC}, {""}};
    const std::string bloom_filter_file = "./be/test/formats/parquet/test_data/data_20200601_120000.parquet";

    auto ctx = _create_scan_context(slot_descs, slot_descs, bloom_filter_file);

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::EQ, 1, 6, &t_conjuncts);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    _create_int_conjunct_ctxs(TExprOpcode::EQ, 1, 6, &ctx->conjunct_ctxs_by_slot[1]);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);

    auto file_reader = _create_file_reader(bloom_filter_file);
    //auto ctx = _create_file_random_read_context(bloom_filter_file, slot_descs);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(file_reader->get_file_metadata() != nullptr);
    std::vector<char> buffer;
    buffer.resize(256);
    std::cout << "bloom filter offset: "
              << file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset
              << "bloom filter length: "
              << file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_length
              << std::endl;
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset,
            buffer.data(), 256);
    uint32_t header_len = 256;
    tparquet::BloomFilterHeader header;
    deserialize_thrift_msg(reinterpret_cast<const uint8*>(buffer.data()), &header_len, TProtocolType::COMPACT, &header);
    //uint64_t value = 1ULL * buffer[0] + (buffer[1] >> 8) + (buffer[2] >> 16) + (buffer[3] >> 24);
    std::cout << "bloom filter header info, header length:" << header_len;
    EXPECT_EQ(header_len, 18);
    header.printTo(std::cout);
    std::cout << std::endl;

    buffer.resize(header.numBytes + 1);
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset +
                    header_len,
            buffer.data(), header.numBytes);

    ParquetBlockSplitBloomFilter bloom_filter;
    auto& col_meta = file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data;
    if (col_meta.__isset.statistics && col_meta.statistics.__isset.null_count) {
        if (col_meta.statistics.null_count > 0) {
            buffer.back() = 1;
        } else {
            buffer.back() = 0;
        }
    } else if (file_reader->group_readers().at(0)->get_column_reader(1)->get_column_parquet_field()->is_nullable) {
        buffer.back() = 1; //set has null as default, to avoid `column is null` to filter the group.
    } else {
        buffer.back() = 0;
    }

    bloom_filter.init(buffer.data(), header.numBytes + 1, Hasher::HashStrategy::XXHASH64, 0);
    for (const auto& [cid, col_children] : ctx->predicate_tree.root().col_children_map()) {
        for (const auto& child : col_children) {
            std::cout << "pred" << child.debug_string() << std::endl;
            ASSERT_TRUE(child.col_pred()->support_original_bloom_filter());
            ASSERT_FALSE(child.col_pred()->original_bloom_filter(&bloom_filter));
        }
    }
    int32_t v = 3;
    char buf[4];
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_TRUE(bloom_filter.test_bytes(buf, 4));
    ASSERT_TRUE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
    v = 10000;
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_FALSE(bloom_filter.test_bytes(buf, 4));
    ASSERT_FALSE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
    v = 1000000000;
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_FALSE(bloom_filter.test_bytes(buf, 4));
    ASSERT_FALSE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
}

TEST_F(FileReaderTest, read_parquet_bloom_filter_by_parquet_hadoop3) {
    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_VARCHAR_DESC}, {"myInteger", TYPE_INT_DESC}, {"c2", TYPE_DATETIME_DESC}, {""}};
    const std::string bloom_filter_file = "./be/test/formats/parquet/test_data/data_20200601_120000.parquet";

    auto ctx = _create_scan_context(slot_descs, slot_descs, bloom_filter_file);

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::EQ, 1, 6, &t_conjuncts);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    _create_int_conjunct_ctxs(TExprOpcode::EQ, 1, 6, &ctx->conjunct_ctxs_by_slot[1]);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);

    auto file_reader = _create_file_reader(bloom_filter_file);
    //auto ctx = _create_file_random_read_context(bloom_filter_file, slot_descs);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(file_reader->get_file_metadata() != nullptr);
    std::vector<char> buffer;
    buffer.resize(256);
    std::cout << "bloom filter offset: "
              << file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset
              << "bloom filter length: "
              << file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_length
              << std::endl;
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset,
            buffer.data(), 256);
    uint32_t header_len = 256;
    tparquet::BloomFilterHeader header;
    deserialize_thrift_msg(reinterpret_cast<const uint8*>(buffer.data()), &header_len, TProtocolType::COMPACT, &header);
    //uint64_t value = 1ULL * buffer[0] + (buffer[1] >> 8) + (buffer[2] >> 16) + (buffer[3] >> 24);
    std::cout << "bloom filter header info, header length:" << header_len;
    EXPECT_EQ(header_len, 18);
    header.printTo(std::cout);
    std::cout << std::endl;

    buffer.resize(header.numBytes + 1);
    file_reader->_file->read_at_fully(
            file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data.bloom_filter_offset +
                    header_len,
            buffer.data(), header.numBytes);

    ParquetBlockSplitBloomFilter bloom_filter;
    auto& col_meta = file_reader->get_file_metadata()->t_metadata().row_groups[0].columns[1].meta_data;
    if (col_meta.__isset.statistics && col_meta.statistics.__isset.null_count) {
        if (col_meta.statistics.null_count > 0) {
            buffer.back() = 1;
        } else {
            buffer.back() = 0;
        }
    } else if (file_reader->group_readers().at(0)->get_column_reader(1)->get_column_parquet_field()->is_nullable) {
        buffer.back() = 1; //set has null as default, to avoid `column is null` to filter the group.
    } else {
        buffer.back() = 0;
    }

    bloom_filter.init(buffer.data(), header.numBytes + 1, Hasher::HashStrategy::XXHASH64, 0);

    for (const auto& [cid, col_children] : ctx->predicate_tree.root().col_children_map()) {
        for (const auto& child : col_children) {
            std::cout << "pred" << child.debug_string() << std::endl;
            ASSERT_TRUE(child.col_pred()->support_original_bloom_filter());
            ASSERT_FALSE(child.col_pred()->original_bloom_filter(&bloom_filter));
        }
    }
    int32_t v = 3;
    char buf[4];
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_TRUE(bloom_filter.test_bytes(buf, 4));
    ASSERT_TRUE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
    v = 10000;
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_FALSE(bloom_filter.test_bytes(buf, 4));
    ASSERT_FALSE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
    v = 1000000000;
    std::memcpy(buf, &v, sizeof(v));
    ASSERT_FALSE(bloom_filter.test_bytes(buf, 4));
    ASSERT_FALSE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
}

TEST_F(FileReaderTest, read_parquet_bloom_filter_by_parquet_hadoop4) {
    TypeDescriptor type_struct = TypeDescriptor::create_struct_type(
            {"col_long", "col_double"},
            {TYPE_BIGINT_DESC, TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE)});

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"col_bool", TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN)},
            {"col_int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"col_float", TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT)},
            {"col_struct", type_struct},
            {"col_string", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"col_date", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE)},
            //{"col_decimal32", TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32)},
            {""},
    };
    const std::string bloom_filter_file =
            "./be/test/formats/parquet/test_data/data_with_page_index_and_bloom_filter.parquet";

    std::vector<std::string> subfield_path({"col_long"});
    std::vector<ExprContext*> expr_ctxs;

    std::vector<TExprNode> nodes;

    TExprNode node0 = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::BIGINT, TExprOpcode::EQ);
    node0.__set_is_monotonic(true);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SUBFIELD_EXPR;
    node1.is_nullable = true;
    node1.type = gen_type_desc(TPrimitiveType::BIGINT);
    node1.num_children = 1;
    node1.used_subfield_names = subfield_path;
    node1.__isset.used_subfield_names = true;
    node1.__set_is_monotonic(true);
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::SLOT_REF;
    node2.type = type_struct.to_thrift();
    node2.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 3;
    t_slot_ref.tuple_id = 0;
    node2.__set_slot_ref(t_slot_ref);
    node2.is_nullable = true;
    node2.__set_is_monotonic(true);
    nodes.emplace_back(node2);

    TExprNode node3;
    node3.node_type = TExprNodeType::INT_LITERAL;
    node3.type = gen_type_desc(TPrimitiveType::BIGINT);
    node3.num_children = 0;
    TIntLiteral int_literal;
    int_literal.value = 20;
    node3.__set_int_literal(int_literal);
    node3.is_nullable = false;
    node3.__set_is_monotonic(true);
    nodes.emplace_back(node3);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    ASSERT_OK(Expr::create_expr_trees(&_pool, t_conjuncts, &expr_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(expr_ctxs, _runtime_state));
    ASSERT_OK(Expr::open(expr_ctxs, _runtime_state));

    auto ctx = _create_scan_context(slot_descs, slot_descs, bloom_filter_file);
    ctx->conjunct_ctxs_by_slot.insert({3, expr_ctxs});

    // ParquetUTBase::append_int_conjunct(TExprOpcode::EQ, 2, 6, &t_conjuncts);
    // ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    // _create_int_conjunct_ctxs(TExprOpcode::EQ, 2, 6, &ctx->conjunct_ctxs_by_slot[1]);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[3], nullptr, tuple_desc, _runtime_state, ctx);
    for (const auto& [cid, col_children] : ctx->predicate_tree.root().col_children_map()) {
        for (const auto& child : col_children) {
            std::cout << "pred type" << child.col_pred()->type() << "pred" << child.debug_string() << std::endl;
        }
    }
    auto file_reader = _create_file_reader(bloom_filter_file);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(file_reader->get_file_metadata() != nullptr);
    EXPECT_EQ(file_reader->row_group_size(), 0);
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

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, true), chunk->num_columns());

    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_VARCHAR_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""}};

    auto ctx = _create_file_random_read_context(file_path, slot_descs);
    // c0 >= 100
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 100, &ctx->conjunct_ctxs_by_slot[0]);
    // c1 <= 100
    _create_int_conjunct_ctxs(TExprOpcode::LE, 1, 100, &ctx->conjunct_ctxs_by_slot[1]);
    auto file_reader = _create_file_reader(file_path);
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

    TypeDescriptor type_struct = TypeDescriptor::create_struct_type({"c0"}, {TYPE_VARCHAR_DESC});
    TypeDescriptor type_struct_in_struct = TypeDescriptor::create_struct_type({"c_struct"}, {type_struct});

    std::vector<std::string> subfield_path({"c_struct", "c0"});
    _create_struct_subfield_predicate_conjunct_ctxs(TExprOpcode::EQ, 1, type_struct_in_struct, subfield_path, "55",
                                                    &ctx->conjunct_ctxs_by_slot[1]);
    auto file_reader = _create_file_reader(struct_in_struct_file_path);

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

TEST_F(FileReaderTest, TestReadFooterCache) {
    auto block_cache = std::make_shared<BlockCache>();
    CacheOptions options;
    options.mem_space_size = 100 * 1024 * 1024;
    options.max_concurrent_inserts = 100000;
    options.engine = "starcache";
    Status status = block_cache->init(options);
    ASSERT_TRUE(status.ok());
    auto cache = std::make_shared<StarCacheModule>(block_cache->starcache_instance());

    auto file = _create_file(_file1_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file1_path), _mock_datacache_options());
    file_reader->_cache = cache.get();

    // first init, populcate footer cache
    auto* ctx = _create_file1_base_context();
    ctx->stats->footer_cache_read_count = 0;
    ctx->stats->footer_cache_write_count = 0;
    status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ctx->stats->footer_cache_read_count, 0);
    ASSERT_EQ(ctx->stats->footer_cache_write_count, 1);

    auto file_reader2 = std::make_shared<FileReader>(
            config::vector_chunk_size, file.get(), std::filesystem::file_size(_file1_path), _mock_datacache_options());
    file_reader2->_cache = cache.get();

    // second init, read footer cache
    auto* ctx2 = _create_file1_base_context();
    ctx2->stats->footer_cache_read_count = 0;
    ctx2->stats->footer_cache_write_count = 0;
    ctx2->stats->footer_cache_read_ns = 0;
    Status status2 = file_reader2->init(ctx2);
    ASSERT_TRUE(status2.ok());
    ASSERT_EQ(ctx2->stats->footer_cache_read_count, 1);
    ASSERT_EQ(ctx2->stats->footer_cache_write_count, 0);
}

TEST_F(FileReaderTest, TestTime) {
    // format:
    // id: INT, b: TIME
    const std::string filepath = "./be/test/formats/parquet/test_data/test_parquet_time_type.parquet";

    auto file_reader = _create_file_reader(filepath);

    // --------------init context---------------
    Utils::SlotDesc slot_descs[] = {
            {"c1", TYPE_INT_DESC},
            {"c2", TYPE_TIME_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, filepath);
    // --------------finish init context---------------

    ASSERT_OK(file_reader->init(ctx));
    EXPECT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_TIME_DESC, true), chunk->num_columns());

    ASSERT_OK(file_reader->get_next(&chunk));

    chunk->check_or_die();

    EXPECT_EQ("[1, 3723]", chunk->debug_row(0));
    EXPECT_EQ("[4, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[3, 11045]", chunk->debug_row(2));
    EXPECT_EQ("[2, 7384]", chunk->debug_row(3));

    size_t total_row_nums = 0;
    total_row_nums += chunk->num_rows();

    {
        Status status;
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            total_row_nums += chunk->num_rows();
        }
    }

    EXPECT_EQ(4, total_row_nums);
}

TEST_F(FileReaderTest, TestReadNoMinMaxStatistics) {
    auto file_reader = _create_file_reader(_file_no_min_max_stats_path);

    // --------------init context---------------
    Utils::SlotDesc slot_descs[] = {
            {"attr_value", TYPE_VARCHAR_DESC},
            {""},
    };
    auto ctx = _create_scan_context(slot_descs, slot_descs, _file_no_min_max_stats_path);

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_string_conjunct(TExprOpcode::GE, 0, "2", &t_conjuncts);
    ParquetUTBase::append_string_conjunct(TExprOpcode::LE, 0, "2", &t_conjuncts);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);

    // attr_value = '2'
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 0, "2", &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    ASSERT_OK(file_reader->init(ctx));

    EXPECT_EQ(file_reader->row_group_size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());

    ASSERT_OK(file_reader->get_next(&chunk));

    chunk->check_or_die();

    EXPECT_EQ("['2']", chunk->debug_row(0));
    EXPECT_EQ("['2']", chunk->debug_row(1));
    EXPECT_EQ("['2']", chunk->debug_row(2));
    EXPECT_EQ(chunk->num_rows(), 111);
}

TEST_F(FileReaderTest, TestIsNotNullStatistics) {
    auto file_reader = _create_file_reader(_file1_path);

    auto* ctx = _create_file1_base_context();
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::is_null_pred(0, false, &t_conjuncts);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjunct_ctxs_by_slot[0]);

    ASSERT_OK(file_reader->init(ctx));
    EXPECT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, TestIsNullStatistics) {
    const std::string small_page_file = "./be/test/formats/parquet/test_data/read_range_test.parquet";
    auto file = _create_file(small_page_file);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(small_page_file));

    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_VARCHAR_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""},
    };
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::is_null_pred(0, true, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto ctx = _create_file_random_read_context(small_page_file, slot_descs);
    ctx->conjunct_ctxs_by_slot.insert({0, expr_ctxs});

    // setup OlapScanConjunctsManager
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[0], nullptr, tuple_desc, _runtime_state, ctx);

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, TestMapKeyIsStruct) {
    const std::string filename = "./be/test/formats/parquet/test_data/map_key_is_struct.parquet";

    auto file_reader = _create_file_reader(filename);
    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_VARCHAR_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""},
    };
    auto ctx = _create_file_random_read_context(filename, slot_descs);
    Status status = file_reader->init(ctx);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("Map keys must be primitive type.", status.message());
}

TEST_F(FileReaderTest, TestInFilterStatitics) {
    // there are 4 row groups
    const std::string multi_rg_file = "./be/test/formats/parquet/test_data/page_index_big_page.parquet";

    auto file_reader = _create_file_reader(multi_rg_file);
    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_VARCHAR_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""},
    };
    // min value and max value in this file, so it will be in the first and last row group
    std::set<int32_t> in_oprands{1, 100000};
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_in_predicate_int_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);

    auto ctx = _create_file_random_read_context(multi_rg_file, slot_descs);
    ctx->conjunct_ctxs_by_slot.insert({0, expr_ctxs});

    // setup OlapScanConjunctsManager
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[0], nullptr, tuple_desc, _runtime_state, ctx);

    ASSERT_OK(file_reader->init(ctx));
    EXPECT_EQ(file_reader->row_group_size(), 2);
}

// parquet has no null
// filter the first row group
TEST_F(FileReaderTest, filter_row_group_with_rf_1) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_filter_row_group_path_1);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 5, 6, false);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
}

// parquet has no null
// filter no group
TEST_F(FileReaderTest, filter_row_group_with_rf_2) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_filter_row_group_path_1);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 2, 5, false);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 2);
}

// parquet has no null
// filter all group
TEST_F(FileReaderTest, filter_row_group_with_rf_3) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_filter_row_group_path_1);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 7, 10, false);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 0);
}

// parquet has null
// filter no group
TEST_F(FileReaderTest, filter_row_group_with_rf_4) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_filter_row_group_path_2);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 5, 6, true);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 2);
}

// parquet has null
// partition column has no null
// filter no group
TEST_F(FileReaderTest, filter_row_group_with_rf_5) {
    SlotId slot_id = 3;

    auto file_reader = _create_file_reader(_filter_row_group_path_2);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 5, 6, true);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 2);
}

// parquet has null
// partition column has no null
// filter all group
TEST_F(FileReaderTest, filter_row_group_with_rf_6) {
    SlotId slot_id = 4;

    auto file_reader = _create_file_reader(_filter_row_group_path_2);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 5, 6, true);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 0);
}

// parquet has null
// partition column has null
// filter no group
TEST_F(FileReaderTest, filter_row_group_with_rf_7) {
    SlotId slot_id = 5;

    auto file_reader = _create_file_reader(_filter_row_group_path_2);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 5, 6, true);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 2);
}

// parquet has null
// column not exist
// filter no group
TEST_F(FileReaderTest, filter_row_group_with_rf_8) {
    SlotId slot_id = 8;

    auto file_reader = _create_file_reader(_filter_row_group_path_2);

    auto ret = _create_context_for_filter_row_group_1(slot_id, 5, 6, true);
    ASSERT_TRUE(ret.ok());

    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 2);
}

TEST_F(FileReaderTest, update_rf_and_filter_row_group) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_filter_row_group_path_3);
    ASSIGN_OR_ASSERT_FAIL(auto* ctx, _create_context_for_filter_row_group_update_rf(slot_id));

    ASSERT_OK(file_reader->init(ctx));
    ASSERT_EQ(file_reader->row_group_size(), 3);

    ChunkPtr chunk = _create_int_chunk();
    ASSERT_OK(file_reader->get_next(&chunk));
    ASSERT_EQ(chunk->num_rows(), 3);
    ASSERT_EQ(chunk->debug_row(0), "[1, 11]");
    ASSERT_EQ(chunk->debug_row(1), "[2, 22]");
    ASSERT_EQ(chunk->debug_row(2), "[3, 33]");

    auto* rf = MinMaxRuntimeFilter<TYPE_INT>::create_with_range<false>(&_pool, 3, false);
    ctx->runtime_filter_collector->descriptors().at(1)->set_runtime_filter(rf);

    chunk->reset();
    ASSERT_OK(file_reader->get_next(&chunk));
    ASSERT_EQ(chunk->num_rows(), 3);
    ASSERT_EQ(chunk->debug_row(0), "[4, 44]");
    ASSERT_EQ(chunk->debug_row(1), "[5, 55]");
    ASSERT_EQ(chunk->debug_row(2), "[6, 66]");

    chunk->reset();
    auto st = file_reader->get_next(&chunk);
    ASSERT_TRUE(st.is_end_of_file());
}

TEST_F(FileReaderTest, filter_page_index_with_rf_has_null) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_filter_page_index_with_rf_has_null);
    auto ret = _create_context_for_filter_page_index(slot_id, 92880, 92990, true);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([0,20000), [40000,40100))");
}

TEST_F(FileReaderTest, all_type_has_null_page_bool) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_bool(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([0,90000))");
}

TEST_F(FileReaderTest, all_type_has_null_page_smallint) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_smallint(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([40000,90000))");
}

TEST_F(FileReaderTest, all_type_has_null_page_int) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_int32(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([40000,90000))");
}

TEST_F(FileReaderTest, all_type_has_null_page_bigint) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_int64(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([40000,90000))");
}

TEST_F(FileReaderTest, all_type_has_null_page_datetime) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_datetime(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([40000,90000))");
}

TEST_F(FileReaderTest, all_type_has_null_page_string) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_string(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([40000,90000))");
}

TEST_F(FileReaderTest, all_type_has_null_page_decimal) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_has_null_page_file);
    auto ret = _create_context_for_has_null_page_decimal(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
    const auto& group_readers = file_reader->group_readers();
    ASSERT_EQ(group_readers[0]->get_range().to_string(), "([40000,90000))");
}

TEST_F(FileReaderTest, all_null_group_in_filter) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_all_null_parquet_file);
    auto ret = _create_context_for_in_filter(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, in_filter_filter_one_group) {
    SlotId slot_id = 1;

    auto file_reader = _create_file_reader(_filter_row_group_path_1);
    auto ret = _create_context_for_in_filter_normal(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 1);
}

TEST_F(FileReaderTest, min_max_filter_all_null_group) {
    SlotId slot_id = 0;

    auto file_reader = _create_file_reader(_all_null_parquet_file);
    auto ret = _create_context_for_min_max_all_null_group(slot_id);

    ASSERT_TRUE(ret.ok());
    ASSERT_OK(file_reader->init(ret.value()));
    ASSERT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, low_card_reader) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {""}};

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 100; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[1] = &g_dict;

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_dictmapping_string_conjunct(TExprOpcode::EQ, 1, "2", &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto ctx = _create_file_random_read_context(small_page_file, slot_descs);
    ctx->conjunct_ctxs_by_slot.insert({1, expr_ctxs});
    ctx->global_dictmaps = &dict_map;
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);

    auto file_reader = _create_file_reader(small_page_file);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        ColumnPtr c0 = chunk->get_column_by_index(0);
        ColumnPtr c1 = chunk->get_column_by_index(1);
        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
            int32_t c0_value = c0->get(row_index).get_int32();
            if (c0_value % 10 == 0) {
                EXPECT_TRUE(c1->is_null(row_index));
            } else {
                EXPECT_FALSE(c1->is_null(row_index));
                std::string expected_string = std::to_string(c0_value % 100);
                int32_t global_code = g_dict.at(Slice(expected_string));
                EXPECT_EQ(global_code, c1->get(row_index).get_int32());
            }
        }
    }

    EXPECT_EQ(200, total_row_nums);
}

TEST_F(FileReaderTest, low_card_reader_filter_group) {
    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {""}};

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 100; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[1] = &g_dict;

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_dictmapping_string_conjunct(TExprOpcode::GT, 1, "a", &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto ctx = _create_file_random_read_context(small_page_file, slot_descs);
    ctx->conjunct_ctxs_by_slot.insert({1, expr_ctxs});
    ctx->global_dictmaps = &dict_map;
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    tuple_desc->decoded_slots()[1]->type().type = TYPE_VARCHAR;
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);

    auto file_reader = _create_file_reader(small_page_file);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, low_card_reader_dict_not_match) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {""}};
    auto ctx = _create_file_random_read_context(small_page_file, slot_descs);

    std::vector<std::string> values;
    for (int i = 0; i < 90; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 90; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[1] = &g_dict;

    ctx->global_dictmaps = &dict_map;

    auto file_reader = _create_file_reader(small_page_file);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        if (!status.ok()) {
            ASSERT_EQ("Global dictionary not match", status.code_as_string());
            return;
        }
    }

    ASSERT_TRUE(false);
}

TEST_F(FileReaderTest, no_matched_reader) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());

    const std::string file = "./be/test/formats/parquet/test_data/page_index_repeated_nodict.parquet";

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {""}};
    auto ctx = _create_file_random_read_context(file, slot_descs);

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 100; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[1] = &g_dict;

    ctx->global_dictmaps = &dict_map;

    auto file_reader = _create_file_reader(file);
    Status status = file_reader->init(ctx);
    ASSERT_EQ("Global dictionary not match", status.code_as_string());
}

TEST_F(FileReaderTest, low_rows_reader) {
    auto chunk = std::make_shared<Chunk>();

    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, true), chunk->num_columns());

    const std::string low_rows_file = "./be/test/formats/parquet/test_data/low_rows_non_dict.parquet";

    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""}};
    auto ctx = _create_file_random_read_context(low_rows_file, slot_descs);

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 100; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[2] = &g_dict;
    dict_map[3] = &g_dict;

    ctx->global_dictmaps = &dict_map;

    auto file_reader = _create_file_reader(low_rows_file);
    Status status = file_reader->init(ctx);

    ASSERT_TRUE(status.ok());
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        ColumnPtr c0 = chunk->get_column_by_index(0);
        ColumnPtr c1 = chunk->get_column_by_index(1);
        ColumnPtr c2 = chunk->get_column_by_index(2);
        ColumnPtr c3 = chunk->get_column_by_index(3);
        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
            int32_t c0_value = c0->get(row_index).get_int32();
            if (c0_value % 10 == 0) {
                EXPECT_TRUE(c2->is_null(row_index));
                EXPECT_TRUE(c3->is_null(row_index));
            } else {
                EXPECT_FALSE(c2->is_null(row_index));
                EXPECT_FALSE(c3->is_null(row_index));
                int32_t c1_value = c1->get(row_index).get_int32();
                std::string expected_c0_string = std::to_string(c0_value % 100);
                std::string expected_c1_string = std::to_string(c1_value % 100);
                int32_t c0_global_code = g_dict.at(Slice(expected_c0_string));
                int32_t c1_global_code = g_dict.at(Slice(expected_c1_string));
                EXPECT_EQ(c0_global_code, c2->get(row_index).get_int32());
                DatumArray c3_value = c3->get(row_index).get_array();
                EXPECT_EQ(3, c3_value.size());
                EXPECT_EQ(c0_global_code, c3_value[0].get_int32());
                EXPECT_TRUE(c3_value[1].is_null());
                EXPECT_EQ(c1_global_code, c3_value[2].get_int32());
            }
        }
    }

    EXPECT_EQ(100, total_row_nums);
}

TEST_F(FileReaderTest, low_rows_reader_empty_not_null_not_match) {
    auto chunk = std::make_shared<Chunk>();

    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, true), chunk->num_columns());

    const std::string low_rows_file = "./be/test/formats/parquet/test_data/low_rows_non_dict_empty.parquet";

    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""}};
    auto ctx = _create_file_random_read_context(low_rows_file, slot_descs);

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 100; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[2] = &g_dict;
    dict_map[3] = &g_dict;

    ctx->global_dictmaps = &dict_map;

    auto file_reader = _create_file_reader(low_rows_file);
    Status status = file_reader->init(ctx);

    ASSERT_TRUE(status.ok());
    chunk->reset();
    status = file_reader->get_next(&chunk);
    ASSERT_EQ("Global dictionary not match", status.code_as_string());
}

TEST_F(FileReaderTest, low_rows_reader_empty_not_null) {
    auto chunk = std::make_shared<Chunk>();

    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, true), chunk->num_columns());

    const std::string low_rows_file = "./be/test/formats/parquet/test_data/low_rows_non_dict_empty.parquet";

    Utils::SlotDesc slot_descs[] = {
            {"c0", TYPE_INT_DESC}, {"c1", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {"c3", TYPE_INT_ARRAY_DESC}, {""}};
    auto ctx = _create_file_random_read_context(low_rows_file, slot_descs);

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    values.push_back("");
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 101; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[2] = &g_dict;
    dict_map[3] = &g_dict;

    ctx->global_dictmaps = &dict_map;

    auto file_reader = _create_file_reader(low_rows_file);
    Status status = file_reader->init(ctx);

    ASSERT_TRUE(status.ok());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        ColumnPtr c0 = chunk->get_column_by_index(0);
        ColumnPtr c1 = chunk->get_column_by_index(1);
        ColumnPtr c2 = chunk->get_column_by_index(2);
        ColumnPtr c3 = chunk->get_column_by_index(3);
        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
            int32_t c0_value = c0->get(row_index).get_int32();
            EXPECT_FALSE(c2->is_null(row_index));
            EXPECT_FALSE(c3->is_null(row_index));
            int32_t c1_value = c1->get(row_index).get_int32();
            std::string expected_c0_string = std::to_string(c0_value % 100);
            std::string expected_c1_string = std::to_string(c1_value % 100);
            int32_t c0_global_code = g_dict.at(Slice(expected_c0_string));
            int32_t c1_global_code = g_dict.at(Slice(expected_c1_string));
            int32_t empty_global_code = g_dict.at(Slice(""));
            DatumArray c3_value = c3->get(row_index).get_array();
            if (c0_value % 10 == 0) {
                EXPECT_EQ(empty_global_code, c2->get(row_index).get_int32());
                EXPECT_EQ(0, c3_value.size());
            } else {
                EXPECT_EQ(c0_global_code, c2->get(row_index).get_int32());
                EXPECT_EQ(3, c3_value.size());
                EXPECT_EQ(c0_global_code, c3_value[0].get_int32());
                EXPECT_EQ(empty_global_code, c3_value[1].get_int32());
                EXPECT_EQ(c1_global_code, c3_value[2].get_int32());
            }
        }
    }

    EXPECT_EQ(100, total_row_nums);
}

TEST_F(FileReaderTest, low_rows_reader_filter_group) {
    const std::string file = "./be/test/formats/parquet/test_data/low_rows_non_dict.parquet";

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC}, {"c2", TYPE_INT_DESC}, {""}};

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        values.push_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 100; ++i) {
        g_dict[Slice(values[i])] = i + 1;
    }
    dict_map[1] = &g_dict;

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_dictmapping_string_conjunct(TExprOpcode::EQ, 1, "a", &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto ctx = _create_file_random_read_context(file, slot_descs);
    ctx->conjunct_ctxs_by_slot.insert({1, expr_ctxs});
    ctx->global_dictmaps = &dict_map;
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    tuple_desc->decoded_slots()[1]->type().type = TYPE_VARCHAR;
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[1], nullptr, tuple_desc, _runtime_state, ctx);

    auto file_reader = _create_file_reader(file);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(file_reader->row_group_size(), 0);
}

TEST_F(FileReaderTest, plain_string_decode) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_INT_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_DESC, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TYPE_VARCHAR_ARRAY_DESC, true), chunk->num_columns());

    const std::string file = "./be/test/formats/parquet/test_data/low_rows_non_dict.parquet";

    Utils::SlotDesc slot_descs[] = {{"c0", TYPE_INT_DESC},
                                    {"c1", TYPE_INT_DESC},
                                    {"c2", TYPE_VARCHAR_DESC},
                                    {"c3", TYPE_VARCHAR_ARRAY_DESC},
                                    {""}};

    std::set<int32_t> in_oprands{1, 100};
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_in_predicate_int_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands, &t_conjuncts);
    std::vector<ExprContext*> expr_ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
    auto ctx = _create_file_random_read_context(file, slot_descs);
    ctx->conjunct_ctxs_by_slot.insert({0, expr_ctxs});

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    ParquetUTBase::setup_conjuncts_manager(ctx->conjunct_ctxs_by_slot[0], nullptr, tuple_desc, _runtime_state, ctx);

    auto file_reader = _create_file_reader(file);
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
        if (chunk->num_rows() == 2) {
            ASSERT_EQ(chunk->debug_row(0), "[1, 100, '1', ['1',NULL,'0']]");
            ASSERT_EQ(chunk->debug_row(1), "[100, 1, NULL, NULL]");
        }
    }

    EXPECT_EQ(2, total_row_nums);
}

} // namespace starrocks::parquet
