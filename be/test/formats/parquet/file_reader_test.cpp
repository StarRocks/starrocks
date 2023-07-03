// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/parquet/file_reader.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/binary_predicate.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/page_reader.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"

namespace starrocks::parquet {

static vectorized::HdfsScanStats g_hdfs_scan_stats;
using starrocks::vectorized::HdfsScannerContext;

// TODO: min/max conjunct
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
    HdfsScannerContext* _create_file_map_partial_materialize_context();

    void _create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                   std::vector<ExprContext*>* conjunct_ctxs);
    void _create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                      std::vector<ExprContext*>* conjunct_ctxs);

    static vectorized::ChunkPtr _create_chunk();
    static vectorized::ChunkPtr _create_multi_page_chunk();
    static vectorized::ChunkPtr _create_struct_chunk();
    static vectorized::ChunkPtr _create_required_array_chunk();
    static vectorized::ChunkPtr _create_chunk_for_partition();
    static vectorized::ChunkPtr _create_chunk_for_not_exist();
    static void _append_column_for_chunk(PrimitiveType column_type, vectorized::ChunkPtr* chunk);

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

    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
};

struct SlotDesc {
    string name;
    TypeDescriptor type;
};

TupleDescriptor* create_tuple_descriptor(RuntimeState* state, ObjectPool* pool, const SlotDesc* slot_descs) {
    TDescriptorTableBuilder table_desc_builder;

    TTupleDescriptorBuilder tuple_desc_builder;
    int size = 0;
    for (int i = 0;; i++) {
        if (slot_descs[i].name == "") {
            break;
        }
        TSlotDescriptorBuilder b2;
        b2.column_name(slot_descs[i].name).type(slot_descs[i].type).id(i).nullable(true);
        tuple_desc_builder.add_slot(b2.build());
        size += 1;
    }
    tuple_desc_builder.build(&table_desc_builder);

    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(state, pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);

    RowDescriptor* row_desc = pool->add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
    return row_desc->tuple_descriptors()[0];
}

void make_column_info_vector(const TupleDescriptor* tuple_desc, std::vector<HdfsScannerContext::ColumnInfo>* columns) {
    columns->clear();
    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = tuple_desc->slots()[i];
        HdfsScannerContext::ColumnInfo c;
        c.col_name = slot->col_name();
        c.col_idx = i;
        c.slot_id = slot->id();
        c.col_type = slot->type();
        c.slot_desc = slot;
        columns->emplace_back(c);
    }
}

std::unique_ptr<RandomAccessFile> FileReaderTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

HdfsScannerContext* FileReaderTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file1_base_context() {
    auto ctx = _create_scan_context();

    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file1_path, 1024));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_partition() {
    auto ctx = _create_scan_context();

    SlotDesc slot_descs[] = {
            // {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            // {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file1_path, 1024));
    auto column = vectorized::ColumnHelper::create_const_column<PrimitiveType::TYPE_INT>(1, 1);
    ctx->partition_values.emplace_back(column);

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_not_exist() {
    auto ctx = _create_scan_context();

    SlotDesc slot_descs[] = {
            // {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            // {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file1_path, 1024));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file2_base_context() {
    auto ctx = _create_scan_context();

    // tuple desc and conjuncts
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file2_path, 850));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_min_max() {
    auto* ctx = _create_file2_base_context();

    SlotDesc min_max_slots[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    ctx->min_max_tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    // create min max conjuncts
    // c1 >= 1
    _create_int_conjunct_ctxs(TExprOpcode::GE, 0, 1, &ctx->min_max_conjunct_ctxs);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_filter_file() {
    auto* ctx = _create_file2_base_context();

    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);

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
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"B1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file5_base_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_inner(PrimitiveType::TYPE_ARRAY);
    type_inner.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    TypeDescriptor type_outer(PrimitiveType::TYPE_ARRAY);
    type_outer.children.emplace_back(type_inner);

    // tuple desc
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", type_outer},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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

    TypeDescriptor array_column(PrimitiveType::TYPE_ARRAY);
    array_column.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    // tuple desc and conjuncts
    // struct columns are not supported now, so we skip reading them
    SlotDesc slot_descs[] = {
            {"col_int", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"col_array", array_column},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file6_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_map_char_key_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_map_char(PrimitiveType::TYPE_MAP);
    type_map_char.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_CHAR));
    type_map_char.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor type_map_varchar(PrimitiveType::TYPE_MAP);
    type_map_varchar.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map_varchar.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", type_map_char},
            {"c3", type_map_varchar},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_char_key_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_map_base_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor type_map_map(PrimitiveType::TYPE_MAP);
    type_map_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(PrimitiveType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    TypeDescriptor type_map_array(PrimitiveType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map_array.children.emplace_back(type_array);

    // tuple desc
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", type_map},
            {"c3", type_map_map},
            {"c4", type_map_array},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file_map_partial_materialize_context() {
    auto ctx = _create_scan_context();

    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    // only key will be materialized
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::INVALID_TYPE));

    TypeDescriptor type_map_map(PrimitiveType::TYPE_MAP);
    // the first level value will be materialized, and the second level key will be materialized
    type_map_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::INVALID_TYPE));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(PrimitiveType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    // only value will be materialized
    TypeDescriptor type_map_array(PrimitiveType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::INVALID_TYPE));
    type_map_array.children.emplace_back(type_array);

    // tuple desc
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", type_map},
            {"c3", type_map_map},
            {"c4", type_map_array},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_path));

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
    node0.use_vectorized = true;
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.use_vectorized = true;
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::INT_LITERAL;
    node2.type = gen_type_desc(TPrimitiveType::INT);
    node2.num_children = 0;
    TIntLiteral int_literal;
    int_literal.value = value;
    node2.__set_int_literal(int_literal);
    node2.use_vectorized = true;
    node2.is_nullable = false;
    nodes.emplace_back(node2);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs);
    Expr::prepare(*conjunct_ctxs, _runtime_state);
    Expr::open(*conjunct_ctxs, _runtime_state);
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
    node0.use_vectorized = true;
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.use_vectorized = true;
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::STRING_LITERAL;
    node2.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node2.num_children = 0;
    TStringLiteral string_literal;
    string_literal.value = value;
    node2.__set_string_literal(string_literal);
    node2.use_vectorized = true;
    node2.is_nullable = false;
    nodes.emplace_back(node2);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);

    Expr::create_expr_trees(&_pool, t_conjuncts, conjunct_ctxs);
    Expr::prepare(*conjunct_ctxs, _runtime_state);
    Expr::open(*conjunct_ctxs, _runtime_state);
}

THdfsScanRange* FileReaderTest::_create_scan_range(const std::string& file_path, size_t scan_length) {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = file_path;
    scan_range->file_length = std::filesystem::file_size(file_path);
    scan_range->offset = 4;
    scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;

    return scan_range;
}

void FileReaderTest::_append_column_for_chunk(PrimitiveType column_type, vectorized::ChunkPtr* chunk) {
    auto c = vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(column_type), true);
    (*chunk)->append_column(c, (*chunk)->num_columns());
}

vectorized::ChunkPtr FileReaderTest::_create_chunk() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_BIGINT, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_DATETIME, &chunk);
    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_multi_page_chunk() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_BIGINT, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_DATETIME, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_struct_chunk() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_required_array_chunk() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);

    TypeDescriptor array_column(PrimitiveType::TYPE_ARRAY);
    array_column.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    auto c = vectorized::ColumnHelper::create_column(array_column, true);
    chunk->append_column(c, chunk->num_columns());
    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_chunk_for_partition() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_chunk_for_not_exist() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
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
                                                    std::filesystem::file_size(_file2_path));
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
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_filter_column_indices[0];
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
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_filter_column_indices[0];
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
    ASSERT_FALSE(status.is_end_of_file());
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
        for (int i = 0; i < chunk->num_rows(); ++i) {
            std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
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
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_column_indices.size());
        int col_idx = file_reader->_row_group_readers[0]->_dict_filter_column_indices[0];
        ASSERT_EQ(1, file_reader->_row_group_readers[0]->_param.read_cols[col_idx].slot_id);
    }

    // get next
    auto chunk = _create_struct_chunk();
    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }

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
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }

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

    TypeDescriptor type_inner(PrimitiveType::TYPE_ARRAY);
    type_inner.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    TypeDescriptor type_outer(PrimitiveType::TYPE_ARRAY);
    type_outer.children.emplace_back(type_inner);

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    auto c = vectorized::ColumnHelper::create_column(type_outer, true);
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
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c2.value, c3.key, c3.value
    EXPECT_EQ(ranges.size(), 5);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 1);
    TypeDescriptor type_map_char(PrimitiveType::TYPE_MAP);
    type_map_char.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_CHAR));
    type_map_char.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor type_map_varchar(PrimitiveType::TYPE_MAP);
    type_map_varchar.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map_varchar.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    auto c = vectorized::ColumnHelper::create_column(type_map_char, true);
    chunk->append_column(c, chunk->num_columns());
    auto c_map1 = vectorized::ColumnHelper::create_column(type_map_varchar, true);
    chunk->append_column(c_map1, chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 1);
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }
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
    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor type_map_map(PrimitiveType::TYPE_MAP);
    type_map_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(PrimitiveType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    TypeDescriptor type_map_array(PrimitiveType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map_array.children.emplace_back(type_array);

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    auto c = vectorized::ColumnHelper::create_column(type_map, true);
    chunk->append_column(c, chunk->num_columns());
    auto c_map_map = vectorized::ColumnHelper::create_column(type_map_map, true);
    chunk->append_column(c_map_map, chunk->num_columns());
    auto c_map_array = vectorized::ColumnHelper::create_column(type_map_array, true);
    chunk->append_column(c_map_array, chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 8);
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }
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

    TypeDescriptor c1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    // Test unordered field name
    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c2.field_names.emplace_back("f2");

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2.field_names.emplace_back("f1");

    TypeDescriptor f3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("f3");

    TypeDescriptor c3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);

    TypeDescriptor c4 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);

    // start to build inner struct
    TypeDescriptor c4_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    c4_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c4_struct.field_names.emplace_back("e1");

    c4_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c4_struct.field_names.emplace_back("e2");
    // end to build inner struct

    c4.children.emplace_back(c4_struct);

    TypeDescriptor B1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);

    SlotDesc slot_descs[] = {
            {"c1", c1}, {"c2", c2}, {"c3", c3}, {"c4", c4}, {"B1", B1}, {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c3, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c4, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(B1, true), chunk->num_columns());

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

    TypeDescriptor c1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2.field_names.emplace_back("f1");

    TypeDescriptor f3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("f3");

    TypeDescriptor c3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);

    TypeDescriptor c4 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);

    // start to build inner struct
    // dont't load subfield e1
    TypeDescriptor c4_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    c4_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c4_struct.field_names.emplace_back("e2");
    // end to build inner struct

    c4.children.emplace_back(c4_struct);

    TypeDescriptor B1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);

    SlotDesc slot_descs[] = {
            {"c1", c1}, {"c2", c2}, {"c3", c3}, {"c4", c4}, {"B1", B1}, {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c3, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c4, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(B1, true), chunk->num_columns());

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

    TypeDescriptor c1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2.field_names.emplace_back("f1");

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c2.field_names.emplace_back("f2");

    TypeDescriptor f3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("f3");

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c2.field_names.emplace_back("not_existed");

    SlotDesc slot_descs[] = {
            {"c1", c1},
            {"c2", c2},
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c2, true), chunk->num_columns());

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
    auto file = _create_file(_file4_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file4_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor c1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2.field_names.emplace_back("F1");

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c2.field_names.emplace_back("F2");

    TypeDescriptor f3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("F3");

    SlotDesc slot_descs[] = {{"c1", c1}, {"c2", c2}, {""}};
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file4_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.get_error_msg() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c2, true), chunk->num_columns());

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

    TypeDescriptor c1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor c2 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2.field_names.emplace_back("F1");

    c2.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    c2.field_names.emplace_back("F2");

    TypeDescriptor f3 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    f3.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    c2.children.emplace_back(f3);
    c2.field_names.emplace_back("F3");

    SlotDesc slot_descs[] = {{"c1", c1}, {"c2", c2}, {""}};
    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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

    TypeDescriptor c0 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor c1 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);

    c1.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c1.field_names.emplace_back("c1_0");

    c1.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY));
    c1.field_names.emplace_back("c1_1");

    c1.children.at(1).children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor c2 = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    TypeDescriptor c2_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    c2_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    c2_struct.field_names.emplace_back("c2_0");
    c2_struct.field_names.emplace_back("c2_1");
    c2.children.emplace_back(c2_struct);

    SlotDesc slot_descs[] = {{"c0", c0}, {"c1", c1}, {"c2", c2}, {""}};

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_struct_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.get_error_msg() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(c0, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c1, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(c2, true), chunk->num_columns());

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

TEST_F(FileReaderTest, TestReadMapColumnWithPartialMaterialize) {
    auto file = _create_file(_file_map_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_map_path));

    //init
    auto* ctx = _create_file_map_partial_materialize_context();

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // c1, c2.key, c3.value.key, c4.value
    EXPECT_EQ(ranges.size(), 4);

    EXPECT_EQ(file_reader->_file_metadata->num_rows(), 8);
    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::INVALID_TYPE));

    TypeDescriptor type_map_map(PrimitiveType::TYPE_MAP);
    type_map_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::INVALID_TYPE));
    type_map_map.children.emplace_back(type_map);

    TypeDescriptor type_array(PrimitiveType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    TypeDescriptor type_map_array(PrimitiveType::TYPE_MAP);
    type_map_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::INVALID_TYPE));
    type_map_array.children.emplace_back(type_array);

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    auto c = vectorized::ColumnHelper::create_column(type_map, true);
    chunk->append_column(c, chunk->num_columns());
    auto c_map_map = vectorized::ColumnHelper::create_column(type_map_map, true);
    chunk->append_column(c_map_map, chunk->num_columns());
    auto c_map_array = vectorized::ColumnHelper::create_column(type_map_array, true);
    chunk->append_column(c_map_array, chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(chunk->num_rows(), 8);
    for (int i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "row" << i << ": " << chunk->debug_row(i) << std::endl;
    }
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
    auto file = _create_file(_file_col_not_null_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file_col_not_null_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();

    TypeDescriptor type_int = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor type_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_struct.field_names.emplace_back("a");

    type_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_struct.field_names.emplace_back("b");

    SlotDesc slot_descs[] = {
            {"col_int", type_int},
            {"col_map", type_map},
            {"col_struct", type_struct},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_map, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_struct, true), chunk->num_columns());

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

    TypeDescriptor type_int = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor type_array = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);

    TypeDescriptor type_array_array = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);
    type_array_array.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    type_array.children.emplace_back(type_array_array);

    SlotDesc slot_descs[] = {
            {"id", type_int},
            {"b", type_array},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_array, true), chunk->num_columns());

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

    TypeDescriptor type_int = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    SlotDesc slot_descs[] = {
            {"uuid", type_int},
            {"c1", type_map},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_map_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_map, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, chunk->num_rows());

    EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
    EXPECT_EQ("[2, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[3, NULL]", chunk->debug_row(2));
}

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

    TypeDescriptor type_string = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);

    TypeDescriptor type_map(PrimitiveType::TYPE_MAP);
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_map.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    TypeDescriptor type_array_map(PrimitiveType::TYPE_ARRAY);
    type_array_map.children.emplace_back(type_map);

    SlotDesc slot_descs[] = {
            {"uuid", type_string},
            {"col_array_map", type_array_map},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_array_map_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_string, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_array_map, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(2, chunk->num_rows());

    EXPECT_EQ("['0', [{'def':11,'abc':10},{'ghi':12},{'jkl':13}]]", chunk->debug_row(0));
    EXPECT_EQ("['1', [{'happy new year':11,'hello world':10},{'vary happy':12},{'ok':13}]]", chunk->debug_row(1));
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

        TypeDescriptor type_int = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

        TypeDescriptor type_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
        type_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
        type_struct.field_names.emplace_back("a");

        TypeDescriptor type_array = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);

        TypeDescriptor type_array_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
        type_array_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
        type_array_struct.field_names.emplace_back("c");
        type_array_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
        type_array_struct.field_names.emplace_back("d");

        type_array.children.emplace_back(type_array_struct);

        type_struct.children.emplace_back(type_array);
        type_struct.field_names.emplace_back("b");

        SlotDesc slot_descs[] = {
                {"id", type_int},
                {"col", type_struct},
                {""},
        };

        ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<vectorized::Chunk>();
        chunk->append_column(vectorized::ColumnHelper::create_column(type_int, true), chunk->num_columns());
        chunk->append_column(vectorized::ColumnHelper::create_column(type_struct, true), chunk->num_columns());

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

        TypeDescriptor type_int = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

        TypeDescriptor type_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
        type_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
        type_struct.field_names.emplace_back("a");

        TypeDescriptor type_array = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);

        TypeDescriptor type_array_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
        type_array_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
        type_array_struct.field_names.emplace_back("c");
        type_array_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
        type_array_struct.field_names.emplace_back("d");

        type_array.children.emplace_back(type_array_struct);

        type_struct.children.emplace_back(type_array);
        type_struct.field_names.emplace_back("b");

        SlotDesc slot_descs[] = {
                {"id", type_int},
                {"col", type_struct},
                {""},
        };

        ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<vectorized::Chunk>();
        chunk->append_column(vectorized::ColumnHelper::create_column(type_int, true), chunk->num_columns());
        chunk->append_column(vectorized::ColumnHelper::create_column(type_struct, true), chunk->num_columns());

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

    TypeDescriptor type_int = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor type_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    type_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_struct.field_names.emplace_back("a");

    TypeDescriptor type_array = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY);

    TypeDescriptor type_array_struct = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    type_array_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_array_struct.field_names.emplace_back("c");
    type_array_struct.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR));
    type_array_struct.field_names.emplace_back("d");

    type_array.children.emplace_back(type_array_struct);

    type_struct.children.emplace_back(type_array);
    type_struct.field_names.emplace_back("b");

    SlotDesc slot_descs[] = {
            {"id", type_int},
            {"col", type_struct},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file_col_not_null_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_int, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_struct, true), chunk->num_columns());

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

    TypeDescriptor type_a = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor type_b = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    type_b.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_b.field_names.emplace_back("b1");
    type_b.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_b.field_names.emplace_back("b2");

    TypeDescriptor type_c = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_MAP);
    type_c.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_c.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    SlotDesc slot_descs[] = {
            {"a", type_a},
            {"b", type_b},
            {"c", type_c},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    _create_int_conjunct_ctxs(TExprOpcode::EQ, 0, 8000, &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 3);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_a, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_b, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_c, true), chunk->num_columns());

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

    TypeDescriptor type_a = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);

    TypeDescriptor type_b = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_STRUCT);
    type_b.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_b.field_names.emplace_back("b1");
    type_b.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_b.field_names.emplace_back("b2");

    TypeDescriptor type_c = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_MAP);
    type_c.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    type_c.children.emplace_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    SlotDesc slot_descs[] = {
            {"a", type_a},
            {"b", type_b},
            {"c", type_c},
            {""},
    };

    ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(filepath));

    _create_int_conjunct_ctxs(TExprOpcode::EQ, 0, 8000, &ctx->conjunct_ctxs_by_slot[0]);
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 3);

    auto chunk = std::make_shared<vectorized::Chunk>();
    chunk->append_column(vectorized::ColumnHelper::create_column(type_a, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_b, true), chunk->num_columns());
    chunk->append_column(vectorized::ColumnHelper::create_column(type_c, true), chunk->num_columns());

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

} // namespace starrocks::parquet
