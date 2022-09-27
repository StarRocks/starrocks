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
#include "runtime/descriptor_helper.h"

namespace starrocks::parquet {

static vectorized::HdfsScanStats g_hdfs_scan_stats;
using starrocks::vectorized::HdfsScannerContext;

// TODO: min/max conjunct
class FileReaderTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

private:
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
    HdfsScannerContext* _create_context_for_struct_solumn();

    HdfsScannerContext* _create_file5_base_context();

    void _create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                   std::vector<ExprContext*>* conjunct_ctxs);
    void _create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                      std::vector<ExprContext*>* conjunct_ctxs);

    static vectorized::ChunkPtr _create_chunk();
    static vectorized::ChunkPtr _create_struct_chunk();
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
    // c1      c2      c3      c4
    // -------------------------------------------
    // 0       10      a       2022-07-12 03:52:14
    // 1       11      a       2022-07-12 03:52:14
    // 2       12      a       2022-07-12 03:52:14
    // 3       13      c       2022-07-12 03:52:14
    // 4       14      c       2022-07-12 03:52:14
    // 5       15      c       2022-07-12 03:52:14
    // 6       16      a       2022-07-12 03:52:14
    // ...    ...     ...      ...
    // 4092   4102     a       2022-07-12 03:52:14
    // 4093   4103     a       2022-07-12 03:52:14
    // 4094   4104     a       2022-07-12 03:52:14
    // 4095   4105     a       2022-07-12 03:52:14
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

    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    ObjectPool _pool;
};

struct SlotDesc {
    string name;
    TypeDescriptor type;
};

TupleDescriptor* create_tuple_descriptor(ObjectPool* pool, const SlotDesc* slot_descs) {
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
    DescriptorTbl::create(pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);

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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
    ctx->min_max_tuple_desc = create_tuple_descriptor(&_pool, min_max_slots);

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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
            {""},
    };
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
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
    ctx->tuple_desc = create_tuple_descriptor(&_pool, slot_descs);
    make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(_file5_path));

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_struct_solumn() {
    auto* ctx = _create_file4_base_context();
    // create conjuncts
    // c3 = "c", c2 is not in slots, so the slot_id=1
    _create_string_conjunct_ctxs(TExprOpcode::EQ, 1, "c", &ctx->conjunct_ctxs_by_slot[1]);
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

vectorized::ChunkPtr FileReaderTest::_create_struct_chunk() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _append_column_for_chunk(PrimitiveType::TYPE_INT, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
    _append_column_for_chunk(PrimitiveType::TYPE_VARCHAR, &chunk);
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
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_columns.size());
    ASSERT_EQ(2, file_reader->_row_group_readers[0]->_dict_filter_columns[0].slot_id);

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
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_columns.size());
    ASSERT_EQ(2, file_reader->_row_group_readers[0]->_dict_filter_columns[0].slot_id);

    // c0 is conjunct filter column
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_left_conjunct_ctxs.size());
    const auto& conjunct_ctxs_by_slot = file_reader->_row_group_readers[0]->_param.conjunct_ctxs_by_slot;
    ASSERT_NE(conjunct_ctxs_by_slot.find(0), conjunct_ctxs_by_slot.end());

    // get next
    auto chunk = _create_chunk();
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
        auto chunk = _create_chunk();
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
    auto* ctx = _create_context_for_struct_solumn();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_columns.size());
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_columns[0].slot_id);

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

TEST_F(FileReaderTest, TestReadArray2dColumn) {
    auto file = _create_file(_file5_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(_file5_path));

    //init
    auto* ctx = _create_file5_base_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<SharedBufferedInputStream::IORange> ranges;
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
    EXPECT_EQ(chunk->debug_row(0), "[1, [[1, 2]]]");
    EXPECT_EQ(chunk->debug_row(1), "[2, [[1, 2], [3, 4]]]");
    EXPECT_EQ(chunk->debug_row(2), "[3, [[1, 2, 3], [4]]]");
    EXPECT_EQ(chunk->debug_row(3), "[4, [[1, 2, 3], [4], [5]]]");
    EXPECT_EQ(chunk->debug_row(4), "[5, [[1, 2, 3], [4, 5]]]");
}

} // namespace starrocks::parquet
