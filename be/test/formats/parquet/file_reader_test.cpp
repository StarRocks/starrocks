// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "formats/parquet/file_reader.h"

#include <gtest/gtest.h>

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

    HdfsScannerContext* _create_context();
    HdfsScannerContext* _create_context_for_partition();
    HdfsScannerContext* _create_context_for_not_exist();

    HdfsScannerContext* _create_file2_base_context();
    HdfsScannerContext* _create_context_for_min_max();
    HdfsScannerContext* _create_context_for_filter_file();
    HdfsScannerContext* _create_context_for_dict_filter();
    HdfsScannerContext* _create_context_for_other_filter();
    HdfsScannerContext* _create_context_for_skip_group();

    HdfsScannerContext* _create_file3_base_context();
    HdfsScannerContext* _create_context_for_multi_filter(HdfsScannerContext* base_ctx);
    HdfsScannerContext* _create_context_for_multi_page(HdfsScannerContext* base_ctx);

    static vectorized::ChunkPtr _create_chunk();
    static vectorized::ChunkPtr _create_chunk_for_partition();
    static vectorized::ChunkPtr _create_chunk_for_not_exist();

    THdfsScanRange* _create_scan_range();

    void _create_conjunct_ctxs_for_min_max(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_filter_file(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_dict_filter(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_other_filter(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_multi_page(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_skip_group(std::vector<ExprContext*>* conjunct_ctxs);

    // c1      c2      c3       c4
    // -------------------------------------------
    // NULL    NULL    NULL    NULL
    // NULL    NULL    NULL    NULL
    // NULL    NULL    NULL    NULL
    // NULL    NULL    NULL    NULL
    std::string _file_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet1";
    int64_t _file_size = 729;

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
    std::string _file_2_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet2";
    int64_t _file_2_size = 850;

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
    // a parquet file with multiple page
    std::string _file_3_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet3";
    int64_t _file_3_size = 46654;

    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    ObjectPool _pool;
};

struct SlotDesc {
    string name;
    TypeDescriptor type;
};

void create_tuple_descriptor(ObjectPool* pool, const SlotDesc* slot_descs, TupleDescriptor** tuple_desc) {
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
    *tuple_desc = row_desc->tuple_descriptors()[0];
    return;
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

void FileReaderTest::_create_conjunct_ctxs_for_min_max(std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::GE;
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
    t_slot_ref.slot_id = 0;
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
    int_literal.value = 1;
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

void FileReaderTest::_create_conjunct_ctxs_for_filter_file(std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::GE;
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
    t_slot_ref.slot_id = 4;
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
    int_literal.value = 1;
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

void FileReaderTest::_create_conjunct_ctxs_for_dict_filter(std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::EQ;
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
    t_slot_ref.slot_id = 2;
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
    string_literal.value = "c";
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

void FileReaderTest::_create_conjunct_ctxs_for_other_filter(std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::GE;
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
    t_slot_ref.slot_id = 0;
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
    int_literal.value = 4;
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

void FileReaderTest::_create_conjunct_ctxs_for_multi_page(std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::GE;
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
    t_slot_ref.slot_id = 0;
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
    int_literal.value = 4080;
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

void FileReaderTest::_create_conjunct_ctxs_for_skip_group(std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::BINARY_PRED;
    node0.opcode = TExprOpcode::GE;
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
    t_slot_ref.slot_id = 0;
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
    int_literal.value = 100000;
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

std::unique_ptr<RandomAccessFile> FileReaderTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

HdfsScannerContext* FileReaderTest::_create_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());

    TupleDescriptor* tuple_desc;
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range());
    ctx->stats = &g_hdfs_scan_stats;

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_partition() {
    auto* ctx = _pool.add(new HdfsScannerContext());

    TupleDescriptor* tuple_desc;
    SlotDesc slot_descs[] = {
            // {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            // {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &ctx->partition_columns);
    auto column = vectorized::ColumnHelper::create_const_column<PrimitiveType::TYPE_INT>(1, 1);
    ctx->partition_values.emplace_back(column);

    ctx->scan_ranges.emplace_back(_create_scan_range());
    ctx->stats = &g_hdfs_scan_stats;

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_not_exist() {
    auto* ctx = _pool.add(new HdfsScannerContext());

    TupleDescriptor* tuple_desc;
    SlotDesc slot_descs[] = {
            // {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            // {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            // {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            // {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {"c5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &ctx->materialized_columns);

    ctx->scan_ranges.emplace_back(_create_scan_range());
    ctx->stats = &g_hdfs_scan_stats;

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file2_base_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());

    // tuple desc and conjuncts
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };
    TupleDescriptor* tuple_desc;
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &ctx->materialized_columns);

    // scan range
    auto* scan_range = _pool.add(new THdfsScanRange());
    scan_range->relative_path = _file_2_path;
    scan_range->offset = 4;
    scan_range->length = 850;
    scan_range->file_length = _file_2_size;
    ctx->scan_ranges.emplace_back(scan_range);

    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_min_max() {
    auto* ctx = _create_file2_base_context();

    SlotDesc min_max_slots[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {""},
    };
    create_tuple_descriptor(&_pool, min_max_slots, &ctx->min_max_tuple_desc);

    // create min max conjuncts
    // c1 >= 1
    _create_conjunct_ctxs_for_min_max(&ctx->min_max_conjunct_ctxs);

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

    TupleDescriptor* tuple_desc;
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &ctx->materialized_columns);

    // create conjuncts
    // c5 >= 1
    ctx->conjunct_ctxs_by_slot[4] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_filter_file(&ctx->conjunct_ctxs_by_slot[4]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_dict_filter() {
    auto* ctx = _create_file2_base_context();
    // create conjuncts
    // c3 = "c"
    ctx->conjunct_ctxs_by_slot[2] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_dict_filter(&ctx->conjunct_ctxs_by_slot[2]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_other_filter() {
    auto* ctx = _create_file2_base_context();
    // create conjuncts
    // c1 >= 4
    ctx->conjunct_ctxs_by_slot[0] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_other_filter(&ctx->conjunct_ctxs_by_slot[0]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_skip_group() {
    auto* ctx = _create_file2_base_context();
    // create conjuncts
    // c1 > 10000
    ctx->conjunct_ctxs_by_slot[0] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_skip_group(&ctx->conjunct_ctxs_by_slot[0]);
    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_file3_base_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());

    // tuple desc and conjuncts
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };
    TupleDescriptor* tuple_desc;
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &ctx->materialized_columns);

    // scan range
    auto* scan_range = _pool.add(new THdfsScanRange());
    scan_range->relative_path = _file_3_path;
    scan_range->offset = 4;
    scan_range->length = _file_3_size;
    scan_range->file_length = _file_3_size;
    ctx->scan_ranges.emplace_back(scan_range);

    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;

    return ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_multi_filter(HdfsScannerContext* base_ctx) {
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };

    TupleDescriptor* tuple_desc;
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    base_ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &base_ctx->materialized_columns);

    base_ctx->conjunct_ctxs_by_slot[2] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_dict_filter(&base_ctx->conjunct_ctxs_by_slot[2]);

    base_ctx->conjunct_ctxs_by_slot[0] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_other_filter(&base_ctx->conjunct_ctxs_by_slot[0]);

    return base_ctx;
}

HdfsScannerContext* FileReaderTest::_create_context_for_multi_page(HdfsScannerContext* base_ctx) {
    SlotDesc slot_descs[] = {
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
            {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
            {""},
    };

    TupleDescriptor* tuple_desc;
    create_tuple_descriptor(&_pool, slot_descs, &tuple_desc);
    base_ctx->tuple_desc = tuple_desc;
    make_column_info_vector(tuple_desc, &base_ctx->materialized_columns);

    base_ctx->conjunct_ctxs_by_slot[0] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_multi_page(&base_ctx->conjunct_ctxs_by_slot[0]);

    return base_ctx;
}

THdfsScanRange* FileReaderTest::_create_scan_range() {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = _file_path;
    scan_range->offset = 4;
    scan_range->length = 1024;
    scan_range->file_length = _file_size;

    return scan_range;
}

vectorized::ChunkPtr FileReaderTest::_create_chunk() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    auto c1 =
            vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT), true);
    auto c2 = vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT),
                                                      true);
    auto c3 = vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR),
                                                      true);
    auto c4 = vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME),
                                                      true);
    chunk->append_column(c1, 0);
    chunk->append_column(c2, 1);
    chunk->append_column(c3, 2);
    chunk->append_column(c4, 3);

    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_chunk_for_partition() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    auto c1 =
            vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT), true);
    chunk->append_column(c1, 0);
    return chunk;
}

vectorized::ChunkPtr FileReaderTest::_create_chunk_for_not_exist() {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    auto c1 =
            vectorized::ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT), true);
    chunk->append_column(c1, 0);
    return chunk;
}

TEST_F(FileReaderTest, TestInit) {
    // create file
    auto file = _create_file(_file_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_size);

    // init
    auto* ctx = _create_context();
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());
}

TEST_F(FileReaderTest, TestGetNext) {
    // create file
    auto file = _create_file(_file_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_size);

    // init
    auto* ctx = _create_context();
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
    // create file
    auto file = _create_file(_file_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_size);

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
    // create file
    auto file = _create_file(_file_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_size);

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
    // create file
    auto file = _create_file(_file_2_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_2_size);

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
    // create file
    auto file = _create_file(_file_2_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_2_size);

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
    // create file
    auto file = _create_file(_file_2_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_2_size);

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
    // create file
    auto file = _create_file(_file_2_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_2_size);

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

TEST_F(FileReaderTest, TestMultiFilter) {
    // create file
    auto file = _create_file(_file_2_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_2_size);

    // c3 = "c", c1 >= 4
    auto* ctx = _create_context_for_multi_filter(_create_file2_base_context());
    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // c3 is dict filter column
    ASSERT_EQ(1, file_reader->_row_group_readers[0]->_dict_filter_columns.size());
    ASSERT_EQ(2, file_reader->_row_group_readers[0]->_dict_filter_columns[0].slot_id);

    // c0 is other conjunct filter column
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
    ASSERT_TRUE(status.is_end_of_file());
}

TEST_F(FileReaderTest, TestMultiFilterWithMultiPage) {
    // create file
    auto file = _create_file(_file_3_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_3_size);

    // c3 = "c", c1 >= 4
    auto* ctx = _create_context_for_multi_filter(_create_file3_base_context());
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
    // create file
    auto file = _create_file(_file_3_path);

    // create file reader
    config::vector_chunk_size = 1024;
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_3_size);

    // c1 >= 4080
    auto* ctx = _create_context_for_multi_page(_create_file3_base_context());
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

TEST_F(FileReaderTest, TestSkipRowGroup) {
    // create file
    auto file = _create_file(_file_2_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), _file_2_size);

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

} // namespace starrocks::parquet
