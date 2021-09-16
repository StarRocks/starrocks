// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/parquet/file_reader.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "env/env.h"
#include "exec/parquet/column_chunk_reader.h"
#include "exec/parquet/metadata.h"
#include "exec/parquet/page_reader.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "exprs/vectorized/binary_predicate.h"
#include "runtime/descriptor_helper.h"

namespace starrocks::parquet {

static vectorized::HdfsScanStats g_hdfs_scan_stats;
using starrocks::vectorized::HdfsFileReaderParam;

// TODO: min/max conjunct
class FileReaderTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

private:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);

    HdfsFileReaderParam* _create_param();
    HdfsFileReaderParam* _create_param_for_partition();
    HdfsFileReaderParam* _create_param_for_not_exist();

    HdfsFileReaderParam* _create_file2_base_param();
    HdfsFileReaderParam* _create_param_for_min_max();
    HdfsFileReaderParam* _create_param_for_filter_file();
    HdfsFileReaderParam* _create_param_for_dict_filter();

    static vectorized::ChunkPtr _create_chunk();
    static vectorized::ChunkPtr _create_chunk_for_partition();
    static vectorized::ChunkPtr _create_chunk_for_not_exist();

    THdfsScanRange* _create_scan_range();

    void _create_conjunct_ctxs_for_min_max(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_filter_file(std::vector<ExprContext*>* conjunct_ctxs);
    void _create_conjunct_ctxs_for_dict_filter(std::vector<ExprContext*>* conjunct_ctxs);

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
    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    ObjectPool _pool;
};

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

std::unique_ptr<RandomAccessFile> FileReaderTest::_create_file(const std::string& file_path) {
    auto* env = Env::Default();
    std::unique_ptr<RandomAccessFile> file;
    env->new_random_access_file(file_path, &file);
    return file;
}

HdfsFileReaderParam* FileReaderTest::_create_param() {
    auto* param = _pool.add(new HdfsFileReaderParam());

    HdfsFileReaderParam::ColumnInfo c1;
    c1.col_name = "c1";
    c1.col_idx = 0;
    c1.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);
    c1.slot_id = 0;

    HdfsFileReaderParam::ColumnInfo c2;
    c2.col_name = "c2";
    c2.col_idx = 1;
    c2.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT);
    c2.slot_id = 1;

    HdfsFileReaderParam::ColumnInfo c3;
    c3.col_name = "c3";
    c3.col_idx = 2;
    c3.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);
    c3.slot_id = 2;

    HdfsFileReaderParam::ColumnInfo c4;
    c4.col_name = "c4";
    c4.col_idx = 3;
    c4.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME);
    c4.slot_id = 3;

    param->materialized_columns.emplace_back(c1);
    param->materialized_columns.emplace_back(c2);
    param->materialized_columns.emplace_back(c3);
    param->materialized_columns.emplace_back(c4);

    param->scan_ranges.emplace_back(_create_scan_range());
    param->stats = &g_hdfs_scan_stats;

    return param;
}

HdfsFileReaderParam* FileReaderTest::_create_param_for_partition() {
    auto* param = _pool.add(new HdfsFileReaderParam());

    HdfsFileReaderParam::ColumnInfo c1;
    c1.col_name = "c5";
    c1.col_idx = 0;
    c1.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);
    c1.slot_id = 0;
    param->partition_columns.emplace_back(c1);

    auto column = vectorized::ColumnHelper::create_const_column<PrimitiveType::TYPE_INT>(1, 1);
    param->partition_values.emplace_back(column);

    param->scan_ranges.emplace_back(_create_scan_range());
    param->stats = &g_hdfs_scan_stats;

    return param;
}

HdfsFileReaderParam* FileReaderTest::_create_param_for_not_exist() {
    auto* param = _pool.add(new HdfsFileReaderParam());

    HdfsFileReaderParam::ColumnInfo c1;
    c1.col_name = "c5";
    c1.col_idx = 0;
    c1.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);
    c1.slot_id = 0;
    param->materialized_columns.emplace_back(c1);

    param->scan_ranges.emplace_back(_create_scan_range());
    param->stats = &g_hdfs_scan_stats;

    return param;
}

HdfsFileReaderParam* FileReaderTest::_create_file2_base_param() {
    auto* param = _pool.add(new HdfsFileReaderParam());

    // tuple desc and conjuncts
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(PrimitiveType::TYPE_INT)
                         .column_name("c1")
                         .column_pos(0)
                         .nullable(true)
                         .id(0)
                         .build();
    auto slot2 = slot_desc_builder.type(PrimitiveType::TYPE_BIGINT)
                         .column_name("c2")
                         .column_pos(1)
                         .nullable(true)
                         .id(1)
                         .build();
    auto slot3 = slot_desc_builder.string_type(255).column_name("c3").column_pos(2).nullable(true).id(2).build();
    auto slot4 = slot_desc_builder.type(PrimitiveType::TYPE_DATETIME)
                         .column_name("c4")
                         .column_pos(3)
                         .nullable(true)
                         .id(3)
                         .build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.add_slot(slot4);
    tuple_desc_builder.build(&table_desc_builder);
    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(&_pool, table_desc_builder.desc_tbl(), &tbl);
    _row_desc = std::make_shared<RowDescriptor>(*tbl, row_tuples, nullable_tuples);
    auto* tuple_desc = _row_desc->tuple_descriptors()[0];
    param->tuple_desc = tuple_desc;

    // materialized columns
    HdfsFileReaderParam::ColumnInfo c1;
    c1.col_name = "c1";
    c1.col_idx = 0;
    c1.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);
    c1.slot_id = 0;

    HdfsFileReaderParam::ColumnInfo c2;
    c2.col_name = "c2";
    c2.col_idx = 1;
    c2.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT);
    c2.slot_id = 1;

    HdfsFileReaderParam::ColumnInfo c3;
    c3.col_name = "c3";
    c3.col_idx = 2;
    c3.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR);
    c3.slot_id = 2;

    HdfsFileReaderParam::ColumnInfo c4;
    c4.col_name = "c4";
    c4.col_idx = 3;
    c4.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME);
    c4.slot_id = 3;

    param->materialized_columns.emplace_back(c1);
    param->materialized_columns.emplace_back(c2);
    param->materialized_columns.emplace_back(c3);
    param->materialized_columns.emplace_back(c4);

    // scan range
    auto* scan_range = _pool.add(new THdfsScanRange());
    scan_range->relative_path = _file_2_path;
    scan_range->offset = 4;
    scan_range->length = 850;
    scan_range->file_length = _file_2_size;
    param->scan_ranges.emplace_back(scan_range);

    param->timezone = "Asia/Shanghai";
    param->stats = &g_hdfs_scan_stats;

    return param;
}

HdfsFileReaderParam* FileReaderTest::_create_param_for_min_max() {
    auto* param = _create_file2_base_param();

    // create min max tuple desc
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot_desc = slot_desc_builder.type(PrimitiveType::TYPE_INT)
                             .column_name("c1")
                             .column_pos(0)
                             .nullable(true)
                             .id(0)
                             .build();

    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot_desc);
    tuple_desc_builder.build(&table_desc_builder);

    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(&_pool, table_desc_builder.desc_tbl(), &tbl);
    _row_desc = std::make_shared<RowDescriptor>(*tbl, row_tuples, nullable_tuples);
    auto* tuple_desc = _row_desc->tuple_descriptors()[0];
    param->min_max_tuple_desc = tuple_desc;

    // create min max conjuncts
    // c1 >= 1
    _create_conjunct_ctxs_for_min_max(&param->min_max_conjunct_ctxs);

    return param;
}

HdfsFileReaderParam* FileReaderTest::_create_param_for_filter_file() {
    auto* param = _create_file2_base_param();
    // add c5 int slot
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(PrimitiveType::TYPE_INT)
                         .column_name("c1")
                         .column_pos(0)
                         .nullable(true)
                         .id(0)
                         .build();
    auto slot2 = slot_desc_builder.type(PrimitiveType::TYPE_BIGINT)
                         .column_name("c2")
                         .column_pos(1)
                         .nullable(true)
                         .id(1)
                         .build();
    auto slot3 = slot_desc_builder.string_type(255).column_name("c3").column_pos(2).nullable(true).id(2).build();
    auto slot4 = slot_desc_builder.type(PrimitiveType::TYPE_DATETIME)
                         .column_name("c4")
                         .column_pos(3)
                         .nullable(true)
                         .id(3)
                         .build();
    auto slot5 = slot_desc_builder.type(PrimitiveType::TYPE_INT)
                         .column_name("c5")
                         .column_pos(4)
                         .nullable(true)
                         .id(4)
                         .build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.add_slot(slot2);
    tuple_desc_builder.add_slot(slot3);
    tuple_desc_builder.add_slot(slot4);
    tuple_desc_builder.add_slot(slot5);
    tuple_desc_builder.build(&table_desc_builder);
    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(&_pool, table_desc_builder.desc_tbl(), &tbl);
    _row_desc = std::make_shared<RowDescriptor>(*tbl, row_tuples, nullable_tuples);
    auto* tuple_desc = _row_desc->tuple_descriptors()[0];
    param->tuple_desc = tuple_desc;

    // add c5 to materialized columns
    HdfsFileReaderParam::ColumnInfo c5;
    c5.col_name = "c5";
    c5.col_idx = 4;
    c5.col_type = TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT);
    c5.slot_id = 4;
    param->materialized_columns.emplace_back(c5);

    // create conjuncts
    // c5 >= 1
    param->conjunct_ctxs_by_slot[4] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_filter_file(&param->conjunct_ctxs_by_slot[4]);
    return param;
}

HdfsFileReaderParam* FileReaderTest::_create_param_for_dict_filter() {
    auto* param = _create_file2_base_param();
    // create conjuncts
    // c3 = "c"
    param->conjunct_ctxs_by_slot[2] = std::vector<ExprContext*>();
    _create_conjunct_ctxs_for_dict_filter(&param->conjunct_ctxs_by_slot[2]);
    return param;
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
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_size);

    // init
    auto* param = _create_param();
    Status status = file_reader->init(*param);
    ASSERT_TRUE(status.ok());
}

TEST_F(FileReaderTest, TestGetNext) {
    // create file
    auto file = _create_file(_file_path);

    // create file reader
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_size);

    // init
    auto* param = _create_param();
    Status status = file_reader->init(*param);
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
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_size);

    // init
    auto* param = _create_param_for_partition();
    Status status = file_reader->init(*param);
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
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_size);

    // init
    auto* param = _create_param_for_not_exist();
    Status status = file_reader->init(*param);
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
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_2_size);

    // init
    auto* param = _create_param_for_min_max();
    Status status = file_reader->init(*param);
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
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_2_size);

    // init
    auto* param = _create_param_for_filter_file();
    Status status = file_reader->init(*param);
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
    auto file_reader = std::make_shared<FileReader>(file.get(), _file_2_size);

    // init
    auto* param = _create_param_for_dict_filter();
    Status status = file_reader->init(*param);
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

} // namespace starrocks::parquet
