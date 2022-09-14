// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/hdfs_scanner.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "exec/vectorized/hdfs_scanner_orc.h"
#include "exec/vectorized/hdfs_scanner_parquet.h"
#include "exec/vectorized/hdfs_scanner_text.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"

namespace starrocks::vectorized {

struct SlotDesc {
    string name;
    TypeDescriptor type;
};

// TODO: partition scan
class HdfsScannerTest : public ::testing::Test {
public:
    void SetUp() override {
        _create_runtime_profile();
        _create_runtime_state("");
    }
    void TearDown() override {}

    //private:
protected:
    void _create_runtime_state(const std::string& timezone);
    void _create_runtime_profile();
    HdfsScannerParams* _create_param(const std::string& file, THdfsScanRange* range, const TupleDescriptor* tuple_desc);
    void build_hive_column_names(HdfsScannerParams* params, const TupleDescriptor* tuple_desc);

    THdfsScanRange* _create_scan_range(const std::string& file, uint64_t offset, uint64_t length);
    TupleDescriptor* _create_tuple_desc(SlotDesc* descs);

    ObjectPool _pool;
    RuntimeProfile* _runtime_profile = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
};

void HdfsScannerTest::_create_runtime_profile() {
    _runtime_profile = _pool.add(new RuntimeProfile("test"));
    _runtime_profile->set_metadata(1);
}

void HdfsScannerTest::_create_runtime_state(const std::string& timezone) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    if (timezone != "") {
        query_globals.__set_time_zone(timezone);
    }
    _runtime_state = _pool.add(new RuntimeState(fragment_id, query_options, query_globals, nullptr));
    _runtime_state->init_instance_mem_tracker();
}

THdfsScanRange* HdfsScannerTest::_create_scan_range(const std::string& file, uint64_t offset, uint64_t length) {
    auto* scan_range = _pool.add(new THdfsScanRange());
    ASSIGN_OR_ABORT(uint64_t file_size, FileSystem::Default()->get_file_size(file));
    scan_range->relative_path = file;
    scan_range->offset = offset;
    scan_range->length = length == 0 ? file_size : length;
    scan_range->file_length = file_size;
    scan_range->text_file_desc.field_delim = ",";
    scan_range->text_file_desc.line_delim = "\n";
    scan_range->text_file_desc.collection_delim = "\003";
    scan_range->text_file_desc.mapkey_delim = "\004";
    return scan_range;
}

HdfsScannerParams* HdfsScannerTest::_create_param(const std::string& file, THdfsScanRange* range,
                                                  TupleDescriptor* tuple_desc) {
    auto* param = _pool.add(new HdfsScannerParams());
    param->fs = FileSystem::Default();
    param->path = file;
    param->scan_ranges.emplace_back(range);
    param->tuple_desc = tuple_desc;
    std::vector<int> materialize_index_in_chunk;
    std::vector<int> partition_index_in_chunk;
    std::vector<SlotDescriptor*> mat_slots;
    std::vector<SlotDescriptor*> part_slots;

    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = tuple_desc->slots()[i];
        if (slot->col_name().find("PART_") != std::string::npos) {
            partition_index_in_chunk.push_back(i);
            part_slots.push_back(slot);
        } else {
            materialize_index_in_chunk.push_back(i);
            mat_slots.push_back(slot);
        }
    }

    param->partition_index_in_chunk = partition_index_in_chunk;
    param->materialize_index_in_chunk = materialize_index_in_chunk;
    param->materialize_slots = mat_slots;
    param->partition_slots = part_slots;
    return param;
}

void HdfsScannerTest::build_hive_column_names(HdfsScannerParams* params, const TupleDescriptor* tuple_desc) {
    std::vector<std::string>* hive_column_names = _pool.add(new std::vector<std::string>());
    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = tuple_desc->slots()[i];
        hive_column_names->emplace_back(slot->col_name());
    }
    params->hive_column_names = hive_column_names;
}

TupleDescriptor* HdfsScannerTest::_create_tuple_desc(SlotDesc* descs) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    int slot_id = 0;
    while (descs->name != "") {
        slot_desc_builder.column_name(descs->name).type(descs->type).id(slot_id).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        descs += 1;
        slot_id += 1;
    }
    tuple_desc_builder.build(&table_desc_builder);
    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(&_pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);
    _row_desc = std::make_shared<RowDescriptor>(*tbl, row_tuples, nullable_tuples);
    auto* tuple_desc = _row_desc->tuple_descriptors()[0];
    return tuple_desc;
}

// ========================= PARQUET SCANNER ============================

static SlotDesc default_parquet_descs[] = {
        {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
        {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR, 22)},
        {"col_varchar", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
        {""}};

std::string default_parquet_file = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet1";

TEST_F(HdfsScannerTest, TestParquetInit) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* param = _create_param(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    ASSERT_TRUE(status.ok());
}

TEST_F(HdfsScannerTest, TestParquetOpen) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* param = _create_param(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok());
}

TEST_F(HdfsScannerTest, TestParquetGetNext) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* param = _create_param(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok());

    auto chunk = vectorized::ChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(chunk->num_rows(), 4);

    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.is_end_of_file());

    scanner->close(_runtime_state);
}

// ========================= ORC SCANNER ============================

static TTypeDesc create_primitive_type_desc(TPrimitiveType::type type) {
    TTypeDesc result;
    TTypeNode node;
    node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar_type;
    scalar_type.__set_type(type);
    node.__set_scalar_type(scalar_type);
    result.types.push_back(node);
    return result;
}

static TExprNode create_int_literal_node(TPrimitiveType::type value_type, int64_t value_literal) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::INT_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TIntLiteral lit_value;
    lit_value.__set_value(value_literal);
    lit_node.__set_int_literal(lit_value);
    lit_node.__set_use_vectorized(true);
    return lit_node;
}

static TExprNode create_string_literal_node(TPrimitiveType::type value_type, const std::string& value_literal) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::STRING_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TStringLiteral lit_value;
    lit_value.__set_value(value_literal);
    lit_node.__set_string_literal(lit_value);
    lit_node.__set_use_vectorized(true);
    return lit_node;
}

static TExprNode create_datetime_literal_node(TPrimitiveType::type value_type, const std::string& value_literal) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::DATE_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TDateLiteral lit_value;
    lit_value.__set_value(value_literal);
    lit_node.__set_date_literal(lit_value);
    lit_node.__set_use_vectorized(true);
    return lit_node;
}

template <typename ValueType>
static void push_binary_pred_texpr_node(std::vector<TExprNode>& nodes, TExprOpcode::type opcode,
                                        SlotDescriptor* slot_desc, ValueType value_type, TExprNode lit_node) {
    TExprNode eq_node;
    eq_node.__set_node_type(TExprNodeType::type::BINARY_PRED);
    eq_node.__set_child_type(value_type);
    eq_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
    eq_node.__set_opcode(opcode);
    eq_node.__set_num_children(2);
    eq_node.__set_use_vectorized(true);

    TExprNode slot_node;
    slot_node.__set_node_type(TExprNodeType::SLOT_REF);
    slot_node.__set_num_children(0);
    slot_node.__set_type(create_primitive_type_desc(value_type));
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(slot_desc->id());
    slot_ref.__set_tuple_id(slot_desc->parent());
    slot_node.__set_slot_ref(slot_ref);
    slot_node.__set_use_vectorized(true);

    nodes.emplace_back(eq_node);
    nodes.emplace_back(slot_node);
    nodes.emplace_back(lit_node);
}

static ExprContext* create_expr_context(ObjectPool* pool, const std::vector<TExprNode>& nodes) {
    TExpr texpr;
    texpr.__set_nodes(nodes);
    ExprContext* ctx;
    Status st = Expr::create_expr_tree(pool, texpr, &ctx);
    DCHECK(st.ok()) << st.get_error_msg();
    return ctx;
}

static void extend_partition_values(ObjectPool* pool, HdfsScannerParams* params, const std::vector<int64_t>& values) {
    std::vector<int> positions;
    std::vector<ExprContext*> part_values;
    for (int i = 0; i < values.size(); i++) {
        int64_t v = values[i];
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, v);
        nodes.emplace_back(lit_node);
        ExprContext* ctx = create_expr_context(pool, nodes);
        part_values.push_back(ctx);
        positions.push_back(i);
    }
    params->_partition_index_in_hdfs_partition_columns = positions;
    params->partition_values = part_values;
}

#define READ_SCANNER_RETURN_ROWS(scanner, records)                                     \
    do {                                                                               \
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 0);                           \
        for (;;) {                                                                     \
            chunk->reset();                                                            \
            status = scanner->get_next(_runtime_state, &chunk);                        \
            if (status.is_end_of_file()) {                                             \
                break;                                                                 \
            }                                                                          \
            if (!status.ok()) {                                                        \
                std::cout << "status not ok: " << status.get_error_msg() << std::endl; \
                break;                                                                 \
            }                                                                          \
            if (chunk->num_rows() > 0) {                                               \
                std::cout << "row#0: " << chunk->debug_row(0) << std::endl;            \
                EXPECT_EQ(chunk->num_columns(), tuple_desc->slots().size());           \
            }                                                                          \
            records += chunk->num_rows();                                              \
        }                                                                              \
    } while (0)

#define READ_SCANNER_ROWS(scanner, exp)             \
    {                                               \
        uint64_t records = 0;                       \
        READ_SCANNER_RETURN_ROWS(scanner, records); \
        EXPECT_EQ(records, exp);                    \
    }

// ====================================================================================================

static SlotDesc mtypes_orc_descs[] = {
        {"id", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
        {"col_float", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_FLOAT)},
        {"col_double", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DOUBLE)},
        {"col_varchar", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col_char", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col_tinyint", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_TINYINT)},
        {"col_smallint", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_SMALLINT)},
        {"col_int", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {"col_bigint", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
        {"col_largeint", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
        {"col0_i32p7s2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i32p7s2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col0_i32p6s3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i32p6s3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col0_i64p7s2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i64p7s2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col0_i64p9s5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i64p9s5", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col0_i128p7s2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i128p7s2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col0_i128p18s9", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i128p18s9", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col0_i128p30s9", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"col1_i128p30s9", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"PART_x", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {"PART_y", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {""}};
std::string mtypes_orc_file = "./be/test/exec/test_data/orc_scanner/mtypes_100.orc.zlib";

static SlotDesc mtypes_orc_min_max_descs[] = {{"id", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
                                              {"PART_y", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
                                              {""}};

TEST_F(HdfsScannerTest, TestOrcGetNext) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* param = _create_param(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    extend_partition_values(&_pool, param, values);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close(_runtime_state);
}

static void extend_mtypes_orc_min_max_conjuncts(ObjectPool* pool, HdfsScannerParams* params,
                                                const std::vector<int>& values) {
    TupleDescriptor* min_max_tuple_desc = params->min_max_tuple_desc;

    // id >= values[0] && id <= values[1] && part_y >= values[2] && part_y <= values[3]
    // id min/max = 2629/5212
    // so there should be no row matched.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::BIGINT, values[0]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::BIGINT,
                                    lit_node);
        ExprContext* ctx = create_expr_context(pool, nodes);
        params->min_max_conjunct_ctxs.push_back(ctx);
    }

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::BIGINT, values[1]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[0], TPrimitiveType::BIGINT,
                                    lit_node);
        ExprContext* ctx = create_expr_context(pool, nodes);
        params->min_max_conjunct_ctxs.push_back(ctx);
    }

    // PART_Y is always 20
    // to test capability to read non-file column from partition column
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, values[2]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[1], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* ctx = create_expr_context(pool, nodes);
        params->min_max_conjunct_ctxs.push_back(ctx);
    }

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, values[3]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[1], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* ctx = create_expr_context(pool, nodes);
        params->min_max_conjunct_ctxs.push_back(ctx);
    }
}

TEST_F(HdfsScannerTest, TestOrcGetNextWithMinMaxFilterNoRows) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* param = _create_param(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    extend_partition_values(&_pool, param, values);

    auto* min_max_tuple_desc = _create_tuple_desc(mtypes_orc_min_max_descs);
    param->min_max_tuple_desc = min_max_tuple_desc;
    // id min/max = 2629/5212, PART_Y min/max=20/20
    std::vector<int> thres = {20, 30, 20, 20};
    extend_mtypes_orc_min_max_conjuncts(&_pool, param, thres);
    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 0);
    EXPECT_EQ(scanner->raw_rows_read(), 0);
    scanner->close(_runtime_state);
}

TEST_F(HdfsScannerTest, TestOrcGetNextWithMinMaxFilterRows1) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* param = _create_param(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    extend_partition_values(&_pool, param, values);

    auto* min_max_tuple_desc = _create_tuple_desc(mtypes_orc_min_max_descs);
    param->min_max_tuple_desc = min_max_tuple_desc;
    // id min/max = 2629/5212, PART_Y min/max=20/20
    std::vector<int> thres = {2000, 5000, 20, 20};
    extend_mtypes_orc_min_max_conjuncts(&_pool, param, thres);
    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close(_runtime_state);
}

TEST_F(HdfsScannerTest, TestOrcGetNextWithMinMaxFilterRows2) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* param = _create_param(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    extend_partition_values(&_pool, param, values);

    auto* min_max_tuple_desc = _create_tuple_desc(mtypes_orc_min_max_descs);
    param->min_max_tuple_desc = min_max_tuple_desc;
    // id min/max = 2629/5212, PART_Y min/max=20/20
    std::vector<int> thres = {3000, 10000, 20, 20};
    extend_mtypes_orc_min_max_conjuncts(&_pool, param, thres);
    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close(_runtime_state);
}

// ====================================================================================================

/**
File be/test/exec/test_data/orc_scanner/string_key_value_10k.orc.zstd has 2 stripes
*** Stripe 0 ***

--- Column 0 ---
Column has 5120 values and has null value: no

--- Column 1 ---
Data type: String
Values: 5120
Has null: no
Minimum: aaaaaaaaaa
Maximum: ffffffffff
Total length: 51200

*** Stripe 1 ***

--- Column 0 ---
Column has 4880 values and has null value: no

--- Column 1 ---
Data type: String
Values: 4880
Has null: no
Minimum: ffffffffff
Maximum: jjjjjjjjjj
Total length: 48800
 */

TEST_F(HdfsScannerTest, TestOrcGetNextWithDictFilter) {
    SlotDesc string_key_value_orc_desc[] = {{"key", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
                                            {"value", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
                                            {""}};
    const std::string string_key_value_orc_file = "./be/test/exec/test_data/orc_scanner/string_key_value_10k.orc.zstd";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(string_key_value_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(string_key_value_orc_desc);
    auto* param = _create_param(string_key_value_orc_file, range, tuple_desc);

    // all in stripe1
    // and there are 1000 occurrences.
    // but we will read 4880 rows.

    // key = "gggggggg"
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "gggggggggg");
        push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, tuple_desc->slots()[0], TPrimitiveType::VARCHAR, lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        std::cout << "equal expr = " << ctx->root()->debug_string() << std::endl;
        param->conjunct_ctxs_by_slot[0].push_back(ctx);
    }

    for (auto& it : param->conjunct_ctxs_by_slot) {
        for (auto& it2 : it.second) {
            ExprContext* ctx = it2;
            ctx->prepare(_runtime_state);
            ctx->open(_runtime_state);
        }
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    for (auto& it : param->conjunct_ctxs_by_slot) {
        for (auto& it2 : it.second) {
            it2->close(_runtime_state);
        }
    }

    // so stripe will not be filtered out by search argument
    // and we can test dict-filtering strategy.
    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1000);
    // since we use dict filter eval cache, we can do filter on orc cvb
    // so actually read rows is 1000.
    EXPECT_EQ(scanner->raw_rows_read(), 1000);
    scanner->close(_runtime_state);
}

// ====================================================================================================

/**
 *
datetime are all in UTC timezone.

File Version: 0.12 with ORC_CPP_ORIGINAL
Rows: 20000
Compression: ZLIB
Compression size: 65536
Calendar: Julian/Gregorian
Type: struct<c0:timestamp,c1:varchar(60)>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 5120 hasNull: false
    Column 1: count: 5120 hasNull: false min: 2021-05-25 04:43:22.941 max: 2021-05-25 06:08:41.941999999
    Column 2: count: 5120 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 307200
  Stripe 2:
    Column 0: count: 5120 hasNull: false
    Column 1: count: 5120 hasNull: false min: 2021-05-25 06:08:42.941 max: 2021-05-25 07:34:01.941999999
    Column 2: count: 5120 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 307200
  Stripe 3:
    Column 0: count: 5120 hasNull: false
    Column 1: count: 5120 hasNull: false min: 2021-05-25 07:34:02.941 max: 2021-05-25 08:59:21.941999999
    Column 2: count: 5120 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 307200
  Stripe 4:
    Column 0: count: 4640 hasNull: false
    Column 1: count: 4640 hasNull: false min: 2021-05-25 08:59:22.941 max: 2021-05-25 10:16:41.941999999
    Column 2: count: 4640 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 278400

File Statistics:
  Column 0: count: 20000 hasNull: false
  Column 1: count: 20000 hasNull: false min: 2021-05-25 04:43:22.941 max: 2021-05-25 10:16:41.941999999
  Column 2: count: 20000 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 1200000

Stripes:
  ....
  // last stripe. Stripe4
  Stripe: offset: 5105 data: 1423 rows: 4640 tail: 71 index: 89 timezone: GMT
    Stream: column 0 section ROW_INDEX start: 5105 length 18
    Stream: column 1 section ROW_INDEX start: 5123 length 35
    Stream: column 2 section ROW_INDEX start: 5158 length 36
    Stream: column 0 section PRESENT start: 5194 length 11
    Stream: column 1 section PRESENT start: 5205 length 11
    Stream: column 1 section DATA start: 5216 length 46
    Stream: column 1 section SECONDARY start: 5262 length 19
    Stream: column 2 section PRESENT start: 5281 length 11
    Stream: column 2 section LENGTH start: 5292 length 16
    Stream: column 2 section DATA start: 5308 length 1309
    Encoding column 0: DIRECT
    Encoding column 1: DIRECT_V2
    Encoding column 2: DIRECT_V2
*/

TEST_F(HdfsScannerTest, TestOrcGetNextWithDatetimeMinMaxFilter) {
    SlotDesc datetime_orc_descs[] = {{"c0", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)}, {""}};
    const std::string datetime_orc_file = "./be/test/exec/test_data/orc_scanner/datetime_20k.orc.zlib";

    _create_runtime_state("GMT");
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(datetime_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(datetime_orc_descs);
    auto* param = _create_param(datetime_orc_file, range, tuple_desc);

    param->min_max_tuple_desc = tuple_desc;
    TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

    // expect c0 >= '2021-05-25 08:59:22'
    // which means only stripe3 matches, and all rows in stripe3 matches.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_datetime_literal_node(TPrimitiveType::DATETIME, "2021-05-25 08:59:22");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::DATETIME,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
    }

    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 4640);
    EXPECT_EQ(scanner->raw_rows_read(), 4640);
    scanner->close(_runtime_state);
}

// ====================================================================================================

/**
Type: struct<c0:char(100),c1:varchar(100)>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1024 hasNull: false
    Column 1: count: 1024 hasNull: false min: hello                                                                                                max: world                                                                                                sum: 102400
    Column 2: count: 1024 hasNull: false min: hello max: world sum: 5120
  Stripe 2:
    Column 0: count: 976 hasNull: false
    Column 1: count: 976 hasNull: false min: hello                                                                                                max: world                                                                                                sum: 97600
    Column 2: count: 976 hasNull: false min: hello max: world sum: 4880

File Statistics:
  Column 0: count: 2000 hasNull: false
  Column 1: count: 2000 hasNull: false min: hello                                                                                                max: world                                                                                                sum: 200000
  Column 2: count: 2000 hasNull: false min: hello max: world sum: 10000

Stripes:
  Stripe: offset: 3 data: 88 rows: 1024 tail: 75 index: 103
    Stream: column 0 section ROW_INDEX start: 3 length 18
    Stream: column 1 section ROW_INDEX start: 21 length 48
    Stream: column 2 section ROW_INDEX start: 69 length 37
    Stream: column 0 section PRESENT start: 106 length 5
    Stream: column 1 section PRESENT start: 111 length 5
    Stream: column 1 section DATA start: 116 length 13
    Stream: column 1 section DICTIONARY_DATA start: 129 length 21
    Stream: column 1 section LENGTH start: 150 length 7
    Stream: column 2 section PRESENT start: 157 length 5
    Stream: column 2 section DATA start: 162 length 13
    Stream: column 2 section DICTIONARY_DATA start: 175 length 13
    Stream: column 2 section LENGTH start: 188 length 6
    Encoding column 0: DIRECT
    Encoding column 1: DICTIONARY_V2[2]
    Encoding column 2: DICTIONARY_V2[2]
  Stripe: offset: 269 data: 90 rows: 976 tail: 77 index: 103
    Stream: column 0 section ROW_INDEX start: 269 length 18
    Stream: column 1 section ROW_INDEX start: 287 length 48
    Stream: column 2 section ROW_INDEX start: 335 length 37
    Stream: column 0 section PRESENT start: 372 length 5
    Stream: column 1 section PRESENT start: 377 length 5
    Stream: column 1 section DATA start: 382 length 14
    Stream: column 1 section DICTIONARY_DATA start: 396 length 21
    Stream: column 1 section LENGTH start: 417 length 7
    Stream: column 2 section PRESENT start: 424 length 5
    Stream: column 2 section DATA start: 429 length 14
    Stream: column 2 section DICTIONARY_DATA start: 443 length 13
    Stream: column 2 section LENGTH start: 456 length 6
    Encoding column 0: DIRECT
    Encoding column 1: DICTIONARY_V2[2]
    Encoding column 2: DICTIONARY_V2[2]

File length: 799 bytes
Padding length: 0 bytes
Padding ratio: 0%
 */

TEST_F(HdfsScannerTest, TestOrcGetNextWithPaddingCharDictFilter) {
    SlotDesc padding_char_varchar_desc[] = {{"c0", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_CHAR)},
                                            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
                                            {""}};
    const std::string padding_char_varchar_orc_file =
            "./be/test/exec/test_data/orc_scanner/padding_char_varchar_10k.orc";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(padding_char_varchar_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(padding_char_varchar_desc);
    auto* param = _create_param(padding_char_varchar_orc_file, range, tuple_desc);

    // c0 <= "hello"
    // and we expect we can strip of ' ' in dictionary data.
    // so return 2000 rows.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "hello");
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, tuple_desc->slots()[0], TPrimitiveType::VARCHAR, lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        std::cout << "less&eq expr = " << ctx->root()->debug_string() << std::endl;
        param->conjunct_ctxs_by_slot[0].push_back(ctx);
    }

    for (auto& it : param->conjunct_ctxs_by_slot) {
        for (auto& it2 : it.second) {
            ExprContext* ctx = it2;
            ctx->prepare(_runtime_state);
            ctx->open(_runtime_state);
        }
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    for (auto& it : param->conjunct_ctxs_by_slot) {
        for (auto& it2 : it.second) {
            it2->close(_runtime_state);
        }
    }

    // so stripe will not be filtered out by search argument
    // and we can test dict-filtering strategy.
    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1000);
    // since we use dict filter eval cache, we can do filter on orc cvb
    // so actually read rows is 1000.
    EXPECT_EQ(scanner->raw_rows_read(), 1000);
    scanner->close(_runtime_state);
}

// ====================================================================================================

/*

Structure for writer_tz_shanghai.orc
File Version: 0.12 with ORC_14 by ORC Java 1.8.0-SNAPSHOT
Rows: 1
Compression: ZLIB
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<c0:timestamp,c1:date>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1 hasNull: false
    Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
    Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

File Statistics:
  Column 0: count: 1 hasNull: false
  Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
  Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

Stripes:
  Stripe: offset: 3 data: 22 rows: 1 tail: 61 index: 68 timezone: Asia/Shanghai


{"c0":"2022-04-09 07:13:00.0","c1":"2022-04-09"}
 
*/

/*

Structure for writer_tz_utc.orc
File Version: 0.12 with ORC_14 by ORC Java 1.8.0-SNAPSHOT
Rows: 1
Compression: ZLIB
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<c0:timestamp,c1:date>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1 hasNull: false
    Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
    Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

File Statistics:
  Column 0: count: 1 hasNull: false
  Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
  Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

Stripes:
  Stripe: offset: 3 data: 22 rows: 1 tail: 51 index: 68 timezone: UTC

{"c0":"2022-04-09 07:13:00.0","c1":"2022-04-09"}

*/

TEST_F(HdfsScannerTest, DecodeMinMaxDateTime) {
    SlotDesc timezone_datetime_slot_descs[] = {{"c0", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
                                               {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATE)},
                                               {""}};

    const std::string timzone_datetime_shanghai_orc_file =
            "./be/test/exec/test_data/orc_scanner/writer_tz_shanghai.orc";
    const std::string timzone_datetime_utc_orc_file = "./be/test/exec/test_data/orc_scanner/writer_tz_utc.orc";

    struct Case {
        std::string file;
        std::string literal;
        std::string query_timezone;
        int exp;
    };
    std::vector<Case> cases = {
            {timzone_datetime_shanghai_orc_file, "2022-04-09 07:13:00", "Asia/Shanghai", 1},
            {timzone_datetime_shanghai_orc_file, "2022-04-09 07:13:00", "UTC", 0},
            {timzone_datetime_shanghai_orc_file, "2022-04-08 23:13:00", "UTC", 1},
            {timzone_datetime_utc_orc_file, "2022-04-09 07:13:00", "Asia/Shanghai", 0},
            {timzone_datetime_utc_orc_file, "2022-04-09 15:13:00", "Asia/Shanghai", 1},
            {timzone_datetime_utc_orc_file, "2022-04-09 07:13:00", "UTC", 1},
    };

    for (const Case& c : cases) {
        _create_runtime_state(c.query_timezone);
        std::cout << "Query in timezone = " << _runtime_state->timezone() << std::endl;
        auto* range = _create_scan_range(c.file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(timezone_datetime_slot_descs);
        auto* param = _create_param(c.file, range, tuple_desc);

        param->min_max_tuple_desc = tuple_desc;
        TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

        {
            std::vector<TExprNode> nodes;
            TExprNode lit_node = create_datetime_literal_node(TPrimitiveType::DATETIME, c.literal);
            push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, min_max_tuple_desc->slots()[0],
                                        TPrimitiveType::DATETIME, lit_node);
            ExprContext* ctx = create_expr_context(&_pool, nodes);
            param->min_max_conjunct_ctxs.push_back(ctx);
        }

        Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
        Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

        auto scanner = std::make_shared<HdfsOrcScanner>();
        Status status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());

        scanner->disable_use_orc_sargs();
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok()) << status.to_string();
        READ_SCANNER_ROWS(scanner, c.exp);
        scanner->close(_runtime_state);
    }
}

// ====================================================================================================
/**
 * Processing data file 00000-26-85109feb-fc85-4ad2-9a97-4102e245220d-00001.orc [length: 1109]
Structure for 00000-26-85109feb-fc85-4ad2-9a97-4102e245220d-00001.orc
File Version: 0.12 with ORC_14 by ORC Java
Rows: 1
Compression: ZLIB
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<col_boolean:boolean,col_int:int,col_long:bigint,col_float:float,col_double:double,col_decimal:decimal(38,18),col_date:date,col_timestamp:timestamp with local time zone,col_string:string,col_binary:binary,col_struct:struct<a:string,b:int>,col_map:map<string,int * 

Stripes:
  Stripe: offset: 3 data: 60 rows: 1 tail: 160 index: 338
    Stream: column 0 section ROW_INDEX start: 3 length 11
    Stream: column 1 section ROW_INDEX start: 14 length 22
    Stream: column 2 section ROW_INDEX start: 36 length 21
    Stream: column 3 section ROW_INDEX start: 57 length 21
    Stream: column 4 section ROW_INDEX start: 78 length 24
    Stream: column 5 section ROW_INDEX start: 102 length 24
    Stream: column 6 section ROW_INDEX start: 126 length 22
    Stream: column 7 section ROW_INDEX start: 148 length 19
    Stream: column 8 section ROW_INDEX start: 167 length 19
    Stream: column 9 section ROW_INDEX start: 186 length 19
    Stream: column 10 section ROW_INDEX start: 205 length 21
    Stream: column 11 section ROW_INDEX start: 226 length 17
    Stream: column 12 section ROW_INDEX start: 243 length 18
    Stream: column 13 section ROW_INDEX start: 261 length 20
    Stream: column 14 section ROW_INDEX start: 281 length 22
    Stream: column 15 section ROW_INDEX start: 303 length 18
    Stream: column 16 section ROW_INDEX start: 321 length 20
    Stream: column 1 section PRESENT start: 341 length 5
    Stream: column 1 section DATA start: 346 length 0
    Stream: column 2 section PRESENT start: 346 length 5
    Stream: column 2 section DATA start: 351 length 0
    Stream: column 3 section PRESENT start: 351 length 5
    Stream: column 3 section DATA start: 356 length 0
    Stream: column 4 section PRESENT start: 356 length 5
    Stream: column 4 section DATA start: 361 length 0
    Stream: column 5 section PRESENT start: 361 length 5
    Stream: column 5 section DATA start: 366 length 0
    Stream: column 6 section PRESENT start: 366 length 5
    Stream: column 6 section DATA start: 371 length 0
    Stream: column 6 section SECONDARY start: 371 length 0
    Stream: column 7 section PRESENT start: 371 length 5
    Stream: column 7 section DATA start: 376 length 0
    Stream: column 8 section PRESENT start: 376 length 5
    Stream: column 8 section DATA start: 381 length 0
    Stream: column 8 section SECONDARY start: 381 length 0
    Stream: column 9 section PRESENT start: 381 length 5
    Stream: column 9 section DATA start: 386 length 0
    Stream: column 9 section LENGTH start: 386 length 0
    Stream: column 9 section DICTIONARY_DATA start: 386 length 0
    Stream: column 10 section PRESENT start: 386 length 5
    Stream: column 10 section DATA start: 391 length 0
    Stream: column 10 section LENGTH start: 391 length 0
    Stream: column 11 section PRESENT start: 391 length 5
    Stream: column 12 section DATA start: 396 length 0
    Stream: column 12 section LENGTH start: 396 length 0
    Stream: column 12 section DICTIONARY_DATA start: 396 length 0
    Stream: column 13 section DATA start: 396 length 0
    Stream: column 14 section PRESENT start: 396 length 5
    Stream: column 14 section LENGTH start: 401 length 0
    Stream: column 15 section DATA start: 401 length 0
    Stream: column 15 section LENGTH start: 401 length 0
    Stream: column 15 section DICTIONARY_DATA start: 401 length 0
    Stream: column 16 section DATA start: 401 length 0
 */
TEST_F(HdfsScannerTest, TestZeroSizeStream) {
    SlotDesc slot_descs[] = {{"col_boolean", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BOOLEAN)},
                             {"col_int", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
                             {"col_long", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
                             {"col_float", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_FLOAT)},
                             {"col_double", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DOUBLE)},
                             {"col_date", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATE)},
                             {"col_timestamp", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DATETIME)},
                             {"col_string", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
                             {"col_binary", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
                             {""}};

    const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_zero_size_stream.orc";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* param = _create_param(input_orc_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1);
    EXPECT_EQ(scanner->raw_rows_read(), 1);
    scanner->close(_runtime_state);
}

// =============================================================================

/*
file:         file:/Users/dirlt/repo/private/project/pyscript/starrocks/small_row_group_data.parquet 
creator:      parquet-cpp-arrow version 7.0.0 
extra:        ARROW:schema = /////9gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAMAAABwAAAAMAAAAAQAAACs////AAABBRAAAAAYAAAABAAAAAAAAAACAAAAYzMAAAQABAAEAAAA1P///wAAAQIQAAAAFAAAAAQAAAAAAAAAAgAAAGMyAADE////AAAAAUAAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQIQAAAAHAAAAAQAAAAAAAAAAgAAAGMxAAAIAAwACAAHAAgAAAAAAAABQAAAAAAAAAA= 

file schema:  schema 
--------------------------------------------------------------------------------
c1:           OPTIONAL INT64 R:0 D:1
c2:           OPTIONAL INT64 R:0 D:1
c3:           OPTIONAL BINARY O:UTF8 R:0 D:1

row group 1:  RC:5120 TS:1197023 OFFSET:4 
--------------------------------------------------------------------------------
c1:            INT64 SNAPPY DO:4 FPO:20522 SZ:28928/49384/1.71 VC:5120 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c2:            INT64 SNAPPY DO:29030 FPO:53541 SZ:32921/49384/1.50 VC:5120 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c3:            BINARY SNAPPY DO:62051 FPO:134420 SZ:81159/1098255/13.53 VC:5120 ENC:PLAIN,PLAIN_DICTIONARY,RLE

...

row group 20: RC:2720 TS:638517 OFFSET:2732382 
--------------------------------------------------------------------------------
c1:            INT64 SNAPPY DO:2732382 FPO:2743296 SZ:15077/25937/1.72 VC:2720 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c2:            INT64 SNAPPY DO:2747562 FPO:2760616 SZ:17217/25937/1.51 VC:2720 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c3:            BINARY SNAPPY DO:2764882 FPO:2803392 SZ:43059/586643/13.62 VC:2720 ENC:PLAIN,PLAIN_DICTIONARY,RLE
 */

TEST_F(HdfsScannerTest, TestParquetCoalesceReadAcrossRowGroup) {
    SlotDesc parquet_descs[] = {{"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
                                {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_BIGINT)},
                                {"c3", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/small_row_group_data.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    ASSERT_TRUE(status.ok()) << status.get_error_msg();

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok()) << status.get_error_msg();

    READ_SCANNER_ROWS(scanner, 100000);

    scanner->close(_runtime_state);
}

// =============================================================================

/*
file:                  file:/Users/dirlt/Downloads/part-00000-4a878ed5-fa12-4e43-a164-1650976be336-c000.snappy.parquet
creator:               parquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1)
extra:                 org.apache.spark.version = 2.4.7
extra:                 org.apache.spark.sql.parquet.row.metadata = {"type":"struct","fields":[{"name":"vin","type":"string","nullable":true,"metadata":{}},{"name":"log_domain","type":"string","nullable":true,"metadata":{}},{"name":"file_name","type":"string","nullable":true,"metadata":{}},{"name":"is_collection","type":"integer","nullable":false,"metadata":{}},{"name":"is_center","type":"integer","nullable":false,"metadata":{}},{"name":"is_cloud","type":"integer","nullable":false,"metadata":{}},{"name":"collection_time","type":"string","nullable":false,"metadata":{}},{"name":"center_time","type":"string","nullable":false,"metadata":{}},{"name":"cloud_time","type":"string","nullable":false,"metadata":{}},{"name":"error_collection_tips","type":"string","nullable":false,"metadata":{}},{"name":"error_center_tips","type":"string","nullable":false,"metadata":{}},{"name":"error_cloud_tips","type":"string","nullable":false,"metadata":{}},{"name":"error_collection_time","type":"string","nullable":false,"metadata":{}},{"name":"error_center_time","type":"string","nullable":false,"metadata":{}},{"name":"error_cloud_time","type":"string","nullable":false,"metadata":{}},{"name":"original_time","type":"string","nullable":false,"metadata":{}},{"name":"is_original","type":"integer","nullable":false,"metadata":{}}]}

file schema:           spark_schema
--------------------------------------------------------------------------------
vin:                   OPTIONAL BINARY O:UTF8 R:0 D:1
log_domain:            OPTIONAL BINARY O:UTF8 R:0 D:1
file_name:             OPTIONAL BINARY O:UTF8 R:0 D:1
is_collection:         REQUIRED INT32 R:0 D:0
is_center:             REQUIRED INT32 R:0 D:0
is_cloud:              REQUIRED INT32 R:0 D:0
collection_time:       REQUIRED BINARY O:UTF8 R:0 D:0
center_time:           REQUIRED BINARY O:UTF8 R:0 D:0
cloud_time:            REQUIRED BINARY O:UTF8 R:0 D:0
error_collection_tips: REQUIRED BINARY O:UTF8 R:0 D:0
error_center_tips:     REQUIRED BINARY O:UTF8 R:0 D:0
error_cloud_tips:      REQUIRED BINARY O:UTF8 R:0 D:0
error_collection_time: REQUIRED BINARY O:UTF8 R:0 D:0
error_center_time:     REQUIRED BINARY O:UTF8 R:0 D:0
error_cloud_time:      REQUIRED BINARY O:UTF8 R:0 D:0
original_time:         REQUIRED BINARY O:UTF8 R:0 D:0
is_original:           REQUIRED INT32 R:0 D:0
*/

TEST_F(HdfsScannerTest, TestParqueTypeMismatchDecodeMinMax) {
    SlotDesc parquet_descs[] = {{"vin", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR, 22)},
                                {"is_cloud", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
                                {""}};

    SlotDesc min_max_descs[] = {{"vin", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR, 22)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/type_mismatch_decode_min_max.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    ObjectPool* pool = &_pool;
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    // select vin,is_cloud from table where is_cloud >= '0';
    auto* min_max_tuple_desc = _create_tuple_desc(min_max_descs);
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "0");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* ctx = create_expr_context(pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
    }

    param->min_max_tuple_desc = min_max_tuple_desc;
    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(!status.ok());
    scanner->close(_runtime_state);
}

// =============================================================================
/*
UID0,ACTION0
UID1,ACTION1
UID2,ACTION2
UID3,ACTION3
UID4,ACTION4
UID5,ACTION5
UID6,ACTION6
UID7,ACTION7
UID8,ACTION8
UID9,ACTION9
...
UID99,ACTION99
*/

TEST_F(HdfsScannerTest, TestCSVCompressed) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR, 22)},
                            {"action", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string uncompressed_file = "./be/test/exec/test_data/csv_scanner/compressed-csv";
    const std::string compressed_file = "./be/test/exec/test_data/csv_scanner/compressed-csv.gz";
    Status status;

    {
        auto* range = _create_scan_range(uncompressed_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(uncompressed_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 100);
        scanner->close(_runtime_state);
    }
    {
        auto* range = _create_scan_range(compressed_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(compressed_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 100);
        scanner->close(_runtime_state);
    }
    {
        auto* range = _create_scan_range(compressed_file, 0, 0);
        // Forcr to parse csv as uncompressed data.
        range->text_file_desc.__set_compression_type(TCompressionType::NO_COMPRESSION);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(compressed_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        uint64_t records = 0;
        READ_SCANNER_RETURN_ROWS(scanner, records);
        EXPECT_NE(records, 100);
        scanner->close(_runtime_state);
    }
}

} // namespace starrocks::vectorized
