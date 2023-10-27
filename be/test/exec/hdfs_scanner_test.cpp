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

#include "exec/hdfs_scanner.h"

#include <gtest/gtest.h>

#include <memory>

#include "block_cache/block_cache.h"
#include "column/column_helper.h"
#include "exec/hdfs_scanner_orc.h"
#include "exec/hdfs_scanner_parquet.h"
#include "exec/hdfs_scanner_text.h"
#include "exec/jni_scanner.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"

namespace starrocks {

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
    Status _init_block_cache(size_t mem_size);
    HdfsScannerParams* _create_param(const std::string& file, THdfsScanRange* range, const TupleDescriptor* tuple_desc);
    void build_hive_column_names(HdfsScannerParams* params, const TupleDescriptor* tuple_desc);

    THdfsScanRange* _create_scan_range(const std::string& file, uint64_t offset, uint64_t length);
    TupleDescriptor* _create_tuple_desc(SlotDesc* descs);

    ObjectPool _pool;
    RuntimeProfile* _runtime_profile = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::string _debug_row_output;
    int _debug_rows_per_call = 1;
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

Status HdfsScannerTest::_init_block_cache(size_t mem_size) {
    BlockCache* cache = BlockCache::instance();
    CacheOptions cache_options;
    cache_options.mem_space_size = mem_size;
    cache_options.block_size = starrocks::config::block_cache_block_size;
    cache_options.enable_checksum = starrocks::config::block_cache_checksum_enable;
    cache_options.engine = starrocks::config::block_cache_engine;
    return cache->init(cache_options);
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
                                                  const TupleDescriptor* tuple_desc) {
    auto* param = _pool.add(new HdfsScannerParams());
    param->fs = FileSystem::Default();
    param->path = file;
    param->file_size = range->file_length;
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
    for (auto slot : tuple_desc->slots()) {
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
    DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);
    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];
    return tuple_desc;
}

// ========================= PARQUET SCANNER ============================

static SlotDesc default_parquet_descs[] = {
        {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
        {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
        {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
        {"col_varchar", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
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

    ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
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
    return lit_node;
}

template <typename ValueType>
static void push_binary_pred_texpr_node(std::vector<TExprNode>& nodes, TExprOpcode::type opcode,
                                        SlotDescriptor* slot_desc, ValueType value_type, const TExprNode& lit_node) {
    TExprNode eq_node;
    eq_node.__set_node_type(TExprNodeType::type::BINARY_PRED);
    eq_node.__set_child_type(value_type);
    eq_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
    eq_node.__set_opcode(opcode);
    eq_node.__set_num_children(2);

    TExprNode slot_node;
    slot_node.__set_node_type(TExprNodeType::SLOT_REF);
    slot_node.__set_num_children(0);
    slot_node.__set_type(create_primitive_type_desc(value_type));
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(slot_desc->id());
    slot_ref.__set_tuple_id(slot_desc->parent());
    slot_node.__set_slot_ref(slot_ref);

    nodes.emplace_back(eq_node);
    nodes.emplace_back(slot_node);
    nodes.emplace_back(lit_node);
}

static ExprContext* create_expr_context(ObjectPool* pool, const std::vector<TExprNode>& nodes) {
    TExpr texpr;
    texpr.__set_nodes(nodes);
    ExprContext* ctx;
    Status st = Expr::create_expr_tree(pool, texpr, &ctx, nullptr);
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

#define READ_SCANNER_RETURN_ROWS(scanner, records)                                        \
    do {                                                                                  \
        _debug_row_output = "";                                                           \
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);                          \
        for (;;) {                                                                        \
            chunk->reset();                                                               \
            status = scanner->get_next(_runtime_state, &chunk);                           \
            if (status.is_end_of_file()) {                                                \
                break;                                                                    \
            }                                                                             \
            if (!status.ok()) {                                                           \
                std::cout << "status not ok: " << status.get_error_msg() << std::endl;    \
                break;                                                                    \
            }                                                                             \
            chunk->check_or_die();                                                        \
            if (chunk->num_rows() > 0) {                                                  \
                int rep = std::min(_debug_rows_per_call, (int)chunk->num_rows());         \
                for (int i = 0; i < rep; i++) {                                           \
                    std::cout << "row#" << i << ": " << chunk->debug_row(i) << std::endl; \
                    _debug_row_output += chunk->debug_row(i);                             \
                    _debug_row_output += '\n';                                            \
                }                                                                         \
                EXPECT_EQ(chunk->num_columns(), tuple_desc->slots().size());              \
            }                                                                             \
            records += chunk->num_rows();                                                 \
        }                                                                                 \
    } while (0)

#define READ_SCANNER_ROWS(scanner, exp)             \
    {                                               \
        uint64_t records = 0;                       \
        READ_SCANNER_RETURN_ROWS(scanner, records); \
        EXPECT_EQ(records, exp);                    \
    }

// ====================================================================================================

static SlotDesc mtypes_orc_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                      {"col_float", TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT)},
                                      {"col_double", TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE)},
                                      {"col_varchar", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col_char", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col_tinyint", TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT)},
                                      {"col_smallint", TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT)},
                                      {"col_int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                      {"col_bigint", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                      {"col_largeint", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                      {"col0_i32p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i32p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i32p6s3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i32p6s3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i64p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i64p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i64p9s5", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i64p9s5", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i128p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i128p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i128p18s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i128p18s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i128p30s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i128p30s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"PART_x", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                      {"PART_y", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                      {""}};
std::string mtypes_orc_file = "./be/test/exec/test_data/orc_scanner/mtypes_100.orc.zlib";

static SlotDesc mtypes_orc_min_max_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                              {"PART_y", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                              {""}};

TEST_F(HdfsScannerTest, TestOrcGetNext) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* param = _create_param(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    extend_partition_values(&_pool, param, values);

    ASSERT_OK(Expr::prepare(param->partition_values, _runtime_state));
    ASSERT_OK(Expr::open(param->partition_values, _runtime_state));

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
    const TupleDescriptor* min_max_tuple_desc = params->min_max_tuple_desc;

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

    ASSERT_OK(Expr::prepare(param->partition_values, _runtime_state));
    ASSERT_OK(Expr::open(param->partition_values, _runtime_state));

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

    ASSERT_OK(Expr::prepare(param->partition_values, _runtime_state));
    ASSERT_OK(Expr::open(param->partition_values, _runtime_state));

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

    ASSERT_OK(Expr::prepare(param->partition_values, _runtime_state));
    ASSERT_OK(Expr::open(param->partition_values, _runtime_state));

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
    SlotDesc string_key_value_orc_desc[] = {{"key", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                            {"value", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
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
        Expr::prepare(it.second, _runtime_state);
        Expr::open(it.second, _runtime_state);
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

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
File be/test/exec/test_data/orc_scanner/two-strips-dict-and-nodict.orc has 2 stripes
Stripe Statistics:
  Stripe 1:
    Column 0: count: 100 hasNull: false
    Column 1: count: 100 hasNull: false bytesOnDisk: 19 min: 0 max: 9 sum: 450
    Column 2: count: 100 hasNull: false bytesOnDisk: 17 min: a max: a sum: 100
  Stripe 2:
    Column 0: count: 100 hasNull: false
    Column 1: count: 100 hasNull: false bytesOnDisk: 20 min: 0 max: 9 sum: 451
    Column 2: count: 100 hasNull: false bytesOnDisk: 1234 min: a max: ztrddxrhvzodrpeomoie sum: 1981

File Statistics:
  Column 0: count: 200 hasNull: false
  Column 1: count: 200 hasNull: false bytesOnDisk: 39 min: 0 max: 9 sum: 901
  Column 2: count: 200 hasNull: false bytesOnDisk: 1251 min: a max: ztrddxrhvzodrpeomoie sum: 2081

Stripes:
 Stripe1: offset: 3 data: 36 rows: 100 tail: 52 index: 63
   Stream: column 0 section ROW_INDEX start: 3 length 11
   Stream: column 1 section ROW_INDEX start: 14 length 25
   Stream: column 2 section ROW_INDEX start: 39 length 27
   Stream: column 1 section DATA start: 66 length 19
   Stream: column 2 section DATA start: 85 length 7
   Stream: column 2 section LENGTH start: 92 length 6
   Stream: column 2 section DICTIONARY_DATA start: 98 length 4
   Encoding column 0: DIRECT
   Encoding column 1: DIRECT_V2
   Encoding column 2: DICTIONARY_V2[1]
 Stripe2: offset: 154 data: 1254 rows: 100 tail: 50 index: 83
   Stream: column 0 section ROW_INDEX start: 154 length 11
   Stream: column 1 section ROW_INDEX start: 165 length 25
   Stream: column 2 section ROW_INDEX start: 190 length 47
   Stream: column 1 section DATA start: 237 length 20
   Stream: column 2 section DATA start: 257 length 1224
   Stream: column 2 section LENGTH start: 1481 length 10
   Encoding column 0: DIRECT
   Encoding column 1: DIRECT_V2
   Encoding column 2: DIRECT_V2
*/

TEST_F(HdfsScannerTest, TestOrcGetNextWithDiffEncodeDictFilter) {
    SlotDesc two_diff_encode_orc_desc[] = {{"x", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                           {"y", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                           {""}};
    const std::string two_diff_encode_orc_file = "./be/test/exec/test_data/orc_scanner/two-strips-dict-and-nodict.orc";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(two_diff_encode_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(two_diff_encode_orc_desc);
    auto* param = _create_param(two_diff_encode_orc_file, range, tuple_desc);

    // key = "a", y = 'a' rows is 101
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "a");
        push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, tuple_desc->slots()[1], TPrimitiveType::VARCHAR, lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        std::cout << "equal expr = " << ctx->root()->debug_string() << std::endl;
        param->conjunct_ctxs_by_slot[1].push_back(ctx);
    }

    for (auto& it : param->conjunct_ctxs_by_slot) {
        Expr::prepare(it.second, _runtime_state);
        Expr::open(it.second, _runtime_state);
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    // so stripe will not be filtered out by search argument
    // and we can test dict-filtering strategy.
    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 101);
    // since we use dict filter eval cache, we can do filter on orc cvb
    // so actually read rows is 200.
    EXPECT_EQ(scanner->raw_rows_read(), 200);
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
    SlotDesc datetime_orc_descs[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)}, {""}};
    const std::string datetime_orc_file = "./be/test/exec/test_data/orc_scanner/datetime_20k.orc.zlib";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(datetime_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(datetime_orc_descs);
    auto* param = _create_param(datetime_orc_file, range, tuple_desc);

    param->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

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
    SlotDesc padding_char_varchar_desc[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR)},
                                            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
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
        Expr::prepare(it.second, _runtime_state);
        Expr::open(it.second, _runtime_state);
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

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
    SlotDesc timezone_datetime_slot_descs[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
                                               {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE)},
                                               {""}};

    // They are a timestamp type, we will ignore all timezone information
    const std::string timezone_datetime_shanghai_orc_file =
            "./be/test/exec/test_data/orc_scanner/writer_tz_shanghai.orc";
    const std::string timezone_datetime_utc_orc_file = "./be/test/exec/test_data/orc_scanner/writer_tz_utc.orc";

    struct Case {
        std::string file;
        std::string literal;
        std::string query_timezone;
        int exp;
    };
    std::vector<Case> cases = {
            {timezone_datetime_shanghai_orc_file, "2022-04-09 07:13:00", "Asia/Shanghai", 1},
            {timezone_datetime_shanghai_orc_file, "2022-04-09 07:13:00", "UTC", 1},
            {timezone_datetime_shanghai_orc_file, "2022-04-08 23:13:00", "UTC", 0},
            {timezone_datetime_utc_orc_file, "2022-04-09 07:13:00", "Asia/Shanghai", 1},
            {timezone_datetime_utc_orc_file, "2022-04-09 15:13:00", "Asia/Shanghai", 0},
            {timezone_datetime_utc_orc_file, "2022-04-09 07:13:00", "UTC", 1},
    };

    for (const Case& c : cases) {
        _create_runtime_state(c.query_timezone);
        std::cout << "Query in timezone = " << _runtime_state->timezone() << std::endl;
        auto* range = _create_scan_range(c.file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(timezone_datetime_slot_descs);
        auto* param = _create_param(c.file, range, tuple_desc);

        param->min_max_tuple_desc = tuple_desc;
        const TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

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
    SlotDesc slot_descs[] = {{"col_boolean", TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN)},
                             {"col_int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                             {"col_long", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                             {"col_float", TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT)},
                             {"col_double", TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE)},
                             {"col_date", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE)},
                             {"col_timestamp", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
                             {"col_string", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                             {"col_binary", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY)},
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

/**
 * ORC format: struct<c0:int,c1:struct<cc0:int,Cc11:string>>
 * Data:
 * {c0: 1, c1: {cc0: 11, Cc1: "Smith"}}
 * {c0: 2, c1: {cc0: 22, Cc1: "Cruise"}}
 * {c0: 3, c1: {cc0: 33, Cc1: "hello"}}
 * {c0: 4, c1: {cc0: 44, Cc1: "world"}}
 */
TEST_F(HdfsScannerTest, TestOrcLazyLoad) {
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_test_struct_basic.orc";

    SlotDesc c0{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)};
    SlotDesc c1{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT)};
    c1.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c1.type.field_names.emplace_back("Cc1");

    SlotDesc slot_descs[] = {c0, c1, {""}};

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* param = _create_param(input_orc_file, range, tuple_desc);

    // c0 >= 3
    // so return 2 rows.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 3);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, tuple_desc->slots()[0], TPrimitiveType::INT, lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        std::cout << "greater&eq expr = " << ctx->root()->debug_string() << std::endl;
        param->conjunct_ctxs_by_slot[0].push_back(ctx);
    }

    for (auto& it : param->conjunct_ctxs_by_slot) {
        Expr::prepare(it.second, _runtime_state);
        Expr::open(it.second, _runtime_state);
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("[3, {Cc1:'hello'}]", chunk->debug_row(0));
    EXPECT_EQ("[4, {Cc1:'World'}]", chunk->debug_row(1));

    status = scanner->get_next(_runtime_state, &chunk);
    // Should be end of file in next read.
    EXPECT_TRUE(status.is_end_of_file());

    scanner->close(_runtime_state);
}

TEST_F(HdfsScannerTest, TestOrcBooleanConjunct) {
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/boolean_slot_ref.orc";

    SlotDesc c0{"sex", TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN)};

    SlotDesc slot_descs[] = {c0, {""}};

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* param = _create_param(input_orc_file, range, tuple_desc);

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node;
        lit_node.__set_node_type(TExprNodeType::SLOT_REF);
        lit_node.__set_num_children(0);
        lit_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
        TSlotRef t_slot_ref = TSlotRef();
        t_slot_ref.slot_id = 0;
        t_slot_ref.tuple_id = 0;
        lit_node.__set_slot_ref(t_slot_ref);
        nodes.emplace_back(lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->conjunct_ctxs_by_slot[0].push_back(ctx);
    }

    for (auto& it : param->conjunct_ctxs_by_slot) {
        Expr::prepare(it.second, _runtime_state);
        Expr::open(it.second, _runtime_state);
    }

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1]", chunk->debug_row(0));

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
    SlotDesc parquet_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
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

TEST_F(HdfsScannerTest, TestParquetRuntimeFilter) {
    SlotDesc parquet_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/small_row_group_data.parquet";

    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    Status status;

    struct Case {
        int min_value;
        int max_value;
        int exp_rows;
    };
    // c1 max is 99999
    Case cases[] = {{.min_value = 10000000, .max_value = 10000000, .exp_rows = 0},
                    {.min_value = -10, .max_value = -10, .exp_rows = 0},
                    {.min_value = -10, .max_value = 10000000, .exp_rows = 100000}};

    for (const Case& tc : cases) {
        auto scanner = std::make_shared<HdfsParquetScanner>();

        RuntimeFilterProbeCollector rf_collector;
        RuntimeFilterProbeDescriptor rf_probe_desc;
        ColumnRef c1ref(tuple_desc->slots()[0]);
        ExprContext probe_expr_ctx(&c1ref);

        status = probe_expr_ctx.prepare(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();
        status = probe_expr_ctx.open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        // build runtime filter.
        JoinRuntimeFilter* f = RuntimeFilterHelper::create_join_runtime_filter(&_pool, LogicalType::TYPE_BIGINT);
        f->init(10);
        ColumnPtr column = ColumnHelper::create_column(tuple_desc->slots()[0]->type(), false);
        auto c = ColumnHelper::cast_to_raw<LogicalType::TYPE_BIGINT>(column);
        c->append(tc.max_value);
        c->append(tc.min_value);
        RuntimeFilterHelper::fill_runtime_bloom_filter(column, LogicalType::TYPE_BIGINT, f, 0, false);

        rf_probe_desc.init(0, &probe_expr_ctx);
        rf_probe_desc.set_runtime_filter(f);
        rf_collector.add_descriptor(&rf_probe_desc);
        param->runtime_filter_collector = &rf_collector;

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, tc.exp_rows);

        scanner->close(_runtime_state);
        probe_expr_ctx.close(_runtime_state);
    }
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
    SlotDesc parquet_descs[] = {{"vin", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {"is_cloud", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                {""}};

    SlotDesc min_max_descs[] = {{"vin", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

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
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"action", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string uncompressed_file = "./be/test/exec/test_data/csv_scanner/compressed.csv";
    const std::string compressed_file = "./be/test/exec/test_data/csv_scanner/compressed.csv.gz";
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

// =============================================================================
/*
UID0,ACTION0
UID1,ACTION1
*/
// there is no newline at EOF.

TEST_F(HdfsScannerTest, TestCSVSmall) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"action", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/small.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 2);
        scanner->close(_runtime_state);
    }
    for (int offset = 10; offset < 20; offset++) {
        auto* range0 = _create_scan_range(small_file, 0, offset);
        // at '\n'
        auto* range1 = _create_scan_range(small_file, offset, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range0, tuple_desc);
        param->scan_ranges.emplace_back(range1);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 2);

        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVCaseIgnore) {
    SlotDesc csv_descs[] = {{"USER_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"ACTION", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/small.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 2);
        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVWithoutEndDelemeter) {
    SlotDesc csv_descs[] = {{"col1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {"col2", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {"col3", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/delimiter.csv";
    Status status;

    {
        status = _init_block_cache(50 * 1024 * 1024); // 50MB
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        param->use_block_cache = true;
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 3);
        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVWithWindowsEndDelemeter) {
    SlotDesc csv_descs[] = {{"uuid", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string windows_file = "./be/test/exec/test_data/csv_scanner/windows.csv";
    Status status;

    {
        auto* range = _create_scan_range(windows_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(windows_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, chunk->num_rows());

        EXPECT_EQ("['hello']", chunk->debug_row(0));
        EXPECT_EQ("['world']", chunk->debug_row(1));
        EXPECT_EQ("['starrocks']", chunk->debug_row(2));
        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVWithUTFBOM) {
    SlotDesc csv_descs[] = {{"uuid", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string bom_file = "./be/test/exec/test_data/csv_scanner/bom.csv";
    Status status;

    {
        auto* range = _create_scan_range(bom_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(bom_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, chunk->num_rows());

        EXPECT_EQ("['5c3ffda0d1d7']", chunk->debug_row(0));
        EXPECT_EQ("['62ef51eae5d8']", chunk->debug_row(1));
        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVNewlyAddColumn) {
    SlotDesc csv_descs[] = {{"name1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"age", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"name2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"newly", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/newly_add_column.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());

        EXPECT_EQ("['hello', '5', 'smith', NULL]", chunk->debug_row(0));
        EXPECT_EQ("['world', '6', 'cruise', NULL]", chunk->debug_row(1));

        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVDifferentOrderColumn) {
    SlotDesc csv_descs[] = {{"name1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"age", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"name2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"newly", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/newly_add_column.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        std::vector<std::string> hive_column_names{"name2", "age", "name1", "newly"};
        param->hive_column_names = &hive_column_names;
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());

        EXPECT_EQ("['smith', '5', 'hello', NULL]", chunk->debug_row(0));
        EXPECT_EQ("['cruise', '6', 'world', NULL]", chunk->debug_row(1));

        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVWithStructMap) {
    TypeDescriptor struct_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    struct_col.field_names.emplace_back("subfield");
    struct_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor array_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    array_struct.children.emplace_back(struct_col);

    TypeDescriptor map_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    map_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    map_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor array_map = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    array_map.children.emplace_back(map_col);

    SlotDesc csv_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {"struct", array_struct},
                            {"map", array_map},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/array_struct_map.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.collection_delim = "|";
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        std::vector<std::string> hive_column_names{"id", "struct", "map"};
        param->hive_column_names = &hive_column_names;
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());

        EXPECT_EQ("[1, [NULL,NULL], [NULL,NULL,NULL]]", chunk->debug_row(0));
        EXPECT_EQ("[2, [NULL], [NULL,NULL]]", chunk->debug_row(1));

        scanner->close(_runtime_state);
    }
}

TEST_F(HdfsScannerTest, TestCSVWithBlankDelimiter) {
    const std::string small_file = "./be/test/exec/test_data/csv_scanner/array_struct_map.csv";

    SlotDesc csv_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    auto* tuple_desc = _create_tuple_desc(csv_descs);
    auto* common_range = _create_scan_range(small_file, 0, 0);
    auto* param = _create_param(small_file, common_range, tuple_desc);
    std::vector<std::string> hive_column_names{"id"};
    param->hive_column_names = &hive_column_names;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.field_delim = "";
        param->scan_ranges[0] = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, *param);
        EXPECT_FALSE(status.ok());
    }
    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.collection_delim = "";
        param->scan_ranges[0] = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, *param);
        EXPECT_FALSE(status.ok());
    }
    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.mapkey_delim = "";
        param->scan_ranges[0] = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, *param);
        EXPECT_FALSE(status.ok());
    }
    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.line_delim = "";
        param->scan_ranges[0] = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, *param);
        EXPECT_FALSE(status.ok());
    }
}

// =============================================================================

/*
row group 1:           RC:100 TS:28244 OFFSET:4
--------------------------------------------------------------------------------
:                       BINARY SNAPPY DO:4 FPO:447 SZ:577/738/1.28 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
vin:                    BINARY SNAPPY DO:640 FPO:1070 SZ:570/1044/1.83 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
log_domain:             BINARY SNAPPY DO:1279 FPO:1810 SZ:685/1758/2.57 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
file_name:              BINARY SNAPPY DO:2054 FPO:2584 SZ:682/1656/2.43 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_collection:          BINARY SNAPPY DO:2823 FPO:3360 SZ:697/2064/2.96 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_center:              BINARY SNAPPY DO:3619 FPO:4149 SZ:682/1656/2.43 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_cloud:               BINARY SNAPPY DO:4388 FPO:4927 SZ:689/1554/2.26 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
collection_time:        INT96 SNAPPY DO:5161 FPO:5189 SZ:61/57/0.93 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
center_time:            INT96 SNAPPY DO:5284 FPO:5312 SZ:61/57/0.93 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
cloud_time:             INT96 SNAPPY DO:5403 FPO:5431 SZ:61/57/0.93 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_collection_tips:  BINARY SNAPPY DO:5521 FPO:6063 SZ:718/2880/4.01 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_center_tips:      BINARY SNAPPY DO:6362 FPO:6900 SZ:706/2472/3.50 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_cloud_tips:       BINARY SNAPPY DO:7179 FPO:7716 SZ:703/2370/3.37 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_collection_time:  BINARY SNAPPY DO:7990 FPO:8532 SZ:718/2880/4.01 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_center_time:      BINARY SNAPPY DO:8833 FPO:9371 SZ:706/2472/3.50 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_cloud_time:       BINARY SNAPPY DO:9653 FPO:10190 SZ:703/2370/3.37 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
original_time:          BINARY SNAPPY DO:10467 FPO:11001 SZ:694/2064/2.97 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_original:            INT64 SNAPPY DO:11263 FPO:11287 SZ:99/95/0.96 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
*/

TEST_F(HdfsScannerTest, TestParqueTypeMismatchInt96String) {
    SlotDesc parquet_descs[] = {{"vin", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {"collection_time", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/type_mismatch_int96_string.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    // parquet column reader: not supported convert from parquet `INT96` to `VARCHAR`
    EXPECT_TRUE(!status.ok()) << status.get_error_msg();
    scanner->close(_runtime_state);
}

// =============================================================================
/* data: we expect to read 3 rows [NULL,"",abc]
\N

abc
*/
// there is no newline at EOF.

TEST_F(HdfsScannerTest, TestCSVSingleColumnNullAndEmpty) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/single_column_null_and_empty.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* param = _create_param(small_file, range, tuple_desc);
        build_hive_column_names(param, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, *param);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();

        READ_SCANNER_ROWS(scanner, 3);
        scanner->close(_runtime_state);
    }
}

// =============================================================================

/*
id 0 ~ 99
A "00" ~ "99"

file schema: schema
--------------------------------------------------------------------------------
id:          OPTIONAL INT64 R:0 D:1
A:           OPTIONAL BINARY O:UTF8 R:0 D:1

row group 1: RC:100 TS:1730 OFFSET:4
--------------------------------------------------------------------------------
id:           INT64 SNAPPY DO:4 FPO:432 SZ:595/981/1.65 VC:100 ENC:PLAIN,RLE,PLAIN_DICTIONARY
A:            BINARY SNAPPY DO:693 FPO:1139 SZ:581/749/1.29 VC:100 ENC:PLAIN,RLE,PLAIN_DICTIONARY
*/

TEST_F(HdfsScannerTest, TestParquetUppercaseFiledPredicate) {
    SlotDesc parquet_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                {"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/upcase_field.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    param->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

    // expect a = '05'
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "05");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[1], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
        param->conjunct_ctxs.push_back(ctx);
    }
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "05");
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[1], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
        param->conjunct_ctxs.push_back(ctx);
    }

    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close(_runtime_state);
}

// =============================================================================

/*
file schema: schema
--------------------------------------------------------------------------------
id:          OPTIONAL INT64 R:0 D:1
f00:         OPTIONAL F:1
.list:       REPEATED F:1
..item:      OPTIONAL INT64 R:1 D:3
f01:         OPTIONAL F:1
.list:       REPEATED F:1
..item:      OPTIONAL INT64 R:1 D:3

row group 1: RC:1500 TS:52144 OFFSET:4
--------------------------------------------------------------------------------
id:           INT64 UNCOMPRESSED DO:4 FPO:12023 SZ:14162/14162/1.00 VC:1500 ENC:RLE,PLAIN_DICTIONARY,PLAIN
f00:
.list:
..item:       INT64 UNCOMPRESSED DO:14264 FPO:26307 SZ:18991/18991/1.00 VC:4500 ENC:RLE,PLAIN_DICTIONARY,PLAIN
f01:
.list:
..item:       INT64 UNCOMPRESSED DO:33367 FPO:45410 SZ:18991/18991/1.00 VC:4500 ENC:RLE,PLAIN_DICTIONARY,PLAIN

python code to generate this file.

def array2_parquet():
    def fn():
        records = []
        N = 1500
        for i in range(N):
            x = {'id': i}
            for f in range(2):
                key = 'f%02d' % f
                x[key] = [(j + i) for j in range(5)]
                if i % 2 == 0:
                    x[key] = None

            records.append(x)
        return records

    name = 'array2.parquet'
    write_parquet_file(name, fn)
*/

TEST_F(HdfsScannerTest, TestParquetArrayDecode) {
    TypeDescriptor array_type(TYPE_ARRAY);
    array_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    SlotDesc parquet_descs[] = {
            {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {"f00", array_type}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/array_decode.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1500);
    EXPECT_EQ(scanner->raw_rows_read(), 1500);
    scanner->close(_runtime_state);
}

// =============================================================================

/*

a column, in a row group it's encoded as dict page
but in another row group it's encoded as plain page.

sandbox-cloud :: ~ » hadoop jar ~/installed/parquet-tools-1.9.0.jar dump -c id --disable-data --disable-crop file:///home/disk4/zhangyan/dict2.parquet                                                                                                                                                                                                         1 ↵
row group 0
--------------------------------------------------------------------------------
id:  BINARY UNCOMPRESSED DO:5709 FPO:5709 SZ:163/163/1.00 VC:100 ENC:PLAIN,RLE_DICTIONARY

    id TV=100 RL=0 DL=1 DS: 3 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                  DLE:RLE RLE:BIT_PACKED VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:103 VC:100

row group 1
--------------------------------------------------------------------------------
id:  BINARY UNCOMPRESSED DO:0 FPO:16447 SZ:721/721/1.00 VC:100 ENC:PLAIN

    id TV=100 RL=0 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:BIT_PACKED VLE:PLAIN ST:[no stats for this column] SZ:701 VC:100


Python code

def dict2_parquet():
    def fn():
        records = []
        d = ['ysq01', 'ysq02', 'ysq03', None]
        N = 100
        for i in range(N):
            x = {'seq': i}
            for f in range(2):
                x['f%02d' % f] = ''.join([chr(ord('a') + (i + f + x) % 26) for x in range(20)])
            x['id'] = d[i % 4]
            for f in range(2):
                x['f%02d' % (f + 3)] = ''.join([chr(ord('a') + (i + f + x) % 26) for x in range(20)])
            records.append(x)
        return records

    name = 'dict2.parquet'
    json_data = fn()
    df = pd.DataFrame.from_records(json_data)
    # to enforce as dictionary page.
    df['id'] = df['id'].astype('category')
    fastparquet.write(name, df)

    df = pd.DataFrame.from_records(json_data)
    fastparquet.write(name, df, append=True)
    peek_parquet_data(name)
*/

TEST_F(HdfsScannerTest, TestParquetDictTwoPage) {
    SlotDesc parquet_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/dict_two_page.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    param->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

    // expect id = 'ysq01'
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "ysq01");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
        param->conjunct_ctxs.push_back(ctx);
    }
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "ysq01");
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[0], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
        param->conjunct_ctxs.push_back(ctx);
    }

    Expr::prepare(param->min_max_conjunct_ctxs, _runtime_state);
    Expr::open(param->min_max_conjunct_ctxs, _runtime_state);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 50);
    EXPECT_EQ(scanner->raw_rows_read(), 200);
    scanner->close(_runtime_state);
}

// Test min-max logic when parquet file contains complex types
TEST_F(HdfsScannerTest, TestMinMaxFilterWhenContainsComplexTypes) {
    SlotDesc parquet_descs[] = {{"col82", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_data/min-max-complex-types.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    param->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = param->min_max_tuple_desc;

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 82);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
        param->conjunct_ctxs.push_back(ctx);
    }
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 82);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[0], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* ctx = create_expr_context(&_pool, nodes);
        param->min_max_conjunct_ctxs.push_back(ctx);
        param->conjunct_ctxs.push_back(ctx);
    }

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

// =======================================================

static bool can_run_jni_test() {
    if (getenv("STARROCKS_SRC_HOME") == nullptr) {
        // Right now it's very hard to construct jni ut, because we only build ut-binary but not java extensions.
        // To make this test run, we have to define `STARROCKS_SRC_HOME` and `STARROCKS_HOME`(which has java-extensions)
        // and we have to set following env vars
        /*
        STARROCKS_SRC_HOME=${STARROCKS_HOME} \
        STARROCKS_HOME=${STARROCKS_HOME}/output/be \
        HADOOP_CLASSPATH=${STARROCKS_HOME}/lib/hadoop/common/*:${STARROCKS_HOME}/lib/hadoop/common/lib/*:${STARROCKS_HOME}/lib/hadoop/hdfs/*:${STARROCKS_HOME}/lib/hadoop/hdfs/lib/* \
        CLASSPATH=$STARROCKS_HOME/conf:$STARROCKS_HOME/lib/jni-packages/*:$HADOOP_CLASSPATH:$CLASSPATH \
        LD_LIBRARY_PATH=$STARROCKS_HOME/lib/hadoop/native:$LD_LIBRARY_PATH \
        */
        return false;
    }
    return true;
}

/*

CREATE TABLE `test_hudi_mor2` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<array<int>>,
  `d` map<string, array<int>>,
  `e` struct<a:array<int>, b:map<string,int>, c:struct<a:array<int>, b:struct<a:int,b:string>>>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

insert into test_hudi_mor2 values('AA0', 10, 0, "hello", array(array(10,20,30), array(40,50,60,70) ), map('key1', array(1,10), 'key2', array(2, 20), 'key3', null), struct(array(10, 20), map('key1', 10), struct(array(10, 20), struct(10, "world")))),
 ('AA1', 10, 0, "hello", null, null , struct(null, map('key1', 10), struct(array(10, 20), struct(10, "world")))),
 ('AA2', 10, 0, null, array(array(30, 40), array(10,20,30)), null , struct(null, map('key1', 10), struct(array(10, 20), null)));

spark-sql> select a,b,c,d,e from test_hudi_mor2;
a       b       c       d       e
0       hello   NULL    NULL    {"a":null,"b":{"key1":10},"c":{"a":[10,20],"b":{"a":10,"b":"world"}}}
0       NULL    [[30,40],[10,20,30]]    NULL    {"a":null,"b":{"key1":10},"c":{"a":[10,20],"b":null}}
0       hello   [[10,20,30],[40,50,60,70]]      {"key1":[1,10],"key2":[2,20],"key3":null}       {"a":[10,20],"b":{"key1":10},"c":{"a":[10,20],"b":{"a":10,"b":"world"}}}

*/
TEST_F(HdfsScannerTest, TestHudiMORArrayMapStruct) {
    if (!can_run_jni_test()) {
        GTEST_SKIP();
    }

    TypeDescriptor C(LogicalType::TYPE_ARRAY);
    TypeDescriptor C1(LogicalType::TYPE_ARRAY);
    C1.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    C.children.push_back(C1); // array<array<int>>

    TypeDescriptor D(LogicalType::TYPE_MAP);
    D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
    D.children.push_back(C1); // map<string, array<int>>

    TypeDescriptor E(LogicalType::TYPE_STRUCT);
    {
        E.children.push_back(C1); // a: array<int>
        E.field_names.emplace_back("a");

        TypeDescriptor B0(LogicalType::TYPE_MAP);
        B0.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        B0.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.children.push_back(B0);
        E.field_names.emplace_back("b");

        TypeDescriptor C0(LogicalType::TYPE_STRUCT);
        {
            C0.children.push_back(C1);
            C0.field_names.emplace_back("a");

            TypeDescriptor B1(LogicalType::TYPE_STRUCT);
            B1.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
            B1.field_names.emplace_back("a");
            B1.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
            B1.field_names.emplace_back("b");
            C0.children.push_back(B1);
            C0.field_names.emplace_back("b");
        }

        E.children.push_back(C0);
        E.field_names.emplace_back("c");
    }

    SlotDesc parquet_descs[] = {{"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                {"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {"c", C},
                                {"d", D},
                                {"e", E},
                                {""}};
    std::string starrocks_home = getenv("STARROCKS_SRC_HOME");
    std::string basePath = starrocks_home + "/be/test/exec/test_data/jni_scanner/test_hudi_mor_ams";
    std::string parquet_file = basePath + "/0df0196b-f46f-43f5-8cf0-06fad7143af3-0_0-27-35_20230110191854854.parquet";

    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    // be/test/exec/test_data/jni_scanner/test_hudi_mor_ams
    std::map<std::string, std::string> params;

    params.emplace("base_path", basePath);
    params.emplace("data_file_path", parquet_file);
    params.emplace("delta_file_paths", "");
    params.emplace("hive_column_names",
                   "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_"
                   "name,uuid,ts,a,b,c,d,e");
    params.emplace("hive_column_types",
                   "string#string#string#string#string#string#int#int#string#array<array<int>>#map<string,array<int>>#"
                   "struct<a:array<int>,b:map<string,int>,c:struct<a:array<int>,b:struct<a:int,b:string>>>");
    params.emplace("instant_time", "20230110185815638");
    params.emplace("data_file_length", "438311");
    params.emplace("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
    params.emplace("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    params.emplace("required_fields", "a,b,c,d,e");

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";
    JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 3);
    EXPECT_EQ(scanner->raw_rows_read(), 3);
    scanner->close(_runtime_state);

    EXPECT_EQ(_debug_row_output, "[0, 'hello', NULL, NULL, {a:NULL,b:{'key1':10},c:{a:[10,20],b:{a:10,b:'world'}}}]\n");
}

/*
CREATE TABLE `test_hudi_mor` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<int>,
  `d` map<string, int>,
  `e` struct<a:int, b:string>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

spark-sql> select a,b,c,d,e from test_hudi_mor;
a       b       c       d       e
1       hello   [10,20,30]      {"key1":1,"key2":2}     {"a":10,"b":"world"}

*/
TEST_F(HdfsScannerTest, TestHudiMORArrayMapStruct2) {
    if (!can_run_jni_test()) {
        GTEST_SKIP();
    }

    std::string starrocks_home = getenv("STARROCKS_SRC_HOME");
    std::string basePath = starrocks_home + "/be/test/exec/test_data/jni_scanner/test_hudi_mor_ams2";
    std::string parquet_file = basePath + "/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet";

    auto* range = _create_scan_range(parquet_file, 0, 0);

    // be/test/exec/test_data/jni_scanner/test_hudi_mor_ams
    std::map<std::string, std::string> params;

    params.emplace("base_path", basePath);
    params.emplace("data_file_path", parquet_file);
    params.emplace("delta_file_paths",
                   basePath + "/.64798197-be6a-4eca-9898-0c2ed75b9d65-0_20230105142938081.log.1_0-95-78");
    params.emplace("hive_column_names",
                   "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_"
                   "name,uuid,ts,a,b,c,d,e");
    params.emplace("hive_column_types",
                   "string#string#string#string#string#string#int#int#string#array<int>#map<string,int>#struct<a:int,b:"
                   "string>");
    params.emplace("instant_time", "20230105143305070");
    params.emplace("data_file_length", "436081");
    params.emplace("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
    params.emplace("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    params.emplace("required_fields", "a,b,c,d,e");

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";

    // select a,b,c,d,e
    {
        // c: array<int>
        TypeDescriptor C(LogicalType::TYPE_ARRAY);
        C.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

        // d: map<string, int>
        TypeDescriptor D(LogicalType::TYPE_MAP);
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        E.field_names.emplace_back("b");

        SlotDesc parquet_descs[] = {{"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                    {"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                    {"c", C},
                                    {"d", D},
                                    {"e", E},
                                    {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* param = _create_param(parquet_file, range, tuple_desc);

        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close(_runtime_state);

        EXPECT_EQ(_debug_row_output, "[1, 'hello', [10,20,30], {'key1':1,'key2':2}, {a:10,b:'world'}]\n");
    }

    // select e but as <b:string, a:int>
    {
        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        E.field_names.emplace_back("b");
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");

        SlotDesc parquet_descs[] = {{"e", E}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* param = _create_param(parquet_file, range, tuple_desc);

        params["required_fields"] = "e";
        params["nested_fields"] = "e.b,e.a";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close(_runtime_state);

        EXPECT_EQ(_debug_row_output, "[{b:'world',a:10}]\n");
    }

    // select e but as <B:string, a:int>
    {
        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        E.field_names.emplace_back("B");
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");

        SlotDesc parquet_descs[] = {{"e", E}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* param = _create_param(parquet_file, range, tuple_desc);

        params["required_fields"] = "e";
        params["nested_fields"] = "e.B,e.a";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close(_runtime_state);

        EXPECT_EQ(_debug_row_output, "[{B:NULL,a:10}]\n");
    }

    // select e but as <a:int>
    {
        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");

        SlotDesc parquet_descs[] = {{"e", E}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* param = _create_param(parquet_file, range, tuple_desc);

        params["required_fields"] = "e";
        params["nested_fields"] = "e.a";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close(_runtime_state);

        EXPECT_EQ(_debug_row_output, "[{a:10}]\n");
    }

    // select d but only key
    {
        // d: map<string, int>
        TypeDescriptor D(LogicalType::TYPE_MAP);
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));

        SlotDesc parquet_descs[] = {{"d", D}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* param = _create_param(parquet_file, range, tuple_desc);

        params["required_fields"] = "d";
        params["nested_fields"] = "d.$0";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, *param);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close(_runtime_state);

        EXPECT_EQ(_debug_row_output, "[{'key1':NULL,'key2':NULL}]\n");
    }
}

// =============================================================================
/*
spark-sql> select * from test_hudi_mor13;
_hoodie_commit_time     _hoodie_commit_seqno    _hoodie_record_key      _hoodie_partition_path  _hoodie_file_name       a       b       ts      uuid
20230216103800029       20230216103800029_0_0   aa1             14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      300     3023-01-01 00:00:00     40      aa1
20230216103800029       20230216103800029_0_1   aa0             14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      200     2023-01-01 00:00:00     30      aa0
20230216103800029       20230216103800029_0_2   aa2             14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      400     1000-01-01 00:00:00     50      aa2
20230216103800029       20230216103800029_0_3   aa              14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      100     1900-01-01 00:00:00     20      aa
*/

/*
_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_0
_hoodie_record_key = aa1
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 300
b = 33229411200000000
ts = 40
uuid = aa1

_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_1
_hoodie_record_key = aa0
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 200
b = 1672502400000000
ts = 30
uuid = aa0

_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_2
_hoodie_record_key = aa2
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 400
b = -30610253143000000
ts = 50
uuid = aa2

_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_3
_hoodie_record_key = aa
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 100
b = -2209017943000000
ts = 20
uuid = aa
*/

TEST_F(HdfsScannerTest, TestParquetTimestampToDatetime) {
    SlotDesc parquet_descs[] = {{"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/timestamp_to_datetime.parquet";

    _create_runtime_state("Asia/Shanghai");
    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    _debug_rows_per_call = 10;
    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 4);
    EXPECT_EQ(scanner->raw_rows_read(), 4);
    EXPECT_EQ(_debug_row_output,
              "[3023-01-01 00:00:00]\n[2023-01-01 00:00:00]\n[1000-01-01 00:00:00]\n[1900-01-01 00:00:00]\n");
    scanner->close(_runtime_state);
}

TEST_F(HdfsScannerTest, TestParquetIcebergCaseSensitive) {
    SlotDesc parquet_descs[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    const std::string parquet_file =
            "./be/test/formats/parquet/test_data/iceberg_schema_evolution/add_struct_subfield.parquet";

    _create_runtime_state("Asia/Shanghai");
    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* param = _create_param(parquet_file, range, tuple_desc);

    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("Id");

    std::vector<TIcebergSchemaField> fields{field_id};
    schema.__set_fields(fields);
    param->iceberg_schema = &schema;

    _debug_rows_per_call = 10;
    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1);
    EXPECT_EQ(scanner->raw_rows_read(), 1);
    EXPECT_EQ(_debug_row_output, "[1]\n");
    scanner->close(_runtime_state);
}

} // namespace starrocks
