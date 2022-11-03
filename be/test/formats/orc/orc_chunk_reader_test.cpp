// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/orc/orc_chunk_reader.h"

#include <gtest/gtest.h>

#include <ctime>
#include <filesystem>
#include <map>
#include <vector>

#include "common/object_pool.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

class OrcChunkReaderTest : public testing::Test {
public:
    void SetUp() override { _create_runtime_state(); }

protected:
    void _create_runtime_state();
    ObjectPool _pool;
    std::shared_ptr<RuntimeState> _runtime_state;
};

void OrcChunkReaderTest::_create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = config::vector_chunk_size;
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
    runtime_state->init_instance_mem_tracker();
    _runtime_state = runtime_state;
}

struct SlotDesc {
    string name;
    TypeDescriptor type;
};

void create_tuple_descriptor(RuntimeState* state, ObjectPool* pool, const SlotDesc* slot_descs,
                             TupleDescriptor** tuple_desc) {
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
    *tuple_desc = row_desc->tuple_descriptors()[0];
    return;
}

void create_slot_descriptors(RuntimeState* state, ObjectPool* pool, std::vector<SlotDescriptor*>* res,
                             SlotDesc* slot_descs) {
    TupleDescriptor* tuple_desc;
    create_tuple_descriptor(state, pool, slot_descs, &tuple_desc);
    *res = tuple_desc->slots();
    return;
}

/**
 * orc-statistics /home/disk2/zy/tpch_10k.orc.zstd
 * 
File /home/disk2/zy/tpch_10k.orc.zstd has 7 columns
*** Column 0 ***
Column has 10000 values and has null value: no
*** Column 1 ***
Data type: Integer  Values: 10000 Has null: no Minimum: 1 Maximum: 7 Sum: 29963
*** Column 2 ***
Data type: Integer Values: 10000 Has null: no Minimum: 9 Maximum: 200000 Sum: 990162252
*** Column 3 ***
Data type: Integer Values: 10000 Has null: no Minimum: 1075520 Maximum: 1085632 Sum: 10806360843
*** Column 4 ***
Data type: String Values: 10000 Has null: no Minimum: 0.00 Maximum: 0.10 Total length: 40000
*** Column 5 ***
Data type: Integer Values: 10000 Has null: no Minimum: 1 Maximum: 50 Sum: 254871
*** Column 6 ***
Data type: Integer Values: 10000 Has null: no Minimum: 954 Maximum: 102549 Sum: 381929645

File /home/disk2/zy/tpch_10k.orc.zstd has 2 stripes
*** Stripe 0 ***

--- Column 0 ---
Column has 5120 values and has null value: no
--- Column 1 ---
Data type: Integer Values: 5120 Has null: no Minimum: 1 Maximum: 7 Sum: 15172
--- Column 2 ---
Data type: Integer Values: 5120 Has null: no Minimum: 9 Maximum: 199927 Sum: 507887212
--- Column 3 ---
Data type: Integer Values: 5120 Has null: no Minimum: 1075520 Maximum: 1080801 Sum: 5520239705
--- Column 4 ---
Data type: String Values: 5120 Has null: no Minimum: 0.00 Maximum: 0.10 Total length: 20480
--- Column 5 ---
Data type: Integer Values: 5120 Has null: no Minimum: 1 Maximum: 50 Sum: 129769
--- Column 6 ---
Data type: Integer Values: 5120 Has null: no Minimum: 1026 Maximum: 101380 Sum: 194471830

*** Stripe 1 ***

--- Column 0 ---
Column has 4880 values and has null value: no
--- Column 1 ---
Data type: Integer Values: 4880 Has null: no Minimum: 1 Maximum: 7 Sum: 14791
--- Column 2 ---
Data type: Integer Values: 4880 Has null: no Minimum: 19 Maximum: 200000 Sum: 482275040
--- Column 3 ---
Data type: Integer Values: 4880 Has null: no Minimum: 1080801 Maximum: 1085632 Sum: 5286121138
--- Column 4 ---
Data type: String Values: 4880 Has null: no Minimum: 0.00 Maximum: 0.10 Total length: 19520
--- Column 5 ---
Data type: Integer Values: 4880 Has null: no Minimum: 1 Maximum: 50 Sum: 125102
--- Column 6 ---
Data type: Integer Values: 4880 Has null: no Minimum: 954 Maximum: 102549 Sum: 187457815
*/

static const std::string default_orc_file = "./be/test/exec/test_data/orc_scanner/tpch_10k.orc.zstd";
static const uint64_t total_record_num = 10 * 1000;
static const uint64_t default_row_group_size = 1000;
/** 
 * orc-metadata /home/disk2/zy/tpch_10k.orc.zstd
 * 
{ "name": "/home/disk2/zy/tpch_10k.orc.zstd",
  "type": "struct<lo_custkey:tinyint,lo_orderdate:int,lo_orderkey:int,lo_orderpriority:string,lo_partkey:tinyint,lo_suppkey:int>",
  "attributes": {},
  "rows": 10000,
  "stripe count": 2,
  "format": "0.12", "writer version": "future - 9",
  "compression": "zstd", "compression block": 4096,
  "file length": 76791,
  "content": 76199, "stripe stats": 268, "footer": 299, "postscript": 24,
  "row index stride": 1000,
  "user metadata": {
  },
  "stripes": [
    { "stripe": 0, "rows": 5120,
      "offset": 3, "length": 39170,
      "index": 850, "data": 38187, "footer": 133
    },
    { "stripe": 1, "rows": 4880,
      "offset": 39173, "length": 37026,
      "index": 739, "data": 36156, "footer": 131
    }
  ]
}
*/

SlotDesc default_slot_descs[] = {
        {"lo_custkey", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_TINYINT)},
        {"lo_orderdate", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {"lo_orderkey", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {"lo_orderpriority", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
        {"lo_partkey", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_TINYINT)},
        {"lo_suppkey", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
        {""},
};

static uint64_t get_hit_rows(OrcChunkReader* reader) {
    uint64_t records = 0;
    for (;;) {
        Status st = reader->read_next();
        if (st.is_end_of_file()) {
            break;
        }
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr ckptr = reader->create_chunk();
        DCHECK(ckptr != nullptr);
        st = reader->fill_chunk(&ckptr);
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr result = reader->cast_chunk(&ckptr);
        DCHECK(result != nullptr);
        records += result->num_rows();
        DCHECK(result->num_columns() == reader->num_columns());
    }
    return records;
}

TEST_F(OrcChunkReaderTest, Normal) {
    std::vector<SlotDescriptor*> src_slot_descs;
    create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, default_slot_descs);
    OrcChunkReader reader(_runtime_state.get(), src_slot_descs);
    auto input_stream = orc::readLocalFile(default_orc_file);
    reader.init(std::move(input_stream));
    uint64_t records = get_hit_rows(&reader);
    EXPECT_EQ(records, total_record_num);
}

class SkipStripeRowFilter : public orc::RowReaderFilter {
public:
    bool filterOnOpeningStripe(uint64_t stripIndex, const orc::proto::StripeInformation* stripeInformation) override {
        // we are going to read strip 1
        // and there are 4880 rows
        if (stripIndex == 1) {
            return false;
        }
        return true;
    }
    uint64_t expected_rows() const { return 4880; }
};

TEST_F(OrcChunkReaderTest, SkipStripe) {
    std::vector<SlotDescriptor*> src_slot_descs;
    create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, default_slot_descs);
    OrcChunkReader reader(_runtime_state.get(), src_slot_descs);
    auto filter = std::make_shared<SkipStripeRowFilter>();
    reader.set_row_reader_filter(filter);

    auto input_stream = orc::readLocalFile(default_orc_file);
    reader.init(std::move(input_stream));

    uint64_t records = get_hit_rows(&reader);
    EXPECT_EQ(records, filter->expected_rows());
}

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

[[maybe_unused]] static TExprNode create_date_literal_value(TPrimitiveType::type value_type, const std::string& value) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::DATE_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TDateLiteral lit_value;
    lit_value.__set_value(value);
    lit_node.__set_date_literal(lit_value);
    lit_node.__set_use_vectorized(true);
    return lit_node;
}

[[maybe_unused]] static TExprNode create_string_literal_node(TPrimitiveType::type value_type,
                                                             const std::string& value_literal) {
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

template <typename ValueType>
static void push_binary_pred_texpr_node(std::vector<TExprNode>& nodes, TExprOpcode::type opcode,
                                        SlotDescriptor* slot_desc, ValueType value_type, const TExprNode& lit_node) {
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

template <typename ValueType>
static void push_inpred_texpr_node(std::vector<TExprNode>& nodes, TExprOpcode::type opcode, SlotDescriptor* slot_desc,
                                   ValueType value_type, std::vector<TExprNode>& lit_nodes) {
    TExprNode eq_node;
    // vectorized does not support is null pred
    // eq_node.__set_node_type(TExprNodeType::type::IS_NULL_PRED);
    eq_node.__set_node_type(TExprNodeType::type::IN_PRED);
    eq_node.__set_opcode(opcode);
    eq_node.__set_child_type(value_type);
    eq_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
    eq_node.__set_num_children(1 + lit_nodes.size());
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

    for (TExprNode& lit_node : lit_nodes) {
        nodes.emplace_back(lit_node);
    }
}

static ExprContext* create_expr_context(ObjectPool* pool, const std::vector<TExprNode>& nodes) {
    TExpr texpr;
    texpr.__set_nodes(nodes);
    ExprContext* ctx;
    Status st = Expr::create_expr_tree(pool, texpr, &ctx);
    DCHECK(st.ok()) << st.get_error_msg();
    return ctx;
}

TEST_F(OrcChunkReaderTest, SkipFileByConjunctsEQ) {
    std::vector<SlotDescriptor*> src_slot_descs;
    create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, default_slot_descs);
    OrcChunkReader reader(_runtime_state.get(), src_slot_descs);

    // lo_custkey == 0, min/max is 1,7.
    std::vector<TExprNode> nodes;
    int slot_index = 0;
    TExprNode lit_node = create_int_literal_node(TPrimitiveType::TINYINT, 0);
    push_binary_pred_texpr_node(nodes, TExprOpcode::type::EQ, src_slot_descs[slot_index], TPrimitiveType::TINYINT,
                                lit_node);
    ExprContext* ctx = create_expr_context(&_pool, nodes);
    std::vector<Expr*> conjuncts = {ctx->root()};
    reader.set_conjuncts(conjuncts);

    auto input_stream = orc::readLocalFile(default_orc_file);
    reader.init(std::move(input_stream));
    uint64_t records = get_hit_rows(&reader);
    EXPECT_EQ(records, 0);
}

TEST_F(OrcChunkReaderTest, SkipStripeByConjunctsEQ) {
    std::vector<SlotDescriptor*> src_slot_descs;
    create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, default_slot_descs);
    OrcChunkReader reader(_runtime_state.get(), src_slot_descs);

    // lo_orderdate == 200000
    // stripe0 min/max = 9/199927 [5120]
    // stripe1 min/max= 19/200000 [4880]
    std::vector<TExprNode> nodes;
    int slot_index = 1;
    TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 200000);
    push_binary_pred_texpr_node(nodes, TExprOpcode::type::EQ, src_slot_descs[slot_index], TPrimitiveType::INT,
                                lit_node);
    ExprContext* ctx = create_expr_context(&_pool, nodes);
    std::vector<Expr*> conjuncts = {ctx->root()};
    reader.set_conjuncts(conjuncts);

    auto input_stream = orc::readLocalFile(default_orc_file);
    reader.init(std::move(input_stream));
    uint64_t records = get_hit_rows(&reader);
    // at most read one stripe.
    EXPECT_LE(records, 4880);
    // and actually just read one row group.
    // because there is only one item value is 200000
    // and that item belongs to one row group.
    EXPECT_EQ(records, default_row_group_size);
}

TEST_F(OrcChunkReaderTest, SkipStripeByConjunctsInPred) {
    std::vector<SlotDescriptor*> src_slot_descs;
    create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, default_slot_descs);
    OrcChunkReader reader(_runtime_state.get(), src_slot_descs);

    // lo_orderdate min/max = 9/200000
    std::vector<TExprNode> nodes;
    int slot_index = 1;
    std::vector<int64_t> values = {0, 1, 2, 3, 4, 200001, 200002};
    std::vector<TExprNode> lit_nodes;
    for (int64_t v : values) {
        auto lit_node = create_int_literal_node(TPrimitiveType::INT, v);
        lit_nodes.emplace_back(lit_node);
    }
    push_inpred_texpr_node(nodes, TExprOpcode::type::FILTER_IN, src_slot_descs[slot_index], TPrimitiveType::INT,
                           lit_nodes);
    ExprContext* ctx = create_expr_context(&_pool, nodes);
    std::vector<Expr*> conjuncts = {ctx->root()};
    reader.set_conjuncts(conjuncts);

    auto input_stream = orc::readLocalFile(default_orc_file);
    reader.init(std::move(input_stream));
    uint64_t records = get_hit_rows(&reader);
    EXPECT_LE(records, 0);
}

class SkipRowGroupRowFilter : public orc::RowReaderFilter {
public:
    bool filterOnOpeningStripe(uint64_t stripeIndex, const orc::proto::StripeInformation* stripeInformation) override {
        currentStripeIndex = stripeIndex;
        return false;
    }

    bool filterOnPickRowGroup(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) override {
        // only reads first two row group2 of first stripe
        if (rowGroupIdx < 2 && currentStripeIndex == 1) {
            return false;
        }
        return true;
    }
    void onStartingPickRowGroups() override {
        std::cout << "on starting pick row groups of stripe = " << currentStripeIndex << std::endl;
    }
    void onEndingPickRowGroups() override {
        std::cout << "on ending pick row groups of stripe = " << currentStripeIndex << std::endl;
    }
    uint64_t expected_rows() const { return default_row_group_size * 2; }

private:
    uint64_t currentStripeIndex;
};

TEST_F(OrcChunkReaderTest, SkipRowGroups) {
    std::vector<SlotDescriptor*> src_slot_descs;
    create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, default_slot_descs);
    OrcChunkReader reader(_runtime_state.get(), src_slot_descs);
    auto filter = std::make_shared<SkipRowGroupRowFilter>();
    reader.set_row_reader_filter(filter);

    auto input_stream = orc::readLocalFile(default_orc_file);
    reader.init(std::move(input_stream));

    uint64_t records = get_hit_rows(&reader);
    EXPECT_EQ(records, filter->expected_rows());
}

template <int ORC_PRECISION, int ORC_SCALE, typename ValueType>
std::vector<DecimalV2Value> convert_orc_to_starrocks_decimalv2(RuntimeState* state, ObjectPool* pool,
                                                               const std::vector<ValueType>& values) {
    std::cout << "orc precision=" << ORC_PRECISION << " scale=" << ORC_SCALE << std::endl;
    if constexpr (std::is_same_v<ValueType, int64_t>) {
        static_assert(ORC_PRECISION <= 18);
    } else {
        static_assert(std::is_same_v<ValueType, int128_t>);
        static_assert(ORC_PRECISION > 18);
    }

    using OrcDecimalVectorBatch =
            std::conditional_t<ORC_PRECISION <= 18, orc::Decimal64VectorBatch, orc::Decimal128VectorBatch>;

    const char* filename = "orc_scanner_test_decimal.orc";
    std::filesystem::remove(filename);
    ORC_UNIQUE_PTR<orc::OutputStream> outStream = orc::writeLocalFile(filename);
    ORC_UNIQUE_PTR<orc::Type> schema(
            orc::Type::buildTypeFromString(strings::Substitute("struct<c0:decimal($0,$1)>", ORC_PRECISION, ORC_SCALE)));
    ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, outStream.get(), orc::WriterOptions{});

    CHECK_LE(values.size(), 1024);
    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(1024);
    auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
    auto* c0 = dynamic_cast<OrcDecimalVectorBatch*>(root->fields[0]);

    if constexpr (std::is_same_v<ValueType, int64_t>) {
        for (int i = 0; i < values.size(); i++) {
            c0->values[i] = values[i];
        }
    } else {
        for (int i = 0; i < values.size(); i++) {
            c0->values[i] = orc::Int128(values[i] >> 64, values[i]);
        }
    }
    root->numElements = values.size();
    c0->numElements = values.size();
    writer->add(*batch);
    writer->close();
    outStream->close();

    ORC_UNIQUE_PTR<orc::Reader> reader0 = orc::createReader(orc::readLocalFile(filename), orc::ReaderOptions{});

    TDescriptorTableBuilder builder;
    TTupleDescriptorBuilder b3;

    TypeDescriptor type(TYPE_DECIMALV2);
    type.precision = 27;
    type.scale = 9;
    DescriptorTbl* tbl;
    std::vector<SlotDescriptor*> slots;
    TSlotDescriptorBuilder b2;
    b2.column_name("c0").type(type).id(0).nullable(true);
    b3.add_slot(b2.build());
    b3.build(&builder);

    Status status = DescriptorTbl::create(state, pool, builder.desc_tbl(), &tbl, config::vector_chunk_size);
    DCHECK(status.ok()) << status.get_error_msg();
    slots.push_back(tbl->get_slot_descriptor(0));

    OrcChunkReader reader(state, slots);
    reader.init(std::move(reader0));
    Status st = reader.read_next();
    CHECK(st.ok()) << st.to_string();

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type, true), 0);

    st = reader.fill_chunk(&chunk);
    CHECK(st.ok()) << st.to_string();

    st = reader.read_next();
    CHECK(!st.ok());
    std::filesystem::remove(filename);

    auto nullable = std::static_pointer_cast<NullableColumn>(chunk->get_column_by_index(0));
    CHECK(!nullable->has_null());
    auto decimal_col = std::static_pointer_cast<DecimalColumn>(nullable->data_column());
    return decimal_col->get_data();
}

TEST_F(OrcChunkReaderTest, TestDecimal64) {
    const std::vector<int64_t> orc_values = {
            0,
            1,
            -1,
            123,
            123'000'000'000,
            -999'999'999,
            -999'999'999'999'999'999,
            999'999'999,
            999'999'999'999'999'999,
            1'000'000'000,
            999'999'999'000'000'000,
    };

    auto check_results = [](const std::vector<const char*>& exp, const std::vector<DecimalV2Value>& real) {
        ASSERT_EQ(exp.size(), real.size());
        for (int i = 0; i < exp.size(); i++) {
            EXPECT_EQ(exp[i], real[i].to_string());
        }
    };

    ObjectPool pool;
    {
        std::vector<const char*> exp = {
                "0",
                "0.000000001",
                "-0.000000001",
                "0.000000123",
                "123",
                "-0.999999999",
                "-999999999.999999999",
                "0.999999999",
                "999999999.999999999",
                "1",
                "999999999",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 9>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",           "0.00000001",           "-0.00000001", "0.00000123",          "1230",
                "-9.99999999", "-9999999999.99999999", "9.99999999",  "9999999999.99999999", "10",
                "9999999990",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 8>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0.0000001",
                "-0.0000001",
                "0.0000123",
                "12300",
                "-99.9999999",
                "-99999999999.9999999",
                "99.9999999",
                "99999999999.9999999",
                "100",
                "99999999900",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 7>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "1",
                "-1",
                "123",
                "123000000000",
                "-999999999",
                "-999999999999999999",
                "999999999",
                "999999999999999999",
                "1000000000",
                "999999999000000000",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 0>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0",
                "0",
                "0.000000012",
                "12.3",
                "-0.099999999",
                "-99999999.999999999",
                "0.099999999",
                "99999999.999999999",
                "0.1",
                "99999999.9",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 10>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0",
                "0",
                "0.000000001",
                "1.23",
                "-0.009999999",
                "-9999999.999999999",
                "0.009999999",
                "9999999.999999999",
                "0.01",
                "9999999.99",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 11>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",           "0",           "0",          "0",          "0.00000123", "-0.000000009", "-9.999999999",
                "0.000000009", "9.999999999", "0.00000001", "9.99999999",
        };
        auto real = convert_orc_to_starrocks_decimalv2<18, 17>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
}

TEST_F(OrcChunkReaderTest, TestDecimal128) {
    const std::vector<int128_t> orc_values = {
            (int128_t)0,
            (int128_t)1,
            (int128_t)-1,
            (int128_t)123,
            (int128_t)123'000'000'000,
            (int128_t)-999'999'999,
            // -999'999'999'999'999'999'999'999'999
            -((int128_t)999'999'999'999'999'999 * (int128_t)1'000'000'000 + (int128_t)999'999'999),
            (int128_t)999'999'999,
            // +999'999'999'999'999'999'999'999'999
            (int128_t)999'999'999'999'999'999 * (int128_t)1'000'000'000 + (int128_t)999'999'999,
            (int128_t)1'000'000'000,
            (int128_t)999'999'999'000'000'000,
    };

    auto check_results = [](const std::vector<const char*>& exp, const std::vector<DecimalV2Value>& real) {
        ASSERT_EQ(exp.size(), real.size());
        for (int i = 0; i < exp.size(); i++) {
            EXPECT_EQ(exp[i], real[i].to_string());
        }
    };

    ObjectPool pool;
    {
        std::vector<const char*> exp = {
                "0",
                "0.000000001",
                "-0.000000001",
                "0.000000123",
                "123",
                "-0.999999999",
                "-999999999999999999.999999999",
                "0.999999999",
                "999999999999999999.999999999",
                "1",
                "999999999",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 9>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0.00000001",
                "-0.00000001",
                "0.00000123",
                "1230",
                "-9.99999999",
                "-9999999999999999999.99999999",
                "9.99999999",
                "9999999999999999999.99999999",
                "10",
                "9999999990",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 8>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0.0000001",
                "-0.0000001",
                "0.0000123",
                "12300",
                "-99.9999999",
                "-99999999999999999999.9999999",
                "99.9999999",
                "99999999999999999999.9999999",
                "100",
                "99999999900",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 7>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "1",
                "-1",
                "123",
                "123000000000",
                "-999999999",
                "-999999999999999999999999999",
                "999999999",
                "999999999999999999999999999",
                "1000000000",
                "999999999000000000",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 0>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0",
                "0",
                "0.000000012",
                "12.3",
                "-0.099999999",
                "-99999999999999999.999999999",
                "0.099999999",
                "99999999999999999.999999999",
                "0.1",
                "99999999.9",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 10>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0",
                "0",
                "0",
                "0.000000001",
                "1.23",
                "-0.009999999",
                "-9999999999999999.999999999",
                "0.009999999",
                "9999999999999999.999999999",
                "0.01",
                "9999999.99",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 11>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
    {
        std::vector<const char*> exp = {
                "0", "0", "0", "0", "0", "0", "-9.999999999", "0", "9.999999999", "0", "0.000000009",
        };
        auto real = convert_orc_to_starrocks_decimalv2<27, 26>(_runtime_state.get(), &pool, orc_values);
        check_results(exp, real);
    }
}

std::vector<TimestampValue> convert_orc_to_starrocks_timestamp(RuntimeState* state, ObjectPool* pool,
                                                               const std::string& reader_tz,
                                                               const std::string& write_tz,
                                                               const std::vector<int64_t>& values) {
    const char* filename = "orc_scanner_test_timestamp.orc";
    std::filesystem::remove(filename);
    ORC_UNIQUE_PTR<orc::OutputStream> outStream = orc::writeLocalFile(filename);
    ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:timestamp>"));

    orc::WriterOptions writer_options;
    writer_options.setTimezoneName(write_tz);
    ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, outStream.get(), writer_options);

    CHECK_LE(values.size(), 1024);
    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(1024);
    auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
    auto* c0 = dynamic_cast<orc::TimestampVectorBatch*>(root->fields[0]);

    for (int i = 0; i < values.size(); i++) {
        c0->data[i] = values[i];
        c0->nanoseconds[i] = 0;
    }

    root->numElements = values.size();
    c0->numElements = values.size();
    writer->add(*batch);
    writer->close();
    outStream->close();

    ORC_UNIQUE_PTR<orc::Reader> reader0 = orc::createReader(orc::readLocalFile(filename), orc::ReaderOptions{});

    TDescriptorTableBuilder builder;
    TTupleDescriptorBuilder b3;

    TypeDescriptor type(TYPE_DATETIME);
    DescriptorTbl* tbl;
    std::vector<SlotDescriptor*> slots;
    TSlotDescriptorBuilder b2;
    b2.column_name("c0").type(type).id(0).nullable(true);
    b3.add_slot(b2.build());
    b3.build(&builder);

    Status status = DescriptorTbl::create(state, pool, builder.desc_tbl(), &tbl, config::vector_chunk_size);
    DCHECK(status.ok()) << status.get_error_msg();
    slots.push_back(tbl->get_slot_descriptor(0));

    OrcChunkReader reader(state, slots);
    reader.set_timezone(reader_tz);
    reader.init(std::move(reader0));
    Status st = reader.read_next();
    CHECK(st.ok()) << st.to_string();

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type, true), 0);

    st = reader.fill_chunk(&chunk);
    CHECK(st.ok()) << st.to_string();

    st = reader.read_next();
    CHECK(!st.ok());
    std::filesystem::remove(filename);

    auto nullable = std::static_pointer_cast<NullableColumn>(chunk->get_column_by_index(0));
    CHECK(!nullable->has_null());
    auto ts_col = std::static_pointer_cast<TimestampColumn>(nullable->data_column());
    return ts_col->get_data();
}

TEST_F(OrcChunkReaderTest, TestTimestamp) {
    // clang-format off
    const std::vector<int64_t> orc_values = {
        // 2021.5.25 1:18:40 GMT
        // 2021.5.25 9:18:40 Asia/Shanghai
        1621905520,
        // 1970.1.1 0:0:0 GMT
        // 1970.1.1 8:00:0 Asia/Shanghai
        0,
        // 1702.5.31 19:55:52 GMT
        // to Asia/Shanghai, it's supposed to be
        // 1702.6.01 03:55:52
        // but acutally it's
        // 1702-06-01 04:01:35
        // before unix epoch, time conversion is totoally a mess.
        -8444232248
    };
    const std::vector<std::string> exp_values = {
        "2021-05-25 09:18:40",
        "1970-01-01 08:00:00",
        "1702-06-01 04:01:35",
    };
    // clang-format on
    ObjectPool pool;
    auto res = convert_orc_to_starrocks_timestamp(_runtime_state.get(), &pool, "Asia/Shanghai", "UTC", orc_values);
    EXPECT_EQ(res.size(), orc_values.size());
    for (size_t i = 0; i < res.size(); i++) {
        std::string o = res[i].to_string();
        EXPECT_EQ(o, exp_values[i]);
    }
}

/**
 { "name": "StarRocks/be/test/exec/test_data/orc_scanner/orc_test_positional_column.orc",
  "type": "struct<a:int,b:string,_col12:int,c:double>",
  "attributes": {},
  "rows": 2,
  "stripe count": 1,
  "format": "0.12", "writer version": "ORC-135",
  "compression": "zlib", "compression block": 65536,
  "file length": 566,
  "content": 308, "stripe stats": 81, "footer": 149, "postscript": 24,
  "row index stride": 10000,
  "user metadata": {
  },
  "stripes": [
    { "stripe": 0, "rows": 2,
      "offset": 3, "length": 308,
      "index": 146, "data": 77, "footer": 85
    }
  ]
}

{"a": 10, "b": "hello", "_col12": 30, "c": 10.01}
{"a": 20, "b": "world", "_col12": 40, "c": 20.01}
 */

TEST_F(OrcChunkReaderTest, TestReadPositionalColumn) {
    SlotDesc slot_descs[] = {
            {"a", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"b", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_VARCHAR)},
            {"c", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DOUBLE)},
            {""},
    };
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_test_positional_column.orc";
    std::vector<SlotDescriptor*> src_slot_descriptors;
    ObjectPool pool;
    create_slot_descriptors(_runtime_state.get(), &pool, &src_slot_descriptors, slot_descs);

    {
        OrcChunkReader reader(_runtime_state.get(), src_slot_descriptors);
        auto input_stream = orc::readLocalFile(input_orc_file);
        Status st = reader.init(std::move(input_stream));
        DCHECK(st.ok()) << st.get_error_msg();

        st = reader.read_next();
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr ckptr = reader.create_chunk();
        DCHECK(ckptr != nullptr);
        st = reader.fill_chunk(&ckptr);
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr result = reader.cast_chunk(&ckptr);
        DCHECK(result != nullptr);

        EXPECT_EQ(result->num_rows(), 2);
        EXPECT_EQ(result->num_columns(), 3);

        // slot_id = 0 is column a
        // we read 10, 20
        ColumnPtr col = result->get_column_by_slot_id(0);
        EXPECT_FALSE(col->is_null(0));
        EXPECT_EQ(col->get(0).get_int32(), 10);
        EXPECT_FALSE(col->is_null(1));
        EXPECT_EQ(col->get(1).get_int32(), 20);
    }

    {
        OrcChunkReader reader(_runtime_state.get(), src_slot_descriptors);
        std::vector<std::string> hive_column_names = {"mm", "b", "a", "c"};
        reader.set_hive_column_names(&hive_column_names);
        auto input_stream = orc::readLocalFile(input_orc_file);
        Status st = reader.init(std::move(input_stream));
        DCHECK(st.ok()) << st.get_error_msg();

        st = reader.read_next();
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr ckptr = reader.create_chunk();
        DCHECK(ckptr != nullptr);
        st = reader.fill_chunk(&ckptr);
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr result = reader.cast_chunk(&ckptr);
        DCHECK(result != nullptr);

        EXPECT_EQ(result->num_rows(), 2);
        EXPECT_EQ(result->num_columns(), 3);

        // slot_id = 0 is column a
        // since we have hive column names
        // we can do positional read.
        // so content is 30, 40
        ColumnPtr col = result->get_column_by_slot_id(0);
        EXPECT_FALSE(col->is_null(0));
        EXPECT_EQ(col->get(0).get_int32(), 30);
        EXPECT_FALSE(col->is_null(1));
        EXPECT_EQ(col->get(1).get_int32(), 40);
    }
}

/**
 *
{ "name": "/home/disk2/zy/StarRocks/be/test/exec/test_data/orc_scanner/orc_test_array_basic.orc",
  "type": "struct<c0:int,c1:array<int>,c2:array<int>,c3:int,c4:double>",
  "attributes": {},
  "rows": 2,
  "stripe count": 1,
  "format": "0.12", "writer version": "ORC-135",
  "compression": "zlib", "compression block": 65536,
  "file length": 689,
  "content": 399, "stripe stats": 92, "footer": 170, "postscript": 24,
  "row index stride": 10000,
  "user metadata": {
  },
  "stripes": [
    { "stripe": 0, "rows": 2,
      "offset": 3, "length": 399,
      "index": 195, "data": 105, "footer": 99
    }
  ]
}

{"c0": 10, "c1": [10, 20, 30], "c2": [30, 40, 50], "c3": 60, "c4": 60.1}
{"c0": 20, "c1": [20, 30, 40], "c2": [40, 50, 60], "c3": 70, "c4": 70.2}
 */

TEST_F(OrcChunkReaderTest, TestReadArrayBasic) {
    SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"c1", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY)},
            {"c2", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_ARRAY)},
            {"c4", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_DOUBLE)},
            {""},
    };

    slot_descs[1].type.children.push_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));
    slot_descs[2].type.children.push_back(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT));

    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_test_array_basic.orc";
    std::vector<SlotDescriptor*> src_slot_descriptors;
    ObjectPool pool;
    create_slot_descriptors(_runtime_state.get(), &pool, &src_slot_descriptors, slot_descs);

    {
        OrcChunkReader reader(_runtime_state.get(), src_slot_descriptors);
        auto input_stream = orc::readLocalFile(input_orc_file);
        Status st = reader.init(std::move(input_stream));
        DCHECK(st.ok()) << st.get_error_msg();

        st = reader.read_next();
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr ckptr = reader.create_chunk();
        DCHECK(ckptr != nullptr);
        st = reader.fill_chunk(&ckptr);
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr result = reader.cast_chunk(&ckptr);
        DCHECK(result != nullptr);

        EXPECT_EQ(result->num_rows(), 2);
        EXPECT_EQ(result->num_columns(), 4);

        for (int i = 0; i < 4; i++) {
            ColumnPtr col = result->get_column_by_slot_id(i);
            std::cout << "column" << i << ": " << col->debug_string() << std::endl;
        }
    }
}

/**
 * File Version: 0.12 with ORC_135
Rows: 1
Compression: NONE
Calendar: Julian/Gregorian
Type: struct<col_tinyint:tinyint,col_smallint:smallint,col_int:int,col_integer:int,col_bigint:bigint,col_float:float,col_double:double,col_double_precision:double,col_decimal:decimal(10,0),col_timestamp:timestamp,col_date:date,col_string:string,col_varchar:varchar(100),col_binary:binary,col_char:char(100),col_boolean:boolean>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1 hasNull: false
    Column 1: count: 1 hasNull: false min: 1 max: 1 sum: 1
    Column 2: count: 1 hasNull: false min: 1 max: 1 sum: 1
    Column 3: count: 1 hasNull: false min: 1 max: 1 sum: 1
    Column 4: count: 1 hasNull: false min: 1 max: 1 sum: 1
    Column 5: count: 1 hasNull: false min: 1 max: 1 sum: 1
    Column 6: count: 1 hasNull: false min: 1.0010000467300415 max: 1.0010000467300415 sum: 1.0010000467300415
    Column 7: count: 1 hasNull: false min: 10.1 max: 10.1 sum: 10.1
    Column 8: count: 1 hasNull: false min: 110.1 max: 110.1 sum: 110.1
    Column 9: count: 1 hasNull: false min: 1110 max: 1110 sum: 1110
    Column 10: count: 1 hasNull: false min: 2021-10-30 12:10:23.0 max: 2021-10-30 12:10:23.000999999
    Column 11: count: 1 hasNull: false min: Hybrid AD 2021-10-30 max: Hybrid AD 2021-10-30
    Column 12: count: 1 hasNull: false min: hello world max: hello world sum: 11
    Column 13: count: 1 hasNull: false min: hi max: hi sum: 2
    Column 14: count: 1 hasNull: false sum: 22
    Column 15: count: 1 hasNull: false min: nihao                                                                                                max: nihao                                                                                                sum: 100
    Column 16: count: 1 hasNull: false true: 1

Processing data file padding-char.orc [length: 2664]
{"col_tinyint":1,"col_smallint":1,"col_int":1,"col_integer":1,"col_bigint":1,"col_float":1.0010000467300415,"col_double":10.1,"col_double_precision":110.1,"col_decimal":"1110","col_timestamp":"2021-10-30 12:10:23.0","col_date":"2021-10-30","col_string":"hello world","col_varchar":"hi",
"col_binary":[49,49,49,48,48,48,49,48,49,48,49,48,49,48,49,49,48,48,49,48,48,49],"col_char":"nihao","col_boolean":true}    
*/

TEST_F(OrcChunkReaderTest, TestReadPaddingChar) {
    SlotDesc slot_descs[] = {
            {"col_char", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_CHAR)},
            {""},
    };

    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_test_padding_char.orc";
    std::vector<SlotDescriptor*> src_slot_descriptors;
    ObjectPool pool;
    create_slot_descriptors(_runtime_state.get(), &pool, &src_slot_descriptors, slot_descs);

    {
        OrcChunkReader reader(_runtime_state.get(), src_slot_descriptors);
        auto input_stream = orc::readLocalFile(input_orc_file);
        Status st = reader.init(std::move(input_stream));
        DCHECK(st.ok()) << st.get_error_msg();

        st = reader.read_next();
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr ckptr = reader.create_chunk();
        DCHECK(ckptr != nullptr);
        st = reader.fill_chunk(&ckptr);
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr result = reader.cast_chunk(&ckptr);
        DCHECK(result != nullptr);

        EXPECT_EQ(result->num_rows(), 1);
        EXPECT_EQ(result->num_columns(), 1);

        ColumnPtr col = result->get_column_by_slot_id(0);
        Slice s = col->get(0).get_slice();
        std::string res(s.data, s.size);
        EXPECT_EQ(res, "nihao"); // no-padding version.
    }
}

/**
 *
File Version: 0.12 with ORC_CPP_ORIGINAL
Rows: 1
Compression: ZLIB
Compression size: 65536
Type: struct<Col_Upper_Int:int,Col_Upper_Char:string>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1 hasNull: false
    Column 1: count: 1 hasNull: false min: 888 max: 888 sum: 888
    Column 2: count: 1 hasNull: false min: nihao max: nihao sum: 5

File Statistics:
  Column 0: count: 1 hasNull: false
  Column 1: count: 1 hasNull: false min: 888 max: 888 sum: 888
  Column 2: count: 1 hasNull: false min: nihao max: nihao sum: 5

Stripes:
  Stripe: offset: 3 data: 36 rows: 1 tail: 63 index: 77
    Stream: column 0 section ROW_INDEX start: 3 length 17
    Stream: column 1 section ROW_INDEX start: 20 length 29
    Stream: column 2 section ROW_INDEX start: 49 length 31
    Stream: column 0 section PRESENT start: 80 length 5
    Stream: column 1 section PRESENT start: 85 length 5
    Stream: column 1 section DATA start: 90 length 7
    Stream: column 2 section PRESENT start: 97 length 5
    Stream: column 2 section LENGTH start: 102 length 6
    Stream: column 2 section DATA start: 108 length 8
    Encoding column 0: DIRECT
    Encoding column 1: DIRECT_V2
    Encoding column 2: DIRECT_V2

File length: 375 bytes
Padding length: 0 bytes
Padding ratio: 0%

[(888, 'nihao')]
 */
TEST_F(OrcChunkReaderTest, TestColumnWithUpperCase) {
    SlotDesc slot_descs[] = {
            {"col_upper_int", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_INT)},
            {"col_upper_char", TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_CHAR)},
            {""},
    };

    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_test_upper_case.orc";
    std::vector<SlotDescriptor*> src_slot_descriptors;
    ObjectPool pool;
    create_slot_descriptors(_runtime_state.get(), &pool, &src_slot_descriptors, slot_descs);

    {
        OrcChunkReader reader(_runtime_state.get(), src_slot_descriptors);
        auto input_stream = orc::readLocalFile(input_orc_file);
        Status st = reader.init(std::move(input_stream));
        DCHECK(st.ok()) << st.get_error_msg();

        st = reader.read_next();
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr ckptr = reader.create_chunk();
        DCHECK(ckptr != nullptr);
        st = reader.fill_chunk(&ckptr);
        DCHECK(st.ok()) << st.get_error_msg();
        ChunkPtr result = reader.cast_chunk(&ckptr);
        DCHECK(result != nullptr);

        EXPECT_EQ(result->num_rows(), 1);
        EXPECT_EQ(result->num_columns(), 2);

        ColumnPtr int_col = result->get_column_by_slot_id(0);
        int i = int_col->get(0).get_int32();
        EXPECT_EQ(i, 888);

        ColumnPtr char_col = result->get_column_by_slot_id(1);
        Slice s = char_col->get(0).get_slice();
        std::string res(s.data, s.size);
        EXPECT_EQ(res, "nihao");
    }
}

} // namespace starrocks::vectorized
