// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/chunk_helper.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
<<<<<<< HEAD
#include "util/logging.h"
=======
#include "types/logical_type.h"
>>>>>>> 23f21c09f2 ([Enhancement] Optimize the performance of calc mem usage for bitmap column (#38411))

namespace starrocks::vectorized {

<<<<<<< HEAD
class ChunkHelperTest : public testing::Test {
public:
    void add_tablet_column(TabletSchemaPB& tablet_schema_pb, int32_t id, bool is_key, const std::string& type,
                           int32_t length, bool is_nullable);
    vectorized::SchemaPtr gen_v_schema(bool is_nullable);
    void check_chunk(Chunk* chunk, size_t column_size, size_t row_size);
    void check_chunk_nullable(Chunk* chunk, size_t column_size, size_t row_size);
    void check_column(Column* column, FieldType type, size_t row_size);

private:
    FieldType _type[9] = {OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_INT,
                          OLAP_FIELD_TYPE_BIGINT,  OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_FLOAT,
                          OLAP_FIELD_TYPE_DOUBLE,  OLAP_FIELD_TYPE_VARCHAR,  OLAP_FIELD_TYPE_CHAR};

    PrimitiveType _primitive_type[9] = {
            PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_INT,
            PrimitiveType::TYPE_BIGINT,  PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_FLOAT,
            PrimitiveType::TYPE_DOUBLE,  PrimitiveType::TYPE_VARCHAR,  PrimitiveType::TYPE_CHAR};
=======
class ChunkHelperTest : public ::testing::Test {
protected:
    LogicalType _primitive_type[9] = {LogicalType::TYPE_TINYINT, LogicalType::TYPE_SMALLINT, LogicalType::TYPE_INT,
                                      LogicalType::TYPE_BIGINT,  LogicalType::TYPE_LARGEINT, LogicalType::TYPE_FLOAT,
                                      LogicalType::TYPE_DOUBLE,  LogicalType::TYPE_VARCHAR,  LogicalType::TYPE_CHAR};
>>>>>>> 23f21c09f2 ([Enhancement] Optimize the performance of calc mem usage for bitmap column (#38411))

    TSlotDescriptor _create_slot_desc(PrimitiveType type, const std::string& col_name, int col_pos);
    TupleDescriptor* _create_tuple_desc();

    // A tuple with one column
    TupleDescriptor* _create_simple_desc() {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(PrimitiveType::TYPE_INT, "c0", 0));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples{0};
        std::vector<bool> nullable_tuples{true};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TSlotDescriptor ChunkHelperTest::_create_slot_desc(PrimitiveType type, const std::string& col_name, int col_pos) {
    TSlotDescriptorBuilder builder;

    if (type == PrimitiveType::TYPE_VARCHAR || type == PrimitiveType::TYPE_CHAR) {
        return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
    } else {
        return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
    }
}

TupleDescriptor* ChunkHelperTest::_create_tuple_desc() {
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;

    for (size_t i = 0; i < 9; i++) {
        tuple_builder.add_slot(_create_slot_desc(_primitive_type[i], "c" + std::to_string(i), 0));
    }

    tuple_builder.build(&table_builder);

    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];

    return tuple_desc;
}

<<<<<<< HEAD
void ChunkHelperTest::add_tablet_column(TabletSchemaPB& tablet_schema_pb, int32_t id, bool is_key,
                                        const std::string& type, int32_t length, bool is_nullable) {
    ColumnPB* column = tablet_schema_pb.add_column();
    column->set_unique_id(id);
    column->set_name("c" + std::to_string(id));
    column->set_type(type);
    column->set_is_key(is_key);
    column->set_length(length);
    column->set_is_nullable(is_nullable);
    column->set_aggregation("NONE");
}

vectorized::SchemaPtr ChunkHelperTest::gen_v_schema(bool is_nullable) {
    vectorized::Fields fields;
    fields.emplace_back(std::make_shared<Field>(0, "c0", get_type_info(OLAP_FIELD_TYPE_TINYINT), is_nullable));
    fields.emplace_back(std::make_shared<Field>(1, "c1", get_type_info(OLAP_FIELD_TYPE_SMALLINT), is_nullable));
    fields.emplace_back(std::make_shared<Field>(2, "c2", get_type_info(OLAP_FIELD_TYPE_INT), is_nullable));
    fields.emplace_back(std::make_shared<Field>(3, "c3", get_type_info(OLAP_FIELD_TYPE_BIGINT), is_nullable));
    fields.emplace_back(std::make_shared<Field>(4, "c4", get_type_info(OLAP_FIELD_TYPE_LARGEINT), is_nullable));
    fields.emplace_back(std::make_shared<Field>(5, "c5", get_type_info(OLAP_FIELD_TYPE_FLOAT), is_nullable));
    fields.emplace_back(std::make_shared<Field>(6, "c6", get_type_info(OLAP_FIELD_TYPE_DOUBLE), is_nullable));
    fields.emplace_back(std::make_shared<Field>(7, "c7", get_type_info(OLAP_FIELD_TYPE_VARCHAR), is_nullable));
    fields.emplace_back(std::make_shared<Field>(8, "c8", get_type_info(OLAP_FIELD_TYPE_CHAR), is_nullable));
    return std::make_shared<Schema>(fields);
}

void ChunkHelperTest::check_chunk(Chunk* chunk, size_t column_size, size_t row_size) {
    CHECK_EQ(chunk->columns().size(), column_size);
    for (size_t i = 0; i < column_size; i++) {
        check_column(chunk->get_column_by_index(i).get(), _type[i], row_size);
    }
}

void ChunkHelperTest::check_chunk_nullable(Chunk* chunk, size_t column_size, size_t row_size) {
    CHECK_EQ(chunk->columns().size(), column_size);
    for (size_t i = 0; i < column_size; i++) {
        Column* d_column =
                (reinterpret_cast<NullableColumn*>(chunk->get_column_by_index(i).get()))->data_column().get();
        check_column(d_column, _type[i], row_size);
    }
}

void ChunkHelperTest::check_column(Column* column, FieldType type, size_t row_size) {
    ASSERT_EQ(column->size(), row_size);

    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT: {
        const auto* data = reinterpret_cast<const int8_t*>(static_cast<Int8Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int8_t>(i * 2));
        }
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        const auto* data = reinterpret_cast<const int16_t*>(static_cast<Int16Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int16_t>(i * 2 * 10));
        }
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        const auto* data = reinterpret_cast<const int32_t*>(static_cast<Int32Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int32_t>(i * 2 * 100));
        }
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        const auto* data = reinterpret_cast<const int64_t*>(static_cast<Int64Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int64_t>(i * 2 * 1000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        const auto* data = reinterpret_cast<const int128_t*>(static_cast<Int128Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int128_t>(i * 2 * 10000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        const auto* data = reinterpret_cast<const float*>(static_cast<FloatColumn*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<float>(i * 2 * 100000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        const auto* data = reinterpret_cast<const double*>(static_cast<DoubleColumn*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<double>(i * 2 * 1000000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_VARCHAR: {
        const auto* data = reinterpret_cast<const BinaryColumn*>(column);
        for (int i = 0; i < row_size; i++) {
            Slice l = data->get_slice(i);
            Slice r(std::to_string(i * 2 * 10000000));
            ASSERT_EQ(l, r);
        }
        break;
    }
    case OLAP_FIELD_TYPE_CHAR: {
        const auto* data = reinterpret_cast<const BinaryColumn*>(column);
        for (int i = 0; i < row_size; i++) {
            Slice l = data->get_slice(i);
            Slice r(std::to_string(i * 2 * 100000000));
            ASSERT_EQ(l, r);
        }
        break;
    }
    default:
        break;
    }
}

TEST_F(ChunkHelperTest, NewChunkWithTuple) {
=======
TEST_F(ChunkHelperTest, new_chunk_with_tuple) {
>>>>>>> 23f21c09f2 ([Enhancement] Optimize the performance of calc mem usage for bitmap column (#38411))
    auto* tuple_desc = _create_tuple_desc();

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 1024);

    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->get_name(), "integral-1");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->get_name(), "integral-2");
    ASSERT_EQ(chunk->get_column_by_slot_id(2)->get_name(), "integral-4");
    ASSERT_EQ(chunk->get_column_by_slot_id(3)->get_name(), "integral-8");
    ASSERT_EQ(chunk->get_column_by_slot_id(4)->get_name(), "int128");
    ASSERT_EQ(chunk->get_column_by_slot_id(5)->get_name(), "float-4");
    ASSERT_EQ(chunk->get_column_by_slot_id(6)->get_name(), "float-8");
    ASSERT_EQ(chunk->get_column_by_slot_id(7)->get_name(), "binary");
    ASSERT_EQ(chunk->get_column_by_slot_id(8)->get_name(), "binary");
}

TEST_F(ChunkHelperTest, ReorderChunk) {
    auto* tuple_desc = _create_tuple_desc();

    auto reversed_slots = tuple_desc->slots();
    std::reverse(reversed_slots.begin(), reversed_slots.end());
    auto chunk = ChunkHelper::new_chunk(reversed_slots, 1024);

    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->columns()[8]->get_name(), "integral-1");
    ASSERT_EQ(chunk->columns()[7]->get_name(), "integral-2");
    ASSERT_EQ(chunk->columns()[6]->get_name(), "integral-4");
    ASSERT_EQ(chunk->columns()[5]->get_name(), "integral-8");
    ASSERT_EQ(chunk->columns()[4]->get_name(), "int128");
    ASSERT_EQ(chunk->columns()[3]->get_name(), "float-4");
    ASSERT_EQ(chunk->columns()[2]->get_name(), "float-8");
    ASSERT_EQ(chunk->columns()[1]->get_name(), "binary");
    ASSERT_EQ(chunk->columns()[0]->get_name(), "binary");

    ChunkHelper::reorder_chunk(*tuple_desc, chunk.get());
    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->columns()[0]->get_name(), "integral-1");
    ASSERT_EQ(chunk->columns()[1]->get_name(), "integral-2");
    ASSERT_EQ(chunk->columns()[2]->get_name(), "integral-4");
    ASSERT_EQ(chunk->columns()[3]->get_name(), "integral-8");
    ASSERT_EQ(chunk->columns()[4]->get_name(), "int128");
    ASSERT_EQ(chunk->columns()[5]->get_name(), "float-4");
    ASSERT_EQ(chunk->columns()[6]->get_name(), "float-8");
    ASSERT_EQ(chunk->columns()[7]->get_name(), "binary");
    ASSERT_EQ(chunk->columns()[8]->get_name(), "binary");
}

TEST_F(ChunkHelperTest, Accumulator) {
    constexpr size_t kDesiredSize = 4096;
    auto* tuple_desc = _create_simple_desc();
    ChunkAccumulator accumulator(kDesiredSize);
    size_t input_rows = 0;
    size_t output_rows = 0;
    // push small chunks
    for (int i = 0; i < 10; i++) {
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 1025);
        chunk->get_column_by_index(0)->append_default(1025);
        input_rows += 1025;

        accumulator.push(std::move(chunk));
        if (ChunkPtr output = accumulator.pull()) {
            output_rows += output->num_rows();
            EXPECT_EQ(kDesiredSize, output->num_rows());
        }
    }
    // push large chunks
    for (int i = 0; i < 10; i++) {
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 8888);
        chunk->get_column_by_index(0)->append_default(8888);
        input_rows += 8888;
        accumulator.push(std::move(chunk));
    }

    accumulator.finalize();
    while (ChunkPtr output = accumulator.pull()) {
        EXPECT_LE(output->num_rows(), kDesiredSize);
        output_rows += output->num_rows();
    }
    EXPECT_EQ(input_rows, output_rows);

    // push empty chunks
    for (int i = 0; i < ChunkAccumulator::kAccumulateLimit; i++) {
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        accumulator.push(std::move(chunk));
    }
    EXPECT_TRUE(accumulator.reach_limit());
    auto output = accumulator.pull();
    EXPECT_EQ(nullptr, output);
    EXPECT_TRUE(accumulator.reach_limit());
}

<<<<<<< HEAD
} // namespace starrocks::vectorized
=======
class ChunkPipelineAccumulatorTest : public ::testing::Test {
protected:
    ChunkPtr _generate_chunk(size_t rows, size_t cols);
};

ChunkPtr ChunkPipelineAccumulatorTest::_generate_chunk(size_t rows, size_t cols) {
    auto chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < cols; i++) {
        auto col = Int8Column::create(rows, 0);
        chunk->append_column(col, i);
    }
    return chunk;
}

TEST_F(ChunkPipelineAccumulatorTest, test_push) {
    ChunkPipelineAccumulator accumulator;

    // rows reach limit
    accumulator.push(_generate_chunk(4093, 1));
    ASSERT_TRUE(accumulator.has_output());
    auto result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 4093);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // mem reach limit
    accumulator.reset_state();
    accumulator.push(_generate_chunk(2048, 64));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 2048);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // merge chunk and reach rows limit
    accumulator.reset_state();
    for (size_t i = 0; i < 3; i++) {
        accumulator.push(_generate_chunk(1000, 1));
        ASSERT_FALSE(accumulator.has_output());
    }
    accumulator.push(_generate_chunk(1000, 1));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 4000);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // merge chunk and read mem limit
    accumulator.reset_state();
    for (size_t i = 0; i < 2; i++) {
        accumulator.push(_generate_chunk(1000, 30));
        ASSERT_FALSE(accumulator.has_output());
    }
    accumulator.push(_generate_chunk(1000, 30));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // merge chunk and rows overflow
    accumulator.reset_state();
    accumulator.push(_generate_chunk(3000, 1));
    ASSERT_FALSE(accumulator.has_output());
    accumulator.push(_generate_chunk(3000, 1));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    accumulator.finalize();
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    ASSERT_FALSE(accumulator.has_output());
}

} // namespace starrocks
>>>>>>> 23f21c09f2 ([Enhancement] Optimize the performance of calc mem usage for bitmap column (#38411))
