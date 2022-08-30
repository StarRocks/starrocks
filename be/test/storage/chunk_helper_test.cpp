// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/chunk_helper.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/field.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "storage/schema.h"
#include "util/logging.h"

namespace starrocks {
namespace vectorized {

class ChunkHelperTest : public testing::Test {
public:
    void check_column(Column* column, FieldType type, size_t row_size);

private:
    FieldType _type[9] = {OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_INT,
                          OLAP_FIELD_TYPE_BIGINT,  OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_FLOAT,
                          OLAP_FIELD_TYPE_DOUBLE,  OLAP_FIELD_TYPE_VARCHAR,  OLAP_FIELD_TYPE_CHAR};

    PrimitiveType _primitive_type[9] = {
            PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_INT,
            PrimitiveType::TYPE_BIGINT,  PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_FLOAT,
            PrimitiveType::TYPE_DOUBLE,  PrimitiveType::TYPE_VARCHAR,  PrimitiveType::TYPE_CHAR};

    TSlotDescriptor _create_slot_desc(PrimitiveType type, const std::string& col_name, int col_pos);
    TupleDescriptor* _create_tuple_desc();

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
    DescriptorTbl::create(&_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];

    return tuple_desc;
}

void ChunkHelperTest::check_column(Column* column, FieldType type, size_t row_size) {
    ASSERT_EQ(column->size(), row_size);

    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT: {
        const int8_t* data = reinterpret_cast<const int8_t*>(static_cast<Int8Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int8_t>(i * 2));
        }
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        const int16_t* data = reinterpret_cast<const int16_t*>(static_cast<Int16Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int16_t>(i * 2 * 10));
        }
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        const int32_t* data = reinterpret_cast<const int32_t*>(static_cast<Int32Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int32_t>(i * 2 * 100));
        }
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        const int64_t* data = reinterpret_cast<const int64_t*>(static_cast<Int64Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int64_t>(i * 2 * 1000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        const int128_t* data = reinterpret_cast<const int128_t*>(static_cast<Int128Column*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<int128_t>(i * 2 * 10000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        const float* data = reinterpret_cast<const float*>(static_cast<FloatColumn*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<float>(i * 2 * 100000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        const double* data = reinterpret_cast<const double*>(static_cast<DoubleColumn*>(column)->raw_data());
        for (int i = 0; i < row_size; i++) {
            ASSERT_EQ(*(data + i), static_cast<double>(i * 2 * 1000000));
        }
        break;
    }
    case OLAP_FIELD_TYPE_VARCHAR: {
        const BinaryColumn* data = reinterpret_cast<const BinaryColumn*>(column);
        for (int i = 0; i < row_size; i++) {
            Slice l = data->get_slice(i);
            Slice r(std::to_string(i * 2 * 10000000));
            ASSERT_EQ(l, r);
        }
        break;
    }
    case OLAP_FIELD_TYPE_CHAR: {
        const BinaryColumn* data = reinterpret_cast<const BinaryColumn*>(column);
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

} // namespace vectorized
} // namespace starrocks
