// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "serde/protobuf_serde.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "runtime/types.h"
#include "testutil/parallel_test.h"

namespace starrocks::serde {

namespace {

std::string make_string(size_t i) {
    return std::string("c").append(std::to_string(static_cast<int32_t>(i)));
}

vectorized::FieldPtr make_field(size_t i) {
    return std::make_shared<vectorized::Field>(i, make_string(i), get_type_info(OLAP_FIELD_TYPE_INT), false);
}

vectorized::Fields make_fields(size_t size) {
    vectorized::Fields fields;
    for (size_t i = 0; i < size; i++) {
        fields.emplace_back(make_field(i));
    }
    return fields;
}

vectorized::SchemaPtr make_schema(size_t i) {
    vectorized::Fields fields = make_fields(i);
    return std::make_shared<vectorized::Schema>(fields);
}

vectorized::ColumnPtr make_column(size_t start) {
    auto column = vectorized::FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 100; i++) {
        column->append(start + i);
    }
    return column;
}

vectorized::Columns make_columns(size_t size) {
    vectorized::Columns columns;
    for (size_t i = 0; i < size; i++) {
        columns.emplace_back(make_column(i));
    }
    return columns;
}
} // namespace

// NOLINTNEXTLINE
PARALLEL_TEST(ProtobufChunkSerde, test_serde) {
    auto chunk = std::make_unique<vectorized::Chunk>(make_columns(2), make_schema(2));

    StatusOr<ChunkPB> res = serde::ProtobufChunkSerde::serialize_without_meta(*chunk);
    ASSERT_TRUE(res.ok()) << res.status();
    const std::string& serialized_data = res->data();

    ProtobufChunkMeta meta;
    meta.slot_id_to_index[0] = 0;
    meta.slot_id_to_index[1] = 1;
    meta.is_nulls.resize(2, false);
    meta.is_consts.resize(2, false);
    meta.types.resize(2);
    meta.types[0] = TypeDescriptor(PrimitiveType::TYPE_INT);
    meta.types[1] = TypeDescriptor(PrimitiveType::TYPE_INT);

    ProtobufChunkDeserializer deserializer(meta);
    auto chunk_or = deserializer.deserialize(serialized_data);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();
    vectorized::Chunk& new_chunk = *chunk_or;
    ASSERT_EQ(new_chunk.num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk.columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk.columns()[i]->get(j).get_int32());
        }
    }
}

} // namespace starrocks::serde
