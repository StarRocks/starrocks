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

#include "serde/protobuf_serde.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/chunk_extra_data.h"
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

FieldPtr make_field(size_t i) {
    return std::make_shared<Field>(i, make_string(i), get_type_info(TYPE_INT), false);
}

Fields make_fields(size_t size) {
    Fields fields;
    for (size_t i = 0; i < size; i++) {
        fields.emplace_back(make_field(i));
    }
    return fields;
}

SchemaPtr make_schema(size_t i) {
    Fields fields = make_fields(i);
    return std::make_shared<Schema>(fields);
}

ColumnPtr make_column(size_t start) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 100; i++) {
        column->append(start + i);
    }
    return column;
}

Columns make_columns(size_t size) {
    Columns columns;
    for (size_t i = 0; i < size; i++) {
        columns.emplace_back(make_column(i));
    }
    return columns;
}
} // namespace

// NOLINTNEXTLINE
PARALLEL_TEST(ProtobufChunkSerde, test_serde) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    StatusOr<ChunkPB> res = serde::ProtobufChunkSerde::serialize_without_meta(*chunk);
    ASSERT_TRUE(res.ok()) << res.status();
    const std::string& serialized_data = res->data();

    ProtobufChunkMeta meta;
    meta.slot_id_to_index[0] = 0;
    meta.slot_id_to_index[1] = 1;
    meta.is_nulls.resize(2, false);
    meta.is_consts.resize(2, false);
    meta.types.resize(2);
    meta.types[0] = TypeDescriptor(LogicalType::TYPE_INT);
    meta.types[1] = TypeDescriptor(LogicalType::TYPE_INT);

    ProtobufChunkDeserializer deserializer(meta);
    auto chunk_or = deserializer.deserialize(serialized_data);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();
    Chunk& new_chunk = *chunk_or;
    ASSERT_EQ(new_chunk.num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk.columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk.columns()[i]->get(j).get_int32());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ProtobufChunkSerde, TestChunkWithExtraData) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    auto extra_data_meta = std::vector<ChunkExtraColumnsMeta>{
            ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_INT), .is_null = false, .is_const = false}};
    auto extra_data_cols = std::vector<ColumnPtr>{make_columns(2)};
    auto extra_data = std::make_shared<ChunkExtraColumnsData>(std::move(extra_data_meta), std::move(extra_data_cols));
    chunk->set_extra_data(extra_data);

    StatusOr<ChunkPB> res = serde::ProtobufChunkSerde::serialize_without_meta(*chunk);
    ASSERT_TRUE(res.ok()) << res.status();
    const std::string& serialized_data = res->data();

    ProtobufChunkMeta meta;
    meta.slot_id_to_index[0] = 0;
    meta.slot_id_to_index[1] = 1;
    meta.is_nulls.resize(2, false);
    meta.is_consts.resize(2, false);
    meta.types.resize(2);
    meta.types[0] = TypeDescriptor(LogicalType::TYPE_INT);
    meta.types[1] = TypeDescriptor(LogicalType::TYPE_INT);
    meta.extra_data_metas = std::vector<ChunkExtraColumnsMeta>{
            ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_INT), .is_null = false, .is_const = false}};

    ProtobufChunkDeserializer deserializer(meta);
    auto chunk_or = deserializer.deserialize(serialized_data);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();

    // check original chunk data
    Chunk& new_chunk = *chunk_or;
    ASSERT_EQ(new_chunk.num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk.columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk.columns()[i]->get(j).get_int32());
        }
    }

    // check extra chunk data
    DCHECK(new_chunk.has_extra_data());
    auto new_extra_data = dynamic_cast<ChunkExtraColumnsData*>(new_chunk.get_extra_data().get());
    auto old_extra_data = dynamic_cast<ChunkExtraColumnsData*>(chunk->get_extra_data().get());
    for (size_t i = 0; i < new_extra_data->columns().size(); ++i) {
        ASSERT_EQ(old_extra_data->columns()[i]->size(), new_extra_data->columns()[i]->size());
        for (size_t j = 0; j < old_extra_data->columns()[i]->size(); ++j) {
            ASSERT_EQ(old_extra_data->columns()[i]->get(j).get_int32(),
                      new_extra_data->columns()[i]->get(j).get_int32());
        }
    }
}

} // namespace starrocks::serde
