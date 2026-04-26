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

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/field.h"
#include "column/fixed_length_column.h"

namespace starrocks {

class ChunkCoreTest : public ::testing::Test {
protected:
    FieldPtr _make_field(size_t idx) {
        return std::make_shared<Field>(idx, "c" + std::to_string(idx), get_type_info(TYPE_INT), false);
    }

    SchemaPtr _make_schema(size_t num_fields) {
        Fields fields;
        fields.reserve(num_fields);
        for (size_t i = 0; i < num_fields; ++i) {
            fields.emplace_back(_make_field(i));
        }
        return std::make_shared<Schema>(std::move(fields));
    }

    ColumnPtr _make_int_column(int32_t start, size_t num_rows) {
        auto col = Int32Column::create();
        for (size_t i = 0; i < num_rows; ++i) {
            col->append(start + static_cast<int32_t>(i));
        }
        return col;
    }

    MutableColumnPtr _make_mutable_int_column(int32_t start, size_t num_rows) {
        auto col = Int32Column::create();
        for (size_t i = 0; i < num_rows; ++i) {
            col->append(start + static_cast<int32_t>(i));
        }
        return col;
    }
};

TEST_F(ChunkCoreTest, BasicConstructAppendRemove) {
    Columns cols;
    cols.emplace_back(_make_int_column(10, 5));
    cols.emplace_back(_make_int_column(20, 5));
    auto schema = _make_schema(2);

    Chunk chunk(std::move(cols), schema);
    ASSERT_EQ(2, chunk.num_columns());
    ASSERT_EQ(5, chunk.num_rows());
    ASSERT_EQ(2, chunk.schema()->num_fields());

    chunk.append_column(_make_int_column(200, 5), _make_field(2));
    ASSERT_EQ(3, chunk.num_columns());
    ASSERT_EQ(3, chunk.schema()->num_fields());
    ASSERT_EQ(200, chunk.get_column_by_id(2)->get(0).get_int32());

    chunk.remove_column_by_index(1);
    ASSERT_EQ(2, chunk.num_columns());
    ASSERT_EQ(2, chunk.schema()->num_fields());
    ASSERT_EQ(200, chunk.get_column_by_id(2)->get(0).get_int32());
}

TEST_F(ChunkCoreTest, CloneAppendFilter) {
    Columns cols;
    cols.emplace_back(_make_int_column(10, 6));
    cols.emplace_back(_make_int_column(20, 6));
    Chunk src(std::move(cols), _make_schema(2));

    auto dst = src.clone_empty(0);
    dst->append(src, 1, 4);
    ASSERT_EQ(4, dst->num_rows());
    ASSERT_EQ(11, dst->get_column_by_index(0)->get(0).get_int32());
    ASSERT_EQ(14, dst->get_column_by_index(0)->get(3).get_int32());

    Filter selection = {1, 0, 1, 0};
    dst->filter(selection, true);
    ASSERT_EQ(2, dst->num_rows());
    ASSERT_EQ(11, dst->get_column_by_index(0)->get(0).get_int32());
    ASSERT_EQ(13, dst->get_column_by_index(0)->get(1).get_int32());
}

TEST_F(ChunkCoreTest, MutableChunkRoundTrip) {
    MutableColumns mcols;
    mcols.emplace_back(_make_mutable_int_column(1, 3));
    mcols.emplace_back(_make_mutable_int_column(10, 3));

    MutableChunk mchunk(std::move(mcols), _make_schema(2));
    Chunk chunk = mchunk.to_chunk();
    ASSERT_EQ(3, chunk.num_rows());
    ASSERT_EQ(2, chunk.num_columns());
    ASSERT_EQ(2, chunk.get_column_by_index(0)->get(1).get_int32());

    MutableChunk moved_back(std::move(chunk));
    ASSERT_EQ(3, moved_back.num_rows());
    ASSERT_EQ(2, moved_back.num_columns());
    ASSERT_EQ(11, moved_back.get_column_by_index(1)->get(1).get_int32());
}

TEST_F(ChunkCoreTest, OwnerInfoBehavior) {
    Chunk chunk;
    auto& info = chunk.owner_info();
    info.set_owner_id(7, false);
    ASSERT_EQ(7, info.owner_id());
    ASSERT_FALSE(info.is_last_chunk());
    info.set_last_chunk(true);
    ASSERT_TRUE(info.is_last_chunk());
    info.set_passthrough(true);
    ASSERT_TRUE(info.is_passthrough());

    Columns cols;
    cols.emplace_back(_make_int_column(100, 1));
    Chunk data_chunk(std::move(cols), _make_schema(1));
    data_chunk.owner_info().set_owner_id(9, true);
    data_chunk.owner_info().set_passthrough(true);

    auto cloned = data_chunk.clone_unique();
    ASSERT_EQ(9, cloned->owner_info().owner_id());
    ASSERT_TRUE(cloned->owner_info().is_last_chunk());
    ASSERT_TRUE(cloned->owner_info().is_passthrough());
}

} // namespace starrocks
