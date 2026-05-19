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

#include "runtime/checked_chunk_factory.h"

#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/field.h"
#include "column/schema.h"
#include "gtest/gtest.h"

namespace starrocks {

TEST(CheckedChunkFactoryTest, checked_schema_construction) {
    Fields fields;
    fields.emplace_back(std::make_shared<Field>(0, "c0", TYPE_INT, false));
    fields.emplace_back(std::make_shared<Field>(1, "c1", TYPE_VARCHAR, true));
    Schema schema(std::move(fields), DUP_KEYS, {});

    auto chunk_or = CheckedChunkFactory::new_chunk_checked(schema, 8);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();
    ASSERT_EQ(2, chunk_or.value()->num_columns());

    auto mutable_chunk_or = CheckedChunkFactory::new_mutable_chunk_checked(schema, 8);
    ASSERT_TRUE(mutable_chunk_or.ok()) << mutable_chunk_or.status();
    ASSERT_EQ(2, mutable_chunk_or.value()->num_columns());

    auto pooled_chunk_or = CheckedChunkFactory::new_chunk_pooled_checked(schema, 8);
    ASSERT_TRUE(pooled_chunk_or.ok()) << pooled_chunk_or.status();
    std::unique_ptr<Chunk> pooled_chunk(pooled_chunk_or.value());
    ASSERT_EQ(2, pooled_chunk->num_columns());
}

} // namespace starrocks
