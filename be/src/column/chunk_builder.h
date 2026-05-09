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

#pragma once

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

class ChunkBuilder {
public:
    // Get non nullable version schema
    static SchemaPtr get_non_nullable_schema(const starrocks::SchemaPtr& schema, const std::vector<int>* keys);

    static ColumnId max_column_id(const Schema& schema);

    static Chunk* new_chunk_pooled(const Schema& schema, size_t n);

    // Create an empty chunk according to the |schema| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const Schema& schema, size_t n);

    static MutableChunkPtr new_mutable_chunk(const Schema& schema, size_t n);

    // Create a vectorized column from field .
    // REQUIRE: |type| must be scalar type.
    static MutableColumnPtr column_from_field_type(LogicalType type, bool nullable);

    // Create a vectorized column from field.
    static MutableColumnPtr column_from_field(const Field& field);
};

} // namespace starrocks
