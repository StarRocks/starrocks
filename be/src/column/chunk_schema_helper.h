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

#include <cstddef>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/column_id.h"

namespace starrocks {

class Schema;

class ChunkSchemaHelper {
public:
    // Get non nullable version schema
    static SchemaPtr get_non_nullable_schema(const SchemaPtr& schema, const std::vector<int>* keys);

    static ColumnId max_column_id(const Schema& schema);

    // Get char column indexes
    static std::vector<size_t> get_char_field_indexes(const Schema& schema);

    static ChunkPtr createDummyChunk();
};

} // namespace starrocks
