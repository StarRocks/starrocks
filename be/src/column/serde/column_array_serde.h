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

#include <cstdint>

#include "common/statusor.h"

namespace starrocks {
struct TypeDescriptor;
}

namespace starrocks {
class Column;
}

namespace starrocks::serde {

// True if |column| is a NullableColumn whose every row is NULL, i.e. the only shape whose layout
// ENCODE_ALL_NULL actually changes. Exposed so a caller can decide whether to enable the bit for a
// whole chunk: if no column qualifies, enabling it is pure cost (a tag byte per nullable column
// plus a per-column encode_level in ChunkPB) for no benefit. Shares its definition with the serde,
// so the two can never disagree about what "all NULL" means.
bool is_all_null_column(const Column& column);

// ColumnArraySerde used to serialize/deserialize a column to/from an in-memory array.
class ColumnArraySerde {
public:
    // 0 means does not support the type of column
    static int64_t max_serialized_size(const Column& column, const int encode_level = 0);

    static StatusOr<uint8_t*> serialize(const Column& column, uint8_t* buff, bool sorted = false,
                                        const int encode_level = 0);

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, Column* column,
                                                bool sorted = false, const int encode_level = 0);
};

} //  namespace starrocks::serde
