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

namespace starrocks {
struct TypeDescriptor;
}

namespace starrocks {
class Column;
}

namespace starrocks::serde {

// ColumnArraySerde used to serialize/deserialize a column to/from an in-memory array.
class ColumnArraySerde {
public:
    // 0 means does not support the type of column
    static int64_t max_serialized_size(const Column& column, const int encode_level = 0);

    // Return nullptr on error.
    static uint8_t* serialize(const Column& column, uint8_t* buff, bool sorted = false, const int encode_level = 0);

    // Return nullptr on error.
    static const uint8_t* deserialize(const uint8_t* buff, Column* column, bool sorted = false,
                                      const int encode_level = 0);
};

} //  namespace starrocks::serde
