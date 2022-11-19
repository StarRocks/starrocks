// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
