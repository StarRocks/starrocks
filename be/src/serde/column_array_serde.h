// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <stdint.h>

namespace starrocks {
class TypeDescriptor;
}

namespace starrocks::vectorized {
class Column;
}

namespace starrocks::serde {

// ColumnArraySerde used to serialize/deserialize a column to/from an in-memory array.
class ColumnArraySerde {
public:
    // 0 means does not support the type of column
    static int64_t max_serialized_size(const vectorized::Column& column, const int encode_level);

    // Return nullptr on error.
    static uint8_t* serialize(const vectorized::Column& column, uint8_t* buff, bool sorted, const int encode_level);

    // Return nullptr on error.
    static const uint8_t* deserialize(const uint8_t* buff, vectorized::Column* column, bool sorted,
                                      const int encode_level);
};

} //  namespace starrocks::serde
