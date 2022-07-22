// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "formats/csv/converter.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::vectorized::csv {
// NOLINTNEXTLINE
TEST(ConverterTest, test_get_collection_delimiter) {
    // Rule refer to:
    // https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
    std::vector<char> separators;

    for (char i = 1; i <= 8; ++i) {
        separators.push_back(i);
    }

    separators.push_back(11);

    for (char i = 14; i <= 26; i++) {
        separators.push_back(i);
    }

    for (char i = 28; i <= 31; i++) {
        separators.push_back(i);
    }

    for (char i = -128; i <= -1; i++) {
        separators.push_back(i);
    }

    const char DEFAULT_MAPKEY_DELIMITER = '\003';
    // Start to check, ignore first element in separators, because array delimiter start from second element.
    for (size_t i = 1, nested_array_level = 1; i < separators.size(); i++) {
        EXPECT_EQ(csv::get_collection_delimiter(DEFAULT_MAPKEY_DELIMITER, nested_array_level++), separators[i]);
    }
}
} // namespace starrocks::vectorized::csv