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

#include "formats/csv/csv_reader.h"

#include <gtest/gtest.h>

#include "runtime/types.h"

namespace starrocks::csv {

// Mock CSVReader for testing - implements the pure virtual function
class MockCSVReader : public starrocks::CSVReader {
public:
    explicit MockCSVReader(const starrocks::CSVParseOptions& parse_options) : CSVReader(parse_options) {}

protected:
    starrocks::Status _fill_buffer() override {
        // Mock implementation - not needed for split_record tests
        return starrocks::Status::OK();
    }

    char* _find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) override {
        // Mock implementation - not needed for split_record tests
        return nullptr;
    }
};

class CSVReaderTest : public ::testing::Test {
public:
    CSVReaderTest() = default;

protected:
    void SetUp() override {
        _parse_options.column_delimiter = ",";
        _parse_options.row_delimiter = "\n";
        _parse_options.trim_space = false;
    }

    starrocks::CSVParseOptions _parse_options;
};

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_single_delimiter) {
    MockCSVReader reader(_parse_options);

    // Test basic splitting
    starrocks::CSVReader::Record record1{"a,b,c", 5};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_empty_fields) {
    MockCSVReader reader(_parse_options);

    // Test empty fields
    starrocks::CSVReader::Record record1{",,", 2};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
    EXPECT_EQ("", fields1[1].to_string());
    EXPECT_EQ("", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_ends_with_delimiter) {
    MockCSVReader reader(_parse_options);

    // Test string ending with delimiter
    starrocks::CSVReader::Record record1{"a,b,", 4};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_starts_with_delimiter) {
    MockCSVReader reader(_parse_options);

    // Test string starting with delimiter
    starrocks::CSVReader::Record record1{",a,b", 4};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
    EXPECT_EQ("a", fields1[1].to_string());
    EXPECT_EQ("b", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_single_field) {
    MockCSVReader reader(_parse_options);

    // Test single field (no delimiters)
    starrocks::CSVReader::Record record1{"single_field", 12};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(1, fields1.size());
    EXPECT_EQ("single_field", fields1[0].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_empty_string) {
    MockCSVReader reader(_parse_options);

    // Test empty string
    starrocks::CSVReader::Record record1{"", 0};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(1, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_multi_character_delimiter) {
    starrocks::CSVParseOptions options;
    options.column_delimiter = "||";
    options.row_delimiter = "\n";
    options.trim_space = false;

    MockCSVReader reader(options);

    // Test multi-character delimiter
    starrocks::CSVReader::Record record1{"a||b||c", 7};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_with_trim_space) {
    starrocks::CSVParseOptions options;
    options.column_delimiter = ",";
    options.row_delimiter = "\n";
    options.trim_space = true;

    MockCSVReader reader(options);

    // Test with trim_space enabled
    starrocks::CSVReader::Record record1{" a , b , c ", 11};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_large_data) {
    MockCSVReader reader(_parse_options);

    // Test with larger data to verify performance optimization
    std::string large_data;
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) large_data += ",";
        large_data += "field" + std::to_string(i);
    }

    starrocks::CSVReader::Record record1{large_data.c_str(), large_data.size()};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(1000, fields1.size());
    EXPECT_EQ("field0", fields1[0].to_string());
    EXPECT_EQ("field999", fields1[999].to_string());
}

} // namespace starrocks::csv