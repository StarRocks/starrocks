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

#include "formats/csv/csv_reader.h"
#include "runtime/types.h"

namespace starrocks::csv {

class CSVReaderTest : public ::testing::Test {
public:
    CSVReaderTest() = default;

protected:
    void SetUp() override {
        _parse_options.column_delimiter = ",";
        _parse_options.row_delimiter = "\n";
        _parse_options.trim_space = false;
    }

    CSVReader::ParseOptions _parse_options;
};

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_single_delimiter) {
    CSVReader reader(_parse_options);
    
    // Test basic splitting
    CSVReader::Record record1{"a,b,c", 5};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_empty_fields) {
    CSVReader reader(_parse_options);
    
    // Test empty fields
    CSVReader::Record record1{",,", 3};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
    EXPECT_EQ("", fields1[1].to_string());
    EXPECT_EQ("", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_ends_with_delimiter) {
    CSVReader reader(_parse_options);
    
    // Test string ending with delimiter
    CSVReader::Record record1{"a,b,", 4};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_starts_with_delimiter) {
    CSVReader reader(_parse_options);
    
    // Test string starting with delimiter
    CSVReader::Record record1{",a,b", 4};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
    EXPECT_EQ("a", fields1[1].to_string());
    EXPECT_EQ("b", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_single_field) {
    CSVReader reader(_parse_options);
    
    // Test single field (no delimiters)
    CSVReader::Record record1{"single_field", 11};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(1, fields1.size());
    EXPECT_EQ("single_field", fields1[0].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_empty_string) {
    CSVReader reader(_parse_options);
    
    // Test empty string
    CSVReader::Record record1{"", 0};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(1, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_multi_character_delimiter) {
    CSVReader::ParseOptions options;
    options.column_delimiter = "||";
    options.row_delimiter = "\n";
    options.trim_space = false;
    
    CSVReader reader(options);
    
    // Test multi-character delimiter
    CSVReader::Record record1{"a||b||c", 7};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_with_trim_space) {
    CSVReader::ParseOptions options;
    options.column_delimiter = ",";
    options.row_delimiter = "\n";
    options.trim_space = true;
    
    CSVReader reader(options);
    
    // Test with trim_space enabled
    CSVReader::Record record1{" a , b , c ", 11};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_large_data) {
    CSVReader reader(_parse_options);
    
    // Test with larger data to verify performance optimization
    std::string large_data;
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) large_data += ",";
        large_data += "field" + std::to_string(i);
    }
    
    CSVReader::Record record1{large_data.c_str(), large_data.size()};
    CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);
    
    EXPECT_EQ(1000, fields1.size());
    EXPECT_EQ("field0", fields1[0].to_string());
    EXPECT_EQ("field999", fields1[999].to_string());
}

} // namespace starrocks::csv