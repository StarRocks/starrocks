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

#include "formats/parquet/parquet_pos_reader.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "storage/range.h"

namespace starrocks::parquet {

class ParquetPosReaderTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test basic functionality of ParquetPosReader
TEST_F(ParquetPosReaderTest, TestReadRangeWithoutFilter) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare the reader (should always return OK)
    ASSERT_TRUE(reader.prepare().ok());
    
    // Create a range representing rows 100-105 (absolute positions in file)
    Range<uint64_t> range(100, 105);
    
    // Create a column to store the results
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    
    // Read the range
    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());
    
    // Check that we got 5 rows (positions 100, 101, 102, 103, 104)
    ASSERT_EQ(column->size(), 5);
    
    // Check the values
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 100 + i);
    }
}

// Test ParquetPosReader with a filter
TEST_F(ParquetPosReaderTest, TestReadRangeWithFilter) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare the reader
    ASSERT_TRUE(reader.prepare().ok());
    
    // Create a range representing rows 200-205 (absolute positions in file)
    Range<uint64_t> range(200, 205);
    
    // Create a filter that selects only positions 201 and 203
    Filter filter = {false, true, false, true, false}; // Size 5, positions 1 and 3 (0-indexed)
    
    // Create a column to store the results
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    
    // Read the range with filter
    ASSERT_TRUE(reader.read_range(range, &filter, column).ok());
    
    // Check that we got 2 rows (positions 201 and 203)
    ASSERT_EQ(column->size(), 2);
    
    // Check the values
    ASSERT_EQ(column->get(0).get_int64(), 201);
    ASSERT_EQ(column->get(1).get_int64(), 203);
}

// Test ParquetPosReader with a larger range
TEST_F(ParquetPosReaderTest, TestReadLargeRange) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare the reader
    ASSERT_TRUE(reader.prepare().ok());
    
    // Create a range representing rows 1000-1010 (absolute positions in file)
    Range<uint64_t> range(1000, 1010);
    
    // Create a column to store the results
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    
    // Read the range
    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());
    
    // Check that we got 10 rows
    ASSERT_EQ(column->size(), 10);
    
    // Check the values
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 1000 + i);
    }
}

// Test ParquetPosReader with empty range
TEST_F(ParquetPosReaderTest, TestReadEmptyRange) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare the reader
    ASSERT_TRUE(reader.prepare().ok());
    
    // Create an empty range
    Range<uint64_t> range(500, 500);
    
    // Create a column to store the results
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    
    // Read the range
    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());
    
    // Check that we got 0 rows
    ASSERT_EQ(column->size(), 0);
}

// Test ParquetPosReader with filter that selects no rows
TEST_F(ParquetPosReaderTest, TestReadRangeWithNoSelectedRows) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare the reader
    ASSERT_TRUE(reader.prepare().ok());
    
    // Create a range representing rows 300-303 (absolute positions in file)
    Range<uint64_t> range(300, 303);
    
    // Create a filter that selects no rows
    Filter filter = {false, false, false}; // Size 3, no selected rows
    
    // Create a column to store the results
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    
    // Read the range with filter
    ASSERT_TRUE(reader.read_range(range, &filter, column).ok());
    
    // Check that we got 0 rows
    ASSERT_EQ(column->size(), 0);
}

// Test ParquetPosReader with all rows selected by filter
TEST_F(ParquetPosReaderTest, TestReadRangeWithAllRowsSelected) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare the reader
    ASSERT_TRUE(reader.prepare().ok());
    
    // Create a range representing rows 400-404 (absolute positions in file)
    Range<uint64_t> range(400, 404);
    
    // Create a filter that selects all rows
    Filter filter = {true, true, true, true}; // Size 4, all selected
    
    // Create a column to store the results
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    
    // Read the range with filter
    ASSERT_TRUE(reader.read_range(range, &filter, column).ok());
    
    // Check that we got 4 rows
    ASSERT_EQ(column->size(), 4);
    
    // Check the values
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 400 + i);
    }
}

} // namespace starrocks::parquet