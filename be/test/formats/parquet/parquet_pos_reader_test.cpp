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
#include "storage/predicate_tree/predicate_tree_fwd.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "common/logging.h"

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

// Test ParquetPosReader prepare method
TEST_F(ParquetPosReaderTest, TestPrepare) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Prepare should always return OK
    ASSERT_TRUE(reader.prepare().ok());
}

// Test ParquetPosReader get_levels method
TEST_F(ParquetPosReaderTest, TestGetLevels) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Call get_levels (should not crash)
    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    reader.get_levels(&def_levels, &rep_levels, &num_levels);
    
    // Values should remain nullptr/0 as the method is a no-op
    ASSERT_EQ(def_levels, nullptr);
    ASSERT_EQ(rep_levels, nullptr);
    ASSERT_EQ(num_levels, 0u);
}

// Test ParquetPosReader set_need_parse_levels method
TEST_F(ParquetPosReaderTest, TestSetNeedParseLevels) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Call set_need_parse_levels (should not crash)
    reader.set_need_parse_levels(true);
    reader.set_need_parse_levels(false);
}

// Test ParquetPosReader collect_column_io_range method
TEST_F(ParquetPosReaderTest, TestCollectColumnIoRange) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Call collect_column_io_range (should not crash)
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    reader.collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);
    reader.collect_column_io_range(nullptr, nullptr, ColumnIOType::PAGES, false);
}

// Test ParquetPosReader select_offset_index method
TEST_F(ParquetPosReaderTest, TestSelectOffsetIndex) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Call select_offset_index (should not crash)
    SparseRange<uint64_t> range;
    reader.select_offset_index(range, 100);
}

// Test ParquetPosReader row_group_zone_map_filter method
TEST_F(ParquetPosReaderTest, TestRowGroupZoneMapFilter) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Call row_group_zone_map_filter
    std::vector<const ColumnPredicate*> predicates;
    auto result = reader.row_group_zone_map_filter(predicates, CompoundNodeType::AND, 100, 50);
    
    // Should return false (position column does not support zone map filtering)
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value());
}

// Test ParquetPosReader page_index_zone_map_filter method
TEST_F(ParquetPosReaderTest, TestPageIndexZoneMapFilter) {
    // Create a ParquetPosReader
    ParquetPosReader reader;
    
    // Call page_index_zone_map_filter
    std::vector<const ColumnPredicate*> predicates;
    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 100, 50);
    
    // Should return false (position column does not support page index filtering)
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value());
}

} // namespace starrocks::parquet