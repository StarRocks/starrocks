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

#include "formats/parquet/iceberg_row_id_reader.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "storage/column_predicate.h"
#include "storage/range.h"
#include "types/datum.h"

namespace starrocks::parquet {

class IcebergRowIdReaderTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    ObjectPool _pool;
};

// ==================== Basic read_range tests ====================

TEST_F(IcebergRowIdReaderTest, TestReadRangeWithoutFilter) {
    IcebergRowIdReader reader(1000);
    ASSERT_TRUE(reader.prepare().ok());

    Range<uint64_t> range(0, 5);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);

    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());
    ASSERT_EQ(column->size(), 5);
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 1000 + i);
    }
}

TEST_F(IcebergRowIdReaderTest, TestReadRangeIgnoresFilter) {
    // IcebergRowIdReader ignores filter and outputs all rows (consistent with other reserved
    // column readers). The caller applies chunk->filter_range() uniformly afterwards.
    IcebergRowIdReader reader(500);
    ASSERT_TRUE(reader.prepare().ok());

    Range<uint64_t> range(0, 5);
    Filter filter = {true, false, true, false, true};

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);

    ASSERT_TRUE(reader.read_range(range, &filter, column).ok());
    // All 5 rows should be output regardless of filter
    ASSERT_EQ(column->size(), 5);
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 500 + i);
    }
}

TEST_F(IcebergRowIdReaderTest, TestReadRangeWithOffset) {
    // Simulate reading from a row group that doesn't start at row 0
    IcebergRowIdReader reader(1000);
    ASSERT_TRUE(reader.prepare().ok());

    // Range [10, 15) means rows 10..14 within the row group
    Range<uint64_t> range(10, 15);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);

    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());
    ASSERT_EQ(column->size(), 5);
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 1010 + i);
    }
}

TEST_F(IcebergRowIdReaderTest, TestReadEmptyRange) {
    IcebergRowIdReader reader(0);
    ASSERT_TRUE(reader.prepare().ok());

    Range<uint64_t> range(5, 5);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);

    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());
    ASSERT_EQ(column->size(), 0);
}

TEST_F(IcebergRowIdReaderTest, TestFillDstColumn) {
    IcebergRowIdReader reader(100);
    ASSERT_TRUE(reader.prepare().ok());

    Range<uint64_t> range(0, 3);
    ColumnPtr src = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    ASSERT_TRUE(reader.read_range(range, nullptr, src).ok());

    ColumnPtr dst = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    ASSERT_TRUE(reader.fill_dst_column(dst, src).ok());
    ASSERT_EQ(dst->size(), 3);
    ASSERT_EQ(dst->get(0).get_int64(), 100);
    ASSERT_EQ(dst->get(1).get_int64(), 101);
    ASSERT_EQ(dst->get(2).get_int64(), 102);
}

// ==================== No-op method tests ====================

TEST_F(IcebergRowIdReaderTest, TestNoOpMethods) {
    IcebergRowIdReader reader(0);
    ASSERT_TRUE(reader.prepare().ok());

    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    reader.get_levels(&def_levels, &rep_levels, &num_levels);
    reader.set_need_parse_levels(true);

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    reader.collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);
    ASSERT_TRUE(ranges.empty());

    SparseRange<uint64_t> sparse_range;
    reader.select_offset_index(sparse_range, 100);
}

// ==================== Zone map filter tests ====================

TEST_F(IcebergRowIdReaderTest, TestRowGroupZoneMapFilterEQ) {
    // first_row_id=1000, row_group starts at row 0, 100 rows
    // zone map: [1000, 1099]
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // EQ predicate matching within range
    Datum eq_val(static_cast<int64_t>(1050));
    ColumnPredicate* eq_pred = _pool.add(new_column_eq_predicate_from_datum(type_info, 0, eq_val));
    std::vector<const ColumnPredicate*> predicates = {eq_pred};

    auto result = reader.row_group_zone_map_filter(predicates, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    // Should NOT be filtered out (predicate falls within zone map)
    ASSERT_FALSE(result.value());
}

TEST_F(IcebergRowIdReaderTest, TestRowGroupZoneMapFilterOutOfRange) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // EQ predicate outside range: row_ids are [1000,1099], predicate is 2000
    Datum eq_val(static_cast<int64_t>(2000));
    ColumnPredicate* eq_pred = _pool.add(new_column_eq_predicate_from_datum(type_info, 0, eq_val));
    std::vector<const ColumnPredicate*> predicates = {eq_pred};

    auto result = reader.row_group_zone_map_filter(predicates, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    // Should be filtered out
    ASSERT_TRUE(result.value());
}

// ==================== Page index zone map filter tests ====================

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterEQ) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // EQ _row_id = 1005
    Datum eq_val(static_cast<int64_t>(1005));
    ColumnPredicate* eq_pred = _pool.add(new_column_eq_predicate_from_datum(type_info, 0, eq_val));
    std::vector<const ColumnPredicate*> predicates = {eq_pred};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value());
    // Should produce a single range [5, 6)
    ASSERT_EQ(row_ranges.size(), 1);
    ASSERT_EQ(row_ranges[0].begin(), 5);
    ASSERT_EQ(row_ranges[0].end(), 6);
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterGE) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // GE _row_id >= 1090
    Datum ge_val(static_cast<int64_t>(1090));
    ColumnPredicate* ge_pred = _pool.add(new_column_ge_predicate_from_datum(type_info, 0, ge_val));
    std::vector<const ColumnPredicate*> predicates = {ge_pred};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value());
    // Should produce range [90, 100)
    ASSERT_EQ(row_ranges.size(), 1);
    ASSERT_EQ(row_ranges[0].begin(), 90);
    ASSERT_EQ(row_ranges[0].end(), 100);
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterLT) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // LT _row_id < 1003
    Datum lt_val(static_cast<int64_t>(1003));
    ColumnPredicate* lt_pred = _pool.add(new_column_lt_predicate_from_datum(type_info, 0, lt_val));
    std::vector<const ColumnPredicate*> predicates = {lt_pred};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value());
    // Should produce range [0, 3)
    ASSERT_EQ(row_ranges.size(), 1);
    ASSERT_EQ(row_ranges[0].begin(), 0);
    ASSERT_EQ(row_ranges[0].end(), 3);
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterNoFilteringWhenAllRowsMatch) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // GE _row_id >= 0 — all rows match, no filtering possible
    Datum ge_val(static_cast<int64_t>(0));
    ColumnPredicate* ge_pred = _pool.add(new_column_ge_predicate_from_datum(type_info, 0, ge_val));
    std::vector<const ColumnPredicate*> predicates = {ge_pred};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    // Returns false when all rows match (no filtering benefit)
    ASSERT_FALSE(result.value());
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterAND) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // AND: _row_id >= 1010 AND _row_id < 1020
    Datum ge_val(static_cast<int64_t>(1010));
    ColumnPredicate* ge_pred = _pool.add(new_column_ge_predicate_from_datum(type_info, 0, ge_val));
    Datum lt_val(static_cast<int64_t>(1020));
    ColumnPredicate* lt_pred = _pool.add(new_column_lt_predicate_from_datum(type_info, 0, lt_val));
    std::vector<const ColumnPredicate*> predicates = {ge_pred, lt_pred};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value());
    // Should produce range [10, 20)
    ASSERT_EQ(row_ranges.size(), 1);
    ASSERT_EQ(row_ranges[0].begin(), 10);
    ASSERT_EQ(row_ranges[0].end(), 20);
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterOR) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // OR: _row_id = 1005 OR _row_id = 1010
    Datum val1(static_cast<int64_t>(1005));
    ColumnPredicate* eq1 = _pool.add(new_column_eq_predicate_from_datum(type_info, 0, val1));
    Datum val2(static_cast<int64_t>(1010));
    ColumnPredicate* eq2 = _pool.add(new_column_eq_predicate_from_datum(type_info, 0, val2));
    std::vector<const ColumnPredicate*> predicates = {eq1, eq2};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::OR, 0, 100);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value());
    // Should produce two ranges: [5, 6) and [10, 11)
    ASSERT_EQ(row_ranges.size(), 2);
    ASSERT_EQ(row_ranges[0].begin(), 5);
    ASSERT_EQ(row_ranges[0].end(), 6);
    ASSERT_EQ(row_ranges[1].begin(), 10);
    ASSERT_EQ(row_ranges[1].end(), 11);
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterIsNull) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // IS NULL: _row_id is never null, so no rows match -> AND with empty range = empty
    ColumnPredicate* is_null = _pool.add(new_column_null_predicate(type_info, 0, true));
    std::vector<const ColumnPredicate*> predicates = {is_null};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    // Empty intersection means filtering happened, but no rows left
    ASSERT_TRUE(result.value());
    ASSERT_TRUE(row_ranges.empty());
}

TEST_F(IcebergRowIdReaderTest, TestPageIndexFilterIsNotNull) {
    IcebergRowIdReader reader(1000);

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_BIGINT);

    // IS NOT NULL: all rows match for _row_id
    ColumnPredicate* is_not_null = _pool.add(new_column_null_predicate(type_info, 0, false));
    std::vector<const ColumnPredicate*> predicates = {is_not_null};

    SparseRange<uint64_t> row_ranges;
    auto result = reader.page_index_zone_map_filter(predicates, &row_ranges, CompoundNodeType::AND, 0, 100);
    ASSERT_TRUE(result.ok());
    // All rows match, no filtering benefit
    ASSERT_FALSE(result.value());
}

// ==================== FixedValueColumnReader for row lineage fallback ====================

TEST_F(IcebergRowIdReaderTest, TestFixedValueColumnReaderForNullRowId) {
    // When first_row_id is unavailable (late materialization on pre-v3 files),
    // a FixedValueColumnReader with kNullDatum is used.
    auto reader = std::make_unique<FixedValueColumnReader>(kNullDatum);
    ASSERT_TRUE(reader->prepare().ok());

    Range<uint64_t> range(0, 5);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);

    ASSERT_TRUE(reader->read_range(range, nullptr, column).ok());
    ASSERT_EQ(column->size(), 5);
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(column->get(i).is_null());
    }
}

TEST_F(IcebergRowIdReaderTest, TestFixedValueColumnReaderForSequenceNumber) {
    // When the Parquet file has no physical _last_updated_sequence_number column,
    // a FixedValueColumnReader with the file-level dataSequenceNumber is used.
    int64_t sequence_number = 42;
    auto reader = std::make_unique<FixedValueColumnReader>(Datum(sequence_number));
    ASSERT_TRUE(reader->prepare().ok());

    Range<uint64_t> range(0, 5);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);

    ASSERT_TRUE(reader->read_range(range, nullptr, column).ok());
    ASSERT_EQ(column->size(), 5);
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(column->get(i).get_int64(), 42);
    }
}

TEST_F(IcebergRowIdReaderTest, TestFixedValueColumnReaderWithFilter) {
    int64_t sequence_number = 99;
    auto reader = std::make_unique<FixedValueColumnReader>(Datum(sequence_number));
    ASSERT_TRUE(reader->prepare().ok());

    Range<uint64_t> range(0, 4);
    Filter filter = {true, false, true, false};

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);

    ASSERT_TRUE(reader->read_range(range, &filter, column).ok());
    // FixedValueColumnReader fills all rows in range (filter is applied later by caller)
    ASSERT_EQ(column->size(), 4);
    for (size_t i = 0; i < column->size(); i++) {
        ASSERT_EQ(column->get(i).get_int64(), 99);
    }
}

// ==================== Simulating post-compaction scenario ====================
// After compaction, _row_id and _last_updated_sequence_number are physical columns.
// This is handled by the standard parquet ColumnReader (created via _create_reserved_iceberg_column_reader).
// Here we verify the fallback readers produce distinct per-row values (IcebergRowIdReader)
// vs. constant values (FixedValueColumnReader).

TEST_F(IcebergRowIdReaderTest, TestRowIdReaderVsFixedValueDifferentBehavior) {
    // IcebergRowIdReader: generates distinct row_id per row (first_row_id + position)
    IcebergRowIdReader row_id_reader(5000);
    ASSERT_TRUE(row_id_reader.prepare().ok());

    Range<uint64_t> range(0, 3);
    ColumnPtr row_id_col =
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    ASSERT_TRUE(row_id_reader.read_range(range, nullptr, row_id_col).ok());

    // FixedValueColumnReader: returns the same value for every row
    auto seq_reader = std::make_unique<FixedValueColumnReader>(Datum(static_cast<int64_t>(10)));
    ASSERT_TRUE(seq_reader->prepare().ok());

    ColumnPtr seq_col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_TRUE(seq_reader->read_range(range, nullptr, seq_col).ok());

    ASSERT_EQ(row_id_col->size(), 3);
    ASSERT_EQ(seq_col->size(), 3);

    // Row IDs should be distinct and increasing
    ASSERT_EQ(row_id_col->get(0).get_int64(), 5000);
    ASSERT_EQ(row_id_col->get(1).get_int64(), 5001);
    ASSERT_EQ(row_id_col->get(2).get_int64(), 5002);

    // Sequence numbers should all be the same (file-level constant)
    ASSERT_EQ(seq_col->get(0).get_int64(), 10);
    ASSERT_EQ(seq_col->get(1).get_int64(), 10);
    ASSERT_EQ(seq_col->get(2).get_int64(), 10);
}

// ==================== Large first_row_id (post-compaction) ====================

TEST_F(IcebergRowIdReaderTest, TestLargeFirstRowId) {
    // After compaction, first_row_id can be very large
    int64_t large_first_row_id = 1000000000000LL;
    IcebergRowIdReader reader(large_first_row_id);
    ASSERT_TRUE(reader.prepare().ok());

    Range<uint64_t> range(0, 3);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), false);
    ASSERT_TRUE(reader.read_range(range, nullptr, column).ok());

    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(column->get(0).get_int64(), large_first_row_id);
    ASSERT_EQ(column->get(1).get_int64(), large_first_row_id + 1);
    ASSERT_EQ(column->get(2).get_int64(), large_first_row_id + 2);
}

} // namespace starrocks::parquet
