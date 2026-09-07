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

#include "storage/rowset/cast_column_iterator.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/series_column_iterator.h"
#include "storage/tablet_schema_helper.h"
#include "storage/types.h"
#include "storage_primitive/column_predicate_factory.h"
#include "testutil/schema_test_helper.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

class CastColumnIteratorTestBase : public ::testing::Test {};

class CastColumnIteratorWithDefaultValueColumnIteratorTest : public CastColumnIteratorTestBase {};

TEST_F(CastColumnIteratorWithDefaultValueColumnIteratorTest, test01) {
    auto source_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    auto target_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
    auto source_iter =
            std::make_unique<DefaultValueColumnIterator>(true, "NULL", true, get_type_info(source_type.type), 0, 2);
    auto cast_iter = new CastColumnIterator(std::move(source_iter), source_type, target_type, true);
    DeferOp defer([&]() { delete cast_iter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(cast_iter->init(opts));
    auto column = NullableColumn(Int64Column::create(), NullColumn ::create());
    auto range = SparseRange<>{0, 2};
    ASSERT_OK(cast_iter->next_batch(range, &column));
    ASSERT_EQ(2, column.size());
    ASSERT_TRUE(column.is_null(0));
    ASSERT_TRUE(column.is_null(1));

    // meaningless test, just for better code coverage
    ASSERT_OK(cast_iter->get_row_ranges_by_bloom_filter({}, &range));
}

TEST_F(CastColumnIteratorWithDefaultValueColumnIteratorTest, test02) {
    auto source_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    auto target_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
    auto source_iter =
            std::make_unique<DefaultValueColumnIterator>(true, "10", false, get_type_info(source_type.type), 0, 2);
    auto cast_iter = new CastColumnIterator(std::move(source_iter), source_type, target_type, true);
    DeferOp defer([&]() { delete cast_iter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(cast_iter->init(opts));
    auto column = NullableColumn(Int64Column::create(), NullColumn ::create());
    auto range = SparseRange<>{0, 2};
    ASSERT_OK(cast_iter->next_batch(range, &column));
    ASSERT_EQ(2, column.size());
    ASSERT_FALSE(column.is_null(0));
    ASSERT_FALSE(column.is_null(1));
    ASSERT_EQ(10, column.get(0).get_int64());
    ASSERT_EQ(10, column.get(1).get_int64());
}

TEST_F(CastColumnIteratorWithDefaultValueColumnIteratorTest, test_get_io_range_vec) {
    auto source_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    auto target_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
    auto source_iter =
            std::make_unique<DefaultValueColumnIterator>(true, "10", false, get_type_info(source_type.type), 0, 2);
    auto cast_iter = new CastColumnIterator(std::move(source_iter), source_type, target_type, true);
    DeferOp defer([&]() { delete cast_iter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(cast_iter->init(opts));
    auto column = NullableColumn(Int64Column::create(), NullColumn ::create());
    auto range = SparseRange<>{0, 2};
    auto status_or = cast_iter->get_io_range_vec(range, &column);
    // will return default value's get_io_range_vec, which is expected to be empty
    ASSERT_TRUE(status_or.ok());
    ASSERT_EQ((*status_or).size(), 0);
}

namespace {
struct TestParameter {
    bool nullable_source;
    bool nullable_target;
};
} // namespace

class CastColumnIteratorWithNumericTypeTest : public CastColumnIteratorTestBase,
                                              public ::testing::WithParamInterface<TestParameter> {};

TEST_P(CastColumnIteratorWithNumericTypeTest, test) {
    auto source_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
    auto target_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
    auto source_iter = std::make_unique<SeriesColumnIterator<int16_t>>(0, 31);
    auto nullable_source = GetParam().nullable_source;
    auto cast_iter = new CastColumnIterator(std::move(source_iter), source_type, target_type, nullable_source);
    DeferOp defer([&]() { delete cast_iter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(cast_iter->init(opts));
    MutableColumnPtr column = (GetParam().nullable_target) ? (MutableColumnPtr)NullableColumn::create(
                                                                     Int64Column::create(), NullColumn::create())
                                                           : (MutableColumnPtr)Int64Column::create();
    auto n = size_t{10};
    ASSERT_OK(cast_iter->next_batch(&n, column.get()));
    ASSERT_EQ(10, n);
    ASSERT_EQ(n, column->size());
    ASSERT_EQ(n, cast_iter->get_current_ordinal());
    EXPECT_FALSE(column->has_null());
    for (int i = 0; i < n; i++) {
        EXPECT_FALSE(column->is_null(i));
        EXPECT_EQ(i, column->get(i).get_int64());
    }

    n = 30;
    ASSERT_OK(cast_iter->next_batch(&n, column.get()));
    ASSERT_EQ(22, n);
    ASSERT_EQ(32, column->size());
    ASSERT_EQ(32, cast_iter->get_current_ordinal());
    for (int i = 0; i < 32; i++) {
        EXPECT_FALSE(column->is_null(i));
        EXPECT_EQ(i, column->get(i).get_int64());
    }
    ASSERT_OK(cast_iter->next_batch(&n, column.get()));
    ASSERT_EQ(0, n);
    ASSERT_EQ(32, column->size());

    auto range = SparseRange<>{};
    range.add({5, 15});
    range.add({20, 30});
    column->reset_column();
    ASSERT_OK(cast_iter->next_batch(range, column.get()));
    ASSERT_EQ(20, column->size());
    for (int i = 0; i < 20; i++) {
        if (i < 10) {
            EXPECT_EQ(i + 5, column->get(i).get_int64());
        } else {
            EXPECT_EQ((i - 10) + 20, column->get(i).get_int64());
        }
    }

    auto rowids = std::vector<rowid_t>{6, 8, 9, 24};
    column->reset_column();
    ASSERT_OK(cast_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), column.get()));
    ASSERT_EQ(rowids.size(), column->size());
    for (int i = 0; i < rowids.size(); i++) {
        EXPECT_EQ(rowids[i], column->get(i).get_int64());
    }
}
INSTANTIATE_TEST_SUITE_P(numeric_test, CastColumnIteratorWithNumericTypeTest,
                         testing::Values(TestParameter{false, false}, TestParameter{false, true},
                                         TestParameter{true, false}, TestParameter{true, true}));

// A zone map records the min and max the source column itself computed, but ColumnReader parses those
// bytes back with the *predicate's* type. Behind a CastColumnIterator the two types differ by
// construction, so these tests pin down which pairs may keep sharing a zone map and which have to hand
// the whole column back instead.
//
// Note the writer option below: be/test/storage/rowset/flat_json_column_rw_test.cpp turns
// need_zone_map off at all three of its writer sites, which is why a page-level zone map had never
// been read through a cast until now.
class CastColumnIteratorZoneMapTest : public CastColumnIteratorTestBase {
protected:
    // Written in ascending order, 4096 rows over pages of a few dozen rows each. A page therefore holds
    // a run of consecutive numbers that crosses a decade, and that is what makes the numeric order and
    // the lexicographic order of one page's own min/max disagree.
    static constexpr int kNumRows = 4096;
    static constexpr uint32_t kDataPageSize = 128;
    static constexpr const char* kTestDir = "/cast_column_iterator_zone_map_test";

    void SetUp() override {
        TabletSchemaPB schema_pb;
        SchemaTestHelper::add_column_pb(&schema_pb, "pk", "BIGINT", true);
        SchemaTestHelper::add_column_pb(&schema_pb, "v1", "INT", false);
        _dummy_segment_schema = TabletSchema::create(schema_pb);

        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
    }

    void write_column_with_zone_map(const TabletColumn& column, const Column& src) {
        _fname = std::string(kTestDir) + "/" + generate_uuid_string() + ".data";
        _segment = std::make_shared<Segment>(_fs, FileInfo{_fname}, 1, _dummy_segment_schema, nullptr);
        // ColumnReader::num_rows() reads through to the segment, and this one has no footer to read it
        // from. The row count is what the fallback range spans, so say it out loud.
        _segment->set_num_rows(kNumRows);

        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(_fname));
        ColumnWriterOptions writer_opts;
        writer_opts.meta = &_meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(column.type());
        writer_opts.meta->set_length(is_string_type(column.type()) ? column.length() : 0);
        writer_opts.meta->set_encoding(DEFAULT_ENCODING);
        writer_opts.meta->set_compression(LZ4_FRAME);
        writer_opts.meta->set_is_nullable(false);
        writer_opts.data_page_size = kDataPageSize;
        writer_opts.need_zone_map = true;

        ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &column, wfile.get()));
        ASSERT_OK(writer->init());
        ASSERT_OK(writer->append(src));
        ASSERT_OK(writer->finish());
        ASSERT_OK(writer->write_data());
        ASSERT_OK(writer->write_ordinal_index());
        ASSERT_OK(writer->write_zone_map());
        ASSERT_OK(wfile->close());

        ASSIGN_OR_ABORT(_reader, ColumnReader::create(&_meta, _segment.get(), nullptr));
        ASSIGN_OR_ABORT(_read_file, _fs->new_random_access_file(_fname));
    }

    SparseRange<> zone_map_ranges(const TypeDescriptor& source_type, const TypeDescriptor& target_type,
                                  const ColumnPredicate* predicate) {
        ASSIGN_OR_ABORT(auto source_iter, _reader->new_iterator());
        CastColumnIterator cast_iter(std::move(source_iter), source_type, target_type, false);
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = &_stats;
        iter_opts.read_file = _read_file.get();
        CHECK_OK(cast_iter.init(iter_opts));

        SparseRange<> row_ranges;
        CHECK_OK(cast_iter.get_row_ranges_by_zone_map({predicate}, nullptr, &row_ranges, CompoundNodeType::AND));
        return row_ranges;
    }

    static bool covers(const SparseRange<>& ranges, rowid_t rowid) {
        for (size_t i = 0; i < ranges.size(); i++) {
            if (ranges[i].begin() <= rowid && rowid < ranges[i].end()) {
                return true;
            }
        }
        return false;
    }

    // Ascending integers, so row i holds the value i + 1.
    static MutableColumnPtr ascending_ints() {
        auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
        column->reserve(kNumRows);
        for (int32_t i = 1; i <= kNumRows; i++) {
            (void)column->append_numbers(&i, sizeof(i));
        }
        return column;
    }

    // The same numbers spelled out. Unpadded, their lexicographic order is not their numeric one;
    // zero-padded to a fixed width, the two orders agree again.
    static MutableColumnPtr ascending_int_strings(bool zero_padded) {
        auto column = ChunkFactory::column_from_field_type(TYPE_VARCHAR, false);
        column->reserve(kNumRows);
        for (int i = 1; i <= kNumRows; i++) {
            std::string s = std::to_string(i);
            if (zero_padded) {
                s.insert(0, 4 - s.size(), '0');
            }
            CHECK(column->append_strings(std::vector<Slice>{Slice(s)}));
        }
        return column;
    }

    // Consecutive days from 2000-01-02, so a page again holds one ascending run.
    static MutableColumnPtr ascending_dates() {
        auto column = ChunkFactory::column_from_field_type(TYPE_DATE, false);
        column->reserve(kNumRows);
        for (int i = 1; i <= kNumRows; i++) {
            column->append_datum(Datum(DateValue::create(2000, 1, 1).add<TimeUnit::DAY>(i)));
        }
        return column;
    }

    // The same days at 13:45:00, so the zone map text carries a time of day.
    static MutableColumnPtr ascending_datetimes() {
        auto column = ChunkFactory::column_from_field_type(TYPE_DATETIME, false);
        column->reserve(kNumRows);
        for (int i = 1; i <= kNumRows; i++) {
            auto ts = TimestampValue::create(2000, 1, 1, 13, 45, 0, 0);
            column->append_datum(Datum(ts.add<TimeUnit::DAY>(i)));
        }
        return column;
    }

    ColumnMetaPB _meta;
    std::string _fname;
    std::shared_ptr<MemoryFileSystem> _fs;
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
    std::shared_ptr<Segment> _segment;
    std::unique_ptr<ColumnReader> _reader;
    std::unique_ptr<RandomAccessFile> _read_file;
    OlapReaderStatistics _stats;
};

// The bug: a BIGINT flat JSON subfield read as VARCHAR, e.g. get_json_string(j, '$.x'). The page
// holding 1..32 records the min "1" and the max "32", and read as strings "9" sorts above that max, so
// the page carrying the only matching row is dropped and the query answers zero.
TEST_F(CastColumnIteratorZoneMapTest, zone_map_is_not_shared_between_int_and_varchar) {
    write_column_with_zone_map(create_int_key(1, false), *ascending_ints());

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "9"));
    auto ranges = zone_map_ranges(TypeDescriptor::from_logical_type(TYPE_INT),
                                  TypeDescriptor::from_logical_type(TYPE_VARCHAR, 128), pred.get());

    // Row 8 holds the value 9, the one row the predicate matches.
    EXPECT_TRUE(covers(ranges, 8));
    EXPECT_EQ(kNumRows, ranges.span_size());
}

// And the reverse direction: a VARCHAR subfield read as BIGINT, e.g. get_json_int(j, '$.s'). The page
// holding "1".."32" records the lexicographic min "1" and max "9"; read as numbers that is the range
// [1, 9], which drops the page even though it is where 20 lives.
TEST_F(CastColumnIteratorZoneMapTest, zone_map_is_not_shared_between_varchar_and_bigint) {
    write_column_with_zone_map(create_varchar_key(1, false, 128), *ascending_int_strings(false));

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_BIGINT), 0, "20"));
    auto ranges = zone_map_ranges(TypeDescriptor::from_logical_type(TYPE_VARCHAR, 128),
                                  TypeDescriptor::from_logical_type(TYPE_BIGINT), pred.get());

    // Row 19 holds the value 20.
    EXPECT_TRUE(covers(ranges, 19));
    EXPECT_EQ(kNumRows, ranges.span_size());
}

// The pruning that has to survive: an integer widened by a fast schema evolution reads its old pages
// through this iterator, and both types spell a zone map as the same decimal literal.
TEST_F(CastColumnIteratorZoneMapTest, zone_map_still_prunes_across_integer_widening) {
    write_column_with_zone_map(create_int_key(1, false), *ascending_ints());

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_BIGINT), 0, "9"));
    auto ranges = zone_map_ranges(TypeDescriptor::from_logical_type(TYPE_INT),
                                  TypeDescriptor::from_logical_type(TYPE_BIGINT), pred.get());

    EXPECT_TRUE(covers(ranges, 8));
    EXPECT_LT(ranges.span_size(), kNumRows / 8);
}

// The other pruning that has to survive: a CHAR whose length changed, which reaches the reader as a
// CHAR-to-VARCHAR cast. ColumnReader::_get_zone_map_parse_type() already keeps parsing those zone maps
// as CHAR so the padding is stripped, and the byte-wise order is the same on both sides.
TEST_F(CastColumnIteratorZoneMapTest, zone_map_still_prunes_across_char_to_varchar) {
    write_column_with_zone_map(create_char_key(1, false, 4), *ascending_int_strings(true));

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "0009"));
    auto ranges = zone_map_ranges(TypeDescriptor::from_logical_type(TYPE_CHAR, 4),
                                  TypeDescriptor::from_logical_type(TYPE_VARCHAR, 128), pred.get());

    EXPECT_TRUE(covers(ranges, 8));
    EXPECT_LT(ranges.span_size(), kNumRows / 8);
}

// The third pruning that has to survive: a DATE widened to DATETIME. The zone map reads back as that
// day's midnight, which is exactly where the cast puts every stored DATE.
TEST_F(CastColumnIteratorZoneMapTest, zone_map_still_prunes_across_date_to_datetime) {
    write_column_with_zone_map(TabletColumn(STORAGE_AGGREGATE_NONE, TYPE_DATE, false), *ascending_dates());

    std::unique_ptr<ColumnPredicate> pred(
            new_column_eq_predicate(get_type_info(TYPE_DATETIME), 0, "2000-01-10 00:00:00"));
    auto ranges = zone_map_ranges(TypeDescriptor::from_logical_type(TYPE_DATE),
                                  TypeDescriptor::from_logical_type(TYPE_DATETIME), pred.get());

    // Row 8 holds 2000-01-10: the dates start at 2000-01-02.
    EXPECT_TRUE(covers(ranges, 8));
    EXPECT_LT(ranges.span_size(), kNumRows / 8);
}

// But not the other way round. Turning a DATETIME back into a DATE is a rewriting schema change
// today, so this direction reads no zone map and has no pruning to win, and it stays shut because
// the cost of being wrong is asymmetric: DateValue::from_string() answers a string it cannot read by
// substituting 1400-01-01 rather than failing, and a min and a max collapsed there take the whole
// column with them in silence. This pins the rule as one-directional -- made symmetric, it answers
// this query with 16 rows out of 4096.
TEST_F(CastColumnIteratorZoneMapTest, zone_map_is_not_shared_between_datetime_and_date) {
    write_column_with_zone_map(TabletColumn(STORAGE_AGGREGATE_NONE, TYPE_DATETIME, false), *ascending_datetimes());

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_DATE), 0, "2000-01-10"));
    auto ranges = zone_map_ranges(TypeDescriptor::from_logical_type(TYPE_DATETIME),
                                  TypeDescriptor::from_logical_type(TYPE_DATE), pred.get());

    // Row 8 holds 2000-01-10 13:45:00, the day the predicate names.
    EXPECT_TRUE(covers(ranges, 8));
    EXPECT_EQ(kNumRows, ranges.span_size());
}

} // namespace starrocks
