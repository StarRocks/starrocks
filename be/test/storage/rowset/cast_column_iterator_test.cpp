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
#include <velocypack/vpack.h>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/series_column_iterator.h"
#include "types/json_value.h"

namespace starrocks {

class CastColumnIteratorTestBase : public ::testing::Test {};

class CastColumnIteratorWithDefaultValueColumnIteratorTest : public CastColumnIteratorTestBase {};

TEST_F(CastColumnIteratorWithDefaultValueColumnIteratorTest, test01) {
    auto source_type = TYPE_INT_DESC;
    auto target_type = TYPE_BIGINT_DESC;
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
    auto source_type = TYPE_INT_DESC;
    auto target_type = TYPE_BIGINT_DESC;
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

// A flat JSON sub-column of type VARCHAR holds JSON strings: read as JSON it must stay a string even when it
// reads like JSON text. Any other VARCHAR source (a column whose type changed to JSON) is still parsed.
TEST_F(CastColumnIteratorWithDefaultValueColumnIteratorTest, flat_json_varchar_source_stays_json_string) {
    const std::string text = R"({"a":1})";
    auto source_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    auto target_type = TypeDescriptor(TYPE_JSON);
    for (bool flat_json_source : {true, false}) {
        SCOPED_TRACE(flat_json_source ? "flat_json_source" : "plain_source");
        auto source_iter =
                std::make_unique<DefaultValueColumnIterator>(true, text, true, get_type_info(source_type.type), 0, 2);
        CastColumnIterator cast_iter(std::move(source_iter), source_type, target_type, true, flat_json_source);
        ASSERT_OK(cast_iter.init(ColumnIteratorOptions{}));
        auto column = ColumnHelper::create_column(target_type, true);
        auto range = SparseRange<>{0, 2};
        ASSERT_OK(cast_iter.next_batch(range, column.get()));
        ASSERT_EQ(2, column->size());
        for (size_t i = 0; i < column->size(); i++) {
            Datum datum = column->get(i);
            ASSERT_FALSE(datum.is_null());
            vpack::Slice slice = datum.get_json()->to_vslice();
            if (flat_json_source) {
                ASSERT_TRUE(slice.isString()) << datum.get_json()->to_string_uncheck();
                ASSERT_EQ(text, slice.copyString());
            } else {
                ASSERT_TRUE(slice.isObject()) << datum.get_json()->to_string_uncheck();
            }
        }
    }
}

// A source whose zone map prunes every page, to tell whether CastColumnIterator lets that zone map prune.
class PruneAllZoneMapColumnIterator final : public ColumnIterator {
public:
    explicit PruneAllZoneMapColumnIterator(ordinal_t num_rows) : _num_rows(num_rows) {}
    Status seek_to_first() override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ord) override { return Status::OK(); }
    ordinal_t num_rows() const override { return _num_rows; }
    Status next_batch(size_t* n, Column* dst) override {
        *n = 0;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return 0; }
    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                      CompoundNodeType pred_relation, const Range<>* src_range) override {
        return Status::OK();
    }

private:
    ordinal_t _num_rows;
};

class CastColumnIteratorZoneMapTest : public CastColumnIteratorTestBase {};

// A flat JSON sub-column keeps its zone map in the stored type, and the reader parses it as the type the query reads.
// Only a BIGINT read as DOUBLE keeps the order, so only that zone map may prune.
TEST_F(CastColumnIteratorZoneMapTest, flat_json_zone_map_prunes_only_for_order_keeping_casts) {
    struct Case {
        LogicalType source;
        LogicalType target;
        bool flat_json_source;
        bool prunes;
    };
    const std::vector<Case> cases = {
            {TYPE_BIGINT, TYPE_VARCHAR, true, false},
            {TYPE_DOUBLE, TYPE_VARCHAR, true, false},
            {TYPE_VARCHAR, TYPE_BIGINT, true, false},
            {TYPE_DOUBLE, TYPE_BIGINT, true, false},
            {TYPE_BIGINT, TYPE_DOUBLE, true, true},
            // A fast schema evolution cast, which the FE allows only when the zone map can be reused.
            {TYPE_INT, TYPE_BIGINT, false, true},
    };
    auto type_of = [](LogicalType type) {
        return type == TYPE_VARCHAR ? TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)
                                    : TypeDescriptor(type);
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(type_to_string(c.source) + " -> " + type_to_string(c.target) +
                     (c.flat_json_source ? ", flat_json_source" : ""));
        CastColumnIterator cast_iter(std::make_unique<PruneAllZoneMapColumnIterator>(10), type_of(c.source),
                                     type_of(c.target), true, c.flat_json_source);
        SparseRange<> row_ranges;
        ASSERT_OK(cast_iter.get_row_ranges_by_zone_map({}, nullptr, &row_ranges, CompoundNodeType::AND));
        EXPECT_EQ(c.prunes ? 0 : 10, row_ranges.span_size());
    }

    // Every row of the given range is kept.
    CastColumnIterator cast_iter(std::make_unique<PruneAllZoneMapColumnIterator>(10), type_of(TYPE_BIGINT),
                                 type_of(TYPE_VARCHAR), true, true);
    SparseRange<> row_ranges;
    const Range<> src_range(2, 5);
    ASSERT_OK(cast_iter.get_row_ranges_by_zone_map({}, nullptr, &row_ranges, CompoundNodeType::AND, &src_range));
    EXPECT_EQ(3, row_ranges.span_size());
}

TEST_F(CastColumnIteratorWithDefaultValueColumnIteratorTest, test_get_io_range_vec) {
    auto source_type = TYPE_INT_DESC;
    auto target_type = TYPE_BIGINT_DESC;
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
    auto source_type = TYPE_SMALLINT_DESC;
    auto target_type = TYPE_BIGINT_DESC;
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

} // namespace starrocks
