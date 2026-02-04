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

#include "exec/range_router.h"

#include <gtest/gtest.h>

#include <optional>
#include <tuple>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/datum.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "storage/type_utils.h"

namespace starrocks {

// Test fixture providing helpers to construct chunks and tablet ranges
// in the same way as the FE encodes TVariant / TTabletRange.
class RangeRouterTest : public ::testing::Test {
protected:
    // ---- TVariant helpers ----

    TVariant make_int_variant(int64_t v) {
        TVariant tv;
        tv.__set_type(TYPE_INT_DESC.to_thrift());
        tv.__set_value(std::to_string(v));
        return tv;
    }

    TVariant make_null_variant(const TypeDescriptor& type_desc) {
        TVariant tv;
        tv.__set_type(type_desc.to_thrift());
        tv.__set_variant_type(TVariantType::NULL_VALUE);
        return tv;
    }

    TVariant make_varchar_variant(const std::string& v) {
        TVariant tv;
        tv.__set_type(TYPE_VARCHAR_DESC.to_thrift());
        tv.__set_value(v);
        return tv;
    }

    TVariant make_datetime_variant(const std::string& v) {
        TVariant tv;
        tv.__set_type(TYPE_DATETIME_DESC.to_thrift());
        tv.__set_value(v);
        return tv;
    }

    TVariant make_string_variant(const std::string& v) {
        TVariant tv;
        tv.__set_type(TYPE_VARCHAR_DESC.to_thrift());
        tv.__set_value(v);
        return tv;
    }

    TVariant make_hll_variant(const std::string& v) {
        TVariant tv;
        TypeDescriptor type_desc = TypeDescriptor::create_hll_type();
        tv.__set_type(type_desc.to_thrift());
        tv.__set_value(v);
        return tv;
    }

    // ---- Chunk helpers ----

    Chunk make_int_chunk(const std::string& name, const std::vector<int32_t>& values) {
        auto col = FixedLengthColumn<int32_t>::create();
        for (int32_t v : values) {
            col->append(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_INT), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(std::move(cols), std::move(schema));
    }

    Chunk make_varchar_chunk(const std::string& name, const std::vector<std::string>& values) {
        auto col = BinaryColumn::create();
        for (const auto& v : values) {
            col->append_string(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_VARCHAR), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(std::move(cols), std::move(schema));
    }

    // ---- Single-column TTabletRange helpers ----

    // Helper to build a TTabletRange on a single INT column using long_value.
    // If lower/upper is std::nullopt, the corresponding bound is treated as -inf / +inf.
    TTabletRange make_single_long_range(std::optional<int64_t> lower, bool lower_included, std::optional<int64_t> upper,
                                        bool upper_included) {
        TTabletRange range;
        if (lower.has_value()) {
            TVariant v = make_int_variant(lower.value());
            TTuple tuple;
            tuple.__set_values(std::vector<TVariant>{v});
            range.__set_lower_bound(tuple);
            range.__set_lower_bound_included(lower_included);
        }
        if (upper.has_value()) {
            TVariant v = make_int_variant(upper.value());
            TTuple tuple;
            tuple.__set_values(std::vector<TVariant>{v});
            range.__set_upper_bound(tuple);
            range.__set_upper_bound_included(upper_included);
        }
        return range;
    }

    // Helper to build a TTabletRange on a single VARCHAR column using string_value.
    TTabletRange make_single_string_range(const std::optional<std::string>& lower, bool lower_included,
                                          const std::optional<std::string>& upper, bool upper_included) {
        TTabletRange range;
        if (lower.has_value()) {
            TVariant v = make_varchar_variant(lower.value());
            TTuple tuple;
            tuple.__set_values(std::vector<TVariant>{v});
            range.__set_lower_bound(tuple);
            range.__set_lower_bound_included(lower_included);
        }
        if (upper.has_value()) {
            TVariant v = make_varchar_variant(upper.value());
            TTuple tuple;
            tuple.__set_values(std::vector<TVariant>{v});
            range.__set_upper_bound(tuple);
            range.__set_upper_bound_included(upper_included);
        }
        return range;
    }
};

// Small helper to assert that an error Status message contains a substring.
static void assert_status_message_contains(const Status& status, const std::string& needle) {
    ASSERT_NE(status.message().find(needle), std::string::npos)
            << "Expected error message to contain '" << needle << "', got: " << status.message();
}

// ----------------------------------------------------------------------
// Happy-path routing tests
// ----------------------------------------------------------------------

// Single (-inf, +inf) tablet range: every row should be routed to the only
// candidate destination.
// This also exercises the fast path in route_chunk_rows() when there is only
// one range.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleInfiniteRangeSelectAll) {
    std::vector<TTabletRange> ranges(1); // no lower / upper bound set => (-inf, +inf)
    std::vector<int64_t> tablet_ids{42};

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 1).ok());

    std::vector<int32_t> values{-100, -1, 0, 5, 10, 100};
    Chunk chunk = make_int_chunk("c1", values);

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());
    ASSERT_EQ(row_indices.size(), routed_ids.size());
    for (size_t i = 0; i < row_indices.size(); ++i) {
        ASSERT_EQ(42, routed_ids[i]) << "row " << row_indices[i] << " mismatch";
    }
}

// Three ranges on a single INT column fully covering (-inf, +inf) with left-closed/right-open semantics:
//   R0: (-inf, 10)
//   R1: [10, 20)
//   R2: [20, +inf)
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, IntThreeRangesFullCoverage) {
    std::vector<TTabletRange> ranges;
    ranges.emplace_back(make_single_long_range(std::nullopt, false, 10, false)); // (-inf, 10)
    ranges.emplace_back(make_single_long_range(10, true, 20, false));            // [10, 20)
    ranges.emplace_back(make_single_long_range(20, true, std::nullopt, false));  // [20, +inf)

    std::vector<int64_t> tablet_ids{100, 200, 300};

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 1).ok());

    std::vector<int32_t> values{-5, 0, 5, 10, 11, 19, 20, 21, 100};
    Chunk chunk = make_int_chunk("c1", values);

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    // With left-closed/right-open ranges, value 10 belongs to R1 and value 20 belongs to R2.
    std::vector<int64_t> expected = {100, 100, 100, 200, 200, 200, 300, 300, 300};
    ASSERT_EQ(expected.size(), routed_ids.size());
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " value " << values[i] << " mismatch";
    }
}

// Many consecutive ranges to exercise the binary search logic in
// _find_tablet_index_for_row().
// Ranges (left-closed/right-open):
//   R0: (-inf, 0)
//   R1: [0, 10)
//   R2: [10, 20)
//   ...
//   R7: [60, 70)
//   R8: [70, +inf)
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ManyRangesBinarySearchFullCoverage) {
    std::vector<TTabletRange> ranges;
    std::vector<int64_t> tablet_ids;

    // R0: (-inf, 0)
    ranges.emplace_back(make_single_long_range(std::nullopt, false, 0, false));
    tablet_ids.emplace_back(100);
    // R1..R7: [k*10, (k+1)*10) for k = 0..6 -> values [0,10), ..., [60,70)
    for (int k = 1; k <= 7; ++k) {
        ranges.emplace_back(make_single_long_range((k - 1) * 10, true, k * 10, false));
        tablet_ids.emplace_back(100 + k);
    }
    // R8: [70, +inf)
    ranges.emplace_back(make_single_long_range(70, true, std::nullopt, false));
    tablet_ids.emplace_back(108);

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 1).ok());

    // For each range, pick two sample points.
    std::vector<int32_t> values = {
            -10, 0,  // R0
            1,   10, // R1
            11,  20, // R2
            21,  30, // R3
            31,  40, // R4
            41,  50, // R5
            51,  60, // R6
            61,  70, // R7
            71,  100 // R8
    };
    Chunk chunk = make_int_chunk("c1", values);

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    auto expected_range_idx = [](int v) -> int {
        if (v < 0) return 0;
        if (v < 10) return 1;
        if (v < 20) return 2;
        if (v < 30) return 3;
        if (v < 40) return 4;
        if (v < 50) return 5;
        if (v < 60) return 6;
        if (v < 70) return 7;
        return 8;
    };

    for (int i = 0; i < chunk.num_rows(); ++i) {
        int v = values[i];
        int idx = expected_range_idx(v);
        ASSERT_EQ(tablet_ids[idx], routed_ids[i]) << "row " << i << " value " << v << " mismatch";
    }
}

// Multi-column routing with two ranges fully covering (-inf, +inf) lexicographically.
// Ranges (left-closed/right-open):
//   R0: (-inf, -inf) .. (2, 50)
//   R1: [2, 50) .. (+inf, +inf)
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, MultiColumnTwoRangesFullCoverage) {
    std::vector<TTabletRange> ranges;

    // R0: (-inf, -inf) .. (2, 50)
    TTabletRange r0;
    {
        TVariant v1_up = make_int_variant(2);
        TVariant v2_up = make_int_variant(50);
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{v1_up, v2_up});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
    }

    // R1: [2, 50) .. (+inf, +inf)
    TTabletRange r1;
    {
        TVariant v1_low = make_int_variant(2);
        TVariant v2_low = make_int_variant(50);
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{v1_low, v2_low});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);
    }

    ranges.push_back(r0);
    ranges.push_back(r1);

    std::vector<int64_t> tablet_ids{100, 200};

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 2).ok());

    // (c1, c2) tuples:
    // 0: (1,10)   -> R0
    // 1: (2,10)   -> R0
    // 2: (2,50)   -> R1 (lower inclusive)
    // 3: (3,10)   -> R1
    // 4: (3,60)   -> R1
    std::vector<int32_t> col1_vals{1, 2, 2, 3, 3};
    std::vector<int32_t> col2_vals{10, 10, 50, 10, 60};

    auto c1 = FixedLengthColumn<int32_t>::create();
    auto c2 = FixedLengthColumn<int32_t>::create();
    for (auto v : col1_vals) c1->append(v);
    for (auto v : col2_vals) c2->append(v);

    Chunk chunk;
    chunk.append_column(c1, 0);
    chunk.append_column(c2, 1);

    SlotDescriptor slot_desc1(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    SlotDescriptor slot_desc2(1, "c2", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc1, &slot_desc2};
    chunk.set_slot_id_to_index(slot_desc1.id(), 0);
    chunk.set_slot_id_to_index(slot_desc2.id(), 1);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    std::vector<int64_t> expected = {100, 100, 200, 200, 200};
    ASSERT_EQ(expected.size(), routed_ids.size());
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " mismatch";
    }
}

// Multi-column ranges with NULL boundaries (infinite bounds).
// This test is adapted from the legacy MultiColumnNullBoundaries test to
// match the current RangeRouter invariants.
// Ranges:
//   R0: (-inf, -inf) .. (10, "b")
//   R1: [10, "b") .. (+inf, +inf)
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, MultiColumnNullBoundaries) {
    std::vector<TTabletRange> ranges;

    // R0: (-inf, -inf) .. (10, "b")
    TTabletRange r0;
    r0.upper_bound.values.push_back(make_int_variant(10));
    r0.upper_bound.values.push_back(make_string_variant("b"));
    r0.upper_bound.__isset.values = true;
    r0.upper_bound_included = false;
    r0.__isset.upper_bound_included = true;
    r0.__isset.upper_bound = true;

    // R1: [10, "b") .. (+inf, +inf)
    TTabletRange r1;
    r1.lower_bound.values.push_back(make_int_variant(10));
    r1.lower_bound.values.push_back(make_string_variant("b"));
    r1.lower_bound.__isset.values = true;
    r1.lower_bound_included = true;
    r1.__isset.lower_bound_included = true;
    r1.__isset.lower_bound = true;
    // No upper_bound means (+inf, +inf)

    ranges.push_back(r0);
    ranges.push_back(r1);

    std::vector<int64_t> tablet_ids{100, 200};

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 2).ok());

    // Test data: (c1, c2, expected_tablet_id)
    std::vector<std::tuple<int32_t, std::string, int64_t>> test_data = {
            {5, "a", 100},  // < (10,"b")
            {10, "a", 100}, // < (10,"b")
            {10, "b", 200}, // boundary, goes to R1 (inclusive lower)
            {10, "c", 200}, // > (10,"b")
            {20, "a", 200}, // > (10,"b")
    };

    std::vector<int32_t> col1_values;
    std::vector<std::string> col2_strings;
    for (const auto& [v1, v2, expected] : test_data) {
        (void)expected;
        col1_values.push_back(v1);
        col2_strings.push_back(v2);
    }

    auto col1 = FixedLengthColumn<int32_t>::create();
    for (auto v : col1_values) {
        col1->append(v);
    }
    auto col2 = BinaryColumn::create();
    for (const auto& s : col2_strings) {
        col2->append_string(s);
    }

    Chunk chunk;
    chunk.append_column(col1, 0);
    chunk.append_column(col2, 1);

    SlotDescriptor slot_desc1(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    SlotDescriptor slot_desc2(1, "c2", TypeDescriptor::from_logical_type(TYPE_VARCHAR));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc1, &slot_desc2};
    chunk.set_slot_id_to_index(slot_desc1.id(), 0);
    chunk.set_slot_id_to_index(slot_desc2.id(), 1);

    std::vector<uint16_t> row_indices;
    for (size_t i = 0; i < test_data.size(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());
    ASSERT_EQ(row_indices.size(), routed_ids.size());

    for (size_t i = 0; i < row_indices.size(); ++i) {
        const auto& [v1, v2, expected] = test_data[i];
        ASSERT_EQ(expected, routed_ids[i]) << "Value (" << v1 << ", " << v2 << ") mismatch";
    }
}

// VARCHAR ranges with three ranges fully covering (-inf, +inf) with left-closed/right-open semantics:
//   R0: (-inf, "banana")
//   R1: ["banana", "date")
//   R2: ["date", +inf)
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, VarcharThreeRangesFullCoverage) {
    std::vector<TTabletRange> ranges;
    ranges.emplace_back(
            make_single_string_range(std::nullopt, false, std::string("banana"), false)); // (-inf, "banana")
    ranges.emplace_back(
            make_single_string_range(std::string("banana"), true, std::string("date"), false));    // ["banana","date")
    ranges.emplace_back(make_single_string_range(std::string("date"), true, std::nullopt, false)); // ["date", +inf)

    std::vector<int64_t> tablet_ids{10, 20, 30};

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 1).ok());

    std::vector<std::string> values = {"apple", "banana", "cherry", "date", "fig"};
    Chunk chunk = make_varchar_chunk("s1", values);

    SlotDescriptor slot_desc(0, "s1", TypeDescriptor::from_logical_type(TYPE_VARCHAR));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    // "apple"           -> R0 (10)
    // "banana","cherry" -> R1 (20)
    // "date", "fig"     -> R2 (30)
    std::vector<int64_t> expected = {10, 20, 20, 30, 30};
    ASSERT_EQ(expected.size(), routed_ids.size());
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " value " << values[i] << " mismatch";
    }
}

// NULL_TYPE boundary values should be treated as NULL and route with NULL as minimum.
// Ranges:
//   R0: (-inf, NULL)
//   R1: [NULL, 10)
//   R2: [10, +inf)
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, NullTypeBoundaryRouting) {
    std::vector<TTabletRange> ranges;

    TTabletRange r0;
    {
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{make_null_variant(TYPE_INT_DESC)});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
    }

    TTabletRange r1;
    {
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{make_null_variant(TYPE_INT_DESC)});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);

        TTuple upper;
        upper.__set_values(std::vector<TVariant>{make_int_variant(10)});
        r1.__set_upper_bound(upper);
        r1.__set_upper_bound_included(false);
    }

    TTabletRange r2;
    {
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{make_int_variant(10)});
        r2.__set_lower_bound(lower);
        r2.__set_lower_bound_included(true);
    }

    ranges.push_back(r0);
    ranges.push_back(r1);
    ranges.push_back(r2);

    std::vector<int64_t> tablet_ids{10, 20, 30};

    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 1).ok());

    auto data = FixedLengthColumn<int32_t>::create();
    auto nulls = NullColumn::create();
    auto nullable = NullableColumn::create(std::move(data), std::move(nulls));
    nullable->append_nulls(1);
    nullable->append_datum(Datum(0));
    nullable->append_datum(Datum(5));
    nullable->append_datum(Datum(10));
    nullable->append_datum(Datum(20));

    Chunk chunk;
    chunk.append_column(nullable, 0);

    SlotDescriptor slot_desc(0, "v1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    std::vector<int64_t> expected = {20, 20, 20, 30, 30};
    ASSERT_EQ(expected.size(), routed_ids.size());
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " mismatch";
    }
}

// ----------------------------------------------------------------------
// Tests that explicitly exercise RangeRouter::_validate_range() error
// branches.

TEST_F(RangeRouterTest, InitRejectsInvalidBoundVariantType) {
    TVariant invalid_variant;
    invalid_variant.__set_type(TYPE_INT_DESC.to_thrift());
    invalid_variant.__set_variant_type(TVariantType::MINIMUM);
    invalid_variant.__set_value("0");

    TTabletRange r0;
    {
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{invalid_variant});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
    }

    TTabletRange r1;
    {
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{invalid_variant});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);
    }

    std::vector<TTabletRange> ranges{r0, r1};
    RangeRouter router;
    auto status = router.init(ranges, 1);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is_internal_error());
    ASSERT_EQ("Invalid value in range bound", status.message());
}

TEST_F(RangeRouterTest, ValidateRangeRejectsInvalidVariantValue) {
    TVariant invalid_variant;
    invalid_variant.__set_type(TYPE_INT_DESC.to_thrift());
    invalid_variant.__set_variant_type(TVariantType::NORMAL_VALUE);

    TTabletRange r0;
    {
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{invalid_variant});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
    }

    TTabletRange r1;
    {
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{invalid_variant});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);
    }

    std::vector<TTabletRange> ranges{r0, r1};
    RangeRouter router;
    auto status = router.init(ranges, 1);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is_internal_error());
    ASSERT_EQ("Invalid variant for range validation", status.message());
}
// ----------------------------------------------------------------------

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeRejectsOverlappingInclusiveBounds) {
    // Two ranges that both include the shared boundary value at 10.
    // R0: (-inf, 10]
    // R1: [10, +inf)
    std::vector<TTabletRange> ranges;
    ranges.emplace_back(make_single_long_range(std::nullopt, false, 10, true));
    ranges.emplace_back(make_single_long_range(10, true, std::nullopt, false));

    RangeRouter router;
    Status st = router.init(ranges, 1);
    ASSERT_FALSE(st.ok());
    assert_status_message_contains(
            st, "adjacent ranges are overlapping / not complementary for the inclusive/exclusive bound");
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeRejectsTypeMismatchOnBoundary) {
    // Two ranges whose boundary values are equal in string form but use
    // different TVariant.type descriptors, which should be rejected.
    //
    // To simulate this, construct the TTabletRanges manually instead of using
    // the helpers above.
    std::vector<TTabletRange> ranges(2);

    // R0: (-inf, 10) with INT type
    {
        TVariant v;
        v.__set_type(TYPE_INT_DESC.to_thrift());
        v.__set_value("10");
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{v});
        ranges[0].__set_upper_bound(upper);
        ranges[0].__set_upper_bound_included(false);
    }

    // R1: [10, +inf) with VARCHAR type
    {
        TVariant v;
        v.__set_type(TYPE_VARCHAR_DESC.to_thrift());
        v.__set_value("10");
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{v});
        ranges[1].__set_lower_bound(lower);
        ranges[1].__set_lower_bound_included(true);
    }

    RangeRouter router;
    Status st = router.init(ranges, 1);
    ASSERT_FALSE(st.ok());
    assert_status_message_contains(st, "Type mismatch at column 0 between range[0] and range[1]");
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeRejectsUpperLowerValueMismatch) {
    // Two ranges whose boundary types are the same but the encoded values
    // differ, which should be rejected.
    //
    // R0: (-inf, 10)
    // R1: [20, +inf)
    std::vector<TTabletRange> ranges(2);

    {
        TVariant v;
        v.__set_type(TYPE_INT_DESC.to_thrift());
        v.__set_value("10");
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{v});
        ranges[0].__set_upper_bound(upper);
        ranges[0].__set_upper_bound_included(false);
    }

    {
        TVariant v;
        v.__set_type(TYPE_INT_DESC.to_thrift());
        v.__set_value("20");
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{v});
        ranges[1].__set_lower_bound(lower);
        ranges[1].__set_lower_bound_included(true);
    }

    RangeRouter router;
    Status st = router.init(ranges, 1);
    ASSERT_FALSE(st.ok());
    assert_status_message_contains(st, "Range[0] != range[1] at column 0");
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, RangeRouterSingleIntRangesRouting) {
    // Build four ranges on a single INT column:
    //   R0: (-inf, 10)
    //   R1: [10, 20)
    //   R2: [20, 30)
    //   R3: [30, +inf)
    //
    // Verify that RangeRouter correctly routes values into these ranges.
    std::vector<TTabletRange> ranges;
    ranges.emplace_back(make_single_long_range(std::nullopt, false, 10, false));
    ranges.emplace_back(make_single_long_range(10, true, 20, false));
    ranges.emplace_back(make_single_long_range(20, true, 30, false));
    ranges.emplace_back(make_single_long_range(30, true, std::nullopt, false));

    // The RangeRouter should be able to initialize and route correctly.
    std::vector<int64_t> tablet_ids{100, 200, 300, 400};
    RangeRouter router;
    ASSERT_TRUE(router.init(ranges, 1).ok());

    std::vector<int32_t> values{5, 10, 15, 20, 25, 30, 35};
    Chunk chunk = make_int_chunk("c1", values);

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids;

    ASSERT_TRUE(router.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    std::vector<int64_t> expected = {100, 200, 200, 300, 300, 400, 400};
    ASSERT_EQ(expected.size(), routed_ids.size());
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " value " << values[i] << " mismatch";
    }
}

// ----------------------------------------------------------------------
// Validation tests for _validate_range()
// ----------------------------------------------------------------------

// Missing -inf/+inf ranges should be rejected.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeMissingInfiniteBounds) {
    // Case 1: no -inf and no +inf (both ranges have finite lower/upper)
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(0, true, 10, true));   // [0,10]
        ranges.emplace_back(make_single_long_range(10, false, 20, true)); // (10,20]

        RangeRouter router;
        auto st = router.init(ranges, 1);
        ASSERT_FALSE(st.ok());
        assert_status_message_contains(st, "lower_inf_count and upper_inf_count must be 1");
    }

    // Case 2: two -inf ranges (both missing lower bound)
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(std::nullopt, false, 10, true)); // (-inf,10]
        ranges.emplace_back(make_single_long_range(std::nullopt, false, 20, true)); // another -inf range

        RangeRouter router;
        auto st = router.init(ranges, 1);
        ASSERT_FALSE(st.ok());
        assert_status_message_contains(st, "lower_inf_count and upper_inf_count must be 1");
    }
}

// -inf/+inf ranges must be placed at the first / last tablet range.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeInfiniteNotAtEdges) {
    // Case 1: -inf range exists but is not at first position.
    // We also keep a single +inf range so that lower_inf_count == upper_inf_count == 1,
    // and the error is triggered by the "at edges" check instead of the count check.
    {
        std::vector<TTabletRange> ranges;
        // finite range at index 0: [0,10)
        TTabletRange finite = make_single_long_range(0, true, 10, false);

        // r1 has no lower bound => -inf, but appears at index 1.
        TTabletRange r1;
        TTuple upper_tuple;
        upper_tuple.__set_values(std::vector<TVariant>{make_int_variant(20)});
        r1.__set_upper_bound(upper_tuple);
        r1.__set_upper_bound_included(false);

        // r2 is the +inf range at the last position: [20,+inf)
        TTabletRange r2 = make_single_long_range(20, true, std::nullopt, false);

        ranges.push_back(finite);
        ranges.push_back(r1);
        ranges.push_back(r2);

        RangeRouter router;
        auto st = router.init(ranges, 1);
        ASSERT_FALSE(st.ok());
        assert_status_message_contains(st, "-inf/inf range must be set for the first and last tablet range");
    }

    // Case 2: +inf range exists but is not at last position.
    // Similarly, we keep a single -inf range so that the count check passes first.
    {
        std::vector<TTabletRange> ranges;

        // r0 has no lower bound => -inf and is placed at index 0.
        TTabletRange r0 = make_single_long_range(std::nullopt, false, 10, true); // (-inf,10]

        // r1 has no upper bound => +inf, but it is not the last range.
        TTabletRange r1 = make_single_long_range(10, true, std::nullopt, false); // [10,+inf)

        // finite range at the last index
        TTabletRange r2 = make_single_long_range(20, true, 30, false); // [20,30)

        ranges.push_back(r0);
        ranges.push_back(r1);
        ranges.push_back(r2);

        RangeRouter router;
        auto st = router.init(ranges, 1);
        ASSERT_FALSE(st.ok());
        assert_status_message_contains(st, "-inf/inf range must be set for the first and last tablet range");
    }
}

// Adjacent ranges must have complementary inclusive/exclusive flags on the
// shared boundary; both-inclusive or both-exclusive should be rejected.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeAdjacentInclusiveExclusiveOverlap) {
    // Case 1: both-inclusive at boundary 10: (-inf,10] and [10,+inf)
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(std::nullopt, false, 10, true)); // (-inf,10]
        ranges.emplace_back(make_single_long_range(10, true, std::nullopt, false)); // [10,+inf)

        RangeRouter router;
        auto st = router.init(ranges, 1);
        ASSERT_FALSE(st.ok());
        assert_status_message_contains(
                st, "adjacent ranges are overlapping / not complementary for the inclusive/exclusive bound");
    }

    // Case 2: both-exclusive at boundary 10: (-inf,10) and (10,+inf)
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(std::nullopt, false, 10, false)); // (-inf,10)
        ranges.emplace_back(make_single_long_range(10, false, std::nullopt, false)); // (10,+inf)

        RangeRouter router;
        auto st = router.init(ranges, 1);
        ASSERT_FALSE(st.ok());
        assert_status_message_contains(
                st, "adjacent ranges are overlapping / not complementary for the inclusive/exclusive bound");
    }
}

// Type mismatch on adjacent boundaries should be detected.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeTypeMismatchAtBoundary) {
    std::vector<TTabletRange> ranges;

    // R0 upper bound uses INT (long_value)
    TTabletRange r0;
    {
        TVariant v_up = make_int_variant(10);
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{v_up});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
    }

    // R1 lower bound uses VARCHAR (string_value) => type mismatch in TVariant payload.
    TTabletRange r1;
    {
        TVariant v_low = make_string_variant("10");
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{v_low});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);
    }

    ranges.push_back(r0);
    ranges.push_back(r1);

    RangeRouter router;
    auto st = router.init(ranges, 1);
    ASSERT_FALSE(st.ok());
    assert_status_message_contains(st, "Type mismatch at column 0 between range[0] and range[1]");
}

// Adjacent boundaries with different values should be rejected as they would
// leave a gap or create overlap.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ValidateRangeEndpointsNotEqual) {
    std::vector<TTabletRange> ranges;

    // R0: (-inf,10)
    TTabletRange r0 = make_single_long_range(std::nullopt, false, 10, false);
    // R1: [20,+inf) -> lower bound 20, which does not equal the previous upper bound 10.
    TTabletRange r1 = make_single_long_range(20, true, std::nullopt, false);

    ranges.push_back(r0);
    ranges.push_back(r1);

    RangeRouter router;
    auto st = router.init(ranges, 1);
    ASSERT_FALSE(st.ok());
    assert_status_message_contains(st, "Range[0] != range[1] at column 0");
}

// ----------------------------------------------------------------------
// HLL type handling
// ----------------------------------------------------------------------

// HLL TVariant encoded using string_value only. RangeRouter currently does not
// support building HLL boundaries via datum_from_string(), but it should fail
// gracefully with a NotSupported status once _validate_range() passes.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, HllVariantInitNotSupported) {
    // Build two HLL ranges that satisfy _validate_range():
    //   R0: (-inf, "boundary"]
    //   R1: ("boundary", +inf)
    std::vector<TTabletRange> ranges;

    // R0: (-inf, "boundary")
    {
        TTabletRange r0;
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{make_hll_variant("boundary")});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }

    // R1: ["boundary", +inf)
    {
        TTabletRange r1;
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{make_hll_variant("boundary")});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);
        ranges.push_back(r1);
    }

    RangeRouter router;
    Status st = router.init(ranges, 1);
    // The exact error code is implementation-defined, but we at least
    // expect a NotSupported error rather than a crash.
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
}

// ----------------------------------------------------------------------
// Datetime TVariant with ISO-8601 + nanos format
// ----------------------------------------------------------------------

// Simulate FE sending a DATETIME boundary encoded as Instant.toString(),
// e.g. "2024-01-01T00:00:00.123456789Z". RangeRouter should still be able
// to initialize successfully via the generic datetime parser.
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, DateTimeIsoWithNanosInitOk) {
    std::vector<TTabletRange> ranges;

    // Boundary in ISO-8601 format with 9 fractional digits and 'Z' suffix.
    const std::string boundary = "2024-01-01T00:00:00.123456789Z";

    // R0: (-inf, boundary)
    TTabletRange r0;
    {
        TVariant v_up = make_datetime_variant(boundary);
        TTuple upper;
        upper.__set_values(std::vector<TVariant>{v_up});
        r0.__set_upper_bound(upper);
        r0.__set_upper_bound_included(false);
    }

    // R1: [boundary, +inf)
    TTabletRange r1;
    {
        TVariant v_low = make_datetime_variant(boundary);
        TTuple lower;
        lower.__set_values(std::vector<TVariant>{v_low});
        r1.__set_lower_bound(lower);
        r1.__set_lower_bound_included(true);
    }

    ranges.push_back(r0);
    ranges.push_back(r1);

    RangeRouter router;
    // If the FE-style ISO-8601 datetime with nanos cannot be parsed, init()
    // would return an error here. We only care that initialization succeeds.
    ASSERT_TRUE(router.init(ranges, 1).ok());
}

} // namespace starrocks
