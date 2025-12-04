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
#include <vector>

#include "column/chunk.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/binary_column.h"
#include "column/schema.h"
#include "runtime/types.h"
#include "runtime/descriptors.h"
#include "storage/type_utils.h"

namespace starrocks {

class RangeRouterTest : public ::testing::Test {
protected:
    // Helper to build a TVariant for INT type with a long_value.
    TVariant make_int_variant(int64_t v) {
        TVariant tv;
        tv.__set_type(TYPE_INT_DESC.to_thrift());
        tv.__set_long_value(v);
        return tv;
    }

    // Helper to build a TVariant for STRING type with a string_value.
    TVariant make_string_variant(const std::string& v) {
        TVariant tv;
        tv.__set_type(TYPE_VARCHAR_DESC.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Helper to build a TVariant for BOOLEAN type with a long_value(0/1).
    // This follows FE BoolVariant.toThrift(), which encodes BOOLEAN via long_value.
    TVariant make_bool_variant(bool v) {
        TVariant tv;
        tv.__set_type(TYPE_BOOLEAN_DESC.to_thrift());
        tv.__set_long_value(v ? 1 : 0);
        return tv;
    }

    // Helper to build a TVariant for VARCHAR type with string_value.
    // This follows FE StringVariant/LargeIntVariant/DateVariant.toThrift(), which
    // encode their payloads via string_value.
    TVariant make_varchar_variant(const std::string& v) {
        TVariant tv;
        tv.__set_type(TYPE_VARCHAR_DESC.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Helper to build a TVariant for CHAR type with string_value.
    TVariant make_char_variant(const std::string& v) {
        TVariant tv;
        TypeDescriptor type_desc = TypeDescriptor::create_char_type(TypeDescriptor::MAX_CHAR_LENGTH);
        tv.__set_type(type_desc.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Helper to build a TVariant for BINARY type with string_value.
    TVariant make_binary_variant(const std::string& v) {
        TVariant tv;
        TypeDescriptor type_desc = TypeDescriptor::from_logical_type(TYPE_BINARY);
        tv.__set_type(type_desc.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Helper to build a TVariant for VARBINARY type with string_value.
    TVariant make_varbinary_variant(const std::string& v) {
        TVariant tv;
        tv.__set_type(TYPE_VARBINARY_DESC.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Helper to build a TVariant for HLL type with string_value.
    TVariant make_hll_variant(const std::string& v) {
        TVariant tv;
        TypeDescriptor type_desc = TypeDescriptor::create_hll_type();
        tv.__set_type(type_desc.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Helper to build a TVariant for LARGEINT type with string_value.
    // This follows FE LargeIntVariant.toThrift(), which encodes large integers via string_value.
    TVariant make_largeint_variant(const std::string& v) {
        TVariant tv;
        TypeDescriptor type_desc = TypeDescriptor::from_logical_type(TYPE_LARGEINT);
        tv.__set_type(type_desc.to_thrift());
        tv.__set_string_value(v);
        return tv;
    }

    // Create a Chunk with a single INT column named |name|.
    Chunk make_int_chunk(const std::string& name, const std::vector<int32_t>& values) {
        auto col = FixedLengthColumn<int32_t>::create();
        for (auto v : values) {
            col->append(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_INT), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Create a Chunk with a single CHAR column named |name|.
    Chunk make_char_chunk(const std::string& name, const std::vector<std::string>& values) {
        auto col = BinaryColumn::create();
        for (const auto& v : values) {
            col->append_string(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_CHAR), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Create a Chunk with a single BOOLEAN column named |name|.
    Chunk make_bool_chunk(const std::string& name, const std::vector<uint8_t>& values) {
        auto col = FixedLengthColumn<uint8_t>::create();
        for (auto v : values) {
            col->append(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_BOOLEAN), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Create a Chunk with a single VARCHAR column named |name|.
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
        return Chunk(cols, schema);
    }

    // Create a Chunk with a single BINARY column named |name|.
    Chunk make_binary_chunk(const std::string& name, const std::vector<std::string>& values) {
        auto col = BinaryColumn::create();
        for (const auto& v : values) {
            col->append_string(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_BINARY), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Create a Chunk with a single VARBINARY column named |name|.
    Chunk make_varbinary_chunk(const std::string& name, const std::vector<std::string>& values) {
        auto col = BinaryColumn::create();
        for (const auto& v : values) {
            col->append_string(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_VARBINARY), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Create a Chunk with a single LARGEINT column named |name|.
    Chunk make_largeint_chunk(const std::string& name, const std::vector<int128_t>& values) {
        auto col = FixedLengthColumn<int128_t>::create();
        for (auto v : values) {
            col->append(v);
        }
        Columns cols;
        cols.emplace_back(col);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, name, get_type_info(TYPE_LARGEINT), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Create a Chunk with two INT columns (c1, c2).
    Chunk make_two_int_chunk(const std::vector<int32_t>& v1, const std::vector<int32_t>& v2) {
        auto c1 = FixedLengthColumn<int32_t>::create();
        auto c2 = FixedLengthColumn<int32_t>::create();
        for (auto v : v1) {
            c1->append(v);
        }
        for (auto v : v2) {
            c2->append(v);
        }
        Columns cols;
        cols.emplace_back(c1);
        cols.emplace_back(c2);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false));
        fields.emplace_back(std::make_shared<Field>(1, "c2", get_type_info(TYPE_INT), false));
        auto schema = std::make_shared<Schema>(fields);
        return Chunk(cols, schema);
    }

    // Helper to build a TTabletRange on a single column using long_value.
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

    // Helper to build a TTabletRange on a single BOOLEAN column.
    TTabletRange make_single_bool_range(std::optional<bool> lower, bool lower_included, std::optional<bool> upper,
                                        bool upper_included) {
        TTabletRange range;
        if (lower.has_value()) {
            TVariant v = make_bool_variant(lower.value());
            TTuple tuple;
            tuple.__set_values(std::vector<TVariant>{v});
            range.__set_lower_bound(tuple);
            range.__set_lower_bound_included(lower_included);
        }
        if (upper.has_value()) {
            TVariant v = make_bool_variant(upper.value());
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

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleColumnClosedRange) {
    // Values 0..9, range [3,7]
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/3, /*lower_included=*/true,
                                                /*upper=*/7, /*upper_included=*/true);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    // Build slot descriptors for single column "c1".
    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    // Only route rows that fall into [3,7].
    for (int i = 3; i <= 7; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        if (i >= 3 && i <= 7) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleColumnHalfOpenRange) {
    // Values 0..9, range [3,7)
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/3, /*lower_included=*/true,
                                                /*upper=*/7, /*upper_included=*/false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    // Only route rows that fall into [3,7).
    for (int i = 3; i < 7; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        if (i >= 3 && i < 7) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleColumnOpenRange) {
    // Values 0..9, range (3,7)
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/3, /*lower_included=*/false,
                                                /*upper=*/7, /*upper_included=*/false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    // Only route rows that fall into (3,7).
    for (int i = 4; i < 7; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        if (i > 3 && i < 7) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleColumnPointRange) {
    // Values 0..9, range [3,3]
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/3, /*lower_included=*/true,
                                                /*upper=*/3, /*upper_included=*/true);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    // Only route the point range row.
    row_indices.emplace_back(3);
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        if (i == 3) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleColumnLowerOnly) {
    // Values 0..9, range (5, +inf)
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/5, /*lower_included=*/false,
                                                /*upper=*/std::nullopt, /*upper_included=*/false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    // Only route rows that fall into (5, +inf).
    for (int i = 6; i < 10; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        if (i > 5) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SingleColumnUpperOnly) {
    // Values 0..9, range (-inf, 5)
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/std::nullopt, /*lower_included=*/false,
                                                /*upper=*/5, /*upper_included=*/false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    // Only route rows that fall into (-inf, 5).
    for (int i = 0; i < 5; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        if (i < 5) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, NoBoundsSelectAll) {
    // Values 0..9, empty TTabletRange => all rows selected
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range; // no lower / upper bound set

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    for (int i = 0; i < 10; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, MultiColumnLexicographicRange) {
    // (c1, c2) tuples:
    // 0: (1,10)
    // 1: (2,10)
    // 2: (2,50)
    // 3: (3,10)
    // 4: (3,60)
    // 5: (4,10)
    // 6: (4,50)
    //
    // Range: [(2,10), (3,60)] (both inclusive)
    // Expected selected indices: 1,2,3,4
    Chunk chunk = make_two_int_chunk({1, 2, 2, 3, 3, 4, 4}, {10, 10, 50, 10, 60, 10, 50});

    // Build lower bound tuple (2,10)
    TVariant v1_low = make_int_variant(2);
    TVariant v2_low = make_int_variant(10);
    TTuple lower;
    lower.__set_values(std::vector<TVariant>{v1_low, v2_low});

    // Build upper bound tuple (3,60)
    TVariant v1_up = make_int_variant(3);
    TVariant v2_up = make_int_variant(60);
    TTuple upper;
    upper.__set_values(std::vector<TVariant>{v1_up, v2_up});

    TTabletRange range;
    range.__set_lower_bound(lower);
    range.__set_upper_bound(upper);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(true);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 2).ok());

    SlotDescriptor slot_desc1(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    SlotDescriptor slot_desc2(1, "c2", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc1, &slot_desc2};
    chunk.set_slot_id_to_index(slot_desc1.id(), 0);
    chunk.set_slot_id_to_index(slot_desc2.id(), 1);
    // Only route rows that are inside [(2,10), (3,60)].
    std::vector<uint16_t> row_indices = {1, 2, 3, 4};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    std::vector<int64_t> expected = {0, 1, 1, 1, 1, 0, 0};
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " mismatch";
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, MultiColumnHalfOpenUpperExcluded) {
    // Same tuples as MultiColumnLexicographicRange:
    // 0: (1,10)
    // 1: (2,10)
    // 2: (2,50)
    // 3: (3,10)
    // 4: (3,60)
    // 5: (4,10)
    // 6: (4,50)
    //
    // Range: [(2,10), (3,60)) (upper excluded)
    // Expected selected indices: 1,2,3
    Chunk chunk = make_two_int_chunk({1, 2, 2, 3, 3, 4, 4}, {10, 10, 50, 10, 60, 10, 50});

    // Build lower bound tuple (2,10)
    TVariant v1_low = make_int_variant(2);
    TVariant v2_low = make_int_variant(10);
    TTuple lower;
    lower.__set_values(std::vector<TVariant>{v1_low, v2_low});

    // Build upper bound tuple (3,60)
    TVariant v1_up = make_int_variant(3);
    TVariant v2_up = make_int_variant(60);
    TTuple upper;
    upper.__set_values(std::vector<TVariant>{v1_up, v2_up});

    TTabletRange range;
    range.__set_lower_bound(lower);
    range.__set_upper_bound(upper);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(false); // half-open on upper bound

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 2).ok());

    SlotDescriptor slot_desc1(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    SlotDescriptor slot_desc2(1, "c2", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc1, &slot_desc2};
    chunk.set_slot_id_to_index(slot_desc1.id(), 0);
    chunk.set_slot_id_to_index(slot_desc2.id(), 1);
    // Only route rows that are inside [(2,10), (3,60)).
    std::vector<uint16_t> row_indices = {1, 2, 3};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    std::vector<int64_t> expected = {0, 1, 1, 1, 0, 0, 0};
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        ASSERT_EQ(expected[i], routed_ids[i]) << "row " << i << " mismatch";
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, SubRangeFromOffset) {
    // Values 0..9, range [0,9], but only check rows [2,5)
    Chunk chunk = make_int_chunk("c1", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    TTabletRange range = make_single_long_range(/*lower=*/0, /*lower_included=*/true,
                                                /*upper=*/9, /*upper_included=*/true);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    std::vector<uint16_t> row_indices;
    for (int i = 2; i < 5; ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    // Only indices [2,4] should be touched and set to 1.
    for (int i = 0; i < 10; ++i) {
        if (i >= 2 && i < 5) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should remain 0";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, CharVariantRange) {
    // CHAR column with lexicographically ordered values.
    std::vector<std::string> values = {"apple", "banana", "cherry", "date", "fig"};
    Chunk chunk = make_char_chunk("c1", values);

    // Range ["banana", "date") encoded as CHAR TVariant using string_value only.
    TVariant v_low = make_char_variant("banana");
    TVariant v_up = make_char_variant("date");
    TTuple lower;
    lower.__set_values(std::vector<TVariant>{v_low});
    TTuple upper;
    upper.__set_values(std::vector<TVariant>{v_up});

    TTabletRange range;
    range.__set_lower_bound(lower);
    range.__set_upper_bound(upper);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_CHAR));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    // Only route rows that are expected to fall into ["banana","date").
    std::vector<uint16_t> row_indices = {1, 2};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    // Expected selected indices: 1 ("banana"), 2 ("cherry").
    for (int i = 0; i < static_cast<int>(values.size()); ++i) {
        if (i == 1 || i == 2) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " value " << values[i] << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " value " << values[i] << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, BooleanVariantPointRange) {
    // BOOLEAN column with values [false, true, false, true, true, false].
    Chunk chunk = make_bool_chunk("b1", {0, 1, 0, 1, 1, 0});
    // Range [true, true] encoded as BOOLEAN TVariant using long_value(0/1),
    // consistent with FE BoolVariant.toThrift().
    TTabletRange range = make_single_bool_range(/*lower=*/true, /*lower_included=*/true,
                                                /*upper=*/true, /*upper_included=*/true);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "b1", TypeDescriptor::from_logical_type(TYPE_BOOLEAN));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    // Only route rows with value == true (1).
    std::vector<uint16_t> row_indices = {1, 3, 4};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    // Only rows with value == true (1) should be selected.
    std::vector<uint8_t> values = {0, 1, 0, 1, 1, 0};
    for (int i = 0; i < static_cast<int>(values.size()); ++i) {
        if (values[i] == 1) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, VarcharVariantRange) {
    // VARCHAR column with lexicographically ordered values.
    std::vector<std::string> values = {"apple", "banana", "cherry", "date", "fig"};
    Chunk chunk = make_varchar_chunk("s1", values);

    // Range ["banana", "date") encoded as VARCHAR TVariant using string_value,
    // consistent with FE StringVariant.toThrift()/DateVariant.toThrift()/LargeIntVariant.toThrift().
    TTabletRange range =
            make_single_string_range(/*lower=*/std::string("banana"), /*lower_included=*/true,
                                     /*upper=*/std::string("date"), /*upper_included=*/false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "s1", TypeDescriptor::from_logical_type(TYPE_VARCHAR));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    // Only route rows that are expected to fall into ["banana","date").
    std::vector<uint16_t> row_indices = {1, 2};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    // Expected selected indices: 1 ("banana"), 2 ("cherry").
    for (int i = 0; i < static_cast<int>(values.size()); ++i) {
        if (i == 1 || i == 2) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " value " << values[i] << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " value " << values[i] << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, BinaryVariantEncoding) {
    // Verify that BINARY TVariant is initialized with string_value only
    // and correct type descriptor from FE side.
    TVariant v = make_binary_variant("banana");
    ASSERT_TRUE(v.__isset.type);
    ASSERT_FALSE(v.__isset.long_value);
    ASSERT_TRUE(v.__isset.string_value);
    ASSERT_EQ("banana", v.string_value);
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, VarbinaryVariantRange) {
    // VARBINARY column with lexicographically ordered values (treated as raw bytes).
    std::vector<std::string> values = {"apple", "banana", "cherry", "date", "fig"};
    Chunk chunk = make_varbinary_chunk("v1", values);

    // Range ["banana", "date") encoded as VARBINARY TVariant using string_value only.
    TVariant v_low = make_varbinary_variant("banana");
    TVariant v_up = make_varbinary_variant("date");
    TTuple lower;
    lower.__set_values(std::vector<TVariant>{v_low});
    TTuple upper;
    upper.__set_values(std::vector<TVariant>{v_up});

    TTabletRange range;
    range.__set_lower_bound(lower);
    range.__set_upper_bound(upper);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "v1", TypeDescriptor::from_logical_type(TYPE_VARBINARY));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    // Only route rows that are expected to fall into ["banana","date").
    std::vector<uint16_t> row_indices = {1, 2};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < static_cast<int>(values.size()); ++i) {
        if (i == 1 || i == 2) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " value " << values[i] << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " value " << values[i] << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, HllVariantInitNotSupported) {
    // HLL TVariant encoded using string_value only.
    TVariant v_low = make_hll_variant("dummy_hll_value");
    TTuple lower;
    lower.__set_values(std::vector<TVariant>{v_low});

    TTabletRange range;
    range.__set_lower_bound(lower);
    range.__set_lower_bound_included(true);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    // Single HLL-distributed column named "h1".
    RangeRouter checker;

    // Currently HLL type is not supported in RangeRouter.init() because
    // datum_from_string() does not handle TYPE_HLL. We still create this case to
    // verify TVariant is initialized via string_value and that init() fails with
    // a NotSupported status instead of crashing.
    Status st = checker.init(ranges, 1);
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, LargeIntVariantRange) {
    // LARGEINT column with ordered values.
    std::vector<int128_t> values = {static_cast<int128_t>(100), static_cast<int128_t>(200), static_cast<int128_t>(300),
                                    static_cast<int128_t>(400), static_cast<int128_t>(500)};
    Chunk chunk = make_largeint_chunk("l1", values);

    // Range ["200", "400") encoded as LARGEINT TVariant using string_value only.
    TVariant v_low = make_largeint_variant("200");
    TVariant v_up = make_largeint_variant("400");
    TTuple lower;
    lower.__set_values(std::vector<TVariant>{v_low});
    TTuple upper;
    upper.__set_values(std::vector<TVariant>{v_up});

    TTabletRange range;
    range.__set_lower_bound(lower);
    range.__set_upper_bound(upper);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(false);

    std::vector<TTabletRange> ranges{range};
    std::vector<int64_t> tablet_ids{1};
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    SlotDescriptor slot_desc(0, "l1", TypeDescriptor::from_logical_type(TYPE_LARGEINT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    // Only route rows that are expected to fall into ["200","400").
    std::vector<uint16_t> row_indices = {1, 2};
    std::vector<int64_t> routed_ids(chunk.num_rows(), 0);
    Status st = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_TRUE(st.ok());

    // Expected selected indices: 1 (200), 2 (300).
    for (int i = 0; i < static_cast<int>(values.size()); ++i) {
        if (i == 1 || i == 2) {
            ASSERT_EQ(1, routed_ids[i]) << "row " << i << " should be selected";
        } else {
            ASSERT_EQ(0, routed_ids[i]) << "row " << i << " should not be selected";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(RangeRouterTest, ManyRangesBinarySearch) {
    // Construct 9 consecutive ranges to exercise the binary search path.
    // Ranges: [0,10), [10,20), ..., [80,90)
    std::vector<TTabletRange> ranges;
    std::vector<int64_t> tablet_ids;
    for (int i = 0; i < 9; ++i) {
        ranges.emplace_back(make_single_long_range(i * 10, true, (i + 1) * 10, false));
        tablet_ids.emplace_back(100 + i);
    }

    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());

    // Build a chunk whose values fall into each range.
    std::vector<int32_t> values;
    for (int i = 0; i < 9; ++i) {
        values.push_back(i * 10);     // left boundary
        values.push_back(i * 10 + 5); // middle point
    }
    Chunk chunk = make_int_chunk("c1", values);

    std::vector<uint16_t> row_indices;
    for (int i = 0; i < chunk.num_rows(); ++i) {
        row_indices.emplace_back(i);
    }
    std::vector<int64_t> routed_ids(chunk.num_rows(), -1);
    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    ASSERT_TRUE(checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids).ok());

    for (int i = 0; i < chunk.num_rows(); ++i) {
        int v = values[i];
        int range_idx = v / 10; // each [k*10, (k+1)*10)
        ASSERT_EQ(100 + range_idx, routed_ids[i]) << "row " << i << " value " << v << " mismatch";
    }
}

// Test overlapping ranges detection
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, OverlappingRangesDetection) {
    // Case 1: Ranges with overlapping values [0, 10] and [5, 15]
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(0, true, 10, true));  // [0, 10]
        ranges.emplace_back(make_single_long_range(5, true, 15, true));  // [5, 15] - overlaps!
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.message().find("Overlapping ranges detected") != std::string::npos) 
            << "Expected overlapping ranges error, got: " << status.message();
    }
    
    // Case 2: Adjacent ranges with inclusive bounds should be detected as overlap
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(0, true, 10, true));   // [0, 10]
        ranges.emplace_back(make_single_long_range(10, true, 20, false)); // [10, 20) - overlap at 10
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.message().find("Overlapping ranges detected") != std::string::npos) 
            << "Expected overlapping ranges error, got: " << status.message();
    }
    
    // Case 3: Adjacent ranges with exclusive-inclusive bounds should be OK
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(0, true, 10, false));  // [0, 10)
        ranges.emplace_back(make_single_long_range(10, true, 20, false)); // [10, 20) - OK, no overlap
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_TRUE(status.ok()) << "Expected success for non-overlapping ranges, got: " << status.message();
    }
}

// Test out-of-order ranges detection  
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, OutOfOrderRangesDetection) {
    // Case 1: Ranges completely out of order [20, 30) before [0, 10)
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(20, true, 30, false)); // [20, 30)
        ranges.emplace_back(make_single_long_range(0, true, 10, false));  // [0, 10) - wrong order!
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.message().find("not properly ordered") != std::string::npos) 
            << "Expected out-of-order error, got: " << status.message();
    }
    
    // Case 2: Ranges with gap but wrong order [10, 20) before [5, 8)
    {
        std::vector<TTabletRange> ranges;
        ranges.emplace_back(make_single_long_range(10, true, 20, false)); // [10, 20)
        ranges.emplace_back(make_single_long_range(5, true, 8, false));   // [5, 8) - wrong order!
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.message().find("not properly ordered") != std::string::npos) 
            << "Expected out-of-order error, got: " << status.message();
    }
}

// Test infinity ranges validation
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, InfinityRangesValidation) {
    // Case 1: -inf range not at first position
    {
        std::vector<TTabletRange> ranges;
        TTabletRange r1 = make_single_long_range(0, true, 10, false);
        TTabletRange r2;
        TTuple upper_tuple;
        upper_tuple.__set_values(std::vector<TVariant>{make_int_variant(20)});
        r2.upper_bound = upper_tuple;
        r2.upper_bound_included = false;
        // r2 has no lower bound (i.e., -inf), but it's not the first range
        ranges.push_back(r1);
        ranges.push_back(r2);
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.message().find("no lower bound but is not the first range") != std::string::npos) 
            << "Expected -inf position error, got: " << status.message();
    }
    
    // Case 2: +inf range not at last position
    {
        std::vector<TTabletRange> ranges;
        TTabletRange r1;
        TTuple lower_tuple;
        lower_tuple.__set_values(std::vector<TVariant>{make_int_variant(0)});
        r1.lower_bound = lower_tuple;
        r1.lower_bound_included = true;
        // r1 has no upper bound (i.e., +inf)
        
        TTabletRange r2 = make_single_long_range(20, true, 30, false);
        ranges.push_back(r1);
        ranges.push_back(r2); // r1 has +inf but is not last
        
        RangeRouter checker;
        auto status = checker.init(ranges, 1);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.message().find("no upper bound but is not the last range") != std::string::npos) 
            << "Expected +inf position error, got: " << status.message();
    }
}

// Test multi-column range validation with overlapping
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, MultiColumnOverlappingRanges) {
    // Multi-column ranges: [(1, 'a'), (2, 'b')) and [(1, 'z'), (3, 'a'))
    // These ranges overlap because (2, 'a') would fall in between
    std::vector<TTabletRange> ranges;
    
    // First range: [(1, 'a'), (2, 'b'))
    TTabletRange r1;
    r1.lower_bound.values.push_back(make_int_variant(1));
    r1.lower_bound.values.push_back(make_string_variant("a"));
    r1.lower_bound.__isset.values = true;
    r1.lower_bound_included = true;
    r1.__isset.lower_bound_included = true;
    r1.__isset.lower_bound = true;
    
    r1.upper_bound.values.push_back(make_int_variant(2));
    r1.upper_bound.values.push_back(make_string_variant("b"));
    r1.upper_bound.__isset.values = true;
    r1.upper_bound_included = false;
    r1.__isset.upper_bound_included = true;
    r1.__isset.upper_bound = true;
    
    // Second range: [(1, 'z'), (3, 'a'))  
    TTabletRange r2;
    r2.lower_bound.values.push_back(make_int_variant(1));
    r2.lower_bound.values.push_back(make_string_variant("z"));
    r2.lower_bound.__isset.values = true;
    r2.lower_bound_included = true;
    r2.__isset.lower_bound_included = true;
    r2.__isset.lower_bound = true;
    
    r2.upper_bound.values.push_back(make_int_variant(3));
    r2.upper_bound.values.push_back(make_string_variant("a"));
    r2.upper_bound.__isset.values = true;
    r2.upper_bound_included = false;
    r2.__isset.upper_bound_included = true;
    r2.__isset.upper_bound = true;
    
    ranges.push_back(r1);
    ranges.push_back(r2);
    
    RangeRouter checker;
    auto status = checker.init(ranges, 2);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.message().find("not properly ordered") != std::string::npos) 
        << "Expected ordering error for multi-column ranges, got: " << status.message();
}

// Test error messages with detailed row information
// NOLINTNEXTLINE
TEST_F(RangeRouterTest, DetailedErrorMessages) {
    // Create ranges [0, 10) and [20, 30) with a gap
    std::vector<TTabletRange> ranges;
    ranges.emplace_back(make_single_long_range(0, true, 10, false));
    ranges.emplace_back(make_single_long_range(20, true, 30, false));
    std::vector<int64_t> tablet_ids{100, 200};
    
    RangeRouter checker;
    ASSERT_TRUE(checker.init(ranges, 1).ok());
    
    // Create chunk with value 15 that falls in the gap
    std::vector<int32_t> values{5, 15, 25}; // 15 is in the gap
    Chunk chunk = make_int_chunk("c1", values);
    
    SlotDescriptor slot_desc(0, "c1", TypeDescriptor::from_logical_type(TYPE_INT));
    std::vector<SlotDescriptor*> slot_descs{&slot_desc};
    chunk.set_slot_id_to_index(slot_desc.id(), 0);
    
    std::vector<uint16_t> row_indices{0, 1, 2};
    std::vector<int64_t> routed_ids(chunk.num_rows(), -1);
    
    auto status = checker.route_chunk_rows(&chunk, slot_descs, row_indices, tablet_ids, &routed_ids);
    ASSERT_FALSE(status.ok());
    // Check that error message contains row information
    ASSERT_TRUE(status.message().find("row 1") != std::string::npos ||
                status.message().find("Row index: 1") != std::string::npos) 
        << "Expected error message to contain row index, got: " << status.message();
}

} // namespace starrocks
