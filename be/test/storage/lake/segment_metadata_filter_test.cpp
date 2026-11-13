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

#include "storage/lake/segment_metadata_filter.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

<<<<<<< HEAD
#include "runtime/types.h"
#include "storage/column_predicate.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/tablet_schema.h"
=======
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "storage/datum_variant.h"
#include "storage/lake/sort_key_bounds_test_util.h"
#include "storage/sort_key_bounds.h"
#include "storage/tablet_schema.h"
#include "storage/variant_tuple.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_tree/predicate_tree.hpp"
#include "types/type_descriptor.h"
>>>>>>> cb9d0bd ([BugFix][RB-1485] Store segment sort key bounds in a compact encoding (#83155))

namespace starrocks::lake {

class SegmentMetadataFilterTest : public ::testing::Test {
public:
    void SetUp() override {
        // Create a simple tablet schema with two INT columns as sort keys.
        // Column 0 (c0): INT, sort key
        // Column 1 (c1): INT, sort key
        // Column 2 (c2): INT, not a sort key
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(2);

        auto* c0 = schema_pb.add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto* c1 = schema_pb.add_column();
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(true);
        c1->set_is_nullable(false);

        auto* c2 = schema_pb.add_column();
        c2->set_unique_id(2);
        c2->set_name("c2");
        c2->set_type("INT");
        c2->set_is_key(false);
        c2->set_is_nullable(false);
        c2->set_aggregation("NONE");

        // Set sort key column indices: [0, 1]
        schema_pb.add_sort_key_idxes(0);
        schema_pb.add_sort_key_idxes(1);

        _tablet_schema = TabletSchema::create(schema_pb);
    }

protected:
    // Helper function to create a VariantPB for an INT value.
    static VariantPB make_int_variant(int32_t value) {
        VariantPB var;
        TypeDescriptor td(TYPE_INT);
        *var.mutable_type() = td.to_protobuf();
        var.set_value(std::to_string(value));
        return var;
    }

    // Helper function to create a VariantPB for a null INT value.
    static VariantPB make_null_int_variant() {
        VariantPB var;
        TypeDescriptor td(TYPE_INT);
        *var.mutable_type() = td.to_protobuf();
        // No value set means null
        return var;
    }

    // Helper function to create segment metadata with sort key min/max.
    static SegmentMetadataPB create_segment_meta(const std::vector<int32_t>& min_values,
                                                 const std::vector<int32_t>& max_values) {
        SegmentMetadataPB meta;
        auto* min_tuple = meta.mutable_deprecated_sort_key_min();
        for (int32_t v : min_values) {
            *min_tuple->add_values() = make_int_variant(v);
        }
        auto* max_tuple = meta.mutable_deprecated_sort_key_max();
        for (int32_t v : max_values) {
            *max_tuple->add_values() = make_int_variant(v);
        }
        meta.set_num_rows(100);
        return meta;
    }

    // Helper function to create a predicate tree with a single column predicate.
    static PredicateTree create_pred_tree_with_column_pred(const ColumnPredicate* pred) {
        PredicateAndNode root;
        root.add_child(PredicateColumnNode(pred));
        return PredicateTree::create(std::move(root));
    }

    std::shared_ptr<TabletSchema> _tablet_schema;
};

// Test: No sort_key_min/max metadata, should return true (may contain).
TEST_F(SegmentMetadataFilterTest, test_no_metadata) {
    SegmentMetadataPB meta;
    // No sort_key_min or sort_key_max set

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: Empty min_tuple values, should return true.
TEST_F(SegmentMetadataFilterTest, test_empty_tuple_values) {
    SegmentMetadataPB meta;
    meta.mutable_deprecated_sort_key_min(); // Empty tuple
    auto* max_tuple = meta.mutable_deprecated_sort_key_max();
    *max_tuple->add_values() = make_int_variant(100);

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: EQ predicate - value within range, should return true.
TEST_F(SegmentMetadataFilterTest, test_eq_within_range) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 = 50 (within range)
    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: EQ predicate - value outside range, should return false.
TEST_F(SegmentMetadataFilterTest, test_eq_outside_range) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 = 5 (below range)
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "5"));
    PredicateTree pred_tree1 = create_pred_tree_with_column_pred(pred1.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree1, *_tablet_schema));

    // Predicate: c0 = 200 (above range)
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "200"));
    PredicateTree pred_tree2 = create_pred_tree_with_column_pred(pred2.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree2, *_tablet_schema));
}

// Test: GE predicate - range overlaps, should return true.
TEST_F(SegmentMetadataFilterTest, test_ge_overlaps) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 >= 50 (overlaps with [50, 100])
    std::unique_ptr<ColumnPredicate> pred(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: GE predicate - no overlap, should return false.
TEST_F(SegmentMetadataFilterTest, test_ge_no_overlap) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 >= 200 (no overlap, segment max is 100)
    std::unique_ptr<ColumnPredicate> pred(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "200"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: LE predicate - range overlaps, should return true.
TEST_F(SegmentMetadataFilterTest, test_le_overlaps) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 <= 50 (overlaps with [10, 50])
    std::unique_ptr<ColumnPredicate> pred(new_column_le_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: LE predicate - no overlap, should return false.
TEST_F(SegmentMetadataFilterTest, test_le_no_overlap) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 <= 5 (no overlap, segment min is 10)
    std::unique_ptr<ColumnPredicate> pred(new_column_le_predicate(get_type_info(TYPE_INT), 0, "5"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: Second sort key column predicate should NOT be used for pruning.
// sort_key_min/max are composite tuple values (first/last row in sort order).
// Only the leading sort key column (position 0) has correct per-column min/max.
// For non-leading columns, the tuple values are NOT per-column min/max.
TEST_F(SegmentMetadataFilterTest, test_second_sort_key_column) {
    // Segment composite tuple: min=(10, 20), max=(100, 200)
    // But c1's actual per-column min/max in the segment could be anything.
    SegmentMetadataPB meta = create_segment_meta({10, 20}, {100, 200});

    // Predicate: c1 = 50
    // Non-leading sort key column cannot be used for pruning, must return may_contain=true.
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "50"));
    PredicateTree pred_tree1 = create_pred_tree_with_column_pred(pred1.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree1, *_tablet_schema));

    // Predicate: c1 = 5 (appears outside composite tuple range, but we cannot prune)
    // Because composite tuple min/max != per-column min/max for non-leading columns.
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));
    PredicateTree pred_tree2 = create_pred_tree_with_column_pred(pred2.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree2, *_tablet_schema));
}

// Test: Non-sort-key column predicate - cannot filter.
TEST_F(SegmentMetadataFilterTest, test_non_sort_key_column) {
    // Segment has c0 range [10, 100], c1 range [20, 200]
    SegmentMetadataPB meta = create_segment_meta({10, 20}, {100, 200});

    // Predicate: c2 = 5 (c2 is not a sort key, cannot filter)
    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "5"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // Should return true because we cannot use non-sort-key columns for filtering.
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: AND predicate tree - c1 is non-leading sort key, cannot prune.
TEST_F(SegmentMetadataFilterTest, test_and_predicate_any_can_prune) {
    // Segment composite tuple: min=(10, 20), max=(100, 200)
    SegmentMetadataPB meta = create_segment_meta({10, 20}, {100, 200});

    // Predicate: c0 = 50 AND c1 = 5
    // c0 = 50 is within range (cannot prune).
    // c1 is non-leading sort key column, cannot be used for pruning.
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));

    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred1.get()));
    root.add_child(PredicateColumnNode(pred2.get()));
    PredicateTree pred_tree = PredicateTree::create(std::move(root));

    // Neither can prune: c0=50 is in range, c1 is non-leading. Should return true.
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: AND predicate - leading sort key outside range can still prune.
TEST_F(SegmentMetadataFilterTest, test_and_predicate_leading_key_can_prune) {
    // Segment composite tuple: min=(10, 20), max=(100, 200)
    SegmentMetadataPB meta = create_segment_meta({10, 20}, {100, 200});

    // Predicate: c0 = 5 AND c1 = 50
    // c0 = 5 is outside range for leading sort key, CAN prune.
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "5"));
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "50"));

    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred1.get()));
    root.add_child(PredicateColumnNode(pred2.get()));
    PredicateTree pred_tree = PredicateTree::create(std::move(root));

    // c0 = 5 can prune (leading key outside range), so should return false.
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: AND predicate tree - none can prune.
TEST_F(SegmentMetadataFilterTest, test_and_predicate_none_can_prune) {
    // Segment has c0 range [10, 100], c1 range [20, 200]
    SegmentMetadataPB meta = create_segment_meta({10, 20}, {100, 200});

    // Predicate: c0 = 50 AND c1 = 50
    // Both within range.
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "50"));

    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred1.get()));
    root.add_child(PredicateColumnNode(pred2.get()));
    PredicateTree pred_tree = PredicateTree::create(std::move(root));

    // Neither can prune, should return true
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: OR predicate tree - all children must prune for the whole OR to prune.
TEST_F(SegmentMetadataFilterTest, test_or_predicate_all_prune) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 = 5 OR c0 = 200
    // Both outside range.
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "5"));
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "200"));

    PredicateOrNode or_node;
    or_node.add_child(PredicateColumnNode(pred1.get()));
    or_node.add_child(PredicateColumnNode(pred2.get()));

    PredicateAndNode root;
    root.add_child(std::move(or_node));
    PredicateTree pred_tree = PredicateTree::create(std::move(root));

    // Both can prune, so should return false
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: OR predicate tree - not all can prune.
TEST_F(SegmentMetadataFilterTest, test_or_predicate_partial_prune) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 = 50 OR c0 = 200
    // c0 = 50 is within range, c0 = 200 is outside range.
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "200"));

    PredicateOrNode or_node;
    or_node.add_child(PredicateColumnNode(pred1.get()));
    or_node.add_child(PredicateColumnNode(pred2.get()));

    PredicateAndNode root;
    root.add_child(std::move(or_node));
    PredicateTree pred_tree = PredicateTree::create(std::move(root));

    // c0 = 50 cannot prune, so should return true
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: IS NULL predicate - not supported, cannot filter.
TEST_F(SegmentMetadataFilterTest, test_is_null_predicate) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // IS NULL predicates are not supported for metadata filtering.
    std::unique_ptr<ColumnPredicate> pred(new_column_null_predicate(get_type_info(TYPE_INT), 0, true));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // Should return true (cannot filter with IS NULL)
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: IS NOT NULL predicate - not supported, cannot filter.
TEST_F(SegmentMetadataFilterTest, test_is_not_null_predicate) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // IS NOT NULL predicates are not supported for metadata filtering.
    std::unique_ptr<ColumnPredicate> pred(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // Should return true (cannot filter with IS NOT NULL)
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: Empty predicate tree, should return true.
TEST_F(SegmentMetadataFilterTest, test_empty_predicate_tree) {
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    PredicateTree pred_tree;
    ASSERT_TRUE(pred_tree.empty());

    // Empty predicate tree should be handled by the caller.
    // But our function should still work correctly.
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: LT predicate.
TEST_F(SegmentMetadataFilterTest, test_lt_predicate) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 < 5 (no overlap)
    std::unique_ptr<ColumnPredicate> pred1(new_column_lt_predicate(get_type_info(TYPE_INT), 0, "5"));
    PredicateTree pred_tree1 = create_pred_tree_with_column_pred(pred1.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree1, *_tablet_schema));

    // Predicate: c0 < 50 (overlap)
    std::unique_ptr<ColumnPredicate> pred2(new_column_lt_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree2 = create_pred_tree_with_column_pred(pred2.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree2, *_tablet_schema));
}

// Test: GT predicate.
TEST_F(SegmentMetadataFilterTest, test_gt_predicate) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 > 200 (no overlap)
    std::unique_ptr<ColumnPredicate> pred1(new_column_gt_predicate(get_type_info(TYPE_INT), 0, "200"));
    PredicateTree pred_tree1 = create_pred_tree_with_column_pred(pred1.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree1, *_tablet_schema));

    // Predicate: c0 > 50 (overlap)
    std::unique_ptr<ColumnPredicate> pred2(new_column_gt_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree2 = create_pred_tree_with_column_pred(pred2.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree2, *_tablet_schema));
}

// Test: NE predicate - generally cannot be used for zone map filtering effectively.
TEST_F(SegmentMetadataFilterTest, test_ne_predicate) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 != 50
    // NE predicates typically cannot prune unless all values equal the NE value.
    std::unique_ptr<ColumnPredicate> pred(new_column_ne_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // Should return true (NE with a value in range cannot prune)
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: Range where min == max (single value segment).
TEST_F(SegmentMetadataFilterTest, test_single_value_range) {
    // Segment has c0 = 50 (min == max)
    SegmentMetadataPB meta = create_segment_meta({50, 0}, {50, 100});

    // Predicate: c0 = 50 (match)
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "50"));
    PredicateTree pred_tree1 = create_pred_tree_with_column_pred(pred1.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree1, *_tablet_schema));

    // Predicate: c0 = 60 (no match)
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "60"));
    PredicateTree pred_tree2 = create_pred_tree_with_column_pred(pred2.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree2, *_tablet_schema));
}

// Test: Empty sort_key_idxes in tablet schema.
TEST_F(SegmentMetadataFilterTest, test_empty_sort_key_idxes) {
    // Create a tablet schema without sort keys.
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(0);

    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(false);
    c0->set_is_nullable(false);
    c0->set_aggregation("NONE");

    auto schema_no_sort_key = TabletSchema::create(schema_pb);

    SegmentMetadataPB meta = create_segment_meta({10}, {100});

    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "5"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // Should return true because no sort keys.
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *schema_no_sort_key));
}

// Test: Boundary cases - predicate value equals min or max.
TEST_F(SegmentMetadataFilterTest, test_boundary_values) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 = 10 (equals min, should match)
    std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "10"));
    PredicateTree pred_tree1 = create_pred_tree_with_column_pred(pred1.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree1, *_tablet_schema));

    // Predicate: c0 = 100 (equals max, should match)
    std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
    PredicateTree pred_tree2 = create_pred_tree_with_column_pred(pred2.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree2, *_tablet_schema));

    // Predicate: c0 >= 100 (boundary, should match)
    std::unique_ptr<ColumnPredicate> pred3(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "100"));
    PredicateTree pred_tree3 = create_pred_tree_with_column_pred(pred3.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree3, *_tablet_schema));

    // Predicate: c0 <= 10 (boundary, should match)
    std::unique_ptr<ColumnPredicate> pred4(new_column_le_predicate(get_type_info(TYPE_INT), 0, "10"));
    PredicateTree pred_tree4 = create_pred_tree_with_column_pred(pred4.get());
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree4, *_tablet_schema));

    // Predicate: c0 > 100 (just beyond max, should not match)
    std::unique_ptr<ColumnPredicate> pred5(new_column_gt_predicate(get_type_info(TYPE_INT), 0, "100"));
    PredicateTree pred_tree5 = create_pred_tree_with_column_pred(pred5.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree5, *_tablet_schema));

    // Predicate: c0 < 10 (just below min, should not match)
    std::unique_ptr<ColumnPredicate> pred6(new_column_lt_predicate(get_type_info(TYPE_INT), 0, "10"));
    PredicateTree pred_tree6 = create_pred_tree_with_column_pred(pred6.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree6, *_tablet_schema));
}

// Test: IN predicate - all values outside range.
TEST_F(SegmentMetadataFilterTest, test_in_predicate_all_outside) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 IN (1, 2, 3) - all values below min
    std::vector<std::string> values = {"1", "2", "3"};
    std::unique_ptr<ColumnPredicate> pred(new_column_in_predicate(get_type_info(TYPE_INT), 0, values));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // All IN values are outside range, should not match
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: IN predicate - some values inside range.
TEST_F(SegmentMetadataFilterTest, test_in_predicate_partial_inside) {
    // Segment has c0 range [10, 100]
    SegmentMetadataPB meta = create_segment_meta({10, 0}, {100, 100});

    // Predicate: c0 IN (5, 50, 200) - 50 is within range
    std::vector<std::string> values = {"5", "50", "200"};
    std::unique_ptr<ColumnPredicate> pred(new_column_in_predicate(get_type_info(TYPE_INT), 0, values));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());

    // 50 is within range, should match
    ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema));
}

// Test: Non-leading sort key with custom ORDER BY — the exact bug scenario.
// Table: PRIMARY KEY(c0, c1) ORDER BY(c2, c0, c1)
// sort_key_idxes = [2, 0, 1]
// Query: WHERE c0 = X
// c0 is at sort_key_pos=1 (non-leading). The composite tuple min/max at position 1
// are NOT the actual per-column min/max of c0. Must NOT prune.
TEST_F(SegmentMetadataFilterTest, test_non_leading_sort_key_custom_order_by) {
    // Create schema: c0 INT (key), c1 INT (key), c2 INT (not key)
    // ORDER BY(c2, c0, c1) => sort_key_idxes = [2, 0, 1]
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(2);

    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto* c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(true);
    c1->set_is_nullable(false);

    auto* c2 = schema_pb.add_column();
    c2->set_unique_id(2);
    c2->set_name("c2");
    c2->set_type("INT");
    c2->set_is_key(false);
    c2->set_is_nullable(false);
    c2->set_aggregation("NONE");

    // ORDER BY(c2, c0, c1) => sort_key_idxes = [2, 0, 1]
    schema_pb.add_sort_key_idxes(2);
    schema_pb.add_sort_key_idxes(0);
    schema_pb.add_sort_key_idxes(1);

    auto custom_schema = TabletSchema::create(schema_pb);

    // Simulate a segment sorted by (c2, c0, c1).
    // Composite min row: (c2=1, c0=500, c1=10)  — first row in sort order
    // Composite max row: (c2=99, c0=50, c1=90)   — last row in sort order
    // Actual c0 values in segment could range from e.g. 10 to 900.
    // The composite tuple gives c0 "min"=500, c0 "max"=50 — which is WRONG as per-column min/max.
    SegmentMetadataPB meta;
    auto* min_tuple = meta.mutable_deprecated_sort_key_min();
    *min_tuple->add_values() = make_int_variant(1);   // c2 min (leading, correct)
    *min_tuple->add_values() = make_int_variant(500); // c0 from first row (NOT per-column min)
    *min_tuple->add_values() = make_int_variant(10);  // c1 from first row (NOT per-column min)
    auto* max_tuple = meta.mutable_deprecated_sort_key_max();
    *max_tuple->add_values() = make_int_variant(99); // c2 max (leading, correct)
    *max_tuple->add_values() = make_int_variant(50); // c0 from last row (NOT per-column max)
    *max_tuple->add_values() = make_int_variant(90); // c1 from last row (NOT per-column max)
    meta.set_num_rows(100);

    // Predicate: c2 = 50 (leading sort key, within range [1, 99]) — should not prune.
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "50"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }

    // Predicate: c2 = 200 (leading sort key, outside range [1, 99]) — should prune.
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "200"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }

    // Predicate: c0 = 100 (non-leading sort key)
    // Composite tuple gives c0 "range" [500, 50] which is nonsensical.
    // Before the fix, this would incorrectly prune the segment.
    // After the fix, non-leading columns must NOT prune.
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }

    // Predicate: c0 = 500 (equals composite min row's c0 value)
    // Still non-leading, must NOT prune.
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "500"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }

    // Predicate: c1 = 5 (non-leading, position 2 in sort key)
    // Must NOT prune.
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }

    // AND predicate: c2 = 200 AND c0 = 100
    // c2 = 200 can prune (leading, outside range). c0 = 100 cannot (non-leading).
    // AND: if any child can prune, the whole AND prunes.
    {
        std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "200"));
        std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
        PredicateAndNode root;
        root.add_child(PredicateColumnNode(pred1.get()));
        root.add_child(PredicateColumnNode(pred2.get()));
        PredicateTree pred_tree = PredicateTree::create(std::move(root));
        ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }

    // AND predicate: c2 = 50 AND c0 = 100
    // c2 = 50 cannot prune (in range). c0 = 100 cannot (non-leading).
    // Should not prune.
    {
        std::unique_ptr<ColumnPredicate> pred1(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "50"));
        std::unique_ptr<ColumnPredicate> pred2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
        PredicateAndNode root;
        root.add_child(PredicateColumnNode(pred1.get()));
        root.add_child(PredicateColumnNode(pred2.get()));
        PredicateTree pred_tree = PredicateTree::create(std::move(root));
        ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *custom_schema));
    }
}

// Regression test: VARCHAR sort key with EQ predicate previously caused heap-use-after-free
// because build_zone_map_detail returned a ZoneMapDetail holding shallow-copied Slices
// while the owning DatumVariants had already been destroyed.
TEST_F(SegmentMetadataFilterTest, test_varchar_sort_key_no_use_after_free) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);

    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("VARCHAR");
    c0->set_length(128);
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto* c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(false);
    c1->set_aggregation("NONE");

    schema_pb.add_sort_key_idxes(0);
    auto varchar_schema = TabletSchema::create(schema_pb);

    auto make_varchar_variant = [](const std::string& value) {
        VariantPB var;
        TypeDescriptor td = TypeDescriptor::create_varchar_type(128);
        *var.mutable_type() = td.to_protobuf();
        var.set_variant_type(VariantTypePB::NORMAL_VALUE);
        var.set_value(value);
        return var;
    };

    SegmentMetadataPB meta;
    auto* min_tuple = meta.mutable_deprecated_sort_key_min();
    *min_tuple->add_values() = make_varchar_variant("apple");
    auto* max_tuple = meta.mutable_deprecated_sort_key_max();
    *max_tuple->add_values() = make_varchar_variant("mango");
    meta.set_num_rows(100);

    // Value within range
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "banana"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *varchar_schema));
    }

    // Value below range
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "aaa"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *varchar_schema));
    }

    // Value above range
    {
        std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "zzz"));
        PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
        ASSERT_FALSE(SegmentMetadataFilter::may_contain(meta, pred_tree, *varchar_schema));
    }
}

namespace {

// The same segment with its bounds moved into the encoded pair.
SegmentMetadataPB with_encoded_bounds(const SegmentMetadataPB& legacy) {
    RowsetMetadataPB rowset;
    *rowset.add_segment_metas() = legacy;
    convert_sort_key_bounds(&rowset, SortKeyBoundsRepr::kEncoded);
    return rowset.segment_metas(0);
}

// A two-column INT tuple whose first value may be NULL, in the form the segment writer stores in the deprecated pair.
TuplePB make_int_pair_tuple(std::optional<int32_t> first, int32_t second) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(TYPE_INT), first.has_value() ? Datum(*first) : Datum()));
    tuple.append(DatumVariant(get_type_info(TYPE_INT), Datum(second)));
    TuplePB tuple_pb;
    tuple.to_proto(&tuple_pb);
    return tuple_pb;
}

// Predicates on the leading sort key column c0, each with a name for failure messages.
std::vector<std::pair<std::string, std::unique_ptr<ColumnPredicate>>> leading_column_predicates() {
    std::vector<std::pair<std::string, std::unique_ptr<ColumnPredicate>>> predicates;
    const auto type = get_type_info(TYPE_INT);
    for (const char* value : {"-1", "5", "10", "50", "100", "200"}) {
        predicates.emplace_back(fmt::format("c0 = {}", value), new_column_eq_predicate(type, 0, value));
        predicates.emplace_back(fmt::format("c0 < {}", value), new_column_lt_predicate(type, 0, value));
        predicates.emplace_back(fmt::format("c0 > {}", value), new_column_gt_predicate(type, 0, value));
    }
    for (const std::vector<std::string>& values :
         std::vector<std::vector<std::string>>{{"1", "2", "3"}, {"5", "50", "200"}, {"100"}, {"150", "300"}}) {
        const std::string list = fmt::format("({})", fmt::join(values, ", "));
        predicates.emplace_back("c0 IN " + list, new_column_in_predicate(type, 0, values));
        predicates.emplace_back("c0 NOT IN " + list, new_column_not_in_predicate(type, 0, values));
    }
    return predicates;
}

} // namespace

// The encoded pair prunes exactly like the deprecated one for every comparison, IN and NOT IN predicate on the
// leading sort key column.
TEST_F(SegmentMetadataFilterTest, EncodedBoundsPruneLikeLegacy) {
    const std::vector<SegmentMetadataPB> legacy_segments = {create_segment_meta({10, 0}, {100, 100}),
                                                            create_segment_meta({50, 0}, {50, 7})};
    int pruned = 0;
    for (const auto& legacy : legacy_segments) {
        const auto encoded = with_encoded_bounds(legacy);
        ASSERT_FALSE(encoded.has_deprecated_sort_key_min());
        for (const auto& [name, pred] : leading_column_predicates()) {
            PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
            const bool legacy_may_contain = SegmentMetadataFilter::may_contain(legacy, pred_tree, *_tablet_schema);
            EXPECT_EQ(legacy_may_contain, SegmentMetadataFilter::may_contain(encoded, pred_tree, *_tablet_schema))
                    << name << " on " << legacy.ShortDebugString();
            pruned += legacy_may_contain ? 0 : 1;
        }
    }
    EXPECT_GT(pruned, 0) << "no predicate pruned, so the comparison above proves nothing";
}

// A nullable leading sort key column whose minimum is NULL: the encoded pair prunes exactly like the deprecated one,
// including a segment whose maximum is NULL as well.
TEST_F(SegmentMetadataFilterTest, EncodedNullLeadingMinMatchesLegacy) {
    TabletSchemaPB schema_pb;
    _tablet_schema->to_schema_pb(&schema_pb);
    schema_pb.mutable_column(0)->set_is_nullable(true);
    auto nullable_schema = TabletSchema::create(schema_pb);

    int pruned = 0;
    for (const std::optional<int32_t> max_leading : {std::optional<int32_t>(100), std::optional<int32_t>()}) {
        SegmentMetadataPB legacy;
        *legacy.mutable_deprecated_sort_key_min() = make_int_pair_tuple(std::nullopt, 0);
        *legacy.mutable_deprecated_sort_key_max() = make_int_pair_tuple(max_leading, 100);
        legacy.set_num_rows(100);
        const auto encoded = with_encoded_bounds(legacy);
        DatumVariant min;
        DatumVariant max;
        ASSERT_OK(decode_segment_leading_sort_key_bounds(encoded, &min, &max));
        ASSERT_TRUE(min.value().is_null());

        auto predicates = leading_column_predicates();
        predicates.emplace_back("c0 IS NULL", new_column_null_predicate(get_type_info(TYPE_INT), 0, true));
        predicates.emplace_back("c0 IS NOT NULL", new_column_null_predicate(get_type_info(TYPE_INT), 0, false));
        for (const auto& [name, pred] : predicates) {
            PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
            const bool legacy_may_contain = SegmentMetadataFilter::may_contain(legacy, pred_tree, *nullable_schema);
            EXPECT_EQ(legacy_may_contain, SegmentMetadataFilter::may_contain(encoded, pred_tree, *nullable_schema))
                    << name << " on " << legacy.ShortDebugString();
            pruned += legacy_may_contain ? 0 : 1;
        }
    }
    EXPECT_GT(pruned, 0) << "no predicate pruned, so the comparison above proves nothing";
}

// Encoded bounds whose header or leading value does not decode never prune: the segment may contain anything. The
// same bounds intact do prune. Like the deprecated pair, only the leading column is read, so these damage the bytes
// up to the end of the leading value.
TEST_F(SegmentMetadataFilterTest, CorruptEncodedBoundsMayContain) {
    const auto encoded = with_encoded_bounds(create_segment_meta({10, 0}, {100, 100}));
    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "5"));
    PredicateTree pred_tree = create_pred_tree_with_column_pred(pred.get());
    ASSERT_FALSE(SegmentMetadataFilter::may_contain(encoded, pred_tree, *_tablet_schema));

    // Two INT columns: version (byte 0), column count (bytes 1-2), column types (bytes 3-4), then the leading
    // value's marker (byte 5) and payload (bytes 6-9).
    const std::string& valid = encoded.sort_key_min_encoded();
    ASSERT_EQ(15u, valid.size());
    auto with_byte = [&](size_t pos, char value) {
        std::string damaged = valid;
        damaged[pos] = value;
        return damaged;
    };
    auto damage_one_side = [&](const std::string& corrupt, bool corrupt_min) {
        SegmentMetadataPB meta(encoded);
        if (corrupt_min) {
            meta.set_sort_key_min_encoded(corrupt);
        } else {
            meta.set_sort_key_max_encoded(corrupt);
        }
        return meta;
    };

    // A format version this binary does not know means the segment has no bounds at all.
    for (const std::string& corrupt : {std::string("garbage"), std::string(), with_byte(0, '\x02')}) {
        for (const bool corrupt_min : {true, false}) {
            const SegmentMetadataPB meta = damage_one_side(corrupt, corrupt_min);
            EXPECT_FALSE(has_segment_sort_key_bounds(meta))
                    << (corrupt_min ? "min " : "max ") << meta.ShortDebugString();
            EXPECT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema))
                    << (corrupt_min ? "min " : "max ") << meta.ShortDebugString();
        }
    }

    // A damaged header in format version 1, or a damaged leading value, leaves the bounds present but unreadable.
    for (const std::string& corrupt :
         {valid.substr(0, 4), with_byte(3, '\x7f'), with_byte(5, '\x02'), valid.substr(0, 8)}) {
        for (const bool corrupt_min : {true, false}) {
            const SegmentMetadataPB meta = damage_one_side(corrupt, corrupt_min);
            EXPECT_TRUE(has_segment_sort_key_bounds(meta))
                    << (corrupt_min ? "min " : "max ") << meta.ShortDebugString();
            DatumVariant min;
            DatumVariant max;
            EXPECT_TRUE(decode_segment_leading_sort_key_bounds(meta, &min, &max).is_corruption())
                    << (corrupt_min ? "min " : "max ") << meta.ShortDebugString();
            EXPECT_TRUE(SegmentMetadataFilter::may_contain(meta, pred_tree, *_tablet_schema))
                    << (corrupt_min ? "min " : "max ") << meta.ShortDebugString();
        }
    }
}

} // namespace starrocks::lake
