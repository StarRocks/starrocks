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

#include "storage/lake/cross_publish_context.h"

#include <gtest/gtest.h>

#include <array>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "storage/chunk_helper.h"
#include "storage/datum_variant.h"
#include "storage/tablet_range.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

namespace {

TuplePB make_int_tuple_pb(int32_t value) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum(value)));
    TuplePB tuple_pb;
    tuple.to_proto(&tuple_pb);
    return tuple_pb;
}

// c0 (key), c1 (key), c2 (value); V2 encoding, which is what range distribution requires.
TabletSchemaPB make_pk_schema_pb() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    const std::array<const char*, 3> names = {"c0", "c1", "c2"};
    for (int i = 0; i < 3; ++i) {
        auto* column = schema_pb.add_column();
        column->set_name(names[i]);
        column->set_type("INT");
        column->set_is_key(i < 2);
        column->set_is_nullable(false);
    }
    return schema_pb;
}

// [ (2,0), (4,0) )
void set_range(TabletMetadataPB* metadata) {
    auto* range = metadata->mutable_range();
    range->mutable_lower_bound()->CopyFrom(make_int_tuple_pb(2));
    *range->mutable_lower_bound()->add_values() = make_int_tuple_pb(0).values(0);
    range->set_lower_bound_included(true);
    range->mutable_upper_bound()->CopyFrom(make_int_tuple_pb(4));
    *range->mutable_upper_bound()->add_values() = make_int_tuple_pb(0).values(0);
    range->set_upper_bound_included(false);
}

RowsetMetadataPB make_rowset(bool shared) {
    RowsetMetadataPB rowset;
    auto* segment_meta = rowset.add_segment_metas();
    segment_meta->set_filename("seg.dat");
    segment_meta->set_num_rows(4);
    segment_meta->set_shared(shared);
    return rowset;
}

ChunkUniquePtr make_pk_chunk(const TabletSchemaCSPtr& tablet_schema,
                             const std::vector<std::pair<int32_t, int32_t>>& keys) {
    std::vector<ColumnId> pk_columns = {0, 1};
    auto schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    auto chunk = ChunkFactory::new_chunk(schema, keys.size());
    for (const auto& [c0, c1] : keys) {
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(c0));
        chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(c1));
    }
    return chunk;
}

} // namespace

// An ordinary publish must pay nothing: a rowset this tablet's own sink produced carries no shared
// segment, and every row in it is in range by construction.
TEST(CrossPublishRowSelectorTest, test_no_selector_without_a_shared_segment) {
    auto schema_pb = make_pk_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);
    TabletMetadataPB metadata;
    set_range(&metadata);

    ASSIGN_OR_ABORT(auto selector,
                    CrossPublishRowSelector::create_if_needed(metadata, tablet_schema, make_rowset(false)));
    EXPECT_EQ(nullptr, selector);
}

// A tablet with no range is not range-distributed, so there is nothing to select against.
TEST(CrossPublishRowSelectorTest, test_no_selector_without_a_range) {
    auto schema_pb = make_pk_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);
    TabletMetadataPB metadata; // no range

    ASSIGN_OR_ABORT(auto selector,
                    CrossPublishRowSelector::create_if_needed(metadata, tablet_schema, make_rowset(true)));
    EXPECT_EQ(nullptr, selector);
}

// Only a primary-key tablet routes by a key space separate from its physical order.
TEST(CrossPublishRowSelectorTest, test_no_selector_for_a_non_primary_key_tablet) {
    auto schema_pb = make_pk_schema_pb();
    schema_pb.set_keys_type(DUP_KEYS);
    auto tablet_schema = TabletSchema::create(schema_pb);
    TabletMetadataPB metadata;
    set_range(&metadata);

    ASSIGN_OR_ABORT(auto selector,
                    CrossPublishRowSelector::create_if_needed(metadata, tablet_schema, make_rowset(true)));
    EXPECT_EQ(nullptr, selector);
}

// The verdict itself, and the property the whole stage exists for: the selection is a MASK, so a
// row's index in the chunk still names its physical rowid in the source segment. Compacting the
// chunk instead would renumber rows 2 and 3 to 0 and 1 and every delvec entry derived from them
// would name the wrong row.
TEST(CrossPublishRowSelectorTest, test_selection_marks_owned_rows_without_moving_them) {
    auto schema_pb = make_pk_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);
    TabletMetadataPB metadata;
    set_range(&metadata);

    ASSIGN_OR_ABORT(auto selector,
                    CrossPublishRowSelector::create_if_needed(metadata, tablet_schema, make_rowset(true)));
    ASSERT_NE(nullptr, selector);

    // (1,9) below the range; (2,0) the inclusive lower bound; (3,5) inside; (4,0) the exclusive
    // upper bound, so a sibling's.
    auto chunk = make_pk_chunk(tablet_schema, {{1, 9}, {2, 0}, {3, 5}, {4, 0}});
    ASSIGN_OR_ABORT(auto selection, selector->select(*chunk));
    ASSERT_EQ(4, selection.size()) << "the mask must be as long as the chunk, not as its owned rows";
    EXPECT_EQ(0, selection[0]);
    EXPECT_EQ(1, selection[1]);
    EXPECT_EQ(1, selection[2]);
    EXPECT_EQ(0, selection[3]);
    EXPECT_EQ(4, chunk->num_rows()) << "the chunk itself must be untouched";
}

TEST(CrossPublishRowSelectorTest, test_selection_with_separate_sort_key_uses_primary_key_range) {
    auto schema_pb = make_pk_schema_pb();
    schema_pb.add_sort_key_idxes(2); // ORDER BY c2 differs from PK (c0, c1).
    auto tablet_schema = TabletSchema::create(schema_pb);
    ASSERT_TRUE(tablet_schema->has_separate_sort_key());
    TabletMetadataPB metadata;
    set_range(&metadata);

    ASSIGN_OR_ABORT(auto selector,
                    CrossPublishRowSelector::create_if_needed(metadata, tablet_schema, make_rowset(true)));
    ASSERT_NE(nullptr, selector);

    auto chunk = make_pk_chunk(tablet_schema, {{1, 9}, {2, 0}, {3, 5}, {4, 0}});
    ASSIGN_OR_ABORT(auto selection, selector->select(*chunk));
    ASSERT_EQ(4, selection.size());
    EXPECT_EQ(0, selection[0]);
    EXPECT_EQ(1, selection[1]);
    EXPECT_EQ(1, selection[2]);
    EXPECT_EQ(0, selection[3]);
}

// One selector serves every segment and every chunk of a rowset, so consecutive calls must be
// independent of each other. They are by construction now -- select() is const and allocates its
// encode column locally -- and this pins that: a stale buffer would leak the first chunk's keys into
// the second chunk's comparison.
TEST(CrossPublishRowSelectorTest, test_selection_is_not_polluted_by_the_previous_chunk) {
    auto schema_pb = make_pk_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);
    TabletMetadataPB metadata;
    set_range(&metadata);

    ASSIGN_OR_ABORT(auto selector,
                    CrossPublishRowSelector::create_if_needed(metadata, tablet_schema, make_rowset(true)));
    ASSERT_NE(nullptr, selector);

    auto first = make_pk_chunk(tablet_schema, {{1, 9}, {2, 0}, {3, 5}, {4, 0}});
    ASSIGN_OR_ABORT(auto ignored, selector->select(*first));

    auto second = make_pk_chunk(tablet_schema, {{0, 0}, {2, 5}});
    ASSIGN_OR_ABORT(auto selection, selector->select(*second));
    ASSERT_EQ(2, selection.size());
    EXPECT_EQ(0, selection[0]);
    EXPECT_EQ(1, selection[1]);

    auto empty = make_pk_chunk(tablet_schema, {});
    ASSIGN_OR_ABORT(auto empty_selection, selector->select(*empty));
    EXPECT_TRUE(empty_selection.empty());
}

} // namespace starrocks::lake
