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

#include "storage/delta_writer.h"

#include <gtest/gtest.h>

namespace starrocks {

// Column-mode partial update rewrites the partial schema's sort key to all primary key columns, so
// the memtable bounds that artificial ordering. Missing-key upserts are later materialised under the
// table's ORIGINAL ordering, and the encoded size is order-sensitive, so DeltaWriter also bounds the
// original ordering -- which it can only do when every sort key column is addressable in the partial
// schema.
TEST(DeltaWriterTest, test_map_sort_key_to_partial_schema) {
    // Sort key fully present: mapped to positions within referenced_column_ids, preserving the
    // table's original sort key order rather than the referenced-column order.
    EXPECT_EQ(std::vector<ColumnId>({2, 0}), DeltaWriter::map_sort_key_to_partial_schema({7, 3}, {3, 5, 7}));
    EXPECT_EQ(std::vector<ColumnId>({0, 1, 2}), DeltaWriter::map_sort_key_to_partial_schema({0, 1, 2}, {0, 1, 2}));
    EXPECT_EQ(std::vector<ColumnId>({1}), DeltaWriter::map_sort_key_to_partial_schema({4}, {2, 4, 6}));

    // Any absent sort key column makes the mapping non-total, and the caller must then skip the
    // check rather than address a column it does not have. A delete-only column-mode upsert is
    // allowed to omit sort key columns, and materialises nothing.
    EXPECT_TRUE(DeltaWriter::map_sort_key_to_partial_schema({3, 9}, {3, 5, 7}).empty());
    EXPECT_TRUE(DeltaWriter::map_sort_key_to_partial_schema({9}, {3, 5, 7}).empty());
    EXPECT_TRUE(DeltaWriter::map_sort_key_to_partial_schema({0}, {}).empty());

    // An empty sort key maps to an empty result, which the caller also treats as "nothing to check".
    EXPECT_TRUE(DeltaWriter::map_sort_key_to_partial_schema({}, {0, 1}).empty());
}

TEST(DeltaWriterTest, test_partial_update_sort_key_conflict_check) {
    {
        // case-1. Row mode
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::ROW_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::ROW_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::ROW_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }
    {
        // case-2. Column update mode
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {1, 2};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }

    {
        // case-3. Column update mode with upsert
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {1, 2};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {0, 1};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }

    {
        // case-4. auto mode & unknow mode
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::AUTO_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::UNKNOWN_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::AUTO_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::UNKNOWN_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::AUTO_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::UNKNOWN_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }
}

} // namespace starrocks