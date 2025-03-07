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