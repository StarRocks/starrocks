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

#include <gtest/gtest.h>

#include <vector>

#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/segment_pk_iterator.h"

namespace starrocks::lake {

namespace {

SegmentPKChunkRef make_ref(uint32_t physical_rowid_offset, std::vector<uint8_t> owned) {
    SegmentPKChunkRef ref;
    ref.physical_rowid_offset = physical_rowid_offset;
    ref.owned.assign(owned.begin(), owned.end());
    return ref;
}

} // namespace

// The rowids handed to the index must be the row's position in the SOURCE SEGMENT, not its position
// among the survivors. Getting this wrong is silent: the upsert succeeds and the index points at a
// different row of the same segment.
TEST(LakePrimaryIndexSelectionTest, test_owned_rowids_are_absolute_source_positions) {
    auto ref = make_ref(/*physical_rowid_offset=*/100, {0, 1, 0, 1, 1});
    EXPECT_EQ((std::vector<uint32_t>{101, 103, 104}), owned_rowids_of(ref));
}

// The offset is the chunk's own base, so a chunk that starts at 0 still yields plain indexes.
TEST(LakePrimaryIndexSelectionTest, test_owned_rowids_without_an_offset) {
    auto ref = make_ref(/*physical_rowid_offset=*/0, {1, 0, 1});
    EXPECT_EQ((std::vector<uint32_t>{0, 2}), owned_rowids_of(ref));
}

// A sibling that owns nothing in this chunk must produce no rowids at all, so the caller can skip
// the upsert rather than issue an empty one.
TEST(LakePrimaryIndexSelectionTest, test_owned_rowids_when_nothing_is_owned) {
    EXPECT_TRUE(owned_rowids_of(make_ref(7, {0, 0, 0})).empty());
}

} // namespace starrocks::lake
