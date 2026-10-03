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

#include "exec/pipeline/hashjoin/hash_join_build_chunk_slicer.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_view/column_view.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "testutil/column_test_helper.h"

namespace starrocks::pipeline {
namespace {

// A build chunk as a hash table holds it: a dummy row first, then one row per value, with a VARCHAR column at slot 0
// and an INT column at slot 1. The VARCHAR column gets 64-bit offsets, which is how it looks past 4GB.
ChunkPtr build_chunk_with_large_offsets(const std::vector<std::string>& values, bool nullable) {
    MutableColumnPtr strings = BinaryColumn::create();
    auto ints = Int32Column::create();
    strings->append_default();
    ints->append_default();
    for (size_t i = 0; i < values.size(); i++) {
        strings->append_datum(Datum(Slice(values[i])));
        ints->append(static_cast<int32_t>(i));
    }
    if (nullable) {
        strings = NullableColumn::create(std::move(strings), NullColumn::create(values.size() + 1, 0));
    }
    ColumnTestHelper::force_large_offsets(strings.get());
    Chunk::SlotHashMap slot_map{{0, 0}, {1, 1}};
    return std::make_shared<Chunk>(Columns{std::move(strings), std::move(ints)}, slot_map);
}

// Returns every slice until EndOfFile.
std::vector<ChunkPtr> slice_all(HashJoinBuildChunkSlicer* slicer, size_t chunk_size) {
    std::vector<ChunkPtr> slices;
    while (true) {
        auto slice = slicer->next(chunk_size);
        if (slice.status().is_end_of_file()) {
            return slices;
        }
        EXPECT_TRUE(slice.ok()) << slice.status();
        if (!slice.ok()) {
            return slices;
        }
        slices.push_back(std::move(slice.value()));
    }
}

} // namespace

// The spill used to upgrade a build chunk over 4GB to LargeBinaryColumn and downgrade each slice back. Now a build
// chunk keeps its BinaryColumn, whose offsets are 64-bit past 4GB, and each slice must come out as a plain
// BinaryColumn. Also covers skipping the dummy rows and a build chunk that holds only the dummy row.
TEST(HashJoinBuildChunkSlicerTest, SliceBuildChunksWithLargeOffsets) {
    for (bool nullable : {false, true}) {
        SCOPED_TRACE(nullable ? "nullable" : "not nullable");
        auto first = build_chunk_with_large_offsets({"a0", "a1", "a2", "a3", "a4"}, nullable);
        auto only_dummy = build_chunk_with_large_offsets({}, nullable);
        auto last = build_chunk_with_large_offsets({"c0", "c1", "c2"}, nullable);

        HashJoinBuildChunkSlicer slicer;
        ASSERT_OK(slicer.reset({first, only_dummy, last}));
        const auto slices = slice_all(&slicer, 2);

        std::vector<std::string> strings;
        std::vector<int32_t> ints;
        for (const auto& slice : slices) {
            ASSERT_LE(slice->num_rows(), 2u);
            const Column* data = ColumnHelper::get_data_column(slice->get_column_by_slot_id(0).get());
            ASSERT_TRUE(data->is_binary());
            ASSERT_FALSE(data->is_large_binary());
            for (size_t i = 0; i < slice->num_rows(); i++) {
                strings.push_back(slice->get_column_by_slot_id(0)->get(i).get_slice().to_string());
                ints.push_back(slice->get_column_by_slot_id(1)->get(i).get_int32());
            }
        }
        EXPECT_EQ((std::vector<std::string>{"a0", "a1", "a2", "a3", "a4", "c0", "c1", "c2"}), strings);
        EXPECT_EQ((std::vector<int32_t>{0, 1, 2, 3, 4, 0, 1, 2}), ints);

        // The slicer drops its build chunks at EndOfFile, and keeps returning EndOfFile.
        EXPECT_EQ(1, first.use_count());
        EXPECT_EQ(1, last.use_count());
        EXPECT_TRUE(slicer.next(2).status().is_end_of_file());
    }
}

TEST(HashJoinBuildChunkSlicerTest, NoBuildChunks) {
    HashJoinBuildChunkSlicer slicer;
    ASSERT_OK(slicer.reset({}));
    EXPECT_TRUE(slicer.next(4).status().is_end_of_file());
}

// reset() replaced upgrade_if_overflow() with capacity_limit_reached(), which must still reject a build chunk over its
// limits. A ConstColumn reports more rows than the limit without allocating them.
TEST(HashJoinBuildChunkSlicerTest, RejectBuildChunkOverCapacity) {
    const uint64_t capacity_limit = Column::MAX_CAPACITY_LIMIT;
    auto value = BinaryColumn::create();
    value->append("x");
    Chunk::SlotHashMap slot_map{{0, 0}};
    auto build_chunk =
            std::make_shared<Chunk>(Columns{ConstColumn::create(std::move(value), capacity_limit + 1)}, slot_map);

    HashJoinBuildChunkSlicer slicer;
    auto status = slicer.reset({build_chunk});
    EXPECT_TRUE(status.is_capacity_limit_exceeded()) << status;
}

// A hash join with column_view_concat_* enabled keeps its non-key VARCHAR build columns as ColumnView, whose
// capacity_limit_reached() used to throw.
TEST(HashJoinBuildChunkSlicerTest, AcceptColumnViewBuildChunk) {
    auto src = BinaryColumn::create();
    src->append("a");
    src->append("b");
    auto view = ColumnView::create(BinaryColumn::create(), 0, -1);
    view->append_default();
    view->append(*src, 0, src->size());
    Chunk::SlotHashMap slot_map{{0, 0}};
    auto build_chunk = std::make_shared<Chunk>(Columns{std::move(view)}, slot_map);

    HashJoinBuildChunkSlicer slicer;
    ASSERT_OK(slicer.reset({build_chunk}));
}

} // namespace starrocks::pipeline
