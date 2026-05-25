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

#include <memory>
#include <vector>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/field.h"
#include "column/schema.h"
#include "common/config_primary_key_fwd.h"
#include "storage/chunk_iterator.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/olap_common.h"
#include "storage/primary_key_encoding_types.h"

namespace starrocks::lake {

// Mock ChunkIterator that emits a programmable script of (chunk_size, first_rowid) pairs.
// Each emit fills the chunk with `chunk_size` default rows and the rowid buffer with the
// values from the script (defaults to a contiguous [first_rowid, first_rowid+chunk_size)
// range; pass custom_rowids to inject sparse/non-contiguous patterns).
class MockSegmentChunkIterator : public ChunkIterator {
public:
    struct EmitScript {
        size_t chunk_size = 0;
        uint32_t first_rowid = 0;
        std::vector<uint32_t> custom_rowids; // size must equal chunk_size when non-empty
    };

    MockSegmentChunkIterator(Schema schema, std::vector<EmitScript> script)
            : ChunkIterator(std::move(schema)), _script(std::move(script)) {}

    void close() override {}

protected:
    Status do_get_next(Chunk* /*chunk*/) override {
        return Status::NotSupported("MockSegmentChunkIterator requires the (chunk, rowids) form");
    }

    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowids) override {
        if (_emit_idx >= _script.size()) {
            return Status::EndOfFile("done");
        }
        const auto& step = _script[_emit_idx++];
        const auto& cols = chunk->columns();
        if (cols.empty()) {
            return Status::InternalError("chunk has no columns");
        }
        if (step.chunk_size > 0) {
            // Chunk::columns() returns Cow Column::Ptr (const-pointing); for test
            // purposes we just need to grow the column to the right row count.
            auto* col = const_cast<Column*>(cols[0].get());
            col->append_default(step.chunk_size);
        }
        rowids->clear();
        if (!step.custom_rowids.empty()) {
            *rowids = step.custom_rowids;
        } else {
            for (size_t i = 0; i < step.chunk_size; ++i) {
                rowids->push_back(step.first_rowid + static_cast<uint32_t>(i));
            }
        }
        return Status::OK();
    }

private:
    std::vector<EmitScript> _script;
    size_t _emit_idx = 0;
};

class SegmentPKIteratorTest : public ::testing::Test {
protected:
    Schema make_pkey_schema() {
        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "k", TYPE_INT, /*nullable=*/false));
        return Schema(std::move(fields), PRIMARY_KEYS, std::vector<ColumnId>{0});
    }

    std::shared_ptr<MockSegmentChunkIterator> make_iter(std::vector<MockSegmentChunkIterator::EmitScript> script) {
        return std::make_shared<MockSegmentChunkIterator>(make_pkey_schema(), std::move(script));
    }
};

TEST_F(SegmentPKIteratorTest, NonSharedSegment_PhysicalBaseIsZero) {
    auto chunk_iter = make_iter({{/*chunk_size*/ 4, /*first_rowid*/ 0, {}}});
    SegmentPKIterator iter;
    ASSERT_TRUE(iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/false,
                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                          /*defer_data_load=*/false)
                        .ok());
    ASSERT_FALSE(iter.done());
    auto ref = iter.current();
    EXPECT_EQ(0u, ref.physical_rowid_offset);
    EXPECT_EQ(0u, ref.logical_rowid_offset);
    EXPECT_EQ(4u, ref.chunk->num_rows());
}

TEST_F(SegmentPKIteratorTest, SharedSegmentRangeFilter_BaseEqualsRangeStart) {
    // Iterator starts at physical row 100 (range filter result on a sorted PK segment).
    // First chunk's physical_rowid_offset (= 100) is the segment-wide range start;
    // its logical_rowid_offset is 0 (no rows emitted before it).
    auto chunk_iter = make_iter({{/*chunk_size*/ 4, /*first_rowid*/ 100, {}}});
    SegmentPKIterator iter;
    ASSERT_TRUE(iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/false,
                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                          /*defer_data_load=*/false)
                        .ok());
    ASSERT_FALSE(iter.done());
    auto ref = iter.current();
    EXPECT_EQ(100u, ref.physical_rowid_offset);
    EXPECT_EQ(0u, ref.logical_rowid_offset);
    EXPECT_EQ(4u, ref.chunk->num_rows());
}

TEST_F(SegmentPKIteratorTest, MultipleInnerGetNextCalls_BaseSetOnce) {
    // Three emits in one _load() call accumulate into a single logical chunk; the
    // base must be set from the first emit and validated across subsequent ones.
    auto chunk_iter = make_iter({
            {/*chunk_size*/ 10, /*first_rowid*/ 50, {}}, // physical 50..59
            {/*chunk_size*/ 10, /*first_rowid*/ 60, {}}, // physical 60..69 (must match expected_next)
            {/*chunk_size*/ 10, /*first_rowid*/ 70, {}}, // physical 70..79
    });
    SegmentPKIterator iter;
    ASSERT_TRUE(iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/false,
                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                          /*defer_data_load=*/false)
                        .ok());
    ASSERT_FALSE(iter.done());
    auto ref = iter.current();
    EXPECT_EQ(50u, ref.physical_rowid_offset);
    EXPECT_EQ(0u, ref.logical_rowid_offset);
    EXPECT_EQ(30u, ref.chunk->num_rows());
}

TEST_F(SegmentPKIteratorTest, SingleEmitNonContiguousRowids_ReturnsInternalError) {
    // A single get_next() returning {5, 7, 9} (sparse) must be rejected -- a future
    // iterator with delete bitmap or row predicates could produce this and the
    // downstream `base + i` arithmetic would silently compute wrong rowids.
    MockSegmentChunkIterator::EmitScript step;
    step.chunk_size = 3;
    step.custom_rowids = {5, 7, 9};
    auto chunk_iter = make_iter({step});
    SegmentPKIterator iter;
    auto st =
            iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/false, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                      /*defer_data_load=*/false);
    ASSERT_FALSE(st.ok()) << "expected an error for sparse intra-emit rowids";
    EXPECT_TRUE(st.is_internal_error()) << st;
}

TEST_F(SegmentPKIteratorTest, CrossEmitNonContiguousRowids_ReturnsInternalError) {
    // First emit: {5, 6, 7}, second emit: {10, 11} -- gap means the iterator
    // skipped rows, breaking the contiguity invariant across emits.
    MockSegmentChunkIterator::EmitScript step1;
    step1.chunk_size = 3;
    step1.custom_rowids = {5, 6, 7};
    MockSegmentChunkIterator::EmitScript step2;
    step2.chunk_size = 2;
    step2.custom_rowids = {10, 11};
    auto chunk_iter = make_iter({step1, step2});
    SegmentPKIterator iter;
    auto st =
            iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/false, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                      /*defer_data_load=*/false);
    ASSERT_FALSE(st.ok()) << "expected an error for cross-emit non-contiguous rowids";
    EXPECT_TRUE(st.is_internal_error()) << st;
}

TEST_F(SegmentPKIteratorTest, EmptyInnerEmit_PreservesBaseState) {
    // Iterator emits an empty buffer between non-empty ones. Empty must not
    // reset the base; subsequent emits must still match the running counter.
    MockSegmentChunkIterator::EmitScript step1{/*chunk_size*/ 2, /*first_rowid*/ 100, {}};
    MockSegmentChunkIterator::EmitScript step2; // empty
    MockSegmentChunkIterator::EmitScript step3{/*chunk_size*/ 3, /*first_rowid*/ 102, {}};
    auto chunk_iter = make_iter({step1, step2, step3});
    SegmentPKIterator iter;
    ASSERT_TRUE(iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/false,
                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                          /*defer_data_load=*/false)
                        .ok());
    ASSERT_FALSE(iter.done());
    auto ref = iter.current();
    EXPECT_EQ(100u, ref.physical_rowid_offset);
    EXPECT_EQ(0u, ref.logical_rowid_offset);
    EXPECT_EQ(5u, ref.chunk->num_rows()); // 2 + 0 + 3
}

TEST_F(SegmentPKIteratorTest, LazyLoadAcrossNextCalls_BaseUnchangedAndChunkBaseAdvances) {
    // lazy_load=true with tiny thresholds: each _load() emits exactly one
    // batch, so iter.next() triggers a fresh _load() that must continue from
    // where the previous one left off without resetting the segment base.
    // Each successive current() must show physical_rowid_offset advancing in
    // lockstep with logical_rowid_offset, with the difference (= segment-wide
    // base) staying constant.
    auto chunk_iter = make_iter({
            {/*chunk_size*/ 2, /*first_rowid*/ 200, {}}, // physical 200..201
            {/*chunk_size*/ 2, /*first_rowid*/ 202, {}}, // physical 202..203
            {/*chunk_size*/ 2, /*first_rowid*/ 204, {}}, // physical 204..205
    });
    const auto saved_min_rows = config::pk_index_parallel_execution_min_rows;
    const auto saved_threshold = config::pk_column_lazy_load_threshold_bytes;
    config::pk_index_parallel_execution_min_rows = 1;
    config::pk_column_lazy_load_threshold_bytes = 1;
    DeferOp restore_config([&] {
        config::pk_index_parallel_execution_min_rows = saved_min_rows;
        config::pk_column_lazy_load_threshold_bytes = saved_threshold;
    });
    SegmentPKIterator iter;
    ASSERT_TRUE(iter.init(chunk_iter, make_pkey_schema(), /*lazy_load=*/true,
                          PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                          /*defer_data_load=*/false)
                        .ok());

    const std::vector<uint32_t> expected_physical_offsets = {200, 202, 204};
    const std::vector<size_t> expected_logical_offsets = {0, 2, 4};
    size_t emit_index = 0;
    while (!iter.done()) {
        ASSERT_LT(emit_index, expected_physical_offsets.size());
        auto ref = iter.current();
        EXPECT_EQ(expected_physical_offsets[emit_index], ref.physical_rowid_offset);
        EXPECT_EQ(expected_logical_offsets[emit_index], ref.logical_rowid_offset);
        // Difference is the segment-wide base, which must NOT change across
        // _load() calls. 200 here = the iterator's range_start.
        EXPECT_EQ(200u, ref.physical_rowid_offset - ref.logical_rowid_offset);
        EXPECT_EQ(2u, ref.chunk->num_rows());
        ++emit_index;
        iter.next();
    }
    EXPECT_EQ(expected_physical_offsets.size(), emit_index);
}

} // namespace starrocks::lake
