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
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "fs/fs_memory.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/segment_file_info.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

// Matches the default of config::segment_sort_key_sample_row_interval
// (declared in be/src/common/config.h). Kept in sync manually; if the config
// default changes, update this test constant too.
static constexpr int64_t kInterval = 65536;

// Build a DUP_KEYS-style tablet schema with a single INT sort-key column and
// one INT value column. INT (4 bytes) is sufficient to exercise the sampler;
// the sampler behavior is independent of sort-key data type.
static std::shared_ptr<TabletSchema> make_int_key_schema() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
    cols.push_back(create_int_value_pb(/*id=*/2));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

// Fixture that spins up an in-memory writable file and builds SegmentWriters.
class SegmentWriterSortKeySamplingTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
        _schema = make_int_key_schema();
    }

    std::unique_ptr<SegmentWriter> new_writer(uint32_t seg_id, const SegmentWriterOptions& opts = {}) {
        std::string filename = strings::Substitute("$0/seg_$1.dat", kDir, seg_id);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        return std::make_unique<SegmentWriter>(std::move(wfile), seg_id, _schema, opts);
    }

    // Create a chunk with `n` rows where column 0 (key) is a monotonic int32
    // starting at `start_key`, and column 1 (value) is a constant 0.
    ChunkUniquePtr make_increasing_chunk(int64_t n, int32_t start_key) {
        auto s = ChunkHelper::convert_schema(_schema);
        auto chunk = ChunkHelper::new_chunk(s, n);
        auto cols = chunk->mutable_columns();
        for (int64_t i = 0; i < n; ++i) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(start_key + i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(0)));
        }
        return chunk;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    std::shared_ptr<TabletSchema> _schema;
    const std::string kDir = "/sampling_test";
};

// ---------------------------------------------------------------------------
// Row count sweep: for each N, verify sample count matches (N-1)/interval and
// the invariant samples.size() * interval < num_rows (strict) holds.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, row_count_sweep_produces_expected_sample_counts) {
    const int64_t row_interval = kInterval;
    // Mix of edge cases: below interval, at interval, just over, 2*interval
    // boundaries. Use small row counts relative to kInterval for test speed --
    // the sampler's behavior is identical at any scale.
    std::vector<int64_t> row_counts = {1,
                                       row_interval - 1,
                                       row_interval,
                                       row_interval + 1,
                                       2 * row_interval - 1,
                                       2 * row_interval,
                                       2 * row_interval + 1};
    for (int64_t num_rows : row_counts) {
        auto w = new_writer(/*seg_id=*/static_cast<uint32_t>(num_rows));
        ASSERT_OK(w->init(true));
        auto chunk = make_increasing_chunk(num_rows, /*start_key=*/0);
        ASSERT_OK(w->append_chunk(*chunk));

        const auto& samples = w->get_sort_key_samples();
        // Expected: samples fire whenever _num_rows_written (pre-increment)
        // equals k*row_interval, so the largest k with
        // k*row_interval <= num_rows - 1 wins.
        const int64_t expected = (num_rows >= 1) ? ((num_rows - 1) / row_interval) : 0;
        EXPECT_EQ(static_cast<size_t>(expected), samples.size())
                << "num_rows=" << num_rows << " row_interval=" << row_interval;
        if (!samples.empty()) {
            EXPECT_LT(static_cast<int64_t>(samples.size()) * row_interval, num_rows);
        }
        EXPECT_EQ(row_interval, w->get_sort_key_sample_row_interval());
    }
}

// ---------------------------------------------------------------------------
// Samples are non-decreasing on ordered input.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, samples_are_non_decreasing_on_ordered_input) {
    const int64_t num_rows = 3 * kInterval + 123;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));
    auto chunk = make_increasing_chunk(num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_FALSE(samples.empty());
    for (size_t i = 1; i < samples.size(); ++i) {
        EXPECT_LE(samples[i - 1].compare(samples[i]), 0);
    }
}

// ---------------------------------------------------------------------------
// Off-by-one: feed interval+1 rows where last row's key equals max_key.
// Expected: samples.size() == 1 and samples[0] == max_key.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, off_by_one_trailing_max_sample) {
    const int64_t num_rows = kInterval + 1;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));
    auto chunk = make_increasing_chunk(num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_EQ(1U, samples.size());
    EXPECT_EQ(0, samples[0].compare(w->get_sort_key_max()));
}

// ---------------------------------------------------------------------------
// Leading-min duplicates: feed `interval + 1` rows of key 0 followed by
// further monotonic rows. samples[0] should equal min (0).
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, leading_min_duplicate_samples) {
    const int64_t leading = kInterval + 1;
    const int64_t trailing = kInterval + 5;
    const int64_t num_rows = leading + trailing;

    auto w = new_writer(0);
    ASSERT_OK(w->init(true));

    auto s = ChunkHelper::convert_schema(_schema);
    auto chunk = ChunkHelper::new_chunk(s, num_rows);
    auto cols = chunk->mutable_columns();
    for (int64_t i = 0; i < leading; ++i) {
        cols[0]->append_datum(Datum(static_cast<int32_t>(0)));
        cols[1]->append_datum(Datum(static_cast<int32_t>(0)));
    }
    for (int64_t i = 0; i < trailing; ++i) {
        cols[0]->append_datum(Datum(static_cast<int32_t>(1 + i)));
        cols[1]->append_datum(Datum(static_cast<int32_t>(0)));
    }
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_FALSE(samples.empty());
    EXPECT_EQ(0, samples[0].compare(w->get_sort_key_min()));
}

// ---------------------------------------------------------------------------
// All-identical-key segment: samples all equal the single key; min == max ==
// every sample.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, all_identical_key) {
    const int64_t num_rows = 2 * kInterval + 7;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));

    auto s = ChunkHelper::convert_schema(_schema);
    auto chunk = ChunkHelper::new_chunk(s, num_rows);
    auto cols = chunk->mutable_columns();
    for (int64_t i = 0; i < num_rows; ++i) {
        cols[0]->append_datum(Datum(static_cast<int32_t>(42)));
        cols[1]->append_datum(Datum(static_cast<int32_t>(0)));
    }
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_FALSE(samples.empty());
    for (const auto& sample : samples) {
        EXPECT_EQ(0, sample.compare(w->get_sort_key_min()));
        EXPECT_EQ(0, sample.compare(w->get_sort_key_max()));
    }
}

// ---------------------------------------------------------------------------
// Vertical writer re-entry: init() with is_key=true produces samples, then a
// subsequent init({non_key_cols}, is_key=false) MUST NOT wipe the sampler
// state. This is the vertical-compaction-compat contract.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, vertical_writer_reinit_preserves_sampler_state) {
    const int64_t num_rows = kInterval + 5;
    auto w = new_writer(0);

    // First init: column 0 only (the key column). is_key=true arms the sampler.
    std::vector<uint32_t> key_cols{0};
    ASSERT_OK(w->init(key_cols, true));

    // Append key-column-only chunks.
    auto key_schema_view = ChunkHelper::convert_schema(_schema, key_cols);
    auto key_chunk = ChunkHelper::new_chunk(key_schema_view, num_rows);
    auto* key_col = key_chunk->mutable_columns()[0].get();
    for (int64_t i = 0; i < num_rows; ++i) {
        key_col->append_datum(Datum(static_cast<int32_t>(i)));
    }
    ASSERT_OK(w->append_chunk(*key_chunk));

    const auto samples_after_key = w->get_sort_key_samples();
    const int64_t row_interval_after_key = w->get_sort_key_sample_row_interval();
    ASSERT_EQ(1U, samples_after_key.size());
    ASSERT_EQ(kInterval, row_interval_after_key);

    // Vertical writer calls finalize_columns between column-group passes; this
    // clears _column_writers/_column_indexes so the next init() does not
    // assert. We mirror that sequence here.
    uint64_t index_size = 0;
    ASSERT_OK(w->finalize_columns(&index_size));

    // Second init: column 1 only (value column), is_key=false.
    std::vector<uint32_t> non_key_cols{1};
    ASSERT_OK(w->init(non_key_cols, false));

    // Sampler state must survive re-init.
    EXPECT_EQ(row_interval_after_key, w->get_sort_key_sample_row_interval());
    ASSERT_EQ(samples_after_key.size(), w->get_sort_key_samples().size());
    for (size_t i = 0; i < samples_after_key.size(); ++i) {
        EXPECT_EQ(0, samples_after_key[i].compare(w->get_sort_key_samples()[i]));
    }
}

// ---------------------------------------------------------------------------
// write_sort_key_fields_to(SegmentFileInfo&) transfers min, max, samples, and
// interval into a SegmentFileInfo. Preserves the carrier invariant
// (samples.empty() <=> 0).
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, write_sort_key_fields_to_transfers_ownership) {
    const int64_t num_rows = 2 * kInterval + 3;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));
    auto chunk = make_increasing_chunk(num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));

    SegmentFileInfo file_info;
    w->write_sort_key_fields_to(file_info);
    EXPECT_EQ(2U, file_info.sort_key_samples.size());
    EXPECT_EQ(kInterval, file_info.sort_key_sample_row_interval);
    EXPECT_FALSE(file_info.sort_key_min.empty());
    EXPECT_FALSE(file_info.sort_key_max.empty());
    // After fill, writer's samples should be moved out.
    EXPECT_TRUE(w->get_sort_key_samples().empty());
}

} // namespace starrocks
