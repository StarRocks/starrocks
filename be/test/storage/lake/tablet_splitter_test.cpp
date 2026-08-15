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

#include "storage/lake/tablet_splitter.h"

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <optional>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "common/config_lake_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "platform/store_path.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_range.h"
#include "storage/tablet_schema.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

using google::protobuf::RepeatedPtrField;
using google::protobuf::util::MessageDifferencer;

namespace {

static VariantTuple make_int_tuple(int64_t value) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_BIGINT), Datum(value)));
    return tuple;
}

// Build a SegmentSplitInfo without samples.
static SegmentSplitInfo make_seg(int64_t min_v, int64_t max_v, int64_t num_rows, int64_t data_size,
                                 uint32_t source_id = 0) {
    SegmentSplitInfo s;
    s.min_key = make_int_tuple(min_v);
    s.max_key = make_int_tuple(max_v);
    s.num_rows = num_rows;
    s.data_size = data_size;
    s.source_id = source_id;
    return s;
}

// Build a SegmentSplitInfo with sort-key samples at row interval `row_interval`
// and row count covering exactly N samples + tail, where N = sample_values.size().
// Producer invariant: sort_key_samples.size() * row_interval < num_rows.
static SegmentSplitInfo make_sampled_seg(int64_t min_v, int64_t max_v, int64_t num_rows, int64_t data_size,
                                         int64_t row_interval, const std::vector<int64_t>& sample_values,
                                         uint32_t source_id = 0) {
    SegmentSplitInfo s = make_seg(min_v, max_v, num_rows, data_size, source_id);
    s.sort_key_sample_row_interval = row_interval;
    s.sort_key_samples.reserve(sample_values.size());
    for (int64_t v : sample_values) {
        s.sort_key_samples.push_back(make_int_tuple(v));
    }
    return s;
}

// Sum of per-source row/byte stats across all split groups for a given source.
static std::pair<int64_t, int64_t> sum_source_stats(const RangeSplitResult& result, uint32_t source_id) {
    int64_t rows = 0;
    int64_t bytes = 0;
    for (const auto& group : result.range_source_stats) {
        auto it = group.find(source_id);
        if (it != group.end()) {
            rows += it->second.first;
            bytes += it->second.second;
        }
    }
    return {rows, bytes};
}

} // namespace

// -----------------------------------------------------------------------------
// Baseline: N == 0 (no samples) matches pre-sampling behavior on non-degenerate
// segments. A single segment produces exactly 1 ordered range (two boundary
// points: its min and its max). The algorithm needs at least `split_count`
// non-empty ranges to produce a boundary, so a single segment alone cannot
// be split into 2 — this mirrors the pre-sampling algorithm's behavior and
// is the reason the real RCA required >1 overlapping segment.
// Two non-overlapping segments produce 2 ordered ranges and can therefore
// be split.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, two_disjoint_segments_no_samples_split_in_half) {
    std::vector<SegmentSplitInfo> segs = {make_seg(0, 50, 100, 1000, /*source_id=*/1),
                                          make_seg(100, 200, 100, 1000, /*source_id=*/2)};
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/100,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));
    ASSERT_EQ(1, result.boundaries.size());
    ASSERT_EQ(2, result.range_num_rows.size());
    EXPECT_EQ(200, result.range_num_rows[0] + result.range_num_rows[1]);
    EXPECT_EQ(2000, result.range_data_sizes[0] + result.range_data_sizes[1]);
    auto [s1_rows, s1_bytes] = sum_source_stats(result, 1);
    auto [s2_rows, s2_bytes] = sum_source_stats(result, 2);
    EXPECT_EQ(100, s1_rows);
    EXPECT_EQ(1000, s1_bytes);
    EXPECT_EQ(100, s2_rows);
    EXPECT_EQ(1000, s2_bytes);
}

// A single segment (only 1 ordered range exists) cannot be 2-way split; the
// algorithm returns empty boundaries rather than fabricating one. Matches
// pre-sampling behavior exactly.
TEST(TabletSplitterTest, single_segment_cannot_split_without_samples) {
    std::vector<SegmentSplitInfo> segs = {make_seg(0, 100, 100, 1000, /*source_id=*/1)};
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/50,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));
    EXPECT_TRUE(result.boundaries.empty()) << "1 segment yields 1 range; cannot produce N>=2 splits without samples";
}

// -----------------------------------------------------------------------------
// With samples, the algorithm can split overlapping segments accurately.
// Two overlapping segments each [0, 100] with 10 samples each (11 sub-segments
// of 9 or 10 rows). A 2-way split should land near the median, and row total
// should be exactly preserved.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, overlapping_segments_with_samples_balanced_split) {
    // 2 segments, each 100 rows, 10 samples at rows 10, 20, ..., 90.
    std::vector<int64_t> samples;
    for (int64_t v = 10; v < 100; v += 10) samples.push_back(v);

    std::vector<SegmentSplitInfo> segs;
    segs.push_back(make_sampled_seg(0, 100, 100, 1000, /*iv=*/10, samples, /*source_id=*/1));
    segs.push_back(make_sampled_seg(0, 100, 100, 1000, /*iv=*/10, samples, /*source_id=*/2));

    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/100,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));
    ASSERT_EQ(1, result.boundaries.size());

    int64_t total_rows = 0;
    int64_t total_bytes = 0;
    for (int i = 0; i < 2; ++i) {
        total_rows += result.range_num_rows[i];
        total_bytes += result.range_data_sizes[i];
    }
    EXPECT_EQ(200, total_rows);
    EXPECT_EQ(2000, total_bytes);

    // Each side should be roughly balanced (±20 rows acceptable due to sample
    // granularity); if sampling is doing its job this should be much tighter
    // than a by-overlap-count baseline.
    EXPECT_LT(std::abs(result.range_num_rows[0] - result.range_num_rows[1]), 30);
}

// -----------------------------------------------------------------------------
// Off-by-one: a sample equals max_key (producer's off-by-one when
// num_rows == N * interval + 1). The tail sub-segment is [max, max], a
// zero-width point. The point-ownership fallback must credit the rightmost
// range whose r.max == global_max (via the last-range closed comparator).
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, zero_width_tail_sample_equals_max) {
    // Segment with num_rows = interval + 1 = 11, one sample at row 10 which
    // coincidentally equals max_key.
    auto seg = make_sampled_seg(/*min=*/0, /*max=*/50, /*num_rows=*/11, /*data_size=*/110,
                                /*iv=*/10, /*samples=*/{50}, /*source_id=*/1);
    // Include a second non-overlapping segment so ordered_ranges.size() >= 2.
    auto seg2 = make_seg(/*min=*/100, /*max=*/200, /*num_rows=*/50, /*data_size=*/500, /*source_id=*/2);

    std::vector<SegmentSplitInfo> segs = {seg, seg2};
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/30,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));

    // Conservation: all 61 rows and 610 bytes must be credited somewhere.
    auto [s1_rows, s1_bytes] = sum_source_stats(result, 1);
    auto [s2_rows, s2_bytes] = sum_source_stats(result, 2);
    EXPECT_EQ(11, s1_rows);
    EXPECT_EQ(110, s1_bytes);
    EXPECT_EQ(50, s2_rows);
    EXPECT_EQ(500, s2_bytes);
}

// -----------------------------------------------------------------------------
// Leading-min duplicate: first sample equals min_key (producer's case when the
// first `interval + 1` rows share the min value). The leading sub-segment is
// [min, min] (zero-width); point-ownership fallback should credit the leftmost
// range (whose r.min == min) — and all rows must be conserved.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, zero_width_head_sample_equals_min) {
    // Segment with 21 rows, 2 samples at rows 10 and 20.
    // If rows 0..10 are all `0`, sample[0] == 0 == min_key.
    auto seg = make_sampled_seg(/*min=*/0, /*max=*/50, /*num_rows=*/21, /*data_size=*/210,
                                /*iv=*/10, /*samples=*/{0, 25}, /*source_id=*/1);
    auto seg2 = make_seg(/*min=*/100, /*max=*/200, /*num_rows=*/50, /*data_size=*/500, /*source_id=*/2);

    std::vector<SegmentSplitInfo> segs = {seg, seg2};
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/40,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));

    auto [s1_rows, s1_bytes] = sum_source_stats(result, 1);
    auto [s2_rows, s2_bytes] = sum_source_stats(result, 2);
    EXPECT_EQ(21, s1_rows);
    EXPECT_EQ(210, s1_bytes);
    EXPECT_EQ(50, s2_rows);
    EXPECT_EQ(500, s2_bytes);
}

// -----------------------------------------------------------------------------
// Interior duplicate samples: sample[i] == sample[i+1] (heavy clustering on
// one specific key). The sub-segment [x, x] is zero-width and must be
// attributed to the range whose `[r.min, r.max)` owns the point (right range
// at an internal shared boundary), preserving row conservation.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, zero_width_interior_duplicate_samples) {
    // samples[1] == samples[2] == 30. Produces sub-segments
    //   [0, 10], [10, 30], [30, 30], [30, 40], [40, 50]
    // where [30, 30] is zero-width.
    auto seg = make_sampled_seg(/*min=*/0, /*max=*/50, /*num_rows=*/41, /*data_size=*/410,
                                /*iv=*/10, /*samples=*/{10, 30, 30, 40}, /*source_id=*/1);
    auto seg2 = make_seg(/*min=*/100, /*max=*/200, /*num_rows=*/50, /*data_size=*/500, /*source_id=*/2);
    std::vector<SegmentSplitInfo> segs = {seg, seg2};
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/40,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));

    auto [s1_rows, s1_bytes] = sum_source_stats(result, 1);
    EXPECT_EQ(41, s1_rows);
    EXPECT_EQ(410, s1_bytes);
}

// -----------------------------------------------------------------------------
// Loader validity check formula: producers guarantee
//   sort_key_samples.size() * sort_key_sample_row_interval < num_rows
// strictly. The loaders (get_tablet_split_ranges and _collect_segment_key_bounds)
// encode this with the overflow-safe form
//   num_samples <= (num_rows - 1) / row_interval
// This test directly exercises the formula at the boundary, ensuring that:
//   (a) a maximally-valid layout (ns * iv == num_rows - 1) is accepted; and
//   (b) the smallest-invalid layout (ns * iv == num_rows) is rejected.
// If the formula ever drifts (e.g. someone changes <= to <), this test fires.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, loader_validity_formula_boundary) {
    auto check = [](int64_t ns, int64_t iv, int64_t num_rows) {
        return iv > 0 && num_rows > 0 && ns <= (num_rows - 1) / iv;
    };

    // Boundary-valid: num_rows = N*iv + 1, accepted.
    EXPECT_TRUE(check(/*ns=*/2, /*iv=*/10, /*num_rows=*/21));
    // Boundary-invalid: num_rows = N*iv, rejected (would imply tail_rows == 0
    // which the producer never creates).
    EXPECT_FALSE(check(/*ns=*/2, /*iv=*/10, /*num_rows=*/20));
    // Way under: small segment with no samples.
    EXPECT_TRUE(check(/*ns=*/0, /*iv=*/10, /*num_rows=*/5));
    // Pathological iv == 0 (would crash on division), rejected by `iv > 0`.
    EXPECT_FALSE(check(/*ns=*/1, /*iv=*/0, /*num_rows=*/100));
    // Pathological num_rows == 0, rejected.
    EXPECT_FALSE(check(/*ns=*/1, /*iv=*/10, /*num_rows=*/0));

    // Overflow-safety: large N and iv that would overflow (ns*iv) in int64
    // but for which (num_rows - 1) / iv is well-defined. Confirms the
    // overflow-safe form.
    constexpr int64_t kBigIv = 1LL << 32;
    constexpr int64_t kBigN = 1LL << 32; // ns * iv = 2^64 in math, overflows int64
    constexpr int64_t kSmallNum = 100;
    EXPECT_FALSE(check(kBigN, kBigIv, kSmallNum)); // (100-1)/2^32 = 0; ns(2^32) > 0 -> rejected.
}

// -----------------------------------------------------------------------------
// Defense-in-depth: even if a corrupt SegmentSplitInfo somehow slips past the
// loader (e.g. test setup, future helper that bypasses the validity check),
// `calculate_range_split_boundaries` itself must not crash in DEBUG builds.
// In release builds the DCHECK is a no-op; the function may produce garbled
// per-range stats but must still return without UB.
//
// We construct an invalid segment with ns * iv == num_rows (tail_rows == 0)
// and surround it with a valid second segment so ordered_ranges has >= 2
// boundaries (otherwise the function returns early at step 2). We expect:
//  - In release: the call returns Status::OK (or empty boundaries) without UB.
//  - In debug: DCHECK_GT(tail_rows, 0) fires; we don't run this branch when
//    NDEBUG is unset.
// -----------------------------------------------------------------------------
#ifdef NDEBUG
TEST(TabletSplitterTest, calculate_range_split_handles_invalid_input_without_ub) {
    // num_rows=20, samples.size()=2, iv=10  ->  ns*iv=20=num_rows  ->  tail=0.
    auto bad = make_sampled_seg(0, 50, 20, 200, 10, {10, 20}, /*source_id=*/1);
    auto good = make_seg(100, 200, 50, 500, /*source_id=*/2);

    auto result_or = calculate_range_split_boundaries({bad, good}, /*target_split_count=*/2,
                                                      /*target_value_per_split=*/40,
                                                      /*use_num_rows=*/true, /*track_sources=*/true);
    EXPECT_TRUE(result_or.ok());
    // The good segment's 50 rows must still be credited; the bad segment's
    // 20 rows may be partially mis-attributed but must not blow up.
    if (result_or.ok()) {
        auto [good_rows, good_bytes] = sum_source_stats(result_or.value(), 2);
        EXPECT_EQ(50, good_rows);
        EXPECT_EQ(500, good_bytes);
    }
}
#endif

// -----------------------------------------------------------------------------
// Overflow safety: very large total_bytes and total_rows should not overflow
// the 128-bit intermediate in bytes_for. With num_rows = 1e9 and
// data_size = 1e11, the raw product rows * total_bytes overflows int64 at
// ~1e20, so the __int128 cast is required.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, bytes_for_does_not_overflow_at_extreme_scale) {
    constexpr int64_t kRows = 1'000'000'000LL;    // 1e9
    constexpr int64_t kBytes = 100'000'000'000LL; // 1e11
    constexpr int64_t kInterval = 65536LL;
    constexpr int64_t kN = (kRows - 1) / kInterval; // ~15258, fits invariant

    std::vector<int64_t> sample_values;
    sample_values.reserve(kN);
    for (int64_t k = 1; k <= kN; ++k) {
        sample_values.push_back(k); // monotonic; values distinct
    }
    auto seg = make_sampled_seg(0, kN + 1, kRows, kBytes, kInterval, sample_values, /*source_id=*/1);

    ASSIGN_OR_ABORT(auto result, calculate_range_split_boundaries({seg}, /*target_split_count=*/2,
                                                                  /*target_value_per_split=*/kBytes / 2,
                                                                  /*use_num_rows=*/false, /*track_sources=*/true));

    // Must not crash; byte conservation must hold exactly.
    int64_t total_bytes = 0;
    int64_t total_rows = 0;
    for (size_t i = 0; i < result.range_data_sizes.size(); ++i) {
        total_bytes += result.range_data_sizes[i];
        total_rows += result.range_num_rows[i];
    }
    EXPECT_EQ(kBytes, total_bytes);
    EXPECT_EQ(kRows, total_rows);
}

// -----------------------------------------------------------------------------
// Parallel-compaction-style invocation: use_num_rows=false (byte-weighted) and
// track_sources=false. Verifies the sampled path is usable in that mode too.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, byte_weighted_mode_and_no_track_sources) {
    std::vector<SegmentSplitInfo> segs;
    for (int i = 0; i < 4; ++i) {
        std::vector<int64_t> samples = {10 + i, 20 + i, 30 + i, 40 + i};
        segs.push_back(make_sampled_seg(/*min=*/i, /*max=*/50, /*num_rows=*/50, /*data_size=*/500,
                                        /*iv=*/10, samples, /*source_id=*/static_cast<uint32_t>(i)));
    }
    ASSIGN_OR_ABORT(auto result, calculate_range_split_boundaries(segs, /*target_split_count=*/2,
                                                                  /*target_value_per_split=*/1000,
                                                                  /*use_num_rows=*/false, /*track_sources=*/false));
    // No source stats when track_sources=false.
    EXPECT_TRUE(result.range_source_stats.empty());
    ASSERT_EQ(1, result.boundaries.size());

    int64_t total_rows = 0;
    int64_t total_bytes = 0;
    for (size_t i = 0; i < result.range_num_rows.size(); ++i) {
        total_rows += result.range_num_rows[i];
        total_bytes += result.range_data_sizes[i];
    }
    EXPECT_EQ(4 * 50, total_rows);
    EXPECT_EQ(4 * 500, total_bytes);
}

// -----------------------------------------------------------------------------
// Whole tablet has a single distinct key: every segment's min == max == k.
// ordered_boundaries has one unique value, ordered_ranges is empty; the
// algorithm must return an empty RangeSplitResult (no boundaries), not crash
// and not silently attribute rows to a non-existent range.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, all_segments_single_identical_key_no_split) {
    std::vector<SegmentSplitInfo> segs;
    for (int i = 0; i < 3; ++i) {
        segs.push_back(make_seg(/*min=*/42, /*max=*/42, /*num_rows=*/10, /*data_size=*/100,
                                /*source_id=*/static_cast<uint32_t>(i)));
    }
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/15,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));
    EXPECT_TRUE(result.boundaries.empty());
    EXPECT_TRUE(result.range_num_rows.empty());
    EXPECT_TRUE(result.range_data_sizes.empty());
}

// -----------------------------------------------------------------------------
// Large-scale reproducer of the RCA scenario (scaled down): overlapping
// segments, each carrying samples that densify the middle key interval. With
// samples, the split point should land near the median, not at a cluster edge.
// -----------------------------------------------------------------------------
TEST(TabletSplitterTest, rca_reproducer_scaled_down) {
    // 40 segments, each with min ~100-300, max ~5700-5900. Each has 100 rows
    // and 10 uniform samples spanning ~300..5800. Total 4000 rows; without
    // samples the algorithm would treat each segment as one [min, max] range
    // and pick a pathological split point; with samples it should find the
    // median near 3000.
    std::vector<SegmentSplitInfo> segs;
    for (int i = 0; i < 40; ++i) {
        int64_t min_v = 100 + i * 5;
        int64_t max_v = 5800 + i * 3;
        std::vector<int64_t> samples;
        // 9 samples evenly spaced between min and max
        // (sort_key_sample_row_interval = 10, num_rows = 100, so 9 samples +
        // tail = 10 rows per sub-segment).
        for (int s = 1; s <= 9; ++s) {
            int64_t v = min_v + (max_v - min_v) * s / 10;
            samples.push_back(v);
        }
        segs.push_back(make_sampled_seg(min_v, max_v, /*num_rows=*/100, /*data_size=*/1000, /*iv=*/10, samples,
                                        /*source_id=*/static_cast<uint32_t>(i)));
    }

    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/2000,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));
    ASSERT_EQ(1, result.boundaries.size());

    // Balanced split: |left - right| should be small compared to total 4000.
    int64_t left = result.range_num_rows[0];
    int64_t right = result.range_num_rows[1];
    EXPECT_EQ(4000, left + right);
    EXPECT_LT(std::abs(left - right) * 4, 4000) << "sampled-path split should be within 25% imbalance";
}

// -----------------------------------------------------------------------------
// PR #1: precise tablet_range clipping in calculate_range_split_boundaries.
//
// shared-data tablet split shares physical segments across child tablets, so
// after a tablet has been split once its remaining shared rowsets still report
// physical sort_key_min / sort_key_max that extend past the child tablet's
// range. When that child is split again, the algorithm builds ordered_ranges
// from segment boundaries plus tablet_range bounds (after this fix). The
// candidate filter must precisely exclude any ordered_range that falls
// outside the tablet's range, including ordered_ranges that previously
// straddled a tablet edge.
// -----------------------------------------------------------------------------

// Upper crossing: a segment whose physical extent crosses the tablet's
// upper_bound used to leak its out-of-range rows/bytes into the per-split
// estimates because the candidate filter only excluded ranges with no overlap.
TEST(TabletSplitterTest, tablet_range_excludes_data_past_upper_when_segments_straddle) {
    // seg1 [0, 50) is fully within tablet [0, 70).
    // seg2 [40, 100) physically crosses tablet's upper bound 70.
    auto seg1 = make_seg(/*min=*/0, /*max=*/50, /*num_rows=*/50, /*data_size=*/500, /*source_id=*/1);
    auto seg2 = make_seg(/*min=*/40, /*max=*/100, /*num_rows=*/60, /*data_size=*/600, /*source_id=*/2);

    TabletRange tablet_range(make_int_tuple(0), make_int_tuple(70),
                             /*lower_bound_included=*/true, /*upper_bound_included=*/false);

    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries({seg1, seg2}, /*target_split_count=*/2,
                                                     /*target_value_per_split=*/40,
                                                     /*use_num_rows=*/true, /*track_sources=*/true, &tablet_range));
    ASSERT_EQ(1u, result.boundaries.size());
    int64_t total_rows = 0;
    int64_t total_bytes = 0;
    for (size_t i = 0; i < result.range_num_rows.size(); ++i) {
        total_rows += result.range_num_rows[i];
        total_bytes += result.range_data_sizes[i];
    }
    // After inserting tablet_range bound 70, ordered_ranges become
    //   [0,40), [40,50), [50,70), [70,100)
    // seg2's 60 rows are split evenly across the 3 overlapping ranges
    // [40,50)/[50,70)/[70,100), giving 20 rows each. Only [70,100) is outside
    // the tablet, so seg2 contributes 40 rows. seg1 stays at 50 rows.
    EXPECT_EQ(90, total_rows);
    EXPECT_EQ(900, total_bytes);
    // Per-source sums: seg1 fully kept, seg2 trimmed.
    auto [s1_rows, s1_bytes] = sum_source_stats(result, 1);
    EXPECT_EQ(50, s1_rows);
    EXPECT_EQ(500, s1_bytes);
    auto [s2_rows, s2_bytes] = sum_source_stats(result, 2);
    EXPECT_EQ(40, s2_rows);
    EXPECT_EQ(400, s2_bytes);
}

// Lower-bound inclusivity: child tablets produced by split have
// lower_bound_included=true, but TabletRange::greater_than returns false when
// r.max == lower_bound under that flag. Without inserting the tablet bound and
// using a precise overlap predicate, an ordered_range whose r.max ==
// tablet.lower_bound is incorrectly kept and inflates the in-tablet sum.
TEST(TabletSplitterTest, tablet_range_excludes_data_below_when_lower_bound_inclusive) {
    auto seg1 = make_seg(/*min=*/0, /*max=*/60, /*num_rows=*/30, /*data_size=*/300, /*source_id=*/1);
    auto seg2 = make_sampled_seg(/*min=*/50, /*max=*/90, /*num_rows=*/40, /*data_size=*/400,
                                 /*iv=*/20, /*samples=*/{60}, /*source_id=*/2);
    auto seg3 = make_seg(/*min=*/70, /*max=*/100, /*num_rows=*/25, /*data_size=*/250, /*source_id=*/3);

    TabletRange tablet_range(make_int_tuple(60), make_int_tuple(100),
                             /*lower_bound_included=*/true, /*upper_bound_included=*/false);

    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries({seg1, seg2, seg3}, /*target_split_count=*/2,
                                                     /*target_value_per_split=*/30,
                                                     /*use_num_rows=*/true, /*track_sources=*/true, &tablet_range));
    ASSERT_EQ(1u, result.boundaries.size());

    // seg1 [0, 60) is entirely below the tablet (its r.max == tablet.lower_bound,
    // and the tablet's smallest key is 60-included; seg1 has no key in [60, _)).
    auto [s1_rows, s1_bytes] = sum_source_stats(result, 1);
    EXPECT_EQ(0, s1_rows);
    EXPECT_EQ(0, s1_bytes);
    // seg2's sample at 60 splits its rows into [50,60) (below) and [60,90)
    // (within), 20 rows per sub-segment.
    auto [s2_rows, s2_bytes] = sum_source_stats(result, 2);
    EXPECT_EQ(20, s2_rows);
    EXPECT_EQ(200, s2_bytes);
    // seg3 is fully within the tablet.
    auto [s3_rows, s3_bytes] = sum_source_stats(result, 3);
    EXPECT_EQ(25, s3_rows);
    EXPECT_EQ(250, s3_bytes);

    int64_t total_rows = 0;
    int64_t total_bytes = 0;
    for (size_t i = 0; i < result.range_num_rows.size(); ++i) {
        total_rows += result.range_num_rows[i];
        total_bytes += result.range_data_sizes[i];
    }
    EXPECT_EQ(45, total_rows);
    EXPECT_EQ(450, total_bytes);
}

// Regression: when segment boundaries are entirely inside tablet_range (the
// first-split shape), adding tablet_range bounds to ordered_boundaries must
// not change the per-split totals. The empty edge ranges contribute zero and
// the greedy still picks the same split point.
TEST(TabletSplitterTest, tablet_range_covering_all_data_does_not_change_totals) {
    auto seg1 = make_seg(/*min=*/10, /*max=*/50, /*num_rows=*/50, /*data_size=*/500, /*source_id=*/1);
    auto seg2 = make_seg(/*min=*/60, /*max=*/100, /*num_rows=*/50, /*data_size=*/500, /*source_id=*/2);

    ASSIGN_OR_ABORT(auto without, calculate_range_split_boundaries({seg1, seg2}, /*target_split_count=*/2,
                                                                   /*target_value_per_split=*/50,
                                                                   /*use_num_rows=*/true, /*track_sources=*/true));

    TabletRange tablet_range(make_int_tuple(0), make_int_tuple(150),
                             /*lower_bound_included=*/true, /*upper_bound_included=*/false);
    ASSIGN_OR_ABORT(auto with,
                    calculate_range_split_boundaries({seg1, seg2}, /*target_split_count=*/2,
                                                     /*target_value_per_split=*/50,
                                                     /*use_num_rows=*/true, /*track_sources=*/true, &tablet_range));

    ASSERT_EQ(without.boundaries.size(), with.boundaries.size());
    EXPECT_EQ(without.range_num_rows, with.range_num_rows);
    EXPECT_EQ(without.range_data_sizes, with.range_data_sizes);
    // Both source totals must match too.
    auto [w_s1_r, w_s1_b] = sum_source_stats(without, 1);
    auto [t_s1_r, t_s1_b] = sum_source_stats(with, 1);
    EXPECT_EQ(w_s1_r, t_s1_r);
    EXPECT_EQ(w_s1_b, t_s1_b);
    auto [w_s2_r, w_s2_b] = sum_source_stats(without, 2);
    auto [t_s2_r, t_s2_b] = sum_source_stats(with, 2);
    EXPECT_EQ(w_s2_r, t_s2_r);
    EXPECT_EQ(w_s2_b, t_s2_b);
}

// Conservation across split count: for the same input + tablet_range, the sum
// of per-split rows/bytes must be invariant regardless of the split count
// chosen. Runs split_count = 2..5 against a tablet whose bounds fall inside
// the segments' physical extent on both sides.
TEST(TabletSplitterTest, tablet_range_total_invariant_across_split_counts) {
    std::vector<int64_t> samples;
    for (int64_t v = 20; v <= 180; v += 20) samples.push_back(v);
    std::vector<SegmentSplitInfo> segs;
    segs.push_back(make_sampled_seg(/*min=*/0, /*max=*/200, /*num_rows=*/200, /*data_size=*/2000,
                                    /*iv=*/20, samples, /*source_id=*/1));
    segs.push_back(make_seg(/*min=*/50, /*max=*/150, /*num_rows=*/100, /*data_size=*/1000, /*source_id=*/2));

    TabletRange tablet_range(make_int_tuple(30), make_int_tuple(170),
                             /*lower_bound_included=*/true, /*upper_bound_included=*/false);

    int64_t reference_rows = -1;
    int64_t reference_bytes = -1;
    int succeeded_runs = 0;
    for (int32_t split_count = 2; split_count <= 5; ++split_count) {
        ASSIGN_OR_ABORT(auto result,
                        calculate_range_split_boundaries(segs, split_count, /*target_value_per_split=*/0,
                                                         /*use_num_rows=*/true, /*track_sources=*/true, &tablet_range));
        if (result.boundaries.empty()) continue;
        ++succeeded_runs;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;
        for (auto v : result.range_num_rows) total_rows += v;
        for (auto v : result.range_data_sizes) total_bytes += v;
        if (reference_rows < 0) {
            reference_rows = total_rows;
            reference_bytes = total_bytes;
        } else {
            EXPECT_EQ(reference_rows, total_rows) << "row total should be invariant across split count " << split_count;
            EXPECT_EQ(reference_bytes, total_bytes)
                    << "byte total should be invariant across split count " << split_count;
        }
    }
    EXPECT_GT(succeeded_runs, 0) << "expected at least one split count to succeed";
    EXPECT_GT(reference_rows, 0);
    EXPECT_GT(reference_bytes, 0);
}

// Helper unit tests for `allocate_proportionally` and `cap_and_redistribute_dels`
// live in `tablet_reshard_helper_test.cpp` — those helpers were moved to the
// `tablet_reshard_helper` namespace as part of the per-rowset anchor change so
// they can be reused by the planned cross-publish (P2) refactor without
// re-introducing a private split-only header.

// Helpers below serve two test groups: colocate-aware data-driven splitter
// tests (TabletSplitterTest.colocate_*) and external boundaries external-boundaries tests
// (TabletSplitterExternalBoundariesTest.*). Kept in one anonymous namespace to avoid
// duplicate-symbol churn.
namespace {

// Build a 2-column key tuple (k1, k2). Used for colocate-aware splitter tests.
static VariantTuple make_two_int_tuple(int64_t k1, int64_t k2) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_BIGINT), Datum(k1)));
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_BIGINT), Datum(k2)));
    return tuple;
}

static SegmentSplitInfo make_two_col_seg(int64_t lo_k1, int64_t lo_k2, int64_t hi_k1, int64_t hi_k2, int64_t num_rows,
                                         int64_t data_size, uint32_t source_id) {
    SegmentSplitInfo s;
    s.min_key = make_two_int_tuple(lo_k1, lo_k2);
    s.max_key = make_two_int_tuple(hi_k1, hi_k2);
    s.num_rows = num_rows;
    s.data_size = data_size;
    s.source_id = source_id;
    return s;
}

// =============================================================================
// external boundaries external-boundaries path helpers
// =============================================================================
//
// These exercise compute_split_ranges_from_external_boundaries' validation
// layers (1a/1b/1c/3a/3b). Empty tablets are used throughout so the production
// path returns at the empty fast-path (step 4) without needing a real
// TabletManager. tablet_manager = nullptr is safe in that regime.

static VariantPB make_bigint_variant_pb(int64_t value) {
    DatumVariant dv(get_type_info(LogicalType::TYPE_BIGINT), Datum(value));
    VariantPB pb;
    dv.to_proto(&pb);
    return pb;
}

static VariantPB make_varchar_variant_pb(const std::string& value) {
    auto type_info = get_type_info(LogicalType::TYPE_VARCHAR);
    DatumVariant dv(type_info, Datum(Slice(value)));
    VariantPB pb;
    dv.to_proto(&pb);
    return pb;
}

static VariantPB make_decimal64_variant_pb(int precision, int scale, int64_t raw_value) {
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, precision, scale);
    auto type_info = get_type_info(type_desc);
    DatumVariant dv(type_info, Datum(raw_value));
    VariantPB pb;
    dv.to_proto(&pb);
    return pb;
}

static TuplePB make_bigint_tuple_pb(int64_t value) {
    TuplePB t;
    *t.add_values() = make_bigint_variant_pb(value);
    return t;
}

// Build a closed-open [lower, upper) range. Either side may be absent (nullopt)
// for unbounded ±infinity semantics.
static TabletRangePB make_bigint_range_pb(std::optional<int64_t> lower, std::optional<int64_t> upper) {
    TabletRangePB r;
    if (lower.has_value()) {
        *r.mutable_lower_bound() = make_bigint_tuple_pb(*lower);
        r.set_lower_bound_included(true);
    }
    if (upper.has_value()) {
        *r.mutable_upper_bound() = make_bigint_tuple_pb(*upper);
        r.set_upper_bound_included(false);
    }
    return r;
}

// Builds a minimal empty TabletMetadataPtr with a single sort-key column of
// the given LogicalType and the requested parent range. Used by every external boundaries
// validation test that expects the function to return before touching
// segments/anchors. precision/scale/length are populated only when non-zero
// (relevant for decimal sort keys).
struct SortKeyColumnSpec {
    LogicalType type;
    int precision = 0;
    int scale = 0;
    int length = 0;
};

static TabletMetadataPtr make_empty_metadata_with_sort_key(const SortKeyColumnSpec& col_spec,
                                                           const TabletRangePB& parent_range,
                                                           bool set_sort_key_idxes = true) {
    auto m = std::make_shared<TabletMetadataPB>();
    m->set_id(1);
    m->set_version(1);

    auto* schema = m->mutable_schema();
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_id(100);
    auto* col = schema->add_column();
    col->set_unique_id(0);
    col->set_name("k1");
    col->set_type(logical_type_to_string(col_spec.type));
    col->set_is_key(true);
    col->set_is_nullable(false);
    if (col_spec.precision > 0) col->set_precision(col_spec.precision);
    if (col_spec.scale > 0) col->set_frac(col_spec.scale); // ColumnPB.frac is the scale field
    if (col_spec.length > 0) col->set_length(col_spec.length);
    if (set_sort_key_idxes) {
        schema->add_sort_key_idxes(0);
    }

    *m->mutable_range() = parent_range;
    return m;
}

static TabletMetadataPtr make_empty_metadata_bigint_key(std::optional<int64_t> parent_lower,
                                                        std::optional<int64_t> parent_upper) {
    return make_empty_metadata_with_sort_key({.type = TYPE_BIGINT}, make_bigint_range_pb(parent_lower, parent_upper));
}

// Like make_empty_metadata_bigint_key but with an empty sort_key_idxes -- the schema
// shape of a range-distributed DUPLICATE table created without an explicit ORDER BY.
// The external-boundaries path must fall back to the key columns instead of rejecting.
static TabletMetadataPtr make_empty_metadata_bigint_key_no_sort_key_idxes(std::optional<int64_t> parent_lower,
                                                                          std::optional<int64_t> parent_upper) {
    return make_empty_metadata_with_sort_key({.type = TYPE_BIGINT}, make_bigint_range_pb(parent_lower, parent_upper),
                                             /*set_sort_key_idxes=*/false);
}

static TabletMetadataPtr make_empty_metadata_decimal64_key(int precision, int scale) {
    // Decimal parent range: cover [10000, 100000) raw to keep tests simple.
    // (raw value space; the unit-test rationale doesn't depend on the human value)
    TabletRangePB parent_range;
    *parent_range.mutable_lower_bound()->add_values() = make_decimal64_variant_pb(precision, scale, 10000);
    parent_range.set_lower_bound_included(true);
    *parent_range.mutable_upper_bound()->add_values() = make_decimal64_variant_pb(precision, scale, 100000);
    parent_range.set_upper_bound_included(false);
    return make_empty_metadata_with_sort_key(
            {.type = TYPE_DECIMAL64, .precision = precision, .scale = scale, .length = 8}, parent_range);
}

static RepeatedPtrField<TabletRangePB> make_ranges(std::initializer_list<TabletRangePB> ranges) {
    RepeatedPtrField<TabletRangePB> r;
    for (const auto& range : ranges) {
        *r.Add() = range;
    }
    return r;
}

// Build a closed-open DECIMAL64 range with explicit raw values on both bounds.
static TabletRangePB make_decimal64_range_pb(int precision, int scale, int64_t lo_raw, int64_t hi_raw) {
    TabletRangePB r;
    *r.mutable_lower_bound()->add_values() = make_decimal64_variant_pb(precision, scale, lo_raw);
    r.set_lower_bound_included(true);
    *r.mutable_upper_bound()->add_values() = make_decimal64_variant_pb(precision, scale, hi_raw);
    r.set_upper_bound_included(false);
    return r;
}

// external-boundaries validation test helper: invokes compute_split_ranges_from_external_boundaries
// and asserts the call rejected with an InvalidArgument. Used by every test that
// expects the function to fail early before touching segments/anchors.
static void expect_external_boundaries_rejected(const TabletMetadataPtr& m,
                                                const RepeatedPtrField<TabletRangePB>& ranges) {
    std::vector<TabletRangeInfo> out;
    auto status = compute_split_ranges_from_external_boundaries(/*tablet_manager=*/nullptr, m, ranges, &out);
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is_invalid_argument()) << status;
}

} // namespace

// Three disjoint segments with three distinct k1 values produce candidate
// ranges between every adjacent pair. With colocate_column_count=1 and a 2-way
// split, the chosen boundary lands at a k1 transition; canonicalization
// rewrites the boundary to (right_k1, NULL).
TEST(TabletSplitterTest, colocate_aware_split_picks_canonical_boundary) {
    std::vector<SegmentSplitInfo> segs = {make_two_col_seg(/*lo*/ 100, 1, /*hi*/ 100, 999, 100, 1000, /*source_id=*/1),
                                          make_two_col_seg(/*lo*/ 200, 1, /*hi*/ 200, 999, 100, 1000, /*source_id=*/2),
                                          make_two_col_seg(/*lo*/ 300, 1, /*hi*/ 300, 999, 100, 1000, /*source_id=*/3)};

    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/150,
                                                     /*use_num_rows=*/true, /*track_sources=*/true,
                                                     /*tablet_range=*/nullptr, /*colocate_column_count=*/1));
    ASSERT_EQ(1, result.boundaries.size());
    const auto& b = result.boundaries[0];
    ASSERT_EQ(2u, b.size());
    // First column = the right neighbor's k1 (200 or 300, depending on greedy choice).
    EXPECT_FALSE(b[0].value().is_null()) << "colocate-prefix position must be non-null";
    // Second column must be NULL (canonical NULL filler).
    EXPECT_TRUE(b[1].value().is_null()) << "trailing positions must be NULL after canonicalization";
}

// When all data lives within a single colocate prefix value, the splitter has
// no transition to canonicalize and falls back to the within-key boundary.
// The resulting boundary's leading column equals the prefix and trailing
// columns hold real (non-null) values.
TEST(TabletSplitterTest, colocate_aware_split_falls_back_within_prefix) {
    // Two non-overlapping segments, both with k1=100, separated by k2.
    std::vector<SegmentSplitInfo> segs = {
            make_two_col_seg(/*lo*/ 100, 1, /*hi*/ 100, 100, 100, 1000, /*source_id=*/1),
            make_two_col_seg(/*lo*/ 100, 200, /*hi*/ 100, 300, 100, 1000, /*source_id=*/2)};

    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/100,
                                                     /*use_num_rows=*/true, /*track_sources=*/true,
                                                     /*tablet_range=*/nullptr, /*colocate_column_count=*/1));
    ASSERT_EQ(1, result.boundaries.size());
    const auto& b = result.boundaries[0];
    ASSERT_EQ(2u, b.size());
    // No prefix transition → no canonicalization → trailing position is non-null.
    EXPECT_FALSE(b[1].value().is_null()) << "within-prefix split must keep the actual k2 boundary";
}

// colocate_column_count == 0 must reproduce pre-P3 behavior exactly: no
// canonicalization, boundaries are unchanged data tuples.
TEST(TabletSplitterTest, colocate_column_count_zero_unchanged) {
    std::vector<SegmentSplitInfo> segs = {make_two_col_seg(/*lo*/ 100, 1, /*hi*/ 100, 100, 100, 1000, /*source_id=*/1),
                                          make_two_col_seg(/*lo*/ 200, 1, /*hi*/ 200, 100, 100, 1000, /*source_id=*/2)};

    ASSIGN_OR_ABORT(auto result_with_colocate,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/100,
                                                     /*use_num_rows=*/true, /*track_sources=*/true,
                                                     /*tablet_range=*/nullptr, /*colocate_column_count=*/1));
    ASSIGN_OR_ABORT(auto result_default,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/100,
                                                     /*use_num_rows=*/true, /*track_sources=*/true,
                                                     /*tablet_range=*/nullptr, /*colocate_column_count=*/0));

    ASSERT_EQ(1, result_default.boundaries.size());
    ASSERT_EQ(1, result_with_colocate.boundaries.size());
    // Default-mode boundary's trailing column is non-null (came from a real key).
    EXPECT_FALSE(result_default.boundaries[0][1].value().is_null());
    // Colocate-aware boundary's trailing column IS null (synthesized NULL filler).
    EXPECT_TRUE(result_with_colocate.boundaries[0][1].value().is_null());
}

// Regression for the prefix-straddling-segment case. A single segment whose key range crosses a
// colocate-prefix transition must have its rows distributed across LEFT and RIGHT child groups
// when the split lands on a canonical boundary inside the segment, rather than being assigned
// wholly to the right child by the post-pass `range->max.compare(boundary)` walk.
TEST(TabletSplitterTest, colocate_aware_split_keeps_stats_consistent_across_prefix_boundary) {
    // s1 spans prefix 100..300 (single segment crossing two prefix transitions).
    // s2 sits at prefix 400 to drive a 2-way split.
    std::vector<SegmentSplitInfo> segs = {make_two_col_seg(/*lo*/ 100, 0, /*hi*/ 300, 50, 200, 2000, /*source_id=*/1),
                                          make_two_col_seg(/*lo*/ 400, 0, /*hi*/ 400, 999, 100, 1000, /*source_id=*/2)};
    ASSIGN_OR_ABORT(auto result,
                    calculate_range_split_boundaries(segs, /*target_split_count=*/2, /*target_value_per_split=*/150,
                                                     /*use_num_rows=*/true, /*track_sources=*/true,
                                                     /*tablet_range=*/nullptr, /*colocate_column_count=*/1));
    ASSERT_EQ(1, result.boundaries.size());
    // Boundary must be canonical so FE classifies the split as Level 1.
    EXPECT_TRUE(result.boundaries[0][1].value().is_null())
            << "boundary trailing column must be NULL after canonicalization";
    // Σ children must equal Σ inputs.
    int64_t total_rows = result.range_num_rows[0] + result.range_num_rows[1];
    int64_t total_bytes = result.range_data_sizes[0] + result.range_data_sizes[1];
    EXPECT_EQ(300, total_rows);
    EXPECT_EQ(3000, total_bytes);
    // The straddling-segment's rows must be partly attributed to the LEFT child — the bug was
    // assigning them entirely to RIGHT when the canonical boundary sorts before range->max.
    auto left_it = result.range_source_stats[0].find(1);
    ASSERT_NE(result.range_source_stats[0].end(), left_it)
            << "rows from the prefix-crossing segment must contribute to the LEFT child";
    EXPECT_GT(left_it->second.first, 0);
    EXPECT_GT(left_it->second.second, 0);
}

// Regression for the DATA-DRIVEN colocate split rejecting tables whose sort key is implicit.
// A PRIMARY KEY (or DUPLICATE KEY without an explicit ORDER BY) table leaves sort_key_idxes empty
// in the raw TabletSchemaPB. get_tablet_split_ranges_impl must resolve the arity from the
// materialized schema (empty sort_key_idxes => key columns) before validating colocate_column_count;
// reading the raw PB saw arity 0 and rejected every colocate split, silently falling back to an
// identical tablet so an oversized colocate tablet never split.
TEST(TabletSplitterTest, data_driven_colocate_split_resolves_empty_sort_key_idxes) {
    auto m = make_empty_metadata_bigint_key_no_sort_key_idxes(0, 100);
    ASSERT_TRUE(m->schema().sort_key_idxes().empty());

    // colocate_column_count == 1 (the single key column) must pass the arity gate now that it is
    // resolved against the materialized schema. The empty tablet then fails later at segment
    // loading -- proving the gate was passed (pre-fix it failed with "Invalid colocate_column_count").
    std::vector<TabletRangeInfo> split_ranges;
    auto s = get_tablet_split_ranges(/*tablet_manager=*/nullptr, m, /*split_count=*/2, &split_ranges,
                                     /*colocate_column_count=*/1);
    ASSERT_FALSE(s.ok());
    const std::string msg = s.to_string();
    EXPECT_EQ(std::string::npos, msg.find("Invalid colocate_column_count"))
            << "arity gate must resolve empty sort_key_idxes to key columns; actual: " << msg;
    EXPECT_NE(std::string::npos, msg.find("No segments"))
            << "empty tablet must fail at segment loading, not the arity gate; actual: " << msg;
}

// -----------------------------------------------------------------------------
// Happy path: empty tablet, K=2 well-formed ranges covering parent.
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, empty_tablet_happy_path_k2) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    auto ranges = make_ranges({make_bigint_range_pb(0, 50), make_bigint_range_pb(50, 100)});
    std::vector<TabletRangeInfo> out;
    auto status = compute_split_ranges_from_external_boundaries(/*tablet_manager=*/nullptr, m, ranges, &out);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(2, out.size());
    EXPECT_TRUE(out[0].rowset_stats.empty());
    EXPECT_TRUE(out[1].rowset_stats.empty());
}

// -----------------------------------------------------------------------------
// Empty sort_key_idxes (table created without an explicit ORDER BY): the sort key
// falls back to the key columns, so the split must succeed -- not be rejected and
// degraded to an identical (un-split) tablet.
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, empty_sort_key_idxes_falls_back_to_key_columns) {
    auto m = make_empty_metadata_bigint_key_no_sort_key_idxes(0, 100);
    ASSERT_TRUE(m->schema().sort_key_idxes().empty());
    auto ranges = make_ranges({make_bigint_range_pb(0, 50), make_bigint_range_pb(50, 100)});
    std::vector<TabletRangeInfo> out;
    auto status = compute_split_ranges_from_external_boundaries(/*tablet_manager=*/nullptr, m, ranges, &out);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(2, out.size());
}

// -----------------------------------------------------------------------------
// 1a: tuple arity mismatch (2-column tuple for a 1-column sort key)
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, tuple_arity_mismatch_is_rejected) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    TabletRangePB r0;
    auto* lo = r0.mutable_lower_bound();
    *lo->add_values() = make_bigint_variant_pb(0);
    *lo->add_values() = make_bigint_variant_pb(0); // extra column
    r0.set_lower_bound_included(true);
    *r0.mutable_upper_bound() = make_bigint_tuple_pb(50);
    r0.set_upper_bound_included(false);
    expect_external_boundaries_rejected(m, make_ranges({r0, make_bigint_range_pb(50, 100)}));
}

// -----------------------------------------------------------------------------
// 1b: variant type mismatch (VARCHAR variant for BIGINT sort key)
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, tuple_type_mismatch_is_rejected) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    TabletRangePB r0 = make_bigint_range_pb(0, 50);
    // Replace upper bound with a VARCHAR variant.
    r0.mutable_upper_bound()->clear_values();
    *r0.mutable_upper_bound()->add_values() = make_varchar_variant_pb("50");
    expect_external_boundaries_rejected(m, make_ranges({r0, make_bigint_range_pb(50, 100)}));
}

// -----------------------------------------------------------------------------
// 1b: decimal precision mismatch (schema DECIMAL64(10,2), bound DECIMAL64(11,2))
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, decimal_precision_mismatch_is_rejected) {
    auto m = make_empty_metadata_decimal64_key(10, 2);
    // Build two adjacent ranges spanning parent [10000, 100000) but with bound
    // precision 11 instead of 10.
    expect_external_boundaries_rejected(m, make_ranges({make_decimal64_range_pb(11, 2, 10000, 50000),
                                                        make_decimal64_range_pb(11, 2, 50000, 100000)}));
}

// -----------------------------------------------------------------------------
// 1b: decimal scale mismatch (schema DECIMAL64(10,2), bound DECIMAL64(10,3))
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, decimal_scale_mismatch_is_rejected) {
    auto m = make_empty_metadata_decimal64_key(10, 2);
    expect_external_boundaries_rejected(m, make_ranges({make_decimal64_range_pb(10, 3, 10000, 50000),
                                                        make_decimal64_range_pb(10, 3, 50000, 100000)}));
}

// -----------------------------------------------------------------------------
// 1b: variant has no `type` field
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, variant_missing_type_is_rejected) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    TabletRangePB r0 = make_bigint_range_pb(0, 50);
    // Drop the `type` field on the upper bound's variant.
    r0.mutable_upper_bound()->mutable_values(0)->clear_type();
    expect_external_boundaries_rejected(m, make_ranges({r0, make_bigint_range_pb(50, 100)}));
}

// -----------------------------------------------------------------------------
// 1b: variant has `type` but PTypeDesc.types is empty
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, variant_empty_ptypedesc_is_rejected) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    TabletRangePB r0 = make_bigint_range_pb(0, 50);
    r0.mutable_upper_bound()->mutable_values(0)->mutable_type()->clear_types();
    expect_external_boundaries_rejected(m, make_ranges({r0, make_bigint_range_pb(50, 100)}));
}

// -----------------------------------------------------------------------------
// 1b: variant uses a non-SCALAR PTypeNode (ARRAY) — must reject before
// TypeDescriptor::from_protobuf reaches unsafe child-node access.
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, variant_non_scalar_node_is_rejected) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    TabletRangePB r0 = make_bigint_range_pb(0, 50);
    // Mutate the single PTypeNode on the upper bound's variant to look like
    // ARRAY (TTypeNodeType::ARRAY == 1) and remove its scalar_type payload.
    auto* var = r0.mutable_upper_bound()->mutable_values(0);
    var->mutable_type()->mutable_types(0)->set_type(static_cast<int32_t>(TTypeNodeType::ARRAY));
    var->mutable_type()->mutable_types(0)->clear_scalar_type();
    expect_external_boundaries_rejected(m, make_ranges({r0, make_bigint_range_pb(50, 100)}));
}

// -----------------------------------------------------------------------------
// 3b: adjacent ranges out of order semantically. Important: empty tablets must
// also reject this (the fix that moved the semantic check before the empty
// fast-path).
// -----------------------------------------------------------------------------
TEST(TabletSplitterExternalBoundariesTest, empty_tablet_rejects_semantic_inversion) {
    auto m = make_empty_metadata_bigint_key(0, 100);
    // 3 ranges that tile structurally (first.lower==0, last.upper==100, byte
    // adjacencies hold), but the middle range is semantically inverted:
    //   [0, 50) [50, 40) [40, 100)
    // ranges[0].upper == ranges[1].lower byte-wise (50==50).
    // ranges[1].upper == ranges[2].lower byte-wise (40==40).
    // Structural validation accepts; step 3a semantic check rejects on the
    // middle range's lower(50) >= upper(40).
    expect_external_boundaries_rejected(
            m, make_ranges({make_bigint_range_pb(0, 50), make_bigint_range_pb(50, 40), make_bigint_range_pb(40, 100)}));
}

// =============================================================================
// Parity: external-boundaries path produces the same per-range output as the data-driven path
// when given the same K-1 boundaries.
// =============================================================================
//
// Both paths share build_rowset_anchor + apply_rowset_anchor downstream, so
// equivalent boundary inputs SHOULD yield equivalent outputs (ranges + per-
// rowset stats). This test:
//   1. Runs the data-driven path on a synthetic 2-rowset tablet.
//   2. Extracts the K-1 boundary it picked.
//   3. Feeds the same boundary back through the external-boundaries path.
//   4. Asserts the resulting K TabletRangeInfo entries are byte-equal.
//
// DUP_KEYS + explicit rowset-level num_dels lets us pass tablet_manager =
// nullptr (no PK delvec fallback).

namespace {

// Build a DUP_KEYS tablet with N rowsets at disjoint integer key ranges. Each
// rowset gets one segment populated with sort_key_min/max + num_rows; the
// rowset-level num_rows/data_size/num_dels are set explicitly to skip the
// build_rowset_anchor PK-fallback path. {@code parent_range} is an explicit
// closed-open parent range; pass an empty TabletRangePB for Range.all() parent.
static TabletMetadataPtr make_dup_keys_metadata_with_rowsets(
        std::initializer_list<std::tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>> rowsets,
        const TabletRangePB& parent_range = {}) {
    auto m = std::make_shared<TabletMetadataPB>();
    m->set_id(1);
    m->set_version(1);
    auto* schema = m->mutable_schema();
    schema->set_keys_type(DUP_KEYS);
    schema->set_id(100);
    auto* col = schema->add_column();
    col->set_unique_id(0);
    col->set_name("k1");
    col->set_type("BIGINT");
    col->set_is_key(true);
    col->set_is_nullable(false);
    schema->add_sort_key_idxes(0);
    *m->mutable_range() = parent_range;

    for (const auto& [id, lo, hi, num_rows, data_size] : rowsets) {
        auto* r = m->add_rowsets();
        r->set_id(id);
        r->set_num_rows(num_rows);
        r->set_data_size(data_size);
        r->set_num_dels(0); // explicit to skip the PK delvec fallback in build_rowset_anchor.
        auto* sm = r->add_segment_metas();
        sm->set_filename("seg" + std::to_string(id));
        sm->set_size(data_size);
        sm->set_num_rows(num_rows);
        *sm->mutable_sort_key_min() = make_bigint_tuple_pb(lo);
        *sm->mutable_sort_key_max() = make_bigint_tuple_pb(hi);
    }
    return m;
}

} // namespace

TEST(TabletSplitterExternalBoundariesTest, parent_envelope_clips_effective_lo_hi) {
    // Parent [0, 100). Two rowsets fully within parent at [10, 40] and [60, 90].
    // external boundaries K=2 split at 50. Exercises the effective-envelope branches that read
    // parent.lower_bound / parent.upper_bound (otherwise unreachable from the
    // Range.all()-parent parity test).
    auto m = make_dup_keys_metadata_with_rowsets(
            {
                    std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 10, 40, 100, 1000),
                    std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(2, 60, 90, 100, 1000),
            },
            make_bigint_range_pb(0, 100));

    auto ranges = make_ranges({make_bigint_range_pb(0, 50), make_bigint_range_pb(50, 100)});
    std::vector<TabletRangeInfo> out;
    auto s = compute_split_ranges_from_external_boundaries(/*tablet_manager=*/nullptr, m, ranges, &out);
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_EQ(2u, out.size());
    // Each rowset's stats must sum to the parent's totals (anchor conservation).
    for (uint32_t rowset_id : {1u, 2u}) {
        int64_t sum_rows = 0;
        int64_t sum_size = 0;
        for (const auto& tri : out) {
            auto it = tri.rowset_stats.find(rowset_id);
            if (it != tri.rowset_stats.end()) {
                sum_rows += it->second.num_rows;
                sum_size += it->second.data_size;
            }
        }
        EXPECT_EQ(100, sum_rows) << "rowset " << rowset_id;
        EXPECT_EQ(1000, sum_size) << "rowset " << rowset_id;
    }
}

TEST(TabletSplitterParityTest, external_boundaries_matches_data_driven_when_fed_same_boundaries) {
    // Two disjoint rowsets at [0,50] and [60,100], 100 rows each. Data-driven
    // will pick a boundary somewhere in the gap (50..60) for K=2.
    auto m = make_dup_keys_metadata_with_rowsets({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 50, 100, 1000),
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(2, 60, 100, 100, 1000),
    });

    std::vector<TabletRangeInfo> split_data_driven;
    auto s = get_tablet_split_ranges(/*tablet_manager=*/nullptr, m, /*split_count=*/2, &split_data_driven,
                                     /*colocate_column_count=*/0);
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_EQ(2u, split_data_driven.size());
    ASSERT_TRUE(split_data_driven[0].range.has_upper_bound());
    ASSERT_TRUE(split_data_driven[1].range.has_lower_bound());
    // Byte-equality of the interior boundary (data-driven emits one tuple,
    // not two — same TuplePB on both sides).
    ASSERT_TRUE(MessageDifferencer::Equals(split_data_driven[0].range.upper_bound(),
                                           split_data_driven[1].range.lower_bound()))
            << "data-driven path's interior boundary must be a single TuplePB on both sides";

    // Build external boundaries external_ranges from the data-driven boundary, mirroring the
    // parent Range.all() (first.lower unset, last.upper unset).
    RepeatedPtrField<TabletRangePB> external_ranges;
    {
        auto* r0 = external_ranges.Add();
        *r0->mutable_upper_bound() = split_data_driven[0].range.upper_bound();
        r0->set_upper_bound_included(false);
    }
    {
        auto* r1 = external_ranges.Add();
        *r1->mutable_lower_bound() = split_data_driven[1].range.lower_bound();
        r1->set_lower_bound_included(true);
    }

    std::vector<TabletRangeInfo> split_external_boundaries;
    s = compute_split_ranges_from_external_boundaries(/*tablet_manager=*/nullptr, m, external_ranges,
                                                      &split_external_boundaries);
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_EQ(2u, split_external_boundaries.size());

    // Ranges must be byte-equal proto messages.
    for (size_t i = 0; i < split_data_driven.size(); ++i) {
        EXPECT_TRUE(MessageDifferencer::Equals(split_data_driven[i].range, split_external_boundaries[i].range))
                << "range[" << i << "] differs between paths";
    }

    // Per-rowset stats must match field-by-field.
    for (size_t i = 0; i < split_data_driven.size(); ++i) {
        EXPECT_EQ(split_data_driven[i].rowset_stats.size(), split_external_boundaries[i].rowset_stats.size())
                << "rowset_stats size differs at range " << i;
        for (const auto& [rowset_id, stats_dd] : split_data_driven[i].rowset_stats) {
            auto it = split_external_boundaries[i].rowset_stats.find(rowset_id);
            ASSERT_NE(it, split_external_boundaries[i].rowset_stats.end())
                    << "rowset " << rowset_id << " missing in external boundaries output at range " << i;
            EXPECT_EQ(stats_dd.num_rows, it->second.num_rows)
                    << "range[" << i << "] rowset[" << rowset_id << "] num_rows differs";
            EXPECT_EQ(stats_dd.data_size, it->second.data_size)
                    << "range[" << i << "] rowset[" << rowset_id << "] data_size differs";
            EXPECT_EQ(stats_dd.num_dels, it->second.num_dels)
                    << "range[" << i << "] rowset[" << rowset_id << "] num_dels differs";
        }
    }

    // Sanity: anchored stats sum to parent totals (the property both paths
    // claim). Aggregate per rowset across the two child ranges.
    for (uint32_t rowset_id : {1u, 2u}) {
        int64_t sum_rows = 0;
        int64_t sum_size = 0;
        for (const auto& s : split_data_driven) {
            auto it = s.rowset_stats.find(rowset_id);
            if (it != s.rowset_stats.end()) {
                sum_rows += it->second.num_rows;
                sum_size += it->second.data_size;
            }
        }
        EXPECT_EQ(100, sum_rows) << "rowset " << rowset_id << ": Σ children num_rows must equal parent";
        EXPECT_EQ(1000, sum_size) << "rowset " << rowset_id << ": Σ children data_size must equal parent";
    }
}

// -----------------------------------------------------------------------------
// Phase-1 per-segment shared ownership helpers.
// -----------------------------------------------------------------------------

namespace {
// Append a segment with bigint [mn, mx] sort-key bounds.
static void add_bigint_seg(RowsetMetadataPB* rs, int64_t mn, int64_t mx, const std::string& name) {
    auto* m = rs->add_segment_metas();
    m->set_filename(name);
    *m->mutable_sort_key_min() = make_bigint_tuple_pb(mn);
    *m->mutable_sort_key_max() = make_bigint_tuple_pb(mx);
}
} // namespace

TEST(TabletSplitterTest, CanPruneRowsetSegments_predicates) {
    auto make_pruneable = []() {
        RowsetMetadataPB rs;
        add_bigint_seg(&rs, 0, 1, "s0");
        return rs;
    };
    EXPECT_TRUE(can_prune_rowset_segments(make_pruneable()));

    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->set_bundle_file_offset(0); // bundled segment is pruneable
        EXPECT_TRUE(can_prune_rowset_segments(rs));
    } // (a) ok
    {
        auto rs = make_pruneable();
        rs.set_next_compaction_offset(1);
        EXPECT_FALSE(can_prune_rowset_segments(rs));
    } // (b)
    {
        auto rs = make_pruneable();
        rs.add_segment_metas(); // extra segment lacking sort-key bounds
        EXPECT_FALSE(can_prune_rowset_segments(rs));
    } // (c) missing bound on extra segment
    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->clear_sort_key_max();
        EXPECT_FALSE(can_prune_rowset_segments(rs));
    } // (c) missing bound
    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->set_shared(true);
        EXPECT_TRUE(can_prune_rowset_segments(rs));
    } // (d) ok
}

TEST(TabletSplitterTest, ComputeOwnership_exclusive_segment_becomes_private) {
    // Tiled ranges [0,11) and [11,21); segment [1,5] strictly inside range 0.
    std::vector<TabletRangePB> ranges{make_bigint_range_pb(0, 11), make_bigint_range_pb(11, 21)};
    RowsetMetadataPB rs;
    add_bigint_seg(&rs, 1, 5, "s0");
    auto own = compute_rowset_segment_ownership(rs, ranges);
    ASSERT_TRUE(own.ok());
    EXPECT_TRUE(own->segments[0].keep[0]);
    EXPECT_FALSE(own->segments[0].keep[1]);
    EXPECT_FALSE(own->segments[0].shared[0]); // exclusive + contained -> private
}

TEST(TabletSplitterTest, ComputeOwnership_spanning_segment_stays_shared) {
    std::vector<TabletRangePB> ranges{make_bigint_range_pb(0, 11), make_bigint_range_pb(11, 21)};
    RowsetMetadataPB rs;
    add_bigint_seg(&rs, 5, 15, "s0"); // spans both
    auto own = compute_rowset_segment_ownership(rs, ranges);
    ASSERT_TRUE(own.ok());
    EXPECT_TRUE(own->segments[0].keep[0]);
    EXPECT_TRUE(own->segments[0].keep[1]);
    EXPECT_TRUE(own->segments[0].shared[0]); // overlap_count>=2 -> shared
    EXPECT_TRUE(own->segments[0].shared[1]);
}

TEST(TabletSplitterTest, ComputeOwnership_old_shared_stays_shared) {
    std::vector<TabletRangePB> ranges{make_bigint_range_pb(0, 11), make_bigint_range_pb(11, 21)};
    RowsetMetadataPB rs;
    add_bigint_seg(&rs, 1, 5, "s0");
    rs.mutable_segment_metas(0)->set_shared(true); // inherited shared (multi-level)
    auto own = compute_rowset_segment_ownership(rs, ranges);
    ASSERT_TRUE(own.ok());
    EXPECT_TRUE(own->segments[0].shared[0]); // old_shared -> stays shared even though exclusive
}

TEST(TabletSplitterTest, ComputeOwnership_failclosed_when_not_contained) {
    // Single range [0,11) does not cover the segment's max (15). overlap_count==1,
    // old_shared=false, but seg_max=15 not contained -> must stay shared (fail-closed).
    std::vector<TabletRangePB> ranges{make_bigint_range_pb(0, 11)};
    RowsetMetadataPB rs;
    add_bigint_seg(&rs, 5, 15, "s0");
    auto own = compute_rowset_segment_ownership(rs, ranges);
    ASSERT_TRUE(own.ok());
    EXPECT_TRUE(own->segments[0].keep[0]);
    EXPECT_TRUE(own->segments[0].shared[0]); // fail-closed (not provably contained)
}

TEST(TabletSplitterTest, ApplyOwnership_prunes_and_rebuilds_shared) {
    RowsetMetadataPB source_rowset;
    add_bigint_seg(&source_rowset, 0, 1, "s0");
    add_bigint_seg(&source_rowset, 5, 15, "s1");
    add_bigint_seg(&source_rowset, 18, 19, "s2");
    source_rowset.mutable_segment_metas(0)->set_size(10);
    source_rowset.mutable_segment_metas(1)->set_size(20);
    source_rowset.mutable_segment_metas(2)->set_size(30);

    RowsetOwnership ownership;
    ownership.segments.resize(3);
    ownership.segments[0] = {{true, false}, {false, false}};
    ownership.segments[1] = {{true, true}, {true, true}};
    ownership.segments[2] = {{false, true}, {false, false}};

    RowsetMetadataPB rowset_for_tablet0 = source_rowset;
    ASSERT_OK(apply_segment_ownership_to_new_tablet_rowset(&rowset_for_tablet0, ownership, 0));
    ASSERT_EQ(2, rowset_for_tablet0.segment_metas_size());
    EXPECT_EQ("s0", rowset_for_tablet0.segment_metas(0).filename());
    EXPECT_EQ("s1", rowset_for_tablet0.segment_metas(1).filename());
    EXPECT_FALSE(rowset_for_tablet0.segment_metas(0).shared());
    EXPECT_TRUE(rowset_for_tablet0.segment_metas(1).shared());
    EXPECT_EQ(0u, rowset_for_tablet0.segment_metas(0).segment_idx()); // synthesized to original positional index
    EXPECT_EQ(1u, rowset_for_tablet0.segment_metas(1).segment_idx());

    RowsetMetadataPB rowset_for_tablet1 = source_rowset;
    ASSERT_OK(apply_segment_ownership_to_new_tablet_rowset(&rowset_for_tablet1, ownership, 1));
    ASSERT_EQ(2, rowset_for_tablet1.segment_metas_size());
    EXPECT_EQ("s1", rowset_for_tablet1.segment_metas(0).filename());
    EXPECT_EQ("s2", rowset_for_tablet1.segment_metas(1).filename());
    EXPECT_TRUE(rowset_for_tablet1.segment_metas(0).shared());
    EXPECT_FALSE(rowset_for_tablet1.segment_metas(1).shared());
    EXPECT_EQ(1u, rowset_for_tablet1.segment_metas(0).segment_idx());
    EXPECT_EQ(2u, rowset_for_tablet1.segment_metas(1).segment_idx());
}

// encryption_meta travels inside each SegmentMetadataPB, so it must prune in lockstep
// with the segment it describes; a misaligned/dropped entry is load-time corruption.
TEST(TabletSplitterTest, ApplyOwnership_prunes_segment_encryption_metas) {
    RowsetMetadataPB source_rowset;
    add_bigint_seg(&source_rowset, 0, 1, "s0");
    add_bigint_seg(&source_rowset, 5, 15, "s1");
    add_bigint_seg(&source_rowset, 18, 19, "s2");
    source_rowset.mutable_segment_metas(0)->set_size(10);
    source_rowset.mutable_segment_metas(1)->set_size(20);
    source_rowset.mutable_segment_metas(2)->set_size(30);
    source_rowset.mutable_segment_metas(0)->set_encryption_meta("enc0");
    source_rowset.mutable_segment_metas(1)->set_encryption_meta("enc1");
    source_rowset.mutable_segment_metas(2)->set_encryption_meta("enc2");

    RowsetOwnership ownership;
    ownership.segments.resize(3);
    ownership.segments[0] = {{true, false}, {false, false}};
    ownership.segments[1] = {{true, true}, {true, true}};
    ownership.segments[2] = {{false, true}, {false, false}};

    // tablet0 keeps segments 0,1 -> encryption metas align to enc0,enc1.
    RowsetMetadataPB rowset_for_tablet0 = source_rowset;
    ASSERT_OK(apply_segment_ownership_to_new_tablet_rowset(&rowset_for_tablet0, ownership, 0));
    ASSERT_EQ(2, rowset_for_tablet0.segment_metas_size());
    EXPECT_EQ("enc0", rowset_for_tablet0.segment_metas(0).encryption_meta());
    EXPECT_EQ("enc1", rowset_for_tablet0.segment_metas(1).encryption_meta());

    // tablet1 keeps segments 1,2 -> enc1,enc2.
    RowsetMetadataPB rowset_for_tablet1 = source_rowset;
    ASSERT_OK(apply_segment_ownership_to_new_tablet_rowset(&rowset_for_tablet1, ownership, 1));
    ASSERT_EQ(2, rowset_for_tablet1.segment_metas_size());
    EXPECT_EQ("enc1", rowset_for_tablet1.segment_metas(0).encryption_meta());
    EXPECT_EQ("enc2", rowset_for_tablet1.segment_metas(1).encryption_meta());
}

// bundle_file_offset is an absolute byte offset into one shared physical file carried
// inside each SegmentMetadataPB; it must prune in lockstep with the segment. The offsets
// are positional (not derived from neighbors), so pruning a middle segment must not shift
// survivors.
TEST(TabletSplitterTest, ApplyOwnership_prunes_bundle_file_offsets) {
    RowsetMetadataPB source_rowset;
    add_bigint_seg(&source_rowset, 0, 1, "bundle.dat");
    add_bigint_seg(&source_rowset, 5, 15, "bundle.dat");
    add_bigint_seg(&source_rowset, 18, 19, "bundle.dat");
    source_rowset.mutable_segment_metas(0)->set_size(10);
    source_rowset.mutable_segment_metas(1)->set_size(20);
    source_rowset.mutable_segment_metas(2)->set_size(30);
    source_rowset.mutable_segment_metas(0)->set_bundle_file_offset(0);
    source_rowset.mutable_segment_metas(1)->set_bundle_file_offset(10);
    source_rowset.mutable_segment_metas(2)->set_bundle_file_offset(30);

    RowsetOwnership ownership;
    ownership.segments.resize(3);
    ownership.segments[0] = {{true, false}, {false, false}};
    ownership.segments[1] = {{true, true}, {true, true}};
    ownership.segments[2] = {{false, true}, {false, false}};

    // tablet0 keeps segments 0,1 -> offsets 0,10.
    RowsetMetadataPB rowset_for_tablet0 = source_rowset;
    ASSERT_OK(apply_segment_ownership_to_new_tablet_rowset(&rowset_for_tablet0, ownership, 0));
    ASSERT_EQ(2, rowset_for_tablet0.segment_metas_size());
    EXPECT_EQ(0, rowset_for_tablet0.segment_metas(0).bundle_file_offset());
    EXPECT_EQ(10, rowset_for_tablet0.segment_metas(1).bundle_file_offset());

    // tablet1 keeps segments 1,2 -> offsets 10,30 (pruned middle does not shift them).
    RowsetMetadataPB rowset_for_tablet1 = source_rowset;
    ASSERT_OK(apply_segment_ownership_to_new_tablet_rowset(&rowset_for_tablet1, ownership, 1));
    ASSERT_EQ(2, rowset_for_tablet1.segment_metas_size());
    EXPECT_EQ(10, rowset_for_tablet1.segment_metas(0).bundle_file_offset());
    EXPECT_EQ(30, rowset_for_tablet1.segment_metas(1).bundle_file_offset());
}

TEST(TabletSplitterTest, ApplyOwnership_step0_shape_mismatch_aborts_unmodified) {
    RowsetMetadataPB source_rowset;
    add_bigint_seg(&source_rowset, 0, 1, "s0");
    add_bigint_seg(&source_rowset, 2, 3, "s1");
    RowsetOwnership ownership;
    ownership.segments.resize(1); // size != segment_metas_size() (2)
    ownership.segments[0] = {{true}, {false}};
    auto status = apply_segment_ownership_to_new_tablet_rowset(&source_rowset, ownership, 0);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(2, source_rowset.segment_metas_size()); // unmodified
}

// =============================================================================
// SegmentSplitInfo::load_samples_from_short_key_index -- decodes samples directly
// from a real full-key short key index (config::enable_full_sort_key_index = true
// at write time), rather than from SegmentMetadataPB.deprecated_sort_key_samples.
// BE_TEST's default SegmentWriterOptions::num_rows_per_block == 100 keeps these
// tests small (see segment_writer.h).
// =============================================================================

namespace {

VariantTuple make_int32_tuple(int32_t value) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum(value)));
    return tuple;
}

SegmentSplitInfo make_int32_seg(int32_t min_v, int32_t max_v, int64_t num_rows, int64_t data_size, uint32_t source_id) {
    SegmentSplitInfo s;
    s.min_key = make_int32_tuple(min_v);
    s.max_key = make_int32_tuple(max_v);
    s.num_rows = num_rows;
    s.data_size = data_size;
    s.source_id = source_id;
    return s;
}

SegmentSplitInfo make_int32_sampled_seg(int32_t min_v, int32_t max_v, int64_t num_rows, int64_t data_size,
                                        int64_t row_interval, const std::vector<int32_t>& sample_values,
                                        uint32_t source_id) {
    SegmentSplitInfo s = make_int32_seg(min_v, max_v, num_rows, data_size, source_id);
    s.sort_key_sample_row_interval = row_interval;
    s.sort_key_samples.reserve(sample_values.size());
    for (int32_t v : sample_values) {
        s.sort_key_samples.push_back(make_int32_tuple(v));
    }
    return s;
}

// Single INT key column + single INT value column, DUP_KEYS. num_short_key_columns=1.
std::shared_ptr<TabletSchema> make_full_sort_key_test_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_id(9001);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(65535);
    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(1);
    c0->set_name("k1");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);
    auto* c1 = schema_pb.add_column();
    c1->set_unique_id(2);
    c1->set_name("v1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(false);
    c1->set_aggregation("REPLACE");
    return TabletSchema::create(schema_pb);
}

// Writes a segment with |num_rows| rows (key column = 0..num_rows-1 in order, value
// column constant 0) to |fs| at |path| under the writer-time value of
// config::enable_full_sort_key_index, then opens it and loads its short key index.
StatusOr<std::shared_ptr<Segment>> write_and_open_int_key_segment(const std::shared_ptr<FileSystem>& fs,
                                                                  const std::string& path,
                                                                  const std::shared_ptr<TabletSchema>& schema,
                                                                  int64_t num_rows) {
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(path));
    SegmentWriterOptions opts;
    SegmentWriter writer(std::move(wfile), /*segment_id=*/0, schema, opts);
    RETURN_IF_ERROR(writer.init());

    auto chunk_schema = ChunkHelper::convert_schema(schema);
    auto chunk = ChunkFactory::new_chunk(chunk_schema, num_rows);
    auto cols = chunk->columns();
    for (int64_t i = 0; i < num_rows; ++i) {
        cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
    }
    RETURN_IF_ERROR(writer.append_chunk(*chunk));

    uint64_t file_size = 0, index_size = 0, footer_position = 0;
    RETURN_IF_ERROR(writer.finalize(&file_size, &index_size, &footer_position));

    FileInfo file_info{.path = path};
    ASSIGN_OR_RETURN(auto segment, Segment::open(fs, file_info, /*segment_id=*/0, schema));
    RETURN_IF_ERROR(segment->load_index());
    return segment;
}

} // namespace

class FullSortKeyShortKeyIndexLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _old_enable_full_sort_key_index = config::enable_full_sort_key_index;
        config::enable_full_sort_key_index = true;
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
        _schema = make_full_sort_key_test_schema();
        const auto idxes = _schema->sort_key_idxes();
        _sort_key_idxes.assign(idxes.begin(), idxes.end());
    }

    void TearDown() override { config::enable_full_sort_key_index = _old_enable_full_sort_key_index; }

    Schema schema() const { return ChunkHelper::convert_schema(_schema); }

    const std::string kDir = "/full_key_loader_test";
    std::shared_ptr<MemoryFileSystem> _fs;
    std::shared_ptr<TabletSchema> _schema;
    std::vector<uint32_t> _sort_key_idxes;
    bool _old_enable_full_sort_key_index = false;
};

// Fewer than 2 short-key blocks (entry 0 alone is row 0 == min_key, not a sample) must
// yield empty samples AND interval == 0, preserving the samples.empty() <=>
// interval==0 invariant. Covers a below-block-size segment and an exactly-one-block
// segment (num_rows == num_rows_per_block).
TEST_F(FullSortKeyShortKeyIndexLoaderTest, FewerThanTwoBlocksYieldsEmptySamplesAndZeroInterval) {
    for (int64_t num_rows : {int64_t{1}, int64_t{50}, int64_t{100}}) {
        ASSIGN_OR_ABORT(auto segment,
                        write_and_open_int_key_segment(_fs, kDir + "/seg_" + std::to_string(num_rows) + ".dat", _schema,
                                                       num_rows));
        ASSERT_TRUE(segment->has_full_sort_key_index_page());
        ASSERT_TRUE(segment->ensure_full_sort_key_index_usable());

        SegmentSplitInfo info;
        info.num_rows = num_rows;
        info.min_key = make_int32_tuple(0);
        info.max_key = make_int32_tuple(static_cast<int32_t>(num_rows > 0 ? num_rows - 1 : 0));
        Schema s = schema();
        ASSIGN_OR_ABORT(bool loaded, info.load_samples_from_short_key_index(*segment, s, _sort_key_idxes));
        EXPECT_TRUE(loaded) << "num_rows=" << num_rows;
        EXPECT_TRUE(info.sort_key_samples.empty()) << "num_rows=" << num_rows;
        EXPECT_EQ(0, info.sort_key_sample_row_interval) << "num_rows=" << num_rows;
    }
}

// Ordered rows with a partial last block (250 = 2*100 + 50): samples must start at row
// num_rows_per_block (100), be NON-DECREASING, and satisfy
// samples.size() * interval < num_rows.
TEST_F(FullSortKeyShortKeyIndexLoaderTest, OrderedRowsNonDecreasingSamplesPartialLastBlock) {
    const int64_t num_rows = 250;
    ASSIGN_OR_ABORT(auto segment, write_and_open_int_key_segment(_fs, kDir + "/seg_ordered.dat", _schema, num_rows));
    ASSERT_TRUE(segment->has_full_sort_key_index_page());
    ASSERT_TRUE(segment->ensure_full_sort_key_index_usable());
    ASSERT_EQ(100u, segment->num_rows_per_block());

    SegmentSplitInfo info;
    info.num_rows = num_rows;
    info.min_key = make_int32_tuple(0);
    info.max_key = make_int32_tuple(static_cast<int32_t>(num_rows - 1));
    Schema s = schema();
    ASSIGN_OR_ABORT(bool loaded, info.load_samples_from_short_key_index(*segment, s, _sort_key_idxes));
    ASSERT_TRUE(loaded);

    ASSERT_EQ(2u, info.sort_key_samples.size());
    EXPECT_EQ(100, info.sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, info.sort_key_samples[1][0].value().get_int32());
    EXPECT_EQ(100, info.sort_key_sample_row_interval);
    for (size_t i = 1; i < info.sort_key_samples.size(); ++i) {
        EXPECT_LE(info.sort_key_samples[i - 1].compare(info.sort_key_samples[i]), 0);
    }
    EXPECT_LT(static_cast<int64_t>(info.sort_key_samples.size()) * info.sort_key_sample_row_interval, num_rows);
}

// Exactly block-aligned (300 == 3*100, no partial tail): still 2 samples (entries
// 1 and 2; entry 0 is min_key, not a sample), at rows 100 and 200.
TEST_F(FullSortKeyShortKeyIndexLoaderTest, ExactlyBlockAligned) {
    const int64_t num_rows = 300;
    ASSIGN_OR_ABORT(auto segment, write_and_open_int_key_segment(_fs, kDir + "/seg_aligned.dat", _schema, num_rows));
    ASSERT_TRUE(segment->has_full_sort_key_index_page());
    ASSERT_TRUE(segment->ensure_full_sort_key_index_usable());

    SegmentSplitInfo info;
    info.num_rows = num_rows;
    info.min_key = make_int32_tuple(0);
    info.max_key = make_int32_tuple(static_cast<int32_t>(num_rows - 1));
    Schema s = schema();
    ASSIGN_OR_ABORT(bool loaded, info.load_samples_from_short_key_index(*segment, s, _sort_key_idxes));
    ASSERT_TRUE(loaded);

    ASSERT_EQ(2u, info.sort_key_samples.size());
    EXPECT_EQ(100, info.sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, info.sort_key_samples[1][0].value().get_int32());
    EXPECT_LT(static_cast<int64_t>(info.sort_key_samples.size()) * info.sort_key_sample_row_interval, num_rows);
}

// A full page that is present + usable but decoded with sort_key_idxes whose arity does not
// match the persisted num_sort_key_columns must return false (NOT abort) and leave
// sort_key_samples empty, so the caller falls back to metadata samples / coarse [min, max].
TEST_F(FullSortKeyShortKeyIndexLoaderTest, ArityMismatchReturnsFalseAndLeavesSamplesEmpty) {
    const int64_t num_rows = 250;
    ASSIGN_OR_ABORT(auto segment, write_and_open_int_key_segment(_fs, kDir + "/seg_arity.dat", _schema, num_rows));
    ASSERT_TRUE(segment->has_full_sort_key_index_page());
    ASSERT_TRUE(segment->ensure_full_sort_key_index_usable());
    ASSERT_EQ(1u, segment->num_sort_key_columns());

    SegmentSplitInfo info;
    info.num_rows = num_rows;
    info.min_key = make_int32_tuple(0);
    info.max_key = make_int32_tuple(static_cast<int32_t>(num_rows - 1));
    Schema s = schema();
    // Empty sort_key_idxes -> decoded arity 0 != persisted arity 1 -> runtime arity check rejects.
    std::vector<uint32_t> mismatched_idxes;
    ASSIGN_OR_ABORT(bool loaded, info.load_samples_from_short_key_index(*segment, s, mismatched_idxes));
    EXPECT_FALSE(loaded);
    EXPECT_TRUE(info.sort_key_samples.empty());
    EXPECT_EQ(0, info.sort_key_sample_row_interval);
}

// A full page that is present + usable but whose decoded samples fall outside the
// SegmentSplitInfo's [min_key, max_key] must return false (NOT abort) and leave
// sort_key_samples empty for the caller's fallback.
TEST_F(FullSortKeyShortKeyIndexLoaderTest, OutOfRangeSamplesReturnFalseAndLeaveSamplesEmpty) {
    const int64_t num_rows = 250;
    ASSIGN_OR_ABORT(auto segment, write_and_open_int_key_segment(_fs, kDir + "/seg_oor.dat", _schema, num_rows));
    ASSERT_TRUE(segment->has_full_sort_key_index_page());
    ASSERT_TRUE(segment->ensure_full_sort_key_index_usable());

    SegmentSplitInfo info;
    info.num_rows = num_rows;
    // Real samples are 100 and 200; deliberately narrow [min, max] excludes them.
    info.min_key = make_int32_tuple(500);
    info.max_key = make_int32_tuple(600);
    Schema s = schema();
    ASSIGN_OR_ABORT(bool loaded, info.load_samples_from_short_key_index(*segment, s, _sort_key_idxes));
    EXPECT_FALSE(loaded);
    EXPECT_TRUE(info.sort_key_samples.empty());
    EXPECT_EQ(0, info.sort_key_sample_row_interval);
}

// Feeding calculate_range_split_boundaries with a SegmentSplitInfo whose samples came
// from a real full-key short key index must produce IDENTICAL results to feeding it an
// equivalent SegmentSplitInfo whose samples came from the legacy
// deprecated_sort_key_samples metadata path, for the same underlying key distribution
// (250 ordered rows, sampled every 100 rows -> samples [100, 200]).
TEST_F(FullSortKeyShortKeyIndexLoaderTest, BoundariesMatchMetadataSamplePath) {
    const int64_t num_rows = 250;
    ASSIGN_OR_ABORT(auto segment, write_and_open_int_key_segment(_fs, kDir + "/seg_cmp.dat", _schema, num_rows));
    ASSERT_TRUE(segment->has_full_sort_key_index_page());
    ASSERT_TRUE(segment->ensure_full_sort_key_index_usable());

    SegmentSplitInfo from_index = make_int32_seg(0, static_cast<int32_t>(num_rows - 1), num_rows, /*data_size=*/2500,
                                                 /*source_id=*/1);
    Schema s = schema();
    ASSIGN_OR_ABORT(bool loaded, from_index.load_samples_from_short_key_index(*segment, s, _sort_key_idxes));
    ASSERT_TRUE(loaded);

    SegmentSplitInfo from_metadata = make_int32_sampled_seg(0, static_cast<int32_t>(num_rows - 1), num_rows,
                                                            /*data_size=*/2500, /*row_interval=*/100,
                                                            /*sample_values=*/{100, 200}, /*source_id=*/1);

    // A second, disjoint segment so ordered_ranges has >= 2 boundary points and a
    // 2-way split is possible.
    SegmentSplitInfo other = make_int32_seg(1000, 2000, 100, 1000, /*source_id=*/2);

    ASSIGN_OR_ABORT(auto result_index, calculate_range_split_boundaries({from_index, other}, /*target_split_count=*/2,
                                                                        /*target_value_per_split=*/200,
                                                                        /*use_num_rows=*/true,
                                                                        /*track_sources=*/true));
    ASSIGN_OR_ABORT(auto result_metadata,
                    calculate_range_split_boundaries({from_metadata, other}, /*target_split_count=*/2,
                                                     /*target_value_per_split=*/200,
                                                     /*use_num_rows=*/true, /*track_sources=*/true));

    ASSERT_EQ(result_metadata.boundaries.size(), result_index.boundaries.size());
    for (size_t i = 0; i < result_metadata.boundaries.size(); ++i) {
        EXPECT_EQ(0, result_metadata.boundaries[i].compare(result_index.boundaries[i]));
    }
    EXPECT_EQ(result_metadata.range_num_rows, result_index.range_num_rows);
    EXPECT_EQ(result_metadata.range_data_sizes, result_index.range_data_sizes);
}

// =============================================================================
// build_segments_from_rowsets plumbing: per-segment source selection when a real
// lake TabletManager is threaded through. Mirrors LakeTabletReshardTest's real-disk
// fixture pattern (be/test/storage/lake/tablet_reshard_test.cpp).
// =============================================================================

class BuildSegmentsFromRowsetsLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/tablet_splitter_loader_test";
        _location_provider = std::make_shared<FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 16384);
    }

    void TearDown() override {
        auto status = fs::remove_all(_test_dir);
        EXPECT_TRUE(status.ok() || status.is_not_found()) << status;
    }

    void prepare_tablet_dirs(int64_t tablet_id) {
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(tablet_id)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(tablet_id)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(tablet_id)));
    }

    static std::shared_ptr<TabletSchema> int_key_schema() { return make_full_sort_key_test_schema(); }

    void set_int_key_schema(TabletMetadataPB* metadata) {
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(9101);
        schema->set_num_short_key_columns(1);
        auto* c0 = schema->add_column();
        c0->set_unique_id(1);
        c0->set_name("k1");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto* c1 = schema->add_column();
        c1->set_unique_id(2);
        c1->set_name("v1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("REPLACE");
    }

    // Writes a real segment (key column 0..num_rows-1, value column constant 0) under
    // this fixture's TabletManager-resolved path for |tablet_id|/|segment_name|, under
    // the writer-time value of config::enable_full_sort_key_index. Returns file size.
    uint64_t write_int_key_segment(int64_t tablet_id, const std::string& segment_name, int64_t num_rows) {
        auto tablet_schema = int_key_schema();
        auto segment_path = _tablet_manager->segment_location(tablet_id, segment_name);
        WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wfile_or = fs::new_writable_file(fopts, segment_path);
        CHECK_OK(wfile_or.status());

        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile_or.value()), 0, tablet_schema, opts);
        CHECK_OK(writer.init());

        auto chunk_schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkFactory::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));
        return file_size;
    }

    std::string _test_dir;
    std::shared_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
};

// A real, non-null TabletManager plumbed through build_segments_from_rowsets opens the
// rowset's segment via Rowset::get_rowsets/load_segments, resolves the rowset's own
// (historical) schema and sort_key_idxes, and -- for a full-key segment -- reads its
// short key index into SegmentSplitInfo.sort_key_samples, exactly matching direct
// SegmentSplitInfo::load_samples_from_short_key_index usage.
TEST_F(BuildSegmentsFromRowsetsLoaderTest, RealFullKeySegmentPopulatesSamplesViaLoader) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);
    const int64_t num_rows = 250;
    const std::string seg_name = "seg_full_key.dat";
    const uint64_t seg_size = write_int_key_segment(tablet_id, seg_name, num_rows);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    set_int_key_schema(metadata.get());
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(seg_size);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename(seg_name);
    sm->set_size(seg_size);
    sm->set_num_rows(num_rows);

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(1u, segments.size());
    ASSERT_EQ(2u, segments[0].sort_key_samples.size());
    EXPECT_EQ(100, segments[0].sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, segments[0].sort_key_samples[1][0].value().get_int32());
    EXPECT_EQ(100, segments[0].sort_key_sample_row_interval);
}

// Tablet split is NOT read-config-gated: with config::enable_full_sort_key_index_read = false,
// build_segments_from_rowsets must STILL populate samples from a present + usable full page (the
// read config gates only query seek paths, never split-boundary precision).
TEST_F(BuildSegmentsFromRowsetsLoaderTest, ReadConfigOffStillLoadsSamplesFromFullPage) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore_write([&] { config::enable_full_sort_key_index = old_enable; });
    const bool old_read = config::enable_full_sort_key_index_read;
    config::enable_full_sort_key_index_read = false; // split must ignore this
    DeferOp restore_read([&] { config::enable_full_sort_key_index_read = old_read; });

    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);
    const int64_t num_rows = 250;
    const std::string seg_name = "seg_read_off.dat";
    const uint64_t seg_size = write_int_key_segment(tablet_id, seg_name, num_rows);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    set_int_key_schema(metadata.get());
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(seg_size);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename(seg_name);
    sm->set_size(seg_size);
    sm->set_num_rows(num_rows);
    make_int32_tuple(0).to_proto(sm->mutable_sort_key_min());
    make_int32_tuple(static_cast<int32_t>(num_rows - 1)).to_proto(sm->mutable_sort_key_max());

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(1u, segments.size());
    ASSERT_EQ(2u, segments[0].sort_key_samples.size());
    EXPECT_EQ(100, segments[0].sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, segments[0].sort_key_samples[1][0].value().get_int32());
    EXPECT_EQ(100, segments[0].sort_key_sample_row_interval);
}

// A legacy (non full-key) segment falls back to the pre-existing
// deprecated_sort_key_samples metadata path even when a real TabletManager
// successfully opens the segment file.
TEST_F(BuildSegmentsFromRowsetsLoaderTest, LegacySegmentFallsBackToDeprecatedSamples) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = false;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);
    const int64_t num_rows = 250;
    const std::string seg_name = "seg_legacy.dat";
    const uint64_t seg_size = write_int_key_segment(tablet_id, seg_name, num_rows);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    set_int_key_schema(metadata.get());
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(seg_size);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename(seg_name);
    sm->set_size(seg_size);
    sm->set_num_rows(num_rows);
    sm->set_deprecated_sort_key_sample_row_interval(100);
    make_int32_tuple(100).to_proto(sm->add_deprecated_sort_key_samples());
    make_int32_tuple(200).to_proto(sm->add_deprecated_sort_key_samples());

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(1u, segments.size());
    ASSERT_EQ(2u, segments[0].sort_key_samples.size());
    EXPECT_EQ(100, segments[0].sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, segments[0].sort_key_samples[1][0].value().get_int32());
    EXPECT_EQ(100, segments[0].sort_key_sample_row_interval);
}

// A skipped/ignored/lost segment (Rowset::LoadedSegment::segment == nullptr, produced
// here via a missing segment file + experimental_lake_ignore_lost_segment=true) must
// fall back to coarse (empty samples, interval 0) WITHOUT dereferencing the null
// segment -- while a sibling real full-key segment in the SAME rowset still gets its
// samples from the loader.
TEST_F(BuildSegmentsFromRowsetsLoaderTest, LostSegmentFallsBackWithoutDereferencingNull) {
    const bool old_enable_full_key = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore_full_key([&] { config::enable_full_sort_key_index = old_enable_full_key; });
    const bool old_ignore_lost = config::experimental_lake_ignore_lost_segment;
    config::experimental_lake_ignore_lost_segment = true;
    DeferOp restore_ignore_lost([&] { config::experimental_lake_ignore_lost_segment = old_ignore_lost; });

    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);
    const int64_t num_rows = 250;
    const std::string present_seg_name = "seg_present.dat";
    const uint64_t present_seg_size = write_int_key_segment(tablet_id, present_seg_name, num_rows);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    set_int_key_schema(metadata.get());
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(present_seg_size);

    auto* sm_present = rowset->add_segment_metas();
    sm_present->set_filename(present_seg_name);
    sm_present->set_size(present_seg_size);
    sm_present->set_num_rows(num_rows);

    // Never written to disk; with experimental_lake_ignore_lost_segment=true this
    // becomes a null LoadedSegment placeholder instead of a hard load error.
    auto* sm_lost = rowset->add_segment_metas();
    sm_lost->set_filename("seg_missing.dat");
    sm_lost->set_size(100);
    sm_lost->set_num_rows(10);

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(2u, segments.size());

    // segments[0]: the real, present full-key segment -- still gets samples.
    ASSERT_EQ(2u, segments[0].sort_key_samples.size());
    EXPECT_EQ(100, segments[0].sort_key_sample_row_interval);

    // segments[1]: the lost segment -- coarse fallback, no crash.
    EXPECT_TRUE(segments[1].sort_key_samples.empty());
    EXPECT_EQ(0, segments[1].sort_key_sample_row_interval);
    EXPECT_EQ(10, segments[1].num_rows);
}

// Regression for the split-reader crash: metadata carrying a sample-less segment but an
// UNSET schema id (as synthetic reshard metadata routinely produces) must degrade to the
// coarse path, NOT abort in the Rowset ctor's GlobalTabletSchemaMap::emplace, which
// asserts DCHECK_NE(TabletSchema::invalid_id(), id). A real, non-null TabletManager is
// supplied; the schema gate must reject the loader before the Rowset is constructed, so
// no file I/O and no abort occur (no segment is written to disk here).
TEST_F(BuildSegmentsFromRowsetsLoaderTest, UnsetSchemaIdDegradesToCoarseWithoutAbort) {
    const int64_t tablet_id = next_id();
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    // Schema left entirely unset -> schema().id() == TabletSchema::invalid_id().
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename("never_written.dat");
    sm->set_size(100);
    sm->set_num_rows(10);
    make_int32_tuple(0).to_proto(sm->mutable_sort_key_min());
    make_int32_tuple(9).to_proto(sm->mutable_sort_key_max());

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(1u, segments.size());
    EXPECT_TRUE(segments[0].sort_key_samples.empty());
    EXPECT_EQ(0, segments[0].sort_key_sample_row_interval);
    EXPECT_EQ(0, segments[0].min_key[0].value().get_int32());
    EXPECT_EQ(9, segments[0].max_key[0].value().get_int32());
}

// A present, non-full-key segment that carries NO deprecated_sort_key_samples: the
// sample-less perf gate lets the loader open it, the segment is found to be legacy
// (no full sort key index), and it degrades to the coarse [min, max] path -- empty
// samples, interval 0, with the metadata bounds preserved.
TEST_F(BuildSegmentsFromRowsetsLoaderTest, OpenedNonFullKeySegmentWithoutSamplesFallsBackToCoarse) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = false;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);
    const int64_t num_rows = 250;
    const std::string seg_name = "seg_coarse.dat";
    const uint64_t seg_size = write_int_key_segment(tablet_id, seg_name, num_rows);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    set_int_key_schema(metadata.get());
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(seg_size);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename(seg_name);
    sm->set_size(seg_size);
    sm->set_num_rows(num_rows);
    // Coarse bounds only, NO deprecated samples -> the perf gate cannot skip the loader,
    // so the segment is genuinely opened and only then found to be legacy.
    make_int32_tuple(0).to_proto(sm->mutable_sort_key_min());
    make_int32_tuple(249).to_proto(sm->mutable_sort_key_max());

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(1u, segments.size());
    EXPECT_TRUE(segments[0].sort_key_samples.empty());
    EXPECT_EQ(0, segments[0].sort_key_sample_row_interval);
    EXPECT_EQ(0, segments[0].min_key[0].value().get_int32());
    EXPECT_EQ(249, segments[0].max_key[0].value().get_int32());
}

// Two rowsets, each mapped through rowset_to_schema to its OWN historical schema, both
// backed by real full-key segments of different row counts. build_segments_from_rowsets
// must resolve each rowset's schema independently (from historical_schemas) and decode
// each segment's short key index with it. The top-level schema().id() is deliberately
// cleared so that a wrong fallback to it -- rather than the per-rowset historical schema
// -- would trip the invalid-id gate and skip the loader, failing these assertions.
TEST_F(BuildSegmentsFromRowsetsLoaderTest, TwoRowsetsDistinctHistoricalSchemasDecodeIndependently) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);
    const std::string seg0 = "seg_hist0.dat";
    const std::string seg1 = "seg_hist1.dat";
    const uint64_t size0 = write_int_key_segment(tablet_id, seg0, 250);
    const uint64_t size1 = write_int_key_segment(tablet_id, seg1, 350);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    set_int_key_schema(metadata.get());
    // No top-level schema id: forces resolution through the per-rowset historical schemas.
    metadata->mutable_schema()->clear_id();

    auto& hist = *metadata->mutable_historical_schemas();
    hist[5001] = metadata->schema();
    hist[5001].set_id(5001);
    hist[5002] = metadata->schema();
    hist[5002].set_id(5002);
    auto& r2s = *metadata->mutable_rowset_to_schema();

    auto* rowset0 = metadata->add_rowsets();
    rowset0->set_id(10);
    rowset0->set_num_rows(250);
    rowset0->set_data_size(size0);
    auto* sm0 = rowset0->add_segment_metas();
    sm0->set_filename(seg0);
    sm0->set_size(size0);
    sm0->set_num_rows(250);
    r2s[10] = 5001;

    auto* rowset1 = metadata->add_rowsets();
    rowset1->set_id(11);
    rowset1->set_num_rows(350);
    rowset1->set_data_size(size1);
    auto* sm1 = rowset1->add_segment_metas();
    sm1->set_filename(seg1);
    sm1->set_size(size1);
    sm1->set_num_rows(350);
    r2s[11] = 5002;

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, &segments));
    ASSERT_EQ(2u, segments.size());

    // rowset0 (schema 5001, 250 rows): samples at rows 100, 200.
    ASSERT_EQ(2u, segments[0].sort_key_samples.size());
    EXPECT_EQ(100, segments[0].sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, segments[0].sort_key_samples[1][0].value().get_int32());

    // rowset1 (schema 5002, 350 rows): samples at rows 100, 200, 300 -- decoded with its
    // own schema, proving per-rowset resolution rather than a shared/top-level schema.
    ASSERT_EQ(3u, segments[1].sort_key_samples.size());
    EXPECT_EQ(100, segments[1].sort_key_samples[0][0].value().get_int32());
    EXPECT_EQ(200, segments[1].sort_key_samples[1][0].value().get_int32());
    EXPECT_EQ(300, segments[1].sort_key_samples[2][0].value().get_int32());
}

// tablet_manager == nullptr (synthetic metadata-only callers, e.g. every other test in
// this file) must skip the loader entirely and source purely from
// deprecated_sort_key_samples / coarse, without attempting any file I/O.
TEST(TabletSplitterTest, BuildSegmentsFromRowsets_NullTabletManagerSkipsLoader) {
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(1);
    metadata->set_version(1);
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(1);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename("never_written.dat");
    sm->set_size(100);
    sm->set_num_rows(10);
    sm->mutable_sort_key_min()->CopyFrom(make_bigint_tuple_pb(0));
    sm->mutable_sort_key_max()->CopyFrom(make_bigint_tuple_pb(9));

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(/*tablet_manager=*/nullptr, metadata, &segments));
    ASSERT_EQ(1u, segments.size());
    EXPECT_TRUE(segments[0].sort_key_samples.empty());
    EXPECT_EQ(0, segments[0].sort_key_sample_row_interval);
}

} // namespace starrocks::lake
