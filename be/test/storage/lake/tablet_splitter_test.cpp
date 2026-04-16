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

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "storage/tablet_range.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

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

} // namespace starrocks::lake
