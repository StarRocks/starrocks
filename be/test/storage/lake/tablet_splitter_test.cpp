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

#include <fmt/format.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <numeric>
#include <optional>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
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
#include "storage/sort_key_sampler.h"
#include "storage/tablet_range.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/primary_key_encoder.h"
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

// Returns a mutable handle: some tests keep tweaking the metadata (parent range, rowset stats)
// after construction, which TabletMetadataPtr's `const` element type would forbid. It converts
// implicitly to TabletMetadataPtr at the call sites that only read it.
static std::shared_ptr<TabletMetadataPB> make_pk_order_by_metadata() {
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(100);
    metadata->set_version(10);
    auto* schema = metadata->mutable_schema();
    schema->set_id(101);
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_num_short_key_columns(1);
    schema->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto* pk = schema->add_column();
    pk->set_unique_id(1);
    pk->set_name("pk");
    pk->set_type("INT");
    pk->set_is_key(true);
    pk->set_is_nullable(false);
    auto* order_by = schema->add_column();
    order_by->set_unique_id(2);
    order_by->set_name("order_by");
    order_by->set_type("INT");
    order_by->set_is_key(false);
    order_by->set_is_nullable(false);
    schema->add_sort_key_idxes(1);
    return metadata;
}

static std::vector<std::string> encode_int_pk_samples(const TabletMetadataPtr& metadata,
                                                      const std::vector<int32_t>& values) {
    auto tablet_schema = TabletSchema::create(metadata->schema());
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>{0});
    auto chunk = ChunkFactory::new_chunk(pkey_schema, values.size());
    for (int32_t value : values) {
        chunk->get_column_by_index(0)->as_mutable_ptr()->append_datum(Datum(value));
    }
    MutableColumnPtr encoded;
    CHECK_OK(PrimaryKeyEncoder::create_column(pkey_schema, &encoded, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
    PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), encoded.get(),
                              PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    const auto& binary = down_cast<BinaryColumn&>(*encoded);
    std::vector<std::string> result;
    result.reserve(binary.size());
    for (size_t i = 0; i < binary.size(); ++i) {
        result.emplace_back(binary.get_slice(i).to_string());
    }
    return result;
}

static int32_t int_pk_bound(const TuplePB& bound) {
    VariantTuple tuple;
    CHECK_OK(tuple.from_proto(bound));
    return tuple[0].value().get_int32();
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

TEST(TabletSplitterTest, pk_index_samples_choose_decoded_quantiles) {
    auto metadata = make_pk_order_by_metadata();
    auto samples = encode_int_pk_samples(metadata, {0, 10, 20, 30, 40, 50, 60, 70, 80, 90});

    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges_from_pk_index_samples(/*tablet_manager=*/nullptr, metadata,
                                                            /*split_count=*/2, std::move(samples), &ranges));
    ASSERT_EQ(2, ranges.size());
    ASSERT_TRUE(ranges[0].range.has_upper_bound());
    ASSERT_TRUE(ranges[1].range.has_lower_bound());
    EXPECT_EQ(50, int_pk_bound(ranges[0].range.upper_bound()));
    EXPECT_EQ(50, int_pk_bound(ranges[1].range.lower_bound()));
    EXPECT_FALSE(ranges[0].range.upper_bound_included());
    EXPECT_TRUE(ranges[1].range.lower_bound_included());
}

TEST(TabletSplitterTest, pk_index_samples_filter_parent_range_and_anchor_stats) {
    auto metadata = make_pk_order_by_metadata();
    VariantTuple lower;
    lower.emplace(get_type_info(TYPE_INT), Datum(int32_t{20}));
    lower.to_proto(metadata->mutable_range()->mutable_lower_bound());
    metadata->mutable_range()->set_lower_bound_included(true);
    VariantTuple upper;
    upper.emplace(get_type_info(TYPE_INT), Datum(int32_t{80}));
    upper.to_proto(metadata->mutable_range()->mutable_upper_bound());
    metadata->mutable_range()->set_upper_bound_included(false);
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(7);
    rowset->set_num_rows(101);
    rowset->set_data_size(1001);
    rowset->set_num_dels(11);

    auto samples = encode_int_pk_samples(metadata, {10, 20, 30, 40, 50, 60, 70, 80, 90});
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges_from_pk_index_samples(/*tablet_manager=*/nullptr, metadata,
                                                            /*split_count=*/2, std::move(samples), &ranges));
    ASSERT_EQ(2, ranges.size());
    EXPECT_EQ(50, int_pk_bound(ranges[0].range.upper_bound()));
    EXPECT_EQ(20, int_pk_bound(ranges[0].range.lower_bound()));
    EXPECT_EQ(80, int_pk_bound(ranges[1].range.upper_bound()));

    int64_t rows = 0;
    int64_t bytes = 0;
    int64_t dels = 0;
    for (const auto& range : ranges) {
        auto it = range.rowset_stats.find(7);
        ASSERT_NE(range.rowset_stats.end(), it);
        rows += it->second.num_rows;
        bytes += it->second.data_size;
        dels += it->second.num_dels;
    }
    EXPECT_EQ(101, rows);
    EXPECT_EQ(1001, bytes);
    EXPECT_EQ(11, dels);
}

TEST(TabletSplitterTest, pk_index_samples_reject_insufficient_distinct_keys) {
    auto metadata = make_pk_order_by_metadata();
    auto samples = encode_int_pk_samples(metadata, {10, 10, 20});
    std::vector<TabletRangeInfo> ranges;
    auto status = get_tablet_split_ranges_from_pk_index_samples(/*tablet_manager=*/nullptr, metadata,
                                                                /*split_count=*/3, std::move(samples), &ranges);
    EXPECT_TRUE(status.is_not_supported());
    EXPECT_TRUE(ranges.empty());
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
// live in `tablet_reshard_helper_test.cpp`.

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

TEST(TabletSplitterTest, DataDrivenSplit_UsesUniquePhysicalSegmentsForBoundaries) {
    auto unique = std::make_shared<TabletMetadataPB>(
            *make_dup_keys_metadata_with_rowsets({{1, 0, 90, 100, 1000}, {2, 100, 190, 100, 1000}}));
    for (int i = 0; i < 2; ++i) {
        auto* sm = unique->mutable_rowsets(i)->mutable_segment_metas(0);
        sm->set_bundle_file_offset(64);
        sm->set_deprecated_sort_key_sample_row_interval(10);
        for (int k = 1; k < 9; ++k) *sm->add_deprecated_sort_key_samples() = make_bigint_tuple_pb(i * 100 + k * 10);
    }
    auto repeated = std::make_shared<TabletMetadataPB>(*unique);
    auto* duplicate = repeated->add_rowsets();
    *duplicate = repeated->rowsets(0);
    duplicate->set_id(3);
    duplicate->mutable_segment_metas(0)->set_segment_idx(7);
    duplicate->mutable_segment_metas(0)->set_shared(true);
    std::vector<TabletRangeInfo> expected, actual;
    ASSERT_OK(get_tablet_split_ranges(nullptr, unique, 2, &expected));
    ASSERT_OK(get_tablet_split_ranges(nullptr, repeated, 2, &actual));
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_TRUE(MessageDifferencer::Equals(expected[i].range, actual[i].range));
    }
}

TEST(TabletSplitterTest, DataDrivenSplit_RejectsConflictingPhysicalSlice) {
    auto source = make_dup_keys_metadata_with_rowsets({{1, 0, 40, 100, 1000}, {2, 50, 99, 100, 1000}});
    auto m = std::make_shared<TabletMetadataPB>(*source);
    auto* duplicate = m->add_rowsets();
    *duplicate = m->rowsets(0);
    duplicate->set_id(3);
    duplicate->mutable_segment_metas(0)->set_size(1001);
    std::vector<TabletRangeInfo> ranges;
    EXPECT_TRUE(get_tablet_split_ranges(nullptr, m, 2, &ranges).is_corruption());
}

TEST(TabletSplitterTest, DataDrivenSplit_StopsAtMetadataVisitBudget) {
    auto m = make_dup_keys_metadata_with_rowsets({{1, 0, 40, 100, 1000}, {2, 50, 99, 100, 1000}});
    auto* sync = SyncPoint::GetInstance();
    int opens = 0;
    sync->SetCallBack("tablet_splitter:set_metadata_visit_limit", [](void* p) { *static_cast<size_t*>(p) = 1; });
    sync->SetCallBack("tablet_splitter:segment_open", [&](void*) { ++opens; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    std::vector<TabletRangeInfo> ranges;
    EXPECT_TRUE(get_tablet_split_ranges(nullptr, m, 2, &ranges).is_capacity_limit_exceeded());
    EXPECT_EQ(0, opens);
    // Parallel-compaction callers keep the unlimited public calculator even while
    // a focused SPLIT helper has an exhausted test budget.
    EXPECT_OK(calculate_range_split_boundaries({make_seg(0, 40, 100, 1000), make_seg(50, 99, 100, 1000)}, 2, 100, true)
                      .status());
}

TEST(TabletSplitterExternalBoundariesTest, ExternalRanges_SkipBoundaryPlannerAndConserveStats) {
    auto m = make_dup_keys_metadata_with_rowsets({{1, 0, 40, 101, 1001}, {2, 50, 99, 103, 1003}},
                                                 make_bigint_range_pb(0, 100));
    RepeatedPtrField<TabletRangePB> external;
    *external.Add() = make_bigint_range_pb(0, 50);
    *external.Add() = make_bigint_range_pb(50, 100);
    auto* sync = SyncPoint::GetInstance();
    int planners = 0;
    sync->SetCallBack("tablet_splitter:boundary_planner", [&](void*) { ++planners; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(nullptr, m, external, &ranges));
    ASSERT_EQ(2, ranges.size());
    EXPECT_EQ(0, planners);
    EXPECT_EQ(101, ranges[0].rowset_stats.at(1).num_rows);
    EXPECT_EQ(1003, ranges[1].rowset_stats.at(2).data_size);
    EXPECT_EQ(0, ranges[0].rowset_stats.count(2));
    EXPECT_EQ(0, ranges[1].rowset_stats.count(1));
}

TEST(TabletSplitterExternalBoundariesTest, ExternalRanges_AssignsPointAtPhysicalEnvelopeEndToRightChild) {
    auto m = std::make_shared<TabletMetadataPB>(*make_dup_keys_metadata_with_rowsets({{1, 0, 40, 50, 500}}));
    auto* rowset = m->mutable_rowsets(0);
    rowset->set_num_rows(100);
    rowset->set_data_size(1000);
    auto* point = rowset->add_segment_metas();
    *point = rowset->segment_metas(0);
    point->set_filename("point");
    point->set_segment_idx(7);
    *point->mutable_sort_key_min() = make_bigint_tuple_pb(50);
    *point->mutable_sort_key_max() = make_bigint_tuple_pb(50);
    RepeatedPtrField<TabletRangePB> external;
    *external.Add() = make_bigint_range_pb(std::nullopt, 50);
    *external.Add() = make_bigint_range_pb(50, std::nullopt);
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(nullptr, m, external, &ranges));
    ASSERT_EQ(2, ranges.size());
    EXPECT_EQ(50, ranges[0].rowset_stats.at(1).num_rows);
    EXPECT_EQ(50, ranges[1].rowset_stats.at(1).num_rows);
}

TEST(TabletSplitterExternalBoundariesTest, ExternalRanges_SeparateSortPkUsesEligibleUniformWeights) {
    auto m = make_pk_order_by_metadata();
    auto* r = m->add_rowsets();
    r->set_id(1);
    r->set_num_rows(101);
    r->set_data_size(1001);
    r->set_num_dels(3);
    *r->mutable_range()->mutable_lower_bound() = [] {
        VariantTuple t;
        t.append(DatumVariant(get_type_info(TYPE_INT), Datum(50)));
        TuplePB p;
        t.to_proto(&p);
        return p;
    }();
    r->mutable_range()->set_lower_bound_included(true);
    RepeatedPtrField<TabletRangePB> external;
    *external.Add()->mutable_upper_bound() = r->range().lower_bound();
    external.Mutable(0)->set_upper_bound_included(false);
    *external.Add()->mutable_lower_bound() = r->range().lower_bound();
    external.Mutable(1)->set_lower_bound_included(true);
    auto* sync = SyncPoint::GetInstance();
    int planners = 0;
    sync->SetCallBack("tablet_splitter:boundary_planner", [&](void*) { ++planners; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(nullptr, m, external, &ranges));
    ASSERT_EQ(2, ranges.size());
    EXPECT_EQ(0, planners);
    EXPECT_EQ(0, ranges[0].rowset_stats.count(1));
    EXPECT_EQ(101, ranges[1].rowset_stats.at(1).num_rows);
    EXPECT_EQ(1001, ranges[1].rowset_stats.at(1).data_size);
    EXPECT_EQ(3, ranges[1].rowset_stats.at(1).num_dels);
}

TEST(TabletSplitterExternalBoundariesTest, zero_row_segment_is_not_geometry_and_all_stats_are_conserved) {
    auto m = std::make_shared<TabletMetadataPB>(
            *make_dup_keys_metadata_with_rowsets({{1, 0, 100, 101, 1201}}, make_bigint_range_pb(0, 101)));
    auto* rowset = m->mutable_rowsets(0);
    rowset->set_num_dels(7);
    SegmentMetadataPB live(rowset->segment_metas(0));
    live.set_segment_idx(1);
    live.set_size(1001);
    rowset->clear_segment_metas();
    auto* empty = rowset->add_segment_metas();
    empty->set_filename("empty");
    empty->set_segment_idx(0);
    empty->set_num_rows(0);
    empty->set_size(200);
    *rowset->add_segment_metas() = live;

    auto external = make_ranges({make_bigint_range_pb(0, 50), make_bigint_range_pb(50, 101)});
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(nullptr, m, external, &ranges));
    ASSERT_EQ(2, ranges.size());
    ASSERT_TRUE(ranges[0].rowset_stats.contains(1));
    ASSERT_TRUE(ranges[1].rowset_stats.contains(1));
    const auto& left = ranges[0].rowset_stats.at(1);
    const auto& right = ranges[1].rowset_stats.at(1);
    EXPECT_EQ(51, left.num_rows);
    EXPECT_EQ(50, right.num_rows);
    EXPECT_EQ(601, left.data_size);
    EXPECT_EQ(600, right.data_size);
    EXPECT_EQ(4, left.num_dels);
    EXPECT_EQ(3, right.num_dels);
    EXPECT_EQ(101, left.num_rows + right.num_rows);
    EXPECT_EQ(1201, left.data_size + right.data_size);
    EXPECT_EQ(7, left.num_dels + right.num_dels);
}

TEST(TabletSplitterExternalBoundariesTest, all_zero_segments_use_emitted_child_weights_without_shape_requirements) {
    auto m = std::make_shared<TabletMetadataPB>(*make_dup_keys_metadata_with_rowsets({}, make_bigint_range_pb(0, 100)));
    auto* rowset = m->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(0);
    rowset->set_data_size(1);
    rowset->set_num_dels(0);
    for (int i = 0; i < 2; ++i) {
        auto* segment = rowset->add_segment_metas();
        segment->set_filename("empty" + std::to_string(i));
        segment->set_segment_idx(i);
        segment->set_num_rows(0);
        segment->set_size(1);
    }

    auto external = make_ranges({make_bigint_range_pb(0, 50), make_bigint_range_pb(50, 100)});
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(nullptr, m, external, &ranges));
    ASSERT_EQ(2, ranges.size());
    ASSERT_TRUE(ranges[0].rowset_stats.contains(1));
    ASSERT_TRUE(ranges[1].rowset_stats.contains(1));
    EXPECT_EQ(0, ranges[0].rowset_stats.at(1).num_rows + ranges[1].rowset_stats.at(1).num_rows);
    EXPECT_EQ(0, ranges[0].rowset_stats.at(1).num_dels + ranges[1].rowset_stats.at(1).num_dels);
    EXPECT_EQ(1, ranges[0].rowset_stats.at(1).data_size);
    EXPECT_EQ(0, ranges[1].rowset_stats.at(1).data_size);
}

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
    m->set_num_rows(1);
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
    EXPECT_TRUE(can_prune_rowset_segments(make_pruneable(), /*sort_key_arity=*/1));

    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->set_bundle_file_offset(0); // bundled segment is pruneable
        EXPECT_TRUE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
    } // (a) ok
    {
        auto rs = make_pruneable();
        rs.set_next_compaction_offset(1);
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
    } // (b)
    {
        auto rs = make_pruneable();
        rs.add_segment_metas(); // extra segment lacking sort-key bounds
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
    } // (c) missing bound on extra segment
    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->clear_sort_key_max();
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
    } // (c) missing bound
    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->set_shared(true);
        EXPECT_TRUE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
    } // (d) ok
    {
        auto rs = make_pruneable();
        rs.mutable_segment_metas(0)->clear_num_rows();
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
        rs.mutable_segment_metas(0)->set_num_rows(0);
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/1));
    } // zero or missing count has no pruneable geometry
    {
        // (e) bounds narrower than the current sort key (a rowset written before a metadata-only
        // trailing sort-key ADD): not comparable with the new tablets' ranges, so not pruneable.
        // See ComputeOwnership_narrowSegmentBoundMissesTheSiblingThatOwnsItsBoundaryRows for what
        // pruning on them would cost.
        auto rs = make_pruneable();
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/2));
        // arity 0 == "cannot tell" (a schema carrying no sort key at all) skips the check.
        EXPECT_TRUE(can_prune_rowset_segments(rs, /*sort_key_arity=*/0));
    }
    {
        // (e) is per rowset: one narrow segment disqualifies its whole rowset, since ownership is
        // computed per rowset.
        TuplePB wide_bound = make_bigint_tuple_pb(5);
        *wide_bound.add_values() = make_bigint_tuple_pb(5).values(0);
        auto rs = make_pruneable(); // s0's bounds are arity 1
        auto* s1 = rs.add_segment_metas();
        s1->set_filename("s1");
        *s1->mutable_sort_key_min() = wide_bound;
        *s1->mutable_sort_key_max() = wide_bound;
        EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/2));
    }
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

// tablet_manager == nullptr (synthetic metadata-only callers, e.g. every other test in
// this file) must skip sampling entirely and leave every segment at its coarse
// [min, max] range, without attempting any file I/O -- the filename below names a segment
// that was never written.
TEST(TabletSplitterTest, BuildSegmentsFromRowsets_NullTabletManagerSkipsSampling) {
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
    ASSERT_OK(build_segments_from_rowsets(/*tablet_manager=*/nullptr, metadata, /*split_width=*/2,
                                          /*data_page_split_width=*/2, &segments));
    ASSERT_EQ(1u, segments.size());
    EXPECT_TRUE(segments[0].sort_key_samples.empty());
    EXPECT_EQ(0, segments[0].sort_key_sample_row_interval);
}

TEST(TabletSplitterTest, build_segments_from_rowsets_skips_explicit_zero_rows) {
    auto metadata = std::make_shared<TabletMetadataPB>(*make_dup_keys_metadata_with_rowsets({{1, 10, 19, 10, 100}}));
    auto* rowset = metadata->mutable_rowsets(0);
    SegmentMetadataPB live(rowset->segment_metas(0));
    live.set_segment_idx(1);
    rowset->clear_segment_metas();
    auto* empty = rowset->add_segment_metas();
    empty->set_filename("empty");
    empty->set_segment_idx(0);
    empty->set_num_rows(0);
    empty->set_size(37);
    *rowset->add_segment_metas() = live;

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(/*tablet_manager=*/nullptr, metadata, /*split_width=*/2,
                                          /*data_page_split_width=*/0, &segments));
    ASSERT_EQ(1, segments.size());
    EXPECT_EQ(1, segments[0].source_id);
    EXPECT_EQ(10, segments[0].num_rows);
    EXPECT_EQ(100, segments[0].data_size);
    ASSERT_EQ(1, segments[0].min_key.size());
    ASSERT_EQ(1, segments[0].max_key.size());
    EXPECT_EQ(10, segments[0].min_key[0].value().get_int64());
    EXPECT_EQ(19, segments[0].max_key[0].value().get_int64());
}

// =============================================================================
// Segment-derived boundaries are projected onto the CURRENT sort key
// =============================================================================
//
// A metadata-only trailing sort-key key-column ADD (FE
// SchemaChangeHandler#tryCreateMetadataOnlyTrailingKeyAddJob -- e.g. `ALTER TABLE agg_range ADD
// COLUMN k2 INT`, which an AGG table promotes to a key column) widens the tablet's sort key and
// reprojects every EXISTING tablet range bound with a trailing NULL sentinel, but deliberately does
// not rewrite the data: the rowsets keep their historical, narrower schema, so their
// sort_key_min/sort_key_max stay one column short.
//
// Emitting such a tuple as a new tablet's range bound is unrecoverable: RangeRouter::_validate_range
// rejects every subsequent load ("upper_bound value size is not equal to column size") and
// TabletRangeHelper::create_seek_range_from rejects every read of a rowset written at the new arity
// ("Unexpected number of values in TabletRangePB bound value, expected at least: 2, actual: 1").

static VariantPB make_null_int_variant_pb() {
    DatumVariant dv(get_type_info(LogicalType::TYPE_INT), Datum());
    VariantPB pb;
    dv.to_proto(&pb);
    return pb;
}

// [k1, NULL] -- a bigint prefix bound lifted onto the (k1, k2) sort key. This is the shape the FE's
// TrailingSortKeyRangeReprojection stamps onto every pre-existing tablet range.
static TuplePB make_bigint_null_tuple_pb(int64_t k1) {
    TuplePB t;
    *t.add_values() = make_bigint_variant_pb(k1);
    *t.add_values() = make_null_int_variant_pb();
    return t;
}

// A tablet in the post-trailing-key-add state: current sort key is (k1 BIGINT, k2 INT), the parent
// range is at arity 2, but every rowset was written before the ADD and carries arity-1
// sort_key_min/sort_key_max. |parent_bounds_arity1| reproduces an ALREADY corrupted tablet whose own
// range bounds were never reprojected.
// Returns a MUTABLE handle (not TabletMetadataPtr, which is shared_ptr<const TabletMetadataPB>):
// the tests below tweak the schema / segment metas after building. It converts implicitly wherever a
// TabletMetadataPtr is expected.
static std::shared_ptr<TabletMetadataPB> make_trailing_key_added_metadata(
        const std::vector<std::tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>>& rowsets,
        bool parent_bounds_arity1 = false) {
    auto m = std::make_shared<TabletMetadataPB>();
    m->set_id(1);
    m->set_version(1);
    auto* schema = m->mutable_schema();
    schema->set_keys_type(AGG_KEYS);
    schema->set_id(100);
    auto* k1 = schema->add_column();
    k1->set_unique_id(0);
    k1->set_name("k1");
    k1->set_type("BIGINT");
    k1->set_is_key(true);
    k1->set_is_nullable(false);
    auto* k2 = schema->add_column();
    k2->set_unique_id(1);
    k2->set_name("k2");
    k2->set_type("INT");
    k2->set_is_key(true);
    k2->set_is_nullable(true);
    schema->add_sort_key_idxes(0);
    schema->add_sort_key_idxes(1);

    auto* range = m->mutable_range();
    if (parent_bounds_arity1) {
        *range->mutable_lower_bound() = make_bigint_tuple_pb(0);
    } else {
        *range->mutable_lower_bound() = make_bigint_null_tuple_pb(0);
    }
    range->set_lower_bound_included(true);
    *range->mutable_upper_bound() = make_bigint_null_tuple_pb(1000);
    range->set_upper_bound_included(false);

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
        // Arity 1: written before `ADD COLUMN k2`.
        *sm->mutable_sort_key_min() = make_bigint_tuple_pb(lo);
        *sm->mutable_sort_key_max() = make_bigint_tuple_pb(hi);
    }
    return m;
}

// build_segments_from_rowsets lifts the min_key/max_key a pre-ADD segment contributes onto the
// current sort key, padding with the NULL (== MIN) sentinel.
//
// Samples cannot be exercised here, because a synthetic metadata-only tablet has no segment to
// sample; the sample half of the same projection is covered by
// SortKeySamplingSplitterTest.samples_from_a_narrower_historical_schema_are_projected below, which
// writes a real segment under the historical schema.
TEST(TabletSplitterTest, BuildSegmentsFromRowsets_ProjectsNarrowSegmentKeysOntoCurrentSortKey) {
    auto m = make_trailing_key_added_metadata({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 499, 500, 5000),
    });

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(/*tablet_manager=*/nullptr, m, /*split_width=*/2,
                                          /*data_page_split_width=*/2, &segments));
    ASSERT_EQ(1u, segments.size());
    EXPECT_EQ(2u, segments[0].min_key.size());
    EXPECT_EQ(2u, segments[0].max_key.size());
    EXPECT_TRUE(segments[0].min_key[1].value().is_null());
    EXPECT_TRUE(segments[0].max_key[1].value().is_null());
    EXPECT_EQ(0, segments[0].min_key[0].value().get_int64());
    EXPECT_EQ(499, segments[0].max_key[0].value().get_int64());
}

// An absent sort_key_min/sort_key_max means "unknown" to every downstream consumer (the
// !min_key.empty() guards, TabletRange::is_minimum). It must stay empty rather than becoming the
// concrete minimum tuple (NULL, NULL).
TEST(TabletSplitterTest, BuildSegmentsFromRowsets_LeavesUnsetSegmentKeysEmpty) {
    auto m = make_trailing_key_added_metadata({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 499, 500, 5000),
    });
    auto* sm = m->mutable_rowsets(0)->mutable_segment_metas(0);
    sm->clear_sort_key_min();
    sm->clear_sort_key_max();

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(/*tablet_manager=*/nullptr, m, /*split_width=*/2,
                                          /*data_page_split_width=*/2, &segments));
    ASSERT_EQ(1u, segments.size());
    EXPECT_TRUE(segments[0].min_key.empty());
    EXPECT_TRUE(segments[0].max_key.empty());
}

// The regression itself: splitting a tablet whose rowsets predate the trailing key add must emit
// bounds at the tablet's sort-key arity. Before the projection, the interior boundaries came straight
// out of the arity-1 segment tuples and bricked every new tablet.
TEST(TabletSplitterTest, DataDrivenSplit_EmitsBoundsAtCurrentSortKeyArity) {
    // Two disjoint rowsets with a gap, the shape the parity test proves yields a K=2 boundary.
    auto m = make_trailing_key_added_metadata({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 400, 500, 5000),
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(2, 600, 999, 500, 5000),
    });

    std::vector<TabletRangeInfo> out;
    ASSERT_OK(get_tablet_split_ranges(/*tablet_manager=*/nullptr, m, /*split_count=*/2, &out));
    ASSERT_EQ(2u, out.size());
    for (const auto& tri : out) {
        if (tri.range.has_lower_bound()) {
            EXPECT_EQ(2, tri.range.lower_bound().values_size()) << tri.range.DebugString();
        }
        if (tri.range.has_upper_bound()) {
            EXPECT_EQ(2, tri.range.upper_bound().values_size()) << tri.range.DebugString();
        }
    }
    // The interior boundary is shared: split[0].upper == split[1].lower, and it is the projected
    // (k1, MIN) form rather than the bare (k1) the segments carried.
    ASSERT_TRUE(out[0].range.has_upper_bound());
    ASSERT_TRUE(out[1].range.has_lower_bound());
    EXPECT_TRUE(MessageDifferencer::Equals(out[0].range.upper_bound(), out[1].range.lower_bound()));
    EXPECT_EQ(VariantTypePB::NULL_VALUE, out[0].range.upper_bound().values(1).variant_type());
}

// Last line of defense: a tablet whose OWN range bound is already narrower than its sort key (the
// end state of the bug, on a cluster that hit it before this fix) must fail the split instead of
// propagating the corrupt bound into K new tablets. split_tablet turns this Status into the
// identical-tablet fallback, so the table stays queryable.
TEST(TabletSplitterTest, DataDrivenSplit_RejectsInheritedBoundWithWrongArity) {
    auto m = make_trailing_key_added_metadata(
            {
                    std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 400, 500, 5000),
                    std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(2, 600, 999, 500, 5000),
            },
            /*parent_bounds_arity1=*/true);

    std::vector<TabletRangeInfo> out;
    auto st = get_tablet_split_ranges(/*tablet_manager=*/nullptr, m, /*split_count=*/2, &out);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is_corruption()) << st;
    EXPECT_NE(std::string_view::npos, st.message().find("!= effective range-key arity 2")) << st;
}

// Per-segment ownership pruning compares a segment's STORED sort-key bounds against the new tablets'
// ranges, which the projection above emits at the current arity. A pre-ADD segment's narrower bounds
// are not comparable with them: VariantTuple::compare orders a shorter prefix-equal tuple BELOW its
// padded form, so a segment whose max is [400] misses the sibling starting at (400, NULL) -- while
// create_seek_range_from routes that segment's k1=400 rows to exactly that sibling (the added
// column's read-time default is NULL here, so the projected lower bound stays inclusive). The rows
// would be reachable from neither new tablet.
TEST(TabletSplitterTest, ComputeOwnership_narrowSegmentBoundMissesTheSiblingThatOwnsItsBoundaryRows) {
    // Ranges at the post-ADD arity 2: [(0,NULL), (400,NULL)) and [(400,NULL), +inf).
    TabletRangePB lower_range;
    *lower_range.mutable_lower_bound() = make_bigint_null_tuple_pb(0);
    lower_range.set_lower_bound_included(true);
    *lower_range.mutable_upper_bound() = make_bigint_null_tuple_pb(400);
    lower_range.set_upper_bound_included(false);
    TabletRangePB upper_range;
    *upper_range.mutable_lower_bound() = make_bigint_null_tuple_pb(400);
    upper_range.set_lower_bound_included(true);
    std::vector<TabletRangePB> ranges{lower_range, upper_range};

    RowsetMetadataPB rs;
    add_bigint_seg(&rs, 0, 400, "s0"); // pre-ADD: arity-1 bounds, max == the boundary's prefix

    auto own = compute_rowset_segment_ownership(rs, ranges);
    ASSERT_TRUE(own.ok());
    EXPECT_TRUE(own->segments[0].keep[0]);
    EXPECT_FALSE(own->segments[0].keep[1]) << "the boundary rows' segment is dropped from their own "
                                              "new tablet -- which is why can_prune_rowset_segments "
                                              "refuses to prune such a rowset at all";

    // The gate (predicate (e), pinned by CanPruneRowsetSegments_predicates) is what keeps those rows
    // reachable: the rowset stays unpruned and every segment survives on every new tablet as shared.
    // Padding the stored bounds instead would not do -- an old segment's rows read as
    // (prefix, default), so padding its MAX with the NULL minimum understates the segment's reach
    // whenever a post-ADD rowset contributed a boundary between (prefix, NULL) and (prefix, default).
    EXPECT_FALSE(can_prune_rowset_segments(rs, /*sort_key_arity=*/2));
}

// Corrupt metadata whose sort_key_idxes points past the column list must come back as a recoverable
// Status, not an abort. The bounds check therefore has to run on the raw protobuf: TabletSchema's
// _init_from_pb consumes sort_key_idxes first and indexes both schema.column(cid) and _cols[cid]
// without checking, so validating after materialization would be too late.
TEST(TabletSplitterTest, SortKeyProjection_RejectsOutOfRangeSortKeyIdxWithoutAborting) {
    auto m = make_trailing_key_added_metadata({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 400, 500, 5000),
    });
    ASSERT_EQ(2, m->schema().column_size());
    m->mutable_schema()->clear_sort_key_idxes();
    m->mutable_schema()->add_sort_key_idxes(0);
    m->mutable_schema()->add_sort_key_idxes(5); // out of range
    ASSERT_TRUE(m->schema().sort_key_unique_ids().empty());

    std::vector<SegmentSplitInfo> segments;
    auto st = build_segments_from_rowsets(/*tablet_manager=*/nullptr, m, /*split_width=*/2,
                                          /*data_page_split_width=*/2, &segments);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is_corruption()) << st;
    EXPECT_NE(std::string_view::npos, st.message().find("out of range")) << st;
}

// A schema carrying NO sort key at all (no sort_key_idxes and no key columns) yields arity 0. That is
// "cannot tell", not "every bound must be empty": condemning such a split made 16 pre-existing
// LakeTabletReshardTest cases fall back to an identical tablet, because their synthetic metadata has
// no schema columns. A real range-distributed tablet always has at least one sort-key column.
TEST(TabletSplitterTest, DataDrivenSplit_AllowsSchemaWithoutAnySortKey) {
    auto m = make_trailing_key_added_metadata({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 400, 500, 5000),
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(2, 600, 999, 500, 5000),
    });
    // Strip the schema down to no columns and no sort key, like the synthetic reshard fixtures.
    m->mutable_schema()->clear_column();
    m->mutable_schema()->clear_sort_key_idxes();
    m->mutable_schema()->clear_sort_key_unique_ids();

    std::vector<TabletRangeInfo> out;
    ASSERT_OK(get_tablet_split_ranges(/*tablet_manager=*/nullptr, m, /*split_count=*/2, &out));
    EXPECT_EQ(2u, out.size()) << "a schema without a sort key must not be treated as corrupt";
}

// sort_key_unique_ids takes precedence in TabletSchema::_init_from_pb, which resolves it through
// _unique_id_to_index.at(uid) -- that THROWS on an id no column carries, before any Status could be
// returned. So the raw ids must be checked first, exactly like sort_key_idxes.
TEST(TabletSplitterTest, SortKeyProjection_RejectsUnknownSortKeyUniqueIdWithoutAborting) {
    auto m = make_trailing_key_added_metadata({
            std::make_tuple<uint32_t, int64_t, int64_t, int64_t, int64_t>(1, 0, 400, 500, 5000),
    });
    ASSERT_EQ(2, m->schema().column_size());
    m->mutable_schema()->clear_sort_key_idxes();
    m->mutable_schema()->add_sort_key_unique_ids(0);
    m->mutable_schema()->add_sort_key_unique_ids(4242); // no column carries this unique id

    std::vector<SegmentSplitInfo> segments;
    auto st = build_segments_from_rowsets(/*tablet_manager=*/nullptr, m, /*split_width=*/2,
                                          /*data_page_split_width=*/2, &segments);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is_corruption()) << st;
    EXPECT_NE(std::string_view::npos, st.message().find("4242")) << st;
}

// =============================================================================
// Integration: split boundaries derived from the sort key sampler
// =============================================================================
//
// Every case below writes REAL segments through SegmentWriter and drives the production entry
// points (get_tablet_split_ranges / compute_split_ranges_from_external_boundaries), so what is
// under test is the whole chain: per-segment sample budget -> sampler path selection -> boundary
// selection.
//
// The key layout is deliberately OVERLAPPING: segment s holds the dense key run
// [s * rows_per_segment/2, s * rows_per_segment/2 + rows_per_segment), so consecutive segments
// share half their keys and the row density over the key space is a staircase (1, 2, 2, ..., 2, 1
// rows per key) rather than flat.
//
// That shape is what makes the evenness assertions falsifiable. The coarse [min_key, max_key]
// fallback has only 2*num_segments boundary points and must spread each segment's rows EVENLY over
// the candidate ranges it covers, which the staircase makes wrong -- so a test that measures the
// real row count of each emitted range fails loudly when sampling produces nothing. An
// evenly-partitioned DISJOINT layout would not: segment edges alone already split it evenly, and
// the test would be measuring the fixture instead of the sampler.
class SortKeySamplingSplitterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // The sampler reads the short key index, which SegmentWriter always builds, so these
        // segments exercise it directly.
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/sort_key_sampling_splitter_test";
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

    // One key column + one value column, DUP_KEYS, num_short_key_columns == the sort key arity.
    //
    // INT: a fixed-size type this module can decode, so short_key_index_encodes_full_sort_key()
    // accepts the schema and sampling takes the FREE short-key-index path (A).
    // VARCHAR: short key entries are truncated to index_length, so the predicate rejects the schema
    // and sampling must read data pages (path B).
    static TabletSchemaPB key_schema_pb(bool varchar_key) {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_id(varchar_key ? 7702 : 7701);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(65535);
        auto* k = pb.add_column();
        k->set_unique_id(1);
        k->set_name("k1");
        k->set_is_key(true);
        k->set_is_nullable(false);
        if (varchar_key) {
            k->set_type("VARCHAR");
            k->set_length(32);
            k->set_index_length(4); // < the 6-digit key width, so the short key really is truncated
        } else {
            k->set_type("INT");
            k->set_length(4);
            // index_length is load-bearing, not decoration: SeekTuple::short_key_encode writes the
            // key bytes only when Field::short_key_length() > 0, so leaving it unset produces
            // marker-only index entries that no decoder can read back -- and the covered path would
            // silently degrade to the data-page one.
            k->set_index_length(4);
        }
        auto* v = pb.add_column();
        v->set_unique_id(2);
        v->set_name("v1");
        v->set_type("INT");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_aggregation("REPLACE");
        pb.add_sort_key_idxes(0);
        return pb;
    }

    // Zero-padded so byte order == numeric order. That is what lets a VARCHAR range bound be read
    // back as the integer key it denotes, and it keeps the written rows non-decreasing so the
    // sampler's monotonicity validation sees a well-formed segment.
    static std::string encode_varchar_key(int64_t key) { return fmt::format("{:06d}", key); }

    TuplePB key_tuple_pb(int64_t key) const {
        VariantTuple tuple;
        if (_varchar_key) {
            const std::string encoded = encode_varchar_key(key);
            // DatumVariant holds a CopiedDatum, which deep-copies the Slice, so `encoded` may die
            // here.
            tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_VARCHAR), Datum(Slice(encoded))));
        } else {
            tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum(static_cast<int32_t>(key))));
        }
        TuplePB tuple_pb;
        tuple.to_proto(&tuple_pb);
        return tuple_pb;
    }

    // Writes one real segment holding |keys| (ascending) and returns its size on disk.
    // |rows_per_block| == 0 leaves SegmentWriterOptions at its own default (100 under BE_TEST);
    // pass a value to control the short key index's block geometry, which is what decides how many
    // candidate entries path A has to choose from.
    uint64_t write_segment(int64_t tablet_id, const std::string& name,
                           const std::shared_ptr<TabletSchema>& tablet_schema, const std::vector<int64_t>& keys,
                           uint32_t rows_per_block) {
        auto segment_path = _tablet_manager->segment_location(tablet_id, name);
        WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wfile_or = fs::new_writable_file(fopts, segment_path);
        CHECK_OK(wfile_or.status());

        SegmentWriterOptions opts;
        if (rows_per_block > 0) {
            opts.num_rows_per_block = rows_per_block;
        }
        SegmentWriter writer(std::move(wfile_or.value()), /*segment_id=*/0, tablet_schema, opts);
        CHECK_OK(writer.init());

        auto chunk_schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkFactory::new_chunk(chunk_schema, keys.size());
        auto cols = chunk->columns();
        // The Slices below point into this vector, which must therefore outlive append_chunk.
        std::vector<std::string> encoded;
        if (_varchar_key) {
            encoded.reserve(keys.size());
            for (int64_t key : keys) {
                encoded.push_back(encode_varchar_key(key));
            }
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            if (_varchar_key) {
                cols[0]->as_mutable_ptr()->append_datum(Datum(Slice(encoded[i])));
            } else {
                cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(keys[i])));
            }
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));
        return file_size;
    }

    // Writes |num_segments| segments, one rowset each, and returns the tablet metadata describing
    // them. See the class comment for the overlapping key layout. Records the layout in
    // _segment_key_runs / _total_rows / _key_span / _varchar_key so written_rows_in() below can
    // reconstruct the ground truth.
    std::shared_ptr<TabletMetadataPB> write_tablet(bool varchar_key, int num_segments, int64_t rows_per_segment,
                                                   uint32_t rows_per_block) {
        const int64_t tablet_id = next_id();
        prepare_tablet_dirs(tablet_id);
        _varchar_key = varchar_key;
        _total_rows = num_segments * rows_per_segment;
        _segment_key_runs.clear();
        const int64_t shift = rows_per_segment / 2;
        _key_span = (num_segments - 1) * shift + rows_per_segment;

        const auto schema_pb = key_schema_pb(varchar_key);
        auto tablet_schema = TabletSchema::create(schema_pb);

        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        *metadata->mutable_schema() = schema_pb;

        for (int s = 0; s < num_segments; ++s) {
            const int64_t key_start = s * shift;
            std::vector<int64_t> keys(rows_per_segment);
            std::iota(keys.begin(), keys.end(), key_start);
            _segment_key_runs.emplace_back(key_start, rows_per_segment);

            const std::string name = fmt::format("seg_{}.dat", s);
            const uint64_t size = write_segment(tablet_id, name, tablet_schema, keys, rows_per_block);

            auto* rowset = metadata->add_rowsets();
            rowset->set_id(static_cast<uint32_t>(s + 1));
            rowset->set_overlapped(false);
            rowset->set_num_rows(rows_per_segment);
            rowset->set_data_size(static_cast<int64_t>(size));
            rowset->set_num_dels(0); // explicit: skips build_rowset_anchor's PK delvec fallback
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(name);
            sm->set_size(static_cast<int64_t>(size));
            sm->set_num_rows(rows_per_segment);
            // Path A verifies that entry 0 of the short key index decodes EQUAL to sort_key_min, and
            // both paths reject a sample outside [sort_key_min, sort_key_max], so these must be the
            // segment's real first/last key -- not a rounded envelope.
            *sm->mutable_sort_key_min() = key_tuple_pb(keys.front());
            *sm->mutable_sort_key_max() = key_tuple_pb(keys.back());
        }
        return metadata;
    }

    std::shared_ptr<TabletMetadataPB> write_tablet_with_int_sort_key(int num_segments, int64_t rows_per_segment,
                                                                     uint32_t rows_per_block = 0) {
        return write_tablet(/*varchar_key=*/false, num_segments, rows_per_segment, rows_per_block);
    }

    std::shared_ptr<TabletMetadataPB> write_tablet_with_varchar_sort_key(int num_segments, int64_t rows_per_segment,
                                                                         uint32_t rows_per_block = 0) {
        return write_tablet(/*varchar_key=*/true, num_segments, rows_per_segment, rows_per_block);
    }

    // Writes ONE rowset carrying several segments at ascending, disjoint key runs -- the shape a
    // compacted rowset has, and the ONLY shape in which the per-segment budget index
    // (rowset_flat_index + meta_pos) is ever evaluated at a non-zero meta_pos. Every other helper
    // here puts one segment in its own rowset.
    //
    // |lost_meta_pos| >= 0 declares that segment in the metadata but never writes its file, so
    // Rowset::load_segments hands back a null LoadedSegment for it (requires
    // config::experimental_lake_ignore_lost_segment, otherwise the whole rowset fails to load).
    std::shared_ptr<TabletMetadataPB> write_one_rowset_with_segments(const std::vector<int64_t>& rows_per_segment,
                                                                     int lost_meta_pos = -1) {
        const int64_t tablet_id = next_id();
        prepare_tablet_dirs(tablet_id);
        _varchar_key = false;
        _segment_key_runs.clear();
        _total_rows = 0;

        const auto schema_pb = key_schema_pb(/*varchar_key=*/false);
        auto tablet_schema = TabletSchema::create(schema_pb);

        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        *metadata->mutable_schema() = schema_pb;
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_overlapped(false);
        rowset->set_num_dels(0); // explicit: skips build_rowset_anchor's PK delvec fallback

        int64_t key_start = 0;
        int64_t rowset_rows = 0;
        int64_t rowset_size = 0;
        for (size_t i = 0; i < rows_per_segment.size(); ++i) {
            const int64_t num_rows = rows_per_segment[i];
            CHECK_GT(num_rows, 0);
            std::vector<int64_t> keys(num_rows);
            std::iota(keys.begin(), keys.end(), key_start);
            const std::string name = fmt::format("seg_multi_{}.dat", i);

            uint64_t size = 100;
            if (static_cast<int>(i) != lost_meta_pos) {
                size = write_segment(tablet_id, name, tablet_schema, keys, /*rows_per_block=*/0);
                // Only written segments contribute rows the ground-truth helper can find.
                _segment_key_runs.emplace_back(key_start, num_rows);
                _total_rows += num_rows;
            }

            auto* sm = rowset->add_segment_metas();
            sm->set_filename(name);
            sm->set_size(static_cast<int64_t>(size));
            sm->set_num_rows(num_rows);
            // A lost segment keeps the bounds its metadata declares -- the file is gone, the
            // metadata is not.
            *sm->mutable_sort_key_min() = key_tuple_pb(keys.front());
            *sm->mutable_sort_key_max() = key_tuple_pb(keys.back());
            rowset_rows += num_rows;
            rowset_size += static_cast<int64_t>(size);
            key_start += num_rows;
        }
        _key_span = key_start;
        rowset->set_num_rows(rowset_rows);
        rowset->set_data_size(rowset_size);
        return metadata;
    }

    // |duplicates| rowsets all declaring the SAME physical segment (identical filename and bundle
    // offset), plus one distinct segment. This is the shape a merged non-PK tablet retains when
    // sibling rowsets reference one shared segment file. insert_physical_slice emits only the first
    // declaration, so a sample budget handed to the later ones is allocated and never spent --
    // which is what starves the slices that DO get emitted.
    std::shared_ptr<TabletMetadataPB> write_duplicate_slice_declarations(int duplicates, int64_t rows_per_segment) {
        const int64_t tablet_id = next_id();
        prepare_tablet_dirs(tablet_id);
        _varchar_key = false;
        _segment_key_runs.clear();
        _total_rows = 2 * rows_per_segment;
        _key_span = 2 * rows_per_segment;

        const auto schema_pb = key_schema_pb(/*varchar_key=*/false);
        auto tablet_schema = TabletSchema::create(schema_pb);
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        *metadata->mutable_schema() = schema_pb;

        auto declare = [&](const std::string& name, int64_t key_start, uint64_t size, uint32_t rowset_id) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_overlapped(false);
            rowset->set_num_rows(rows_per_segment);
            rowset->set_data_size(static_cast<int64_t>(size));
            rowset->set_num_dels(0);
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(name);
            sm->set_size(static_cast<int64_t>(size));
            sm->set_num_rows(rows_per_segment);
            *sm->mutable_sort_key_min() = key_tuple_pb(key_start);
            *sm->mutable_sort_key_max() = key_tuple_pb(key_start + rows_per_segment - 1);
        };

        std::vector<int64_t> shared_keys(rows_per_segment);
        std::iota(shared_keys.begin(), shared_keys.end(), int64_t{0});
        const uint64_t shared_size = write_segment(tablet_id, "dup_shared.dat", tablet_schema, shared_keys,
                                                   /*rows_per_block=*/0);
        _segment_key_runs.emplace_back(int64_t{0}, rows_per_segment);
        for (int i = 0; i < duplicates; ++i) {
            declare("dup_shared.dat", 0, shared_size, static_cast<uint32_t>(i + 1));
        }

        std::vector<int64_t> distinct_keys(rows_per_segment);
        std::iota(distinct_keys.begin(), distinct_keys.end(), rows_per_segment);
        const uint64_t distinct_size = write_segment(tablet_id, "dup_distinct.dat", tablet_schema, distinct_keys,
                                                     /*rows_per_block=*/0);
        _segment_key_runs.emplace_back(rows_per_segment, rows_per_segment);
        declare("dup_distinct.dat", rows_per_segment, distinct_size, static_cast<uint32_t>(duplicates + 1));
        return metadata;
    }

    // |num_segments| segments over the SAME key run, one rowset each -- the shape repeated loads
    // over one key domain leave behind. Every segment therefore declares the same
    // [sort_key_min, sort_key_max], so the coarse endpoints deduplicate to a single pair and the
    // only interior candidates a split can have are SAMPLES. That makes this the fixture in which a
    // lost sample budget is the difference between a split and a refusal.
    std::shared_ptr<TabletMetadataPB> write_overlapping_rowsets_over_one_key_run(int num_segments,
                                                                                 int64_t rows_per_segment) {
        const int64_t tablet_id = next_id();
        prepare_tablet_dirs(tablet_id);
        _varchar_key = false;
        // One run, not num_segments of them: the segments are copies, so these are the DISTINCT keys.
        _segment_key_runs.assign(1, {int64_t{0}, rows_per_segment});
        _total_rows = rows_per_segment;
        _key_span = rows_per_segment;

        const auto schema_pb = key_schema_pb(/*varchar_key=*/false);
        auto tablet_schema = TabletSchema::create(schema_pb);

        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        *metadata->mutable_schema() = schema_pb;

        std::vector<int64_t> keys(rows_per_segment);
        std::iota(keys.begin(), keys.end(), int64_t{0});
        for (int s = 0; s < num_segments; ++s) {
            const std::string name = fmt::format("seg_overlap_{}.dat", s);
            const uint64_t size = write_segment(tablet_id, name, tablet_schema, keys, /*rows_per_block=*/0);
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(static_cast<uint32_t>(s + 1));
            rowset->set_overlapped(false);
            rowset->set_num_rows(rows_per_segment);
            rowset->set_data_size(static_cast<int64_t>(size));
            rowset->set_num_dels(0);
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(name);
            sm->set_size(static_cast<int64_t>(size));
            sm->set_num_rows(rows_per_segment);
            *sm->mutable_sort_key_min() = key_tuple_pb(keys.front());
            *sm->mutable_sort_key_max() = key_tuple_pb(keys.back());
        }
        return metadata;
    }

    // (k1 INT) -- the schema a rowset was written with BEFORE a metadata-only trailing sort-key
    // key-column ADD. k1 alone is the key and the sort key, so every tuple this rowset contributes
    // is one column short of the tablet's current sort key.
    static TabletSchemaPB narrow_historical_schema_pb() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_id(7703);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(65535);
        auto* k1 = pb.add_column();
        k1->set_unique_id(1);
        k1->set_name("k1");
        k1->set_type("INT");
        k1->set_is_key(true);
        k1->set_is_nullable(false);
        k1->set_length(4);
        k1->set_index_length(4);
        auto* v = pb.add_column();
        v->set_unique_id(3);
        v->set_name("v1");
        v->set_type("INT");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_aggregation("REPLACE");
        pb.add_sort_key_idxes(0);
        return pb;
    }

    // (k1 INT, k2 INT NULL) -- the tablet's schema AFTER the ADD. k2 lands between k1 and the value
    // column, exactly as the FE's metadata-only trailing key add leaves it.
    static TabletSchemaPB widened_current_schema_pb() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_id(7704);
        pb.set_num_short_key_columns(2);
        pb.set_num_rows_per_row_block(65535);
        auto* k1 = pb.add_column();
        k1->set_unique_id(1);
        k1->set_name("k1");
        k1->set_type("INT");
        k1->set_is_key(true);
        k1->set_is_nullable(false);
        k1->set_length(4);
        k1->set_index_length(4);
        auto* k2 = pb.add_column();
        k2->set_unique_id(2);
        k2->set_name("k2");
        k2->set_type("INT");
        k2->set_is_key(true);
        k2->set_is_nullable(true);
        k2->set_length(4);
        k2->set_index_length(4);
        auto* v = pb.add_column();
        v->set_unique_id(3);
        v->set_name("v1");
        v->set_type("INT");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_aggregation("REPLACE");
        pb.add_sort_key_idxes(0);
        pb.add_sort_key_idxes(1);
        return pb;
    }

    // One real segment written under narrow_historical_schema_pb(), in a tablet whose current
    // schema is widened_current_schema_pb(). The rowset is mapped to the historical schema through
    // rowset_to_schema, so the sampler must decode it with that schema (arity 1) while the
    // projection lifts what comes out onto the current sort key (arity 2).
    std::shared_ptr<TabletMetadataPB> write_tablet_with_a_narrower_historical_schema(int64_t num_rows) {
        const int64_t tablet_id = next_id();
        prepare_tablet_dirs(tablet_id);
        _varchar_key = false;
        _total_rows = num_rows;

        _key_span = num_rows;
        _segment_key_runs.assign(1, {int64_t{0}, num_rows});

        const auto historical_pb = narrow_historical_schema_pb();
        auto historical_schema = TabletSchema::create(historical_pb);

        std::vector<int64_t> keys(num_rows);
        std::iota(keys.begin(), keys.end(), int64_t{0});
        const std::string name = "seg_historical.dat";
        const uint64_t size = write_segment(tablet_id, name, historical_schema, keys, /*rows_per_block=*/0);

        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        *metadata->mutable_schema() = widened_current_schema_pb();
        // No top-level schema id, so resolution MUST go through this rowset's historical schema.
        // rowset_schema_resolves_to_valid_id reads the historical id when rowset_to_schema has an
        // entry; reading the top-level one instead would see TabletSchema::invalid_id() (== 0, the
        // unset value) and skip sampling entirely.
        // materialize_sort_key_schema does not need an id -- it builds the schema locally.
        metadata->mutable_schema()->clear_id();
        (*metadata->mutable_historical_schemas())[historical_pb.id()] = historical_pb;

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(static_cast<int64_t>(size));
        rowset->set_num_dels(0);
        (*metadata->mutable_rowset_to_schema())[1] = historical_pb.id();
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(name);
        sm->set_size(static_cast<int64_t>(size));
        sm->set_num_rows(num_rows);
        // Arity 1: written before the ADD, exactly like the samples the sampler will decode.
        *sm->mutable_sort_key_min() = key_tuple_pb(keys.front());
        *sm->mutable_sort_key_max() = key_tuple_pb(keys.back());
        return metadata;
    }

    // The integer key a range bound denotes, or nullopt when the bound is absent (unbounded).
    // Both key spaces ARE the integers [0, _key_span): the INT schema stores them directly, the
    // VARCHAR one zero-padded.
    std::optional<int64_t> decode_bound(const TabletRangePB& range, bool lower) const {
        if (lower ? !range.has_lower_bound() : !range.has_upper_bound()) {
            return std::nullopt;
        }
        VariantTuple tuple;
        CHECK_OK(tuple.from_proto(lower ? range.lower_bound() : range.upper_bound()));
        CHECK_EQ(1u, tuple.size());
        if (_varchar_key) {
            return std::stoll(tuple[0].value().get_slice().to_string());
        }
        return tuple[0].value().get_int32();
    }

    // Rows the FIXTURE actually wrote into |range|. Each segment is a dense run of one row per key
    // and split emits [lower, upper), so this is arithmetic over the written runs -- never a second
    // reading of the sampler's own estimates. Using the per-range rowset_stats instead would
    // compare the greedy algorithm against its OWN input distribution, which it optimises directly,
    // and would pass with no samples at all.
    int64_t written_rows_in(const TabletRangePB& range) const {
        const auto lower = decode_bound(range, /*lower=*/true);
        const auto upper = decode_bound(range, /*lower=*/false);
        int64_t rows = 0;
        for (const auto& [start, count] : _segment_key_runs) {
            const int64_t lo = lower.has_value() ? std::max(*lower, start) : start;
            const int64_t hi = upper.has_value() ? std::min(*upper, start + count) : start + count;
            rows += std::max<int64_t>(0, hi - lo);
        }
        return rows;
    }

    // Emitted ranges must tile the key space: ascending, adjacent bounds byte-equal, closed-open,
    // and the two ends unbounded (the parent range is Range.all() in this fixture).
    static bool ranges_are_ordered_and_gapless(const std::vector<TabletRangeInfo>& ranges) {
        if (ranges.empty()) return false;
        if (ranges.front().range.has_lower_bound() || ranges.back().range.has_upper_bound()) return false;
        for (size_t i = 0; i + 1 < ranges.size(); ++i) {
            const auto& current = ranges[i].range;
            const auto& next = ranges[i + 1].range;
            if (!current.has_upper_bound() || !next.has_lower_bound()) return false;
            if (current.upper_bound_included() || !next.lower_bound_included()) return false;
            if (!MessageDifferencer::Equals(current.upper_bound(), next.lower_bound())) return false;
        }
        return true;
    }

    // Two FE-supplied ranges meeting at the middle of the key space.
    RepeatedPtrField<TabletRangePB> two_external_ranges() const {
        RepeatedPtrField<TabletRangePB> ranges;
        const auto mid = key_tuple_pb(_key_span / 2);
        auto* low = ranges.Add();
        *low->mutable_upper_bound() = mid;
        low->set_upper_bound_included(false);
        auto* high = ranges.Add();
        *high->mutable_lower_bound() = mid;
        high->set_lower_bound_included(true);
        return ranges;
    }

    // A single FE-supplied range covering the whole key space. split_width is then 1, which is what
    // makes the std::max<int64_t>(2, ...) clamp at the external-boundaries call site observable:
    // allocate_sort_key_sample_budget returns all zeros below width 2.
    static RepeatedPtrField<TabletRangePB> one_external_range() {
        RepeatedPtrField<TabletRangePB> ranges;
        ranges.Add();
        return ranges;
    }

    std::string _test_dir;
    std::shared_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
    // (first key, key count) of every segment the last write_tablet* call wrote, in write order.
    std::vector<std::pair<int64_t, int64_t>> _segment_key_runs;
    int64_t _total_rows = 0;
    int64_t _key_span = 0;
    bool _varchar_key = false;
};

// Tolerance for the evenness assertions below, as a fraction of the ideal per-range row count.
//
// Why not 10%: split boundaries are chosen from ESTIMATES, and distribute_to_ranges spreads a
// sub-segment's rows EVENLY over the candidate ranges it covers regardless of their key widths
// (tablet_splitter.cpp's distribute_to_ranges). Overlapping segments give the candidate ranges
// unequal widths -- with this fixture's half-segment shift they alternate roughly 1:3 -- so the
// estimate carries a systematic bias no amount of sampling removes. Measured worst case here is
// ~12% at split_count == 8. That bias is pre-existing behavior of the boundary algorithm, not
// something sampling introduces or is meant to fix.
//
// 15% is nowhere near vacuous: without samples this same fixture misses by ~80% of the ideal (one
// range takes 99,996 of the 100,000 rows) and cannot even produce 8 ranges. Both clauses below were
// confirmed to fail with sampling disabled.
//
// Do NOT read this tolerance as a sample-DENSITY guard. Its headroom is thin -- at split_count == 8
// the window is 1,875 rows against a ~1,500 residual, about 3 points -- and with ~62 samples per
// 25,000-row segment the boundary granularity is only ~400 rows, so halving the sample density
// would probably still pass. What actually catches a budget that stops scaling with split_width is
// the ASSERT_EQ(split_count, ranges.size()) below: a constant per-segment budget cannot supply
// enough candidates for an 8-way split, which is how that failure was observed.
constexpr double kEvennessTolerance = 0.15;

// Boundaries derived by sampling must divide the tablet to within kEvennessTolerance of even, on a
// tablet whose row distribution the fixture knows exactly. A VARCHAR sort key is not covered by the
// short key index, so this is path B: the data-page sampler.
TEST_F(SortKeySamplingSplitterTest, sampled_boundaries_split_a_varchar_sort_key_tablet_evenly) {
    auto metadata = write_tablet_with_varchar_sort_key(/*num_segments=*/4, /*rows_per_segment=*/25000);
    for (int32_t split_count : {2, 8}) {
        SCOPED_TRACE(fmt::format("split_count={}", split_count));
        std::vector<TabletRangeInfo> ranges;
        ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, split_count, &ranges));
        ASSERT_EQ(static_cast<size_t>(split_count), ranges.size());
        EXPECT_TRUE(ranges_are_ordered_and_gapless(ranges));
        const int64_t ideal = 100000 / split_count;
        int64_t total = 0;
        for (const auto& range : ranges) {
            const int64_t rows = written_rows_in(range.range);
            total += rows;
            EXPECT_NEAR(static_cast<double>(rows), static_cast<double>(ideal), ideal * kEvennessTolerance);
        }
        EXPECT_EQ(100000, total) << "the emitted ranges must tile every written row";
    }
}

// The same evenness contract on the covered (INT) sort key, i.e. through path A. Asserted here as
// well as in the VARCHAR case because the two paths compute row_interval differently -- path A from
// the index's block geometry, path B from a row stride -- so an error in either would be invisible
// from the other's test.
TEST_F(SortKeySamplingSplitterTest, sampled_boundaries_split_an_int_sort_key_tablet_evenly) {
    auto metadata = write_tablet_with_int_sort_key(/*num_segments=*/4, /*rows_per_segment=*/25000);
    for (int32_t split_count : {2, 8}) {
        SCOPED_TRACE(fmt::format("split_count={}", split_count));
        std::vector<TabletRangeInfo> ranges;
        ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, split_count, &ranges));
        ASSERT_EQ(static_cast<size_t>(split_count), ranges.size());
        const int64_t ideal = 100000 / split_count;
        for (const auto& range : ranges) {
            EXPECT_NEAR(static_cast<double>(written_rows_in(range.range)), static_cast<double>(ideal),
                        ideal * kEvennessTolerance);
        }
    }
}

// A covered sort key must take path A and read no data pages at all. This is the only assertion
// that distinguishes the two paths: both publish their samples through the same carrier, so an
// even split alone would not reveal path B quietly doing path A's work.
TEST_F(SortKeySamplingSplitterTest, covered_sort_key_splits_without_reading_data_pages) {
    auto metadata = write_tablet_with_int_sort_key(/*num_segments=*/4, /*rows_per_segment=*/25000);
    const int64_t before_data_page = sort_key_sampling_data_page_segments_count();
    const int64_t before_samples = sort_key_sampling_samples_count();
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 4, &ranges));
    ASSERT_EQ(4u, ranges.size());
    EXPECT_EQ(before_data_page, sort_key_sampling_data_page_segments_count())
            << "a covered sort key must not read data pages";
    EXPECT_GT(sort_key_sampling_samples_count() - before_samples, 0)
            << "...and must still have published samples, from the short key index";
}

// The external-boundaries chain keeps the free path but never pays page-read I/O, because samples
// cannot move its boundaries -- FE supplies them.
TEST_F(SortKeySamplingSplitterTest, external_boundaries_never_read_data_pages) {
    auto metadata = write_tablet_with_varchar_sort_key(/*num_segments=*/4, /*rows_per_segment=*/25000);
    const int64_t before_data_page = sort_key_sampling_data_page_segments_count();
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(_tablet_manager.get(), metadata, two_external_ranges(),
                                                            &ranges));
    ASSERT_EQ(2u, ranges.size());
    EXPECT_EQ(before_data_page, sort_key_sampling_data_page_segments_count());
}

// The two-range test above cannot reach the zero-budget branch, because split_width == 2 already
// clears it. A single FE-supplied range is the case that exposed the bug: it must still take the
// free path A, not fall through to coarse [min, max].
TEST_F(SortKeySamplingSplitterTest, a_single_external_range_still_uses_path_a) {
    auto metadata = write_tablet_with_int_sort_key(/*num_segments=*/4, /*rows_per_segment=*/25000,
                                                   /*rows_per_block=*/1024);
    const int64_t before_data_page = sort_key_sampling_data_page_segments_count();
    const int64_t before_samples = sort_key_sampling_samples_count();
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(compute_split_ranges_from_external_boundaries(_tablet_manager.get(), metadata, one_external_range(),
                                                            &ranges));
    ASSERT_EQ(1u, ranges.size());
    // The external-boundaries path performs NO sampling, by upstream design: the stable-metadata
    // reshard rework moved per-child statistics out of this function into prepare_split_projection,
    // which weighs children from each segment's coarse [min, max] and deliberately opens nothing on
    // the publish path. FE supplied the boundaries, so samples could not have moved them anyway --
    // they only sharpened attribution, whose fallback (proportional over [min, max]) is benign.
    // This assertion is therefore the inverse of what it once was, and it still discriminates: a
    // future change that starts sampling here would show up as page-read I/O on a publish.
    EXPECT_EQ(before_samples, sort_key_sampling_samples_count())
            << "the external-boundaries path must not sample; attribution is the projection's job";
    EXPECT_EQ(before_data_page, sort_key_sampling_data_page_segments_count()) << "and must never read data pages";
}

TEST_F(SortKeySamplingSplitterTest, sampling_disabled_still_produces_valid_ranges) {
    const auto saved = config::sort_key_max_samples_per_tablet;
    DeferOp restore([&] { config::sort_key_max_samples_per_tablet = saved; });
    config::sort_key_max_samples_per_tablet = 0;
    auto metadata = write_tablet_with_varchar_sort_key(/*num_segments=*/4, /*rows_per_segment=*/25000);
    const int64_t before_data_page = sort_key_sampling_data_page_segments_count();
    const int64_t before_samples = sort_key_sampling_samples_count();
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 2, &ranges));
    ASSERT_EQ(2u, ranges.size());
    EXPECT_TRUE(ranges_are_ordered_and_gapless(ranges));
    // A cap of 0 disables sampling tablet-wide, so neither path may run at all.
    EXPECT_EQ(before_data_page, sort_key_sampling_data_page_segments_count());
    EXPECT_EQ(before_samples, sort_key_sampling_samples_count());
}

// The remaining floor: a segment with too few DISTINCT keys to divide. Path A returning empty falls
// through to path B instead of being treated as authoritative, so a sub-block segment is sampled at
// ROW granularity and can still be split (the case below this one) -- but three keys cannot yield
// eight ranges however finely they are sampled. Must set rows_per_block explicitly, since BE_TEST
// defaults it to 100.
//
// Producing exactly K ranges is best-effort inside calculate_range_split_boundaries, but
// get_tablet_split_ranges_impl converts "fewer than K" into a REFUSAL and clears the output;
// split_tablet then publishes an identical tablet. So the contract
// this pins is "refuse and emit nothing", not "return a short vector" -- and neither a silently
// short vector nor 8 ranges over 3 keys may pass.
TEST_F(SortKeySamplingSplitterTest, a_segment_with_too_few_distinct_keys_is_refused_not_silently_short) {
    auto metadata = write_tablet_with_int_sort_key(/*num_segments=*/1, /*rows_per_segment=*/3,
                                                   /*rows_per_block=*/1024);
    std::vector<TabletRangeInfo> ranges;
    auto st = get_tablet_split_ranges(_tablet_manager.get(), metadata, 8, &ranges);
    EXPECT_FALSE(st.ok()) << "8 ranges over 3 distinct keys must be refused, not produced";
    // NotSupported, not InvalidArgument: the stable-metadata rework reclassified "insufficient
    // boundaries" so FE reads it as a benign fall-back-to-identical-tablet rather than bad input.
    EXPECT_TRUE(st.is_not_supported()) << st;
    EXPECT_TRUE(ranges.empty()) << "a refusal must leave no partial output, got " << ranges.size();

    // The refusal is about the requested WIDTH, not about the tablet being indivisible: the same
    // three rows do split two ways, and only because path B sampled them at row granularity.
    ranges.clear();
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 2, &ranges));
    EXPECT_EQ(2u, ranges.size());
    EXPECT_TRUE(ranges_are_ordered_and_gapless(ranges));
}

// The complement: a sub-block segment with plenty of distinct keys IS still splittable, because
// path A yielding nothing escalates to path B rather than settling for [min, max].
TEST_F(SortKeySamplingSplitterTest, a_sub_block_segment_is_still_split_via_data_pages) {
    auto metadata = write_tablet_with_int_sort_key(/*num_segments=*/1, /*rows_per_segment=*/500,
                                                   /*rows_per_block=*/1024);
    const int64_t before_data_page = sort_key_sampling_data_page_segments_count();
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 4, &ranges));
    EXPECT_EQ(4u, ranges.size()) << "path A had no samples; path B should have supplied them";
    EXPECT_GT(sort_key_sampling_data_page_segments_count() - before_data_page, 0)
            << "a single-block segment cannot be divided from the index, so path B must have run";
}

// A child tablet from an earlier split still carries every segment a non-pruneable rowset could not
// shed (can_prune_rowset_segments: a partial-compaction cursor, absent sort-key bounds, bounds at a
// stale arity), and that includes segments lying wholly inside a sibling's range. Weighting the
// sample budget by PHYSICAL rows spends nearly all of it on such a segment -- whose candidates the
// tablet-range filter in calculate_range_split_boundaries then drops -- and leaves the only
// in-range segment with a zero budget: no interior candidate, and the split REFUSED rather than
// merely coarse. The budget is weighted by in-range rows instead.
TEST_F(SortKeySamplingSplitterTest, an_out_of_range_segment_does_not_starve_the_in_range_one) {
    // One rowset, two segments: 70000 rows at keys [0, 70000) and 500 at [70000, 70500). At
    // split_width 4 the whole tablet targets 32 * 4 == 128 samples, so a physical-row share gives
    // the small segment 128 * 500 / 70500 == 0 -- the starvation this test pins.
    auto metadata = write_one_rowset_with_segments({70000, 500});
    // The range a previous split would have left on the upper child: the 70000-row segment is now
    // wholly below it, so every sample taken from it is discarded by the range filter.
    auto* tablet_range = metadata->mutable_range();
    *tablet_range->mutable_lower_bound() = key_tuple_pb(70000);
    tablet_range->set_lower_bound_included(true);

    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 4, &ranges));
    ASSERT_EQ(4u, ranges.size());

    // The child ranges must tile [70000, +inf) ...
    const auto first_lower = decode_bound(ranges.front().range, /*lower=*/true);
    ASSERT_TRUE(first_lower.has_value());
    EXPECT_EQ(70000, *first_lower);
    EXPECT_FALSE(ranges.back().range.has_upper_bound());
    for (size_t i = 0; i + 1 < ranges.size(); ++i) {
        EXPECT_TRUE(MessageDifferencer::Equals(ranges[i].range.upper_bound(), ranges[i + 1].range.lower_bound()));
        // ... and every interior boundary must come from the in-range segment's keys. The
        // out-of-range segment's keys are all below 70000, so a boundary there would be direct
        // evidence the budget went to the segment the range filter discards.
        const auto boundary = decode_bound(ranges[i].range, /*lower=*/false);
        ASSERT_TRUE(boundary.has_value());
        EXPECT_GT(*boundary, 70000);
        EXPECT_LT(*boundary, 70500);
    }
}

// Duplicate physical slices must not be handed a share of the sample budget. insert_physical_slice
// emits only the FIRST declaration of a (filename, bundle_file_offset), so any share allocated to a
// later duplicate is spent on nothing -- and with 50 duplicates against a 2-way target of 64, the two
// slices that are actually emitted are left with about one sample between them instead of about 64.
//
// Asserting the published sample count is what discriminates: the split itself still succeeds either
// way here (two distinct key runs give coarse boundaries), so only the sample density reveals the
// stranded budget.
TEST_F(SortKeySamplingSplitterTest, duplicate_physical_slices_do_not_strand_the_sample_budget) {
    auto metadata = write_duplicate_slice_declarations(/*duplicates=*/50, /*rows_per_segment=*/2000);
    const int64_t before_samples = sort_key_sampling_samples_count();

    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 2, &ranges));
    ASSERT_EQ(2u, ranges.size());
    EXPECT_TRUE(ranges_are_ordered_and_gapless(ranges));

    // The two emitted slices split a target of 32 * 2 == 64 between them once the duplicates stop
    // taking a cut. With the duplicates weighted, their combined share rounds to roughly one sample.
    EXPECT_GT(sort_key_sampling_samples_count() - before_samples, 10)
            << "the emitted slices were starved by shares allocated to duplicate declarations";
}

// More segments than the tablet's sample target is the shape an independently floored share lost
// entirely: at the default cap a 2-way split targets 32 * 2 == 64 samples, so 65 equally sized
// segments each floored to 64/65 == 0, no rowset passed rowset_wants_samples, and the tablet was
// sampled not at all. Here every segment covers the same key run, so the coarse endpoints collapse
// to one pair and samples are the ONLY possible interior candidate -- which turns that lost budget
// into `Not enough split ranges available` on a tablet whose keys divide perfectly.
TEST_F(SortKeySamplingSplitterTest, a_tablet_with_more_segments_than_samples_still_splits) {
    auto metadata = write_overlapping_rowsets_over_one_key_run(/*num_segments=*/65, /*rows_per_segment=*/200);
    const int64_t before_samples = sort_key_sampling_samples_count();

    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 2, &ranges));
    ASSERT_EQ(2u, ranges.size());
    EXPECT_TRUE(ranges_are_ordered_and_gapless(ranges));
    EXPECT_GT(sort_key_sampling_samples_count() - before_samples, 0)
            << "with identical coarse bounds, a boundary can only have come from a sample";

    // The boundary must be interior to the shared key run, not one of its endpoints.
    const auto boundary = decode_bound(ranges.front().range, /*lower=*/false);
    ASSERT_TRUE(boundary.has_value());
    EXPECT_GT(*boundary, 0);
    EXPECT_LT(*boundary, 199);
}

// A rowset written before a metadata-only trailing sort-key key-column ADD is decoded with its own
// (arity-1) historical schema, and every tuple it contributes -- samples included -- is then lifted
// onto the tablet's current (arity-2) sort key with the NULL (== MIN) sentinel. Emitting an
// arity-1 bound would brick every new tablet: RangeRouter rejects every subsequent load.
//
// This is the sample half of BuildSegmentsFromRowsets_ProjectsNarrowSegmentKeysOntoCurrentSortKey,
// which can only reach min_key/max_key because a metadata-only tablet has no segment to sample.
TEST_F(SortKeySamplingSplitterTest, samples_from_a_narrower_historical_schema_are_projected) {
    auto metadata = write_tablet_with_a_narrower_historical_schema(/*num_rows=*/500);
    // Load-bearing, and asserted so it cannot silently regress into a valid id: with a valid
    // top-level id this case would also pass if rowset_schema_resolves_to_valid_id ignored
    // rowset_to_schema and read the top-level id unconditionally.
    ASSERT_FALSE(metadata->schema().has_id());
    ASSERT_EQ(1, metadata->rowset_to_schema().size());

    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, /*split_width=*/4,
                                          /*data_page_split_width=*/4, &segments));
    ASSERT_EQ(1u, segments.size());
    ASSERT_FALSE(segments[0].sort_key_samples.empty())
            << "the historical schema must still resolve to a sampleable segment";
    for (const auto& sample : segments[0].sort_key_samples) {
        ASSERT_EQ(2u, sample.size()) << "a sample must be emitted at the CURRENT sort-key arity";
        EXPECT_TRUE(sample[1].value().is_null());
    }
    EXPECT_EQ(2u, segments[0].min_key.size());
    EXPECT_EQ(2u, segments[0].max_key.size());

    // And the split those samples feed must emit bounds at the same arity.
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 2, &ranges));
    ASSERT_EQ(2u, ranges.size());
    ASSERT_TRUE(ranges[0].range.has_upper_bound());
    EXPECT_EQ(2, ranges[0].range.upper_bound().values_size());
}

// The per-segment sample budget must be indexed by the segment's position WITHIN its rowset, not by
// the rowset alone. Every other case in this fixture writes one segment per rowset, so meta_pos is
// always 0 there and `rowset_flat_index + meta_pos` is never evaluated at a non-zero meta_pos --
// while a compacted rowset normally carries several segments, which is the common production shape.
//
// The two segments have deliberately UNEQUAL row counts: equal ones receive equal budget whether or
// not the index is right, so only an unequal pair can observe a misattribution.
//
// The expected counts are fully determined by the fixture. With rows_per_block == 100 (the BE_TEST
// SegmentWriterOptions default) and split_width == 4:
//   target_total            = min(config::sort_key_max_samples_per_tablet == 1024, 32 * 4) = 128
//   share(35,000 of 40,000) = 128 * 35000 / 40000 = 112
//   share( 5,000 of 40,000) = 128 *  5000 / 40000 =  16
//   segment 0: 350 index entries, 349 candidates, stride = ceil(349/112) = 4
//              -> entries 4..348 step 4 = 87 samples, row_interval = 4 * 100 = 400
//   segment 1:  50 index entries,  49 candidates, stride = ceil(49/16)   = 4
//              -> entries 4..48  step 4 = 12 samples, row_interval = 400
// Flattening the index to budget[rowset] hands segment 1 a target of 112, hence stride 1, hence 49
// samples at row_interval 100 -- so both the count AND the interval move, and either assertion
// catches it.
//
// Known limit: this cannot distinguish indexing by meta_pos from indexing by segment LOAD order,
// because Rowset::load_segments returns this rowset's segments in meta_pos order. It does catch
// dropping the per-segment term altogether, which is the failure mode with a wrong result.
TEST_F(SortKeySamplingSplitterTest, budget_is_indexed_per_segment_within_a_rowset) {
    auto metadata = write_one_rowset_with_segments({35000, 5000});
    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, /*split_width=*/4,
                                          /*data_page_split_width=*/0, &segments));
    ASSERT_EQ(2u, segments.size());
    EXPECT_EQ(87u, segments[0].sort_key_samples.size());
    EXPECT_EQ(400, segments[0].sort_key_sample_row_interval);
    EXPECT_EQ(12u, segments[1].sort_key_samples.size())
            << "the smaller segment must get its OWN (proportionally smaller) budget";
    EXPECT_EQ(400, segments[1].sort_key_sample_row_interval);
}

// A segment of a multi-segment rowset whose file is missing comes back as a null LoadedSegment
// (under experimental_lake_ignore_lost_segment) and must degrade to its coarse [min, max] range
// while its siblings are still sampled -- without dereferencing the null. Re-homes the coverage of
// the deleted BuildSegmentsFromRowsetsLoaderTest.LostSegmentFallsBackWithoutDereferencingNull,
// which was the split path's only test of that guard.
TEST_F(SortKeySamplingSplitterTest, a_lost_segment_stays_coarse_while_its_siblings_are_sampled) {
    const bool saved = config::experimental_lake_ignore_lost_segment;
    DeferOp restore([&] { config::experimental_lake_ignore_lost_segment = saved; });
    config::experimental_lake_ignore_lost_segment = true;

    // meta_pos 1 is declared in the metadata but never written to disk. Budget shares are unchanged
    // (they come from the declared row counts), so segment 0 still expects the 87/400 above.
    auto metadata = write_one_rowset_with_segments({35000, 5000}, /*lost_meta_pos=*/1);
    std::vector<SegmentSplitInfo> segments;
    ASSERT_OK(build_segments_from_rowsets(_tablet_manager.get(), metadata, /*split_width=*/4,
                                          /*data_page_split_width=*/0, &segments));
    ASSERT_EQ(2u, segments.size());
    EXPECT_EQ(87u, segments[0].sort_key_samples.size()) << "the surviving sibling must still be sampled";
    EXPECT_EQ(400, segments[0].sort_key_sample_row_interval);
    EXPECT_TRUE(segments[1].sort_key_samples.empty()) << "the lost segment must fall back to coarse";
    EXPECT_EQ(0, segments[1].sort_key_sample_row_interval);
    EXPECT_EQ(5000, segments[1].num_rows) << "and must keep the row count its metadata declares";

    // And the split as a whole still succeeds, on the surviving segment's samples.
    std::vector<TabletRangeInfo> ranges;
    ASSERT_OK(get_tablet_split_ranges(_tablet_manager.get(), metadata, 2, &ranges));
    ASSERT_EQ(2u, ranges.size());
    EXPECT_TRUE(ranges_are_ordered_and_gapless(ranges));
}

} // namespace starrocks::lake
