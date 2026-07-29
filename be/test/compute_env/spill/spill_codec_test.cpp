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

// Roundtrip tests for the spill column codec module (compute_env/spill/codec).
// Shapes are produced by the same fixed-seed factory as the frozen benchmark dataset
// (bench/spill_bench_data.h), plus explicit null-ratio variants which the frozen data
// does not cover.

#include "compute_env/spill/codec/spill_codec.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "bench/spill_bench_data.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/serde/column_array_serde.h"
#include "compute_env/spill/codec/codec_selector.h"
#include "compute_env/spill/codec/leaf_classify.h"

namespace starrocks::spill {

using spill_bench::DataSet;
using spill_bench::GenConfig;

namespace {

// Value-exact column equality via the raw (level-0) serde bytes.
std::string raw_bytes_of(const Column& col) {
    int64_t cap = serde::ColumnArraySerde::max_serialized_size(col, 0);
    std::string buf;
    buf.resize(cap);
    auto* base = reinterpret_cast<uint8_t*>(buf.data());
    auto r = serde::ColumnArraySerde::serialize(col, base, false, 0);
    CHECK(r.ok());
    buf.resize(r.value() - base);
    return buf;
}

void roundtrip_expect_equal(const SpillColumnCodec* codec, const Column& col, uint32_t param) {
    int64_t cap = codec->max_encoded_size(col, param);
    ASSERT_GT(cap, 0) << "codec " << static_cast<int>(codec->id()) << " not applicable";
    std::string enc;
    enc.resize(cap + 16); // slack mirrors the serde's trailing decode pad
    auto* base = reinterpret_cast<uint8_t*>(enc.data());
    auto e = codec->encode(col, base, param);
    ASSERT_TRUE(e.ok()) << e.status();
    size_t n = e.value() - base;
    ASSERT_LE(static_cast<int64_t>(n), cap) << "encode overflowed max_encoded_size";

    MutableColumnPtr target = col.clone_empty();
    auto d = codec->decode(base, base + n + 16, target.get());
    ASSERT_TRUE(d.ok()) << d.status();
    ASSERT_EQ(static_cast<const uint8_t*>(d.value()), base + n) << "decode cursor mismatch";

    ASSERT_EQ(col.size(), target->size());
    // Row-wise semantic equality. NOTE: bytes under NULL positions are undefined (raw
    // column allocators skip zero-init), and codecs are free to canonicalize them
    // (BOOL_BITPACK does), so a bit-exact whole-column compare would be over-strict.
    for (size_t i = 0; i < col.size(); ++i) {
        ASSERT_EQ(col.is_null(i), target->is_null(i)) << "null flag mismatch at row " << i;
        if (!col.is_null(i)) {
            ASSERT_EQ(0, col.compare_at(i, i, *target, 1)) << "value mismatch at row " << i;
        }
    }
}

std::vector<CodecId> applicable_codecs_for(const Column& col) {
    std::vector<CodecId> out;
    for (const auto& cand : CodecRegistry::instance()->candidates(col, 7)) {
        out.push_back(cand.id);
    }
    return out;
}

} // namespace

class SpillCodecRoundtripTest : public ::testing::TestWithParam<std::tuple<DataSet, double>> {};

TEST_P(SpillCodecRoundtripTest, roundtrip_all_candidates) {
    auto [ds, null_ratio] = GetParam();
    GenConfig cfg;
    cfg.num_rows = 4096;
    cfg.null_ratio = null_ratio;
    ColumnPtr col = spill_bench::build_scalar_column(ds, cfg, /*salt=*/0x1);
    ASSERT_NE(col, nullptr);

    const std::string before = raw_bytes_of(*col);
    for (CodecId id : applicable_codecs_for(*col)) {
        const auto* codec = CodecRegistry::instance()->get(id);
        ASSERT_NE(codec, nullptr);
        uint32_t param = id == CodecId::LEGACY ? 7 : 0;
        SCOPED_TRACE("codec_id=" + std::to_string(static_cast<int>(id)) + " dataset=" +
                     std::string(spill_bench::dataset_name(ds)) + " null_ratio=" + std::to_string(null_ratio));
        roundtrip_expect_equal(codec, *col, param);
        ASSERT_EQ(before, raw_bytes_of(*col)) << "SOURCE COLUMN MUTATED by codec " << static_cast<int>(id);
    }
}

INSTANTIATE_TEST_SUITE_P(
        AllScalarShapes, SpillCodecRoundtripTest,
        ::testing::Combine(::testing::Values(DataSet::BOOL_RUNS, DataSet::BOOL_RANDOM, DataSet::INT64_CONST,
                                             DataSet::INT64_SEQ, DataSet::INT64_NARROW, DataSet::INT64_OUTLIER,
                                             DataSet::INT64_RANDOM, DataSet::DECIMAL64_MONEY, DataSet::DOUBLE_DECIMAL,
                                             DataSet::DOUBLE_RANDOM, DataSet::TS_MONOTONIC, DataSet::DATE_LOWRES,
                                             DataSet::STR_LOWCARD, DataSet::STR_TEMPLATE, DataSet::STR_SORTED,
                                             DataSet::STR_LONGTEXT, DataSet::STR_UUID),
                           ::testing::Values(0.0, 0.3, 1.0)));

TEST(SpillCodecTest, null_framing_edge_cases) {
    // single-row, empty, and tiny columns through every int codec
    GenConfig cfg;
    for (size_t rows : {0, 1, 2, 3}) {
        cfg.num_rows = rows;
        cfg.null_ratio = rows > 1 ? 0.5 : 0.0;
        ColumnPtr col = spill_bench::build_scalar_column(DataSet::INT64_NARROW, cfg, 0x2);
        for (CodecId id : {CodecId::INT_RLE, CodecId::INT_DELTA, CodecId::INT_FOR, CodecId::INT_PFOR}) {
            SCOPED_TRACE("rows=" + std::to_string(rows) + " codec=" + std::to_string(static_cast<int>(id)));
            roundtrip_expect_equal(CodecRegistry::instance()->get(id), *col, 0);
        }
    }
}

TEST(SpillCodecTest, generic_block_roundtrips_complex_types) {
    // Array<int> -- first non-LEGACY compression option for complex types (M2.5)
    auto col = ColumnHelper::create_column(TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)), true);
    col->reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        DatumArray arr{Datum(i % 10), Datum(i % 100), Datum(i)};
        col->append_datum(Datum(arr));
    }
    for (CodecId id : {CodecId::GENERIC_BLOCK_LZ4, CodecId::GENERIC_BLOCK_ZSTD}) {
        SCOPED_TRACE("codec=" + std::to_string(static_cast<int>(id)));
        roundtrip_expect_equal(CodecRegistry::instance()->get(id), *col, 0);
    }
    // and complex-type candidate sets now include the generic blocks
    auto cands = CodecRegistry::instance()->candidates(*col, 7);
    bool has_gb = false;
    for (const auto& c : cands) has_gb |= (c.id == CodecId::GENERIC_BLOCK_ZSTD);
    ASSERT_TRUE(has_gb);
}

TEST(SpillCodecTest, selector_converges_to_best) {
    // disk priced at 0.2 bytes/ns (~200MB/s)
    CodecSelector selector(/*column_count=*/1, /*session_encode_level=*/7, /*disk_bytes_per_ns=*/0.2);
    // DELTA: much smaller and cheap -> should win. LEGACY: mid. RAW: baseline.
    for (uint64_t i = 0; i < CodecSelector::kSamples; ++i) {
        ASSERT_TRUE(selector.begin_chunk());
        selector.record_sample(0, {CodecId::RAW, 0}, 1000, 100);      // score 1020
        selector.record_sample(0, {CodecId::INT_DELTA, 0}, 100, 500); // score 200
        selector.record_sample(0, {CodecId::LEGACY, 7}, 500, 300);    // score 560
        selector.finalize_sampling();
    }
    ASSERT_FALSE(selector.begin_chunk()); // locked phase
    ASSERT_EQ(CodecId::INT_DELTA, selector.chosen(0).id);
}

TEST(SpillCodecTest, selector_demotes_to_raw_when_cpu_not_worth_it) {
    CodecSelector selector(1, 7, 0.2);
    for (uint64_t i = 0; i < CodecSelector::kSamples; ++i) {
        ASSERT_TRUE(selector.begin_chunk());
        selector.record_sample(0, {CodecId::RAW, 0}, 1000, 100); // score 1020
        // smaller payload, but the encode CPU costs more than the disk bytes it saves
        selector.record_sample(0, {CodecId::INT_PFOR, 0}, 900, 5000); // score 1900
        selector.finalize_sampling();
    }
    ASSERT_FALSE(selector.begin_chunk());
    ASSERT_EQ(CodecId::RAW, selector.chosen(0).id);
}

TEST(SpillCodecTest, selector_stability_backoff) {
    CodecSelector selector(1, 7, 0.2);
    auto run_window = [&](bool expect_sampled) {
        size_t sampled = 0;
        for (uint64_t i = 0; i < CodecSelector::kWindow; ++i) {
            if (selector.begin_chunk()) {
                ++sampled;
                selector.record_sample(0, {CodecId::RAW, 0}, 1000, 10);
                selector.finalize_sampling();
            }
        }
        ASSERT_EQ(expect_sampled, sampled > 0);
    };
    run_window(true);  // window 0 always samples; decision stays RAW -> stable
    run_window(false); // backoff kicks in: next windows skip sampling
    // decision remained stable, so several subsequent windows stay sampling-free
    run_window(true); // window 2 = 0 + backoff(2^1) -> samples again
    run_window(false);
    run_window(false);
    run_window(false);
    run_window(true); // window 6 = 2 + backoff(2^2)
}

TEST(SpillCodecTest, selector_resamples_when_locked_ratio_degrades) {
    // Trials always score a FRESHLY built context, so the selector never observes a reused context
    // aging inside its own reign -- the backoff would keep ramping while the reused parameters
    // drift out of tune. The locked phase therefore reports what it actually achieved: a material
    // degradation must pull the next sampling window forward, a ratio still on target must not.
    CodecSelector selector(1, 7, 0.2);
    auto run_window = [&](bool expect_sampled) {
        size_t sampled = 0;
        for (uint64_t i = 0; i < CodecSelector::kWindow; ++i) {
            if (selector.begin_chunk()) {
                ++sampled;
                selector.record_sample(0, {CodecId::RAW, 0}, 1000, 10);      // score 2002
                selector.record_sample(0, {CodecId::INT_DELTA, 0}, 400, 10); // score  802
                selector.finalize_sampling();
            }
        }
        ASSERT_EQ(expect_sampled, sampled > 0);
    };
    run_window(true); // window 0: RAW -> DELTA at ratio 0.4, decision changed -> backoff 1
    ASSERT_EQ(CodecId::INT_DELTA, selector.chosen(0).id);
    run_window(true); // window 1: decision stable -> backoff 2, next scheduled window is 3

    // Still on target: 0.42 is inside kRatioDriftFactor (1.15) of the 0.4 baseline.
    selector.report_locked_ratio(0, 420, 1000);
    run_window(false); // window 2 stays skipped, as scheduled
    run_window(true);  // window 3 samples on schedule -> backoff 4, next scheduled window is 7

    // The reused context has drifted: 0.6 is 1.5x the decision-time ratio.
    selector.report_locked_ratio(0, 600, 1000);
    run_window(true); // window 4 is pulled forward by the trigger (schedule said 7)
}

TEST(SpillCodecTest, selector_marginal_gain_prefers_raw) {
    CodecSelector selector(1, 7, 0.2);
    for (uint64_t i = 0; i < CodecSelector::kSamples; ++i) {
        ASSERT_TRUE(selector.begin_chunk());
        selector.record_sample(0, {CodecId::RAW, 0}, 1000, 0);
        // within the 2% hysteresis margin of RAW -> stay RAW
        selector.record_sample(0, {CodecId::LEGACY, 7}, 985, 0);
        selector.finalize_sampling();
    }
    ASSERT_FALSE(selector.begin_chunk());
    ASSERT_EQ(CodecId::RAW, selector.chosen(0).id);
}

// --- corruption / truncation hardening (decoders must never read or write out of bounds) ---

TEST(SpillCodecTest, int_codec_rejects_mismatched_stream_width) {
    // The INT decoder must derive its write type from the destination column, not the stream's
    // self-describing width byte. A non-nullable int32 column lays out tag(0) at offset 0 then
    // the width byte at offset 1; flipping 4->8 previously routed a uint64 write into an
    // int32-sized buffer (2x heap overflow). It must now be rejected as corruption.
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), /*nullable=*/false);
    for (int i = 0; i < 512; ++i) col->append_datum(Datum(static_cast<int32_t>(i * 3 + 1)));

    for (CodecId id : {CodecId::INT_RLE, CodecId::INT_DELTA, CodecId::INT_FOR, CodecId::INT_PFOR}) {
        const auto* codec = CodecRegistry::instance()->get(id);
        int64_t cap = codec->max_encoded_size(*col, 0);
        ASSERT_GT(cap, 0);
        std::string enc;
        enc.resize(cap + 16);
        auto* base = reinterpret_cast<uint8_t*>(enc.data());
        auto e = codec->encode(*col, base, 0);
        ASSERT_TRUE(e.ok()) << e.status();
        size_t n = e.value() - base;
        ASSERT_EQ(4, base[1]) << "expected width byte 4 at offset 1 for an int32 column";
        base[1] = 8; // tamper: claim an 8-byte element width
        auto target = col->clone_empty();
        auto d = codec->decode(base, base + n + 16, target.get());
        ASSERT_FALSE(d.ok()) << "codec " << static_cast<int>(id) << " accepted a mismatched width";
    }
}

TEST(SpillCodecTest, int_codec_rejects_implausible_row_count) {
    // A decoder reads its row/element count from the stream and resizes buffers to it. A corrupt
    // count (e.g. a bit-flipped u32 read as billions) must be rejected before it drives a
    // multi-GB allocation, rather than trusted. For a non-nullable int32 column the layout is
    // tag(0) @0, width @1, rows u32 @2..5 (little-endian); forcing the rows high byte makes the
    // count implausible. Full fuzz-hardening against arbitrary byte corruption is deliberately
    // out of scope for trusted local spill files -- this only pins the allocation-size guard.
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), /*nullable=*/false);
    for (int i = 0; i < 512; ++i) col->append_datum(Datum(static_cast<int32_t>(i)));

    for (CodecId id : {CodecId::INT_RLE, CodecId::INT_DELTA, CodecId::INT_FOR, CodecId::INT_PFOR}) {
        const auto* codec = CodecRegistry::instance()->get(id);
        int64_t cap = codec->max_encoded_size(*col, 0);
        ASSERT_GT(cap, 0);
        std::string enc;
        enc.resize(cap + 16);
        auto* base = reinterpret_cast<uint8_t*>(enc.data());
        auto e = codec->encode(*col, base, 0);
        ASSERT_TRUE(e.ok()) << e.status();
        size_t n = e.value() - base;
        ASSERT_EQ(4, base[1]) << "expected width byte 4 at offset 1"; // keep width valid
        base[5] = 0xFF; // rows high byte -> count ~4.28e9, far above kMaxDecodeCount
        auto target = col->clone_empty();
        auto d = codec->decode(base, base + n + 16, target.get());
        ASSERT_FALSE(d.ok()) << "codec " << static_cast<int>(id) << " trusted an implausible row count";
    }
}

} // namespace starrocks::spill
