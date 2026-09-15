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

#include "storage/sort_key_sampler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "common/config.h"
#include "fs/fs_memory.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "storage/chunk_helper.h"
#include "storage/decimal12.h"
#include "storage/key_coder.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/seek_tuple.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage/uint24.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {

// Every type the predicate accepts must satisfy: short_key_encode's bytes == the full
// order-preserving encoding's bytes. If a future type's encode_ascending stops tail-calling
// full_encode_ascending, or a fifth delegate_type entry appears, this test fails instead of the
// split boundaries silently going wrong. See spec facts 1-4.
TEST(SortKeySamplerTest, accepted_types_encode_identically_to_full_encoding) {
    struct Case {
        LogicalType type;
        Datum value;
    };
    // All 17 accepted types, not a sample of them: this test is the only thing standing between a
    // future coder change and silently wrong split boundaries, so an unlisted type is a hole in it.
    // Values are deliberately negative or high-bit-set where the type allows, because the
    // sign-flip in the integral encoder is where an asymmetry would show up.
    // Note on the int128 values: write them as -(int128_t{1} << 90), never
    // static_cast<int128_t>(-1) << 90 -- left-shifting a negative signed value is undefined
    // behaviour, and an ASAN/UBSAN test build will trap on it.
    const std::vector<Case> accepted = {
            {TYPE_BOOLEAN, Datum(static_cast<int8_t>(1))},
            {TYPE_TINYINT, Datum(static_cast<int8_t>(-7))},
            {TYPE_SMALLINT, Datum(static_cast<int16_t>(-300))},
            {TYPE_INT, Datum(static_cast<int32_t>(-70000))},
            {TYPE_UNSIGNED_INT, Datum(static_cast<uint32_t>(4000000000u))},
            {TYPE_BIGINT, Datum(static_cast<int64_t>(-5000000000LL))},
            {TYPE_UNSIGNED_BIGINT, Datum(static_cast<uint64_t>(18000000000000000000ull))},
            {TYPE_LARGEINT, Datum(-(int128_t{1} << 90))},
            {TYPE_DATE, Datum(static_cast<int32_t>(20260907))},
            {TYPE_DATE_V1, Datum(uint24_t(1234567))},
            {TYPE_DATETIME, Datum(static_cast<int64_t>(20260907101112LL))},
            {TYPE_DATETIME_V1, Datum(static_cast<int64_t>(20260907101112LL))},
            {TYPE_DECIMAL, Datum(decimal12_t(-12345, -678))},
            {TYPE_DECIMALV2, Datum(DecimalV2Value(std::string("-123.456")))},
            {TYPE_DECIMAL32, Datum(static_cast<int32_t>(-70000))},
            {TYPE_DECIMAL64, Datum(static_cast<int64_t>(-5000000000LL))},
            {TYPE_DECIMAL128, Datum(-(int128_t{1} << 90))},
    };
    ASSERT_EQ(17u, accepted.size()) << "every accepted type must be covered";
    for (const auto& c : accepted) {
        const size_t width = sort_key_fixed_encode_size(c.type);
        ASSERT_GT(width, 0u) << "type " << c.type << " must be accepted";
        const KeyCoder* coder = get_key_coder(c.type);
        ASSERT_NE(coder, nullptr) << "type " << c.type;

        std::string short_bytes;
        coder->encode_ascending(c.value, width, &short_bytes);
        std::string full_bytes;
        coder->full_encode_ascending(c.value, &full_bytes);
        EXPECT_EQ(full_bytes, short_bytes) << "type " << c.type << " diverges";
        EXPECT_EQ(width, full_bytes.size()) << "type " << c.type << " width mismatch";
    }
}

// The four families whose short-key bytes cannot be decoded back: CHAR pads, VARCHAR/VARBINARY
// truncate, and INT256/DECIMAL256 have an empty short-key encoder. See spec fact 2.
TEST(SortKeySamplerTest, undecodable_types_are_rejected) {
    for (LogicalType t : {TYPE_CHAR, TYPE_VARCHAR, TYPE_VARBINARY, TYPE_INT256, TYPE_DECIMAL256}) {
        EXPECT_EQ(0u, sort_key_fixed_encode_size(t)) << "type " << t << " must be rejected";
    }
}

// DECIMAL32/64/128 reach their coder through delegate_type, so they must report the DELEGATED
// width, not a nominal one. See spec fact 4.
TEST(SortKeySamplerTest, delegating_types_report_delegated_width) {
    EXPECT_EQ(sort_key_fixed_encode_size(TYPE_INT), sort_key_fixed_encode_size(TYPE_DECIMAL32));
    EXPECT_EQ(sort_key_fixed_encode_size(TYPE_BIGINT), sort_key_fixed_encode_size(TYPE_DECIMAL64));
    EXPECT_EQ(sort_key_fixed_encode_size(TYPE_LARGEINT), sort_key_fixed_encode_size(TYPE_DECIMAL128));
    EXPECT_EQ(0u, sort_key_fixed_encode_size(TYPE_DECIMAL256));
}

// Helper: a Schema of |types| whose fields are all key columns.
static Schema make_schema(const std::vector<LogicalType>& types) {
    Fields fields;
    for (size_t i = 0; i < types.size(); ++i) {
        auto f = std::make_shared<Field>(
                static_cast<ColumnId>(i), std::string("c") + std::to_string(i), get_type_info(types[i]),
                STORAGE_AGGREGATE_NONE, static_cast<uint8_t>(std::max<size_t>(1, sort_key_fixed_encode_size(types[i]))),
                /*is_key=*/true, /*nullable=*/false);
        fields.emplace_back(std::move(f));
    }
    return Schema(std::move(fields), KeysType::DUP_KEYS, {});
}

// Helper: the nullable twin of make_schema, for the NULL-column decode test.
static Schema make_nullable_schema(const std::vector<LogicalType>& types) {
    Fields fields;
    for (size_t i = 0; i < types.size(); ++i) {
        auto f = std::make_shared<Field>(
                static_cast<ColumnId>(i), std::string("c") + std::to_string(i), get_type_info(types[i]),
                STORAGE_AGGREGATE_NONE, static_cast<uint8_t>(std::max<size_t>(1, sort_key_fixed_encode_size(types[i]))),
                /*is_key=*/true, /*nullable=*/true);
        fields.emplace_back(std::move(f));
    }
    return Schema(std::move(fields), KeysType::DUP_KEYS, {});
}

// The decoder must round-trip what SeekTuple::short_key_encode actually writes, and must preserve
// the LOGICAL type -- a DECIMAL32 decoded as INT would DCHECK inside DatumVariant::compare the
// moment it is compared with a tuple built from the schema.
TEST(SortKeySamplerTest, decode_short_key_entry_round_trips_and_keeps_logical_types) {
    const auto schema = make_schema({TYPE_INT, TYPE_DECIMAL32, TYPE_DATE});
    std::vector<Datum> values(3);
    values[0] = Datum(static_cast<int32_t>(-70000));
    values[1] = Datum(static_cast<int32_t>(12345)); // DECIMAL32 storage is int32
    values[2] = Datum(static_cast<int32_t>(20260907));
    SeekTuple tuple(schema, std::move(values));
    const std::string encoded = tuple.short_key_encode(3, {0, 1, 2}, 0);

    VariantTuple decoded;
    ASSERT_OK(decode_short_key_entry(Slice(encoded), schema, {0, 1, 2}, &decoded));
    ASSERT_EQ(3u, decoded.size());
    // Logical types preserved, so comparing against a schema-built tuple is well-defined.
    EXPECT_EQ(TYPE_INT, decoded[0].type()->type());
    EXPECT_EQ(TYPE_DECIMAL32, decoded[1].type()->type());
    EXPECT_EQ(TYPE_DATE, decoded[2].type()->type());
    EXPECT_EQ(-70000, decoded[0].value().get_int32());
    EXPECT_EQ(12345, decoded[1].value().get_int32());
    EXPECT_EQ(20260907, decoded[2].value().get_int32());
}

// A NULL column is a bare marker byte with no payload, in both encoder and decoder.
TEST(SortKeySamplerTest, decode_short_key_entry_handles_nulls) {
    const auto schema = make_nullable_schema({TYPE_INT, TYPE_BIGINT});
    std::vector<Datum> values(2);
    values[0] = Datum(); // null
    values[1] = Datum(static_cast<int64_t>(7));
    SeekTuple tuple(schema, std::move(values));
    const std::string encoded = tuple.short_key_encode(2, {0, 1}, 0);

    VariantTuple decoded;
    ASSERT_OK(decode_short_key_entry(Slice(encoded), schema, {0, 1}, &decoded));
    ASSERT_EQ(2u, decoded.size());
    EXPECT_TRUE(decoded[0].value().is_null());
    EXPECT_EQ(7, decoded[1].value().get_int64());
}

// INT256's short-key encoder writes nothing, so the predicate rejects it and the decoder must
// refuse rather than consume 32 bytes that were never written.
TEST(SortKeySamplerTest, decode_short_key_entry_refuses_a_type_the_predicate_rejects) {
    const auto schema = make_schema({TYPE_INT256});
    VariantTuple decoded;
    // The marker byte MUST be KEY_NORMAL_MARKER, or the decoder returns at its bad-marker check
    // and never reaches the type-dispatch default arm this test exists to cover -- the test would
    // then pass even if that arm were deleted.
    std::string bogus(33, '\0');
    bogus[0] = static_cast<char>(KEY_NORMAL_MARKER);
    EXPECT_FALSE(decode_short_key_entry(Slice(bogus), schema, {0}, &decoded).ok());
}

TEST(SortKeySamplerTest, predicate_requires_full_arity_and_fixed_types) {
    const auto three_ints = make_schema({TYPE_INT, TYPE_INT, TYPE_INT});
    // Covered: the short key spans every sort key column and all are fixed-size.
    EXPECT_TRUE(short_key_index_encodes_full_sort_key(three_ints, {0, 1, 2}, 3));
    // Truncated arity: the default short_key = 3 cannot cover a 4-column sort key.
    const auto four_ints = make_schema({TYPE_INT, TYPE_INT, TYPE_INT, TYPE_INT});
    EXPECT_FALSE(short_key_index_encodes_full_sort_key(four_ints, {0, 1, 2, 3}, 3));
    // A VARCHAR anywhere in the sort key disqualifies it, including as the last short key column.
    const auto int_varchar = make_schema({TYPE_INT, TYPE_VARCHAR});
    EXPECT_FALSE(short_key_index_encodes_full_sort_key(int_varchar, {0, 1}, 2));
    // CHAR likewise.
    const auto int_char = make_schema({TYPE_INT, TYPE_CHAR});
    EXPECT_FALSE(short_key_index_encodes_full_sort_key(int_char, {0, 1}, 2));
    // Empty sort key.
    EXPECT_FALSE(short_key_index_encodes_full_sort_key(three_ints, {}, 0));
    // Out-of-range column id must be rejected rather than indexed.
    EXPECT_FALSE(short_key_index_encodes_full_sort_key(three_ints, {0, 1, 99}, 3));
}

TEST(SortKeySamplerTest, budget_is_proportional_to_rows) {
    // 10 equal segments, 2-way split: target_total = min(1024, 32*2) = 64, so each floor is
    // 64 * 1000 / 10000 == 6 and 4 samples are left over. Those 4 are handed out rather than
    // discarded -- an earlier version of this test asserted a flat 6 everywhere on the reasoning
    // that "the cap is an upper bound, not a quota to exhaust", and that reasoning is what let a
    // tablet with more segments than samples floor to a budget of zero everywhere.
    //
    // Equal segments produce exactly equal remainders, so the tie-break decides who gets the extra:
    // lowest index first. Asserting the exact vector, not just the total, is what pins that
    // determinism -- the allocation must not depend on nth_element's ordering among equivalents.
    const std::vector<int64_t> ten_equal(10, 1000);
    auto budget = allocate_sort_key_sample_budget(ten_equal, /*split_width=*/2);
    ASSERT_EQ(10u, budget.size());
    const std::vector<int64_t> expected{7, 7, 7, 7, 6, 6, 6, 6, 6, 6};
    EXPECT_EQ(expected, budget);
    int64_t total = 0;
    for (int64_t n : budget) {
        total += n;
    }
    EXPECT_EQ(64, total) << "the residual must be spent, not floored away";
}

// A single segment must receive the whole tablet target: it is the only claimant, and a 1024-way
// split of it needs 1023 interior boundary points.
TEST(SortKeySamplerTest, budget_gives_a_single_segment_the_whole_target) {
    auto budget = allocate_sort_key_sample_budget({1'000'000}, /*split_width=*/1024);
    ASSERT_EQ(1u, budget.size());
    EXPECT_EQ(1024, budget[0]) << "a lone segment must not be capped below the tablet target";
}

TEST(SortKeySamplerTest, budget_scales_with_split_width) {
    // Narrow splits must not burn the whole cap: 1 segment, 2-way -> min(1024, 64) = 64.
    auto narrow = allocate_sort_key_sample_budget({1'000'000}, /*split_width=*/2);
    EXPECT_EQ(64, narrow[0]);
    auto wide = allocate_sort_key_sample_budget({1'000'000}, /*split_width=*/32);
    EXPECT_EQ(1024, wide[0]);
}

// Asserting EQUALITY with the cap, not `<=`: an earlier `<=` here was vacuous. 2000 equal segments
// against a target of 1024 floor to 1024*1000/2000000 == 0 each, so the whole allocation was zero
// and the assertion still passed -- it could not distinguish "bounded by the cap" from "sampling
// silently switched off".
TEST(SortKeySamplerTest, budget_total_spends_the_cap_and_never_exceeds_it) {
    const std::vector<int64_t> many(2000, 1000);
    auto budget = allocate_sort_key_sample_budget(many, /*split_width=*/1024);
    int64_t total = 0;
    for (int64_t n : budget) {
        EXPECT_GE(n, 0);
        total += n;
    }
    EXPECT_EQ(config::sort_key_max_samples_per_tablet, total);
    // 2000 segments sharing 1024 samples: the residual goes to whole segments, so 1024 of them get
    // exactly one and the rest stay coarse. Nothing may receive two while another receives none.
    int64_t ones = 0;
    for (int64_t n : budget) {
        EXPECT_LE(n, 1);
        ones += (n == 1);
    }
    EXPECT_EQ(config::sort_key_max_samples_per_tablet, ones);
}

// More segments than samples is the shape flooring lost entirely: with the default cap a 2-way split
// targets 32*2 == 64 samples, and 65 equal segments each floor to 64/65 == 0. Every rowset then
// fails rowset_wants_samples and the tablet is sampled not at all -- worst on exactly the fragmented
// tablets that need boundaries most.
TEST(SortKeySamplerTest, budget_survives_more_segments_than_samples) {
    const std::vector<int64_t> many(65, 1000);
    auto budget = allocate_sort_key_sample_budget(many, /*split_width=*/2);
    ASSERT_EQ(65u, budget.size());
    int64_t total = 0;
    int64_t sampled_segments = 0;
    for (int64_t n : budget) {
        EXPECT_GE(n, 0);
        total += n;
        sampled_segments += (n > 0);
    }
    EXPECT_EQ(64, total) << "the whole 32 * split_width target must be spent, not floored away";
    EXPECT_EQ(64, sampled_segments) << "64 of the 65 segments must be sampled, one sample each";
}

// When floors tie at zero, the residual must follow segment SIZE rather than position. 50 segments
// of 1000 rows and 50 of 1001, against a target of 64: the total is 100050, so every share floors to
// zero (a floor of 1 would need more than 1563 rows) and the entire budget is residual. The 1001-row
// segments carry the larger remainder (64064 vs 64000), so all 50 of them must be served before any
// 1000-row segment is. Placing the larger segments SECOND is deliberate: a first-come-first-served
// distribution would serve the 1000-row ones and pass a size-blind assertion.
TEST(SortKeySamplerTest, budget_residual_follows_the_largest_remainders) {
    std::vector<int64_t> rows(50, 1000);
    rows.insert(rows.end(), 50, 1001);
    auto budget = allocate_sort_key_sample_budget(rows, /*split_width=*/2);
    ASSERT_EQ(100u, budget.size());

    int64_t total = 0;
    int64_t served_small = 0;
    for (size_t i = 0; i < budget.size(); ++i) {
        EXPECT_LE(budget[i], 1) << "at index " << i;
        total += budget[i];
        if (i >= 50) {
            EXPECT_EQ(1, budget[i]) << "every larger segment must be served first, missed index " << i;
        } else {
            served_small += budget[i];
        }
    }
    EXPECT_EQ(64, total);
    EXPECT_EQ(14, served_small) << "the 64 samples are 50 larger segments plus 14 of the smaller";
}

TEST(SortKeySamplerTest, budget_favours_large_segments) {
    auto budget = allocate_sort_key_sample_budget({100, 100, 999'800}, /*split_width=*/1024);
    ASSERT_EQ(3u, budget.size());
    EXPECT_GT(budget[2], budget[0]);
    EXPECT_GT(budget[2], budget[1]);
}

TEST(SortKeySamplerTest, budget_zero_cap_disables_sampling) {
    const auto saved = config::sort_key_max_samples_per_tablet;
    DeferOp restore([&] { config::sort_key_max_samples_per_tablet = saved; });
    config::sort_key_max_samples_per_tablet = 0;
    auto budget = allocate_sort_key_sample_budget({1000, 2000}, /*split_width=*/4);
    for (int64_t n : budget) {
        EXPECT_EQ(0, n);
    }
}

// Malformed metadata must not make the arithmetic undefined. int64 row counts near the maximum
// would overflow a signed 64-bit accumulate/multiply; the __int128 intermediates make this defined.
TEST(SortKeySamplerTest, budget_survives_extreme_row_counts) {
    const int64_t huge = std::numeric_limits<int64_t>::max() / 2;
    auto budget = allocate_sort_key_sample_budget({huge, huge}, /*split_width=*/1024);
    ASSERT_EQ(2u, budget.size());
    int64_t total = 0;
    for (int64_t n : budget) {
        EXPECT_GE(n, 0);
        total += n;
    }
    EXPECT_LE(total, config::sort_key_max_samples_per_tablet);
}

TEST(SortKeySamplerTest, budget_handles_empty_and_zero_row_segments) {
    EXPECT_TRUE(allocate_sort_key_sample_budget({}, 4).empty());
    auto budget = allocate_sort_key_sample_budget({0, 0}, 4);
    ASSERT_EQ(2u, budget.size());
    EXPECT_EQ(0, budget[0]);
    EXPECT_EQ(0, budget[1]);
}

// A negative cap is a different failure mode from a zero one: cap == 0 makes target_total collapse
// to 0 through the later min() clamps regardless of the early return, but cap == -1 makes
// target_total == -32 (min(-1, 32*-1)) and every share negative -- and sort_key_max_samples_per_tablet
// is CONF_mInt32, i.e. runtime-mutable, so an operator can set it negative on a live BE. Asserting
// == 0 (not <= 0) is required: a <= 0 assertion would pass on the very negative values this guard
// exists to prevent.
TEST(SortKeySamplerTest, budget_negative_cap_disables_sampling) {
    const auto saved = config::sort_key_max_samples_per_tablet;
    DeferOp restore([&] { config::sort_key_max_samples_per_tablet = saved; });
    config::sort_key_max_samples_per_tablet = -1;
    auto budget = allocate_sort_key_sample_budget({1000, 2000}, /*split_width=*/4);
    for (int64_t n : budget) {
        EXPECT_EQ(0, n);
    }
}

// split_width == 0 is NOT the case that falsifies this guard: it yields target_total == 0 through
// the same min() collapse as a zero cap, and passes with or without the early return. split_width
// == 1 is the case that matters: bounded_width == 1 gives target_total == min(cap, 32) == 32, a real
// budget that a 1-way "split" has no use for.
TEST(SortKeySamplerTest, budget_single_way_split_disables_sampling) {
    auto budget = allocate_sort_key_sample_budget({1000, 2000}, /*split_width=*/1);
    for (int64_t n : budget) {
        EXPECT_EQ(0, n);
    }
}

// segment_num_rows comes from SegmentMetadataPB, i.e. from object storage: a negative entry must
// not produce a negative budget for that segment, and must not perturb the other segments' shares
// (total_rows only accumulates rows > 0, so the negative entry is already excluded from the
// denominator; this pins that the numerator side is equally excluded). The negative magnitude has
// to be large enough that (target_total * rows) / total_rows does not truncate to 0 anyway -- a
// small negative like -5 here would pass with or without the skip, for the same reason a cap of
// exactly 0 is unobservable elsewhere in this file.
TEST(SortKeySamplerTest, budget_skips_negative_row_segments) {
    auto baseline = allocate_sort_key_sample_budget({1000, 2000}, /*split_width=*/4);
    auto budget = allocate_sort_key_sample_budget({1000, -500'000, 2000}, /*split_width=*/4);
    ASSERT_EQ(3u, budget.size());
    EXPECT_EQ(0, budget[1]);
    EXPECT_EQ(baseline[0], budget[0]);
    EXPECT_EQ(baseline[1], budget[2]);
}

// Helper: a VariantTuple of TYPE_INT values, for building sort key bounds/expectations in the
// path A tests below.
static VariantTuple tuple_of_ints(std::initializer_list<int32_t> values) {
    VariantTuple t;
    for (int32_t v : values) {
        t.emplace(get_type_info(TYPE_INT), Datum(v));
    }
    return t;
}

// ---------------------------------------------------------------------------
// Path A: sampling straight out of a real segment's short key index, built with SegmentWriter so
// the block geometry (num_items, num_rows_per_block) is genuine rather than
// mocked. Fixture style follows segment_writer_short_key_encode_test.cpp.
// ---------------------------------------------------------------------------
class SortKeySamplerSegmentTest : public ::testing::Test {
protected:
    // A 3-column INT sort key, matching _schema below (sort_key_idxes = {0, 1, 2}).
    std::shared_ptr<Segment> write_int_key_segment(int64_t num_rows, uint32_t rows_per_block) {
        auto tablet_schema = TabletSchemaHelper::create_tablet_schema(
                {create_int_key_pb(1, /*is_nullable=*/false), create_int_key_pb(2, /*is_nullable=*/false),
                 create_int_key_pb(3, /*is_nullable=*/false)},
                /*num_short_key_columns=*/3);
        std::shared_ptr<TabletSchema> schema(std::move(tablet_schema));

        auto fs = std::make_shared<MemoryFileSystem>();
        const std::string dir = "/sort_key_sampler_test";
        CHECK(fs->create_dir(dir).ok());
        SegmentWriterOptions opts;
        opts.num_rows_per_block = rows_per_block;
        std::string filename = strings::Substitute("$0/seg.dat", dir);
        auto wfile_or = fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile_or.value()), /*segment_id=*/0, schema, opts);
        CHECK(writer->init(true).ok());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            for (auto& col : cols) {
                col->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
            }
        }
        CHECK(writer->append_chunk(*chunk).ok());
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK(writer->finalize(&file_size, &index_size, &footer_position).ok());

        auto segment_or = Segment::open(fs, FileInfo{writer->segment_path()}, 0, schema);
        CHECK(segment_or.ok()) << segment_or.status().to_string();
        return std::move(segment_or.value());
    }

    // One INT key column (unique_id 1), ascending 0..num_rows-1. Sets _tablet_schema. Unlike
    // write_int_key_segment above (3 columns), this is the ONE-column segment path B's
    // missing-column test needs.
    std::shared_ptr<Segment> write_single_int_key_segment(int64_t num_rows) {
        auto tablet_schema = TabletSchemaHelper::create_tablet_schema({create_int_key_pb(1, /*is_nullable=*/false)});
        std::shared_ptr<TabletSchema> schema(std::move(tablet_schema));
        _tablet_schema = schema;

        _fs = std::make_shared<MemoryFileSystem>();
        const std::string dir = "/sort_key_sampler_test";
        CHECK(_fs->create_dir(dir).ok());
        SegmentWriterOptions opts;
        std::string filename = strings::Substitute("$0/seg.dat", dir);
        auto wfile_or = _fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile_or.value()), /*segment_id=*/0, schema, opts);
        CHECK(writer->init(true).ok());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        }
        CHECK(writer->append_chunk(*chunk).ok());
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK(writer->finalize(&file_size, &index_size, &footer_position).ok());

        auto segment_or = Segment::open(_fs, FileInfo{writer->segment_path()}, 0, schema);
        CHECK(segment_or.ok()) << segment_or.status().to_string();
        return std::move(segment_or.value());
    }

    // Two INT key columns (unique_ids 1, 2) holding DIFFERENT values per row: col0(i) = 2*i,
    // col1(i) = 2*i + 1. Unlike write_int_key_segment's 3 IDENTICAL columns (every column holds the
    // same value per row, so permuting positions among them is undetectable), same-type-different-
    // value columns make a permuted sort_key_idxes parameter produce OBSERVABLY wrong output
    // instead of silently identical output. Sets _tablet_schema.
    std::shared_ptr<Segment> write_two_int_key_segment(int64_t num_rows) {
        auto tablet_schema = TabletSchemaHelper::create_tablet_schema(
                {create_int_key_pb(1, /*is_nullable=*/false), create_int_key_pb(2, /*is_nullable=*/false)});
        std::shared_ptr<TabletSchema> schema(std::move(tablet_schema));
        _tablet_schema = schema;

        _fs = std::make_shared<MemoryFileSystem>();
        const std::string dir = "/sort_key_sampler_test";
        CHECK(_fs->create_dir(dir).ok());
        SegmentWriterOptions opts;
        std::string filename = strings::Substitute("$0/seg.dat", dir);
        auto wfile_or = _fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile_or.value()), /*segment_id=*/0, schema, opts);
        CHECK(writer->init(true).ok());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(2 * i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(2 * i + 1)));
        }
        CHECK(writer->append_chunk(*chunk).ok());
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK(writer->finalize(&file_size, &index_size, &footer_position).ok());

        auto segment_or = Segment::open(_fs, FileInfo{writer->segment_path()}, 0, schema);
        CHECK(segment_or.ok()) << segment_or.status().to_string();
        return std::move(segment_or.value());
    }

    // A schema ONE key column WIDER than write_single_int_key_segment's output (unique_ids 1, 2),
    // so segment.new_column_iterator() returns NotFound for the trailing column -- as if this
    // segment predates a trailing key-column add. Independent of _tablet_schema: the test passes
    // this schema directly rather than through the fixture-held one.
    TabletSchemaCSPtr tablet_schema_with_extra_trailing_key_column() {
        auto tablet_schema = TabletSchemaHelper::create_tablet_schema(
                {create_int_key_pb(1, /*is_nullable=*/false), create_int_key_pb(2, /*is_nullable=*/false)});
        return std::shared_ptr<TabletSchema>(std::move(tablet_schema));
    }

    // (int, varchar) sort key: unique_ids 1 (int) and 2 (varchar), ASCENDING. Sets _tablet_schema
    // and _varchar_values -- row i's varchar bytes must outlive this call, since Datum/Slice do not
    // own the string data, so they live on the fixture for as long as the test needs them.
    std::shared_ptr<Segment> write_int_varchar_key_segment(int64_t num_rows) {
        ColumnPB varchar_col;
        varchar_col.set_unique_id(2);
        varchar_col.set_name("2");
        varchar_col.set_type("VARCHAR");
        varchar_col.set_is_key(true);
        varchar_col.set_is_nullable(false);
        varchar_col.set_length(32);
        varchar_col.set_index_length(4);

        auto tablet_schema =
                TabletSchemaHelper::create_tablet_schema({create_int_key_pb(1, /*is_nullable=*/false), varchar_col});
        std::shared_ptr<TabletSchema> schema(std::move(tablet_schema));
        _tablet_schema = schema;

        _fs = std::make_shared<MemoryFileSystem>();
        const std::string dir = "/sort_key_sampler_test";
        CHECK(_fs->create_dir(dir).ok());
        SegmentWriterOptions opts;
        std::string filename = strings::Substitute("$0/seg.dat", dir);
        auto wfile_or = _fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile_or.value()), /*segment_id=*/0, schema, opts);
        CHECK(writer->init(true).ok());

        _varchar_values.resize(num_rows);
        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            _varchar_values[i] = "v" + std::to_string(i);
            // The int column alone is strictly ascending, so the (int, varchar) tuple is already
            // non-decreasing regardless of the varchar content.
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(Slice(_varchar_values[i])));
        }
        CHECK(writer->append_chunk(*chunk).ok());
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK(writer->finalize(&file_size, &index_size, &footer_position).ok());

        auto segment_or = Segment::open(_fs, FileInfo{writer->segment_path()}, 0, schema);
        CHECK(segment_or.ok()) << segment_or.status().to_string();
        return std::move(segment_or.value());
    }

    // The VariantTuple for row i of the segment write_int_varchar_key_segment just wrote, built
    // from the values actually appended.
    VariantTuple int_varchar_tuple(int64_t i) {
        VariantTuple t;
        t.emplace(get_type_info(TYPE_INT), Datum(static_cast<int32_t>(i)));
        t.emplace(get_type_info(TYPE_VARCHAR), Datum(Slice(_varchar_values[i])));
        return t;
    }

    // One nullable INT key column (unique_id 1): a leading run of NULLs, then ascending ints.
    // NULLs sort first, so the column stays non-decreasing while still putting NULLs on some data
    // pages and non-NULLs on others. Sets _tablet_schema.
    std::shared_ptr<Segment> write_nullable_int_key_segment(int64_t num_rows, int64_t num_leading_nulls) {
        auto tablet_schema = TabletSchemaHelper::create_tablet_schema({create_int_key_pb(1, /*is_nullable=*/true)});
        std::shared_ptr<TabletSchema> schema(std::move(tablet_schema));
        _tablet_schema = schema;

        _fs = std::make_shared<MemoryFileSystem>();
        const std::string dir = "/sort_key_sampler_test";
        CHECK(_fs->create_dir(dir).ok());
        SegmentWriterOptions opts;
        std::string filename = strings::Substitute("$0/seg.dat", dir);
        auto wfile_or = _fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile_or.value()), /*segment_id=*/0, schema, opts);
        CHECK(writer->init(true).ok());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            if (i < num_leading_nulls) {
                cols[0]->as_mutable_ptr()->append_datum(Datum());
            } else {
                cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i - num_leading_nulls)));
            }
        }
        CHECK(writer->append_chunk(*chunk).ok());
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK(writer->finalize(&file_size, &index_size, &footer_position).ok());

        auto segment_or = Segment::open(_fs, FileInfo{writer->segment_path()}, 0, schema);
        CHECK(segment_or.ok()) << segment_or.status().to_string();
        return std::move(segment_or.value());
    }

    // One CHAR(char_len) key column (unique_id 1). Values are written in ascending LEXICOGRAPHIC
    // (raw byte) order, not numeric order, since SegmentWriter assumes append order is the true
    // sort order for a sort key column. _first_written_key/_last_written_key are the tuples for
    // the actual first/last rows written -- never index arithmetic, since CHAR compares
    // lexicographically (over "0".."499" the written maximum is "99", not "499"). Sets
    // _tablet_schema.
    std::shared_ptr<Segment> write_char_key_segment(int64_t num_rows, int char_len) {
        ColumnPB char_col;
        char_col.set_unique_id(1);
        char_col.set_name("1");
        char_col.set_type("CHAR");
        char_col.set_is_key(true);
        char_col.set_is_nullable(false);
        char_col.set_length(char_len);
        char_col.set_index_length(char_len);

        auto tablet_schema = TabletSchemaHelper::create_tablet_schema({char_col});
        std::shared_ptr<TabletSchema> schema(std::move(tablet_schema));
        _tablet_schema = schema;

        std::vector<std::string> values(num_rows);
        for (int64_t i = 0; i < num_rows; ++i) {
            values[i] = std::to_string(i);
        }
        std::sort(values.begin(), values.end());

        _fs = std::make_shared<MemoryFileSystem>();
        const std::string dir = "/sort_key_sampler_test";
        CHECK(_fs->create_dir(dir).ok());
        SegmentWriterOptions opts;
        std::string filename = strings::Substitute("$0/seg.dat", dir);
        auto wfile_or = _fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile_or.value()), /*segment_id=*/0, schema, opts);
        CHECK(writer->init(true).ok());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(Slice(values[i])));
        }
        CHECK(writer->append_chunk(*chunk).ok());
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK(writer->finalize(&file_size, &index_size, &footer_position).ok());

        // Zero-pad to char_len: in production these bounds come from SegmentWriter's sort_key_min/
        // sort_key_max, built from an ALREADY-PADDED chunk (ChunkHelper::padding_char_columns,
        // OlapTableSink::_padding_char_column; SegmentWriter::append_chunk). Padding preserves
        // lexicographic order so the test would pass either way, but leaving these raw would never
        // exercise the reason the padding exists.
        _first_written_key_bytes = values.front();
        _first_written_key_bytes.resize(char_len, '\0');
        _last_written_key_bytes = values.back();
        _last_written_key_bytes.resize(char_len, '\0');
        _first_written_key.clear();
        _first_written_key.emplace(get_type_info(TYPE_CHAR), Datum(Slice(_first_written_key_bytes)));
        _last_written_key.clear();
        _last_written_key.emplace(get_type_info(TYPE_CHAR), Datum(Slice(_last_written_key_bytes)));

        auto segment_or = Segment::open(_fs, FileInfo{writer->segment_path()}, 0, schema);
        CHECK(segment_or.ok()) << segment_or.status().to_string();
        return std::move(segment_or.value());
    }

    // Deletes the segment's file out from under it, so any read that reopens it (e.g.
    // new_segment_read_file) fails deterministically. Requires the fixture's own _fs, since
    // MemoryFileSystem has no truncate.
    void corrupt_segment_file(const std::string& path) {
        ASSERT_TRUE(_fs != nullptr);
        ASSERT_OK(_fs->delete_file(path));
    }

    // Test-only: makes the NEXT call to sample_sort_key_from_short_key_index observe a block_rows
    // value that disagrees with the segment's true entry count, without hand-doctoring footer
    // bytes. Pairs with a FAIL_POINT_TRIGGER_EXECUTE block in sort_key_sampler.cpp that halves
    // whatever block_rows it just read -- so |new_block_rows| must be exactly half of the
    // segment's real num_rows_per_block(), enforced here so a future caller cannot pass a value
    // the failpoint would not actually produce.
    //
    // The guard is kept alive on the fixture (not as a local here) so it stays armed across the
    // return from this function into the caller's next statement -- a ScopedFailPoint only fires
    // while a FAIL_POINT_SCOPE guard for its name is live, and self-disarms (erasing itself from
    // the scope set) the first time it fires. Storing it on the fixture means it is torn down for
    // free when the TEST_F instance is destroyed, with no manual DISABLE and no risk of leaking
    // into a later test.
    void perturb_short_key_footer_block_rows(Segment* segment, int64_t new_block_rows) {
#ifdef FIU_ENABLE
        ASSERT_EQ(new_block_rows * 2, static_cast<int64_t>(segment->decoder()->num_rows_per_block()));
        auto* fp = failpoint::FailPointRegistry::GetInstance()->get("sort_key_sampler_perturb_block_rows");
        ASSERT_NE(fp, nullptr);
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        fp->setMode(trigger_mode);
        _perturb_block_rows_guard =
                std::make_unique<failpoint::ScopedFailPointGuard>("sort_key_sampler_perturb_block_rows");
#else
        GTEST_SKIP() << "requires a FIU_ENABLE build to perturb the short key footer";
#endif
    }

    Schema _schema = make_schema({TYPE_INT, TYPE_INT, TYPE_INT});

    // Path B fixture state: the schema and filesystem for the segment the current test wrote, plus
    // the durable byte storage backing the VariantTuples/Slices handed back to path B tests (Datum
    // does not own string data, so it must outlive the writer call that built it).
    TabletSchemaCSPtr _tablet_schema;
    std::shared_ptr<MemoryFileSystem> _fs;
    std::vector<std::string> _varchar_values;
    std::string _first_written_key_bytes;
    std::string _last_written_key_bytes;
    VariantTuple _first_written_key;
    VariantTuple _last_written_key;

#ifdef FIU_ENABLE
    std::unique_ptr<failpoint::ScopedFailPointGuard> _perturb_block_rows_guard;
#endif
};

// A 3-int sort key, 8192 rows, 1024 rows/block -> 8 index entries: entry 0 is row 0 and entries
// 1..7 are candidate samples.
TEST_F(SortKeySamplerSegmentTest, path_a_samples_are_block_boundaries_at_a_uniform_stride) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());

    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({8191, 8191, 8191}), /*num_rows=*/8192, /*target=*/3));
    // 7 candidates, target 3 -> stride ceil(7/3) = 3 -> entries 3 and 6.
    ASSERT_EQ(2u, result.samples.size());
    EXPECT_EQ(3 * 1024, result.row_interval);
    EXPECT_EQ(tuple_of_ints({3072, 3072, 3072}), result.samples[0]);
    EXPECT_EQ(tuple_of_ints({6144, 6144, 6144}), result.samples[1]);
    EXPECT_LT(static_cast<int64_t>(result.samples.size()) * result.row_interval, 8192);
}

TEST_F(SortKeySamplerSegmentTest, path_a_target_at_least_candidate_count_uses_every_entry) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({8191, 8191, 8191}), /*num_rows=*/8192, /*target=*/100));
    EXPECT_EQ(7u, result.samples.size());
    EXPECT_EQ(1024, result.row_interval);
    EXPECT_LT(7 * 1024, 8192);
}

// num_rows an exact multiple of the block size is the tightest case for the carrier invariant:
// 8192 rows / 1024 gives exactly 8 entries and the last sample sits at row 7168 < 8192.
TEST_F(SortKeySamplerSegmentTest, path_a_exact_block_multiple_still_leaves_a_tail) {
    auto segment = write_int_key_segment(/*num_rows=*/4096, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({4095, 4095, 4095}), /*num_rows=*/4096, /*target=*/100));
    EXPECT_EQ(3u, result.samples.size());
    EXPECT_EQ(1024, result.row_interval);
    EXPECT_LT(3 * 1024, 4096);
}

// Fewer than 2 index entries means there is nothing between row 0 and the end to sample. Empty is
// NOT authoritative -- SegmentSplitInfo::load_samples escalates to path B, which can still divide
// this segment at row granularity (see Task 5).
TEST_F(SortKeySamplerSegmentTest, path_a_single_block_yields_no_samples) {
    auto segment = write_int_key_segment(/*num_rows=*/10, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    ASSIGN_OR_ABORT(auto result,
                    sample_sort_key_from_short_key_index(*segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                                         tuple_of_ints({9, 9, 9}), /*num_rows=*/10, /*target=*/32));
    EXPECT_TRUE(result.samples.empty());
    EXPECT_EQ(0, result.row_interval);
}

// The entry-0 guard: if the decoded first entry is not sort_key_min, this index cannot be trusted
// and the caller must fall through to path B rather than emit garbage boundaries.
TEST_F(SortKeySamplerSegmentTest, path_a_rejects_when_entry_zero_is_not_min_key) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    // A min_key that does not match row 0 stands in for a mis-resolved historical schema.
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({1, 1, 1}),
                                         tuple_of_ints({8191, 8191, 8191}), /*num_rows=*/8192, /*target=*/32));
    EXPECT_TRUE(result.samples.empty()) << "rejected -- caller must escalate to path B";
}

TEST_F(SortKeySamplerSegmentTest, path_a_rejects_samples_outside_segment_bounds) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    // A max_key below the real maximum makes the later samples out of range.
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({100, 100, 100}), /*num_rows=*/8192, /*target=*/32));
    EXPECT_TRUE(result.samples.empty()) << "rejected -- caller must escalate to path B";
}

// Metadata that disagrees with the segment must be rejected, not trusted. Note which clause this
// actually exercises: num_rows=2000 against an 8192-row segment trips `num_rows !=
// segment.num_rows()`, NOT the carrier check (which the geometry triple makes unreachable).
TEST_F(SortKeySamplerSegmentTest, path_a_rejects_metadata_row_count_that_disagrees_with_the_segment) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({8191, 8191, 8191}), /*num_rows=*/2000, /*target=*/100));
    EXPECT_TRUE(result.samples.empty()) << "metadata num_rows disagrees with the segment footer";
}

// The third clause of the geometry triple, which nothing else covers: metadata and the segment agree
// on the row count, but the index footer's block size does not describe the entries it holds. A
// footer rewritten to a smaller block keeps a plausible num_items while halving the true stride, so
// accepting it would mis-weight every row it assigns.
TEST_F(SortKeySamplerSegmentTest, path_a_rejects_a_block_size_that_contradicts_the_entry_count) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    perturb_short_key_footer_block_rows(segment.get(), /*new_block_rows=*/512);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({8191, 8191, 8191}), /*num_rows=*/8192, /*target=*/100));
    EXPECT_TRUE(result.samples.empty()) << "num_items != ceil(num_rows / block_rows) must be rejected";
}

// Also pins the latency fix: a covered segment (this fixture's short key index encodes the full
// sort key) with a zero budget must not charge a zero-duration sample into the latency histogram --
// otherwise a tablet where most segments get no budget would look artificially fast.
TEST_F(SortKeySamplerSegmentTest, path_a_zero_target_yields_no_samples) {
    auto segment = write_int_key_segment(/*num_rows=*/8192, /*rows_per_block=*/1024);
    ASSERT_TRUE(segment->load_index().ok());
    const int64_t latency_samples_before = sort_key_sampling_short_key_index_latency_count();
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_short_key_index(
                                         *segment, _schema, {0, 1, 2}, tuple_of_ints({0, 0, 0}),
                                         tuple_of_ints({8191, 8191, 8191}), /*num_rows=*/8192, /*target=*/0));
    EXPECT_TRUE(result.samples.empty());
    EXPECT_EQ(latency_samples_before, sort_key_sampling_short_key_index_latency_count())
            << "target <= 0 must return before the latency timer even starts";
}

// ---------------------------------------------------------------------------
// Path B: sampling from segment data pages at a uniform row stride, used when the short key index
// does not encode the whole sort key (e.g. a VARCHAR/CHAR column is part of it).
// ---------------------------------------------------------------------------

TEST_F(SortKeySamplerSegmentTest, path_b_samples_the_rows_at_the_expected_ordinals) {
    // (int, varchar) sort key: not covered, so this is the real path B shape.
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                                   int_varchar_tuple(0), int_varchar_tuple(999),
                                                                   /*target=*/4, /*fill_data_cache=*/false));
    // row_interval = ceil(1000/5) = 200 -> ordinals 200, 400, 600, 800.
    ASSERT_EQ(4u, result.samples.size());
    EXPECT_EQ(200, result.row_interval);
    EXPECT_EQ(int_varchar_tuple(200), result.samples[0]);
    EXPECT_EQ(int_varchar_tuple(400), result.samples[1]);
    EXPECT_EQ(int_varchar_tuple(600), result.samples[2]);
    EXPECT_EQ(int_varchar_tuple(800), result.samples[3]);
    EXPECT_LT(static_cast<int64_t>(result.samples.size()) * result.row_interval, 1000);
}

// A nullable sort key column with a mix of NULL and non-NULL values. Building the destination
// column from the Schema is what makes this safe; a hand-rolled non-nullable column would be
// undefined behaviour on any row that carries a null. (This test's fixture fits in a single data
// page, so it exercises the null-handling plumbing, not multi-page reads -- see
// path_b_samples_across_multiple_data_pages for that.)
//
// The fixture writes sorted data: a key pass assumes ascending input (SegmentWriter::append_chunk
// takes row 0 as sort_key_min and the last row as sort_key_max on that basis) and NULLs sort first,
// so scattering a NULL every Nth row would make the column non-monotone and Path B's own
// non-decreasing validation would correctly return empty -- the test would then fail for a CORRECT
// reason. A leading run of NULLs followed by ascending ints keeps the column monotone while still
// mixing NULL and non-NULL values, which is all this test needs.
TEST_F(SortKeySamplerSegmentTest, path_b_handles_a_nullable_sort_key_column) {
    auto segment = write_nullable_int_key_segment(/*num_rows=*/1000, /*num_leading_nulls=*/150);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0}, /*num_rows=*/1000,
                                                                   VariantTuple{}, VariantTuple{}, /*target=*/8,
                                                                   /*fill_data_cache=*/false));
    ASSERT_FALSE(result.samples.empty());
    for (size_t i = 1; i < result.samples.size(); ++i) {
        EXPECT_LE(result.samples[i - 1].compare(result.samples[i]), 0) << "not non-decreasing at " << i;
    }
}

// A CHAR sort key must be zero-padded to the schema width after reading, or its encoded order
// disagrees with sort_key_min/max, which came from an already-padded writer chunk.
//
// min_key/max_key come from the FIRST and LAST ROW THE FIXTURE ACTUALLY WROTE, never from index
// arithmetic: CHAR compares lexicographically (KeyCoderTraits<TYPE_CHAR> appends raw bytes), so
// over "0".."499" the maximum is "99", not "499". Passing a
// numerically-derived max would make later samples fail the in-bounds check and empty the result --
// again failing the test for a correct reason.
TEST_F(SortKeySamplerSegmentTest, path_b_pads_char_columns_to_schema_width) {
    auto segment = write_char_key_segment(/*num_rows=*/500, /*char_len=*/16);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0}, /*num_rows=*/500,
                                                                   _first_written_key, _last_written_key,
                                                                   /*target=*/4, /*fill_data_cache=*/false));
    ASSERT_FALSE(result.samples.empty());
    for (const auto& s : result.samples) {
        ASSERT_EQ(1u, s.size());
        EXPECT_EQ(16u, s[0].value().get_slice().size) << "CHAR sample was not padded to schema width";
    }
}

TEST_F(SortKeySamplerSegmentTest, path_b_tiny_segment_yields_no_samples) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1);
    ASSIGN_OR_ABORT(auto result,
                    sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1,
                                                      int_varchar_tuple(0), int_varchar_tuple(0), /*target=*/32,
                                                      /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty());
    EXPECT_EQ(0, result.row_interval);
}

TEST_F(SortKeySamplerSegmentTest, path_b_zero_target_reads_nothing) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    const int64_t segments_before = sort_key_sampling_data_page_segments_count();
    ASSIGN_OR_ABORT(auto result,
                    sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                      int_varchar_tuple(0), int_varchar_tuple(999), /*target=*/0,
                                                      /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty());
    EXPECT_EQ(segments_before, sort_key_sampling_data_page_segments_count())
            << "target <= 0 must return before any data page is even opened";
}

// target < 0 is a DIFFERENT case from target == 0 above: row_interval's denominator is
// (target + 1), so target == -1 would divide by zero if the target <= 0 guard did not return
// first. This is what makes that guard load-bearing in this suite, not merely documentation.
TEST_F(SortKeySamplerSegmentTest, path_b_negative_target_reads_nothing) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    ASSIGN_OR_ABORT(auto result,
                    sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                      int_varchar_tuple(0), int_varchar_tuple(999), /*target=*/-1,
                                                      /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "target < 0 must return, not divide by (target + 1) == 0";
}

// Every path-B fixture above fits in a single data page (config::data_page_size defaults to 65536,
// and the largest fixture here is a few KB), so the strided multi-page scan this module exists for
// -- ScalarColumnIterator::_next_batch_template's cross-page loop, including the non-adjacent-page
// jump through ScalarColumnIterator::seek_to_ordinal -- was untested. Shrinking data_page_size
// forces many small pages instead.
TEST_F(SortKeySamplerSegmentTest, path_b_samples_across_multiple_data_pages) {
    const int32_t saved_page_size = config::data_page_size;
    config::data_page_size = 64;
    DeferOp restore([&]() { config::data_page_size = saved_page_size; });

    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                                   int_varchar_tuple(0), int_varchar_tuple(999),
                                                                   /*target=*/8, /*fill_data_cache=*/false));
    // Confirm the premise, not just assume it: config::data_page_size actually forced more than one
    // data page for this column (segment.h's column(i) is by schema position, matching
    // sort_key_idxes). The ordinal index is lazily loaded by ColumnIterator::init, so this can only
    // be checked after the sampling call above, not before it.
    ASSERT_GT(segment->column(0)->num_data_pages(), 1) << "fixture did not actually span multiple pages";
    // row_interval = ceil(1000/9) = 112 -> 8 ordinals, each many 64-byte pages apart in the int
    // column (4 bytes/value, so ~16 rows/page).
    ASSERT_EQ(8u, result.samples.size());
    EXPECT_EQ(112, result.row_interval);
    for (size_t i = 0; i < result.samples.size(); ++i) {
        const int64_t expected_ordinal = (static_cast<int64_t>(i) + 1) * result.row_interval;
        EXPECT_EQ(int_varchar_tuple(expected_ordinal), result.samples[i]) << "sample " << i;
    }
    for (size_t i = 1; i < result.samples.size(); ++i) {
        EXPECT_LE(result.samples[i - 1].compare(result.samples[i]), 0) << "not non-decreasing at " << i;
    }
    EXPECT_LT(static_cast<int64_t>(result.samples.size()) * result.row_interval, 1000);
}

// Path A has the equivalent test at path_a_rejects_samples_outside_segment_bounds. A min_key above
// the real minimum makes the very first sample (row 200) appear to fall below the caller's own
// coarse bound.
TEST_F(SortKeySamplerSegmentTest, path_b_rejects_a_sample_below_min_key) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                                   int_varchar_tuple(500), int_varchar_tuple(999),
                                                                   /*target=*/4, /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "a sample below min_key must not be published";
}

// A max_key below the real maximum makes a later sample appear to exceed the caller's own coarse
// bound.
TEST_F(SortKeySamplerSegmentTest, path_b_rejects_a_sample_above_max_key) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                                   int_varchar_tuple(0), int_varchar_tuple(100),
                                                                   /*target=*/4, /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "a sample above max_key must not be published";
}

// Path B is documented as fail-open, and the implementation depends on three specific fail-open
// checks. Each gets a test, because "returns empty instead of failing the split" is the property
// the whole design leans on.

// (1) A metadata row count larger than the real segment trips the num_rows == segment.num_rows()
// guard -- with num_rows=1'000'000 against a 1000-row segment, this fires before any column read
// happens, so it does NOT exercise the per-column length check in (2) below.
TEST_F(SortKeySamplerSegmentTest, path_b_refuses_an_inconsistent_high_row_count) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1},
                                                                   /*num_rows=*/1'000'000, int_varchar_tuple(0),
                                                                   int_varchar_tuple(999), /*target=*/8,
                                                                   /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "metadata num_rows disagrees with the segment footer";
}

// (2) The per-column length guard AND the monotonicity check below both defend against a column
// short-read; this test does not isolate the length guard on its own. With the length guard
// removed, Chunk::set_num_rows unconditionally regrows the fail-point-shrunk column, and the
// regrown slot's value happens to violate the pre-existing monotonicity check
// (further down in sample_sort_key_from_segment_data), which then empties the result instead. What
// this test DOES prove, robustly: path B fails open -- never reads out of bounds, never publishes a
// wrong sample -- when a data-page read returns fewer rows than the range asked for.
//
// Reaching this needs a seam, because with num_rows == segment.num_rows() every ordinal is a valid
// row and a well-formed segment will not short-read. Uses the repository's fail-point machinery.
#ifdef FIU_ENABLE
TEST_F(SortKeySamplerSegmentTest, path_b_fails_open_when_a_column_short_reads) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("sort_key_sampler_short_read_one_column");
    ASSERT_NE(fp, nullptr);
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp->setMode(mode);
    FAIL_POINT_SCOPE(sort_key_sampler_short_read_one_column);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                                   int_varchar_tuple(0), int_varchar_tuple(999),
                                                                   /*target=*/8, /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "a short-read column must not be published";
}
#endif // FIU_ENABLE

// (3) An unreadable segment must degrade, not fail. Truncating the file after open is the cheapest
// way to make the column reads fail deterministically; a deleted file works too.
TEST_F(SortKeySamplerSegmentTest, path_b_fails_open_on_an_unreadable_segment) {
    auto segment = write_int_varchar_key_segment(/*num_rows=*/1000);
    corrupt_segment_file(segment->file_info().path);
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {0, 1}, /*num_rows=*/1000,
                                                                   int_varchar_tuple(0), int_varchar_tuple(999),
                                                                   /*target=*/8, /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "an I/O failure must not fail the split";
}

// A sort key column absent from this segment (it predates a trailing key-column add) must yield NO
// samples. Substituting defaults would make every sample identical in that position, i.e. silently
// meaningless boundaries.
TEST_F(SortKeySamplerSegmentTest, path_b_refuses_when_a_sort_key_column_is_missing) {
    auto segment = write_single_int_key_segment(/*num_rows=*/1000); // one key column only
    auto wider = tablet_schema_with_extra_trailing_key_column();    // two
    ASSIGN_OR_ABORT(auto result,
                    sample_sort_key_from_segment_data(*segment, wider, {0, 1}, /*num_rows=*/1000, VariantTuple{},
                                                      VariantTuple{}, /*target=*/8, /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty());
}

// A permuted sort_key_idxes parameter -- same column ids as _tablet_schema->sort_key_idxes(), same
// types, different order -- is the worst case in this function: same-type columns swap without a
// crash, so nothing type-related catches it, and (in write_two_int_key_segment's fixture) both
// columns are independently monotonic in ordinal order, so a permutation still passes the
// monotonicity check below. min_key/max_key are a bounding box over BOTH columns' full value range
// (col0 in [0,1998], col1 in [1,1999]), so a permuted sample still passes the in-bounds check too --
// nothing downstream would catch this without the idxes-equality guard.
TEST_F(SortKeySamplerSegmentTest, path_b_rejects_a_permuted_sort_key_idxes_parameter) {
    auto segment = write_two_int_key_segment(/*num_rows=*/1000);
    const int64_t fallback_before = sort_key_sampling_data_page_fallback_count();
    ASSIGN_OR_ABORT(auto result, sample_sort_key_from_segment_data(*segment, _tablet_schema, {1, 0}, /*num_rows=*/1000,
                                                                   tuple_of_ints({0, 0}), tuple_of_ints({1999, 1999}),
                                                                   /*target=*/4, /*fill_data_cache=*/false));
    EXPECT_TRUE(result.samples.empty()) << "a permuted sort_key_idxes parameter must not be published";
    EXPECT_GT(sort_key_sampling_data_page_fallback_count(), fallback_before)
            << "the idxes-mismatch guard must bump the path-B fallback counter";
}

} // namespace starrocks
