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

#include "storage/full_sort_key_codec.h"

#include <gtest/gtest.h>

#include <cstring>
#include <deque>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "base/types/int256.h"
#include "storage/seek_tuple.h"

namespace starrocks {

namespace {

FieldPtr make_field(ColumnId id, LogicalType type, bool nullable, int precision = -1, int scale = -1) {
    auto field = std::make_shared<Field>(id, "c" + std::to_string(id), type, precision, scale, nullable);
    field->set_is_key(true);
    return field;
}

std::string slice_to_string(const Slice& s) {
    return std::string(s.data, s.size);
}

} // namespace

class FullSortKeyCodecTest : public testing::Test {};

// (int, varchar nullable middle, decimal128, char middle, varbinary-with-embedded-0x00, varchar last),
// asserting typed equality per column plus a NULL round trip for the nullable middle column.
TEST_F(FullSortKeyCodecTest, RoundTripBasicComposite) {
    Fields fields = {
            make_field(0, TYPE_INT, false),
            make_field(1, TYPE_VARCHAR, true),
            make_field(2, TYPE_DECIMAL128, false, 38, 9),
            make_field(3, TYPE_CHAR, false),
            make_field(4, TYPE_VARBINARY, false),
            make_field(5, TYPE_VARCHAR, false),
    };
    Schema schema(fields);
    std::vector<uint32_t> idxes = {0, 1, 2, 3, 4, 5};

    std::string varchar_mid = "hello";
    std::string char_padded(5, '\0');
    std::memcpy(char_padded.data(), "ab", 2); // CHAR(5) "ab" with writer-side NUL padding
    std::string varbinary_val("x\0y", 3);     // embedded 0x00, significant for VARBINARY (never truncated)
    std::string varchar_last = "world!!";

    SeekTuple tuple(schema, {Datum(int32_t(42)), Datum(Slice(varchar_mid)), Datum(int128_t(123456789012345LL)),
                             Datum(Slice(char_padded)), Datum(Slice(varbinary_val)), Datum(Slice(varchar_last))});
    std::string encoded = tuple.full_sort_key_encode(idxes, 0);

    VariantTuple out;
    ASSERT_TRUE(decode_full_sort_key(Slice(encoded), schema, idxes, &out).ok());
    ASSERT_EQ(6u, out.size());

    EXPECT_EQ(42, out[0].value().get_int32());
    ASSERT_FALSE(out[1].value().is_null());
    EXPECT_EQ("hello", slice_to_string(out[1].value().get_slice()));
    EXPECT_EQ(int128_t(123456789012345LL), out[2].value().get_int128());
    EXPECT_EQ("ab", slice_to_string(out[3].value().get_slice())); // NUL-truncated visible prefix
    EXPECT_EQ(varbinary_val, slice_to_string(out[4].value().get_slice()));
    EXPECT_EQ("world!!", slice_to_string(out[5].value().get_slice()));

    // NULL round trip: re-encode with the nullable varchar column set to NULL.
    SeekTuple null_tuple(schema, {Datum(int32_t(7)), Datum(), Datum(int128_t(1)), Datum(Slice(char_padded)),
                                  Datum(Slice(varbinary_val)), Datum(Slice(varchar_last))});
    std::string null_encoded = null_tuple.full_sort_key_encode(idxes, 0);
    VariantTuple null_out;
    ASSERT_TRUE(decode_full_sort_key(Slice(null_encoded), schema, idxes, &null_out).ok());
    ASSERT_EQ(6u, null_out.size());
    EXPECT_TRUE(null_out[1].value().is_null());
}

namespace {

// A key-eligible scalar type together with a strictly ascending list (by the type's own semantics) of
// representative values, covering documented edge cases (min/max/zero/sign flip, etc). Every Datum is
// constructed directly in the type's on-disk StorageCppType representation, since that is exactly what
// Datum stores internally (see full_sort_key_codec.cpp's decode_fixed_datum) -- so a plain Datum::operator==
// after decode is a valid, type-agnostic round-trip check.
struct TypeCase {
    LogicalType lt;
    int precision;
    int scale;
    std::vector<Datum> asc_values;
};

DecimalV2Value make_decimalv2(int128_t raw) {
    DecimalV2Value v;
    v.value() = raw;
    return v;
}

std::vector<TypeCase> build_type_cases() {
    std::vector<TypeCase> cases;

    cases.push_back({TYPE_BOOLEAN, -1, -1, {Datum(false), Datum(true)}});

    cases.push_back({TYPE_TINYINT,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int8_t>::min()), Datum(int8_t(-1)), Datum(int8_t(0)), Datum(int8_t(1)),
                      Datum(std::numeric_limits<int8_t>::max())}});
    cases.push_back({TYPE_SMALLINT,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int16_t>::min()), Datum(int16_t(-1)), Datum(int16_t(0)),
                      Datum(int16_t(1)), Datum(std::numeric_limits<int16_t>::max())}});
    cases.push_back({TYPE_INT,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int32_t>::min()), Datum(int32_t(-1)), Datum(int32_t(0)),
                      Datum(int32_t(1)), Datum(std::numeric_limits<int32_t>::max())}});
    cases.push_back({TYPE_UNSIGNED_INT,
                     -1,
                     -1,
                     {Datum(uint32_t(0)), Datum(uint32_t(1)), Datum(std::numeric_limits<uint32_t>::max() / 2),
                      Datum(std::numeric_limits<uint32_t>::max())}});
    cases.push_back({TYPE_BIGINT,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int64_t>::min()), Datum(int64_t(-1)), Datum(int64_t(0)),
                      Datum(int64_t(1)), Datum(std::numeric_limits<int64_t>::max())}});
    cases.push_back({TYPE_UNSIGNED_BIGINT,
                     -1,
                     -1,
                     {Datum(uint64_t(0)), Datum(uint64_t(1)), Datum(std::numeric_limits<uint64_t>::max() / 2),
                      Datum(std::numeric_limits<uint64_t>::max())}});
    cases.push_back({TYPE_LARGEINT,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int128_t>::min()), Datum(int128_t(-1)), Datum(int128_t(0)),
                      Datum(int128_t(1)), Datum(std::numeric_limits<int128_t>::max())}});

    std::vector<Datum> int256_values = {Datum(INT256_MIN),
                                        Datum(int256_t(-1)),
                                        Datum(int256_t(0)),
                                        Datum(int256_t(1)),
                                        Datum(int256_t(static_cast<int128_t>(1), static_cast<uint128_t>(0))), // 2^128
                                        Datum(INT256_MAX)};
    cases.push_back({TYPE_INT256, -1, -1, int256_values});
    cases.push_back({TYPE_DECIMAL256, 50, 10, int256_values});

    cases.push_back({TYPE_DECIMAL,
                     -1,
                     -1,
                     {Datum(decimal12_t(-100, -500000000)), Datum(decimal12_t(-1, 0)), Datum(decimal12_t(0, 0)),
                      Datum(decimal12_t(0, 1)), Datum(decimal12_t(1, 0)), Datum(decimal12_t(100, 500000000))}});

    cases.push_back({TYPE_DECIMALV2,
                     -1,
                     -1,
                     {Datum(make_decimalv2(DecimalV2Value::MIN_DECIMAL_VALUE)), Datum(make_decimalv2(-1)),
                      Datum(make_decimalv2(0)), Datum(make_decimalv2(1)),
                      Datum(make_decimalv2(DecimalV2Value::MAX_DECIMAL_VALUE))}});

    cases.push_back({TYPE_DECIMAL32,
                     9,
                     2,
                     {Datum(std::numeric_limits<int32_t>::min()), Datum(int32_t(-1)), Datum(int32_t(0)),
                      Datum(int32_t(1)), Datum(std::numeric_limits<int32_t>::max())}});
    cases.push_back({TYPE_DECIMAL64,
                     18,
                     4,
                     {Datum(std::numeric_limits<int64_t>::min()), Datum(int64_t(-1)), Datum(int64_t(0)),
                      Datum(int64_t(1)), Datum(std::numeric_limits<int64_t>::max())}});
    cases.push_back({TYPE_DECIMAL128,
                     38,
                     9,
                     {Datum(std::numeric_limits<int128_t>::min()), Datum(int128_t(-1)), Datum(int128_t(0)),
                      Datum(int128_t(1)), Datum(std::numeric_limits<int128_t>::max())}});

    cases.push_back({TYPE_DATE_V1,
                     -1,
                     -1,
                     {Datum(uint24_t(0u)), Datum(uint24_t(1u)), Datum(uint24_t(10000u)), Datum(uint24_t(0xFFFFFEu))}});
    cases.push_back({TYPE_DATE,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int32_t>::min()), Datum(int32_t(-1)), Datum(int32_t(0)),
                      Datum(int32_t(2451545)), Datum(std::numeric_limits<int32_t>::max())}});
    cases.push_back({TYPE_DATETIME_V1,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int64_t>::min()), Datum(int64_t(-1)), Datum(int64_t(0)),
                      Datum(int64_t(1)), Datum(std::numeric_limits<int64_t>::max())}});
    cases.push_back({TYPE_DATETIME,
                     -1,
                     -1,
                     {Datum(std::numeric_limits<int64_t>::min()), Datum(int64_t(-1)), Datum(int64_t(0)),
                      Datum(int64_t(1)), Datum(std::numeric_limits<int64_t>::max())}});

    return cases;
}

// Round trip (both as the last requested column and as a middle column followed by an INT sentinel),
// ascending-order preservation between adjacent values, and a NULL round trip, all through the physical
// full_sort_key_encode + decode_full_sort_key pair.
void run_type_case(const TypeCase& tc) {
    SCOPED_TRACE(::testing::Message() << "LogicalType=" << static_cast<int>(tc.lt));

    Fields fields2 = {make_field(0, tc.lt, true, tc.precision, tc.scale), make_field(1, TYPE_INT, false)};
    Schema schema2(fields2);
    Fields fields1 = {make_field(0, tc.lt, true, tc.precision, tc.scale)};
    Schema schema1(fields1);
    std::vector<uint32_t> idxes2 = {0, 1};
    std::vector<uint32_t> idxes1 = {0};
    const int32_t sentinel = 777;

    for (size_t i = 0; i < tc.asc_values.size(); i++) {
        {
            SeekTuple t(schema1, {tc.asc_values[i]});
            std::string enc = t.full_sort_key_encode(idxes1, 0);
            VariantTuple out;
            ASSERT_TRUE(decode_full_sort_key(Slice(enc), schema1, idxes1, &out).ok());
            ASSERT_EQ(1u, out.size());
            EXPECT_TRUE(out[0].value() == tc.asc_values[i]) << "last-column round trip, i=" << i;
        }
        {
            SeekTuple t(schema2, {tc.asc_values[i], Datum(sentinel)});
            std::string enc = t.full_sort_key_encode(idxes2, 0);
            VariantTuple out;
            ASSERT_TRUE(decode_full_sort_key(Slice(enc), schema2, idxes2, &out).ok());
            ASSERT_EQ(2u, out.size());
            EXPECT_TRUE(out[0].value() == tc.asc_values[i]) << "middle-column round trip, i=" << i;
            EXPECT_EQ(sentinel, out[1].value().get_int32());
        }
        if (i + 1 < tc.asc_values.size()) {
            SeekTuple ta(schema2, {tc.asc_values[i], Datum(sentinel)});
            SeekTuple tb(schema2, {tc.asc_values[i + 1], Datum(sentinel)});
            EXPECT_LT(ta.full_sort_key_encode(idxes2, 0), tb.full_sort_key_encode(idxes2, 0)) << "i=" << i;
        }
    }

    {
        SeekTuple t(schema2, {Datum(), Datum(sentinel)});
        std::string enc = t.full_sort_key_encode(idxes2, 0);
        VariantTuple out;
        ASSERT_TRUE(decode_full_sort_key(Slice(enc), schema2, idxes2, &out).ok());
        ASSERT_EQ(2u, out.size());
        EXPECT_TRUE(out[0].value().is_null());
        EXPECT_EQ(sentinel, out[1].value().get_int32());
    }
}

} // namespace

TEST_F(FullSortKeyCodecTest, AllScalarTypesOrderAndRoundTripFuzz) {
    for (const auto& tc : build_type_cases()) {
        run_type_case(tc);
    }
}

namespace {

// Order + round trip for a variable-length string type, both as the last requested column (raw
// passthrough) and as a middle column (escape + 0x00 0x00 terminate), including embedded 0x00/0x01/0x02/0xFF
// bytes. |canon| models the physical encoder's canonicalization (identity, except CHAR NUL-truncates).
void run_string_type_case(LogicalType lt, const std::vector<std::string>& asc_raw) {
    SCOPED_TRACE(::testing::Message() << "LogicalType=" << static_cast<int>(lt));
    auto canon = [lt](const std::string& s) -> std::string {
        if (lt != TYPE_CHAR) {
            return s;
        }
        auto pos = s.find('\0');
        return pos == std::string::npos ? s : s.substr(0, pos);
    };

    Fields fields2 = {make_field(0, lt, true), make_field(1, TYPE_INT, false)};
    Schema schema2(fields2);
    Fields fields1 = {make_field(0, lt, true)};
    Schema schema1(fields1);
    std::vector<uint32_t> idxes2 = {0, 1};
    std::vector<uint32_t> idxes1 = {0};
    const int32_t sentinel = 555;

    for (size_t i = 0; i < asc_raw.size(); i++) {
        std::string expected = canon(asc_raw[i]);

        {
            SeekTuple t(schema1, {Datum(Slice(asc_raw[i]))});
            std::string enc = t.full_sort_key_encode(idxes1, 0);
            VariantTuple out;
            ASSERT_TRUE(decode_full_sort_key(Slice(enc), schema1, idxes1, &out).ok());
            EXPECT_EQ(expected, slice_to_string(out[0].value().get_slice())) << "last-column, i=" << i;
        }
        {
            SeekTuple t(schema2, {Datum(Slice(asc_raw[i])), Datum(sentinel)});
            std::string enc = t.full_sort_key_encode(idxes2, 0);
            VariantTuple out;
            ASSERT_TRUE(decode_full_sort_key(Slice(enc), schema2, idxes2, &out).ok());
            EXPECT_EQ(expected, slice_to_string(out[0].value().get_slice())) << "middle-column, i=" << i;
            EXPECT_EQ(sentinel, out[1].value().get_int32());
        }
        if (i + 1 < asc_raw.size()) {
            SeekTuple ta(schema2, {Datum(Slice(asc_raw[i])), Datum(sentinel)});
            SeekTuple tb(schema2, {Datum(Slice(asc_raw[i + 1])), Datum(sentinel)});
            EXPECT_LT(ta.full_sort_key_encode(idxes2, 0), tb.full_sort_key_encode(idxes2, 0)) << "i=" << i;
        }
    }

    {
        SeekTuple t(schema2, {Datum(), Datum(sentinel)});
        std::string enc = t.full_sort_key_encode(idxes2, 0);
        VariantTuple out;
        ASSERT_TRUE(decode_full_sort_key(Slice(enc), schema2, idxes2, &out).ok());
        EXPECT_TRUE(out[0].value().is_null());
        EXPECT_EQ(sentinel, out[1].value().get_int32());
    }
}

} // namespace

TEST_F(FullSortKeyCodecTest, StringTypesEmbeddedBytesAndOrderFuzz) {
    // Ascending by raw lexicographic byte order (verified by hand; see run_string_type_case's order
    // assertion, which cross-checks it against the encoded bytes too).
    run_string_type_case(TYPE_VARCHAR, {std::string("\x00\x01", 2), std::string("\x00\x02", 2), std::string(1, '\x01'),
                                        std::string(1, '\x02'), std::string("a"), std::string("a\x00", 2),
                                        std::string("ab"), std::string(1, '\xFF')});
    run_string_type_case(TYPE_VARBINARY, {std::string("\x00\x01", 2), std::string(1, '\x01'), std::string(1, '\x02'),
                                          std::string("a"), std::string("a\x00", 2), std::string(1, '\xFF')});
    // CHAR canonicalizes (NUL-truncates) at encode time, so an embedded NUL is never observable in the
    // decoded value; the padding/truncation behavior itself is covered by RoundTripBasicComposite and
    // QueryVsStoredCharCrossOrder below.
    run_string_type_case(TYPE_CHAR, {std::string("a"), std::string("ab"), std::string(1, '\xFF')});
}

namespace {

// Prefix-sentinel property, compact query-key encode vs physical stored-row encode: for a two-column
// full sort key (c0, c1), a partial query key carrying only c0 -- encoded via the compact overload with
// KEY_MINIMAL_MARKER padding -- must byte-sort strictly below every physical encoding of a full (c0, c1)
// row sharing the same c0, and with KEY_MAXIMAL_MARKER padding strictly above. This is a pure encode-side
// memcmp property: decode intentionally does not support prefix keys, so only the encoded bytes are
// compared here, never decoded. Modeled on SeekTupleTest.FullSortKeyEncodePrefixSentinel
// (seek_tuple_test.cpp), exercised across a representative fixed type (INT), a variable-length type
// (VARCHAR), and CHAR.
void run_prefix_sentinel_case(LogicalType c0_type, const Datum& c0_value, const std::vector<Datum>& c1_values) {
    SCOPED_TRACE(::testing::Message() << "c0 LogicalType=" << static_cast<int>(c0_type));

    Fields fields = {make_field(0, c0_type, false), make_field(1, TYPE_INT, false)};
    Schema schema(fields);
    std::vector<uint32_t> idxes = {0, 1};

    SeekTuple prefix(schema, {c0_value});
    std::string prefix_min = prefix.full_sort_key_encode(2, KEY_MINIMAL_MARKER);
    std::string prefix_max = prefix.full_sort_key_encode(2, KEY_MAXIMAL_MARKER);

    for (size_t i = 0; i < c1_values.size(); i++) {
        SeekTuple full(schema, {c0_value, c1_values[i]});
        std::string full_encoded = full.full_sort_key_encode(idxes, 0);
        EXPECT_LT(prefix_min, full_encoded) << "c1 index=" << i;
        EXPECT_GT(prefix_max, full_encoded) << "c1 index=" << i;
    }
}

} // namespace

TEST_F(FullSortKeyCodecTest, FullSortKeyEncodePrefixSentinelAcrossTypes) {
    std::vector<Datum> c1_values = {Datum(std::numeric_limits<int32_t>::min()), Datum(int32_t(0)),
                                    Datum(std::numeric_limits<int32_t>::max())};

    run_prefix_sentinel_case(TYPE_INT, Datum(int32_t(42)), c1_values);

    std::string varchar_c0 = "ab";
    run_prefix_sentinel_case(TYPE_VARCHAR, Datum(Slice(varchar_c0)), c1_values);

    std::string char_c0 = "ab";
    run_prefix_sentinel_case(TYPE_CHAR, Datum(Slice(char_c0)), c1_values);
}

// Cross-domain property: for a query key |q| (compact overload, raw-escaped CHAR) and a stored value |s|
// (physical overload, NUL-truncated-then-escaped CHAR), sign(memcmp(compact_encode(q), physical_encode(s)))
// must equal the sign of the typed comparison of raw |q| vs NUL-truncated |s|. Covers an embedded-NUL query
// key and an over-width query literal, each followed by another sort column.
TEST_F(FullSortKeyCodecTest, QueryVsStoredCharCrossOrder) {
    Fields fields = {make_field(0, TYPE_CHAR, false), make_field(1, TYPE_INT, false)};
    Schema schema(fields);
    std::vector<uint32_t> idxes = {0, 1};

    auto check = [&](const std::string& query, const std::string& stored, int32_t query_int, int32_t stored_int) {
        SeekTuple q(schema, {Datum(Slice(query)), Datum(query_int)});
        SeekTuple s(schema, {Datum(Slice(stored)), Datum(stored_int)});

        std::string compact = q.full_sort_key_encode(idxes.size(), 0);
        std::string physical = s.full_sort_key_encode(idxes, 0);
        int memcmp_sign = compact.compare(physical);

        VariantTuple stored_decoded;
        ASSERT_TRUE(decode_full_sort_key(Slice(physical), schema, idxes, &stored_decoded).ok());
        std::string stored_nul_truncated = slice_to_string(stored_decoded[0].value().get_slice());

        int typed_sign;
        if (query != stored_nul_truncated) {
            typed_sign = query < stored_nul_truncated ? -1 : 1;
        } else {
            typed_sign = query_int == stored_int ? 0 : (query_int < stored_int ? -1 : 1);
        }

        if (typed_sign < 0) {
            EXPECT_LT(memcmp_sign, 0);
        } else if (typed_sign > 0) {
            EXPECT_GT(memcmp_sign, 0);
        } else {
            EXPECT_EQ(memcmp_sign, 0);
        }
    };

    // Embedded NUL: query "a\0b" vs stored "a" -> query greater.
    check(std::string("a\0b", 3), "a", 1, 1);
    // Over-width literal: query "abc" vs stored "ab" -> query greater.
    check("abc", "ab", 1, 1);
}

// Seeded (non time-based) random fuzz on top of the fixed edge-case enumeration above: random BIGINT
// values, paired with a fixed trailing VARCHAR column, must preserve typed order and round trip exactly.
TEST_F(FullSortKeyCodecTest, SeededRandomOrderAndRoundTripFuzz) {
    std::mt19937_64 rng(20260722ULL);
    std::uniform_int_distribution<int64_t> dist(std::numeric_limits<int64_t>::min(),
                                                std::numeric_limits<int64_t>::max());

    Fields fields = {make_field(0, TYPE_BIGINT, false), make_field(1, TYPE_VARCHAR, false)};
    Schema schema(fields);
    std::vector<uint32_t> idxes = {0, 1};
    std::string tail = "x";

    for (int i = 0; i < 200; i++) {
        int64_t v1 = dist(rng);
        int64_t v2 = dist(rng);

        SeekTuple t1(schema, {Datum(v1), Datum(Slice(tail))});
        SeekTuple t2(schema, {Datum(v2), Datum(Slice(tail))});
        std::string e1 = t1.full_sort_key_encode(idxes, 0);
        std::string e2 = t2.full_sort_key_encode(idxes, 0);

        if (v1 < v2) {
            EXPECT_LT(e1, e2);
        } else if (v1 > v2) {
            EXPECT_GT(e1, e2);
        } else {
            EXPECT_EQ(e1, e2);
        }

        VariantTuple out1;
        VariantTuple out2;
        ASSERT_TRUE(decode_full_sort_key(Slice(e1), schema, idxes, &out1).ok());
        ASSERT_TRUE(decode_full_sort_key(Slice(e2), schema, idxes, &out2).ok());
        EXPECT_EQ(v1, out1[0].value().get_int64());
        EXPECT_EQ(v2, out2[0].value().get_int64());
    }
}

} // namespace starrocks
