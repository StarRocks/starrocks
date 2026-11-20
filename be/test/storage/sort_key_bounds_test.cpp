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

#include "storage/sort_key_bounds.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

<<<<<<< HEAD
=======
#include "base/decimal_types.h"
#include "base/string/utf8_check.h"
#include "base/testutil/assert.h"
>>>>>>> bf8def1 ([BugFix] Cap the size of segment sort key bounds in lake tablet metadata (#83993))
#include "gen_cpp/lake_types.pb.h"
#include "runtime/decimalv2_value.h"
#include "runtime/time_types.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"
#include "testutil/assert.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {

namespace {

// Values are typed the way the segment writer types them: get_type_info() of the storage field type.
DatumVariant scalar(LogicalType type, const Datum& value) {
    return {get_type_info(type), value};
}

DatumVariant decimal(LogicalType type, int precision, int scale, const Datum& value) {
    return {get_type_info(type, precision, scale), value};
}

DatumVariant bigint(int64_t v) {
    return scalar(TYPE_BIGINT, Datum(v));
}

DatumVariant varchar(const std::string& s) {
    return scalar(TYPE_VARCHAR, Datum(Slice(s)));
}

DatumVariant null_of(LogicalType type) {
    return scalar(type, Datum());
}

VariantTuple tuple_of(std::vector<DatumVariant> values) {
    VariantTuple t;
    for (auto& v : values) {
        t.append(std::move(v));
    }
    return t;
}

std::string encode(const VariantTuple& t) {
    std::string out;
    Status st = encode_sort_key_bound(t, &out);
    EXPECT_TRUE(st.ok()) << st;
    return out;
}

VariantTuple decode(const std::string& bytes) {
    VariantTuple out;
    Status st = decode_sort_key_bound(Slice(bytes), &out);
    EXPECT_TRUE(st.ok()) << st;
    return out;
}

VariantTuple legacy_round_trip(const VariantTuple& t) {
    TuplePB pb;
    t.to_proto(&pb);
    VariantTuple out;
    Status st = out.from_proto(pb);
    EXPECT_TRUE(st.ok()) << st;
    return out;
}

// Same arity and, per column, the same logical type, decimal precision/scale, nullness and value.
void expect_same_tuple(const VariantTuple& expected, const VariantTuple& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        const DatumVariant& e = expected[i];
        const DatumVariant& a = actual[i];
        ASSERT_EQ(e.type()->type(), a.type()->type()) << "column " << i;
        EXPECT_EQ(e.type()->precision(), a.type()->precision()) << "column " << i;
        EXPECT_EQ(e.type()->scale(), a.type()->scale()) << "column " << i;
        EXPECT_EQ(e.value().is_null(), a.value().is_null()) << "column " << i;
        EXPECT_EQ(0, e.compare(a)) << "column " << i;
    }
}

TimestampValue zero_datetime() {
    TimestampValue ts;
    ts.set_timestamp(timestamp::from_julian_and_time(date::ZERO_EPOCH_JULIAN, 0));
    return ts;
}

// One non-NULL value of every supported type except DECIMALV2, whose legacy round trip trips
// create_decimalv2_type's DCHECKs in debug builds. Negative or high-bit-set where the type
// allows. int128 values are written as -(int128_t{1} << 90): left-shifting a negative value is
// undefined behaviour.
VariantTuple every_type_tuple() {
    return tuple_of({
            scalar(TYPE_BOOLEAN, Datum(static_cast<int8_t>(1))),
            scalar(TYPE_TINYINT, Datum(static_cast<int8_t>(-7))),
            scalar(TYPE_SMALLINT, Datum(static_cast<int16_t>(-300))),
            scalar(TYPE_INT, Datum(static_cast<int32_t>(-70000))),
            scalar(TYPE_BIGINT, Datum(static_cast<int64_t>(-5000000000LL))),
            scalar(TYPE_LARGEINT, Datum(-(int128_t{1} << 90))),
            scalar(TYPE_DATE, Datum(DateValue::create(2024, 2, 29))),
            scalar(TYPE_DATETIME, Datum(TimestampValue::create(2024, 2, 29, 13, 45, 6, 123456))),
            decimal(TYPE_DECIMAL32, 9, 2, Datum(static_cast<int32_t>(-1234567))),
            decimal(TYPE_DECIMAL64, 18, 4, Datum(static_cast<int64_t>(-123456789012LL))),
            decimal(TYPE_DECIMAL128, 38, 10, Datum(-(int128_t{1} << 90))),
            // 5 * 2^128 + 7: does not fit in 128 bits.
            decimal(TYPE_DECIMAL256, 76, 20, Datum(int256_t(int128_t{5}, uint128_t{7}))),
            scalar(TYPE_CHAR, Datum(Slice("abc"))),
            scalar(TYPE_VARCHAR, Datum(Slice("hello"))),
            scalar(TYPE_VARBINARY, Datum(Slice("\x01\xfe", 2))),
    });
}

int sign(int v) {
    return (v > 0) - (v < 0);
}

int byte_compare(const std::string& a, const std::string& b) {
    int r = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
    if (r != 0) {
        return sign(r);
    }
    return (a.size() > b.size()) - (a.size() < b.size());
}

void expect_order_preserved(const std::string& label, const VariantTuple& a, const VariantTuple& b) {
    const std::string ea = encode(a);
    const std::string eb = encode(b);
    const VariantTuple da = decode(ea);
    const VariantTuple db = decode(eb);
    EXPECT_EQ(sign(da.compare(db)), byte_compare(ea, eb)) << label;
    EXPECT_EQ(sign(db.compare(da)), byte_compare(eb, ea)) << label;
    EXPECT_EQ(0, byte_compare(ea, ea)) << label;
}

std::string bytes_of(std::initializer_list<uint8_t> bytes) {
    return std::string(bytes.begin(), bytes.end());
}

// Header-level corruptions; the decoder must reject each of them.
std::vector<std::pair<std::string, std::string>> malformed_headers() {
    return {
            {"empty", std::string()},
            {"version 3", bytes_of({0x03, 0x00, 0x01, TYPE_BIGINT, 0x00})},
            {"header of 1 byte", bytes_of({0x01})},
            {"header of 2 bytes", bytes_of({0x01, 0x00})},
            {"count 0", bytes_of({0x01, 0x00, 0x00})},
            {"count 0 with trailing bytes", bytes_of({0x01, 0x00, 0x00, TYPE_BIGINT, 0x00})},
            {"count larger than remaining bytes", bytes_of({0x01, 0x00, 0x05, TYPE_BIGINT, 0x00})},
            {"type byte 0xFF", bytes_of({0x01, 0x00, 0x01, 0xFF, 0x00})},
            {"type TYPE_JSON", bytes_of({0x01, 0x00, 0x01, TYPE_JSON, 0x00})},
            {"decimal header without scale", bytes_of({0x01, 0x00, 0x01, TYPE_DECIMAL64, 18})},
            {"header ends at the second column", bytes_of({0x01, 0x00, 0x02, TYPE_DECIMAL64, 18, 4})},
    };
}

std::string decimal_null_bound(LogicalType type, uint8_t precision, uint8_t scale) {
    return bytes_of({0x01, 0x00, 0x01, static_cast<uint8_t>(type), precision, scale, 0x00});
}

} // namespace

TEST(SortKeyBoundsTest, RoundTripEveryType) {
    VariantTuple t = every_type_tuple();
    ASSERT_EQ(15u, t.size());
    expect_same_tuple(t, decode(encode(t)));
}

TEST(SortKeyBoundsTest, RoundTripDecimalV2) {
    const DecimalV2Value v(std::string("-123.456"));
    VariantTuple t = tuple_of({scalar(TYPE_DECIMALV2, Datum(v)), null_of(TYPE_DECIMALV2)});
    VariantTuple decoded = decode(encode(t));
    expect_same_tuple(t, decoded);
    ASSERT_EQ(2u, decoded.size());
    EXPECT_EQ(v, decoded[0].value().get_decimal());
    EXPECT_TRUE(decoded[1].value().is_null());
}

TEST(SortKeyBoundsTest, MatchesLegacyDecode) {
    VariantTuple t = every_type_tuple();
    expect_same_tuple(legacy_round_trip(t), decode(encode(t)));
}

TEST(SortKeyBoundsTest, RoundTripNulls) {
    VariantTuple t = tuple_of({null_of(TYPE_BIGINT), bigint(5), null_of(TYPE_VARCHAR)});
    VariantTuple decoded = decode(encode(t));
    expect_same_tuple(t, decoded);
    ASSERT_EQ(3u, decoded.size());
    EXPECT_TRUE(decoded[0].value().is_null());
    EXPECT_EQ(5, decoded[1].value().get_int64());
    EXPECT_TRUE(decoded[2].value().is_null());
}

TEST(SortKeyBoundsTest, RoundTripStringEdgeCases) {
    std::string long_value(300, '\0');
    for (size_t i = 0; i < long_value.size(); ++i) {
        long_value[i] = static_cast<char>(i % 256);
    }
    const std::vector<std::pair<LogicalType, std::string>> cases = {
            {TYPE_VARCHAR, std::string()},
            {TYPE_VARCHAR, std::string("a\0b", 3)},
            {TYPE_VARCHAR, std::string("\xff\xff", 2)},
            {TYPE_VARBINARY, std::string("\0\0\x01", 3)},
            {TYPE_VARCHAR, long_value},
    };
    VariantTuple all;
    for (const auto& [type, value] : cases) {
        // Alone, as the last column ...
        VariantTuple single = tuple_of({scalar(type, Datum(Slice(value)))});
        VariantTuple decoded = decode(encode(single));
        ASSERT_EQ(1u, decoded.size());
        EXPECT_EQ(value, decoded[0].value().get_slice().to_string());
        all.append(scalar(type, Datum(Slice(value))));
    }
    // ... and side by side, so each value is followed by another column.
    VariantTuple decoded = decode(encode(all));
    expect_same_tuple(all, decoded);
    ASSERT_EQ(cases.size(), decoded.size());
    for (size_t i = 0; i < cases.size(); ++i) {
        EXPECT_EQ(cases[i].second, decoded[i].value().get_slice().to_string()) << "column " << i;
    }
}

TEST(SortKeyBoundsTest, CharMatchesLegacyNormalization) {
    VariantTuple t = tuple_of({scalar(TYPE_CHAR, Datum(Slice("ab\0\0", 4)))});
    VariantTuple decoded = decode(encode(t));
    ASSERT_EQ(1u, decoded.size());
    EXPECT_EQ("ab", decoded[0].value().get_slice().to_string());
    expect_same_tuple(legacy_round_trip(t), decoded);
}

TEST(SortKeyBoundsTest, ZeroDateMatchesLegacy) {
    VariantTuple t = tuple_of({scalar(TYPE_DATE, Datum(DateValue{date::ZERO_EPOCH_JULIAN})),
                               scalar(TYPE_DATETIME, Datum(zero_datetime()))});
    VariantTuple decoded = decode(encode(t));
    expect_same_tuple(legacy_round_trip(t), decoded);
    ASSERT_EQ(2u, decoded.size());
    EXPECT_EQ(DateValue::create(1400, 1, 1), decoded[0].value().get_date());
    EXPECT_EQ(TimestampValue::create(1400, 1, 1, 0, 0, 0, 0), decoded[1].value().get_timestamp());
}

TEST(SortKeyBoundsTest, EncodesDocumentedLayout) {
    // version 1, count 2 (big-endian), BIGINT, DECIMAL64(18, 4), value 5, NULL.
    std::string expected = bytes_of({0x01, 0x00, 0x02, TYPE_BIGINT, TYPE_DECIMAL64, 18, 4});
    expected += bytes_of({0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05});
    expected += bytes_of({0x00});
    EXPECT_EQ(expected, encode(tuple_of({bigint(5), decimal(TYPE_DECIMAL64, 18, 4, Datum())})));

    // Strings escape 0x00 as 0x00 0x01 and end with 0x00 0x00, also in the last column.
    EXPECT_EQ(bytes_of({0x01, 0x00, 0x01, TYPE_VARCHAR, 0x01, 'a', 0x00, 0x01, 'b', 0x00, 0x00}),
              encode(tuple_of({varchar(std::string("a\0b", 3))})));
}

// This branch encodes BOOLEAN and DECIMAL256 values itself rather than through its KeyCoder. The bytes must be the
// KeyCoder full ascending encoding of later versions, so that each version reads the bounds the other one wrote.
TEST(SortKeyBoundsTest, EncodesBooleanAndDecimal256LikeKeyCoder) {
    auto expect_layout = [](const std::string& label, const VariantTuple& t, const std::string& expected) {
        EXPECT_EQ(expected, encode(t)) << label;
        expect_same_tuple(t, decode(expected));
    };

    // BOOLEAN: one byte, 0 or 1.
    const std::string boolean_header = bytes_of({0x01, 0x00, 0x01, TYPE_BOOLEAN});
    expect_layout("false", tuple_of({scalar(TYPE_BOOLEAN, Datum(static_cast<int8_t>(0)))}),
                  boolean_header + bytes_of({0x01, 0x00}));
    expect_layout("true", tuple_of({scalar(TYPE_BOOLEAN, Datum(static_cast<int8_t>(1)))}),
                  boolean_header + bytes_of({0x01, 0x01}));

    // DECIMAL256: 32 bytes, the high 128 bits with the sign bit flipped and then the low 128 bits, both big-endian.
    const std::string decimal256_header = bytes_of({0x01, 0x00, 0x01, TYPE_DECIMAL256, 76, 20});
    expect_layout("5 * 2^128 + 7",
                  tuple_of({decimal(TYPE_DECIMAL256, 76, 20, Datum(int256_t(int128_t{5}, uint128_t{7})))}),
                  decimal256_header + bytes_of({0x01, 0x80}) + std::string(14, '\x00') + bytes_of({0x05}) +
                          std::string(15, '\x00') + bytes_of({0x07}));
    expect_layout("-1", tuple_of({decimal(TYPE_DECIMAL256, 76, 20, Datum(int256_t(int128_t{-1}, ~uint128_t{0})))}),
                  decimal256_header + bytes_of({0x01, 0x7F}) + std::string(31, '\xFF'));
}

TEST(SortKeyBoundsTest, PreservesOrderForEqualHeaders) {
    auto one = [](DatumVariant v) { return tuple_of({std::move(v)}); };
    auto tinyint = [](int8_t v) { return scalar(TYPE_TINYINT, Datum(v)); };
    auto smallint = [](int16_t v) { return scalar(TYPE_SMALLINT, Datum(v)); };
    auto int32 = [](int32_t v) { return scalar(TYPE_INT, Datum(v)); };
    auto largeint = [](int128_t v) { return scalar(TYPE_LARGEINT, Datum(v)); };
    auto boolean = [](bool v) { return scalar(TYPE_BOOLEAN, Datum(static_cast<int8_t>(v))); };
    auto dec32 = [](int32_t v) { return decimal(TYPE_DECIMAL32, 9, 2, Datum(v)); };
    auto dec64 = [](int64_t v) { return decimal(TYPE_DECIMAL64, 18, 4, Datum(v)); };
    auto dec128 = [](int128_t v) { return decimal(TYPE_DECIMAL128, 38, 10, Datum(v)); };
    auto dec256 = [](int256_t v) { return decimal(TYPE_DECIMAL256, 76, 0, Datum(v)); };
    auto decv2 = [](const std::string& v) { return scalar(TYPE_DECIMALV2, Datum(DecimalV2Value(v))); };
    auto date_of = [](DateValue v) { return scalar(TYPE_DATE, Datum(v)); };
    auto datetime_of = [](TimestampValue v) { return scalar(TYPE_DATETIME, Datum(v)); };
    auto chr = [](const std::string& s) { return scalar(TYPE_CHAR, Datum(Slice(s))); };
    auto varbinary = [](const std::string& s) { return scalar(TYPE_VARBINARY, Datum(Slice(s))); };
    const int256_t big(int128_t{5}, uint128_t{7});
    const DateValue zero_date{date::ZERO_EPOCH_JULIAN};

    // Signed integers, including negatives and the extremes.
    expect_order_preserved("tinyint", one(tinyint(-128)), one(tinyint(127)));
    expect_order_preserved("tinyint -1/0", one(tinyint(-1)), one(tinyint(0)));
    expect_order_preserved("smallint", one(smallint(-300)), one(smallint(-299)));
    expect_order_preserved("int", one(int32(std::numeric_limits<int32_t>::min())),
                           one(int32(std::numeric_limits<int32_t>::max())));
    expect_order_preserved("bigint", one(bigint(std::numeric_limits<int64_t>::min())), one(bigint(-1)));
    expect_order_preserved("bigint -1/0", one(bigint(-1)), one(bigint(0)));
    expect_order_preserved("bigint equal", one(bigint(5)), one(bigint(5)));
    expect_order_preserved("largeint", one(largeint(-(int128_t{1} << 100))), one(largeint(int128_t{1} << 100)));
    expect_order_preserved("largeint -1/0", one(largeint(-1)), one(largeint(0)));
    expect_order_preserved("boolean", one(boolean(false)), one(boolean(true)));

    // Decimals.
    expect_order_preserved("decimal32", one(dec32(-1)), one(dec32(1)));
    expect_order_preserved("decimal64", one(dec64(-123456789012LL)), one(dec64(-123456789011LL)));
    expect_order_preserved("decimal128", one(dec128(-(int128_t{1} << 90))), one(dec128(int128_t{1} << 90)));
    expect_order_preserved("decimal256 -big/big", one(dec256(-big)), one(dec256(big)));
    expect_order_preserved("decimal256 negatives", one(dec256(-big)), one(dec256(-int256_t(int128_t{5}, 6))));
    expect_order_preserved("decimal256 2^128-1/2^128",
                           one(dec256(int256_t(int128_t{0}, std::numeric_limits<uint128_t>::max()))),
                           one(dec256(int256_t(int128_t{1}, uint128_t{0}))));
    expect_order_preserved("decimal256 -1/1", one(dec256(int256_t(-1))), one(dec256(int256_t(1))));
    expect_order_preserved("decimalv2", one(decv2("-123.456")), one(decv2("0.001")));

    // Dates; the zero date normalizes to 1400-01-01, after 1000-01-01.
    expect_order_preserved("date", one(date_of(DateValue::create(2024, 1, 1))),
                           one(date_of(DateValue::create(2024, 1, 2))));
    expect_order_preserved("zero date/1000-01-01", one(date_of(zero_date)),
                           one(date_of(DateValue::create(1000, 1, 1))));
    expect_order_preserved("zero date/1400-01-01", one(date_of(zero_date)),
                           one(date_of(DateValue::create(1400, 1, 1))));
    expect_order_preserved("datetime micros", one(datetime_of(TimestampValue::create(2024, 1, 1, 0, 0, 0, 1))),
                           one(datetime_of(TimestampValue::create(2024, 1, 1, 0, 0, 0, 2))));
    expect_order_preserved("datetime day boundary",
                           one(datetime_of(TimestampValue::create(1999, 12, 31, 23, 59, 59, 999999))),
                           one(datetime_of(TimestampValue::create(2000, 1, 1, 0, 0, 0, 0))));

    // CHAR compares after normalization.
    expect_order_preserved("char padded/unpadded", one(chr(std::string("ab\0\0", 4))), one(chr("ab")));
    expect_order_preserved("char content after 0x00", one(chr(std::string("ab\0z", 4))), one(chr("ab")));
    expect_order_preserved("char prefix", one(chr("a")), one(chr("ab")));
    expect_order_preserved("char", one(chr("ab")), one(chr("b")));

    // Strings: prefixes, embedded 0x00, high bytes.
    expect_order_preserved("varchar prefix", one(varchar("a")), one(varchar("ab")));
    expect_order_preserved("varchar a/a00", one(varchar("a")), one(varchar(std::string("a\0", 2))));
    expect_order_preserved("varchar a00/a01", one(varchar(std::string("a\0", 2))), one(varchar("a\x01")));
    expect_order_preserved("varchar a00b/a00", one(varchar(std::string("a\0b", 3))),
                           one(varchar(std::string("a\0", 2))));
    expect_order_preserved("varchar empty/00", one(varchar("")), one(varchar(std::string("\0", 1))));
    expect_order_preserved("varchar empty/a", one(varchar("")), one(varchar("a")));
    expect_order_preserved("varchar ff/a", one(varchar("\xff")), one(varchar("a")));
    expect_order_preserved("varchar ffff/ff", one(varchar("\xff\xff")), one(varchar("\xff")));
    expect_order_preserved("varbinary", one(varbinary(std::string("\0\0\x01", 3))),
                           one(varbinary(std::string("\0\x01", 2))));

    // NULL sorts first.
    expect_order_preserved("null/bigint min", one(null_of(TYPE_BIGINT)),
                           one(bigint(std::numeric_limits<int64_t>::min())));
    expect_order_preserved("null/empty string", one(null_of(TYPE_VARCHAR)), one(varchar("")));
    expect_order_preserved("null/null", one(null_of(TYPE_BIGINT)), one(null_of(TYPE_BIGINT)));

    // Multi-column: ties broken in a later column, and no ambiguity at column boundaries.
    expect_order_preserved("tie broken in third column", tuple_of({bigint(1), varchar("a"), bigint(3)}),
                           tuple_of({bigint(1), varchar("a"), bigint(4)}));
    expect_order_preserved("tie broken in second column", tuple_of({varchar("ab"), bigint(1)}),
                           tuple_of({varchar("ab"), bigint(-1)}));
    expect_order_preserved("first column decides", tuple_of({varchar("a"), bigint(9)}),
                           tuple_of({varchar(std::string("a\0", 2)), bigint(1)}));
    expect_order_preserved("column boundary", tuple_of({varchar("a"), varchar("b")}),
                           tuple_of({varchar("ab"), varchar("")}));
    expect_order_preserved("null tie", tuple_of({null_of(TYPE_BIGINT), bigint(5)}),
                           tuple_of({null_of(TYPE_BIGINT), bigint(6)}));
    expect_order_preserved("normalized char tie", tuple_of({chr(std::string("ab\0", 3)), int32(2)}),
                           tuple_of({chr("ab"), int32(1)}));
    expect_order_preserved("normalized date tie", tuple_of({date_of(zero_date), int32(1)}),
                           tuple_of({date_of(DateValue::create(1400, 1, 1)), int32(2)}));
}

TEST(SortKeyBoundsTest, EncodeRejectsTooManyColumns) {
    VariantTuple t;
    t.reserve(65536);
    for (int64_t i = 0; i < 65535; ++i) {
        t.append(bigint(i));
    }
    std::string bytes;
    ASSERT_OK(encode_sort_key_bound(t, &bytes));
    VariantTuple decoded;
    ASSERT_OK(decode_sort_key_bound(Slice(bytes), &decoded));
    ASSERT_EQ(65535u, decoded.size());
    EXPECT_EQ(0, t.compare(decoded));
    SegmentMetadataPB meta;
    meta.set_sort_key_min_encoded(bytes);
    meta.set_sort_key_max_encoded(bytes);
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 65535));

    t.append(bigint(65535));
    std::string rejected;
    Status st = encode_sort_key_bound(t, &rejected);
    EXPECT_TRUE(st.is_not_supported()) << st;
}

TEST(SortKeyBoundsTest, DecimalPrecisionLimits) {
    const std::vector<std::pair<LogicalType, uint8_t>> limits = {
            {TYPE_DECIMAL32, 9}, {TYPE_DECIMAL64, 18}, {TYPE_DECIMAL128, 38}, {TYPE_DECIMAL256, 76}};
    for (const auto& [type, max_precision] : limits) {
        const std::string accepted = decimal_null_bound(type, max_precision, max_precision);
        VariantTuple decoded;
        ASSERT_OK(decode_sort_key_bound(Slice(accepted), &decoded));
        ASSERT_EQ(1u, decoded.size());
        EXPECT_EQ(type, decoded[0].type()->type());
        EXPECT_EQ(static_cast<int>(max_precision), decoded[0].type()->precision());
        EXPECT_EQ(static_cast<int>(max_precision), decoded[0].type()->scale());
        EXPECT_TRUE(decoded[0].value().is_null());

        const std::vector<std::pair<std::string, std::string>> rejected = {
                {"precision above the limit", decimal_null_bound(type, max_precision + 1, 0)},
                {"precision 0", decimal_null_bound(type, 0, 0)},
                {"scale above precision", decimal_null_bound(type, 5, 6)},
        };
        for (const auto& [label, bytes] : rejected) {
            VariantTuple out;
            Status st = decode_sort_key_bound(Slice(bytes), &out);
            EXPECT_TRUE(st.is_corruption()) << type_to_string(type) << " " << label << ": " << st;
        }
    }
}

TEST(SortKeyBoundsTest, DecodeRejectsMalformed) {
    const std::string bigint5 = encode(tuple_of({bigint(5)}));
    std::vector<std::pair<std::string, std::string>> cases = malformed_headers();
    cases.emplace_back("marker 0x02", bytes_of({0x01, 0x00, 0x01, TYPE_BIGINT, 0x02}));
    cases.emplace_back("missing marker", bigint5.substr(0, 4));
    cases.emplace_back("missing second column", bytes_of({0x01, 0x00, 0x02, TYPE_BIGINT, TYPE_BIGINT, 0x00}));
    cases.emplace_back("boolean payload 0x02", bytes_of({0x01, 0x00, 0x01, TYPE_BOOLEAN, 0x01, 0x02}));
    cases.emplace_back("truncated bigint", bigint5.substr(0, bigint5.size() - 1));
    cases.emplace_back("varchar escape 0x00 0x02",
                       bytes_of({0x01, 0x00, 0x01, TYPE_VARCHAR, 0x01, 'a', 0x00, 0x02, 0x00, 0x00}));
    cases.emplace_back("varchar without terminator", bytes_of({0x01, 0x00, 0x01, TYPE_VARCHAR, 0x01, 'a', 'b'}));
    cases.emplace_back("varchar ending in a lone 0x00", bytes_of({0x01, 0x00, 0x01, TYPE_VARCHAR, 0x01, 'a', 0x00}));
    cases.emplace_back("one trailing byte", bigint5 + std::string(1, '\0'));
    for (const auto& [label, bytes] : cases) {
        VariantTuple out;
        Status st = decode_sort_key_bound(Slice(bytes), &out);
        EXPECT_TRUE(st.is_corruption()) << label << ": " << st;
    }
}

TEST(SortKeyBoundsTest, EncodeRejectsUnsupportedAndEmpty) {
    std::string out;
    Status st = encode_sort_key_bound(tuple_of({bigint(1), scalar(TYPE_DOUBLE, Datum(1.5))}), &out);
    EXPECT_TRUE(st.is_not_supported()) << st;

    st = encode_sort_key_bound(VariantTuple(), &out);
    EXPECT_TRUE(st.is_invalid_argument()) << st;

    // A decimal type whose precision the decoder would reject is not encoded either.
    st = encode_sort_key_bound(tuple_of({decimal(TYPE_DECIMAL32, 10, 2, Datum(static_cast<int32_t>(1)))}), &out);
    EXPECT_TRUE(st.is_not_supported()) << st;
}

namespace {

void set_deprecated_pair(const VariantTuple& min, const VariantTuple& max, SegmentMetadataPB* meta) {
    min.to_proto(meta->mutable_deprecated_sort_key_min());
    max.to_proto(meta->mutable_deprecated_sort_key_max());
}

void set_encoded_pair(const VariantTuple& min, const VariantTuple& max, SegmentMetadataPB* meta) {
    meta->set_sort_key_min_encoded(encode(min));
    meta->set_sort_key_max_encoded(encode(max));
}

void expect_no_bound_fields(const SegmentMetadataPB& meta) {
    EXPECT_FALSE(meta.has_deprecated_sort_key_min());
    EXPECT_FALSE(meta.has_deprecated_sort_key_max());
    EXPECT_FALSE(meta.has_sort_key_min_encoded());
    EXPECT_FALSE(meta.has_sort_key_max_encoded());
}

} // namespace

TEST(SegmentSortKeyBoundsTest, SetWritesEncodedOnly) {
    VariantTuple min = tuple_of({bigint(1), varchar("a")});
    VariantTuple max = tuple_of({bigint(9), varchar("z")});
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(min, max, &meta);
    EXPECT_TRUE(meta.has_sort_key_min_encoded());
    EXPECT_TRUE(meta.has_sort_key_max_encoded());
    EXPECT_FALSE(meta.has_deprecated_sort_key_min());
    EXPECT_FALSE(meta.has_deprecated_sort_key_max());
    EXPECT_TRUE(has_segment_sort_key_bounds(meta));

    VariantTuple got_min, got_max;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    expect_same_tuple(min, got_min);
    expect_same_tuple(max, got_max);
}

TEST(SegmentSortKeyBoundsTest, SetClearsDeprecatedPairAndStaleEncoded) {
    SegmentMetadataPB meta;
    set_deprecated_pair(tuple_of({bigint(100)}), tuple_of({bigint(200)}), &meta);
    meta.set_sort_key_min_encoded("stale");
    meta.set_sort_key_max_encoded("stale");

    VariantTuple min = tuple_of({bigint(1)});
    VariantTuple max = tuple_of({bigint(2)});
    set_segment_sort_key_bounds(min, max, &meta);
    EXPECT_FALSE(meta.has_deprecated_sort_key_min());
    EXPECT_FALSE(meta.has_deprecated_sort_key_max());
    EXPECT_EQ(encode(min), meta.sort_key_min_encoded());
    EXPECT_EQ(encode(max), meta.sort_key_max_encoded());
}

TEST(SegmentSortKeyBoundsTest, SetEmptyClearsAll) {
    for (int variant = 0; variant < 2; ++variant) {
        SegmentMetadataPB meta;
        set_deprecated_pair(tuple_of({bigint(1)}), tuple_of({bigint(2)}), &meta);
        meta.set_sort_key_min_encoded("stale");
        meta.set_sort_key_max_encoded("stale");
        if (variant == 0) {
            set_segment_sort_key_bounds(VariantTuple(), VariantTuple(), &meta);
        } else {
            set_segment_sort_key_bounds(tuple_of({bigint(1)}), VariantTuple(), &meta);
        }
        expect_no_bound_fields(meta);
        EXPECT_FALSE(has_segment_sort_key_bounds(meta));
    }
}

TEST(SegmentSortKeyBoundsTest, SetUnsupportedFallsBackToDeprecated) {
    VariantTuple min = tuple_of({scalar(TYPE_DOUBLE, Datum(1.5))});
    VariantTuple max = tuple_of({scalar(TYPE_DOUBLE, Datum(2.5))});
    SegmentMetadataPB meta;
    meta.set_sort_key_min_encoded("stale");
    meta.set_sort_key_max_encoded("stale");
    set_segment_sort_key_bounds(min, max, &meta);
    EXPECT_FALSE(meta.has_sort_key_min_encoded());
    EXPECT_FALSE(meta.has_sort_key_max_encoded());
    EXPECT_TRUE(meta.has_deprecated_sort_key_min());
    EXPECT_TRUE(meta.has_deprecated_sort_key_max());
    EXPECT_TRUE(has_segment_sort_key_bounds(meta));

    VariantTuple got_min, got_max;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    expect_same_tuple(min, got_min);
    expect_same_tuple(max, got_max);
}

TEST(SegmentSortKeyBoundsTest, DeprecatedPairWinsOverEncoded) {
    SegmentMetadataPB meta;
    set_encoded_pair(tuple_of({bigint(5)}), tuple_of({bigint(6)}), &meta);
    set_deprecated_pair(tuple_of({bigint(1), bigint(1)}), tuple_of({bigint(2), bigint(2)}), &meta);
    VariantTuple got_min, got_max;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    ASSERT_EQ(2, got_min.size());
    EXPECT_EQ(1, got_min[0].value().get_int64());
    EXPECT_EQ(2, got_max[0].value().get_int64());
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 1));
}

TEST(SegmentSortKeyBoundsTest, EmptyDeprecatedPairHidesStaleEncoded) {
    SegmentMetadataPB meta;
    set_encoded_pair(tuple_of({bigint(5)}), tuple_of({bigint(6)}), &meta);
    meta.mutable_deprecated_sort_key_min();
    meta.mutable_deprecated_sort_key_max();
    EXPECT_FALSE(has_segment_sort_key_bounds(meta));
    VariantTuple got_min = tuple_of({bigint(1)}), got_max = tuple_of({bigint(1)});
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    EXPECT_TRUE(got_min.empty());
    EXPECT_TRUE(got_max.empty());
    DatumVariant lo, hi;
    EXPECT_TRUE(decode_segment_leading_sort_key_bounds(meta, &lo, &hi).is_not_found());
}

TEST(SegmentSortKeyBoundsTest, SingleFieldsAreIgnored) {
    SegmentMetadataPB only_min_encoded;
    only_min_encoded.set_sort_key_min_encoded(encode(tuple_of({bigint(1)})));
    EXPECT_FALSE(has_segment_sort_key_bounds(only_min_encoded));

    SegmentMetadataPB meta;
    set_encoded_pair(tuple_of({bigint(5)}), tuple_of({bigint(6)}), &meta);
    tuple_of({bigint(1)}).to_proto(meta.mutable_deprecated_sort_key_min());
    EXPECT_TRUE(has_segment_sort_key_bounds(meta));
    VariantTuple got_min, got_max;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    EXPECT_EQ(5, got_min[0].value().get_int64());
    EXPECT_EQ(6, got_max[0].value().get_int64());
}

TEST(SegmentSortKeyBoundsTest, DeprecatedOnlyStillDecodes) {
    VariantTuple min = tuple_of({bigint(1), varchar("a")});
    VariantTuple max = tuple_of({bigint(9), varchar("z")});
    SegmentMetadataPB meta;
    set_deprecated_pair(min, max, &meta);
    EXPECT_TRUE(has_segment_sort_key_bounds(meta));
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 3));
    VariantTuple got_min, got_max;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    expect_same_tuple(legacy_round_trip(min), got_min);
    expect_same_tuple(legacy_round_trip(max), got_max);
}

TEST(SegmentSortKeyBoundsTest, AbsentDecodesToEmpty) {
    SegmentMetadataPB meta;
    EXPECT_FALSE(has_segment_sort_key_bounds(meta));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 1));
    VariantTuple got_min = tuple_of({bigint(1)}), got_max = tuple_of({bigint(1)});
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
    EXPECT_TRUE(got_min.empty());
    EXPECT_TRUE(got_max.empty());
    DatumVariant lo, hi;
    EXPECT_TRUE(decode_segment_leading_sort_key_bounds(meta, &lo, &hi).is_not_found());
}

TEST(SegmentSortKeyBoundsTest, HaveArity) {
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(tuple_of({bigint(1), bigint(2), bigint(3)}),
                                tuple_of({bigint(4), bigint(5), bigint(6)}), &meta);
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 3));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 2));

    // Bounds of different arities match neither of them.
    SegmentMetadataPB mixed(meta);
    mixed.set_sort_key_max_encoded(encode(tuple_of({bigint(4), bigint(5)})));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(mixed, 3));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(mixed, 2));

    // A bound in format version 1 that is too short to hold the column count still counts as bounds, but matches
    // no arity.
    meta.set_sort_key_max_encoded(std::string("\x01\x00", 2));
    EXPECT_TRUE(has_segment_sort_key_bounds(meta));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 3));
}

TEST(SegmentSortKeyBoundsTest, LeadingDecode) {
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(tuple_of({bigint(10), varchar("x"), bigint(1)}),
                                tuple_of({bigint(20), varchar("y"), bigint(2)}), &meta);
    DatumVariant lo, hi;
    ASSERT_OK(decode_segment_leading_sort_key_bounds(meta, &lo, &hi));
    EXPECT_EQ(10, lo.value().get_int64());
    EXPECT_EQ(20, hi.value().get_int64());

    SegmentMetadataPB null_meta;
    set_segment_sort_key_bounds(tuple_of({null_of(TYPE_BIGINT), bigint(1)}), tuple_of({bigint(20), bigint(2)}),
                                &null_meta);
    ASSERT_OK(decode_segment_leading_sort_key_bounds(null_meta, &lo, &hi));
    EXPECT_TRUE(lo.value().is_null());
    EXPECT_EQ(20, hi.value().get_int64());

    SegmentMetadataPB legacy;
    set_deprecated_pair(tuple_of({bigint(7), bigint(1)}), tuple_of({bigint(8), bigint(2)}), &legacy);
    ASSERT_OK(decode_segment_leading_sort_key_bounds(legacy, &lo, &hi));
    EXPECT_EQ(7, lo.value().get_int64());
    EXPECT_EQ(8, hi.value().get_int64());
}

namespace {

// A two-column bound (BIGINT, DECIMAL64(18, 4)) of 25 bytes. Its header is bytes 0-6: version, column count
// (bytes 1-2), the BIGINT type byte (byte 3), then the DECIMAL64 type byte, precision and scale (bytes 4-6).
// Column 0's value is bytes 7-15 (marker and 8-byte payload), column 1's is bytes 16-24.
VariantTuple two_column_bound(int64_t key) {
    return tuple_of({bigint(key), decimal(TYPE_DECIMAL64, 18, 4, Datum(key * 10))});
}

SegmentMetadataPB two_column_bounds_meta() {
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(two_column_bound(10), two_column_bound(20), &meta);
    EXPECT_EQ(25u, meta.sort_key_min_encoded().size());
    EXPECT_EQ(25u, meta.sort_key_max_encoded().size());
    return meta;
}

// Turns a valid encoded bound into a damaged one.
using BoundDamage = std::function<std::string(const std::string&)>;

BoundDamage set_byte(size_t pos, uint8_t value) {
    return [=](const std::string& bytes) {
        std::string damaged = bytes;
        damaged[pos] = static_cast<char>(value);
        return damaged;
    };
}

BoundDamage truncate_to(size_t size) {
    return [=](const std::string& bytes) { return bytes.substr(0, size); };
}

BoundDamage replace_with(const std::string& replacement) {
    return [=](const std::string&) { return replacement; };
}

// Applies `damage` to the min bound, the max bound, or both, of an otherwise valid encoded pair.
std::vector<std::pair<std::string, SegmentMetadataPB>> damage_each_side(const SegmentMetadataPB& valid,
                                                                        const BoundDamage& damage) {
    std::vector<std::pair<std::string, SegmentMetadataPB>> metas;
    for (const std::string side : {"min", "max", "both"}) {
        SegmentMetadataPB meta(valid);
        if (side != "max") meta.set_sort_key_min_encoded(damage(valid.sort_key_min_encoded()));
        if (side != "min") meta.set_sort_key_max_encoded(damage(valid.sort_key_max_encoded()));
        metas.emplace_back(side, std::move(meta));
    }
    return metas;
}

} // namespace

// An encoded pair in a format version this binary does not know, such as one written by a later version before a
// rollback, is treated exactly like a segment without bounds by every helper, whether one bound or both carry it.
TEST(SegmentSortKeyBoundsTest, UnknownEncodedFormatVersionIsAbsent) {
    const SegmentMetadataPB valid = two_column_bounds_meta();
    ASSERT_TRUE(has_segment_sort_key_bounds(valid));
    ASSERT_TRUE(segment_sort_key_bounds_have_arity(valid, 2));

    const std::vector<std::pair<std::string, BoundDamage>> damages = {
            {"version 3", set_byte(0, 0x03)},
            {"version 0", set_byte(0, 0x00)},
            {"version 0xFF", set_byte(0, 0xFF)},
            {"garbage", replace_with("garbage")},
            {"empty, without a version byte", replace_with(std::string())},
    };
    for (const auto& [label, damage] : damages) {
        for (const auto& [side, meta] : damage_each_side(valid, damage)) {
            SCOPED_TRACE(label + " in " + side);
            EXPECT_FALSE(has_segment_sort_key_bounds(meta));
            EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 2));
            VariantTuple got_min = tuple_of({bigint(1)}), got_max = tuple_of({bigint(1)});
            ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
            EXPECT_TRUE(got_min.empty());
            EXPECT_TRUE(got_max.empty());
            DatumVariant lo, hi;
            EXPECT_TRUE(decode_segment_leading_sort_key_bounds(meta, &lo, &hi).is_not_found());
        }
    }
}

// An encoded pair in format version 1 with a damaged header still counts as bounds, because only the version byte is
// read to choose the representation. Both decoders return Corruption for it, which callers treat as no bounds for
// the segment. segment_sort_key_bounds_have_arity reads only the column count, so it follows the count bytes.
TEST(SegmentSortKeyBoundsTest, DamagedEncodedHeaderIsCorruption) {
    const SegmentMetadataPB valid = two_column_bounds_meta();

    struct HeaderDamage {
        std::string label;
        BoundDamage damage;
        bool count_intact;
    };
    const std::vector<HeaderDamage> damages = {
            {"type byte 0xFF", set_byte(3, 0xFF), true},
            {"unsupported type TYPE_JSON", set_byte(3, TYPE_JSON), true},
            {"decimal precision 19", set_byte(5, 19), true},
            {"decimal precision 0", set_byte(5, 0), true},
            {"decimal scale above precision", set_byte(6, 19), true},
            {"header truncated in the decimal descriptor", truncate_to(5), true},
            {"header truncated in the fixed part", truncate_to(2), false},
            {"column count 0", set_byte(2, 0x00), false},
            {"column count above the remaining bytes", set_byte(1, 0x01), false},
    };
    for (const auto& [label, damage, count_intact] : damages) {
        for (const auto& [side, meta] : damage_each_side(valid, damage)) {
            SCOPED_TRACE(label + " in " + side);
            EXPECT_TRUE(has_segment_sort_key_bounds(meta));
            EXPECT_EQ(count_intact, segment_sort_key_bounds_have_arity(meta, 2));
            VariantTuple got_min, got_max;
            EXPECT_TRUE(decode_segment_sort_key_bounds(meta, &got_min, &got_max).is_corruption());
            DatumVariant lo, hi;
            EXPECT_TRUE(decode_segment_leading_sort_key_bounds(meta, &lo, &hi).is_corruption());
        }
    }
}

// Only the column count is read, never the values: bounds whose values are damaged still have their arity.
TEST(SegmentSortKeyBoundsTest, HaveArityDoesNotReadValues) {
    for (size_t n : {1, 3, 71}) {
        std::vector<DatumVariant> values;
        for (size_t i = 0; i < n; ++i) {
            switch (i % 3) {
            case 0:
                values.push_back(bigint(static_cast<int64_t>(i)));
                break;
            case 1:
                values.push_back(varchar(std::to_string(i)));
                break;
            default:
                values.push_back(decimal(TYPE_DECIMAL64, 18, 4, Datum(static_cast<int64_t>(i))));
                break;
            }
        }
        const VariantTuple bound = tuple_of(std::move(values));
        SegmentMetadataPB meta;
        set_segment_sort_key_bounds(bound, bound, &meta);
        EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, n)) << n;
        EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, n + 1)) << n;
    }

    const SegmentMetadataPB valid = two_column_bounds_meta();
    const std::vector<std::pair<std::string, BoundDamage>> damages = {
            {"column 0 marker 0x02", set_byte(7, 0x02)},
            {"column 1 payload cut short", truncate_to(20)},
            {"values missing", truncate_to(7)},
    };
    for (const auto& [label, damage] : damages) {
        for (const auto& [side, meta] : damage_each_side(valid, damage)) {
            SCOPED_TRACE(label + " in " + side);
            EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));
            VariantTuple got_min, got_max;
            EXPECT_TRUE(decode_segment_sort_key_bounds(meta, &got_min, &got_max).is_corruption());
        }
    }
}

// Damage after a valid header is not treated as absent bounds: the segment still has bounds of the declared arity,
// the full decode fails with Corruption, and the leading decode fails only when column 0's value is damaged.
TEST(SegmentSortKeyBoundsTest, MalformedEncodedValuesAreCorruption) {
    const SegmentMetadataPB valid = two_column_bounds_meta();

    struct ValueDamage {
        std::string label;
        BoundDamage damage;
        bool leading_damaged;
    };
    const std::vector<ValueDamage> damages = {
            {"column 0 marker 0x02", set_byte(7, 0x02), true},
            {"column 0 value truncated", truncate_to(12), true},
            {"values missing", truncate_to(7), true},
            {"column 1 marker 0x02", set_byte(16, 0x02), false},
            {"column 1 value truncated", truncate_to(24), false},
            {"column 1 value missing", truncate_to(16), false},
            {"one trailing byte", [](const std::string& bytes) { return bytes + std::string(1, '\0'); }, false},
    };
    for (const auto& [label, damage, leading_damaged] : damages) {
        for (const auto& [side, meta] : damage_each_side(valid, damage)) {
            SCOPED_TRACE(label + " in " + side);
            EXPECT_TRUE(has_segment_sort_key_bounds(meta));
            EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));
            VariantTuple got_min, got_max;
            EXPECT_TRUE(decode_segment_sort_key_bounds(meta, &got_min, &got_max).is_corruption());
            DatumVariant lo, hi;
            Status st = decode_segment_leading_sort_key_bounds(meta, &lo, &hi);
            if (leading_damaged) {
                EXPECT_TRUE(st.is_corruption()) << st;
            } else {
                ASSERT_OK(st);
                EXPECT_EQ(10, lo.value().get_int64());
                EXPECT_EQ(20, hi.value().get_int64());
            }
        }
    }
}

// A deprecated pair refreshed by an older binary still wins over an encoded pair this binary cannot read, whether
// its format version is unknown or its header is damaged.
TEST(SegmentSortKeyBoundsTest, DeprecatedPairWinsOverUnreadableEncoded) {
    // One BIGINT column: byte 0 is the format version, byte 3 the type byte.
    for (const auto& [pos, value] : std::vector<std::pair<size_t, uint8_t>>{{0, 0x03}, {3, 0xFF}}) {
        SCOPED_TRACE(pos);
        SegmentMetadataPB meta;
        set_encoded_pair(tuple_of({bigint(5)}), tuple_of({bigint(6)}), &meta);
        (*meta.mutable_sort_key_min_encoded())[pos] = static_cast<char>(value);
        (*meta.mutable_sort_key_max_encoded())[pos] = static_cast<char>(value);
        set_deprecated_pair(tuple_of({bigint(1), bigint(1)}), tuple_of({bigint(2), bigint(2)}), &meta);
        EXPECT_TRUE(has_segment_sort_key_bounds(meta));
        EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));
        VariantTuple got_min, got_max;
        ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
        ASSERT_EQ(2, got_min.size());
        ASSERT_EQ(2, got_max.size());
        EXPECT_EQ(1, got_min[0].value().get_int64());
        EXPECT_EQ(2, got_max[0].value().get_int64());
        DatumVariant lo, hi;
        ASSERT_OK(decode_segment_leading_sort_key_bounds(meta, &lo, &hi));
        EXPECT_EQ(1, lo.value().get_int64());
        EXPECT_EQ(2, hi.value().get_int64());
    }
}

TEST(SegmentSortKeyBoundsTest, LeadingDecodeIgnoresLaterColumns) {
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(tuple_of({bigint(10), bigint(1), bigint(2)}),
                                tuple_of({bigint(20), bigint(3), bigint(4)}), &meta);
    // Corrupt the value marker of column 2 (the last 9 bytes are its marker and 8-byte payload).
    for (auto* bytes : {meta.mutable_sort_key_min_encoded(), meta.mutable_sort_key_max_encoded()}) {
        (*bytes)[bytes->size() - 9] = static_cast<char>(0x7f);
    }
    DatumVariant lo, hi;
    ASSERT_OK(decode_segment_leading_sort_key_bounds(meta, &lo, &hi));
    EXPECT_EQ(10, lo.value().get_int64());
    EXPECT_EQ(20, hi.value().get_int64());
    VariantTuple got_min, got_max;
    EXPECT_TRUE(decode_segment_sort_key_bounds(meta, &got_min, &got_max).is_corruption());

    // The whole header is still validated: a bad descriptor of a later column fails the leading decode too, rather
    // than leaving a usable leading column.
    SegmentMetadataPB bad_header;
    set_segment_sort_key_bounds(tuple_of({bigint(10), bigint(1)}), tuple_of({bigint(20), bigint(3)}), &bad_header);
    (*bad_header.mutable_sort_key_min_encoded())[3 + 1] = static_cast<char>(0x7f); // column 1's type byte
    EXPECT_TRUE(decode_segment_leading_sort_key_bounds(bad_header, &lo, &hi).is_corruption());
}

TEST(SegmentSortKeyBoundsTest, OneSidedEmptyDeprecatedPairIsAbsent) {
    for (int variant = 0; variant < 2; ++variant) {
        SegmentMetadataPB meta;
        set_encoded_pair(tuple_of({bigint(5)}), tuple_of({bigint(6)}), &meta);
        tuple_of({bigint(1)})
                .to_proto(variant == 0 ? meta.mutable_deprecated_sort_key_min()
                                       : meta.mutable_deprecated_sort_key_max());
        (variant == 0 ? meta.mutable_deprecated_sort_key_max() : meta.mutable_deprecated_sort_key_min());
        EXPECT_FALSE(has_segment_sort_key_bounds(meta)) << variant;
        DatumVariant lo, hi;
        EXPECT_TRUE(decode_segment_leading_sort_key_bounds(meta, &lo, &hi).is_not_found()) << variant;
        VariantTuple got_min = tuple_of({bigint(1)}), got_max = tuple_of({bigint(1)});
        ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max));
        EXPECT_TRUE(got_min.empty());
        EXPECT_TRUE(got_max.empty());
    }
}

namespace {

DatumVariant int_(int32_t v) {
    return scalar(TYPE_INT, Datum(v));
}

DatumVariant tinyint(int8_t v) {
    return scalar(TYPE_TINYINT, Datum(v));
}

DatumVariant boolean(bool v) {
    return scalar(TYPE_BOOLEAN, Datum(static_cast<int8_t>(v)));
}

DatumVariant date_of(DateValue v) {
    return scalar(TYPE_DATE, Datum(v));
}

DatumVariant datetime_of(TimestampValue v) {
    return scalar(TYPE_DATETIME, Datum(v));
}

DatumVariant chr(const std::string& s) {
    return scalar(TYPE_CHAR, Datum(Slice(s)));
}

DatumVariant varbinary(const std::string& s) {
    return scalar(TYPE_VARBINARY, Datum(Slice(s)));
}

std::string utf8(uint32_t cp) {
    std::string out;
    if (cp < 0x80) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
    return out;
}

std::string stored(const VariantTuple& t, SortKeyBoundSide side) {
    std::string out;
    Status st = encode_stored_sort_key_bound(t, side, &out);
    EXPECT_TRUE(st.ok()) << st;
    return out;
}

std::string string_of(const DatumVariant& v) {
    return v.value().get_slice().to_string();
}

// `first` followed by five BIGINT maximums: 40 bytes after `first`, so the bound never fits, and none of the
// BIGINT columns has a successor, so a maximum backs off to `first`.
VariantTuple then_max_bigints(DatumVariant first) {
    VariantTuple t = tuple_of({std::move(first)});
    for (int i = 0; i < 5; ++i) {
        t.append(bigint(INT64_MAX));
    }
    return t;
}

uint8_t version_of(const std::string& bytes) {
    return static_cast<uint8_t>(bytes[0]);
}

uint8_t flags_of(const std::string& bytes) {
    return static_cast<uint8_t>(bytes[5]);
}

// The cost of the values of a decoded bound, counted as for kSortKeyBoundPayloadBudget.
size_t payload_of(const VariantTuple& bound) {
    size_t total = 0;
    for (const DatumVariant& value : bound.values()) {
        if (value.value().is_null()) {
            continue;
        }
        const LogicalType type = value.type()->type();
        const bool is_string = type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_VARBINARY;
        total += is_string ? value.value().get_slice().size : value.type()->size();
    }
    return total;
}

} // namespace

// Payload exactly 36: untouched, byte-identical v1. 37: truncated.
TEST(SortKeyBoundsTruncationTest, StaysV1WhenPayloadFits) {
    auto t36 = tuple_of({int_(1), int_(2), int_(3), int_(4), int_(5), int_(6), int_(7), int_(8), int_(9)});
    std::string v1, stored;
    ASSERT_OK(encode_sort_key_bound(t36, &v1));
    for (auto side : {SortKeyBoundSide::kMin, SortKeyBoundSide::kMax}) {
        ASSERT_OK(encode_stored_sort_key_bound(t36, side, &stored));
        EXPECT_EQ(v1, stored);
    }
    auto t37 = t36;
    t37.append(tinyint(1));
    ASSERT_OK(encode_stored_sort_key_bound(t37, SortKeyBoundSide::kMin, &stored));
    EXPECT_EQ(2, static_cast<uint8_t>(stored[0]));
}

// (BIGINT 1..5): min keeps (1,2,3,4) flag 0; max is (1,2,3,5) flag 1.
TEST(SortKeyBoundsTruncationTest, MinDropsColumnsMaxTakesSuccessor) {
    auto t = tuple_of({bigint(1), bigint(2), bigint(3), bigint(4), bigint(5)});
    std::string min_bytes, max_bytes;
    ASSERT_OK(encode_stored_sort_key_bound(t, SortKeyBoundSide::kMin, &min_bytes));
    ASSERT_OK(encode_stored_sort_key_bound(t, SortKeyBoundSide::kMax, &max_bytes));
    // header: version, arity=5, count, flags
    EXPECT_EQ(std::string("\x02\x00\x05\x00\x04\x00", 6), min_bytes.substr(0, 6));
    EXPECT_EQ(std::string("\x02\x00\x05\x00\x04\x01", 6), max_bytes.substr(0, 6));
    EXPECT_EQ(0, decode(min_bytes).compare(tuple_of({bigint(1), bigint(2), bigint(3), bigint(4)})));
    EXPECT_EQ(0, decode(max_bytes).compare(tuple_of({bigint(1), bigint(2), bigint(3), bigint(5)})));
    EXPECT_GT(decode(max_bytes).compare(t), 0);
}

// Four BIGINTs fit (32 bytes), the fifth does not. Columns 3 and 2 hold INT64_MAX (no successor): back off to
// column 1 -> (1, 3).
TEST(SortKeyBoundsTruncationTest, MaxBacksOffWhenColumnHasNoSuccessor) {
    auto t = tuple_of({bigint(1), bigint(2), bigint(INT64_MAX), bigint(INT64_MAX), bigint(5)});
    std::string max_bytes;
    ASSERT_OK(encode_stored_sort_key_bound(t, SortKeyBoundSide::kMax, &max_bytes));
    EXPECT_EQ(0, decode(max_bytes).compare(tuple_of({bigint(1), bigint(3)})));
    EXPECT_EQ(std::string("\x02\x00\x05\x00\x02\x01", 6), max_bytes.substr(0, 6));
}

// (BIGINT 1, NULL BIGINT, BIGINT max x4): column 1 is NULL -> back off to column 0 -> (2).
TEST(SortKeyBoundsTruncationTest, MaxBacksOffOverNull) {
    auto t = tuple_of({bigint(1), null_of(TYPE_BIGINT), bigint(INT64_MAX), bigint(INT64_MAX), bigint(INT64_MAX),
                       bigint(INT64_MAX)});
    const std::string max_bytes = stored(t, SortKeyBoundSide::kMax);
    EXPECT_EQ(std::string("\x02\x00\x06\x00\x01\x01", 6), max_bytes.substr(0, 6));
    EXPECT_EQ(0, decode(max_bytes).compare(tuple_of({bigint(2)})));
}

// (BOOLEAN true, BIGINT max x5): nothing has a successor -> max stored as v1, byte-identical.
TEST(SortKeyBoundsTruncationTest, MaxUntruncatedWhenNoColumnHasSuccessor) {
    const VariantTuple t = then_max_bigints(boolean(true));
    EXPECT_EQ(encode(t), stored(t, SortKeyBoundSide::kMax));
    // The minimum still drops the column that does not fit.
    const std::string min_bytes = stored(t, SortKeyBoundSide::kMin);
    EXPECT_EQ(std::string("\x02\x00\x06\x00\x05\x00", 6), min_bytes.substr(0, 6));
}

// VARCHAR "a"x31 + "日本語" (40 bytes): min = "a"x31+"日" (cut on a code point, flag 1);
// max = "a"x31+"旦" (U+65E5 -> U+65E6). Both decoded are valid UTF-8.
TEST(SortKeyBoundsTruncationTest, StringCutOnCodePointBoundary) {
    const std::string prefix(31, 'a');
    const std::string value = prefix + utf8(0x65E5) + utf8(0x672C) + utf8(0x8A9E);
    ASSERT_EQ(40u, value.size());
    const VariantTuple t = tuple_of({varchar(value)});

    const std::string min_bytes = stored(t, SortKeyBoundSide::kMin);
    EXPECT_EQ(std::string("\x02\x00\x01\x00\x01\x01", 6), min_bytes.substr(0, 6));
    const VariantTuple min = decode(min_bytes);
    ASSERT_EQ(1u, min.size());
    EXPECT_EQ(prefix + utf8(0x65E5), string_of(min[0]));
    EXPECT_LT(min.compare(t), 0);

    const std::string max_bytes = stored(t, SortKeyBoundSide::kMax);
    EXPECT_EQ(std::string("\x02\x00\x01\x00\x01\x01", 6), max_bytes.substr(0, 6));
    const VariantTuple max = decode(max_bytes);
    ASSERT_EQ(1u, max.size());
    EXPECT_EQ(prefix + utf8(0x65E6), string_of(max[0]));
    EXPECT_GT(max.compare(t), 0);

    for (const VariantTuple* bound : {&min, &max}) {
        const std::string s = string_of((*bound)[0]);
        EXPECT_TRUE(validate_utf8(s.data(), s.size()));
    }
}

// "x"x33 + U+D7FF + "日": max successor ends in U+E000 (skips surrogates).
// "x"x32 + U+10FFFF + "日": max successor drops U+10FFFF and yields "x"x31 + "y".
TEST(SortKeyBoundsTruncationTest, CodePointSuccessorSkipsSurrogatesAndCarries) {
    const VariantTuple below_surrogates = tuple_of({varchar(std::string(33, 'x') + utf8(0xD7FF) + utf8(0x65E5))});
    VariantTuple max = decode(stored(below_surrogates, SortKeyBoundSide::kMax));
    ASSERT_EQ(1u, max.size());
    EXPECT_EQ(std::string(33, 'x') + utf8(0xE000), string_of(max[0]));
    EXPECT_GT(max.compare(below_surrogates), 0);

    const VariantTuple last_code_point = tuple_of({varchar(std::string(32, 'x') + utf8(0x10FFFF) + utf8(0x65E5))});
    max = decode(stored(last_code_point, SortKeyBoundSide::kMax));
    ASSERT_EQ(1u, max.size());
    EXPECT_EQ(std::string(31, 'x') + "y", string_of(max[0]));
    EXPECT_GT(max.compare(last_code_point), 0);
}

// VARCHAR of 40 x 0xC3: no 0xC3 is followed by a continuation byte, so each is a unit of one byte: cut at 36,
// max = 0xC3 x35 + 0xC4.
// VARBINARY of 40 x 0xFF: min = 36 x 0xFF (v2); 0xFF has no successor and there is no earlier column -> max is v1.
TEST(SortKeyBoundsTruncationTest, InvalidBytesAreSingleByteUnits) {
    const VariantTuple c3 = tuple_of({varchar(std::string(40, '\xC3'))});
    VariantTuple min = decode(stored(c3, SortKeyBoundSide::kMin));
    ASSERT_EQ(1u, min.size());
    EXPECT_EQ(std::string(36, '\xC3'), string_of(min[0]));
    VariantTuple max = decode(stored(c3, SortKeyBoundSide::kMax));
    ASSERT_EQ(1u, max.size());
    EXPECT_EQ(std::string(35, '\xC3') + "\xC4", string_of(max[0]));

    const VariantTuple ff = tuple_of({varbinary(std::string(40, '\xFF'))});
    const std::string min_bytes = stored(ff, SortKeyBoundSide::kMin);
    EXPECT_EQ(2, version_of(min_bytes));
    min = decode(min_bytes);
    ASSERT_EQ(1u, min.size());
    EXPECT_EQ(std::string(36, '\xFF'), string_of(min[0]));
    EXPECT_EQ(encode(ff), stored(ff, SortKeyBoundSide::kMax));
}

// VARBINARY "a"x34 + U+07FF + "b": its code points are units, as in a VARCHAR; the cut ends on a code point. U+0800
// would need a longer encoding than U+07FF, so the max successor carries to the code point before it: "a"x33 + "b"
// (never the invalid DF C0). Every decoded bound value is valid UTF-8.
TEST(SortKeyBoundsTruncationTest, VarbinaryCodePointsAreUnits) {
    const std::string prefix(34, 'a');
    const VariantTuple t = tuple_of({varbinary(prefix + utf8(0x7FF) + "b")});
    const VariantTuple min = decode(stored(t, SortKeyBoundSide::kMin));
    const VariantTuple max = decode(stored(t, SortKeyBoundSide::kMax));
    ASSERT_EQ(1u, min.size());
    ASSERT_EQ(1u, max.size());
    EXPECT_EQ(prefix + utf8(0x7FF), string_of(min[0]));
    EXPECT_EQ(std::string(33, 'a') + "b", string_of(max[0]));
    EXPECT_GT(max.compare(t), 0);
    for (const VariantTuple* bound : {&min, &max}) {
        const std::string s = string_of((*bound)[0]);
        EXPECT_TRUE(validate_utf8(s.data(), s.size()));
    }
}

namespace {

// `value` alone as a CHAR, a VARCHAR and a VARBINARY bound: both sides are cut (format version 2, flag bit 0 set),
// and the decoded minimum and maximum hold `expected_min` and `expected_max`.
void expect_cut_bounds(const std::string& value, const std::string& expected_min, const std::string& expected_max) {
    for (LogicalType type : {TYPE_CHAR, TYPE_VARCHAR, TYPE_VARBINARY}) {
        SCOPED_TRACE(type_to_string(type));
        const VariantTuple t = tuple_of({scalar(type, Datum(Slice(value)))});
        const std::string min_bytes = stored(t, SortKeyBoundSide::kMin);
        const std::string max_bytes = stored(t, SortKeyBoundSide::kMax);
        // header: version 2, arity 1, count 1, flag bit 0 set
        EXPECT_EQ(std::string("\x02\x00\x01\x00\x01\x01", 6), min_bytes.substr(0, 6));
        EXPECT_EQ(std::string("\x02\x00\x01\x00\x01\x01", 6), max_bytes.substr(0, 6));
        const VariantTuple min = decode(min_bytes);
        const VariantTuple max = decode(max_bytes);
        ASSERT_EQ(1u, min.size());
        ASSERT_EQ(1u, max.size());
        EXPECT_EQ(expected_min, string_of(min[0]));
        EXPECT_EQ(expected_max, string_of(max[0]));
        EXPECT_LT(min.compare(t), 0);
        EXPECT_GT(max.compare(t), 0);
    }
}

} // namespace

// "p"x35 + "é" (C3 A9) + FF is not valid UTF-8, yet its "é" is a valid code point and is never split: "é" does not
// fit, so min = "p"x35 (no dangling C3) and max = "p"x34 + "q". Both are valid UTF-8, so a reader that decodes them
// as UTF-8 gets the same bytes.
TEST(SortKeyBoundsTruncationTest, CutNeverSplitsAValidCodePoint) {
    const std::string min = std::string(35, 'p');
    const std::string max = std::string(34, 'p') + "q";
    expect_cut_bounds(std::string(35, 'p') + "\xC3\xA9\xFF", min, max);
    EXPECT_TRUE(validate_utf8(min.data(), min.size()));
    EXPECT_TRUE(validate_utf8(max.data(), max.size()));
}

// An invalid byte inside the kept prefix stays as it is. FF + "a"x34 + "日" (E6 97 A5) + "z"x5: "日" does not fit,
// so min = FF + "a"x34 and max = FF + "a"x33 + "b".
TEST(SortKeyBoundsTruncationTest, InvalidBytesInsideTheCutAreKept) {
    expect_cut_bounds("\xFF" + std::string(34, 'a') + utf8(0x65E5) + std::string(5, 'z'), "\xFF" + std::string(34, 'a'),
                      "\xFF" + std::string(33, 'a') + "b");
}

// "a"x35 + 80 + "z"x5: the stray continuation byte 80 is a unit of one byte, so min = "a"x35 + 80 and its successor
// increments that byte: max = "a"x35 + 81.
TEST(SortKeyBoundsTruncationTest, TrailingInvalidByteGetsAByteSuccessor) {
    expect_cut_bounds(std::string(35, 'a') + "\x80" + std::string(5, 'z'), std::string(35, 'a') + "\x80",
                      std::string(35, 'a') + "\x81");
}

// Each byte of a sequence that strict validation rejects is a unit of its own: C0 80 (overlong), ED A0 80 (an encoded
// surrogate), F4 90 80 80 (above U+10FFFF), F5 80 80 80 (invalid lead byte). For "a"x(36 - k) + sequence + "z"x5,
// the budget ends after the first k bytes of the sequence: min keeps them and max increments the last of them.
TEST(SortKeyBoundsTruncationTest, MalformedSequencesAreSingleByteUnits) {
    struct Case {
        std::string sequence;
        std::string min_tail;
        std::string max_tail;
    };
    const std::vector<Case> cases = {
            {"\xC0\x80", "\xC0", "\xC1"},
            {"\xED\xA0\x80", "\xED", "\xEE"},
            {"\xED\xA0\x80", "\xED\xA0", "\xED\xA1"},
            {"\xF4\x90\x80\x80", "\xF4", "\xF5"},
            {"\xF4\x90\x80\x80", "\xF4\x90", "\xF4\x91"},
            {"\xF4\x90\x80\x80", "\xF4\x90\x80", "\xF4\x90\x81"},
            {"\xF5\x80\x80\x80", "\xF5", "\xF6"},
            {"\xF5\x80\x80\x80", "\xF5\x80", "\xF5\x81"},
            {"\xF5\x80\x80\x80", "\xF5\x80\x80", "\xF5\x80\x81"},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE(testing::PrintToString(c.sequence) + " cut to " + testing::PrintToString(c.min_tail));
        ASSERT_FALSE(validate_utf8(c.sequence.data(), c.sequence.size()));
        const std::string prefix(kSortKeyBoundPayloadBudget - c.min_tail.size(), 'a');
        expect_cut_bounds(prefix + c.sequence + std::string(5, 'z'), prefix + c.min_tail, prefix + c.max_tail);
    }
}

// Width transitions: incrementing U+007F, U+07FF or U+FFFF would need a longer encoding (1->2, 2->3, 3->4 bytes),
// so the successor drops that code point and increments the one before it. A successor is never longer than the
// string it increments, so the stored payload stays <= 36; the decoded max is > the original value.
TEST(SortKeyBoundsTruncationTest, CodePointSuccessorWidthTransitions) {
    for (uint32_t cp : {0x7Fu, 0x7FFu, 0xFFFFu}) {
        SCOPED_TRACE(cp);
        // A cut string: the cut keeps exactly the prefix and `cp`, filling the budget.
        const std::string prefix(kSortKeyBoundPayloadBudget - utf8(cp).size(), 'a');
        const VariantTuple cut = tuple_of({varchar(prefix + utf8(cp) + "zz")});
        VariantTuple max = decode(stored(cut, SortKeyBoundSide::kMax));
        ASSERT_EQ(1u, max.size());
        std::string s = string_of(max[0]);
        EXPECT_EQ(prefix.substr(1) + "b", s);
        EXPECT_LE(s.size(), (prefix + utf8(cp)).size());
        EXPECT_LE(payload_of(max), kSortKeyBoundPayloadBudget);
        EXPECT_GT(max.compare(cut), 0);
        EXPECT_TRUE(validate_utf8(s.data(), s.size()));

        // A whole string the max backs off to: the successor is not longer than the column value.
        const std::string whole = "a" + utf8(cp);
        const VariantTuple backed_off = then_max_bigints(varchar(whole));
        max = decode(stored(backed_off, SortKeyBoundSide::kMax));
        ASSERT_EQ(1u, max.size());
        s = string_of(max[0]);
        EXPECT_EQ("b", s);
        EXPECT_LE(s.size(), whole.size());
        EXPECT_LE(payload_of(max), kSortKeyBoundPayloadBudget);
        EXPECT_GT(max.compare(backed_off), 0);
    }
}

// DATETIME successor uses calendar arithmetic: 2026-11-18 23:59:59.999999 -> 2026-11-19 00:00:00;
// 2026-12-31 23:59:59.999999 -> 2027-01-01 00:00:00; 2024-02-28 23:59:59.999999 -> 2024-02-29 00:00:00;
// DATE 2024-02-28 -> 2024-02-29, 2026-12-31 -> 2027-01-01. Each decoded successor round-trips through
// DatumVariant::to_proto/from_proto unchanged.
TEST(SortKeyBoundsTruncationTest, DateTimeSuccessorRollsOver) {
    const std::vector<std::pair<DatumVariant, DatumVariant>> cases = {
            {datetime_of(TimestampValue::create(2026, 11, 18, 23, 59, 59, 999999)),
             datetime_of(TimestampValue::create(2026, 11, 19, 0, 0, 0, 0))},
            {datetime_of(TimestampValue::create(2026, 12, 31, 23, 59, 59, 999999)),
             datetime_of(TimestampValue::create(2027, 1, 1, 0, 0, 0, 0))},
            {datetime_of(TimestampValue::create(2024, 2, 28, 23, 59, 59, 999999)),
             datetime_of(TimestampValue::create(2024, 2, 29, 0, 0, 0, 0))},
            {date_of(DateValue::create(2024, 2, 28)), date_of(DateValue::create(2024, 2, 29))},
            {date_of(DateValue::create(2026, 12, 31)), date_of(DateValue::create(2027, 1, 1))},
    };
    for (const auto& [value, expected] : cases) {
        SCOPED_TRACE(datum_to_string(value.type().get(), value.value()));
        const VariantTuple max = decode(stored(then_max_bigints(value), SortKeyBoundSide::kMax));
        ASSERT_EQ(1u, max.size());
        expect_same_tuple(tuple_of({expected}), max);

        VariantPB pb;
        max[0].to_proto(&pb);
        DatumVariant round_tripped;
        ASSERT_OK(round_tripped.from_proto(pb));
        expect_same_tuple(max, tuple_of({round_tripped}));
    }
}

// CHAR "abc\0" + 40 x 'z': the value is normalized to "abc" first, which fits -> no cut of that column.
TEST(SortKeyBoundsTruncationTest, CharIsNormalizedBeforeTruncation) {
    // 3 + 4 * 8 = 35 bytes after normalization; 76 before it.
    const VariantTuple t =
            tuple_of({chr(std::string("abc\0", 4) + std::string(40, 'z')), bigint(1), bigint(2), bigint(3), bigint(4)});
    for (auto side : {SortKeyBoundSide::kMin, SortKeyBoundSide::kMax}) {
        const std::string bytes = stored(t, side);
        EXPECT_EQ(encode(t), bytes);
        const VariantTuple decoded = decode(bytes);
        ASSERT_EQ(5u, decoded.size());
        EXPECT_EQ("abc", string_of(decoded[0]));
    }
}

// Column 0 = <type value>, then BIGINT max x5 to force truncation and back off to column 0.
// Expected decoded max == (next(value)):
//   BOOLEAN false->true; TINYINT 5->6; SMALLINT 5->6; INT 5->6; BIGINT 5->6; LARGEINT 5->6;
//   DATE 2026-11-18 -> 2026-11-19; DATETIME 2026-11-18 10:00:00 -> 2026-11-18 10:00:00.000001;
//   DECIMAL32(9,2) 12.34 -> 12.35; DECIMAL64(18,2) -1.50 -> -1.49; DECIMAL128(38,4) 1.0000 -> 1.0001;
//   DECIMAL256(76,0) 7 -> 8; DECIMALV2 1.000000000 -> 1.000000001;
//   CHAR/VARCHAR "abc" -> "abd"; VARBINARY "ab\xFF" -> "ac".
TEST(SortKeyBoundsTruncationTest, FixedWidthSuccessorPerType) {
    auto smallint = [](int16_t v) { return scalar(TYPE_SMALLINT, Datum(v)); };
    auto largeint = [](int128_t v) { return scalar(TYPE_LARGEINT, Datum(v)); };
    auto decv2 = [](const std::string& v) { return scalar(TYPE_DECIMALV2, Datum(DecimalV2Value(v))); };
    const std::vector<std::pair<DatumVariant, DatumVariant>> cases = {
            {boolean(false), boolean(true)},
            {tinyint(5), tinyint(6)},
            {smallint(5), smallint(6)},
            {int_(5), int_(6)},
            {bigint(5), bigint(6)},
            {largeint(5), largeint(6)},
            {date_of(DateValue::create(2026, 11, 18)), date_of(DateValue::create(2026, 11, 19))},
            {datetime_of(TimestampValue::create(2026, 11, 18, 10, 0, 0, 0)),
             datetime_of(TimestampValue::create(2026, 11, 18, 10, 0, 0, 1))},
            {decimal(TYPE_DECIMAL32, 9, 2, Datum(static_cast<int32_t>(1234))),
             decimal(TYPE_DECIMAL32, 9, 2, Datum(static_cast<int32_t>(1235)))},
            {decimal(TYPE_DECIMAL64, 18, 2, Datum(static_cast<int64_t>(-150))),
             decimal(TYPE_DECIMAL64, 18, 2, Datum(static_cast<int64_t>(-149)))},
            {decimal(TYPE_DECIMAL128, 38, 4, Datum(int128_t{10000})),
             decimal(TYPE_DECIMAL128, 38, 4, Datum(int128_t{10001}))},
            {decimal(TYPE_DECIMAL256, 76, 0, Datum(int256_t(7))), decimal(TYPE_DECIMAL256, 76, 0, Datum(int256_t(8)))},
            {decv2("1.000000000"), decv2("1.000000001")},
            {chr("abc"), chr("abd")},
            {varchar("abc"), varchar("abd")},
            {varbinary("ab\xFF"), varbinary("ac")},
    };
    for (const auto& [value, expected] : cases) {
        SCOPED_TRACE(type_to_string(value.type()->type()));
        const std::string bytes = stored(then_max_bigints(value), SortKeyBoundSide::kMax);
        EXPECT_EQ(2, version_of(bytes));
        EXPECT_EQ(1, flags_of(bytes));
        expect_same_tuple(tuple_of({expected}), decode(bytes));
    }
}

// At the maximum (BOOLEAN true, INT max, LARGEINT max, DATE 9999-12-31, DATETIME max,
// DECIMAL32(9,2) 9999999.99, DECIMALV2 max, empty string) there is no successor -> v1 untruncated.
TEST(SortKeyBoundsTruncationTest, NoSuccessorAtTypeMaximum) {
    const std::vector<DatumVariant> values = {
            boolean(true),
            int_(std::numeric_limits<int32_t>::max()),
            scalar(TYPE_LARGEINT, Datum(std::numeric_limits<int128_t>::max())),
            date_of(DateValue::create(9999, 12, 31)),
            datetime_of(TimestampValue::MAX_TIMESTAMP_VALUE),
            decimal(TYPE_DECIMAL32, 9, 2, Datum(static_cast<int32_t>(999999999))),
            scalar(TYPE_DECIMALV2, Datum(DecimalV2Value::get_max_decimal())),
            varchar(""),
    };
    for (const DatumVariant& value : values) {
        SCOPED_TRACE(type_to_string(value.type()->type()));
        const VariantTuple t = then_max_bigints(value);
        EXPECT_EQ(encode(t), stored(t, SortKeyBoundSide::kMax));
    }
}

// A decimal stops at the maximum of its precision, not of its storage type: DECIMAL64(10,2) 99999999.99 has no
// successor although int64 has room above it, so a maximum backs off past it; 12.34 has 12.35.
TEST(SortKeyBoundsTruncationTest, DecimalSuccessorStopsAtItsPrecision) {
    auto dec = [](int64_t v) { return decimal(TYPE_DECIMAL64, 10, 2, Datum(v)); };

    const VariantTuple alone = then_max_bigints(dec(9999999999LL));
    EXPECT_EQ(encode(alone), stored(alone, SortKeyBoundSide::kMax));

    // (BIGINT 7, the decimal, BIGINT max x4): columns 0-3 fit, column 4 does not; columns 3, 2 and 1 have no
    // successor, so the maximum backs off to column 0 -> (8).
    const VariantTuple behind = tuple_of(
            {bigint(7), dec(9999999999LL), bigint(INT64_MAX), bigint(INT64_MAX), bigint(INT64_MAX), bigint(INT64_MAX)});
    const std::string max_bytes = stored(behind, SortKeyBoundSide::kMax);
    EXPECT_EQ(std::string("\x02\x00\x06\x00\x01\x01", 6), max_bytes.substr(0, 6));
    expect_same_tuple(tuple_of({bigint(8)}), decode(max_bytes));

    const std::string next_bytes = stored(then_max_bigints(dec(1234)), SortKeyBoundSide::kMax);
    EXPECT_EQ(2, version_of(next_bytes));
    EXPECT_EQ(1, flags_of(next_bytes));
    expect_same_tuple(tuple_of({dec(1235)}), decode(next_bytes));
}

namespace {

// A string of at most max_bytes bytes over a small alphabet, so that rows share prefixes and the cut and successor
// meet multi-byte code points, the surrogate gap and U+10FFFF. Half of the strings also mix in pieces that are not
// valid UTF-8: a stray continuation byte, lead bytes without all their continuation bytes, an encoded surrogate, 0xFF.
std::string random_string(std::mt19937* rng, size_t max_bytes) {
    static const std::vector<std::string> pieces = {"a", "b", utf8(0x7F), utf8(0xE9), utf8(0x7FF), utf8(0x65E5),
                                                    utf8(0xD7FF), utf8(0xFFFF), utf8(0x10FFFF),
                                                    // Not valid UTF-8.
                                                    "\x80", "\xC3", "\xE6\x97", "\xED\xA0\x80", "\xFF"};
    constexpr size_t kValidPieces = 9;
    const size_t choices = (*rng)() % 2 == 0 ? kValidPieces : pieces.size();
    const size_t target = std::uniform_int_distribution<size_t>(0, max_bytes)(*rng);
    std::string s;
    while (true) {
        const std::string& piece = pieces[(*rng)() % choices];
        if (s.size() + piece.size() > target) {
            return s;
        }
        s += piece;
    }
}

// True if cutting `s` to its first `len` bytes splits a valid UTF-8 code point of `s`.
bool splits_code_point(const std::string& s, size_t len) {
    for (size_t start = len > 3 ? len - 3 : 0; start < len; ++start) {
        const auto lead = static_cast<uint8_t>(s[start]);
        const size_t width = lead >= 0xF0 ? 4 : lead >= 0xE0 ? 3 : lead >= 0xC0 ? 2 : 1;
        if (start + width > len && start + width <= s.size() && validate_utf8(s.data() + start, width)) {
            return true;
        }
    }
    return false;
}

DatumVariant random_value(std::mt19937* rng, LogicalType type) {
    if ((*rng)() % 8 == 0) {
        return type == TYPE_DECIMAL64 ? decimal(type, 18, 3, Datum()) : null_of(type);
    }
    // Small values make ties likely, so later columns decide; the maximum leaves a column without a successor.
    const bool at_max = (*rng)() % 6 == 0;
    const int small = static_cast<int>((*rng)() % 7) - 3;
    switch (type) {
    case TYPE_INT:
        return int_(at_max ? std::numeric_limits<int32_t>::max() : small);
    case TYPE_BIGINT:
        return bigint(at_max ? std::numeric_limits<int64_t>::max() : small);
    case TYPE_DATE:
        return date_of(at_max ? DateValue::create(9999, 12, 31) : DateValue::create(2024, 2, 27 + small % 3));
    case TYPE_DECIMAL64:
        return decimal(type, 18, 3, Datum(at_max ? get_max_decimal<int64_t>(18) : static_cast<int64_t>(small)));
    default:
        return varchar(random_string(rng, 40));
    }
}

} // namespace

// Property, fixed seed, 1000 rounds: random sorted rows over {INT, BIGINT, VARCHAR(0..40 bytes, ASCII, multi-byte
// and, in half of the values, bytes that are not valid UTF-8), DATE, DECIMAL64(18,3), NULLs}, 1..8 columns. For
// stored min'/max' of (rows.front(), rows.back()): every row r: decode(min').compare(r) <= 0 and
// decode(max').compare(r) >= 0, and > 0 when max' is v2. A string cut in min' is a prefix of the row's value that
// splits none of its valid code points, and a bound value taken from a valid UTF-8 value is valid UTF-8.
TEST(SortKeyBoundsTruncationTest, TruncatedBoundsAreConservative) {
    const std::vector<LogicalType> types = {TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR, TYPE_DATE, TYPE_DECIMAL64};
    std::mt19937 rng(20261118);
    int truncated_mins = 0;
    int truncated_maxes = 0;
    int invalid_utf8_cuts = 0;
    for (int round = 0; round < 1000; ++round) {
        SCOPED_TRACE(round);
        std::vector<LogicalType> columns(1 + rng() % 8);
        for (LogicalType& type : columns) {
            type = types[rng() % types.size()];
        }
        std::vector<VariantTuple> rows(1 + rng() % 16);
        for (VariantTuple& row : rows) {
            for (LogicalType type : columns) {
                row.append(random_value(&rng, type));
            }
        }
        std::sort(rows.begin(), rows.end());

        const std::string min_bytes = stored(rows.front(), SortKeyBoundSide::kMin);
        const std::string max_bytes = stored(rows.back(), SortKeyBoundSide::kMax);
        const bool max_truncated = version_of(max_bytes) == 2;
        truncated_mins += version_of(min_bytes) == 2;
        truncated_maxes += max_truncated;
        const VariantTuple min = decode(min_bytes);
        const VariantTuple max = decode(max_bytes);
        if (version_of(min_bytes) == 2) {
            ASSERT_LE(payload_of(min), kSortKeyBoundPayloadBudget);
        }
        if (max_truncated) {
            ASSERT_LE(payload_of(max), kSortKeyBoundPayloadBudget);
        }
        for (const VariantTuple& row : rows) {
            ASSERT_LE(min.compare(row), 0);
            if (max_truncated) {
                ASSERT_GT(max.compare(row), 0);
            } else {
                ASSERT_GE(max.compare(row), 0);
            }
        }
        if (version_of(min_bytes) == 2 && (flags_of(min_bytes) & 1) != 0) {
            const std::string value = string_of(rows.front()[min.size() - 1]);
            const std::string cut = string_of(min[min.size() - 1]);
            ASSERT_EQ(cut, value.substr(0, cut.size()));
            ASSERT_FALSE(splits_code_point(value, cut.size()));
            invalid_utf8_cuts += !validate_utf8(value.data(), value.size());
        }
        for (const auto& [bound, row] : {std::pair{&min, &rows.front()}, std::pair{&max, &rows.back()}}) {
            for (size_t i = 0; i < bound->size(); ++i) {
                const DatumVariant& value = (*bound)[i];
                if (value.type()->type() != TYPE_VARCHAR || value.value().is_null()) {
                    continue;
                }
                const std::string source = string_of((*row)[i]);
                const std::string s = string_of(value);
                if (validate_utf8(source.data(), source.size())) {
                    ASSERT_TRUE(validate_utf8(s.data(), s.size()));
                }
            }
        }
    }
    // The rounds exercise truncation on both sides, not only bounds that fit, and cut values that are not valid UTF-8.
    EXPECT_GT(truncated_mins, 100);
    EXPECT_GT(truncated_maxes, 100);
    EXPECT_GT(invalid_utf8_cuts, 50);
}

namespace {

// Five BIGINT columns: both bounds are stored in format version 2. Header bytes: version 0, arity 1-2, count 3-4,
// flags 5, then the four BIGINT type bytes 6-9; column 0's value is bytes 10-18.
SegmentMetadataPB truncated_bigint_bounds_meta() {
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(tuple_of({bigint(1), bigint(2), bigint(3), bigint(4), bigint(5)}),
                                tuple_of({bigint(11), bigint(12), bigint(13), bigint(14), bigint(15)}), &meta);
    EXPECT_EQ(2, version_of(meta.sort_key_min_encoded()));
    EXPECT_EQ(2, version_of(meta.sort_key_max_encoded()));
    return meta;
}

// One VARCHAR column of 40 bytes: both bounds are a cut string or its successor (count 1, arity 1, flag 1). The
// value's terminator is the last two bytes.
SegmentMetadataPB truncated_varchar_bounds_meta() {
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(tuple_of({varchar(std::string(40, 'a'))}), tuple_of({varchar(std::string(40, 'b'))}),
                                &meta);
    EXPECT_EQ(2, version_of(meta.sort_key_min_encoded()));
    EXPECT_EQ(2, version_of(meta.sort_key_max_encoded()));
    return meta;
}

BoundDamage set_bytes(std::vector<std::pair<size_t, uint8_t>> changes) {
    return [=](const std::string& bytes) {
        std::string damaged = bytes;
        for (const auto& [pos, value] : changes) {
            damaged[pos] = static_cast<char>(value);
        }
        return damaged;
    };
}

} // namespace

// v2 header: count > arity, count == arity with flags 0, flags 0x02, count 0, arity 0, every fixed-header
// length 1..5, a truncated payload, an invalid escape -> Corruption from decode_sort_key_bound and both segment
// decoders, outputs unchanged on failure.
TEST(SortKeyBoundsTruncationTest, DecodeRejectsMalformedV2Header) {
    struct V2Damage {
        std::string label;
        SegmentMetadataPB valid;
        BoundDamage damage;
    };
    const SegmentMetadataPB bigints = truncated_bigint_bounds_meta();
    const SegmentMetadataPB varchars = truncated_varchar_bounds_meta();
    std::vector<V2Damage> damages = {
            {"count above arity", bigints, set_bytes({{3, 0x00}, {4, 0x06}})},
            {"count equal to arity with flags 0", bigints, set_bytes({{1, 0x00}, {2, 0x04}, {5, 0x00}})},
            {"flags 0x02", bigints, set_byte(5, 0x02)},
            {"flags 0x03", bigints, set_byte(5, 0x03)},
            {"count 0", bigints, set_bytes({{3, 0x00}, {4, 0x00}})},
            {"arity 0", bigints, set_bytes({{1, 0x00}, {2, 0x00}})},
            {"payload of column 0 truncated", bigints, truncate_to(14)},
            {"invalid string escape", varchars,
             [](const std::string& bytes) {
                 std::string damaged = bytes;
                 damaged.back() = static_cast<char>(0x02);
                 return damaged;
             }},
    };
    for (size_t size = 1; size <= 5; ++size) {
        damages.push_back({"fixed header of " + std::to_string(size) + " bytes", bigints, truncate_to(size)});
    }
    for (const auto& [label, valid, damage] : damages) {
        for (const std::string* bytes : {&valid.sort_key_min_encoded(), &valid.sort_key_max_encoded()}) {
            SCOPED_TRACE(label);
            VariantTuple out = tuple_of({bigint(42)});
            Status st = decode_sort_key_bound(Slice(damage(*bytes)), &out);
            EXPECT_TRUE(st.is_corruption()) << st;
            ASSERT_EQ(1u, out.size());
            EXPECT_EQ(42, out[0].value().get_int64());
        }
        for (const auto& [side, meta] : damage_each_side(valid, damage)) {
            SCOPED_TRACE(label + " in " + side);
            EXPECT_TRUE(has_segment_sort_key_bounds(meta));
            VariantTuple got_min = tuple_of({bigint(42)}), got_max = tuple_of({bigint(43)});
            SortKeyBoundsExactness exactness{true, true, 7, 8};
            Status st = decode_segment_sort_key_bounds(meta, &got_min, &got_max, &exactness);
            EXPECT_TRUE(st.is_corruption()) << st;
            EXPECT_EQ(42, got_min[0].value().get_int64());
            EXPECT_EQ(43, got_max[0].value().get_int64());
            EXPECT_TRUE(exactness.min_truncated);
            EXPECT_TRUE(exactness.max_truncated);
            EXPECT_EQ(7u, exactness.min_exact_columns);
            EXPECT_EQ(8u, exactness.max_exact_columns);
            DatumVariant lo = bigint(42), hi = bigint(43);
            st = decode_segment_leading_sort_key_bounds(meta, &lo, &hi);
            EXPECT_TRUE(st.is_corruption()) << st;
            EXPECT_EQ(42, lo.value().get_int64());
            EXPECT_EQ(43, hi.value().get_int64());
        }
    }
}

// A v2 MAX with flag bit 0 clear (bare prefix, which the writer never produces) -> Corruption from the segment
// decoders; the same bytes as a MIN decode fine.
TEST(SegmentSortKeyBoundsTest, BarePrefixMaxIsCorruption) {
    const VariantTuple key = tuple_of({bigint(1), bigint(2), bigint(3), bigint(4), bigint(5)});
    const std::string bare_prefix = stored(key, SortKeyBoundSide::kMin);
    ASSERT_EQ(std::string("\x02\x00\x05\x00\x04\x00", 6), bare_prefix.substr(0, 6));
    // The codec itself does not know the side.
    EXPECT_EQ(4u, decode(bare_prefix).size());

    SegmentMetadataPB as_max;
    as_max.set_sort_key_min_encoded(bare_prefix);
    as_max.set_sort_key_max_encoded(bare_prefix);
    VariantTuple got_min, got_max;
    EXPECT_TRUE(decode_segment_sort_key_bounds(as_max, &got_min, &got_max).is_corruption());
    DatumVariant lo, hi;
    EXPECT_TRUE(decode_segment_leading_sort_key_bounds(as_max, &lo, &hi).is_corruption());

    SegmentMetadataPB as_min;
    as_min.set_sort_key_min_encoded(bare_prefix);
    as_min.set_sort_key_max_encoded(stored(key, SortKeyBoundSide::kMax));
    ASSERT_OK(decode_segment_sort_key_bounds(as_min, &got_min, &got_max));
    EXPECT_EQ(0, got_min.compare(tuple_of({bigint(1), bigint(2), bigint(3), bigint(4)})));
    EXPECT_EQ(0, got_max.compare(tuple_of({bigint(1), bigint(2), bigint(3), bigint(5)})));
    ASSERT_OK(decode_segment_leading_sort_key_bounds(as_min, &lo, &hi));
    EXPECT_EQ(1, lo.value().get_int64());
    EXPECT_EQ(1, hi.value().get_int64());
}

// A v2 MAX with flag bit 0 set whose last value is NULL (which the writer never produces: NULL has no successor) is
// below the keys it claims to bound -> Corruption from every segment decoder that reads that value; the same bytes as
// a MIN decode fine.
TEST(SegmentSortKeyBoundsTest, NullSuccessorMaxIsCorruption) {
    const SegmentMetadataPB valid = truncated_bigint_bounds_meta();
    // The max is (11, 12, 13, 15): 6 header bytes and 4 type bytes, then four 9-byte values. Keep the first three
    // and make the last one NULL.
    const std::string null_last = valid.sort_key_max_encoded().substr(0, 6 + 4 + 3 * 9) + std::string(1, '\x00');
    // Version 2, arity 5, count 1, flags 1, BIGINT, NULL.
    const std::string null_only = bytes_of({0x02, 0x00, 0x05, 0x00, 0x01, 0x01, TYPE_BIGINT, 0x00});
    for (const std::string& bytes : {null_last, null_only}) {
        // The codec itself does not know the side.
        const VariantTuple decoded = decode(bytes);
        ASSERT_FALSE(decoded.empty());
        EXPECT_TRUE(decoded[decoded.size() - 1].value().is_null());

        SegmentMetadataPB as_max = valid;
        as_max.set_sort_key_max_encoded(bytes);
        VariantTuple got_min, got_max;
        EXPECT_TRUE(decode_segment_sort_key_bounds(as_max, &got_min, &got_max).is_corruption());

        SegmentMetadataPB as_min = valid;
        as_min.set_sort_key_min_encoded(bytes);
        ASSERT_OK(decode_segment_sort_key_bounds(as_min, &got_min, &got_max));
        EXPECT_EQ(0, got_min.compare(decoded));
    }

    // The leading decode reads the last value only when it is the first: with count 1 the NULL is the leading
    // maximum, which would let pruning skip the segment. With count 4 the leading value is an exact key column.
    DatumVariant lo, hi;
    SegmentMetadataPB null_only_max = valid;
    null_only_max.set_sort_key_max_encoded(null_only);
    EXPECT_TRUE(decode_segment_leading_sort_key_bounds(null_only_max, &lo, &hi).is_corruption());
    SegmentMetadataPB null_last_max = valid;
    null_last_max.set_sort_key_max_encoded(null_last);
    ASSERT_OK(decode_segment_leading_sort_key_bounds(null_last_max, &lo, &hi));
    EXPECT_EQ(11, hi.value().get_int64());
}

// set_segment_sort_key_bounds with 5 BIGINT columns: have_arity(5) true, have_arity(4) false;
// exactness: min 4, max 3; leading decode returns column 0 of each side.
TEST(SegmentSortKeyBoundsTest, TruncatedPairArityExactnessAndLeadingDecode) {
    const SegmentMetadataPB meta = truncated_bigint_bounds_meta();
    EXPECT_TRUE(has_segment_sort_key_bounds(meta));
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 5));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 4));

    VariantTuple got_min, got_max;
    SortKeyBoundsExactness exactness;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max, &exactness));
    EXPECT_EQ(0, got_min.compare(tuple_of({bigint(1), bigint(2), bigint(3), bigint(4)})));
    EXPECT_EQ(0, got_max.compare(tuple_of({bigint(11), bigint(12), bigint(13), bigint(15)})));
    EXPECT_TRUE(exactness.min_truncated);
    EXPECT_TRUE(exactness.max_truncated);
    EXPECT_EQ(4u, exactness.min_exact_columns);
    EXPECT_EQ(3u, exactness.max_exact_columns);

    DatumVariant lo, hi;
    ASSERT_OK(decode_segment_leading_sort_key_bounds(meta, &lo, &hi));
    EXPECT_EQ(1, lo.value().get_int64());
    EXPECT_EQ(11, hi.value().get_int64());
}

// min short (fits, v1) and max long (v2) on one segment: both decode; exactness min = arity.
TEST(SegmentSortKeyBoundsTest, SidesTruncateIndependently) {
    const VariantTuple min = tuple_of({bigint(1), varchar("a")});
    const VariantTuple max = tuple_of({bigint(9), varchar(std::string(40, 'z'))});
    SegmentMetadataPB meta;
    set_segment_sort_key_bounds(min, max, &meta);
    EXPECT_EQ(encode(min), meta.sort_key_min_encoded());
    EXPECT_EQ(2, version_of(meta.sort_key_max_encoded()));
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));

    VariantTuple got_min, got_max;
    SortKeyBoundsExactness exactness;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max, &exactness));
    expect_same_tuple(min, got_min);
    // 8 bytes for the BIGINT leave 28 for the string: 28 'z' cut, then its successor.
    expect_same_tuple(tuple_of({bigint(9), varchar(std::string(27, 'z') + "{")}), got_max);
    EXPECT_GT(got_max.compare(max), 0);
    EXPECT_FALSE(exactness.min_truncated);
    EXPECT_TRUE(exactness.max_truncated);
    EXPECT_EQ(2u, exactness.min_exact_columns);
    EXPECT_EQ(1u, exactness.max_exact_columns);
}

// A minimum whose last value is a cut string keeps every column (count == arity), but that value is not the
// segment's own: min_exact_columns is count - 1, so the decoded size cannot tell a truncated minimum.
TEST(SegmentSortKeyBoundsTest, CutStringMinimumExactness) {
    struct Case {
        std::string label;
        VariantTuple min;
        VariantTuple expected_min;
    };
    const std::vector<Case> cases = {
            {"VARCHAR", tuple_of({varchar(std::string(40, 'a'))}), tuple_of({varchar(std::string(36, 'a'))})},
            // 8 bytes for the BIGINT leave 28 for the string.
            {"BIGINT, VARCHAR", tuple_of({bigint(1), varchar(std::string(40, 'a'))}),
             tuple_of({bigint(1), varchar(std::string(28, 'a'))})},
    };
    for (const auto& [label, min, expected_min] : cases) {
        SCOPED_TRACE(label);
        SegmentMetadataPB meta;
        set_segment_sort_key_bounds(min, min, &meta);
        ASSERT_EQ(2, version_of(meta.sort_key_min_encoded()));
        EXPECT_EQ(1, flags_of(meta.sort_key_min_encoded()));
        EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, min.size()));

        VariantTuple got_min, got_max;
        SortKeyBoundsExactness exactness;
        ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max, &exactness));
        expect_same_tuple(expected_min, got_min);
        EXPECT_EQ(min.size(), got_min.size());
        EXPECT_TRUE(exactness.min_truncated);
        EXPECT_EQ(got_min.size() - 1, exactness.min_exact_columns);
    }
}

// Deprecated TuplePB pair + v2 encoded pair: deprecated pair wins (existing precedence).
TEST(SegmentSortKeyBoundsTest, DeprecatedPairWinsOverTruncatedEncoded) {
    SegmentMetadataPB meta = truncated_bigint_bounds_meta();
    set_deprecated_pair(tuple_of({bigint(1), bigint(1)}), tuple_of({bigint(2), bigint(2)}), &meta);
    EXPECT_TRUE(segment_sort_key_bounds_have_arity(meta, 2));
    EXPECT_FALSE(segment_sort_key_bounds_have_arity(meta, 5));

    VariantTuple got_min, got_max;
    SortKeyBoundsExactness exactness;
    ASSERT_OK(decode_segment_sort_key_bounds(meta, &got_min, &got_max, &exactness));
    EXPECT_EQ(0, got_min.compare(tuple_of({bigint(1), bigint(1)})));
    EXPECT_EQ(0, got_max.compare(tuple_of({bigint(2), bigint(2)})));
    EXPECT_FALSE(exactness.min_truncated);
    EXPECT_FALSE(exactness.max_truncated);
    EXPECT_EQ(2u, exactness.min_exact_columns);
    EXPECT_EQ(2u, exactness.max_exact_columns);

    DatumVariant lo, hi;
    ASSERT_OK(decode_segment_leading_sort_key_bounds(meta, &lo, &hi));
    EXPECT_EQ(1, lo.value().get_int64());
    EXPECT_EQ(2, hi.value().get_int64());
}

} // namespace starrocks
