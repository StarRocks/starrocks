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
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/variant_tuple.h"
#include "types/date_value.h"
#include "types/decimalv2_value.h"
#include "types/logical_type.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"
#include "types/type_info.h"

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
            {"version 2", bytes_of({0x02, 0x00, 0x01, TYPE_BIGINT, 0x00})},
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
            {"version 2", set_byte(0, 0x02)},
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
    for (const auto& [pos, value] : std::vector<std::pair<size_t, uint8_t>>{{0, 0x02}, {3, 0xFF}}) {
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

} // namespace starrocks
