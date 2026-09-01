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

#include "geo/wkb.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace starrocks {
namespace {

std::string from_hex(const std::string& hex) {
    auto value = [](char digit) -> uint8_t {
        if (digit >= '0' && digit <= '9') {
            return digit - '0';
        }
        if (digit >= 'a' && digit <= 'f') {
            return digit - 'a' + 10;
        }
        return digit - 'A' + 10;
    };

    EXPECT_EQ(0, hex.size() % 2);
    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        bytes.push_back(static_cast<char>((value(hex[i]) << 4) | value(hex[i + 1])));
    }
    return bytes;
}

TEST(WkbCodecTest, RoundTripsAllSupportedGeometryTypes) {
    const std::vector<std::pair<std::string, std::string>> cases = {
            {"point (30.1 10.25)", "POINT (30.1 10.25)"},
            {"LINESTRING(30 10,10 30,40 40)", "LINESTRING (30 10, 10 30, 40 40)"},
            {"POLYGON((30 10,40 40,20 40,10 20,30 10),(20 20,25 20,25 25,20 20))",
             "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10), (20 20, 25 20, 25 25, 20 20))"},
            {"MULTIPOINT(10 40,(40 30),20 20,30 10)", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"},
            {"MULTILINESTRING((10 10,20 20,10 40),(40 40,30 30,40 20,30 10))",
             "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"},
            {"MULTIPOLYGON(((30 20,45 40,10 40,30 20)))", "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))"},
            {"GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))",
             "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))"},
    };

    for (const auto& [input, expected] : cases) {
        WkbGeometry geometry;
        Status status = WkbCodec::parse_wkt(input, &geometry);
        ASSERT_TRUE(status.ok()) << input << ": " << status.to_string();

        std::string wkb;
        status = WkbCodec::to_wkb(geometry, &wkb);
        ASSERT_TRUE(status.ok()) << status.to_string();

        WkbGeometry decoded;
        status = WkbCodec::parse_wkb(Slice(wkb), &decoded);
        ASSERT_TRUE(status.ok()) << status.to_string();

        std::string normalized;
        status = WkbCodec::to_wkt(decoded, &normalized);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(expected, normalized);
    }
}

TEST(WkbCodecTest, RoundTripsEmptyGeometries) {
    const std::vector<std::string> cases = {
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
    };

    for (const auto& input : cases) {
        WkbGeometry geometry;
        Status status = WkbCodec::parse_wkt(input, &geometry);
        ASSERT_TRUE(status.ok()) << input << ": " << status.to_string();
        EXPECT_TRUE(geometry.empty);

        std::string wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(geometry, &wkb).ok());

        WkbGeometry decoded;
        ASSERT_TRUE(WkbCodec::parse_wkb(Slice(wkb), &decoded).ok());
        std::string normalized;
        ASSERT_TRUE(WkbCodec::to_wkt(decoded, &normalized).ok());
        EXPECT_EQ(input, normalized);
    }
}

TEST(WkbCodecTest, AcceptsBothByteOrdersAndWritesCanonicalLittleEndian) {
    const std::string little_endian = from_hex("0101000000000000000000f03f0000000000000040");
    const std::string big_endian = from_hex("00000000013ff00000000000004000000000000000");

    for (const auto& input : {little_endian, big_endian}) {
        WkbGeometry geometry;
        Status status = WkbCodec::parse_wkb(Slice(input), &geometry);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(WkbGeometryType::POINT, geometry.type);
        ASSERT_EQ(1, geometry.coordinates.size());
        EXPECT_DOUBLE_EQ(1.0, geometry.coordinates[0].x);
        EXPECT_DOUBLE_EQ(2.0, geometry.coordinates[0].y);

        std::string canonical;
        ASSERT_TRUE(WkbCodec::to_wkb(geometry, &canonical).ok());
        EXPECT_EQ(little_endian, canonical);
    }
}

TEST(WkbCodecTest, SupportsEmptyChildrenInMultiGeometries) {
    WkbGeometry geometry;
    Status status = WkbCodec::parse_wkt("MULTIPOINT (EMPTY, (1 2))", &geometry);
    ASSERT_TRUE(status.ok()) << status.to_string();

    std::string normalized;
    ASSERT_TRUE(WkbCodec::to_wkt(geometry, &normalized).ok());
    EXPECT_EQ("MULTIPOINT (EMPTY, (1 2))", normalized);
}

TEST(WkbCodecTest, RejectsMalformedWkt) {
    const std::vector<std::string> cases = {
            "",                                   // no type
            "POINT (1-2)",                        // ordinates need whitespace
            "POINT Z (1 2 3)",                    // dimensional coordinates are not in phase one
            "POINT (NaN 2)",                      // non-finite coordinate
            "POINT (0x1p1 2)",                    // hexadecimal floating point is not OGC WKT
            "POINT (1e 2)",                       // exponent requires at least one digit
            "LINESTRING (1 2)",                   // too few vertices
            "POLYGON ((0 0, 1 0, 1 1, 0 1))",     // ring is not closed
            "MULTIPOINT (LINESTRING (0 0, 1 1))", // wrong child syntax
            "SRID=4326;POINT (1 2)",              // EWKT is deliberately deferred
            "POINT (1 2) trailing",               // trailing input
    };

    for (const auto& input : cases) {
        WkbGeometry geometry;
        EXPECT_FALSE(WkbCodec::parse_wkt(input, &geometry).ok()) << input;
    }
}

TEST(WkbCodecTest, RejectsMalformedWkb) {
    const std::vector<std::string> cases = {
            "",
            from_hex("02"),                                           // invalid byte order
            from_hex("0101000000000000000000f03f"),                   // truncated POINT
            from_hex("0101000020"),                                   // EWKB SRID flag
            from_hex("010200000041420f00"),                           // element count exceeds safety limit
            from_hex("010300000040420f00"),                           // polygon ring count exceeds input size
            from_hex("010700000040420f00"),                           // collection count exceeds input size
            from_hex("010400000001000000010200000000000000"),         // MULTIPOINT containing LINESTRING EMPTY
            from_hex("0101000000000000000000f03f000000000000004000"), // trailing byte
    };

    for (const auto& input : cases) {
        WkbGeometry geometry;
        EXPECT_FALSE(WkbCodec::parse_wkb(Slice(input), &geometry).ok());
    }
}

TEST(WkbCodecTest, RejectsExcessiveCollectionNesting) {
    std::string input;
    for (size_t i = 0; i < 34; ++i) {
        input.append("GEOMETRYCOLLECTION(");
    }
    input.append("POINT(1 2)");
    for (size_t i = 0; i < 34; ++i) {
        input.push_back(')');
    }

    WkbGeometry geometry;
    EXPECT_FALSE(WkbCodec::parse_wkt(input, &geometry).ok());
}

TEST(WkbCodecTest, DeterministicMalformedInputFuzz) {
    std::mt19937_64 random(20260831);
    for (size_t iteration = 0; iteration < 5000; ++iteration) {
        std::string input(random() % 128, '\0');
        for (char& byte : input) {
            byte = static_cast<char>(random());
        }

        WkbGeometry geometry;
        if (!WkbCodec::parse_wkb(Slice(input), &geometry).ok()) {
            continue;
        }

        std::string canonical_wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(geometry, &canonical_wkb).ok());
        WkbGeometry reparsed;
        ASSERT_TRUE(WkbCodec::parse_wkb(Slice(canonical_wkb), &reparsed).ok());
        std::string original_wkt;
        std::string reparsed_wkt;
        ASSERT_TRUE(WkbCodec::to_wkt(geometry, &original_wkt).ok());
        ASSERT_TRUE(WkbCodec::to_wkt(reparsed, &reparsed_wkt).ok());
        EXPECT_EQ(original_wkt, reparsed_wkt);
    }
}

} // namespace
} // namespace starrocks
