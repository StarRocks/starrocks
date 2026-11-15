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

#include "types/geo_wkb.h"

#include <gtest/gtest.h>

#include <bit>
#include <limits>
#include <string>
#include <vector>

namespace starrocks {
namespace {

void u32(std::string& out, uint32_t value, bool little) {
    for (int i = 0; i < 4; ++i) out.push_back(char(value >> (8 * (little ? i : 3 - i))));
}
void f64(std::string& out, double value, bool little) {
    auto bits = std::bit_cast<uint64_t>(value);
    for (int i = 0; i < 8; ++i) out.push_back(char(bits >> (8 * (little ? i : 7 - i))));
}
std::string header(uint32_t type, bool little) {
    std::string out(1, char(little));
    u32(out, type, little);
    return out;
}
std::string shape(uint32_t type, uint32_t dimension, bool little, bool empty = false) {
    auto out = header(type + dimension * 1000, little);
    auto tuple = [&] {
        const uint32_t width = 2 + (dimension == 3 ? 2 : dimension != 0);
        for (uint32_t i = 0; i < width; ++i)
            f64(out, empty ? std::numeric_limits<double>::quiet_NaN() : double(i + 1), little);
    };
    if (type == 1) {
        tuple();
    } else if (type == 2) {
        u32(out, empty ? 0 : 2, little);
        if (!empty) {
            tuple();
            tuple();
        }
    } else if (type == 3) {
        u32(out, empty ? 0 : 1, little);
        if (!empty) {
            u32(out, 4, little);
            for (int i = 0; i < 4; ++i) tuple();
        }
    } else {
        u32(out, empty ? 0 : 1, little);
        if (!empty) out += shape(type == 7 ? 6 : type - 3, dimension, !little);
    }
    return out;
}

TEST(GeoWkbTest, AllFamiliesDimensionsAndByteOrders) {
    const GeoDimensionPB dims[] = {GEO_DIMENSION_XY, GEO_DIMENSION_XYZ, GEO_DIMENSION_XYM, GEO_DIMENSION_XYZM};
    for (bool little : {false, true}) {
        for (uint32_t dimension = 0; dimension < 4; ++dimension) {
            for (uint32_t type = 1; type <= 7; ++type) {
                for (bool empty : {false, true}) {
                    auto bytes = shape(type, dimension, little, empty);
                    const auto original = bytes;
                    auto result = inspect_geo_wkb(Slice(bytes));
                    ASSERT_TRUE(result.ok()) << result.status();
                    EXPECT_EQ(type, result->geometry_type);
                    EXPECT_EQ(dims[dimension], result->dimension);
                    EXPECT_EQ(empty, result->empty);
                    EXPECT_EQ(original, bytes);
                }
            }
        }
    }
}

TEST(GeoWkbTest, EveryTruncationIsRejected) {
    for (uint32_t type = 1; type <= 7; ++type) {
        for (bool little : {false, true}) {
            auto bytes = shape(type, 3, little);
            for (size_t end = 0; end < bytes.size(); ++end)
                EXPECT_FALSE(inspect_geo_wkb(Slice(bytes.data(), end)).ok()) << type << " " << end;
        }
    }
    EXPECT_FALSE(inspect_geo_wkb(Slice()).ok());
}

TEST(GeoWkbTest, CountAndPayloadBudgets) {
    auto point = shape(1, 0, true);
    GeoWkbLimits limits;
    limits.max_bytes = point.size() - 1;
    EXPECT_FALSE(inspect_geo_wkb(Slice(point), limits).ok());
    limits.max_bytes = point.size();
    EXPECT_TRUE(inspect_geo_wkb(Slice(point), limits).ok());
    limits.max_coordinates = 0;
    EXPECT_FALSE(inspect_geo_wkb(Slice(point), limits).ok());
    limits.max_coordinates = 1;
    EXPECT_TRUE(inspect_geo_wkb(Slice(point), limits).ok());
    limits.max_components = 0;
    EXPECT_FALSE(inspect_geo_wkb(Slice(point), limits).ok());
    for (uint32_t type = 2; type <= 7; ++type) {
        auto bytes = header(type, true);
        u32(bytes, UINT32_MAX, true);
        EXPECT_FALSE(inspect_geo_wkb(Slice(bytes)).ok());
    }
    auto line = shape(2, 3, true);
    limits = {};
    limits.max_coordinates = 1;
    EXPECT_FALSE(inspect_geo_wkb(Slice(line), limits).ok());
    auto polygon = shape(3, 0, true);
    limits = {};
    limits.max_components = 1; // The ring also consumes work.
    EXPECT_FALSE(inspect_geo_wkb(Slice(polygon), limits).ok());
    limits.max_components = 2;
    EXPECT_TRUE(inspect_geo_wkb(Slice(polygon), limits).ok());
}

TEST(GeoWkbTest, NestingLimitAndHardStackBound) {
    auto bytes = shape(1, 0, true);
    for (uint32_t depth = 2; depth <= 65; ++depth) {
        auto outer = header(7, true);
        u32(outer, 1, true);
        bytes = outer + bytes;
        GeoWkbLimits limits;
        limits.max_depth = depth;
        EXPECT_EQ(depth <= 64, inspect_geo_wkb(Slice(bytes), limits).ok());
        limits.max_depth = depth - 1;
        EXPECT_FALSE(inspect_geo_wkb(Slice(bytes), limits).ok());
    }
    GeoWkbLimits limits;
    limits.max_depth = UINT32_MAX;
    EXPECT_FALSE(inspect_geo_wkb(Slice(bytes), limits).ok());
}

TEST(GeoWkbTest, InvalidHeadersChildrenAndTrailingBytes) {
    auto point = shape(1, 0, true);
    point[0] = 2;
    EXPECT_FALSE(inspect_geo_wkb(Slice(point)).ok());
    for (uint32_t code : {0u, 8u, 4001u, 0x80000001u, 0x20000001u}) {
        auto bytes = header(code, true);
        EXPECT_FALSE(inspect_geo_wkb(Slice(bytes)).ok());
    }
    auto multi = header(4, true);
    u32(multi, 1, true);
    EXPECT_FALSE(inspect_geo_wkb(Slice(multi + shape(2, 0, true))).ok());
    EXPECT_FALSE(inspect_geo_wkb(Slice(multi + shape(1, 1, true))).ok());
    auto bytes = shape(1, 0, true) + "x";
    EXPECT_FALSE(inspect_geo_wkb(Slice(bytes)).ok());
}

TEST(GeoWkbTest, EmptyChildrenAndMixedCollection) {
    auto collection = header(7, true);
    u32(collection, 2, true);
    auto bytes = collection + shape(1, 0, false, true) + shape(3, 0, true, true);
    auto info = inspect_geo_wkb(Slice(bytes));
    ASSERT_TRUE(info.ok());
    EXPECT_TRUE(info->empty);
    EXPECT_EQ(3, info->components);
    bytes = collection + shape(1, 0, false) + shape(2, 2, true);
    info = inspect_geo_wkb(Slice(bytes));
    ASSERT_TRUE(info.ok());
    EXPECT_FALSE(info->empty);
    EXPECT_EQ(GEO_DIMENSION_MIXED, info->dimension);
    EXPECT_EQ(3, info->coordinates);
}

TEST(GeoWkbTest, ResultOutlivesInput) {
    auto info = [] {
        auto bytes = shape(3, 1, false);
        return inspect_geo_wkb(Slice(bytes));
    }();
    ASSERT_TRUE(info.ok());
    EXPECT_EQ(3, info->geometry_type);
    EXPECT_EQ(4, info->coordinates);
    EXPECT_EQ(GEO_DIMENSION_XYZ, info->dimension);
}

TEST(GeoWkbTest, HostileBuffersStayWithinBudgets) {
    GeoWkbLimits limits;
    limits.max_bytes = 256;
    limits.max_components = 32;
    limits.max_coordinates = 32;
    limits.max_depth = 8;
    uint32_t state = 0x9e3779b9;
    for (size_t attempt = 0; attempt < 4096; ++attempt) {
        auto bytes = shape(1 + attempt % 7, attempt % 4, attempt % 2);
        for (size_t i = attempt % 3; i < bytes.size(); i += 7) {
            state = state * 1664525 + 1013904223;
            bytes[i] = char(state >> 24);
        }
        auto info = inspect_geo_wkb(Slice(bytes), limits);
        if (info.ok()) {
            EXPECT_LE(info->components, limits.max_components);
            EXPECT_LE(info->coordinates, limits.max_coordinates);
            EXPECT_GE(info->geometry_type, 1);
            EXPECT_LE(info->geometry_type, 7);
        }
    }
}

} // namespace
} // namespace starrocks
