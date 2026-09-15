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

#include "exprs/mgrs_functions.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class MgrsFunctionsTest : public ::testing::Test {
protected:
    static Columns make_lng_lat(double lng, double lat) {
        auto c0 = DoubleColumn::create();
        c0->append(lng);
        auto c1 = DoubleColumn::create();
        c1->append(lat);
        return {std::move(c0), std::move(c1)};
    }

    static Columns make_lng_lat_prec(double lng, double lat, int32_t prec) {
        auto c0 = DoubleColumn::create();
        c0->append(lng);
        auto c1 = DoubleColumn::create();
        c1->append(lat);
        auto c2 = Int32Column::create();
        c2->append(prec);
        return {std::move(c0), std::move(c1), std::move(c2)};
    }

    static Columns make_varchar(const std::string& s) {
        auto col = BinaryColumn::create();
        col->append(s);
        return {std::move(col)};
    }

    static std::string get_varchar(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_slice().to_string();
    }

    static double get_double(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_double();
    }

    static bool is_null(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        return col->is_null(0);
    }

    std::unique_ptr<FunctionContext> ctx{FunctionContext::create_test_context()};
};

// ─── geoToMGRS ───────────────────────────────────────────────────────────────

TEST_F(MgrsFunctionsTest, geo_to_mgrs_eiffel_tower_default_precision) {
    // ClickHouse reference: geoToMGRS(2.294497, 48.858222) = '31UDQ4825111935'
    EXPECT_EQ("31UDQ4825111935", get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat(2.294497, 48.858222))));
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_eiffel_tower_precision_3) {
    // ClickHouse reference: geoToMGRS(2.294497, 48.858222, 3) = '31UDQ482119'
    EXPECT_EQ("31UDQ482119",
              get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(2.294497, 48.858222, 3))));
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_precision_0) {
    // precision 0 → just zone+band+100km square (5 chars)
    std::string s = get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(2.294497, 48.858222, 0)));
    EXPECT_EQ(5u, s.size());
    EXPECT_EQ("31UDQ", s);
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_precision_lengths) {
    for (int p = 0; p <= 5; ++p) {
        std::string s = get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(2.294497, 48.858222, p)));
        // 5 fixed chars + 2*p digit chars
        EXPECT_EQ(5u + 2u * static_cast<size_t>(p), s.size()) << "precision=" << p;
    }
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_southern_hemisphere) {
    // Sydney, Australia: 151.2093°E, 33.8688°S
    std::string s = get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(151.2093, -33.8688, 5)));
    EXPECT_FALSE(s.empty());
    // Band letter should be in southern bands (C–M)
    EXPECT_LT(s[2], 'N');
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_null_lat_out_of_range_high) {
    EXPECT_TRUE(is_null(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat(0.0, 85.0))));
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_null_lat_out_of_range_low) {
    EXPECT_TRUE(is_null(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat(0.0, -81.0))));
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_null_precision_out_of_range) {
    EXPECT_TRUE(is_null(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(0.0, 0.0, 6))));
    EXPECT_TRUE(is_null(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(0.0, 0.0, -1))));
}

TEST_F(MgrsFunctionsTest, geo_to_mgrs_null_island) {
    // 0°N, 0°E — should encode without error
    std::string s = get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(0.0, 0.0, 5)));
    EXPECT_EQ(15u, s.size());
}

// Norway exception (zone 32 widened)
TEST_F(MgrsFunctionsTest, geo_to_mgrs_norway_zone) {
    // Bergen, Norway: 5.32°E, 60.39°N — should land in zone 32
    std::string s = get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat(5.32, 60.39)));
    EXPECT_EQ("32", s.substr(0, 2));
}

// ─── MGRSToLat / MGRSToLng ───────────────────────────────────────────────────

TEST_F(MgrsFunctionsTest, mgrs_to_lat_eiffel) {
    // ClickHouse reference: MGRSToGeo('31UDQ4825111935') = (2.294495618908297, 48.85822536113692)
    double lat = get_double(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("31UDQ4825111935")));
    EXPECT_NEAR(48.85822536113692, lat, 1e-6);
}

TEST_F(MgrsFunctionsTest, mgrs_to_lng_eiffel) {
    double lng = get_double(MgrsFunctions::mgrs_to_lng(ctx.get(), make_varchar("31UDQ4825111935")));
    EXPECT_NEAR(2.294495618908297, lng, 1e-6);
}

TEST_F(MgrsFunctionsTest, mgrs_roundtrip_within_half_metre) {
    // Encode at precision 5 (1 m), then decode — should be within 0.5 m cell half-width
    const double lng_in = 2.294497, lat_in = 48.858222;
    std::string mgrs = get_varchar(MgrsFunctions::geo_to_mgrs(ctx.get(), make_lng_lat_prec(lng_in, lat_in, 5)));
    double lat_out = get_double(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar(mgrs)));
    double lng_out = get_double(MgrsFunctions::mgrs_to_lng(ctx.get(), make_varchar(mgrs)));
    // 0.5 m ≈ 4.5e-6° latitude; longitude tolerance a bit larger at this latitude
    EXPECT_NEAR(lat_in, lat_out, 5e-6);
    EXPECT_NEAR(lng_in, lng_out, 8e-6);
}

TEST_F(MgrsFunctionsTest, mgrs_to_lat_case_insensitive) {
    double lat_upper = get_double(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("31UDQ4825111935")));
    double lat_lower = get_double(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("31udq4825111935")));
    EXPECT_NEAR(lat_upper, lat_lower, 1e-12);
}

TEST_F(MgrsFunctionsTest, mgrs_to_lat_whitespace_ignored) {
    double lat_clean = get_double(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("31UDQ4825111935")));
    double lat_spaces = get_double(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("31U DQ 48251 11935")));
    EXPECT_NEAR(lat_clean, lat_spaces, 1e-12);
}

TEST_F(MgrsFunctionsTest, mgrs_to_lat_null_for_invalid) {
    EXPECT_TRUE(is_null(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("NOT_AN_MGRS"))));
    EXPECT_TRUE(is_null(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar(""))));
    EXPECT_TRUE(is_null(MgrsFunctions::mgrs_to_lat(ctx.get(), make_varchar("99ZZZ00000"))));
}

TEST_F(MgrsFunctionsTest, mgrs_to_lng_null_for_invalid) {
    EXPECT_TRUE(is_null(MgrsFunctions::mgrs_to_lng(ctx.get(), make_varchar("NOT_AN_MGRS"))));
}

// ─── NULL input propagation ───────────────────────────────────────────────────

TEST_F(MgrsFunctionsTest, null_input_propagates) {
    auto null_dbl = [] {
        auto d = DoubleColumn::create();
        d->append(0.0);
        auto n = NullColumn::create();
        n->append(1);
        return NullableColumn::create(std::move(d), std::move(n));
    };
    auto null_str = [] {
        auto d = BinaryColumn::create();
        d->append(Slice{});
        auto n = NullColumn::create();
        n->append(1);
        return NullableColumn::create(std::move(d), std::move(n));
    };

    auto nd = null_dbl(), nd2 = null_dbl();
    EXPECT_TRUE(is_null(MgrsFunctions::geo_to_mgrs(ctx.get(), {nd, nd2})));
    auto ns = null_str();
    EXPECT_TRUE(is_null(MgrsFunctions::mgrs_to_lat(ctx.get(), {ns})));
    auto ns2 = null_str();
    EXPECT_TRUE(is_null(MgrsFunctions::mgrs_to_lng(ctx.get(), {ns2})));
}

} // namespace starrocks
