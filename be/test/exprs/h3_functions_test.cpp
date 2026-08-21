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

#include "exprs/h3_functions.h"

#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// ─────────────────────────────────────────────────────────────────────────────
// Known H3 test values
//
// San Francisco (37.7749 N, -122.4194 W) at resolution 9:
//   h3index = 617700169958293503  (0x08928308280fffff)
//
// This is the cell whose validity and roundtrip we rely on in all tests.
// ─────────────────────────────────────────────────────────────────────────────
static constexpr int64_t kSfCell9 = 617700169958293503LL;
static constexpr double kSfLng = -122.4194;
static constexpr double kSfLat = 37.7749;
static constexpr int kSfRes = 9;

class H3FunctionsTest : public ::testing::Test {
protected:
    // Create a non-nullable BIGINT column with one value.
    static Columns make_bigint(int64_t v) {
        auto col = Int64Column::create();
        col->append(v);
        return {std::move(col)};
    }

    // Create a non-nullable INT column with one value.
    static Columns make_int(int32_t v) {
        auto col = Int32Column::create();
        col->append(v);
        return {std::move(col)};
    }

    // Create a non-nullable DOUBLE column with one value.
    static Columns make_double(double v) {
        auto col = DoubleColumn::create();
        col->append(v);
        return {std::move(col)};
    }

    // Create a non-nullable VARCHAR column with one value.
    static Columns make_varchar(const std::string& s) {
        auto col = BinaryColumn::create();
        col->append(s);
        return {std::move(col)};
    }

    // Unwrap the first row of a non-nullable BIGINT result.
    static int64_t get_bigint(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        // Peel ConstColumn if present.
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_int64();
    }

    static int32_t get_int(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_int32();
    }

    static double get_double(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_double();
    }

    static bool get_bool(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_uint8() != 0;
    }

    static std::string get_varchar(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        EXPECT_FALSE(col->is_null(0));
        return col->get(0).get_slice().to_string();
    }

    static bool is_null(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        auto col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        return col->is_null(0);
    }

    // Extract the array at row 0 of an ARRAY(BIGINT) result as a sorted vector.
    static std::vector<int64_t> get_array_bigint(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        ColumnPtr col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        // Unwrap nullable outer column.
        if (col->is_nullable()) col = down_cast<NullableColumn*>(col.get())->data_column();
        auto* arr = down_cast<ArrayColumn*>(col.get());
        auto* elems = down_cast<Int64Column*>(arr->elements_column().get());
        uint32_t start = arr->offsets().get_data()[0];
        uint32_t end = arr->offsets().get_data()[1];
        std::vector<int64_t> out;
        for (uint32_t i = start; i < end; ++i) out.push_back(elems->get_data()[i]);
        std::sort(out.begin(), out.end());
        return out;
    }

    static std::vector<int32_t> get_array_int(const StatusOr<ColumnPtr>& res) {
        EXPECT_TRUE(res.ok()) << res.status().message();
        ColumnPtr col = res.value();
        if (col->is_constant()) col = down_cast<ConstColumn*>(col.get())->data_column();
        if (col->is_nullable()) col = down_cast<NullableColumn*>(col.get())->data_column();
        auto* arr = down_cast<ArrayColumn*>(col.get());
        auto* elems = down_cast<Int32Column*>(arr->elements_column().get());
        uint32_t start = arr->offsets().get_data()[0];
        uint32_t end = arr->offsets().get_data()[1];
        std::vector<int32_t> out;
        for (uint32_t i = start; i < end; ++i) out.push_back(elems->get_data()[i]);
        std::sort(out.begin(), out.end());
        return out;
    }

    std::unique_ptr<FunctionContext> ctx{FunctionContext::create_test_context()};
};

// ─────────────────────────────────────────────────────────────────────────────
// Core 5 (regression guard)
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, geo_to_h3_basic) {
    auto lng = DoubleColumn::create();
    lng->append(kSfLng);
    auto lat = DoubleColumn::create();
    lat->append(kSfLat);
    auto res = Int32Column::create();
    res->append(kSfRes);
    Columns cols{std::move(lng), std::move(lat), std::move(res)};
    EXPECT_EQ(kSfCell9, get_bigint(H3Functions::geo_to_h3(ctx.get(), cols)));
}

TEST_F(H3FunctionsTest, geo_to_h3_invalid_resolution) {
    auto lng = DoubleColumn::create();
    lng->append(kSfLng);
    auto lat = DoubleColumn::create();
    lat->append(kSfLat);
    auto res = Int32Column::create();
    res->append(16); // out of range
    Columns cols{std::move(lng), std::move(lat), std::move(res)};
    EXPECT_TRUE(is_null(H3Functions::geo_to_h3(ctx.get(), cols)));
}

TEST_F(H3FunctionsTest, h3_is_valid_true) {
    EXPECT_TRUE(get_bool(H3Functions::h3_is_valid(ctx.get(), make_bigint(kSfCell9))));
}

TEST_F(H3FunctionsTest, h3_is_valid_false) {
    EXPECT_FALSE(get_bool(H3Functions::h3_is_valid(ctx.get(), make_bigint(0LL))));
}

TEST_F(H3FunctionsTest, h3_get_resolution) {
    EXPECT_EQ(kSfRes, get_int(H3Functions::h3_get_resolution(ctx.get(), make_bigint(kSfCell9))));
}

TEST_F(H3FunctionsTest, h3_to_geo_lat_roundtrip) {
    double lat = get_double(H3Functions::h3_to_geo_lat(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_NEAR(kSfLat, lat, 0.1); // cell center is within 0.1° of input
}

TEST_F(H3FunctionsTest, h3_to_geo_lng_roundtrip) {
    double lng = get_double(H3Functions::h3_to_geo_lng(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_NEAR(kSfLng, lng, 0.1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Resolution → scalar
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3_edge_angle_res9) {
    double v = get_double(H3Functions::h3_edge_angle(ctx.get(), make_int(9)));
    EXPECT_GT(v, 0.0);
    EXPECT_LT(v, 1.0); // must be a small angle in degrees
}

TEST_F(H3FunctionsTest, h3_edge_length_m_increases_with_lower_res) {
    double lo = get_double(H3Functions::h3_edge_length_m(ctx.get(), make_int(5)));
    double hi = get_double(H3Functions::h3_edge_length_m(ctx.get(), make_int(9)));
    EXPECT_GT(lo, hi);
}

TEST_F(H3FunctionsTest, h3_edge_length_km_consistent) {
    double m = get_double(H3Functions::h3_edge_length_m(ctx.get(), make_int(9)));
    double km = get_double(H3Functions::h3_edge_length_km(ctx.get(), make_int(9)));
    EXPECT_NEAR(m / 1000.0, km, km * 1e-9);
}

TEST_F(H3FunctionsTest, h3_hex_area_m2_res0) {
    double area = get_double(H3Functions::h3_hex_area_m2(ctx.get(), make_int(0)));
    EXPECT_GT(area, 1e12); // res-0 cells cover ~4 million km²
}

TEST_F(H3FunctionsTest, h3_hex_area_km2_consistent) {
    double m2 = get_double(H3Functions::h3_hex_area_m2(ctx.get(), make_int(5)));
    double km2 = get_double(H3Functions::h3_hex_area_km2(ctx.get(), make_int(5)));
    EXPECT_NEAR(m2 / 1e6, km2, km2 * 1e-9);
}

TEST_F(H3FunctionsTest, h3_num_hexagons_res0) {
    int64_t n = get_bigint(H3Functions::h3_num_hexagons(ctx.get(), make_int(0)));
    EXPECT_EQ(122LL, n);
}

TEST_F(H3FunctionsTest, h3_num_hexagons_grows_with_resolution) {
    int64_t n0 = get_bigint(H3Functions::h3_num_hexagons(ctx.get(), make_int(0)));
    int64_t n3 = get_bigint(H3Functions::h3_num_hexagons(ctx.get(), make_int(3)));
    EXPECT_GT(n3, n0);
}

TEST_F(H3FunctionsTest, h3_edge_length_invalid_res_null) {
    EXPECT_TRUE(is_null(H3Functions::h3_edge_length_m(ctx.get(), make_int(16))));
}

// ─────────────────────────────────────────────────────────────────────────────
// H3 index → scalar
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3_get_base_cell_range) {
    int32_t bc = get_int(H3Functions::h3_get_base_cell(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_GE(bc, 0);
    EXPECT_LT(bc, 122);
}

TEST_F(H3FunctionsTest, h3_is_res_class_iii_odd_res) {
    // Odd resolutions are Class III.
    EXPECT_TRUE(get_bool(H3Functions::h3_is_res_class_iii(ctx.get(), make_bigint(kSfCell9))));
}

TEST_F(H3FunctionsTest, h3_is_pentagon_false_for_hex) {
    EXPECT_FALSE(get_bool(H3Functions::h3_is_pentagon(ctx.get(), make_bigint(kSfCell9))));
}

TEST_F(H3FunctionsTest, h3_cell_area_m2_positive) {
    double area = get_double(H3Functions::h3_cell_area_m2(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_GT(area, 0.0);
}

TEST_F(H3FunctionsTest, h3_cell_area_rads2_positive) {
    double area = get_double(H3Functions::h3_cell_area_rads2(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_GT(area, 0.0);
}

// ─────────────────────────────────────────────────────────────────────────────
// String / index conversions
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3_to_string_roundtrip) {
    std::string s = get_varchar(H3Functions::h3_to_string(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_FALSE(s.empty());
    // Roundtrip: stringToH3(h3ToString(x)) == x
    int64_t back = get_bigint(H3Functions::string_to_h3(ctx.get(), make_varchar(s)));
    EXPECT_EQ(kSfCell9, back);
}

TEST_F(H3FunctionsTest, string_to_h3_invalid_null) {
    EXPECT_TRUE(is_null(H3Functions::string_to_h3(ctx.get(), make_varchar("not_a_cell"))));
}

// ─────────────────────────────────────────────────────────────────────────────
// Two-index scalar functions
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3_to_parent_then_indexes_are_not_neighbors) {
    // parent at res 8 is not a neighbour of the child at res 9
    auto h3c = Int64Column::create();
    h3c->append(kSfCell9);
    auto rc = Int32Column::create();
    rc->append(8);
    int64_t parent = get_bigint(H3Functions::h3_to_parent(ctx.get(), {std::move(h3c), std::move(rc)}));
    EXPECT_EQ(8, get_int(H3Functions::h3_get_resolution(ctx.get(), make_bigint(parent))));

    auto h1 = Int64Column::create();
    h1->append(kSfCell9);
    auto h2 = Int64Column::create();
    h2->append(parent);
    Columns cols{std::move(h1), std::move(h2)};
    EXPECT_FALSE(get_bool(H3Functions::h3_indexes_are_neighbors(ctx.get(), cols)));
}

TEST_F(H3FunctionsTest, h3_to_center_child_has_finer_res) {
    auto h = Int64Column::create();
    h->append(kSfCell9);
    auto r = Int32Column::create();
    r->append(10);
    Columns cols{std::move(h), std::move(r)};
    int64_t child = get_bigint(H3Functions::h3_to_center_child(ctx.get(), cols));
    EXPECT_EQ(10, get_int(H3Functions::h3_get_resolution(ctx.get(), make_bigint(child))));
}

TEST_F(H3FunctionsTest, h3_distance_self_is_zero) {
    auto h1 = Int64Column::create();
    h1->append(kSfCell9);
    auto h2 = Int64Column::create();
    h2->append(kSfCell9);
    Columns cols{std::move(h1), std::move(h2)};
    EXPECT_EQ(0LL, get_bigint(H3Functions::h3_distance(ctx.get(), cols)));
}

// ─────────────────────────────────────────────────────────────────────────────
// Directed edges
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, directed_edge_roundtrip) {
    // Get a neighbour cell (kRing k=1 gives ≥1 neighbour besides self)
    auto h3c = Int64Column::create();
    h3c->append(kSfCell9);
    auto kc = Int32Column::create();
    kc->append(1);
    Columns ring_cols{std::move(h3c), std::move(kc)};
    auto ring = get_array_bigint(H3Functions::h3k_ring(ctx.get(), ring_cols));
    // Pick a neighbour that is different from kSfCell9.
    int64_t neighbour = -1;
    for (auto v : ring) {
        if (v != kSfCell9) {
            neighbour = v;
            break;
        }
    }
    ASSERT_NE(-1, neighbour) << "kRing(k=1) should return at least one neighbour";

    // Build edge
    auto orig = Int64Column::create();
    orig->append(kSfCell9);
    auto dest = Int64Column::create();
    dest->append(neighbour);
    Columns edge_cols{std::move(orig), std::move(dest)};
    int64_t edge = get_bigint(H3Functions::h3_get_unidirectional_edge(ctx.get(), edge_cols));

    EXPECT_TRUE(get_bool(H3Functions::h3_unidirectional_edge_is_valid(ctx.get(), make_bigint(edge))));
    EXPECT_EQ(kSfCell9,
              get_bigint(H3Functions::h3_get_origin_index_from_unidirectional_edge(ctx.get(), make_bigint(edge))));
    EXPECT_EQ(neighbour,
              get_bigint(H3Functions::h3_get_destination_index_from_unidirectional_edge(ctx.get(), make_bigint(edge))));
}

TEST_F(H3FunctionsTest, h3_unidirectional_edge_is_valid_false_for_cell) {
    EXPECT_FALSE(get_bool(H3Functions::h3_unidirectional_edge_is_valid(ctx.get(), make_bigint(kSfCell9))));
}

// ─────────────────────────────────────────────────────────────────────────────
// Exact edge lengths
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3_exact_edge_lengths_consistent) {
    // Build an edge from kSfCell9 to a neighbour.
    auto h3c = Int64Column::create();
    h3c->append(kSfCell9);
    auto kc = Int32Column::create();
    kc->append(1);
    auto ring = get_array_bigint(H3Functions::h3k_ring(ctx.get(), {std::move(h3c), std::move(kc)}));
    int64_t nbr = -1;
    for (auto v : ring) {
        if (v != kSfCell9) {
            nbr = v;
            break;
        }
    }
    ASSERT_NE(-1, nbr);

    auto o = Int64Column::create();
    o->append(kSfCell9);
    auto d = Int64Column::create();
    d->append(nbr);
    int64_t edge = get_bigint(H3Functions::h3_get_unidirectional_edge(ctx.get(), {std::move(o), std::move(d)}));

    double m = get_double(H3Functions::h3_exact_edge_length_m(ctx.get(), make_bigint(edge)));
    double km = get_double(H3Functions::h3_exact_edge_length_km(ctx.get(), make_bigint(edge)));
    double rads = get_double(H3Functions::h3_exact_edge_length_rads(ctx.get(), make_bigint(edge)));
    EXPECT_NEAR(m / 1000.0, km, km * 1e-9);
    EXPECT_GT(rads, 0.0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Point distance functions
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3_point_dist_self_zero) {
    auto la1 = DoubleColumn::create();
    la1->append(kSfLat);
    auto lo1 = DoubleColumn::create();
    lo1->append(kSfLng);
    auto la2 = DoubleColumn::create();
    la2->append(kSfLat);
    auto lo2 = DoubleColumn::create();
    lo2->append(kSfLng);
    Columns cols{std::move(la1), std::move(lo1), std::move(la2), std::move(lo2)};
    EXPECT_NEAR(0.0, get_double(H3Functions::h3_point_dist_m(ctx.get(), cols)), 1e-6);
}

TEST_F(H3FunctionsTest, h3_point_dist_equator) {
    // 20° apart on equator ≈ 2,224 km
    auto la1 = DoubleColumn::create();
    la1->append(0.0);
    auto lo1 = DoubleColumn::create();
    lo1->append(-10.0);
    auto la2 = DoubleColumn::create();
    la2->append(0.0);
    auto lo2 = DoubleColumn::create();
    lo2->append(10.0);
    Columns cols{std::move(la1), std::move(lo1), std::move(la2), std::move(lo2)};

    double m = get_double(H3Functions::h3_point_dist_m(ctx.get(), cols));
    double km = get_double(H3Functions::h3_point_dist_km(ctx.get(), cols));
    double r = get_double(H3Functions::h3_point_dist_rads(ctx.get(), cols));
    EXPECT_NEAR(m / 1000.0, km, km * 1e-6);
    EXPECT_NEAR(r, km / 6371.0, 0.001);
    EXPECT_NEAR(km, 2223.9, 1.0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Array-returning functions
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(H3FunctionsTest, h3k_ring_k0_returns_self) {
    auto h = Int64Column::create();
    h->append(kSfCell9);
    auto k = Int32Column::create();
    k->append(0);
    auto ring = get_array_bigint(H3Functions::h3k_ring(ctx.get(), {std::move(h), std::move(k)}));
    ASSERT_EQ(1u, ring.size());
    EXPECT_EQ(kSfCell9, ring[0]);
}

TEST_F(H3FunctionsTest, h3k_ring_k1_returns_7_cells) {
    auto h = Int64Column::create();
    h->append(kSfCell9);
    auto k = Int32Column::create();
    k->append(1);
    auto ring = get_array_bigint(H3Functions::h3k_ring(ctx.get(), {std::move(h), std::move(k)}));
    EXPECT_EQ(7u, ring.size()); // centre + 6 neighbours (all hex, no pentagon)
    EXPECT_NE(ring.end(), std::find(ring.begin(), ring.end(), kSfCell9));
}

TEST_F(H3FunctionsTest, h3_hex_ring_k1_returns_6_cells) {
    auto h = Int64Column::create();
    h->append(kSfCell9);
    auto k = Int32Column::create();
    k->append(1);
    auto ring = get_array_bigint(H3Functions::h3_hex_ring(ctx.get(), {std::move(h), std::move(k)}));
    EXPECT_EQ(6u, ring.size());
    EXPECT_EQ(ring.end(), std::find(ring.begin(), ring.end(), kSfCell9));
}

TEST_F(H3FunctionsTest, h3_to_children_res10_returns_7) {
    auto h = Int64Column::create();
    h->append(kSfCell9);
    auto r = Int32Column::create();
    r->append(10);
    auto kids = get_array_bigint(H3Functions::h3_to_children(ctx.get(), {std::move(h), std::move(r)}));
    EXPECT_EQ(7u, kids.size()); // 7 children per hex at next resolution
    for (auto c : kids) {
        EXPECT_EQ(10, get_int(H3Functions::h3_get_resolution(ctx.get(), make_bigint(c))));
    }
}

TEST_F(H3FunctionsTest, h3_line_self_returns_1) {
    auto h1 = Int64Column::create();
    h1->append(kSfCell9);
    auto h2 = Int64Column::create();
    h2->append(kSfCell9);
    auto path = get_array_bigint(H3Functions::h3_line(ctx.get(), {std::move(h1), std::move(h2)}));
    EXPECT_EQ(1u, path.size());
    EXPECT_EQ(kSfCell9, path[0]);
}

TEST_F(H3FunctionsTest, h3_get_faces_returns_at_least_one) {
    auto faces = get_array_int(H3Functions::h3_get_faces(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_GE(faces.size(), 1u);
    for (auto f : faces) {
        EXPECT_GE(f, 0);
        EXPECT_LT(f, 20); // icosahedron has 20 faces
    }
}

TEST_F(H3FunctionsTest, h3_get_res0_indexes_returns_122) {
    Columns empty;
    auto cells = get_array_bigint(H3Functions::h3_get_res0_indexes(ctx.get(), empty));
    EXPECT_EQ(122u, cells.size());
    for (auto c : cells) {
        EXPECT_EQ(0, get_int(H3Functions::h3_get_resolution(ctx.get(), make_bigint(c))));
    }
}

TEST_F(H3FunctionsTest, h3_get_pentagon_indexes_returns_12) {
    auto pents = get_array_bigint(H3Functions::h3_get_pentagon_indexes(ctx.get(), make_int(5)));
    EXPECT_EQ(12u, pents.size());
    for (auto p : pents) {
        EXPECT_TRUE(get_bool(H3Functions::h3_is_pentagon(ctx.get(), make_bigint(p))));
    }
}

TEST_F(H3FunctionsTest, h3_get_unidirectional_edges_from_hexagon_returns_6) {
    auto edges =
            get_array_bigint(H3Functions::h3_get_unidirectional_edges_from_hexagon(ctx.get(), make_bigint(kSfCell9)));
    EXPECT_EQ(6u, edges.size());
    for (auto e : edges) {
        EXPECT_TRUE(get_bool(H3Functions::h3_unidirectional_edge_is_valid(ctx.get(), make_bigint(e))));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NULL propagation
// ─────────────────────────────────────────────────────────────────────────────

static ColumnPtr make_null_bigint() {
    auto data = Int64Column::create();
    data->append(0);
    auto nulls = NullColumn::create();
    nulls->append(1);
    return NullableColumn::create(std::move(data), std::move(nulls));
}

static ColumnPtr make_null_int() {
    auto data = Int32Column::create();
    data->append(0);
    auto nulls = NullColumn::create();
    nulls->append(1);
    return NullableColumn::create(std::move(data), std::move(nulls));
}

TEST_F(H3FunctionsTest, null_input_propagates) {
    auto nb = make_null_bigint();
    EXPECT_TRUE(is_null(H3Functions::h3_to_geo_lat(ctx.get(), {nb})));
    EXPECT_TRUE(is_null(H3Functions::h3_is_res_class_iii(ctx.get(), {nb})));
    EXPECT_TRUE(is_null(H3Functions::h3_cell_area_m2(ctx.get(), {nb})));

    auto ni = make_null_int();
    EXPECT_TRUE(is_null(H3Functions::h3_edge_length_m(ctx.get(), {ni})));
}

} // namespace starrocks
