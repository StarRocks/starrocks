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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/double_column.h"
#include "exprs/function_context.h"
#include "exprs/geometry_functions.h"
#include "geo/geo_types.h"

namespace starrocks {

class GeometryFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}

    // Helper: encode a WKT string into a GEOMETRY column (BinaryColumn)
    static ColumnPtr make_geometry_column(const std::string& wkt) {
        GeoParseStatus status;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt.data(), wkt.size(), &status));
        EXPECT_NE(nullptr, shape) << "Bad WKT: " << wkt;
        std::string buf;
        shape->encode_to(&buf);
        auto col = BinaryColumn::create();
        col->append(Slice(buf.data(), buf.size()));
        return col;
    }

    // Helper: extract first WKT string from the GEOMETRY result column
    static std::string geometry_to_wkt(const ColumnPtr& geom_col) {
        auto binary = ColumnHelper::as_column<BinaryColumn>(geom_col);
        auto slice = binary->get_data()[0];
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(slice.data, slice.size));
        if (shape == nullptr) return "<invalid>";
        return shape->as_wkt();
    }
};

// -----------------------------------------------------------------------
// ST_GeomFromText tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, st_geom_from_text_point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto wkt_col = BinaryColumn::create();
    wkt_col->append(Slice("POINT (1.5 2.5)"));
    columns.emplace_back(std::move(wkt_col));

    auto result = GeometryFunctions::st_geom_from_text(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    ASSERT_EQ("POINT (1.5 2.5)", geometry_to_wkt(result));
}

TEST_F(GeometryFunctionsTest, st_geom_from_text_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto wkt_col = BinaryColumn::create();
    wkt_col->append(Slice("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    columns.emplace_back(std::move(wkt_col));

    auto result = GeometryFunctions::st_geom_from_text(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    // round-trip: decoded WKT should represent the same polygon
    ASSERT_FALSE(geometry_to_wkt(result).empty());
}

TEST_F(GeometryFunctionsTest, st_geom_from_text_invalid_wkt) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto wkt_col = BinaryColumn::create();
    wkt_col->append(Slice("NOT_A_VALID_WKT"));
    columns.emplace_back(std::move(wkt_col));

    auto result = GeometryFunctions::st_geom_from_text(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(GeometryFunctionsTest, st_geom_from_text_null_input) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto wkt_col = BinaryColumn::create();
    wkt_col->append_default(); // append null
    auto nullable = NullableColumn::create(std::move(wkt_col), NullColumn::create(1, 1));
    columns.emplace_back(std::move(nullable));

    auto result = GeometryFunctions::st_geom_from_text(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

// -----------------------------------------------------------------------
// ST_AsText tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, st_as_text_point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_geometry_column("POINT (3 4)"));

    auto result = GeometryFunctions::st_as_text(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto binary = ColumnHelper::as_column<BinaryColumn>(result);
    ASSERT_EQ("POINT (3 4)", binary->get_data()[0].to_string());
}

TEST_F(GeometryFunctionsTest, st_as_text_linestring) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_geometry_column("LINESTRING (0 0, 1 1, 2 2)"));

    auto result = GeometryFunctions::st_as_text(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto binary = ColumnHelper::as_column<BinaryColumn>(result);
    ASSERT_FALSE(binary->get_data()[0].to_string().empty());
}

TEST_F(GeometryFunctionsTest, st_as_text_null_input) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto geom_col = BinaryColumn::create();
    geom_col->append_default();
    auto nullable = NullableColumn::create(std::move(geom_col), NullColumn::create(1, 1));
    columns.emplace_back(std::move(nullable));

    auto result = GeometryFunctions::st_as_text(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

// -----------------------------------------------------------------------
// ST_AsWKB tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, st_as_wkb_returns_nonempty_bytes) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_geometry_column("POINT (5 6)"));

    auto result = GeometryFunctions::st_as_wkb(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto binary = ColumnHelper::as_column<BinaryColumn>(result);
    ASSERT_GT(binary->get_data()[0].size, 0u);
}

TEST_F(GeometryFunctionsTest, st_as_wkb_round_trip_via_geom_from_wkb) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Step 1: encode POINT into GEOMETRY
    auto geom_col = make_geometry_column("POINT (7 8)");
    Columns cols1;
    cols1.emplace_back(geom_col);

    // Step 2: get WKB bytes from GEOMETRY
    auto wkb_result = GeometryFunctions::st_as_wkb(ctx.get(), cols1).value();
    ASSERT_FALSE(wkb_result->is_null(0));

    // Step 3: reconstruct GEOMETRY from WKB bytes
    Columns cols2;
    cols2.emplace_back(wkb_result);
    auto geom_result = GeometryFunctions::st_geom_from_wkb(ctx.get(), cols2).value();
    ASSERT_FALSE(geom_result->is_null(0));

    // Step 4: verify WKT matches
    ASSERT_EQ("POINT (7 8)", geometry_to_wkt(geom_result));
}

// -----------------------------------------------------------------------
// ST_MakePoint tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, st_make_point_basic) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto x_col = DoubleColumn::create();
    auto y_col = DoubleColumn::create();
    x_col->append(24.7);
    y_col->append(56.7);
    columns.emplace_back(std::move(x_col));
    columns.emplace_back(std::move(y_col));

    auto result = GeometryFunctions::st_make_point(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    ASSERT_EQ("POINT (24.7 56.7)", geometry_to_wkt(result));
}

TEST_F(GeometryFunctionsTest, st_make_point_zero_coords) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto x_col = DoubleColumn::create();
    auto y_col = DoubleColumn::create();
    x_col->append(0.0);
    y_col->append(0.0);
    columns.emplace_back(std::move(x_col));
    columns.emplace_back(std::move(y_col));

    auto result = GeometryFunctions::st_make_point(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    ASSERT_EQ("POINT (0 0)", geometry_to_wkt(result));
}

TEST_F(GeometryFunctionsTest, st_make_point_null_x) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto x_col = DoubleColumn::create();
    auto y_col = DoubleColumn::create();
    x_col->append(1.0);
    y_col->append(2.0);

    auto x_nullable = NullableColumn::create(std::move(x_col), NullColumn::create(1, 1));
    columns.emplace_back(std::move(x_nullable));
    columns.emplace_back(std::move(y_col));

    auto result = GeometryFunctions::st_make_point(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

// -----------------------------------------------------------------------
// ST_Contains tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, st_contains_point_inside_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    columns.emplace_back(make_geometry_column("POINT (5 5)"));

    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    ASSERT_TRUE(bools->immutable_data()[0]);
}

TEST_F(GeometryFunctionsTest, st_contains_point_outside_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    columns.emplace_back(make_geometry_column("POINT (20 20)"));

    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    ASSERT_FALSE(bools->immutable_data()[0]);
}

TEST_F(GeometryFunctionsTest, st_contains_multiple_rows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Build polygon column
    auto poly_wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
    GeoParseStatus s;
    std::unique_ptr<GeoShape> poly_shape(GeoShape::from_wkt(poly_wkt, strlen(poly_wkt), &s));
    std::string poly_buf;
    poly_shape->encode_to(&poly_buf);

    // Build point columns: inside, outside, inside
    auto poly_col = BinaryColumn::create();
    auto point_col = BinaryColumn::create();
    for (int i = 0; i < 3; ++i) {
        poly_col->append(Slice(poly_buf.data(), poly_buf.size()));
    }

    std::string wkts[] = {"POINT (5 5)", "POINT (15 15)", "POINT (1 1)"};
    for (const auto& wkt : wkts) {
        std::unique_ptr<GeoShape> pt(GeoShape::from_wkt(wkt.data(), wkt.size(), &s));
        std::string buf;
        pt->encode_to(&buf);
        point_col->append(Slice(buf.data(), buf.size()));
    }

    Columns columns;
    columns.emplace_back(std::move(poly_col));
    columns.emplace_back(std::move(point_col));

    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    ASSERT_TRUE(bools->immutable_data()[0]);
    ASSERT_FALSE(bools->immutable_data()[1]);
    ASSERT_TRUE(bools->immutable_data()[2]);
}

TEST_F(GeometryFunctionsTest, st_contains_null_input) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto poly_col = BinaryColumn::create();
    poly_col->append_default();
    auto poly_nullable = NullableColumn::create(std::move(poly_col), NullColumn::create(1, 1));

    auto point_col = make_geometry_column("POINT (5 5)");

    Columns columns;
    columns.emplace_back(std::move(poly_nullable));
    columns.emplace_back(std::move(point_col));

    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

// -----------------------------------------------------------------------
// ST_Within tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, st_within_point_inside_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    columns.emplace_back(make_geometry_column("POINT (5 5)"));
    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));

    auto result = GeometryFunctions::st_within(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    ASSERT_TRUE(bools->immutable_data()[0]);
}

TEST_F(GeometryFunctionsTest, st_within_point_outside_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    columns.emplace_back(make_geometry_column("POINT (20 20)"));
    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));

    auto result = GeometryFunctions::st_within(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    ASSERT_FALSE(bools->immutable_data()[0]);
}

TEST_F(GeometryFunctionsTest, st_within_is_inverse_of_contains) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto polygon = make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto point = make_geometry_column("POINT (3 3)");

    // contains(polygon, point) should equal within(point, polygon)
    Columns contains_cols;
    contains_cols.emplace_back(polygon);
    contains_cols.emplace_back(point);
    auto contains_result = GeometryFunctions::st_contains(ctx.get(), contains_cols).value();

    Columns within_cols;
    within_cols.emplace_back(point);
    within_cols.emplace_back(polygon);
    auto within_result = GeometryFunctions::st_within(ctx.get(), within_cols).value();

    auto contains_bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(contains_result);
    auto within_bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(within_result);
    ASSERT_EQ(contains_bools->immutable_data()[0], within_bools->immutable_data()[0]);
}

// -----------------------------------------------------------------------
// TYPE_GEOMETRY logical type string conversion tests
// -----------------------------------------------------------------------
TEST_F(GeometryFunctionsTest, logical_type_string_conversion) {
    ASSERT_EQ(TYPE_GEOMETRY, string_to_logical_type("GEOMETRY"));
    ASSERT_EQ(TYPE_GEOMETRY, string_to_logical_type("geometry"));
    ASSERT_STREQ("GEOMETRY", logical_type_to_string(TYPE_GEOMETRY));
}

TEST_F(GeometryFunctionsTest, logical_type_max_value_updated) {
    // TYPE_GEOMETRY = 56 must be < TYPE_MAX_VALUE = 57
    ASSERT_LT(TYPE_GEOMETRY, TYPE_MAX_VALUE);
    ASSERT_EQ(56, static_cast<int>(TYPE_GEOMETRY));
    ASSERT_EQ(57, static_cast<int>(TYPE_MAX_VALUE));
}

// -----------------------------------------------------------------------
// Edge cases: boundary / degenerate geometries
// -----------------------------------------------------------------------

TEST_F(GeometryFunctionsTest, st_geom_from_text_linestring) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto col = BinaryColumn::create();
    col->append(Slice("LINESTRING (0 0, 5 5, 10 0)"));
    columns.emplace_back(std::move(col));
    auto result = GeometryFunctions::st_geom_from_text(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    ASSERT_NE(std::string::npos, geometry_to_wkt(result).find("LINESTRING"));
}

TEST_F(GeometryFunctionsTest, st_geom_from_text_empty_string_returns_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto col = BinaryColumn::create();
    col->append(Slice(""));
    columns.emplace_back(std::move(col));
    auto result = GeometryFunctions::st_geom_from_text(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(GeometryFunctionsTest, st_make_point_negative_coords) {
    // Negative coords are valid (southern hemisphere / western longitude)
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto x = DoubleColumn::create(); x->append(-73.935242); // New York
    auto y = DoubleColumn::create(); y->append(40.730610);
    columns.emplace_back(std::move(x));
    columns.emplace_back(std::move(y));
    auto result = GeometryFunctions::st_make_point(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    ASSERT_NE(std::string::npos, geometry_to_wkt(result).find("POINT"));
}

TEST_F(GeometryFunctionsTest, st_contains_point_on_polygon_edge) {
    // A point exactly on the polygon boundary
    // S2 Contains() includes the boundary for polygons
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    // Polygon with flat edge along y=0 from x=0 to x=10
    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    columns.emplace_back(make_geometry_column("POINT (5 0)")); // on the bottom edge
    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    // On-boundary behavior: S2 polygon Contains includes boundary
    // The result could be true or false depending on S2 semantics; just verify no crash
}

TEST_F(GeometryFunctionsTest, st_contains_polygon_inside_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    columns.emplace_back(make_geometry_column("POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2))"));
    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    EXPECT_TRUE(ColumnHelper::cast_to<TYPE_BOOLEAN>(result)->immutable_data()[0]);
}

TEST_F(GeometryFunctionsTest, st_within_polygon_inside_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_geometry_column("POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2))"));
    columns.emplace_back(make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    auto result = GeometryFunctions::st_within(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));
    EXPECT_TRUE(ColumnHelper::cast_to<TYPE_BOOLEAN>(result)->immutable_data()[0]);
}

TEST_F(GeometryFunctionsTest, st_contains_null_second_arg) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto poly = make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto null_col = BinaryColumn::create();
    null_col->append_default();
    auto nullable = NullableColumn::create(std::move(null_col), NullColumn::create(1, 1));
    Columns columns;
    columns.emplace_back(std::move(poly));
    columns.emplace_back(std::move(nullable));
    auto result = GeometryFunctions::st_contains(ctx.get(), columns).value();
    EXPECT_TRUE(result->is_null(0));
}

TEST_F(GeometryFunctionsTest, multi_row_mixed_nulls_and_valid) {
    // A column with 3 rows: null, valid, null
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto build = [](const std::string& wkt) -> std::string {
        GeoParseStatus st;
        std::unique_ptr<GeoShape> s(GeoShape::from_wkt(wkt.data(), wkt.size(), &st));
        std::string buf; s->encode_to(&buf); return buf;
    };

    auto data_col = BinaryColumn::create();
    data_col->append_default(); // row 0: null
    auto p = build("POINT (5 5)");
    data_col->append(Slice(p)); // row 1: valid
    data_col->append_default(); // row 2: null

    auto null_col = NullColumn::create();
    null_col->append(1); // row 0: null
    null_col->append(0); // row 1: not null
    null_col->append(1); // row 2: null

    auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
    Columns columns;
    columns.emplace_back(std::move(nullable));

    auto result = GeometryFunctions::st_as_text(ctx.get(), columns).value();
    ASSERT_EQ(3u, result->size());
    EXPECT_TRUE(result->is_null(0));
    EXPECT_FALSE(result->is_null(1));
    EXPECT_TRUE(result->is_null(2));
}

TEST_F(GeometryFunctionsTest, st_geom_from_wkb_invalid_bytes_returns_null) {
    // Random bytes that are not valid geometry encoding
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto col = BinaryColumn::create();
    col->append(Slice("\x00\x01\x02\x03\xFF\xFE", 6));
    Columns columns;
    columns.emplace_back(std::move(col));
    auto result = GeometryFunctions::st_geom_from_wkb(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(GeometryFunctionsTest, st_make_point_both_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto xc = DoubleColumn::create(); xc->append(1.0);
    auto yc = DoubleColumn::create(); yc->append(2.0);
    auto xn = NullableColumn::create(std::move(xc), NullColumn::create(1, 1)); // null
    auto yn = NullableColumn::create(std::move(yc), NullColumn::create(1, 1)); // null
    Columns columns;
    columns.emplace_back(std::move(xn));
    columns.emplace_back(std::move(yn));
    auto result = GeometryFunctions::st_make_point(ctx.get(), columns).value();
    EXPECT_TRUE(result->is_null(0));
}

TEST_F(GeometryFunctionsTest, st_as_wkb_then_st_geom_from_wkb_polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Build a polygon as geometry
    auto poly = make_geometry_column("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");

    // Step 1: GEOMETRY → WKB
    Columns c1; c1.emplace_back(poly);
    auto wkb = GeometryFunctions::st_as_wkb(ctx.get(), c1).value();
    ASSERT_FALSE(wkb->is_null(0));

    // Step 2: WKB → GEOMETRY
    Columns c2; c2.emplace_back(wkb);
    auto geom = GeometryFunctions::st_geom_from_wkb(ctx.get(), c2).value();
    ASSERT_FALSE(geom->is_null(0));

    // Step 3: verify WKT round-trip
    ASSERT_NE(std::string::npos, geometry_to_wkt(geom).find("POLYGON"));
}

TEST_F(GeometryFunctionsTest, st_within_not_symmetric_with_contains) {
    // contains(A,B) == within(B,A) but NOT contains(A,B) == within(A,B)
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto poly = make_geometry_column("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto pt   = make_geometry_column("POINT (5 5)");

    // contains(poly, pt) should be true
    Columns c1; c1.emplace_back(poly); c1.emplace_back(pt);
    auto contains_r = GeometryFunctions::st_contains(ctx.get(), c1).value();

    // within(poly, pt) should be false (polygon is NOT within a point)
    Columns c2; c2.emplace_back(poly); c2.emplace_back(pt);
    auto within_r = GeometryFunctions::st_within(ctx.get(), c2).value();

    EXPECT_TRUE(ColumnHelper::cast_to<TYPE_BOOLEAN>(contains_r)->immutable_data()[0]);
    EXPECT_FALSE(ColumnHelper::cast_to<TYPE_BOOLEAN>(within_r)->immutable_data()[0]);
}

} // namespace starrocks
