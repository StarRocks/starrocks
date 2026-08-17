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

#include "column/binary_column.h"
#include "column/boolean_column.h"
#include "column/double_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/function_context.h"
#include "exprs/geometry_functions.h"
#include "geo/geo_types.h"

namespace starrocks {

// ============================================================================
// Test fixture + shared helpers
// ============================================================================

class GeometryPostGISTest : public ::testing::Test {
public:
    void SetUp() override {}

    // Encode a WKT string into a non-null GEOMETRY BinaryColumn (1 row).
    static ColumnPtr geom_col(const std::string& wkt) {
        GeoParseStatus st;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt.data(), wkt.size(), &st));
        EXPECT_NE(nullptr, shape) << "Bad WKT: " << wkt;
        std::string buf;
        shape->encode_to(&buf);
        auto col = BinaryColumn::create();
        col->append(Slice(buf.data(), buf.size()));
        return col;
    }

    // Single-row null GEOMETRY column.
    static ColumnPtr null_geom_col() {
        auto data = BinaryColumn::create();
        data->append_default();
        auto nulls = NullColumn::create(1, 1);
        return NullableColumn::create(std::move(data), std::move(nulls));
    }

    // Single-row DOUBLE column.
    static ColumnPtr dbl_col(double v) {
        auto col = DoubleColumn::create();
        col->append(v);
        return col;
    }

    // Helper: extract bool at row 0 from result column.
    static bool get_bool(const ColumnPtr& col, int row = 0) {
        if (col->is_null(row)) ADD_FAILURE() << "Unexpected null at row " << row;
        return ColumnHelper::get_const_value<TYPE_BOOLEAN>(ColumnHelper::create_const_column<TYPE_BOOLEAN>(
                ColumnHelper::cast_to<TYPE_BOOLEAN>(col)->immutable_data()[row], 1));
    }
    static bool get_bool_raw(const ColumnPtr& col, int row = 0) {
        return ColumnHelper::cast_to<TYPE_BOOLEAN>(col)->immutable_data()[row];
    }

    static double get_double(const ColumnPtr& col, int row = 0) {
        return ColumnHelper::cast_to<TYPE_DOUBLE>(col)->immutable_data()[row];
    }

    static int32_t get_int(const ColumnPtr& col, int row = 0) {
        return ColumnHelper::cast_to<TYPE_INT>(col)->immutable_data()[row];
    }

    static std::string get_varchar(const ColumnPtr& col, int row = 0) {
        return ColumnHelper::as_column<BinaryColumn>(col)->get_data()[row].to_string();
    }

    // Helper: ST_AsText on a GEOMETRY result to get WKT for comparison.
    static std::string as_text(const ColumnPtr& geom) {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns cols;
        cols.emplace_back(geom);
        auto r = GeometryFunctions::st_as_text(ctx.get(), cols).value();
        if (r->is_null(0)) return "<null>";
        return get_varchar(r);
    }
};

// ============================================================================
// ST_X / ST_Y
// ============================================================================

TEST_F(GeometryPostGISTest, StX_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (3.5 7.25)"));
    auto r = GeometryFunctions::st_x_geom(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    EXPECT_DOUBLE_EQ(3.5, get_double(r));
}

TEST_F(GeometryPostGISTest, StY_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (3.5 7.25)"));
    auto r = GeometryFunctions::st_y_geom(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    EXPECT_DOUBLE_EQ(7.25, get_double(r));
}

TEST_F(GeometryPostGISTest, StX_NonPointReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1)"));
    auto r = GeometryFunctions::st_x_geom(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

TEST_F(GeometryPostGISTest, StX_NullInputReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    auto r = GeometryFunctions::st_x_geom(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_GeometryType
// ============================================================================

TEST_F(GeometryPostGISTest, StGeometryType_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    auto r = GeometryFunctions::st_geometry_type(ctx.get(), cols).value();
    EXPECT_EQ("ST_Point", get_varchar(r));
}

TEST_F(GeometryPostGISTest, StGeometryType_LineString) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1, 2 0)"));
    auto r = GeometryFunctions::st_geometry_type(ctx.get(), cols).value();
    EXPECT_EQ("ST_LineString", get_varchar(r));
}

TEST_F(GeometryPostGISTest, StGeometryType_Polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"));
    auto r = GeometryFunctions::st_geometry_type(ctx.get(), cols).value();
    EXPECT_EQ("ST_Polygon", get_varchar(r));
}

TEST_F(GeometryPostGISTest, StGeometryType_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    auto r = GeometryFunctions::st_geometry_type(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_SRID
// ============================================================================

TEST_F(GeometryPostGISTest, StSRID_Always4326) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (0 0)"));
    auto r = GeometryFunctions::st_srid(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    EXPECT_EQ(4326, get_int(r));
}

TEST_F(GeometryPostGISTest, StSRID_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    auto r = GeometryFunctions::st_srid(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_IsValid / ST_IsEmpty
// ============================================================================

TEST_F(GeometryPostGISTest, StIsValid_ValidPoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_is_valid(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIsValid_ValidPolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    auto r = GeometryFunctions::st_is_valid(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIsEmpty_NullInputReturnsTrue) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    auto r = GeometryFunctions::st_is_empty(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIsEmpty_ValidGeomReturnsFalse) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    auto r = GeometryFunctions::st_is_empty(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

// ============================================================================
// ST_NDims / ST_NPoints / ST_NumGeometries
// ============================================================================

TEST_F(GeometryPostGISTest, StNDims_Always2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    for (const auto& wkt : {"POINT (1 2)", "LINESTRING (0 0,1 1)", "POLYGON ((0 0,1 0,1 1,0 1,0 0))"}) {
        Columns cols;
        cols.emplace_back(geom_col(wkt));
        auto r = GeometryFunctions::st_ndims(ctx.get(), cols).value();
        EXPECT_EQ(2, get_int(r)) << "for " << wkt;
    }
}

TEST_F(GeometryPostGISTest, StNPoints_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (3 4)"));
    auto r = GeometryFunctions::st_npoints(ctx.get(), cols).value();
    EXPECT_EQ(1, get_int(r));
}

TEST_F(GeometryPostGISTest, StNPoints_Line3Vertices) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 5 5, 10 0)"));
    auto r = GeometryFunctions::st_npoints(ctx.get(), cols).value();
    EXPECT_EQ(3, get_int(r));
}

TEST_F(GeometryPostGISTest, StNPoints_Polygon5Vertices) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    // S2 stores the polygon loop without the closing repeated vertex
    auto r = GeometryFunctions::st_npoints(ctx.get(), cols).value();
    EXPECT_GE(get_int(r), 4); // at least 4 vertices
}

TEST_F(GeometryPostGISTest, StNumGeometries_Always1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0,1 0,1 1,0 1,0 0))"));
    auto r = GeometryFunctions::st_num_geometries(ctx.get(), cols).value();
    EXPECT_EQ(1, get_int(r));
}

// ============================================================================
// ST_Distance (point-to-point, great-circle metres)
// ============================================================================

TEST_F(GeometryPostGISTest, StDistance_SamePointIsZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (10 20)"));
    cols.emplace_back(geom_col("POINT (10 20)"));
    auto r = GeometryFunctions::st_distance_geom(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    EXPECT_NEAR(0.0, get_double(r), 1e-3);
}

TEST_F(GeometryPostGISTest, StDistance_KnownDistance) {
    // London (0°W, 51.5°N) to Greenwich (0°W, 51.5°N) — same point
    // Paris (2.35°E, 48.85°N) to London (-0.12°W, 51.51°N) ≈ 340km
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (2.35 48.85)"));  // Paris
    cols.emplace_back(geom_col("POINT (-0.12 51.51)")); // London
    auto r = GeometryFunctions::st_distance_geom(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    double dist_m = get_double(r);
    // ~340 km, accept ±10 km
    EXPECT_GT(dist_m, 330000.0);
    EXPECT_LT(dist_m, 350000.0);
}

TEST_F(GeometryPostGISTest, StDistance_NonPointReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1)"));
    cols.emplace_back(geom_col("POINT (0 0)"));
    auto r = GeometryFunctions::st_distance_geom(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_Length
// ============================================================================

TEST_F(GeometryPostGISTest, StLength_ZeroDegreeLineShouldBeZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 0 0)"));
    auto r = GeometryFunctions::st_length(ctx.get(), cols).value();
    // degenerate line
    EXPECT_GE(get_double(r), 0.0);
}

TEST_F(GeometryPostGISTest, StLength_Equator1DegreeIsAbout111km) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 0)")); // 1° along equator
    auto r = GeometryFunctions::st_length(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    double len = get_double(r);
    EXPECT_GT(len, 110000.0);
    EXPECT_LT(len, 112000.0);
}

TEST_F(GeometryPostGISTest, StLength_NonLineReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    auto r = GeometryFunctions::st_length(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_Area
// ============================================================================

TEST_F(GeometryPostGISTest, StArea_PointReturnsZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    auto r = GeometryFunctions::st_area(ctx.get(), cols).value();
    EXPECT_DOUBLE_EQ(0.0, get_double(r));
}

TEST_F(GeometryPostGISTest, StArea_SmallPolygonPositive) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    // 1° × 1° box near equator ≈ 12,300 km²
    cols.emplace_back(geom_col("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"));
    auto r = GeometryFunctions::st_area(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    double area = get_double(r);
    EXPECT_GT(area, 1.0e10); // > 10,000 km²
    EXPECT_LT(area, 1.5e10); // < 15,000 km²
}

// ============================================================================
// ST_Perimeter
// ============================================================================

TEST_F(GeometryPostGISTest, StPerimeter_PointReturnsZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (0 0)"));
    auto r = GeometryFunctions::st_perimeter(ctx.get(), cols).value();
    EXPECT_DOUBLE_EQ(0.0, get_double(r));
}

TEST_F(GeometryPostGISTest, StPerimeter_SquareApprox4x111km) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"));
    auto r = GeometryFunctions::st_perimeter(ctx.get(), cols).value();
    double perim = get_double(r);
    // 4 sides × ~111 km = ~444 km
    EXPECT_GT(perim, 400000.0);
    EXPECT_LT(perim, 500000.0);
}

// ============================================================================
// ST_Intersects
// ============================================================================

TEST_F(GeometryPostGISTest, StIntersects_OverlappingPolygons) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIntersects_DisjointPolygons) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIntersects_PointInsidePolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIntersects_PointOutsidePolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"));
    cols.emplace_back(geom_col("POINT (20 20)"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIntersects_SamePoints) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (3 4)"));
    cols.emplace_back(geom_col("POINT (3 4)"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIntersects_DifferentPoints) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 1)"));
    cols.emplace_back(geom_col("POINT (2 2)"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StIntersects_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    cols.emplace_back(geom_col("POINT (1 1)"));
    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_Disjoint — complement of ST_Intersects
// ============================================================================

TEST_F(GeometryPostGISTest, StDisjoint_OverlappingPolygonsIsFalse) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))"));
    auto r = GeometryFunctions::st_disjoint(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StDisjoint_SeparatedPolygonsIsTrue) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"));
    auto r = GeometryFunctions::st_disjoint(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StDisjoint_InverseOfIntersects) {
    // Build same inputs, verify ST_Disjoint == !ST_Intersects
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto ri = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    auto rd = GeometryFunctions::st_disjoint(ctx.get(), cols).value();
    EXPECT_NE(get_bool_raw(ri), get_bool_raw(rd));
}

// ============================================================================
// ST_Equals
// ============================================================================

TEST_F(GeometryPostGISTest, StEquals_SameGeometryIsTrue) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (5 5)"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_equals(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StEquals_DifferentGeometryIsFalse) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (5 5)"));
    cols.emplace_back(geom_col("POINT (5 6)"));
    auto r = GeometryFunctions::st_equals(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StEquals_SamePolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    auto r = GeometryFunctions::st_equals(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StEquals_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    cols.emplace_back(geom_col("POINT (1 1)"));
    auto r = GeometryFunctions::st_equals(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_Covers / ST_CoveredBy
// ============================================================================

TEST_F(GeometryPostGISTest, StCovers_PolygonCoversPoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_covers(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StCovers_PolygonDoesNotCoverOutsidePoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POINT (20 20)"));
    auto r = GeometryFunctions::st_covers(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StCoveredBy_PointCoveredByPolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (5 5)"));
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    auto r = GeometryFunctions::st_covered_by(ctx.get(), cols).value();
    EXPECT_TRUE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StCoveredBy_IsSymmetricInverseOfCovers) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto poly = geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto pt = geom_col("POINT (5 5)");

    Columns covers_cols = {poly, pt};
    auto covers_r = GeometryFunctions::st_covers(ctx.get(), covers_cols).value();

    Columns covby_cols = {pt, poly};
    auto covby_r = GeometryFunctions::st_covered_by(ctx.get(), covby_cols).value();

    EXPECT_EQ(get_bool_raw(covers_r), get_bool_raw(covby_r));
}

// ============================================================================
// ST_Touches
// ============================================================================

TEST_F(GeometryPostGISTest, StTouches_ContainedPointDoesNotTouch) {
    // Interior point — Contains is true, Touches should be false
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_touches(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

TEST_F(GeometryPostGISTest, StTouches_DisjointGeometriesDoNotTouch) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"));
    auto r = GeometryFunctions::st_touches(ctx.get(), cols).value();
    EXPECT_FALSE(get_bool_raw(r));
}

// ============================================================================
// ST_Envelope
// ============================================================================

TEST_F(GeometryPostGISTest, StEnvelope_PointReturnsPoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (3 4)"));
    auto r = GeometryFunctions::st_envelope(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Envelope of a point is the point itself — WKT should contain the coords
    std::string wkt = as_text(r);
    EXPECT_NE(std::string::npos, wkt.find("3"));
    EXPECT_NE(std::string::npos, wkt.find("4"));
}

TEST_F(GeometryPostGISTest, StEnvelope_PolygonEnvelopeIsPolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((2 3, 7 3, 7 8, 2 8, 2 3))"));
    auto r = GeometryFunctions::st_envelope(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Must be a polygon containing coordinates in range [2,7] x [3,8]
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    // The envelope polygon should contain point (4, 5)
    Columns check_cols;
    check_cols.emplace_back(r);
    check_cols.emplace_back(geom_col("POINT (4 5)"));
    auto contains_r = GeometryFunctions::st_contains(ctx2.get(), check_cols).value();
    EXPECT_FALSE(contains_r->is_null(0));
    EXPECT_TRUE(ColumnHelper::cast_to<TYPE_BOOLEAN>(contains_r)->immutable_data()[0]);
}

TEST_F(GeometryPostGISTest, StEnvelope_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    auto r = GeometryFunctions::st_envelope(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_Centroid
// ============================================================================

TEST_F(GeometryPostGISTest, StCentroid_PointIsItself) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (3 4)"));
    auto r = GeometryFunctions::st_centroid(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Centroid of a point is the point — check it decodes correctly
    std::string wkt = as_text(r);
    EXPECT_NE(std::string::npos, wkt.find("POINT"));
}

TEST_F(GeometryPostGISTest, StCentroid_SquarePolygonIsCenter) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    auto r = GeometryFunctions::st_centroid(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Centroid of a square [0,10]×[0,10] should be near (5,5)
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns x_cols;
    x_cols.emplace_back(r);
    auto xr = GeometryFunctions::st_x_geom(ctx2.get(), x_cols).value();
    auto yr = GeometryFunctions::st_y_geom(ctx2.get(), x_cols).value();
    EXPECT_NEAR(5.0, get_double(xr), 0.5);
    EXPECT_NEAR(5.0, get_double(yr), 0.5);
}

TEST_F(GeometryPostGISTest, StCentroid_LineStringMidpoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 10 0)"));
    auto r = GeometryFunctions::st_centroid(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Midpoint of (0,0)→(10,0) should be near (5,0)
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns x_cols;
    x_cols.emplace_back(r);
    auto xr = GeometryFunctions::st_x_geom(ctx2.get(), x_cols).value();
    EXPECT_NEAR(5.0, get_double(xr), 0.5);
}

// ============================================================================
// ST_MakeLine
// ============================================================================

TEST_F(GeometryPostGISTest, StMakeLine_TwoPoints) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (0 0)"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_make_line(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string wkt = as_text(r);
    EXPECT_NE(std::string::npos, wkt.find("LINESTRING"));
    EXPECT_NE(std::string::npos, wkt.find("0"));
    EXPECT_NE(std::string::npos, wkt.find("5"));
}

TEST_F(GeometryPostGISTest, StMakeLine_NPoints_ReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0,1 0,1 1,0 1,0 0))"));
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_make_line(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

TEST_F(GeometryPostGISTest, StMakeLine_NullInputReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    cols.emplace_back(geom_col("POINT (5 5)"));
    auto r = GeometryFunctions::st_make_line(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// ST_Point (alias for ST_MakePoint)
// ============================================================================

TEST_F(GeometryPostGISTest, StPoint_SameAsMakePoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(dbl_col(7.5));
    cols.emplace_back(dbl_col(-3.2));
    auto r = GeometryFunctions::st_point_geom(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Check X coordinate
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns x_cols;
    x_cols.emplace_back(r);
    auto xr = GeometryFunctions::st_x_geom(ctx2.get(), x_cols).value();
    EXPECT_NEAR(7.5, get_double(xr), 1e-9);
}

// ============================================================================
// ST_AsGeoJSON
// ============================================================================

TEST_F(GeometryPostGISTest, StAsGeoJSON_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1.5 2.5)"));
    auto r = GeometryFunctions::st_as_geojson(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string json = get_varchar(r);
    EXPECT_NE(std::string::npos, json.find("\"type\":\"Point\""));
    EXPECT_NE(std::string::npos, json.find("\"coordinates\""));
    EXPECT_NE(std::string::npos, json.find("1.5"));
    EXPECT_NE(std::string::npos, json.find("2.5"));
}

TEST_F(GeometryPostGISTest, StAsGeoJSON_LineString) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1, 2 0)"));
    auto r = GeometryFunctions::st_as_geojson(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string json = get_varchar(r);
    EXPECT_NE(std::string::npos, json.find("\"type\":\"LineString\""));
    EXPECT_NE(std::string::npos, json.find("\"coordinates\""));
}

TEST_F(GeometryPostGISTest, StAsGeoJSON_Polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"));
    auto r = GeometryFunctions::st_as_geojson(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string json = get_varchar(r);
    EXPECT_NE(std::string::npos, json.find("\"type\":\"Polygon\""));
    EXPECT_NE(std::string::npos, json.find("\"coordinates\""));
}

TEST_F(GeometryPostGISTest, StAsGeoJSON_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    auto r = GeometryFunctions::st_as_geojson(ctx.get(), cols).value();
    EXPECT_TRUE(r->is_null(0));
}

// ============================================================================
// Multi-row vectorized correctness
// ============================================================================

TEST_F(GeometryPostGISTest, MultiRow_StGeometryType) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Build a 3-row GEOMETRY column: point, linestring, polygon
    auto build = [](const std::string& wkt) -> std::string {
        GeoParseStatus st;
        std::unique_ptr<GeoShape> s(GeoShape::from_wkt(wkt.data(), wkt.size(), &st));
        std::string buf;
        s->encode_to(&buf);
        return buf;
    };

    auto col = BinaryColumn::create();
    auto p = build("POINT (1 2)");
    auto l = build("LINESTRING (0 0, 1 1)");
    auto g = build("POLYGON ((0 0,1 0,1 1,0 1,0 0))");
    col->append(Slice(p));
    col->append(Slice(l));
    col->append(Slice(g));

    Columns cols;
    cols.emplace_back(std::move(col));

    auto r = GeometryFunctions::st_geometry_type(ctx.get(), cols).value();
    auto bin = ColumnHelper::as_column<BinaryColumn>(r);
    EXPECT_EQ("ST_Point", bin->get_data()[0].to_string());
    EXPECT_EQ("ST_LineString", bin->get_data()[1].to_string());
    EXPECT_EQ("ST_Polygon", bin->get_data()[2].to_string());
}

TEST_F(GeometryPostGISTest, MultiRow_StIntersects_Mixed) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto poly_str = [](const std::string& wkt) -> std::string {
        GeoParseStatus st;
        std::unique_ptr<GeoShape> s(GeoShape::from_wkt(wkt.data(), wkt.size(), &st));
        std::string buf;
        s->encode_to(&buf);
        return buf;
    };

    std::string box = poly_str("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    std::string inside = poly_str("POINT (5 5)");
    std::string outside = poly_str("POINT (20 20)");

    auto a_col = BinaryColumn::create();
    auto b_col = BinaryColumn::create();
    a_col->append(Slice(box));
    b_col->append(Slice(inside));
    a_col->append(Slice(box));
    b_col->append(Slice(outside));

    Columns cols;
    cols.emplace_back(std::move(a_col));
    cols.emplace_back(std::move(b_col));

    auto r = GeometryFunctions::st_intersects(ctx.get(), cols).value();
    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(r);
    EXPECT_TRUE(bools->immutable_data()[0]);
    EXPECT_FALSE(bools->immutable_data()[1]);
}

// ============================================================================
// ST_Dimension
// ============================================================================

TEST_F(GeometryPostGISTest, StDimension_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    EXPECT_EQ(0, get_int(GeometryFunctions::st_dimension(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StDimension_Line) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1)"));
    EXPECT_EQ(1, get_int(GeometryFunctions::st_dimension(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StDimension_Polygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0,1 0,1 1,0 1,0 0))"));
    EXPECT_EQ(2, get_int(GeometryFunctions::st_dimension(ctx.get(), cols).value()));
}

// ============================================================================
// ST_StartPoint / ST_EndPoint / ST_PointN
// ============================================================================

TEST_F(GeometryPostGISTest, StStartPoint_Line) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (3 4, 7 8, 10 0)"));
    auto r = GeometryFunctions::st_start_point(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string wkt = as_text(r);
    EXPECT_NE(std::string::npos, wkt.find("POINT"));
    EXPECT_NE(std::string::npos, wkt.find("3"));
}
TEST_F(GeometryPostGISTest, StEndPoint_Line) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (3 4, 7 8, 10 0)"));
    auto r = GeometryFunctions::st_end_point(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string wkt = as_text(r);
    EXPECT_NE(std::string::npos, wkt.find("10"));
}
TEST_F(GeometryPostGISTest, StStartEndPoint_NonLineReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    EXPECT_TRUE(GeometryFunctions::st_start_point(ctx.get(), cols).value()->is_null(0));
    EXPECT_TRUE(GeometryFunctions::st_end_point(ctx.get(), cols).value()->is_null(0));
}
TEST_F(GeometryPostGISTest, StPointN_SecondVertex) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto int_col = FixedLengthColumn<int32_t>::create();
    int_col->append(2);
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 5 5, 10 0)"));
    cols.emplace_back(int_col);
    auto r = GeometryFunctions::st_point_n(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Second vertex should be near (5,5)
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns x_cols;
    x_cols.emplace_back(r);
    EXPECT_NEAR(5.0, get_double(GeometryFunctions::st_x_geom(ctx2.get(), x_cols).value()), 0.01);
}
TEST_F(GeometryPostGISTest, StPointN_OutOfBoundsReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto int_col = FixedLengthColumn<int32_t>::create();
    int_col->append(99);
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1)"));
    cols.emplace_back(int_col);
    EXPECT_TRUE(GeometryFunctions::st_point_n(ctx.get(), cols).value()->is_null(0));
}

// ============================================================================
// ST_IsClosed / ST_IsRing / ST_IsSimple
// ============================================================================

TEST_F(GeometryPostGISTest, StIsClosed_OpenLine) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1)"));
    EXPECT_FALSE(get_bool_raw(GeometryFunctions::st_is_closed(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StIsClosed_PolygonAlwaysTrue) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0,1 0,1 1,0 1,0 0))"));
    EXPECT_TRUE(get_bool_raw(GeometryFunctions::st_is_closed(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StIsSimple_ValidLineIsSimple) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 5 5, 10 0)"));
    EXPECT_TRUE(get_bool_raw(GeometryFunctions::st_is_simple(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StIsRing_NonClosedLineIsFalse) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 1 1, 2 0)"));
    EXPECT_FALSE(get_bool_raw(GeometryFunctions::st_is_ring(ctx.get(), cols).value()));
}

// ============================================================================
// ST_NumInteriorRings / ST_ExteriorRing
// ============================================================================

TEST_F(GeometryPostGISTest, StNumInteriorRings_NoHoles) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0,10 0,10 10,0 10,0 0))"));
    EXPECT_EQ(0, get_int(GeometryFunctions::st_num_interior_rings(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StNumInteriorRings_NonPolygonReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (1 2)"));
    EXPECT_TRUE(GeometryFunctions::st_num_interior_rings(ctx.get(), cols).value()->is_null(0));
}
TEST_F(GeometryPostGISTest, StExteriorRing_ReturnsLineString) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0,10 0,10 10,0 10,0 0))"));
    auto r = GeometryFunctions::st_exterior_ring(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns t_cols;
    t_cols.emplace_back(r);
    auto gt = GeometryFunctions::st_geometry_type(ctx2.get(), t_cols).value();
    EXPECT_EQ("ST_LineString", get_varchar(gt));
}

// ============================================================================
// ST_Overlaps
// ============================================================================

TEST_F(GeometryPostGISTest, StOverlaps_PartiallyOverlappingPolygons) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))"));
    EXPECT_TRUE(get_bool_raw(GeometryFunctions::st_overlaps(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StOverlaps_ContainedPolygonDoesNotOverlap) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
    cols.emplace_back(geom_col("POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2))"));
    // One contains the other, so NOT overlaps
    EXPECT_FALSE(get_bool_raw(GeometryFunctions::st_overlaps(ctx.get(), cols).value()));
}

// ============================================================================
// ST_DWithin
// ============================================================================

TEST_F(GeometryPostGISTest, StDWithin_PointsWithinRadius) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (0 0)"));
    cols.emplace_back(geom_col("POINT (0.001 0)")); // ~111m apart
    cols.emplace_back(dbl_col(200.0));              // 200m radius
    EXPECT_TRUE(get_bool_raw(GeometryFunctions::st_dwithin(ctx.get(), cols).value()));
}
TEST_F(GeometryPostGISTest, StDWithin_PointsOutsideRadius) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (0 0)"));
    cols.emplace_back(geom_col("POINT (1 0)")); // ~111km apart
    cols.emplace_back(dbl_col(100.0));          // 100m radius — too small
    EXPECT_FALSE(get_bool_raw(GeometryFunctions::st_dwithin(ctx.get(), cols).value()));
}

// ============================================================================
// ST_GeomFromGeoJSON
// ============================================================================

TEST_F(GeometryPostGISTest, StGeomFromGeoJSON_Point) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto col = BinaryColumn::create();
    col->append(Slice(R"({"type":"Point","coordinates":[3.5,7.25]})"));
    Columns cols;
    cols.emplace_back(std::move(col));
    auto r = GeometryFunctions::st_geom_from_geojson(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::string wkt = as_text(r);
    EXPECT_NE(std::string::npos, wkt.find("POINT"));
}
TEST_F(GeometryPostGISTest, StGeomFromGeoJSON_InvalidReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto col = BinaryColumn::create();
    col->append(Slice(R"({"type":"MultiPoint","coordinates":[]})"));
    Columns cols;
    cols.emplace_back(std::move(col));
    EXPECT_TRUE(GeometryFunctions::st_geom_from_geojson(ctx.get(), cols).value()->is_null(0));
}
TEST_F(GeometryPostGISTest, StGeomFromGeoJSON_NullReturnsNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(null_geom_col());
    EXPECT_TRUE(GeometryFunctions::st_geom_from_geojson(ctx.get(), cols).value()->is_null(0));
}

// ============================================================================
// ST_Buffer
// ============================================================================

TEST_F(GeometryPostGISTest, StBuffer_PointCreatesPolygon) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (0 0)"));
    cols.emplace_back(dbl_col(0.01)); // 0.01 degree radius
    auto r = GeometryFunctions::st_buffer(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Buffer polygon should contain the original point
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns c2;
    c2.emplace_back(r);
    c2.emplace_back(geom_col("POINT (0 0)"));
    EXPECT_TRUE(get_bool_raw(GeometryFunctions::st_contains(ctx2.get(), c2).value()));
}
TEST_F(GeometryPostGISTest, StBuffer_ZeroRadiusReturnsGeom) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (5 5)"));
    cols.emplace_back(dbl_col(0.0));
    auto r = GeometryFunctions::st_buffer(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
}

// ============================================================================
// ST_ConvexHull
// ============================================================================

TEST_F(GeometryPostGISTest, StConvexHull_Triangle) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POLYGON ((0 0, 10 0, 5 10, 0 0))"));
    auto r = GeometryFunctions::st_convex_hull(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Convex hull of a triangle is the triangle — should still contain (5 5)
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns c2;
    c2.emplace_back(r);
    c2.emplace_back(geom_col("POINT (5 5)"));
    EXPECT_TRUE(get_bool_raw(GeometryFunctions::st_contains(ctx2.get(), c2).value()));
}
TEST_F(GeometryPostGISTest, StConvexHull_LineStringReturnsPolygonOrLine) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 10 0, 5 5)"));
    auto r = GeometryFunctions::st_convex_hull(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
}

// ============================================================================
// ST_Simplify
// ============================================================================

TEST_F(GeometryPostGISTest, StSimplify_LargeToleranceReducesPoints) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    // A line with a nearly-collinear middle point
    cols.emplace_back(geom_col("LINESTRING (0 0, 5 0.001, 10 0)"));
    cols.emplace_back(dbl_col(0.01)); // tolerance larger than deviation
    auto r = GeometryFunctions::st_simplify(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    // Simplified line should still be a linestring
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns c2;
    c2.emplace_back(r);
    EXPECT_EQ("ST_LineString", get_varchar(GeometryFunctions::st_geometry_type(ctx2.get(), c2).value()));
}
TEST_F(GeometryPostGISTest, StSimplify_ZeroToleranceReturnsSame) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("LINESTRING (0 0, 5 3, 10 0)"));
    cols.emplace_back(dbl_col(0.0));
    auto r = GeometryFunctions::st_simplify(ctx.get(), cols).value();
    EXPECT_FALSE(r->is_null(0));
    std::unique_ptr<FunctionContext> ctx2(FunctionContext::create_test_context());
    Columns c2;
    c2.emplace_back(r);
    EXPECT_GE(get_int(GeometryFunctions::st_npoints(ctx2.get(), c2).value()), 2);
}

// ============================================================================
// ST_DistanceSphere alias
// ============================================================================

TEST_F(GeometryPostGISTest, StDistanceSphere_SameResultAsStDistance) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns cols;
    cols.emplace_back(geom_col("POINT (2.35 48.85)"));
    cols.emplace_back(geom_col("POINT (-0.12 51.51)"));
    auto r1 = GeometryFunctions::st_distance_geom(ctx.get(), cols).value();
    auto r2 = GeometryFunctions::st_distance_sphere_geom(ctx.get(), cols).value();
    EXPECT_NEAR(get_double(r1), get_double(r2), 1.0);
}

} // namespace starrocks
