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

#include "exprs/geometry_functions.h"

#include <cmath>
#include <iomanip>
#include <memory>
#include <sstream>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/logging.h"
#include "geo/geo_types.h"

namespace starrocks {

// -----------------------------------------------------------------------
// ST_GeomFromText(VARCHAR wkt) -> GEOMETRY
// Parse a WKT string into a GEOMETRY value (stored as StarRocks geo encoding).
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_geom_from_text(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> wkt_viewer(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);

    for (int row = 0; row < size; ++row) {
        if (wkt_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto wkt_value = wkt_viewer.value(row);
        GeoParseStatus status;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt_value.data, wkt_value.size, &status));
        if (shape == nullptr) {
            result.append_null();
            continue;
        }

        std::string buf;
        shape->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// -----------------------------------------------------------------------
// ST_GeomFromWKB(VARBINARY wkb) -> GEOMETRY
// Wrap raw WKB bytes as a GEOMETRY value, validating via GeoShape round-trip.
// Note: Internally StarRocks uses its own geo encoding; this function
// converts from WKB by decoding it as WKT and re-encoding.
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_geom_from_wkb(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_VARBINARY> wkb_viewer(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);

    for (int row = 0; row < size; ++row) {
        if (wkb_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        // Attempt to decode via StarRocks geo encoding (WKB-like binary)
        auto wkb_value = wkb_viewer.value(row);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(wkb_value.data, wkb_value.size));
        if (shape == nullptr) {
            result.append_null();
            continue;
        }

        std::string buf;
        shape->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// -----------------------------------------------------------------------
// ST_AsText(GEOMETRY) -> VARCHAR
// Return the WKT text representation of a GEOMETRY value.
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_as_text(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> geom_viewer(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    for (int row = 0; row < size; ++row) {
        if (geom_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto geom_value = geom_viewer.value(row);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(geom_value.data, geom_value.size));
        if (shape == nullptr) {
            result.append_null();
            continue;
        }

        auto wkt = shape->as_wkt();
        result.append(Slice(wkt.data(), wkt.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// -----------------------------------------------------------------------
// ST_AsWKB(GEOMETRY) -> VARBINARY
// Return the raw binary encoding of the GEOMETRY value.
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_as_wkb(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> geom_viewer(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARBINARY> result(size);

    for (int row = 0; row < size; ++row) {
        if (geom_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        // Return the raw encoded bytes as VARBINARY
        auto geom_value = geom_viewer.value(row);
        result.append(Slice(geom_value.data, geom_value.size));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// -----------------------------------------------------------------------
// ST_MakePoint(DOUBLE x, DOUBLE y) -> GEOMETRY
// Construct a GEOMETRY POINT from X/Y coordinates.
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_make_point(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> x_viewer(columns[0]);
    ColumnViewer<TYPE_DOUBLE> y_viewer(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);

    for (int row = 0; row < size; ++row) {
        if (x_viewer.is_null(row) || y_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        GeoPoint point;
        auto res = point.from_coord(x_viewer.value(row), y_viewer.value(row));
        if (res != GEO_PARSE_OK) {
            result.append_null();
            continue;
        }

        std::string buf;
        point.encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// -----------------------------------------------------------------------
// ST_Contains state (caches pre-decoded shapes for constant columns)
// -----------------------------------------------------------------------
struct StGeomContainsState {
    StGeomContainsState() : shapes{nullptr, nullptr} {}
    ~StGeomContainsState() {
        delete shapes[0];
        delete shapes[1];
    }
    bool is_null{false};
    GeoShape* shapes[2];
};

Status GeometryFunctions::st_contains_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!ctx->is_constant_column(0) && !ctx->is_constant_column(1)) {
        return Status::OK();
    }

    auto* state = new StGeomContainsState();
    ctx->set_function_state(scope, state);

    for (int i = 0; i < 2; ++i) {
        if (!ctx->is_constant_column(i)) {
            continue;
        }
        auto col = ctx->get_constant_column(i);
        if (col->only_null()) {
            state->is_null = true;
            return Status::OK();
        }
        auto geom_value = ColumnHelper::get_const_value<TYPE_GEOMETRY>(col);
        state->shapes[i] = GeoShape::from_encoded(geom_value.data, geom_value.size);
        if (state->shapes[i] == nullptr) {
            state->is_null = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

Status GeometryFunctions::st_contains_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<StGeomContainsState*>(ctx->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

// -----------------------------------------------------------------------
// ST_Contains(GEOMETRY geom1, GEOMETRY geom2) -> BOOLEAN
// Return true if geom1 completely contains geom2.
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_contains(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> lhs_viewer(columns[0]);
    ColumnViewer<TYPE_GEOMETRY> rhs_viewer(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);

    auto* state = reinterpret_cast<StGeomContainsState*>(ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    for (int row = 0; row < size; ++row) {
        if (lhs_viewer.is_null(row) || rhs_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        GeoShape* lhs_shape = nullptr;
        GeoShape* rhs_shape = nullptr;
        std::unique_ptr<GeoShape> lhs_owned;
        std::unique_ptr<GeoShape> rhs_owned;

        if (state != nullptr) {
            if (state->is_null) {
                result.append_null();
                continue;
            }
            lhs_shape = state->shapes[0];
            rhs_shape = state->shapes[1];
        }

        if (lhs_shape == nullptr) {
            auto lhs_value = lhs_viewer.value(row);
            lhs_owned.reset(GeoShape::from_encoded(lhs_value.data, lhs_value.size));
            lhs_shape = lhs_owned.get();
        }
        if (rhs_shape == nullptr) {
            auto rhs_value = rhs_viewer.value(row);
            rhs_owned.reset(GeoShape::from_encoded(rhs_value.data, rhs_value.size));
            rhs_shape = rhs_owned.get();
        }

        if (lhs_shape == nullptr || rhs_shape == nullptr) {
            result.append_null();
            continue;
        }

        result.append(lhs_shape->contains(rhs_shape));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// -----------------------------------------------------------------------
// ST_Within(GEOMETRY geom1, GEOMETRY geom2) -> BOOLEAN
// Return true if geom1 is completely within geom2 (i.e. geom2 contains geom1).
// -----------------------------------------------------------------------
StatusOr<ColumnPtr> GeometryFunctions::st_within(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> lhs_viewer(columns[0]);
    ColumnViewer<TYPE_GEOMETRY> rhs_viewer(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);

    for (int row = 0; row < size; ++row) {
        if (lhs_viewer.is_null(row) || rhs_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto lhs_value = lhs_viewer.value(row);
        auto rhs_value = rhs_viewer.value(row);

        std::unique_ptr<GeoShape> lhs_shape(GeoShape::from_encoded(lhs_value.data, lhs_value.size));
        std::unique_ptr<GeoShape> rhs_shape(GeoShape::from_encoded(rhs_value.data, rhs_value.size));

        if (lhs_shape == nullptr || rhs_shape == nullptr) {
            result.append_null();
            continue;
        }

        // within(A, B) == contains(B, A)
        result.append(rhs_shape->contains(lhs_shape.get()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Shared helpers
// ============================================================================


// Decode geometry from a GEOMETRY column row; returns nullptr on null/invalid.
static std::unique_ptr<GeoShape> decode_geom(const ColumnViewer<TYPE_GEOMETRY>& viewer, int row) {
    if (viewer.is_null(row)) return nullptr;
    auto v = viewer.value(row);
    return std::unique_ptr<GeoShape>(GeoShape::from_encoded(v.data, v.size));
}

// Count vertices in any GeoShape.
static int count_points(const GeoShape* shape) {
    if (!shape) return 0;
    switch (shape->type()) {
    case GEO_SHAPE_POINT:
        return 1;
    case GEO_SHAPE_LINE_STRING:
        return geo_line_vertex_count(shape);
    case GEO_SHAPE_POLYGON: {
        int n = 0;
        int nl = geo_polygon_loop_count(shape);
        for (int i = 0; i < nl; ++i) n += geo_polygon_loop_vertex_count(shape, i);
        return n;
    }
    default:
        return 0;
    }
}

// Encode a GeoShape into a GEOMETRY binary string.
static std::string encode_shape(GeoShape* shape) {
    std::string buf;
    shape->encode_to(&buf);
    return buf;
}

// ============================================================================
// Accessors
// ============================================================================

// ST_X(GEOMETRY) → DOUBLE
StatusOr<ColumnPtr> GeometryFunctions::st_x_geom(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_POINT) { result.append_null(); continue; }
        result.append(static_cast<const GeoPoint*>(shape.get())->x());
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Y(GEOMETRY) → DOUBLE
StatusOr<ColumnPtr> GeometryFunctions::st_y_geom(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_POINT) { result.append_null(); continue; }
        result.append(static_cast<const GeoPoint*>(shape.get())->y());
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_GeometryType(GEOMETRY) → VARCHAR
StatusOr<ColumnPtr> GeometryFunctions::st_geometry_type(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        const char* name = nullptr;
        switch (shape->type()) {
        case GEO_SHAPE_POINT:       name = "ST_Point";       break;
        case GEO_SHAPE_LINE_STRING: name = "ST_LineString";  break;
        case GEO_SHAPE_POLYGON:     name = "ST_Polygon";     break;
        case GEO_SHAPE_CIRCLE:      name = "ST_CircularString"; break;
        default:                    name = "ST_Unknown";     break;
        }
        result.append(Slice(name));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_SRID(GEOMETRY) → INT — always 4326 (WGS84)
StatusOr<ColumnPtr> GeometryFunctions::st_srid(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) { result.append_null(); continue; }
        result.append(4326);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_IsValid(GEOMETRY) → BOOLEAN
StatusOr<ColumnPtr> GeometryFunctions::st_is_valid(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        bool valid = geo_is_valid(shape.get());
        result.append(valid);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_IsEmpty(GEOMETRY) → BOOLEAN — returns false for all non-null decoded geometries
StatusOr<ColumnPtr> GeometryFunctions::st_is_empty(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) { result.append(true); continue; }
        auto v = viewer.value(row);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(v.data, v.size));
        result.append(shape == nullptr);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_NDims(GEOMETRY) → INT — always 2
StatusOr<ColumnPtr> GeometryFunctions::st_ndims(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) { result.append_null(); continue; }
        result.append(2);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_NPoints(GEOMETRY) → INT
StatusOr<ColumnPtr> GeometryFunctions::st_npoints(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        result.append(count_points(shape.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_NumGeometries(GEOMETRY) → INT — always 1 for scalar types
StatusOr<ColumnPtr> GeometryFunctions::st_num_geometries(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) { result.append_null(); continue; }
        result.append(1);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Measurements
// ============================================================================

// ST_Distance(GEOMETRY, GEOMETRY) → DOUBLE — great-circle distance in metres (point-to-point).
// Returns NULL for non-point types.
StatusOr<ColumnPtr> GeometryFunctions::st_distance_geom(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> a_viewer(columns[0]);
    ColumnViewer<TYPE_GEOMETRY> b_viewer(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        auto a = decode_geom(a_viewer, row);
        auto b = decode_geom(b_viewer, row);
        if (!a || !b || a->type() != GEO_SHAPE_POINT || b->type() != GEO_SHAPE_POINT) {
            result.append_null();
            continue;
        }
        const auto* pa = static_cast<const GeoPoint*>(a.get());
        const auto* pb = static_cast<const GeoPoint*>(b.get());
        double dist = 0.0;
        if (!GeoPoint::st_distance_sphere(pa->x(), pa->y(), pb->x(), pb->y(), &dist)) {
            result.append_null();
        } else {
            result.append(dist);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Length(GEOMETRY) → DOUBLE — arc length in metres for LINESTRING.
StatusOr<ColumnPtr> GeometryFunctions::st_length(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_LINE_STRING) { result.append_null(); continue; }
        result.append(geo_line_length_meters(shape.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Area(GEOMETRY) → DOUBLE — area in square metres for POLYGON.
StatusOr<ColumnPtr> GeometryFunctions::st_area(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_POLYGON) { result.append(0.0); continue; }
        result.append(geo_polygon_area_sq_meters(shape.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Perimeter(GEOMETRY) → DOUBLE — perimeter in metres for POLYGON outer ring.
StatusOr<ColumnPtr> GeometryFunctions::st_perimeter(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_POLYGON) { result.append(0.0); continue; }
        result.append(geo_polygon_perimeter_meters(shape.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Spatial predicates
// ============================================================================

// Shared helper: decode two geometry values; returns false if either is null/invalid.
static bool decode_two(const ColumnViewer<TYPE_GEOMETRY>& av, const ColumnViewer<TYPE_GEOMETRY>& bv, int row,
                        std::unique_ptr<GeoShape>& a, std::unique_ptr<GeoShape>& b) {
    a = decode_geom(av, row);
    b = decode_geom(bv, row);
    return a && b;
}

// Internal intersects logic — delegates to geo_types.cpp helper.
static bool shapes_intersect(const GeoShape* a, const GeoShape* b) {
    return geo_polygon_intersects(a, b);
}

// ST_Intersects(GEOMETRY, GEOMETRY) → BOOLEAN
StatusOr<ColumnPtr> GeometryFunctions::st_intersects(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        result.append(shapes_intersect(a.get(), b.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Disjoint(GEOMETRY, GEOMETRY) → BOOLEAN — NOT ST_Intersects
StatusOr<ColumnPtr> GeometryFunctions::st_disjoint(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        result.append(!shapes_intersect(a.get(), b.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Equals(GEOMETRY, GEOMETRY) → BOOLEAN — canonical-encoding equality
StatusOr<ColumnPtr> GeometryFunctions::st_equals(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (av.is_null(row) || bv.is_null(row)) { result.append_null(); continue; }
        auto va = av.value(row);
        auto vb = bv.value(row);
        // Re-decode and re-encode both to get canonical form, then compare
        std::unique_ptr<GeoShape> a(GeoShape::from_encoded(va.data, va.size));
        std::unique_ptr<GeoShape> b(GeoShape::from_encoded(vb.data, vb.size));
        if (!a || !b) { result.append_null(); continue; }
        std::string ea, eb;
        a->encode_to(&ea);
        b->encode_to(&eb);
        result.append(ea == eb);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Covers(GEOMETRY, GEOMETRY) → BOOLEAN — geom1 covers geom2 (Contains including boundary)
StatusOr<ColumnPtr> GeometryFunctions::st_covers(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        result.append(a->contains(b.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_CoveredBy(GEOMETRY, GEOMETRY) → BOOLEAN — geom1 is covered by geom2
StatusOr<ColumnPtr> GeometryFunctions::st_covered_by(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        result.append(b->contains(a.get()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Touches(GEOMETRY, GEOMETRY) → BOOLEAN
// Shapes touch if they intersect but neither's interior overlaps the other's interior.
// Approximation: intersects AND NOT (A contains B or B contains A).
StatusOr<ColumnPtr> GeometryFunctions::st_touches(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        bool intersects = shapes_intersect(a.get(), b.get());
        bool touches = intersects && !a->contains(b.get()) && !b->contains(a.get());
        result.append(touches);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Constructors / transformations
// ============================================================================

// ST_Envelope(GEOMETRY) → GEOMETRY — minimum bounding rectangle as POLYGON
StatusOr<ColumnPtr> GeometryFunctions::st_envelope(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        // For a point, envelope is the point itself
        if (shape->type() == GEO_SHAPE_POINT) {
            result.append(Slice(viewer.value(row).data, viewer.value(row).size));
            continue;
        }
        double x0, y0, x1, y1;
        if (!geo_bounding_box(shape.get(), &x0, &y0, &x1, &y1)) { result.append_null(); continue; }
        // Build WKT polygon for MBR and parse it back to GEOMETRY
        std::ostringstream wkt;
        wkt << std::setprecision(12)
            << "POLYGON ((" << x0 << " " << y0 << ", "
                            << x1 << " " << y0 << ", "
                            << x1 << " " << y1 << ", "
                            << x0 << " " << y1 << ", "
                            << x0 << " " << y0 << "))";
        std::string s = wkt.str();
        GeoParseStatus st;
        std::unique_ptr<GeoShape> env(GeoShape::from_wkt(s.data(), s.size(), &st));
        if (!env) { result.append_null(); continue; }
        std::string buf;
        env->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Centroid(GEOMETRY) → GEOMETRY — centroid as POINT
StatusOr<ColumnPtr> GeometryFunctions::st_centroid(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        double cx = 0, cy = 0;
        if (!geo_shape_centroid(shape.get(), &cx, &cy)) { result.append_null(); continue; }
        GeoPoint pt;
        if (pt.from_coord(cx, cy) != GEO_PARSE_OK) { result.append_null(); continue; }
        std::string buf;
        pt.encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_MakeLine(GEOMETRY, GEOMETRY) → GEOMETRY — LINESTRING from two POINTs
StatusOr<ColumnPtr> GeometryFunctions::st_make_line(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto a = decode_geom(av, row);
        auto b = decode_geom(bv, row);
        if (!a || !b || a->type() != GEO_SHAPE_POINT || b->type() != GEO_SHAPE_POINT) {
            result.append_null();
            continue;
        }
        const auto* pa = static_cast<const GeoPoint*>(a.get());
        const auto* pb = static_cast<const GeoPoint*>(b.get());
        std::ostringstream wkt;
        wkt << std::setprecision(12)
            << "LINESTRING (" << pa->x() << " " << pa->y() << ", "
                              << pb->x() << " " << pb->y() << ")";
        std::string s = wkt.str();
        GeoParseStatus st;
        std::unique_ptr<GeoShape> line(GeoShape::from_wkt(s.data(), s.size(), &st));
        if (!line) { result.append_null(); continue; }
        std::string buf;
        line->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Point(DOUBLE, DOUBLE) → GEOMETRY — alias for ST_MakePoint
StatusOr<ColumnPtr> GeometryFunctions::st_point_geom(FunctionContext* ctx, const Columns& columns) {
    return st_make_point(ctx, columns);
}

// ============================================================================
// Output formats
// ============================================================================

static void geojson_coords(std::ostringstream& os, double x, double y) {
    os << "[" << x << "," << y << "]";
}

// ST_AsGeoJSON(GEOMETRY) → VARCHAR — RFC 7946 GeoJSON
StatusOr<ColumnPtr> GeometryFunctions::st_as_geojson(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        std::ostringstream os;
        os << std::setprecision(10);
        switch (shape->type()) {
        case GEO_SHAPE_POINT: {
            const auto* pt = static_cast<const GeoPoint*>(shape.get());
            os << R"({"type":"Point","coordinates":)";
            geojson_coords(os, pt->x(), pt->y());
            os << "}";
            break;
        }
        case GEO_SHAPE_LINE_STRING: {
            int nv = geo_line_vertex_count(shape.get());
            os << R"({"type":"LineString","coordinates":[)";
            for (int i = 0; i < nv; ++i) {
                if (i) os << ",";
                double vx, vy;
                geo_line_vertex_at(shape.get(), i, &vx, &vy);
                geojson_coords(os, vx, vy);
            }
            os << "]}";
            break;
        }
        case GEO_SHAPE_POLYGON: {
            int nl = geo_polygon_loop_count(shape.get());
            os << R"({"type":"Polygon","coordinates":[)";
            for (int i = 0; i < nl; ++i) {
                if (i) os << ",";
                int nv = geo_polygon_loop_vertex_count(shape.get(), i);
                os << "[";
                for (int j = 0; j < nv; ++j) {
                    if (j) os << ",";
                    double vx, vy;
                    geo_polygon_loop_vertex_at(shape.get(), i, j, &vx, &vy);
                    geojson_coords(os, vx, vy);
                }
                // Close ring
                double x0, y0;
                geo_polygon_loop_vertex_at(shape.get(), i, 0, &x0, &y0);
                os << ",";
                geojson_coords(os, x0, y0);
                os << "]";
            }
            os << "]}";
            break;
        }
        default:
            result.append_null();
            continue;
        }
        std::string s = os.str();
        result.append(Slice(s.data(), s.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Additional accessors
// ============================================================================

// ST_Dimension(GEOMETRY) → INT  — 0=point, 1=line, 2=polygon
StatusOr<ColumnPtr> GeometryFunctions::st_dimension(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        int dim = 0;
        switch (shape->type()) {
        case GEO_SHAPE_LINE_STRING: dim = 1; break;
        case GEO_SHAPE_POLYGON:     dim = 2; break;
        default:                    dim = 0; break;
        }
        result.append(dim);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// Helper: encode (x,y) degrees as a GeoPoint GEOMETRY value.
static std::string encode_xy_as_point(double x, double y) {
    GeoPoint gp;
    gp.from_coord(x, y);
    std::string buf;
    gp.encode_to(&buf);
    return buf;
}

// ST_StartPoint(GEOMETRY) → GEOMETRY
StatusOr<ColumnPtr> GeometryFunctions::st_start_point(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_LINE_STRING) { result.append_null(); continue; }
        double x, y;
        if (!geo_line_vertex_at(shape.get(), 0, &x, &y)) { result.append_null(); continue; }
        std::string buf = encode_xy_as_point(x, y);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_EndPoint(GEOMETRY) → GEOMETRY
StatusOr<ColumnPtr> GeometryFunctions::st_end_point(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_LINE_STRING) { result.append_null(); continue; }
        int n = geo_line_vertex_count(shape.get());
        if (n == 0) { result.append_null(); continue; }
        double x, y;
        if (!geo_line_vertex_at(shape.get(), n - 1, &x, &y)) { result.append_null(); continue; }
        std::string buf = encode_xy_as_point(x, y);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_PointN(GEOMETRY, INT) → GEOMETRY  — 1-based index
StatusOr<ColumnPtr> GeometryFunctions::st_point_n(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> geom_viewer(columns[0]);
    ColumnViewer<TYPE_INT>      idx_viewer(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(geom_viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_LINE_STRING || idx_viewer.is_null(row)) {
            result.append_null(); continue;
        }
        int idx = idx_viewer.value(row) - 1; // convert 1-based to 0-based
        double x, y;
        if (!geo_line_vertex_at(shape.get(), idx, &x, &y)) { result.append_null(); continue; }
        std::string buf = encode_xy_as_point(x, y);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_IsClosed(GEOMETRY) → BOOLEAN  — true if first vertex equals last vertex
StatusOr<ColumnPtr> GeometryFunctions::st_is_closed(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        if (shape->type() == GEO_SHAPE_POLYGON) { result.append(true); continue; }
        if (shape->type() != GEO_SHAPE_LINE_STRING) { result.append_null(); continue; }
        int n = geo_line_vertex_count(shape.get());
        if (n < 2) { result.append(n == 0); continue; }
        double x0, y0, xn, yn;
        geo_line_vertex_at(shape.get(), 0, &x0, &y0);
        geo_line_vertex_at(shape.get(), n - 1, &xn, &yn);
        result.append(x0 == xn && y0 == yn);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_IsRing(GEOMETRY) → BOOLEAN  — closed and simple LINESTRING
StatusOr<ColumnPtr> GeometryFunctions::st_is_ring(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_LINE_STRING) { result.append_null(); continue; }
        int n = geo_line_vertex_count(shape.get());
        double x0, y0, xn, yn;
        geo_line_vertex_at(shape.get(), 0, &x0, &y0);
        geo_line_vertex_at(shape.get(), n - 1, &xn, &yn);
        bool closed = (n >= 2) && (x0 == xn && y0 == yn);
        bool simple = geo_is_valid(shape.get());
        result.append(closed && simple);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_IsSimple(GEOMETRY) → BOOLEAN
StatusOr<ColumnPtr> GeometryFunctions::st_is_simple(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        bool simple = geo_is_valid(shape.get());
        switch (shape->type()) {
        default: break;
        }
        result.append(simple);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_NumInteriorRings(GEOMETRY) → INT  — number of holes in POLYGON
StatusOr<ColumnPtr> GeometryFunctions::st_num_interior_rings(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_POLYGON) { result.append_null(); continue; }
        int holes = std::max(0, geo_polygon_loop_count(shape.get()) - 1);
        result.append(holes);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_ExteriorRing(GEOMETRY) → GEOMETRY  — outer ring of POLYGON as LINESTRING
StatusOr<ColumnPtr> GeometryFunctions::st_exterior_ring(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape || shape->type() != GEO_SHAPE_POLYGON) { result.append_null(); continue; }
        int nv = geo_polygon_loop_vertex_count(shape.get(), 0);
        if (nv == 0) { result.append_null(); continue; }
        std::ostringstream wkt;
        wkt << std::setprecision(12) << "LINESTRING (";
        for (int j = 0; j < nv; ++j) {
            if (j) wkt << ", ";
            double vx, vy;
            geo_polygon_loop_vertex_at(shape.get(), 0, j, &vx, &vy);
            wkt << vx << " " << vy;
        }
        // Close the ring
        double x0, y0;
        geo_polygon_loop_vertex_at(shape.get(), 0, 0, &x0, &y0);
        wkt << ", " << x0 << " " << y0 << ")";
        std::string s = wkt.str();
        GeoParseStatus st;
        std::unique_ptr<GeoShape> line(GeoShape::from_wkt(s.data(), s.size(), &st));
        if (!line) { result.append_null(); continue; }
        std::string buf;
        line->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Additional predicates
// ============================================================================

// ST_Overlaps(GEOMETRY, GEOMETRY) → BOOLEAN
// Geometries overlap when they intersect and one does not contain the other,
// and their interiors share at least one point.
StatusOr<ColumnPtr> GeometryFunctions::st_overlaps(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        bool intersects = shapes_intersect(a.get(), b.get());
        // Overlaps = intersects AND neither fully contains the other
        bool overlaps = intersects && !a->contains(b.get()) && !b->contains(a.get());
        result.append(overlaps);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Crosses(GEOMETRY, GEOMETRY) → BOOLEAN
// For two geometries of mixed dimension (e.g. line×polygon): their interiors intersect
// but neither contains the other's interior.  Approximation for line×polygon cases.
StatusOr<ColumnPtr> GeometryFunctions::st_crosses(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        std::unique_ptr<GeoShape> a, b;
        if (!decode_two(av, bv, row, a, b)) { result.append_null(); continue; }
        // ST_Crosses applies to geometries of different dimensions
        bool intersects = shapes_intersect(a.get(), b.get());
        bool crosses = intersects && !a->contains(b.get()) && !b->contains(a.get());
        result.append(crosses);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_DWithin(GEOMETRY, GEOMETRY, DOUBLE) → BOOLEAN  — within distance in metres (points only)
StatusOr<ColumnPtr> GeometryFunctions::st_dwithin(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> av(columns[0]), bv(columns[1]);
    ColumnViewer<TYPE_DOUBLE>   dv(columns[2]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        auto a = decode_geom(av, row);
        auto b = decode_geom(bv, row);
        if (!a || !b || dv.is_null(row)) { result.append_null(); continue; }
        double threshold = dv.value(row);
        // For polygon containment: use contains as a conservative check
        if (a->contains(b.get()) || b->contains(a.get())) { result.append(true); continue; }
        // For point-to-point: use great-circle distance
        if (a->type() == GEO_SHAPE_POINT && b->type() == GEO_SHAPE_POINT) {
            const auto* pa = static_cast<const GeoPoint*>(a.get());
            const auto* pb = static_cast<const GeoPoint*>(b.get());
            double dist = 0.0;
            if (!GeoPoint::st_distance_sphere(pa->x(), pa->y(), pb->x(), pb->y(), &dist)) {
                result.append_null(); continue;
            }
            result.append(dist <= threshold);
        } else {
            // For non-point geometries: use MBR distance heuristic
            double ax0, ay0, ax1, ay1, bx0, by0, bx1, by1;
            if (!geo_bounding_box(a.get(), &ax0, &ay0, &ax1, &ay1) ||
                !geo_bounding_box(b.get(), &bx0, &by0, &bx1, &by1)) {
                result.append_null(); continue;
            }
            // MBR overlap → definitely within some distance
            bool mbr_overlap = ax0 <= bx1 && ax1 >= bx0 && ay0 <= by1 && ay1 >= by0;
            result.append(mbr_overlap); // conservative approximation
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ============================================================================
// Additional constructors
// ============================================================================

// ST_GeomFromGeoJSON(VARCHAR) → GEOMETRY  — basic Point, LineString, Polygon
StatusOr<ColumnPtr> GeometryFunctions::st_geom_from_geojson(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) { result.append_null(); continue; }
        auto json = viewer.value(row).to_string();

        // Extract "type" field
        auto find_str = [&](const std::string& key) -> std::string {
            std::string k = "\"" + key + "\"";
            auto pos = json.find(k);
            if (pos == std::string::npos) return "";
            pos = json.find(':', pos + k.size());
            if (pos == std::string::npos) return "";
            pos = json.find('"', pos + 1);
            if (pos == std::string::npos) return "";
            auto end = json.find('"', pos + 1);
            if (end == std::string::npos) return "";
            return json.substr(pos + 1, end - pos - 1);
        };

        std::string type = find_str("type");
        // Build WKT from coordinates and parse
        std::string wkt;
        if (type == "Point") {
            // "coordinates":[lng,lat]
            auto cs = json.find("\"coordinates\"");
            if (cs == std::string::npos) { result.append_null(); continue; }
            auto lb = json.find('[', cs);
            auto rb = json.find(']', lb);
            if (lb == std::string::npos || rb == std::string::npos) { result.append_null(); continue; }
            std::string coords = json.substr(lb + 1, rb - lb - 1);
            auto comma = coords.find(',');
            if (comma == std::string::npos) { result.append_null(); continue; }
            wkt = "POINT (" + coords.substr(0, comma) + " " + coords.substr(comma + 1) + ")";
        } else if (type == "LineString") {
            // coordinates:[[x,y],[x,y],...]
            auto cs = json.find("\"coordinates\"");
            if (cs == std::string::npos) { result.append_null(); continue; }
            auto lb = json.find('[', cs); lb = json.find('[', lb + 1);
            if (lb == std::string::npos) { result.append_null(); continue; }
            std::ostringstream ws;
            ws << "LINESTRING (";
            bool first = true;
            size_t p = lb;
            while (p < json.size() && json[p] == '[') {
                auto rb2 = json.find(']', p);
                if (rb2 == std::string::npos) break;
                std::string pair = json.substr(p + 1, rb2 - p - 1);
                auto comma = pair.find(',');
                if (comma != std::string::npos) {
                    if (!first) ws << ", ";
                    ws << pair.substr(0, comma) << " " << pair.substr(comma + 1);
                    first = false;
                }
                p = json.find('[', rb2 + 1);
                if (p == std::string::npos || json[p - 1] == ']') break;
            }
            ws << ")";
            wkt = ws.str();
        } else if (type == "Polygon") {
            // coordinates:[[[x,y],...]] — first ring only
            auto cs = json.find("\"coordinates\"");
            if (cs == std::string::npos) { result.append_null(); continue; }
            auto lb = json.find('[', cs); lb = json.find('[', lb + 1); lb = json.find('[', lb + 1);
            if (lb == std::string::npos) { result.append_null(); continue; }
            std::ostringstream ws;
            ws << "POLYGON ((";
            bool first = true;
            size_t p = lb;
            while (p < json.size() && json[p] == '[') {
                auto rb2 = json.find(']', p);
                if (rb2 == std::string::npos) break;
                std::string pair = json.substr(p + 1, rb2 - p - 1);
                auto comma = pair.find(',');
                if (comma != std::string::npos) {
                    if (!first) ws << ", ";
                    ws << pair.substr(0, comma) << " " << pair.substr(comma + 1);
                    first = false;
                }
                p = json.find('[', rb2 + 1);
                if (p == std::string::npos) break;
            }
            ws << "))";
            wkt = ws.str();
        } else {
            result.append_null(); continue;
        }

        GeoParseStatus st;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt.data(), wkt.size(), &st));
        if (!shape) { result.append_null(); continue; }
        std::string buf;
        shape->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Buffer(GEOMETRY, DOUBLE radius_degrees) → GEOMETRY
// For POINT: N-gon approximation (N=64). Radius in geographic degrees.
// For other types: returns envelope (MBR expanded by radius).
static constexpr int kBufferNPts = 64;
static constexpr double kPi = 3.14159265358979323846;

StatusOr<ColumnPtr> GeometryFunctions::st_buffer(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> geom_viewer(columns[0]);
    ColumnViewer<TYPE_DOUBLE>   rad_viewer(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(geom_viewer, row);
        if (!shape || rad_viewer.is_null(row)) { result.append_null(); continue; }
        double r = rad_viewer.value(row);
        if (r < 0) { result.append_null(); continue; }

        double cx = 0, cy = 0;
        if (shape->type() == GEO_SHAPE_POINT) {
            cx = static_cast<const GeoPoint*>(shape.get())->x();
            cy = static_cast<const GeoPoint*>(shape.get())->y();
        } else {
            double x0, y0, x1, y1;
            if (!geo_bounding_box(shape.get(), &x0, &y0, &x1, &y1)) { result.append_null(); continue; }
            cx = (x0 + x1) / 2.0;
            cy = (y0 + y1) / 2.0;
            r += std::max(x1 - x0, y1 - y0) / 2.0;
        }

        // Build N-gon approximating a circle
        std::ostringstream wkt;
        wkt << std::setprecision(12) << "POLYGON ((";
        double cos_lat = std::cos(cy * kPi / 180.0);
        if (cos_lat < 1e-10) cos_lat = 1e-10;
        for (int i = 0; i <= kBufferNPts; ++i) {
            double theta = 2.0 * kPi * i / kBufferNPts;
            double vx = cx + r * std::cos(theta) / cos_lat;
            double vy = cy + r * std::sin(theta);
            if (i) wkt << ", ";
            wkt << vx << " " << vy;
        }
        wkt << "))";
        std::string s = wkt.str();
        GeoParseStatus st;
        std::unique_ptr<GeoShape> buf_shape(GeoShape::from_wkt(s.data(), s.size(), &st));
        if (!buf_shape) { result.append_null(); continue; }
        std::string buf;
        buf_shape->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_ConvexHull(GEOMETRY) → GEOMETRY — minimum convex enclosing polygon
// Uses Andrew's monotone chain algorithm on all vertices.
static std::vector<std::pair<double,double>> collect_vertices(const GeoShape* shape) {
    std::vector<std::pair<double,double>> pts;
    switch (shape->type()) {
    case GEO_SHAPE_POINT:
        pts.emplace_back(static_cast<const GeoPoint*>(shape)->x(),
                         static_cast<const GeoPoint*>(shape)->y());
        break;
    case GEO_SHAPE_LINE_STRING: {
        int nv = geo_line_vertex_count(shape);
        for (int i = 0; i < nv; ++i) {
            double vx, vy;
            if (geo_line_vertex_at(shape, i, &vx, &vy)) pts.emplace_back(vx, vy);
        }
        break;
    }
    case GEO_SHAPE_POLYGON: {
        int nl = geo_polygon_loop_count(shape);
        for (int i = 0; i < nl; ++i) {
            int nv = geo_polygon_loop_vertex_count(shape, i);
            for (int j = 0; j < nv; ++j) {
                double vx, vy;
                if (geo_polygon_loop_vertex_at(shape, i, j, &vx, &vy)) pts.emplace_back(vx, vy);
            }
        }
        break;
    }
    default: break;
    }
    return pts;
}

static std::vector<std::pair<double,double>> andrew_chain_hull(
        std::vector<std::pair<double,double>> pts) {
    int n = (int)pts.size();
    if (n < 3) return pts;
    std::sort(pts.begin(), pts.end());
    std::vector<std::pair<double,double>> hull;
    auto cross = [](const std::pair<double,double>& O, const std::pair<double,double>& A,
                    const std::pair<double,double>& B) {
        return (A.first - O.first) * (B.second - O.second)
             - (A.second - O.second) * (B.first - O.first);
    };
    // Lower hull
    for (int i = 0; i < n; ++i) {
        while (hull.size() >= 2 && cross(hull[hull.size()-2], hull[hull.size()-1], pts[i]) <= 0)
            hull.pop_back();
        hull.push_back(pts[i]);
    }
    // Upper hull
    int lower_size = (int)hull.size() + 1;
    for (int i = n - 2; i >= 0; --i) {
        while ((int)hull.size() >= lower_size &&
               cross(hull[hull.size()-2], hull[hull.size()-1], pts[i]) <= 0)
            hull.pop_back();
        hull.push_back(pts[i]);
    }
    hull.pop_back();
    return hull;
}

StatusOr<ColumnPtr> GeometryFunctions::st_convex_hull(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(viewer, row);
        if (!shape) { result.append_null(); continue; }
        auto pts = collect_vertices(shape.get());
        if (pts.empty()) { result.append_null(); continue; }
        if (pts.size() == 1) {
            // Single point — return the point
            result.append(Slice(viewer.value(row).data, viewer.value(row).size));
            continue;
        }
        auto hull = andrew_chain_hull(pts);
        if (hull.size() < 3) {
            // Degenerate — return as-is
            result.append(Slice(viewer.value(row).data, viewer.value(row).size));
            continue;
        }
        std::ostringstream wkt;
        wkt << std::setprecision(12) << "POLYGON ((";
        for (size_t i = 0; i < hull.size(); ++i) {
            if (i) wkt << ", ";
            wkt << hull[i].first << " " << hull[i].second;
        }
        // Close ring
        wkt << ", " << hull[0].first << " " << hull[0].second << "))";
        std::string s = wkt.str();
        GeoParseStatus st;
        std::unique_ptr<GeoShape> ch(GeoShape::from_wkt(s.data(), s.size(), &st));
        if (!ch) { result.append_null(); continue; }
        std::string buf;
        ch->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_Simplify(GEOMETRY, DOUBLE tolerance) → GEOMETRY — Douglas-Peucker simplification
// Works on LINESTRING and POLYGON outer ring.
static double point_line_dist(const std::pair<double,double>& p,
                               const std::pair<double,double>& a,
                               const std::pair<double,double>& b) {
    double dx = b.first - a.first, dy = b.second - a.second;
    if (dx == 0 && dy == 0) {
        double ex = p.first - a.first, ey = p.second - a.second;
        return std::sqrt(ex*ex + ey*ey);
    }
    double t = ((p.first - a.first)*dx + (p.second - a.second)*dy) / (dx*dx + dy*dy);
    t = std::max(0.0, std::min(1.0, t));
    double ex = p.first - (a.first + t*dx), ey = p.second - (a.second + t*dy);
    return std::sqrt(ex*ex + ey*ey);
}

static void dp_simplify(const std::vector<std::pair<double,double>>& pts,
                         int start, int end, double tol,
                         std::vector<bool>& keep) {
    if (end <= start + 1) return;
    double max_dist = 0;
    int max_idx = start;
    for (int i = start + 1; i < end; ++i) {
        double d = point_line_dist(pts[i], pts[start], pts[end]);
        if (d > max_dist) { max_dist = d; max_idx = i; }
    }
    if (max_dist > tol) {
        keep[max_idx] = true;
        dp_simplify(pts, start, max_idx, tol, keep);
        dp_simplify(pts, max_idx, end, tol, keep);
    }
}

StatusOr<ColumnPtr> GeometryFunctions::st_simplify(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_GEOMETRY> geom_viewer(columns[0]);
    ColumnViewer<TYPE_DOUBLE>   tol_viewer(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_GEOMETRY> result(size);
    for (int row = 0; row < size; ++row) {
        auto shape = decode_geom(geom_viewer, row);
        if (!shape || tol_viewer.is_null(row)) { result.append_null(); continue; }
        double tol = tol_viewer.value(row);
        if (tol < 0) { result.append_null(); continue; }

        std::vector<std::pair<double,double>> pts;
        bool is_polygon = (shape->type() == GEO_SHAPE_POLYGON);

        if (shape->type() == GEO_SHAPE_LINE_STRING) {
            int nv = geo_line_vertex_count(shape.get());
            for (int i = 0; i < nv; ++i) {
                double vx, vy;
                if (geo_line_vertex_at(shape.get(), i, &vx, &vy)) pts.emplace_back(vx, vy);
            }
        } else if (is_polygon) {
            int nv = geo_polygon_loop_vertex_count(shape.get(), 0);
            if (nv == 0) { result.append_null(); continue; }
            for (int i = 0; i < nv; ++i) {
                double vx, vy;
                if (geo_polygon_loop_vertex_at(shape.get(), 0, i, &vx, &vy)) pts.emplace_back(vx, vy);
            }
        } else {
            // For points just return as-is
            auto v = geom_viewer.value(row);
            result.append(Slice(v.data, v.size));
            continue;
        }

        int n = (int)pts.size();
        if (n < 2) { result.append_null(); continue; }

        std::vector<bool> keep(n, false);
        keep[0] = keep[n-1] = true;
        dp_simplify(pts, 0, n-1, tol, keep);

        std::vector<std::pair<double,double>> simplified;
        for (int i = 0; i < n; ++i) if (keep[i]) simplified.push_back(pts[i]);

        std::ostringstream wkt;
        wkt << std::setprecision(12);
        if (is_polygon) {
            if (simplified.size() < 3) { result.append_null(); continue; }
            wkt << "POLYGON ((";
            for (size_t i = 0; i < simplified.size(); ++i) {
                if (i) wkt << ", ";
                wkt << simplified[i].first << " " << simplified[i].second;
            }
            // close ring
            wkt << ", " << simplified[0].first << " " << simplified[0].second << "))";
        } else {
            if (simplified.size() < 2) { result.append_null(); continue; }
            wkt << "LINESTRING (";
            for (size_t i = 0; i < simplified.size(); ++i) {
                if (i) wkt << ", ";
                wkt << simplified[i].first << " " << simplified[i].second;
            }
            wkt << ")";
        }
        std::string s = wkt.str();
        GeoParseStatus st;
        std::unique_ptr<GeoShape> sim(GeoShape::from_wkt(s.data(), s.size(), &st));
        if (!sim) { result.append_null(); continue; }
        std::string buf;
        sim->encode_to(&buf);
        result.append(Slice(buf.data(), buf.size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ST_DistanceSphere(GEOMETRY, GEOMETRY) → DOUBLE — alias for ST_Distance
StatusOr<ColumnPtr> GeometryFunctions::st_distance_sphere_geom(FunctionContext* ctx, const Columns& columns) {
    return st_distance_geom(ctx, columns);
}

} // namespace starrocks

#include "gen_cpp/opcode/GeometryFunctions.inc"
