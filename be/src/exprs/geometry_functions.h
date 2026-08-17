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

#pragma once

#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "geo/geo_common.h"

namespace starrocks {

class GeometryFunctions {
public:
    /**
     * @param: [string_value]  WKT representation (e.g. 'POINT(1 2)')
     * @paramType: [VarcharColumn]
     * @return: GeometryColumn
     * Parse a WKT string into a GEOMETRY value.
     */
    DEFINE_VECTORIZED_FN(st_geom_from_text);

    /**
     * @param: [binary_value]  WKB bytes
     * @paramType: [VarbinaryColumn]
     * @return: GeometryColumn
     * Construct a GEOMETRY from WKB (Well-Known Binary) bytes.
     */
    DEFINE_VECTORIZED_FN(st_geom_from_wkb);

    /**
     * @param: [geometry_value]
     * @paramType: [GeometryColumn]
     * @return: VarcharColumn
     * Return the WKT text representation of a GEOMETRY value.
     */
    DEFINE_VECTORIZED_FN(st_as_text);

    /**
     * @param: [geometry_value]
     * @paramType: [GeometryColumn]
     * @return: VarbinaryColumn
     * Return the WKB byte representation of a GEOMETRY value.
     */
    DEFINE_VECTORIZED_FN(st_as_wkb);

    /**
     * @param: [double_value, double_value]  longitude, latitude
     * @paramType: [DoubleColumn, DoubleColumn]
     * @return: GeometryColumn
     * Construct a GEOMETRY POINT from X/Y coordinates.
     */
    DEFINE_VECTORIZED_FN(st_make_point);

    static Status st_contains_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);
    static Status st_contains_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    /**
     * @param: [geometry_value, geometry_value]
     * @paramType: [GeometryColumn, GeometryColumn]
     * @return: BooleanColumn
     * Return true if geom1 completely contains geom2.
     */
    DEFINE_VECTORIZED_FN(st_contains);

    /**
     * @param: [geometry_value, geometry_value]
     * @paramType: [GeometryColumn, GeometryColumn]
     * @return: BooleanColumn
     * Return true if geom1 is completely within geom2 (i.e. geom2 contains geom1).
     */
    DEFINE_VECTORIZED_FN(st_within);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------
    /** ST_X(GEOMETRY) → DOUBLE  — longitude of a POINT geometry. */
    DEFINE_VECTORIZED_FN(st_x_geom);
    /** ST_Y(GEOMETRY) → DOUBLE  — latitude of a POINT geometry. */
    DEFINE_VECTORIZED_FN(st_y_geom);
    /** ST_GeometryType(GEOMETRY) → VARCHAR  — e.g. 'ST_Point', 'ST_LineString'. */
    DEFINE_VECTORIZED_FN(st_geometry_type);
    /** ST_SRID(GEOMETRY) → INT  — always 4326 (WGS84). */
    DEFINE_VECTORIZED_FN(st_srid);
    /** ST_IsValid(GEOMETRY) → BOOLEAN  — S2-level validity check. */
    DEFINE_VECTORIZED_FN(st_is_valid);
    /** ST_IsEmpty(GEOMETRY) → BOOLEAN  — true for NULL / empty inputs. */
    DEFINE_VECTORIZED_FN(st_is_empty);
    /** ST_NDims(GEOMETRY) → INT  — always 2. */
    DEFINE_VECTORIZED_FN(st_ndims);
    /** ST_NPoints(GEOMETRY) → INT  — total vertex count. */
    DEFINE_VECTORIZED_FN(st_npoints);
    /** ST_NumGeometries(GEOMETRY) → INT  — always 1 for scalar types. */
    DEFINE_VECTORIZED_FN(st_num_geometries);

    // -----------------------------------------------------------------------
    // Measurements
    // -----------------------------------------------------------------------
    /** ST_Distance(GEOMETRY, GEOMETRY) → DOUBLE  — great-circle distance in metres (point-to-point). */
    DEFINE_VECTORIZED_FN(st_distance_geom);
    /** ST_Length(GEOMETRY) → DOUBLE  — arc length in metres (LINESTRING). */
    DEFINE_VECTORIZED_FN(st_length);
    /** ST_Area(GEOMETRY) → DOUBLE  — area in square metres (POLYGON). */
    DEFINE_VECTORIZED_FN(st_area);
    /** ST_Perimeter(GEOMETRY) → DOUBLE  — perimeter in metres (POLYGON). */
    DEFINE_VECTORIZED_FN(st_perimeter);

    // -----------------------------------------------------------------------
    // Spatial predicates
    // -----------------------------------------------------------------------
    /** ST_Intersects(GEOMETRY, GEOMETRY) → BOOLEAN  — geometries share any point. */
    DEFINE_VECTORIZED_FN(st_intersects);
    /** ST_Disjoint(GEOMETRY, GEOMETRY) → BOOLEAN  — NOT ST_Intersects. */
    DEFINE_VECTORIZED_FN(st_disjoint);
    /** ST_Equals(GEOMETRY, GEOMETRY) → BOOLEAN  — spatial equality. */
    DEFINE_VECTORIZED_FN(st_equals);
    /** ST_Covers(GEOMETRY, GEOMETRY) → BOOLEAN  — geom1 covers geom2 (Contains incl. boundary). */
    DEFINE_VECTORIZED_FN(st_covers);
    /** ST_CoveredBy(GEOMETRY, GEOMETRY) → BOOLEAN  — geom1 is covered by geom2. */
    DEFINE_VECTORIZED_FN(st_covered_by);
    /** ST_Touches(GEOMETRY, GEOMETRY) → BOOLEAN  — share boundary, interiors do not intersect. */
    DEFINE_VECTORIZED_FN(st_touches);

    // -----------------------------------------------------------------------
    // Constructors / transformations
    // -----------------------------------------------------------------------
    /** ST_Envelope(GEOMETRY) → GEOMETRY  — minimum bounding rectangle as POLYGON. */
    DEFINE_VECTORIZED_FN(st_envelope);
    /** ST_Centroid(GEOMETRY) → GEOMETRY  — centroid as POINT. */
    DEFINE_VECTORIZED_FN(st_centroid);
    /** ST_MakeLine(GEOMETRY, GEOMETRY) → GEOMETRY  — LINESTRING from two POINTs. */
    DEFINE_VECTORIZED_FN(st_make_line);
    /** ST_Point(DOUBLE, DOUBLE) → GEOMETRY  — alias for ST_MakePoint. */
    DEFINE_VECTORIZED_FN(st_point_geom);

    // -----------------------------------------------------------------------
    // Output formats
    // -----------------------------------------------------------------------
    /** ST_AsGeoJSON(GEOMETRY) → VARCHAR  — GeoJSON representation. */
    DEFINE_VECTORIZED_FN(st_as_geojson);

    // -----------------------------------------------------------------------
    // Additional accessors (PostGIS parity)
    // -----------------------------------------------------------------------
    /** ST_Dimension(GEOMETRY) → INT  — 0=point, 1=line, 2=polygon. */
    DEFINE_VECTORIZED_FN(st_dimension);
    /** ST_StartPoint(GEOMETRY) → GEOMETRY  — first vertex of LINESTRING. */
    DEFINE_VECTORIZED_FN(st_start_point);
    /** ST_EndPoint(GEOMETRY) → GEOMETRY  — last vertex of LINESTRING. */
    DEFINE_VECTORIZED_FN(st_end_point);
    /** ST_PointN(GEOMETRY, INT) → GEOMETRY  — 1-based Nth vertex of LINESTRING. */
    DEFINE_VECTORIZED_FN(st_point_n);
    /** ST_IsClosed(GEOMETRY) → BOOLEAN  — true if LINESTRING start == end. */
    DEFINE_VECTORIZED_FN(st_is_closed);
    /** ST_IsRing(GEOMETRY) → BOOLEAN  — closed and simple LINESTRING. */
    DEFINE_VECTORIZED_FN(st_is_ring);
    /** ST_IsSimple(GEOMETRY) → BOOLEAN  — no self-intersections. */
    DEFINE_VECTORIZED_FN(st_is_simple);
    /** ST_NumInteriorRings(GEOMETRY) → INT  — number of polygon holes. */
    DEFINE_VECTORIZED_FN(st_num_interior_rings);
    /** ST_ExteriorRing(GEOMETRY) → GEOMETRY  — outer ring as LINESTRING. */
    DEFINE_VECTORIZED_FN(st_exterior_ring);

    // -----------------------------------------------------------------------
    // Additional predicates (PostGIS parity)
    // -----------------------------------------------------------------------
    /** ST_Overlaps(GEOMETRY, GEOMETRY) → BOOLEAN  — share interior but neither contains. */
    DEFINE_VECTORIZED_FN(st_overlaps);
    /** ST_Crosses(GEOMETRY, GEOMETRY) → BOOLEAN  — interiors intersect but not fully. */
    DEFINE_VECTORIZED_FN(st_crosses);
    /** ST_DWithin(GEOMETRY, GEOMETRY, DOUBLE) → BOOLEAN  — within distance (metres). */
    DEFINE_VECTORIZED_FN(st_dwithin);

    // -----------------------------------------------------------------------
    // Additional constructors (PostGIS parity)
    // -----------------------------------------------------------------------
    /** ST_GeomFromGeoJSON(VARCHAR) → GEOMETRY  — parse GeoJSON Point/LineString/Polygon. */
    DEFINE_VECTORIZED_FN(st_geom_from_geojson);
    /** ST_Buffer(GEOMETRY, DOUBLE) → GEOMETRY  — buffer around geometry in degrees. */
    DEFINE_VECTORIZED_FN(st_buffer);
    /** ST_ConvexHull(GEOMETRY) → GEOMETRY  — minimum convex enclosing polygon. */
    DEFINE_VECTORIZED_FN(st_convex_hull);
    /** ST_Simplify(GEOMETRY, DOUBLE) → GEOMETRY  — Douglas-Peucker simplification. */
    DEFINE_VECTORIZED_FN(st_simplify);

    // -----------------------------------------------------------------------
    // Additional input format (PostGIS parity)
    // -----------------------------------------------------------------------
    /** ST_DistanceSphere(GEOMETRY, GEOMETRY) → DOUBLE  — alias for ST_Distance. */
    DEFINE_VECTORIZED_FN(st_distance_sphere_geom);
};

} // namespace starrocks
