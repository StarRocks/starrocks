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

namespace starrocks {

class H3Functions {
public:
    /**
     * @param: [lng DOUBLE, lat DOUBLE, resolution INT]
     * @return: BIGINT
     * Returns the H3 cell index (resolution 0-15) containing the given
     * WGS84 longitude/latitude point. Returns NULL on invalid coordinates
     * or resolution out of range [0, 15].
     */
    DEFINE_VECTORIZED_FN(geo_to_h3);

    /**
     * @param: [h3index BIGINT]
     * @return: DOUBLE
     * Returns the latitude (degrees) of the center of the H3 cell.
     * Returns NULL if the index is invalid.
     */
    DEFINE_VECTORIZED_FN(h3_to_geo_lat);

    /**
     * @param: [h3index BIGINT]
     * @return: DOUBLE
     * Returns the longitude (degrees) of the center of the H3 cell.
     * Returns NULL if the index is invalid.
     */
    DEFINE_VECTORIZED_FN(h3_to_geo_lng);

    /**
     * @param: [h3index BIGINT]
     * @return: BOOLEAN
     * Returns true if the index is a valid H3 cell index, false otherwise.
     * Never returns NULL.
     */
    DEFINE_VECTORIZED_FN(h3_is_valid);

    /**
     * @param: [h3index BIGINT]
     * @return: INT
     * Returns the resolution (0-15) of the H3 cell index.
     * Returns NULL if the index is invalid.
     */
    DEFINE_VECTORIZED_FN(h3_get_resolution);

    // ── Resolution → scalar ─────────────────────────────────────────────────

    /** h3EdgeAngle(resolution INT) → DOUBLE
     *  Average H3 hex edge length at the given resolution, in degrees. */
    DEFINE_VECTORIZED_FN(h3_edge_angle);

    /** h3EdgeLengthM(resolution INT) → DOUBLE
     *  Average H3 hex edge length in metres. */
    DEFINE_VECTORIZED_FN(h3_edge_length_m);

    /** h3EdgeLengthKm(resolution INT) → DOUBLE
     *  Average H3 hex edge length in kilometres. */
    DEFINE_VECTORIZED_FN(h3_edge_length_km);

    /** h3HexAreaM2(resolution INT) → DOUBLE
     *  Average H3 hex area in square metres. */
    DEFINE_VECTORIZED_FN(h3_hex_area_m2);

    /** h3HexAreaKm2(resolution INT) → DOUBLE
     *  Average H3 hex area in square kilometres. */
    DEFINE_VECTORIZED_FN(h3_hex_area_km2);

    /** h3NumHexagons(resolution INT) → BIGINT
     *  Total number of H3 cells at the given resolution. */
    DEFINE_VECTORIZED_FN(h3_num_hexagons);

    // ── H3 index → scalar ───────────────────────────────────────────────────

    /** h3GetBaseCell(h3index BIGINT) → INT
     *  Base cell number (0-121) of the H3 cell. */
    DEFINE_VECTORIZED_FN(h3_get_base_cell);

    /** h3IsResClassIII(h3index BIGINT) → BOOLEAN
     *  True if the cell's resolution is Class III oriented. */
    DEFINE_VECTORIZED_FN(h3_is_res_class_iii);

    /** h3IsPentagon(h3index BIGINT) → BOOLEAN
     *  True if the cell is a pentagon cell. */
    DEFINE_VECTORIZED_FN(h3_is_pentagon);

    /** h3CellAreaM2(h3index BIGINT) → DOUBLE
     *  Exact area of this specific cell in square metres. */
    DEFINE_VECTORIZED_FN(h3_cell_area_m2);

    /** h3CellAreaRads2(h3index BIGINT) → DOUBLE
     *  Exact area of this specific cell in steradians. */
    DEFINE_VECTORIZED_FN(h3_cell_area_rads2);

    /** h3ExactEdgeLengthM(h3edge BIGINT) → DOUBLE
     *  Exact edge length of a directed H3 edge in metres. */
    DEFINE_VECTORIZED_FN(h3_exact_edge_length_m);

    /** h3ExactEdgeLengthKm(h3edge BIGINT) → DOUBLE
     *  Exact edge length of a directed H3 edge in kilometres. */
    DEFINE_VECTORIZED_FN(h3_exact_edge_length_km);

    /** h3ExactEdgeLengthRads(h3edge BIGINT) → DOUBLE
     *  Exact edge length of a directed H3 edge in radians. */
    DEFINE_VECTORIZED_FN(h3_exact_edge_length_rads);

    /** h3ToString(h3index BIGINT) → VARCHAR
     *  Hex-string representation of an H3 index. */
    DEFINE_VECTORIZED_FN(h3_to_string);

    /** h3UnidirectionalEdgeIsValid(edge BIGINT) → BOOLEAN
     *  True if the value is a valid directed H3 edge index. */
    DEFINE_VECTORIZED_FN(h3_unidirectional_edge_is_valid);

    /** h3GetOriginIndexFromUnidirectionalEdge(edge BIGINT) → BIGINT
     *  Origin cell of a directed H3 edge. */
    DEFINE_VECTORIZED_FN(h3_get_origin_index_from_unidirectional_edge);

    /** h3GetDestinationIndexFromUnidirectionalEdge(edge BIGINT) → BIGINT
     *  Destination cell of a directed H3 edge. */
    DEFINE_VECTORIZED_FN(h3_get_destination_index_from_unidirectional_edge);

    // ── VARCHAR → H3 index ──────────────────────────────────────────────────

    /** stringToH3(h3string VARCHAR) → BIGINT
     *  Parse a hex-string H3 representation to its integer index. */
    DEFINE_VECTORIZED_FN(string_to_h3);

    // ── Two H3 indexes → scalar ─────────────────────────────────────────────

    /** h3IndexesAreNeighbors(idx1 BIGINT, idx2 BIGINT) → BOOLEAN
     *  True if the two cells share an edge. */
    DEFINE_VECTORIZED_FN(h3_indexes_are_neighbors);

    /** h3Distance(start BIGINT, end BIGINT) → BIGINT
     *  Grid distance (in cells) between two H3 cells. */
    DEFINE_VECTORIZED_FN(h3_distance);

    /** h3GetUnidirectionalEdge(origin BIGINT, destination BIGINT) → BIGINT
     *  Directed edge index between two neighbouring cells. */
    DEFINE_VECTORIZED_FN(h3_get_unidirectional_edge);

    // ── H3 index + resolution → H3 index ────────────────────────────────────

    /** h3ToParent(h3index BIGINT, resolution INT) → BIGINT
     *  Parent cell at a coarser resolution. */
    DEFINE_VECTORIZED_FN(h3_to_parent);

    /** h3ToCenterChild(h3index BIGINT, resolution INT) → BIGINT
     *  Center child cell at a finer resolution. */
    DEFINE_VECTORIZED_FN(h3_to_center_child);

    // ── Four doubles → double ────────────────────────────────────────────────

    /** h3PointDistM(lat1, lon1, lat2, lon2 DOUBLE) → DOUBLE
     *  Great-circle (haversine) distance between two WGS84 points in metres. */
    DEFINE_VECTORIZED_FN(h3_point_dist_m);

    /** h3PointDistKm(lat1, lon1, lat2, lon2 DOUBLE) → DOUBLE
     *  Great-circle distance in kilometres. */
    DEFINE_VECTORIZED_FN(h3_point_dist_km);

    /** h3PointDistRads(lat1, lon1, lat2, lon2 DOUBLE) → DOUBLE
     *  Great-circle distance in radians. */
    DEFINE_VECTORIZED_FN(h3_point_dist_rads);

    // ── Array-returning functions ────────────────────────────────────────────

    /** h3kRing(h3index BIGINT, k INT) → ARRAY(BIGINT)
     *  All cells within grid distance k of the origin (unordered). */
    DEFINE_VECTORIZED_FN(h3k_ring);

    /** h3ToChildren(h3index BIGINT, resolution INT) → ARRAY(BIGINT)
     *  All child cells at a finer resolution. */
    DEFINE_VECTORIZED_FN(h3_to_children);

    /** h3GetFaces(h3index BIGINT) → ARRAY(INT)
     *  Icosahedron face numbers intersected by this cell. */
    DEFINE_VECTORIZED_FN(h3_get_faces);

    /** h3GetRes0Indexes() → ARRAY(BIGINT)
     *  All 122 resolution-0 H3 base cells. */
    DEFINE_VECTORIZED_FN(h3_get_res0_indexes);

    /** h3GetPentagonIndexes(resolution INT) → ARRAY(BIGINT)
     *  All 12 pentagon cells at the given resolution. */
    DEFINE_VECTORIZED_FN(h3_get_pentagon_indexes);

    /** h3Line(start BIGINT, end BIGINT) → ARRAY(BIGINT)
     *  Grid path of cells between two H3 indexes. */
    DEFINE_VECTORIZED_FN(h3_line);

    /** h3HexRing(h3index BIGINT, k INT) → ARRAY(BIGINT)
     *  Cells at exactly grid distance k from origin (hollow ring). */
    DEFINE_VECTORIZED_FN(h3_hex_ring);

    /** h3GetUnidirectionalEdgesFromHexagon(h3index BIGINT) → ARRAY(BIGINT)
     *  All directed edges originating from this cell (up to 6). */
    DEFINE_VECTORIZED_FN(h3_get_unidirectional_edges_from_hexagon);
};

} // namespace starrocks
