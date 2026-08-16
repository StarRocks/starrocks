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

#include <h3/h3api.h>

#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks {

// ────────────────────────────────────────────────────────────────────────────
// Helper: build a nullable array-of-BIGINT result column.
// row_is_null[i] == true → that row's array is NULL.
// rows[i] holds the BIGINT values for row i (empty when null).
// ────────────────────────────────────────────────────────────────────────────
static ColumnPtr build_bigint_array(const std::vector<std::vector<int64_t>>& rows, const std::vector<bool>& row_is_null,
                                    bool all_const) {
    auto offsets = UInt32Column::create();
    auto elements = Int64Column::create();
    offsets->reserve(rows.size() + 1);
    offsets->append(0);
    uint32_t total = 0;

    bool any_null = false;
    for (const auto& b : row_is_null) any_null |= b;

    NullColumn::MutablePtr nulls;
    if (any_null) {
        nulls = NullColumn::create();
        nulls->get_data().resize(rows.size(), 0);
    }

    for (size_t i = 0; i < rows.size(); ++i) {
        if (any_null && row_is_null[i]) {
            nulls->get_data()[i] = 1;
            offsets->append(total);
        } else {
            for (auto v : rows[i]) {
                elements->append(v);
                total++;
            }
            offsets->append(total);
        }
    }

    ColumnPtr arr = ArrayColumn::create(std::move(elements), std::move(offsets));
    if (any_null) arr = NullableColumn::create(std::move(arr), std::move(nulls));
    if (all_const) arr = ConstColumn::create(std::move(arr), 1);
    return arr;
}

// Same helper for INT (icosahedron faces).
static ColumnPtr build_int_array(const std::vector<std::vector<int32_t>>& rows, const std::vector<bool>& row_is_null,
                                 bool all_const) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    offsets->reserve(rows.size() + 1);
    offsets->append(0);
    uint32_t total = 0;

    bool any_null = false;
    for (const auto& b : row_is_null) any_null |= b;

    NullColumn::MutablePtr nulls;
    if (any_null) {
        nulls = NullColumn::create();
        nulls->get_data().resize(rows.size(), 0);
    }

    for (size_t i = 0; i < rows.size(); ++i) {
        if (any_null && row_is_null[i]) {
            nulls->get_data()[i] = 1;
            offsets->append(total);
        } else {
            for (auto v : rows[i]) {
                elements->append(v);
                total++;
            }
            offsets->append(total);
        }
    }

    ColumnPtr arr = ArrayColumn::create(std::move(elements), std::move(offsets));
    if (any_null) arr = NullableColumn::create(std::move(arr), std::move(nulls));
    if (all_const) arr = ConstColumn::create(std::move(arr), 1);
    return arr;
}

// ────────────────────────────────────────────────────────────────────────────
// Original 5 core functions
// ────────────────────────────────────────────────────────────────────────────

// geoToH3(lng DOUBLE, lat DOUBLE, resolution INT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::geo_to_h3(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> lng_col(columns[0]);
    ColumnViewer<TYPE_DOUBLE> lat_col(columns[1]);
    ColumnViewer<TYPE_INT> res_col(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);

    for (int row = 0; row < size; ++row) {
        if (lng_col.is_null(row) || lat_col.is_null(row) || res_col.is_null(row)) {
            result.append_null();
            continue;
        }

        // H3 v4 LatLng uses radians; SQL interface takes degrees.
        LatLng coord{degsToRads(lat_col.value(row)), degsToRads(lng_col.value(row))};
        H3Index h3index;
        H3Error err = latLngToCell(&coord, res_col.value(row), &h3index);
        if (err != E_SUCCESS) {
            result.append_null();
            continue;
        }

        result.append(static_cast<int64_t>(h3index));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ToGeoLat(h3index BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_to_geo_lat(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }

        LatLng coord;
        if (cellToLatLng(static_cast<H3Index>(h3_col.value(row)), &coord) != E_SUCCESS) {
            result.append_null();
            continue;
        }

        result.append(radsToDegs(coord.lat));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ToGeoLng(h3index BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_to_geo_lng(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }

        LatLng coord;
        if (cellToLatLng(static_cast<H3Index>(h3_col.value(row)), &coord) != E_SUCCESS) {
            result.append_null();
            continue;
        }

        result.append(radsToDegs(coord.lng));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// h3IsValid(h3index BIGINT) -> BOOLEAN
StatusOr<ColumnPtr> H3Functions::h3_is_valid(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }

        result.append(isValidCell(static_cast<H3Index>(h3_col.value(row))) != 0);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// h3GetResolution(h3index BIGINT) -> INT
StatusOr<ColumnPtr> H3Functions::h3_get_resolution(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }

        H3Index h3index = static_cast<H3Index>(h3_col.value(row));
        if (isValidCell(h3index) == 0) {
            result.append_null();
            continue;
        }

        result.append(getResolution(h3index));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// Resolution → scalar
// ────────────────────────────────────────────────────────────────────────────

// h3EdgeAngle(resolution INT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_edge_angle(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double rads;
        if (edgeLengthRads(res_col.value(row), &rads) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(radsToDegs(rads));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3EdgeLengthM(resolution INT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_edge_length_m(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double len;
        if (edgeLengthM(res_col.value(row), &len) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(len);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3EdgeLengthKm(resolution INT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_edge_length_km(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double len;
        if (edgeLengthKm(res_col.value(row), &len) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(len);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3HexAreaM2(resolution INT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_hex_area_m2(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double area;
        if (getHexagonAreaAvgM2(res_col.value(row), &area) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(area);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3HexAreaKm2(resolution INT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_hex_area_km2(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double area;
        if (getHexagonAreaAvgKm2(res_col.value(row), &area) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(area);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3NumHexagons(resolution INT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_num_hexagons(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        int64_t num;
        if (getNumCells(res_col.value(row), &num) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(num);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// H3 index → scalar
// ────────────────────────────────────────────────────────────────────────────

// h3GetBaseCell(h3index BIGINT) -> INT
StatusOr<ColumnPtr> H3Functions::h3_get_base_cell(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        result.append(getBaseCellNumber(static_cast<H3Index>(h3_col.value(row))));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3IsResClassIII(h3index BIGINT) -> BOOLEAN
StatusOr<ColumnPtr> H3Functions::h3_is_res_class_iii(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        result.append(isResClassIII(static_cast<H3Index>(h3_col.value(row))) != 0);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3IsPentagon(h3index BIGINT) -> BOOLEAN
StatusOr<ColumnPtr> H3Functions::h3_is_pentagon(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        result.append(isPentagon(static_cast<H3Index>(h3_col.value(row))) != 0);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3CellAreaM2(h3index BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_cell_area_m2(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double area;
        if (cellAreaM2(static_cast<H3Index>(h3_col.value(row)), &area) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(area);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3CellAreaRads2(h3index BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_cell_area_rads2(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double area;
        if (cellAreaRads2(static_cast<H3Index>(h3_col.value(row)), &area) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(area);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ExactEdgeLengthM(h3edge BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_exact_edge_length_m(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double len;
        if (edgeLengthM(static_cast<H3Index>(h3_col.value(row)), &len) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(len);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ExactEdgeLengthKm(h3edge BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_exact_edge_length_km(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double len;
        if (edgeLengthKm(static_cast<H3Index>(h3_col.value(row)), &len) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(len);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ExactEdgeLengthRads(h3edge BIGINT) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_exact_edge_length_rads(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        double len;
        if (edgeLengthRads(static_cast<H3Index>(h3_col.value(row)), &len) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(len);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ToString(h3index BIGINT) -> VARCHAR
StatusOr<ColumnPtr> H3Functions::h3_to_string(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        char buf[17]; // 15 hex chars + null; extra byte for safety
        if (h3ToString(static_cast<H3Index>(h3_col.value(row)), buf, sizeof(buf)) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(Slice(buf));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3UnidirectionalEdgeIsValid(edge BIGINT) -> BOOLEAN
StatusOr<ColumnPtr> H3Functions::h3_unidirectional_edge_is_valid(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        result.append(isValidDirectedEdge(static_cast<H3Index>(h3_col.value(row))) != 0);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3GetOriginIndexFromUnidirectionalEdge(edge BIGINT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_get_origin_index_from_unidirectional_edge(FunctionContext* ctx,
                                                                              const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        H3Index origin;
        if (getDirectedEdgeOrigin(static_cast<H3Index>(h3_col.value(row)), &origin) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(static_cast<int64_t>(origin));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3GetDestinationIndexFromUnidirectionalEdge(edge BIGINT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_get_destination_index_from_unidirectional_edge(FunctionContext* ctx,
                                                                                   const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            result.append_null();
            continue;
        }
        H3Index dest;
        if (getDirectedEdgeDestination(static_cast<H3Index>(h3_col.value(row)), &dest) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(static_cast<int64_t>(dest));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// VARCHAR → H3 index
// ────────────────────────────────────────────────────────────────────────────

// stringToH3(h3string VARCHAR) -> BIGINT
StatusOr<ColumnPtr> H3Functions::string_to_h3(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (str_col.is_null(row)) {
            result.append_null();
            continue;
        }
        Slice s = str_col.value(row);
        std::string str(s.data, s.size); // ensures null-termination for H3 API
        H3Index h;
        if (stringToH3(str.c_str(), &h) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(static_cast<int64_t>(h));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// Two H3 indexes → scalar
// ────────────────────────────────────────────────────────────────────────────

// h3IndexesAreNeighbors(idx1 BIGINT, idx2 BIGINT) -> BOOLEAN
StatusOr<ColumnPtr> H3Functions::h3_indexes_are_neighbors(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h1_col(columns[0]);
    ColumnViewer<TYPE_BIGINT> h2_col(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (h1_col.is_null(row) || h2_col.is_null(row)) {
            result.append_null();
            continue;
        }
        int out;
        if (areNeighborCells(static_cast<H3Index>(h1_col.value(row)), static_cast<H3Index>(h2_col.value(row)), &out) !=
            E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(out != 0);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3Distance(start BIGINT, end BIGINT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_distance(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h1_col(columns[0]);
    ColumnViewer<TYPE_BIGINT> h2_col(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h1_col.is_null(row) || h2_col.is_null(row)) {
            result.append_null();
            continue;
        }
        int64_t dist;
        if (gridDistance(static_cast<H3Index>(h1_col.value(row)), static_cast<H3Index>(h2_col.value(row)), &dist) !=
            E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(dist);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3GetUnidirectionalEdge(origin BIGINT, destination BIGINT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_get_unidirectional_edge(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h1_col(columns[0]);
    ColumnViewer<TYPE_BIGINT> h2_col(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h1_col.is_null(row) || h2_col.is_null(row)) {
            result.append_null();
            continue;
        }
        H3Index edge;
        if (cellsToDirectedEdge(static_cast<H3Index>(h1_col.value(row)), static_cast<H3Index>(h2_col.value(row)),
                                &edge) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(static_cast<int64_t>(edge));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// H3 index + resolution → H3 index
// ────────────────────────────────────────────────────────────────────────────

// h3ToParent(h3index BIGINT, resolution INT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_to_parent(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    ColumnViewer<TYPE_INT> res_col(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row) || res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        H3Index parent;
        if (cellToParent(static_cast<H3Index>(h3_col.value(row)), res_col.value(row), &parent) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(static_cast<int64_t>(parent));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3ToCenterChild(h3index BIGINT, resolution INT) -> BIGINT
StatusOr<ColumnPtr> H3Functions::h3_to_center_child(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    ColumnViewer<TYPE_INT> res_col(columns[1]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row) || res_col.is_null(row)) {
            result.append_null();
            continue;
        }
        H3Index child;
        if (cellToCenterChild(static_cast<H3Index>(h3_col.value(row)), res_col.value(row), &child) != E_SUCCESS) {
            result.append_null();
            continue;
        }
        result.append(static_cast<int64_t>(child));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// Four doubles → double (great-circle distance)
// ────────────────────────────────────────────────────────────────────────────

// h3PointDistM(lat1, lon1, lat2, lon2 DOUBLE) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_point_dist_m(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> lat1(columns[0]), lon1(columns[1]);
    ColumnViewer<TYPE_DOUBLE> lat2(columns[2]), lon2(columns[3]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (lat1.is_null(row) || lon1.is_null(row) || lat2.is_null(row) || lon2.is_null(row)) {
            result.append_null();
            continue;
        }
        LatLng a{degsToRads(lat1.value(row)), degsToRads(lon1.value(row))};
        LatLng b{degsToRads(lat2.value(row)), degsToRads(lon2.value(row))};
        result.append(greatCircleDistanceM(&a, &b));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3PointDistKm(lat1, lon1, lat2, lon2 DOUBLE) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_point_dist_km(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> lat1(columns[0]), lon1(columns[1]);
    ColumnViewer<TYPE_DOUBLE> lat2(columns[2]), lon2(columns[3]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (lat1.is_null(row) || lon1.is_null(row) || lat2.is_null(row) || lon2.is_null(row)) {
            result.append_null();
            continue;
        }
        LatLng a{degsToRads(lat1.value(row)), degsToRads(lon1.value(row))};
        LatLng b{degsToRads(lat2.value(row)), degsToRads(lon2.value(row))};
        result.append(greatCircleDistanceKm(&a, &b));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// h3PointDistRads(lat1, lon1, lat2, lon2 DOUBLE) -> DOUBLE
StatusOr<ColumnPtr> H3Functions::h3_point_dist_rads(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> lat1(columns[0]), lon1(columns[1]);
    ColumnViewer<TYPE_DOUBLE> lat2(columns[2]), lon2(columns[3]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (lat1.is_null(row) || lon1.is_null(row) || lat2.is_null(row) || lon2.is_null(row)) {
            result.append_null();
            continue;
        }
        LatLng a{degsToRads(lat1.value(row)), degsToRads(lon1.value(row))};
        LatLng b{degsToRads(lat2.value(row)), degsToRads(lon2.value(row))};
        result.append(greatCircleDistanceRads(&a, &b));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// ────────────────────────────────────────────────────────────────────────────
// Array-returning functions
// ────────────────────────────────────────────────────────────────────────────

// h3kRing(h3index BIGINT, k INT) -> ARRAY(BIGINT)
StatusOr<ColumnPtr> H3Functions::h3k_ring(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    ColumnViewer<TYPE_INT> k_col(columns[1]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    std::vector<std::vector<int64_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row) || k_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        int64_t buf_size;
        if (maxGridDiskSize(k_col.value(row), &buf_size) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        std::vector<H3Index> buf(buf_size, 0);
        if (gridDisk(static_cast<H3Index>(h3_col.value(row)), k_col.value(row), buf.data()) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto h : buf) {
            if (h != 0) rows[row].push_back(static_cast<int64_t>(h));
        }
    }
    return build_bigint_array(rows, nulls, all_const);
}

// h3ToChildren(h3index BIGINT, resolution INT) -> ARRAY(BIGINT)
StatusOr<ColumnPtr> H3Functions::h3_to_children(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    ColumnViewer<TYPE_INT> res_col(columns[1]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    std::vector<std::vector<int64_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row) || res_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        H3Index h = static_cast<H3Index>(h3_col.value(row));
        int64_t child_size;
        if (cellToChildrenSize(h, res_col.value(row), &child_size) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        std::vector<H3Index> buf(child_size, 0);
        if (cellToChildren(h, res_col.value(row), buf.data()) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto c : buf) {
            if (c != 0) rows[row].push_back(static_cast<int64_t>(c));
        }
    }
    return build_bigint_array(rows, nulls, all_const);
}

// h3GetFaces(h3index BIGINT) -> ARRAY(INT)
StatusOr<ColumnPtr> H3Functions::h3_get_faces(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    std::vector<std::vector<int32_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        H3Index h = static_cast<H3Index>(h3_col.value(row));
        int max_faces;
        if (maxFaceCount(h, &max_faces) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        std::vector<int> buf(max_faces, -1);
        if (getIcosahedronFaces(h, buf.data()) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto f : buf) {
            if (f != -1) rows[row].push_back(static_cast<int32_t>(f));
        }
    }
    return build_int_array(rows, nulls, all_const);
}

// h3GetRes0Indexes() -> ARRAY(BIGINT)
// No input columns; returns the same 122-element array as a const result.
StatusOr<ColumnPtr> H3Functions::h3_get_res0_indexes(FunctionContext* ctx, const Columns& columns) {
    int64_t count = res0CellCount(); // = 122
    std::vector<H3Index> buf(count);
    if (getRes0Cells(buf.data()) != E_SUCCESS) {
        // Failure is very unlikely; return NULL.
        auto offsets = UInt32Column::create();
        auto elements = Int64Column::create();
        offsets->append(0);
        offsets->append(0);
        auto arr = ArrayColumn::create(std::move(elements), std::move(offsets));
        auto nc = NullColumn::create();
        nc->get_data().push_back(1);
        return NullableColumn::create(std::move(arr), std::move(nc));
    }

    auto offsets = UInt32Column::create();
    auto elements = Int64Column::create();
    offsets->append(0);
    elements->reserve(count);
    for (auto h : buf) elements->append(static_cast<int64_t>(h));
    offsets->append(static_cast<uint32_t>(count));

    ColumnPtr arr = ArrayColumn::create(std::move(elements), std::move(offsets));
    return ConstColumn::create(std::move(arr), 1);
}

// h3GetPentagonIndexes(resolution INT) -> ARRAY(BIGINT)
StatusOr<ColumnPtr> H3Functions::h3_get_pentagon_indexes(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_INT> res_col(columns[0]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    int pcount = pentagonCount(); // = 12
    std::vector<std::vector<int64_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (res_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        std::vector<H3Index> buf(pcount, 0);
        if (getPentagons(res_col.value(row), buf.data()) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto h : buf) {
            if (h != 0) rows[row].push_back(static_cast<int64_t>(h));
        }
    }
    return build_bigint_array(rows, nulls, all_const);
}

// h3Line(start BIGINT, end BIGINT) -> ARRAY(BIGINT)
StatusOr<ColumnPtr> H3Functions::h3_line(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h1_col(columns[0]);
    ColumnViewer<TYPE_BIGINT> h2_col(columns[1]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    std::vector<std::vector<int64_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (h1_col.is_null(row) || h2_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        H3Index start = static_cast<H3Index>(h1_col.value(row));
        H3Index end = static_cast<H3Index>(h2_col.value(row));
        int64_t path_size;
        if (gridPathCellsSize(start, end, &path_size) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        std::vector<H3Index> buf(path_size, 0);
        if (gridPathCells(start, end, buf.data()) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto h : buf) {
            if (h != 0) rows[row].push_back(static_cast<int64_t>(h));
        }
    }
    return build_bigint_array(rows, nulls, all_const);
}

// h3HexRing(h3index BIGINT, k INT) -> ARRAY(BIGINT)
StatusOr<ColumnPtr> H3Functions::h3_hex_ring(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    ColumnViewer<TYPE_INT> k_col(columns[1]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    std::vector<std::vector<int64_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row) || k_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        int k = k_col.value(row);
        int64_t ring_size = (k == 0) ? 1 : (int64_t)6 * k;
        std::vector<H3Index> buf(ring_size, 0);
        if (gridRingUnsafe(static_cast<H3Index>(h3_col.value(row)), k, buf.data()) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto h : buf) {
            if (h != 0) rows[row].push_back(static_cast<int64_t>(h));
        }
    }
    return build_bigint_array(rows, nulls, all_const);
}

// h3GetUnidirectionalEdgesFromHexagon(h3index BIGINT) -> ARRAY(BIGINT)
StatusOr<ColumnPtr> H3Functions::h3_get_unidirectional_edges_from_hexagon(FunctionContext* ctx,
                                                                          const Columns& columns) {
    ColumnViewer<TYPE_BIGINT> h3_col(columns[0]);
    auto size = columns[0]->size();
    bool all_const = ColumnHelper::is_all_const(columns);

    // Max 6 directed edges per cell (5 for pentagons; unused slots are 0).
    static constexpr int kMaxEdges = 6;
    std::vector<std::vector<int64_t>> rows(size);
    std::vector<bool> nulls(size, false);

    for (int row = 0; row < size; ++row) {
        if (h3_col.is_null(row)) {
            nulls[row] = true;
            continue;
        }
        H3Index buf[kMaxEdges] = {};
        if (originToDirectedEdges(static_cast<H3Index>(h3_col.value(row)), buf) != E_SUCCESS) {
            nulls[row] = true;
            continue;
        }
        for (auto e : buf) {
            if (e != 0) rows[row].push_back(static_cast<int64_t>(e));
        }
    }
    return build_bigint_array(rows, nulls, all_const);
}

} // namespace starrocks

#include "gen_cpp/opcode/H3Functions.inc"
