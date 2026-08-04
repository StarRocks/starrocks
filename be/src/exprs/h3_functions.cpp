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

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"

namespace starrocks {

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

} // namespace starrocks
