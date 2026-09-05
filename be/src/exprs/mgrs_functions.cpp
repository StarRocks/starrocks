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

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/mgrs_math.h"

namespace starrocks {

using namespace mgrs_detail;

// geoToMGRS(lng DOUBLE, lat DOUBLE [, precision INT]) -> VARCHAR
StatusOr<ColumnPtr> MgrsFunctions::geo_to_mgrs(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> lng_col(columns[0]);
    ColumnViewer<TYPE_DOUBLE> lat_col(columns[1]);

    const bool has_precision = columns.size() >= 3;
    // Use a viewer only when the column is actually present.
    std::unique_ptr<ColumnViewer<TYPE_INT>> prec_col;
    if (has_precision) prec_col = std::make_unique<ColumnViewer<TYPE_INT>>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    for (int row = 0; row < size; ++row) {
        if (lng_col.is_null(row) || lat_col.is_null(row) || (has_precision && prec_col->is_null(row))) {
            result.append_null();
            continue;
        }

        const double lng = lng_col.value(row);
        const double lat = lat_col.value(row);

        // Validate domain before calling the math.
        if (lat < -80.0 || lat > 84.0 || lng < -180.0 || lng > 180.0) {
            result.append_null();
            continue;
        }

        uint8_t precision = 5;
        if (has_precision) {
            const int32_t p = prec_col->value(row);
            if (p < 0 || p > 5) {
                result.append_null();
                continue;
            }
            precision = static_cast<uint8_t>(p);
        }

        const std::string mgrs = mgrsEncode(lng, lat, precision);
        if (mgrs.empty()) {
            result.append_null();
            continue;
        }
        result.append(Slice(mgrs));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// MGRSToLat(mgrs VARCHAR) -> DOUBLE
StatusOr<ColumnPtr> MgrsFunctions::mgrs_to_lat(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);

    for (int row = 0; row < size; ++row) {
        if (str_col.is_null(row)) {
            result.append_null();
            continue;
        }
        Slice s = str_col.value(row);
        double lon, lat;
        if (!mgrsDecode(std::string_view(s.data, s.size), lon, lat)) {
            result.append_null();
            continue;
        }
        result.append(lat);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// MGRSToLng(mgrs VARCHAR) -> DOUBLE
StatusOr<ColumnPtr> MgrsFunctions::mgrs_to_lng(FunctionContext* ctx, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_col(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);

    for (int row = 0; row < size; ++row) {
        if (str_col.is_null(row)) {
            result.append_null();
            continue;
        }
        Slice s = str_col.value(row);
        double lon, lat;
        if (!mgrsDecode(std::string_view(s.data, s.size), lon, lat)) {
            result.append_null();
            continue;
        }
        result.append(lon);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/MgrsFunctions.inc"
