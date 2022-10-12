// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "geo/geo_common.h"
#include "udf/udf.h"

namespace starrocks {
namespace vectorized {

class GeoFunctions {
public:
    /**
     * @param: [double_value, double_value]
     * @paramType: [DoubleColumn, DoubleColumn]
     * @return: BinaryColumn
     * Through the given X coordinate value, y coordinate value returns the corresponding point.
     */
    static ColumnPtr st_point(FunctionContext* context, const Columns& columns);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: DoubleColumn
     * Returns the corresponding X coordinate value
     */
    static ColumnPtr st_x(FunctionContext* context, const Columns& columns);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: DoubleColumn
     * Returns the corresponding y coordinate value
     */
    static ColumnPtr st_y(FunctionContext* context, const Columns& columns);

    /**
     * @param: [double_value, double_value, double_value, double_value]
     * @paramType: [DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn]
     * @return: DoubleColumn
     * Calculate the spherical distance between two points on the earth, in meters.
     */
    static ColumnPtr st_distance_sphere(FunctionContext* context, const Columns& columns);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Transfer this Text to Well Known Text.
     */
    static ColumnPtr st_as_wkt(FunctionContext* context, const Columns& columns);

    // from wkt
    static Status st_from_wkt_prepare_common(FunctionContext*, FunctionContext::FunctionStateScope,
                                             GeoShapeType shape_type);

    static Status st_from_wkt_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope)
            __attribute__((used));

    static Status st_line_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope)
            __attribute__((used));

    static Status st_polygon_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope)
            __attribute__((used));

    static Status st_from_wkt_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static ColumnPtr st_from_wkt_common(FunctionContext* ctx, const Columns& columns, GeoShapeType shape_type);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Convert a wkt (well known text) into the corresponding memory geometry
     */
    static ColumnPtr st_from_wkt(FunctionContext* ctx, const Columns& columns);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Convert a wkt (well known text) into a memory representation of line form
     */
    static ColumnPtr st_line(FunctionContext* ctx, const Columns& columns);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Convert a wkt (well known text) to the corresponding polygon memory form
     */
    static ColumnPtr st_polygon(FunctionContext* ctx, const Columns& columns);

    static Status st_circle_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    /**
     * @param: [double_value, double_value, double_value]
     * @paramType: [DoubleColumn, DoubleColumn, DoubleColumn]
     * @return: BinaryColumn
     * Transfer this WKT to a circle on earth's surface.
     */
    static ColumnPtr st_circle(FunctionContext* ctx, const Columns& columns);

    static Status st_contains_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    static Status st_contains_prepare(FunctionContext*, FunctionContext::FunctionStateScope);

    /**
     * @param: [string_value, string_value]
     * @paramType: [DoubleColumn, DoubleColumn]
     * @return: BinaryColumn
     * Judge whether shape1(columns[0]) can completely contain shape2(columns2)
     */
    static ColumnPtr st_contains(FunctionContext* ctx, const Columns& columns);
};

} // namespace vectorized
} // namespace starrocks
