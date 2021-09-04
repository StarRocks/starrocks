// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/geo/geo_functions.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "geo/geo_common.h"
#include "udf/udf.h"

namespace starrocks {

class GeoFunctions {
public:
    static void init();

    // compute distance between two points in earth sphere
    static DoubleVal st_distance_sphere(FunctionContext* ctx, const DoubleVal& x_lng, const DoubleVal& x_lat,
                                        const DoubleVal& y_lng, const DoubleVal& y_lat);

    // point
    static starrocks_udf::StringVal st_point(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& x,
                                             const starrocks_udf::DoubleVal& y);

    static starrocks_udf::DoubleVal st_x(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& point);
    static starrocks_udf::DoubleVal st_y(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& point);

    // to wkt
    static starrocks_udf::StringVal st_as_wkt(starrocks_udf::FunctionContext* ctx,
                                              const starrocks_udf::StringVal& shape);
    // from wkt
    static void st_from_wkt_prepare_common(starrocks_udf::FunctionContext*,
                                           starrocks_udf::FunctionContext::FunctionStateScope, GeoShapeType shape_type);
    static void st_from_wkt_close(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);
    static starrocks_udf::StringVal st_from_wkt_common(starrocks_udf::FunctionContext* ctx,
                                                       const starrocks_udf::StringVal& wkt, GeoShapeType shape_type);

    static void st_from_wkt_prepare(starrocks_udf::FunctionContext* ctx,
                                    starrocks_udf::FunctionContext::FunctionStateScope scope) __attribute__((used)) {
        st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_ANY);
    }
    static starrocks_udf::StringVal st_from_wkt(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::StringVal& wkt) __attribute__((used)) {
        return st_from_wkt_common(ctx, wkt, GEO_SHAPE_ANY);
    }

    // for line
    static void st_line_prepare(starrocks_udf::FunctionContext* ctx,
                                starrocks_udf::FunctionContext::FunctionStateScope scope) __attribute__((used)) {
        st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_LINE_STRING);
    }
    static starrocks_udf::StringVal st_line(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& wkt)
            __attribute__((used)) {
        return st_from_wkt_common(ctx, wkt, GEO_SHAPE_LINE_STRING);
    }

    // for polygon
    static void st_polygon_prepare(starrocks_udf::FunctionContext* ctx,
                                   starrocks_udf::FunctionContext::FunctionStateScope scope) __attribute__((used)) {
        st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_POLYGON);
    }
    static starrocks_udf::StringVal st_polygon(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& wkt)
            __attribute__((used)) {
        return st_from_wkt_common(ctx, wkt, GEO_SHAPE_POLYGON);
    }

    // for circle
    static starrocks_udf::StringVal st_circle(starrocks_udf::FunctionContext* ctx,
                                              const starrocks_udf::DoubleVal& center_lng,
                                              const starrocks_udf::DoubleVal& center_lat,
                                              const starrocks_udf::DoubleVal& radius_meter);
    static void st_circle_prepare(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);

    // Returns true if and only if no points of the second geometry
    // lie in the exterior of the first geometry, and at least one
    // point of the interior of the first geometry lies in the
    // interior of the second geometry.
    static starrocks_udf::BooleanVal st_contains(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::StringVal& lhs,
                                                 const starrocks_udf::StringVal& rhs);
    static void st_contains_prepare(starrocks_udf::FunctionContext*,
                                    starrocks_udf::FunctionContext::FunctionStateScope);
    static void st_contains_close(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);
};

} // namespace starrocks
