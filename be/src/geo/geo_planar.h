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

#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include "geo/wkb.h"

namespace starrocks {

template <typename Coordinate>
struct PlanarModels {
    using Point = boost::geometry::model::d2::point_xy<Coordinate>;
    using Polygon = boost::geometry::model::polygon<Point, false, true>;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
};

using PlanarPoint = PlanarModels<double>::Point;
using PlanarPolygon = PlanarModels<double>::Polygon;
using PlanarMultiPolygon = PlanarModels<double>::MultiPolygon;
using PlanarLine = boost::geometry::model::linestring<PlanarPoint>;
using PlanarMultiLine = boost::geometry::model::multi_linestring<PlanarLine>;
using PlanarMultiPoint = boost::geometry::model::multi_point<PlanarPoint>;

inline long double signed_ring_area2(const std::vector<WkbCoordinate>& ring) {
    if (ring.empty()) return 0;
    const auto origin = ring.front();
    long double area = 0;
    for (size_t i = 1; i < ring.size(); ++i) {
        const long double x0 = static_cast<long double>(ring[i - 1].x) - origin.x;
        const long double y0 = static_cast<long double>(ring[i - 1].y) - origin.y;
        const long double x1 = static_cast<long double>(ring[i].x) - origin.x;
        const long double y1 = static_cast<long double>(ring[i].y) - origin.y;
        area += x0 * y1 - x1 * y0;
    }
    return area;
}

template <typename Ring>
void append_planar_ring(const std::vector<WkbCoordinate>& input, bool ccw, Ring* output, WkbCoordinate origin = {}) {
    const bool reverse = (signed_ring_area2(input) > 0) != ccw;
    output->reserve(input.size());
    // WkbCodec verifies closure. Adapt orientation only, without closing or repairing rings.
    auto append = [&](const WkbCoordinate& point) {
        using Coordinate = typename boost::geometry::coordinate_type<typename Ring::value_type>::type;
        output->emplace_back(static_cast<Coordinate>(point.x) - static_cast<Coordinate>(origin.x),
                             static_cast<Coordinate>(point.y) - static_cast<Coordinate>(origin.y));
    };
    if (reverse) {
        for (auto it = input.rbegin(); it != input.rend(); ++it) append(*it);
    } else {
        for (const auto& point : input) append(point);
    }
}

template <typename Polygon = PlanarPolygon>
Polygon to_planar_polygon(const WkbGeometry& geometry, WkbCoordinate origin = {}) {
    Polygon result;
    if (geometry.empty) return result;
    append_planar_ring(geometry.rings.front(), true, &result.outer(), origin);
    result.inners().resize(geometry.rings.size() - 1);
    for (size_t i = 1; i < geometry.rings.size(); ++i) {
        append_planar_ring(geometry.rings[i], false, &result.inners()[i - 1], origin);
    }
    return result;
}

} // namespace starrocks
