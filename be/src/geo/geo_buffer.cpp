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

#include "geo/geo_buffer.h"

#include <cmath>
#include <exception>
#include <variant>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#define BOOST_GEOMETRY_NO_ROBUSTNESS
#include <boost/geometry.hpp>

#include "geo/geo_planar.h"

namespace starrocks {
namespace {
namespace bg = boost::geometry;
using Point = PlanarModels<long double>::Point;
using Line = bg::model::linestring<Point>;
using MultiPoint = bg::model::multi_point<Point>;
using MultiLine = bg::model::multi_linestring<Line>;
using Polygon = PlanarModels<long double>::Polygon;
using MultiPolygon = PlanarModels<long double>::MultiPolygon;
constexpr auto kSemantics = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
constexpr size_t kMaxInputBytes = 256 * 1024;

size_t count_coordinates(const WkbGeometry& geometry) {
    size_t count = geometry.coordinates.size();
    for (const auto& ring : geometry.rings) count += ring.size();
    for (const auto& child : geometry.children) count += count_coordinates(child);
    return count;
}

WkbCoordinate first_coordinate(const WkbGeometry& geometry) {
    if (!geometry.coordinates.empty()) return geometry.coordinates.front();
    if (!geometry.rings.empty()) return geometry.rings.front().front();
    for (const auto& child : geometry.children) {
        if (count_coordinates(child) != 0) return first_coordinate(child);
    }
    return {};
}

MultiPolygon polygon_model(const WkbGeometry& geometry, WkbCoordinate origin) {
    MultiPolygon result;
    if (geometry.type == WkbGeometryType::POLYGON) {
        if (!geometry.empty) result.emplace_back(to_planar_polygon<Polygon>(geometry, origin));
    } else {
        result.reserve(geometry.children.size());
        for (const auto& child : geometry.children) {
            if (!child.empty) result.emplace_back(to_planar_polygon<Polygon>(child, origin));
        }
    }
    return result;
}

Status check_translation(const WkbGeometry& geometry, WkbCoordinate origin) {
    auto check = [origin](const WkbCoordinate& coordinate) {
        const long double x = static_cast<long double>(coordinate.x) - origin.x;
        const long double y = static_cast<long double>(coordinate.y) - origin.y;
        return static_cast<double>(x + origin.x) == coordinate.x && static_cast<double>(y + origin.y) == coordinate.y;
    };
    for (const auto& coordinate : geometry.coordinates) {
        if (!check(coordinate)) return Status::InvalidArgument("ST_Buffer input loses precision in local coordinates");
    }
    for (const auto& ring : geometry.rings) {
        for (const auto& coordinate : ring) {
            if (!check(coordinate))
                return Status::InvalidArgument("ST_Buffer input loses precision in local coordinates");
        }
    }
    for (const auto& child : geometry.children) RETURN_IF_ERROR(check_translation(child, origin));
    return Status::OK();
}

StatusOr<WkbGeometry> to_geometry(const MultiPolygon& polygons, WkbCoordinate origin) {
    WkbGeometry result;
    result.type = WkbGeometryType::POLYGON;
    result.empty = polygons.empty();
    if (polygons.empty()) return result;
    auto convert = [origin](const Polygon& polygon) -> StatusOr<WkbGeometry> {
        WkbGeometry child;
        child.type = WkbGeometryType::POLYGON;
        auto append = [&](const auto& ring) -> Status {
            auto& target = child.rings.emplace_back();
            target.reserve(ring.size());
            for (const auto& point : ring) {
                WkbCoordinate value{static_cast<double>(point.x() + origin.x),
                                    static_cast<double>(point.y() + origin.y)};
                if (!std::isfinite(value.x) || !std::isfinite(value.y)) {
                    return Status::InvalidArgument("ST_Buffer result has non-finite coordinates");
                }
                target.emplace_back(value);
            }
            return Status::OK();
        };
        RETURN_IF_ERROR(append(polygon.outer()));
        for (const auto& hole : polygon.inners()) RETURN_IF_ERROR(append(hole));
        return child;
    };
    if (polygons.size() == 1) {
        ASSIGN_OR_RETURN(result, convert(polygons.front()));
    } else {
        result.type = WkbGeometryType::MULTIPOLYGON;
        result.children.reserve(polygons.size());
        for (const auto& polygon : polygons) {
            ASSIGN_OR_RETURN(auto child, convert(polygon));
            result.children.emplace_back(std::move(child));
        }
    }
    // Extended-precision topology must survive the actual WKB double coordinates.
    if (!bg::is_valid(polygon_model(result, origin))) {
        return Status::InvalidArgument("ST_Buffer result loses topology at double precision");
    }
    return result;
}
} // namespace

struct PreparedGeoBuffer::Impl {
    std::variant<MultiPoint, MultiLine, MultiPolygon> model;
    WkbCoordinate origin;
    bool polygonal = false;
};

PreparedGeoBuffer::PreparedGeoBuffer(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
PreparedGeoBuffer::~PreparedGeoBuffer() = default;

StatusOr<std::unique_ptr<PreparedGeoBuffer>> PreparedGeoBuffer::prepare(Slice wkb,
                                                                        const std::function<Status()>& checkpoint) {
    try {
        auto check = [&]() { return checkpoint ? checkpoint() : Status::OK(); };
        RETURN_IF_ERROR(check());
        if (wkb.size > kMaxInputBytes) return Status::InvalidArgument("ST_Buffer input exceeds 256 KiB");
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(wkb, &geometry, kSemantics));
        if (geometry.type == WkbGeometryType::GEOMETRYCOLLECTION) {
            return Status::NotSupported("ST_Buffer does not support GEOMETRYCOLLECTION input");
        }
        if (count_coordinates(geometry) > kGeoBufferMaxInputCoordinates) {
            return Status::InvalidArgument("ST_Buffer input exceeds coordinate limit");
        }
        auto impl = std::make_unique<Impl>();
        impl->origin = first_coordinate(geometry);
        RETURN_IF_ERROR(check_translation(geometry, impl->origin));
        auto point = [&](WkbCoordinate value) {
            return Point(static_cast<long double>(value.x) - impl->origin.x,
                         static_cast<long double>(value.y) - impl->origin.y);
        };
        switch (geometry.type) {
        case WkbGeometryType::POINT:
        case WkbGeometryType::MULTIPOINT: {
            MultiPoint model;
            if (geometry.type == WkbGeometryType::POINT) {
                if (!geometry.empty) model.emplace_back(point(geometry.coordinates.front()));
            } else {
                model.reserve(geometry.children.size());
                for (const auto& child : geometry.children) {
                    if (!child.empty) model.emplace_back(point(child.coordinates.front()));
                }
            }
            impl->model = std::move(model);
            break;
        }
        case WkbGeometryType::LINESTRING:
        case WkbGeometryType::MULTILINESTRING: {
            MultiLine model;
            auto append = [&](const WkbGeometry& child) {
                if (child.empty) return;
                auto& line = model.emplace_back();
                line.reserve(child.coordinates.size());
                for (const auto& coordinate : child.coordinates) line.emplace_back(point(coordinate));
            };
            if (geometry.type == WkbGeometryType::LINESTRING) {
                append(geometry);
            } else {
                model.reserve(geometry.children.size());
                for (const auto& child : geometry.children) append(child);
            }
            impl->model = std::move(model);
            break;
        }
        case WkbGeometryType::POLYGON:
        case WkbGeometryType::MULTIPOLYGON:
            impl->polygonal = true;
            impl->model = polygon_model(geometry, impl->origin);
            break;
        default:
            return Status::NotSupported("ST_Buffer input family is unsupported");
        }
        RETURN_IF_ERROR(check());
        const bool valid =
                std::visit([](const auto& model) { return model.empty() || bg::is_valid(model); }, impl->model);
        if (!valid) return Status::InvalidArgument("ST_Buffer input has invalid topology");
        RETURN_IF_ERROR(check());
        return std::unique_ptr<PreparedGeoBuffer>(new PreparedGeoBuffer(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("ST_Buffer preparation allocation failed");
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("ST_Buffer preparation failed: ") + error.what());
    }
}

StatusOr<WkbGeometry> PreparedGeoBuffer::buffer(double distance, const std::function<Status()>& checkpoint) const {
    try {
        auto check = [&]() { return checkpoint ? checkpoint() : Status::OK(); };
        RETURN_IF_ERROR(check());
        if (!std::isfinite(distance)) return Status::InvalidArgument("ST_Buffer requires a finite distance");
        const bool empty = std::visit([](const auto& model) { return model.empty(); }, _impl->model);
        MultiPolygon output;
        if (!empty && _impl->polygonal && distance == 0) {
            output = std::get<MultiPolygon>(_impl->model);
        } else if (!empty && (_impl->polygonal || distance > 0)) {
            // Boost 1.80 also simplifies the input at abs(distance) / 1000.
            const bg::strategy::buffer::distance_symmetric<long double> distance_strategy(distance);
            const bg::strategy::buffer::side_straight side_strategy;
            const bg::strategy::buffer::join_round join_strategy(kGeoBufferPointsPerCircle);
            const bg::strategy::buffer::end_round end_strategy(kGeoBufferPointsPerCircle);
            const bg::strategy::buffer::point_circle point_strategy(kGeoBufferPointsPerCircle);
            std::visit(
                    [&](const auto& model) {
                        bg::buffer(model, output, distance_strategy, side_strategy, join_strategy, end_strategy,
                                   point_strategy);
                    },
                    _impl->model);
            if (distance > 0 && output.empty()) {
                return Status::InvalidArgument("ST_Buffer lost a non-empty input at working precision");
            }
        }
        RETURN_IF_ERROR(check());
        if (bg::num_points(output) > kGeoBufferMaxOutputCoordinates) {
            return Status::InvalidArgument("ST_Buffer result exceeds coordinate limit");
        }
        if (!output.empty() && !bg::is_valid(output)) {
            return Status::InvalidArgument("ST_Buffer produced invalid topology");
        }
        ASSIGN_OR_RETURN(auto result, to_geometry(output, _impl->origin));
        RETURN_IF_ERROR(check());
        return result;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("ST_Buffer allocation failed");
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("ST_Buffer failed: ") + error.what());
    }
}
} // namespace starrocks
