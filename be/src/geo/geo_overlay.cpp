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

#include "geo/geo_overlay.h"

#include <cmath>
#include <exception>
#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
// Boost 1.80's default is no rescaling. Keep the precision contract explicit.
#define BOOST_GEOMETRY_NO_ROBUSTNESS
#include <boost/geometry.hpp>

#include "geo/geo_planar.h"

namespace starrocks {
namespace {
namespace bg = boost::geometry;
using OverlayPolygon = PlanarModels<long double>::Polygon;
using OverlayMultiPolygon = PlanarModels<long double>::MultiPolygon;
constexpr auto kSemantics = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
constexpr size_t kMaxInputBytes = 256 * 1024;

size_t coordinate_count(const WkbGeometry& geometry) {
    size_t count = 0;
    for (const auto& ring : geometry.rings) count += ring.size();
    for (const auto& child : geometry.children) count += coordinate_count(child);
    return count;
}

OverlayMultiPolygon to_overlay_model(const WkbGeometry& geometry, WkbCoordinate origin) {
    OverlayMultiPolygon result;
    if (geometry.type == WkbGeometryType::POLYGON) {
        if (!geometry.empty) result.emplace_back(to_planar_polygon<OverlayPolygon>(geometry, origin));
    } else {
        result.reserve(geometry.children.size());
        for (const auto& child : geometry.children) {
            if (!child.empty) result.emplace_back(to_planar_polygon<OverlayPolygon>(child, origin));
        }
    }
    return result;
}

bool valid(const OverlayMultiPolygon& model) {
    return model.empty() || bg::is_valid(model);
}
} // namespace

struct PreparedGeoPolygon::Impl {
    OverlayMultiPolygon polygons;
    WkbCoordinate origin;
    size_t coordinates = 0;
};

PreparedGeoPolygon::PreparedGeoPolygon(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
PreparedGeoPolygon::~PreparedGeoPolygon() = default;

StatusOr<std::unique_ptr<PreparedGeoPolygon>> PreparedGeoPolygon::prepare(Slice wkb) {
    try {
        if (wkb.size > kMaxInputBytes) return Status::InvalidArgument("Polygon overlay input exceeds 256 KiB");
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(wkb, &geometry, kSemantics));
        if (geometry.type != WkbGeometryType::POLYGON && geometry.type != WkbGeometryType::MULTIPOLYGON) {
            return Status::InvalidArgument("Polygon overlay requires POLYGON or MULTIPOLYGON inputs");
        }
        auto impl = std::make_unique<Impl>();
        impl->coordinates = coordinate_count(geometry);
        if (impl->coordinates > kGeoOverlayMaxCoordinates ||
            impl->coordinates * impl->coordinates > kGeoOverlayMaxWork) {
            return Status::InvalidArgument("Polygon overlay input exceeds coordinate/work limit");
        }
        if (!geometry.rings.empty()) impl->origin = geometry.rings.front().front();
        for (const auto& child : geometry.children) {
            if (!child.empty) {
                impl->origin = child.rings.front().front();
                break;
            }
        }
        impl->polygons = to_overlay_model(geometry, impl->origin);
        if (!valid(impl->polygons)) return Status::InvalidArgument("Polygon overlay input has invalid topology");
        return std::unique_ptr<PreparedGeoPolygon>(new PreparedGeoPolygon(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Polygon overlay preparation allocation failed");
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("Polygon overlay preparation failed: ") + error.what());
    }
}

StatusOr<WkbGeometry> PreparedGeoPolygon::overlay(const PreparedGeoPolygon& right, GeoOverlayKind kind,
                                                  const std::function<Status()>& checkpoint) const {
    try {
        auto check = [&]() { return checkpoint ? checkpoint() : Status::OK(); };
        RETURN_IF_ERROR(check());
        const auto n = _impl->coordinates;
        const auto m = right._impl->coordinates;
        if (n * n + m * m + n * m > kGeoOverlayMaxWork) {
            return Status::InvalidArgument("Polygon overlay pair exceeds work limit");
        }
        // Prepared models remain immutable. Only the other operand needs translation
        // into this operand's local coordinate system; all Boost scratch is call-local.
        auto right_model = right._impl->polygons;
        const auto origin = _impl->polygons.empty() ? right._impl->origin : _impl->origin;
        const long double dx = static_cast<long double>(right._impl->origin.x) - origin.x;
        const long double dy = static_cast<long double>(right._impl->origin.y) - origin.y;
        bg::for_each_point(right_model, [&](auto& point) {
            point.x(point.x() + dx);
            point.y(point.y() + dy);
        });
        OverlayMultiPolygon output;
        switch (kind) {
        case GeoOverlayKind::UNION:
            bg::union_(_impl->polygons, right_model, output);
            break;
        case GeoOverlayKind::DIFFERENCE:
            bg::difference(_impl->polygons, right_model, output);
            break;
        case GeoOverlayKind::SYMMETRIC_DIFFERENCE: {
            // Boost 1.80 sym_difference computes two differences followed by a union.
            // Use those public primitives explicitly to bound the intermediate pair
            // and observe query cancellation/memory limits between the three calls.
            OverlayMultiPolygon left_only, right_only;
            bg::difference(_impl->polygons, right_model, left_only);
            RETURN_IF_ERROR(check());
            const auto left_count = bg::num_points(left_only);
            if (left_count > kGeoOverlayMaxOutputCoordinates) {
                return Status::InvalidArgument("Polygon overlay intermediate exceeds coordinate limit");
            }
            bg::difference(right_model, _impl->polygons, right_only);
            RETURN_IF_ERROR(check());
            const auto right_count = bg::num_points(right_only);
            if (right_count > kGeoOverlayMaxOutputCoordinates ||
                left_count * left_count + right_count * right_count + left_count * right_count > kGeoOverlayMaxWork) {
                return Status::InvalidArgument("Polygon overlay intermediate exceeds coordinate/work limit");
            }
            bg::union_(left_only, right_only, output);
            break;
        }
        }
        RETURN_IF_ERROR(check());
        if (bg::num_points(output) > kGeoOverlayMaxOutputCoordinates) {
            return Status::InvalidArgument("Polygon overlay result exceeds coordinate limit");
        }
        if (!valid(output)) return Status::InvalidArgument("Polygon overlay produced invalid topology");

        WkbGeometry result;
        result.type = WkbGeometryType::POLYGON;
        result.empty = output.empty();
        if (output.empty()) return result; // Explicit POLYGON EMPTY, never NULL.
        auto convert = [&](const OverlayPolygon& polygon) -> StatusOr<WkbGeometry> {
            WkbGeometry child;
            child.type = WkbGeometryType::POLYGON;
            auto ring = [&](const auto& source) -> Status {
                auto& target = child.rings.emplace_back();
                target.reserve(source.size());
                for (const auto& point : source) {
                    const double x = static_cast<double>(point.x() + origin.x);
                    const double y = static_cast<double>(point.y() + origin.y);
                    if (!std::isfinite(x) || !std::isfinite(y)) {
                        return Status::InvalidArgument("Polygon overlay result has non-finite coordinates");
                    }
                    target.emplace_back(WkbCoordinate{x, y});
                }
                return Status::OK();
            };
            RETURN_IF_ERROR(ring(polygon.outer()));
            for (const auto& hole : polygon.inners()) RETURN_IF_ERROR(ring(hole));
            return child;
        };
        if (output.size() == 1) {
            ASSIGN_OR_RETURN(result, convert(output.front()));
        } else {
            result.type = WkbGeometryType::MULTIPOLYGON;
            result.children.reserve(output.size());
            for (const auto& polygon : output) {
                ASSIGN_OR_RETURN(auto child, convert(polygon));
                result.children.emplace_back(std::move(child));
            }
        }
        // A valid extended-precision result can collapse on serialization to double.
        // Reject it rather than silently repairing, snapping, or dropping components.
        if (!valid(to_overlay_model(result, origin))) {
            return Status::InvalidArgument("Polygon overlay result loses topology at double precision");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Polygon overlay allocation failed");
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("Polygon overlay failed: ") + error.what());
    }
}
} // namespace starrocks
