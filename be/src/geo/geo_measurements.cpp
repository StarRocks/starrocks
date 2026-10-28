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

#include "geo/geo_measurements.h"

#include <array>
#include <cmath>
#include <exception>
#include <limits>
#include <memory>
#include <vector>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include "geo/geo_planar.h"
#include "geo/geo_types.h"

namespace starrocks {
namespace {

namespace bg = boost::geometry;
GeoCoordinateList coordinates(const std::vector<WkbCoordinate>& input) {
    GeoCoordinateList result;
    result.list.reserve(input.size());
    for (const auto& coordinate : input) result.add({coordinate.x, coordinate.y});
    return result;
}

GeoParseStatus spherical_polygon(const WkbGeometry& geometry, GeoPolygon* output) {
    GeoCoordinateListList rings;
    for (const auto& ring : geometry.rings) rings.add(new GeoCoordinateList(coordinates(ring)));
    return output->from_coords(rings);
}

bool spherical_polygon_has_valid_hole_relationships(const WkbGeometry& geometry) {
    std::vector<std::unique_ptr<GeoPolygon>> holes;
    holes.reserve(geometry.rings.size() > 1 ? geometry.rings.size() - 1 : 0);
    for (size_t i = 1; i < geometry.rings.size(); ++i) {
        GeoCoordinateListList ring;
        ring.add(new GeoCoordinateList(coordinates(geometry.rings[i])));
        auto hole = std::make_unique<GeoPolygon>();
        if (hole->from_coords(ring) != GEO_PARSE_OK) return false;
        for (const auto& previous : holes) {
            if (hole->intersects_interior(*previous)) return false;
        }
        holes.emplace_back(std::move(hole));
    }
    return true;
}

GeoParseStatus spherical_line(const WkbGeometry& geometry, GeoLine* output) {
    return output->from_coords(coordinates(geometry.coordinates));
}

PlanarLine to_planar_line(const WkbGeometry& geometry) {
    PlanarLine result;
    result.reserve(geometry.coordinates.size());
    for (const auto& coordinate : geometry.coordinates) result.emplace_back(coordinate.x, coordinate.y);
    return result;
}

bool planar_valid_impl(const WkbGeometry& geometry) {
    if (geometry.empty) return true;
    switch (geometry.type) {
    case WkbGeometryType::POINT:
        return geometry.coordinates.size() == 1;
    case WkbGeometryType::LINESTRING:
        return bg::is_valid(to_planar_line(geometry));
    case WkbGeometryType::POLYGON:
        return bg::is_valid(to_planar_polygon(geometry));
    case WkbGeometryType::MULTIPOINT: {
        PlanarMultiPoint multi;
        for (const auto& child : geometry.children) {
            if (!child.empty) multi.emplace_back(child.coordinates[0].x, child.coordinates[0].y);
        }
        return bg::is_valid(multi);
    }
    case WkbGeometryType::MULTILINESTRING: {
        PlanarMultiLine multi;
        for (const auto& child : geometry.children) {
            if (!child.empty) multi.emplace_back(to_planar_line(child));
        }
        return bg::is_valid(multi);
    }
    case WkbGeometryType::MULTIPOLYGON: {
        PlanarMultiPolygon multi;
        for (const auto& child : geometry.children) {
            if (!child.empty) multi.emplace_back(to_planar_polygon(child));
        }
        return bg::is_valid(multi);
    }
    case WkbGeometryType::GEOMETRYCOLLECTION:
        for (const auto& child : geometry.children) {
            if (!planar_valid_impl(child)) return false;
        }
        return true;
    }
    return false;
}

bool spherical_valid_impl(const WkbGeometry& geometry) {
    if (geometry.empty) return true;
    switch (geometry.type) {
    case WkbGeometryType::POINT: {
        GeoPoint point;
        return geometry.coordinates.size() == 1 &&
               point.from_coord(geometry.coordinates[0].x, geometry.coordinates[0].y) == GEO_PARSE_OK;
    }
    case WkbGeometryType::LINESTRING: {
        GeoLine line;
        GeoSphericalLine line_family;
        return spherical_line(geometry, &line) == GEO_PARSE_OK &&
               line_family.add_component(coordinates(geometry.coordinates)) == GEO_PARSE_OK;
    }
    case WkbGeometryType::POLYGON: {
        GeoPolygon polygon;
        return spherical_polygon(geometry, &polygon) == GEO_PARSE_OK &&
               spherical_polygon_has_valid_hole_relationships(geometry);
    }
    case WkbGeometryType::MULTIPOINT:
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::GEOMETRYCOLLECTION:
        for (const auto& child : geometry.children) {
            if (!spherical_valid_impl(child)) return false;
        }
        return true;
    case WkbGeometryType::MULTIPOLYGON: {
        std::vector<std::unique_ptr<GeoPolygon>> polygons;
        for (const auto& child : geometry.children) {
            if (child.empty) continue;
            auto polygon = std::make_unique<GeoPolygon>();
            if (spherical_polygon(child, polygon.get()) != GEO_PARSE_OK ||
                !spherical_polygon_has_valid_hole_relationships(child)) {
                return false;
            }
            for (const auto& previous : polygons) {
                if (polygon->intersects_interior(*previous)) return false;
            }
            polygons.emplace_back(std::move(polygon));
        }
        return true;
    }
    }
    return false;
}

long double planar_ring_length(const std::vector<WkbCoordinate>& points) {
    long double result = 0;
    for (size_t i = 1; i < points.size(); ++i) {
        result += std::hypotl(static_cast<long double>(points[i].x) - points[i - 1].x,
                              static_cast<long double>(points[i].y) - points[i - 1].y);
    }
    return result;
}

long double planar_measurement_impl(const WkbGeometry& geometry, GeoMeasurementKind kind) {
    if (geometry.empty) return 0;
    switch (geometry.type) {
    case WkbGeometryType::POINT:
    case WkbGeometryType::MULTIPOINT:
        return 0;
    case WkbGeometryType::LINESTRING:
        return kind == GeoMeasurementKind::LENGTH ? planar_ring_length(geometry.coordinates) : 0;
    case WkbGeometryType::POLYGON: {
        if (kind == GeoMeasurementKind::AREA) {
            if (geometry.rings.empty()) return 0;
            long double result = std::abs(signed_ring_area2(geometry.rings[0])) / 2;
            for (size_t i = 1; i < geometry.rings.size(); ++i) {
                result -= std::abs(signed_ring_area2(geometry.rings[i])) / 2;
            }
            return result;
        }
        if (kind == GeoMeasurementKind::PERIMETER) {
            long double result = 0;
            for (const auto& ring : geometry.rings) result += planar_ring_length(ring);
            return result;
        }
        return 0;
    }
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::MULTIPOLYGON:
    case WkbGeometryType::GEOMETRYCOLLECTION: {
        long double result = 0;
        for (const auto& child : geometry.children) result += planar_measurement_impl(child, kind);
        return result;
    }
    }
    return 0;
}
StatusOr<long double> spherical_measurement_impl(const WkbGeometry& geometry, GeoMeasurementKind kind) {
    if (geometry.empty) return 0;
    switch (geometry.type) {
    case WkbGeometryType::POINT:
    case WkbGeometryType::MULTIPOINT:
        return 0;
    case WkbGeometryType::LINESTRING: {
        if (kind != GeoMeasurementKind::LENGTH) return 0;
        GeoLine line;
        if (spherical_line(geometry, &line) != GEO_PARSE_OK) {
            return Status::InvalidArgument("Invalid GEOGRAPHY LINESTRING");
        }
        return static_cast<long double>(line.length_meters());
    }
    case WkbGeometryType::POLYGON: {
        GeoPolygon polygon;
        if (spherical_polygon(geometry, &polygon) != GEO_PARSE_OK) {
            return Status::InvalidArgument("Invalid GEOGRAPHY POLYGON");
        }
        if (kind == GeoMeasurementKind::AREA) return static_cast<long double>(polygon.area_square_meters());
        if (kind == GeoMeasurementKind::PERIMETER) return static_cast<long double>(polygon.perimeter_meters());
        return 0;
    }
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::MULTIPOLYGON:
    case WkbGeometryType::GEOMETRYCOLLECTION: {
        long double result = 0;
        for (const auto& child : geometry.children) {
            ASSIGN_OR_RETURN(auto value, spherical_measurement_impl(child, kind));
            result += value;
        }
        return result;
    }
    }
    return 0;
}
struct PlanarAccumulator {
    long double x = 0;
    long double y = 0;
    long double weight = 0;

    void add(long double value_x, long double value_y, long double value_weight) {
        x += value_x * value_weight;
        y += value_y * value_weight;
        weight += value_weight;
    }
};

void accumulate_planar_line(const std::vector<WkbCoordinate>& line, PlanarAccumulator* output) {
    for (size_t i = 1; i < line.size(); ++i) {
        const long double length = std::hypotl(static_cast<long double>(line[i].x) - line[i - 1].x,
                                               static_cast<long double>(line[i].y) - line[i - 1].y);
        if (length > 0) {
            output->add((static_cast<long double>(line[i - 1].x) + line[i].x) / 2,
                        (static_cast<long double>(line[i - 1].y) + line[i].y) / 2, length);
        }
    }
}

void accumulate_planar_ring_area(const std::vector<WkbCoordinate>& ring, long double sign, PlanarAccumulator* output) {
    if (ring.empty()) return;
    const long double origin_x = ring[0].x;
    const long double origin_y = ring[0].y;
    long double area2 = 0;
    long double x_numerator = 0;
    long double y_numerator = 0;
    for (size_t i = 1; i < ring.size(); ++i) {
        const long double previous_x = static_cast<long double>(ring[i - 1].x) - origin_x;
        const long double previous_y = static_cast<long double>(ring[i - 1].y) - origin_y;
        const long double current_x = static_cast<long double>(ring[i].x) - origin_x;
        const long double current_y = static_cast<long double>(ring[i].y) - origin_y;
        const long double cross = previous_x * current_y - current_x * previous_y;
        area2 += cross;
        x_numerator += (previous_x + current_x) * cross;
        y_numerator += (previous_y + current_y) * cross;
    }
    if (area2 == 0) return;
    const long double area = std::abs(area2) / 2;
    output->add(origin_x + x_numerator / (3 * area2), origin_y + y_numerator / (3 * area2), sign * area);
}

void accumulate_planar(const WkbGeometry& geometry, std::array<PlanarAccumulator, 3>* output) {
    if (geometry.empty) return;
    switch (geometry.type) {
    case WkbGeometryType::POINT:
        (*output)[0].add(geometry.coordinates[0].x, geometry.coordinates[0].y, 1);
        return;
    case WkbGeometryType::LINESTRING: {
        PlanarAccumulator local;
        accumulate_planar_line(geometry.coordinates, &local);
        if (local.weight > 0) {
            (*output)[1].x += local.x;
            (*output)[1].y += local.y;
            (*output)[1].weight += local.weight;
        } else {
            for (const auto& point : geometry.coordinates) (*output)[0].add(point.x, point.y, 1);
        }
        return;
    }
    case WkbGeometryType::POLYGON: {
        PlanarAccumulator local;
        if (!geometry.rings.empty()) {
            accumulate_planar_ring_area(geometry.rings[0], 1, &local);
            for (size_t i = 1; i < geometry.rings.size(); ++i) {
                accumulate_planar_ring_area(geometry.rings[i], -1, &local);
            }
        }
        if (local.weight > 0) {
            (*output)[2].x += local.x;
            (*output)[2].y += local.y;
            (*output)[2].weight += local.weight;
        } else {
            for (const auto& ring : geometry.rings) accumulate_planar_line(ring, &(*output)[1]);
        }
        return;
    }
    case WkbGeometryType::MULTIPOINT:
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::MULTIPOLYGON:
    case WkbGeometryType::GEOMETRYCOLLECTION:
        for (const auto& child : geometry.children) accumulate_planar(child, output);
        return;
    }
}

struct SphericalAccumulator {
    GeoCartesianCentroid value;
    double magnitude = 0;
    bool populated = false;

    void add(const GeoCartesianCentroid& point) {
        value.x += point.x;
        value.y += point.y;
        value.z += point.z;
        magnitude += std::sqrt(point.x * point.x + point.y * point.y + point.z * point.z);
        populated = true;
    }

    double norm2() const { return value.x * value.x + value.y * value.y + value.z * value.z; }

    bool has_direction() const {
        const double tolerance = 32 * std::numeric_limits<double>::epsilon() * magnitude;
        return populated && norm2() > tolerance * tolerance;
    }
};

Status accumulate_spherical(const WkbGeometry& geometry, std::array<SphericalAccumulator, 3>* output) {
    if (geometry.empty) return Status::OK();
    switch (geometry.type) {
    case WkbGeometryType::POINT: {
        const double longitude = geometry.coordinates[0].x * std::acos(-1.0) / 180;
        const double latitude = geometry.coordinates[0].y * std::acos(-1.0) / 180;
        const double cos_latitude = std::cos(latitude);
        (*output)[0].add({cos_latitude * std::cos(longitude), cos_latitude * std::sin(longitude), std::sin(latitude)});
        return Status::OK();
    }
    case WkbGeometryType::LINESTRING: {
        GeoLine line;
        if (spherical_line(geometry, &line) != GEO_PARSE_OK) {
            return Status::InvalidArgument("Invalid GEOGRAPHY LINESTRING");
        }
        (*output)[1].add(line.centroid_vector());
        return Status::OK();
    }
    case WkbGeometryType::POLYGON: {
        GeoPolygon polygon;
        if (spherical_polygon(geometry, &polygon) != GEO_PARSE_OK) {
            return Status::InvalidArgument("Invalid GEOGRAPHY POLYGON");
        }
        (*output)[2].add(polygon.centroid_vector());
        return Status::OK();
    }
    case WkbGeometryType::MULTIPOINT:
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::MULTIPOLYGON:
    case WkbGeometryType::GEOMETRYCOLLECTION:
        for (const auto& child : geometry.children) RETURN_IF_ERROR(accumulate_spherical(child, output));
        return Status::OK();
    }
    return Status::InternalError("Unknown WKB geometry type");
}
} // namespace
StatusOr<bool> planar_is_valid(const WkbGeometry& geometry) {
    try {
        return planar_valid_impl(geometry);
    } catch (const std::exception&) {
        return false;
    }
}

StatusOr<bool> spherical_is_valid(const WkbGeometry& geometry) {
    return spherical_valid_impl(geometry);
}

StatusOr<double> planar_measurement(const WkbGeometry& geometry, GeoMeasurementKind kind) {
    ASSIGN_OR_RETURN(const bool valid, planar_is_valid(geometry));
    if (!valid) return Status::InvalidArgument("Measurement requires a valid GEOMETRY");
    return static_cast<double>(planar_measurement_impl(geometry, kind));
}

StatusOr<double> spherical_measurement(const WkbGeometry& geometry, GeoMeasurementKind kind) {
    ASSIGN_OR_RETURN(const bool valid, spherical_is_valid(geometry));
    if (!valid) return Status::InvalidArgument("Measurement requires a valid GEOGRAPHY");
    ASSIGN_OR_RETURN(const auto value, spherical_measurement_impl(geometry, kind));
    return static_cast<double>(value);
}

StatusOr<GeoCentroidResult> planar_centroid(const WkbGeometry& geometry) {
    ASSIGN_OR_RETURN(const bool valid, planar_is_valid(geometry));
    if (!valid) return Status::InvalidArgument("ST_Centroid requires a valid GEOMETRY");
    std::array<PlanarAccumulator, 3> accumulator;
    accumulate_planar(geometry, &accumulator);
    for (int dimension = 2; dimension >= 0; --dimension) {
        if (accumulator[dimension].weight != 0) {
            return GeoCentroidResult{false,
                                     {static_cast<double>(accumulator[dimension].x / accumulator[dimension].weight),
                                      static_cast<double>(accumulator[dimension].y / accumulator[dimension].weight)}};
        }
    }
    return GeoCentroidResult{};
}

StatusOr<GeoCentroidResult> spherical_centroid(const WkbGeometry& geometry) {
    ASSIGN_OR_RETURN(const bool valid, spherical_is_valid(geometry));
    if (!valid) return Status::InvalidArgument("ST_Centroid requires a valid GEOGRAPHY");
    std::array<SphericalAccumulator, 3> accumulator;
    RETURN_IF_ERROR(accumulate_spherical(geometry, &accumulator));
    for (int dimension = 2; dimension >= 0; --dimension) {
        if (accumulator[dimension].populated) {
            if (!accumulator[dimension].has_direction()) return GeoCentroidResult{};
            const auto& vector = accumulator[dimension].value;
            const double radians_to_degrees = 180 / std::acos(-1.0);
            return GeoCentroidResult{false,
                                     {std::atan2(vector.y, vector.x) * radians_to_degrees,
                                      std::atan2(vector.z, std::hypot(vector.x, vector.y)) * radians_to_degrees}};
        }
    }
    return GeoCentroidResult{};
}

} // namespace starrocks