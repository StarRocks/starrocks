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

#include "exprs/geo_functions.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "geo/geo_buffer.h"
#include "geo/geo_measurements.h"
#include "geo/geo_overlay.h"
#include "geo/geo_types.h"
#include "geo/wkb.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

constexpr int32_t kCrs84Srid = 4326;

Status check_geography_boundary(const GeoColumn& column) {
    const auto& descriptor = column.descriptor();
    const auto& type = descriptor.type;
    const auto dimension = descriptor.storage.dimension;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY || type.coordinate_system != GEO_COORDINATE_SYSTEM_SPHERICAL ||
        type.edge_algorithm != GEO_EDGE_ALGORITHM_SPHERICAL || type.crs != "OGC:CRS84" ||
        (type.srid.has_value() && type.srid.value() != kCrs84Srid) || descriptor.storage.encoding != GEO_ENCODING_WKB ||
        (dimension != GEO_DIMENSION_UNKNOWN && dimension != GEO_DIMENSION_XY && dimension != GEO_DIMENSION_MIXED)) {
        return Status::NotSupported("Unsupported GEOGRAPHY SQL boundary descriptor");
    }
    return Status::OK();
}

std::optional<int32_t> derive_srid(std::string_view crs) {
    if (crs == "OGC:CRS84") return kCrs84Srid;
    constexpr std::string_view prefix = "EPSG:";
    if (!crs.starts_with(prefix)) return std::nullopt;
    int32_t srid;
    const auto* begin = crs.data() + prefix.size();
    const auto* end = crs.data() + crs.size();
    auto [parsed_end, error] = std::from_chars(begin, end, srid);
    return error == std::errc() && parsed_end == end ? std::optional<int32_t>(srid) : std::nullopt;
}

Status check_geometry_boundary(const GeoColumn& column) {
    const auto& descriptor = column.descriptor();
    const auto& type = descriptor.type;
    const auto dimension = descriptor.storage.dimension;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOMETRY || type.coordinate_system != GEO_COORDINATE_SYSTEM_CARTESIAN ||
        type.edge_algorithm != GEO_EDGE_ALGORITHM_PLANAR || type.crs.empty() || type.srid != derive_srid(type.crs) ||
        descriptor.storage.encoding != GEO_ENCODING_WKB ||
        (dimension != GEO_DIMENSION_UNKNOWN && dimension != GEO_DIMENSION_XY && dimension != GEO_DIMENSION_MIXED)) {
        return Status::NotSupported("Unsupported GEOMETRY SQL boundary descriptor");
    }
    return Status::OK();
}

Status check_geometry_compute_boundary(const GeoColumn& column) {
    RETURN_IF_ERROR(check_geometry_boundary(column));
    if (column.descriptor().storage.dimension != GEO_DIMENSION_XY) {
        return Status::NotSupported("GEOMETRY compute functions require XY dimension");
    }
    return Status::OK();
}

struct GeoInput {
    const GeoColumn* data;
    const NullableColumn* nullable;
    bool constant;

    size_t index(size_t row) const { return constant ? 0 : row; }
    bool is_null(size_t row) const { return nullable != nullptr && nullable->is_null(index(row)); }
    Slice wkb(size_t row) const { return data->get_wkb(index(row)); }
};

template <LogicalType Type>
StatusOr<GeoInput> geo_input(const ColumnPtr& column) {
    const bool constant = column->is_constant();
    const Column* source = constant ? down_cast<const ConstColumn*>(column.get())->data_column().get() : column.get();
    const auto* nullable = source->is_nullable() ? down_cast<const NullableColumn*>(source) : nullptr;
    const auto* geo = down_cast<const GeoColumn*>(nullable ? nullable->data_column().get() : source);
    if constexpr (Type == TYPE_GEOGRAPHY) {
        RETURN_IF_ERROR(check_geography_boundary(*geo));
    } else {
        RETURN_IF_ERROR(check_geometry_compute_boundary(*geo));
    }
    return GeoInput{geo, nullable, constant};
}

template <LogicalType Type, bool X>
StatusOr<ColumnPtr> geo_coordinate(const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<Type>(columns[0]));
    const size_t rows = input.constant ? 1 : size;
    ColumnBuilder<TYPE_DOUBLE> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &geometry, semantics));
        if (geometry.type != WkbGeometryType::POINT || geometry.empty) {
            if constexpr (Type == TYPE_GEOGRAPHY) {
                return Status::InvalidArgument(X ? "ST_X requires a non-empty POINT GEOGRAPHY"
                                                 : "ST_Y requires a non-empty POINT GEOGRAPHY");
            } else {
                return Status::InvalidArgument(X ? "ST_X requires a non-empty POINT GEOMETRY"
                                                 : "ST_Y requires a non-empty POINT GEOMETRY");
            }
        }
        result.append(X ? geometry.coordinates[0].x : geometry.coordinates[0].y);
    }
    auto output = result.build(false);
    if (input.constant) return ConstColumn::create(std::move(output), size);
    return output;
}

const char* geo_type_name(WkbGeometryType type) {
    switch (type) {
    case WkbGeometryType::POINT:
        return "ST_Point";
    case WkbGeometryType::LINESTRING:
        return "ST_LineString";
    case WkbGeometryType::POLYGON:
        return "ST_Polygon";
    case WkbGeometryType::MULTIPOINT:
        return "ST_MultiPoint";
    case WkbGeometryType::MULTILINESTRING:
        return "ST_MultiLineString";
    case WkbGeometryType::MULTIPOLYGON:
        return "ST_MultiPolygon";
    case WkbGeometryType::GEOMETRYCOLLECTION:
        return "ST_GeometryCollection";
    }
    return "";
}

template <LogicalType Type>
StatusOr<ColumnPtr> geo_type(const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<Type>(columns[0]));
    const size_t rows = input.constant ? 1 : size;
    ColumnBuilder<TYPE_VARCHAR> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &geometry, semantics));
        result.append(Slice(geo_type_name(geometry.type)));
    }
    auto output = result.build(false);
    if (input.constant) return ConstColumn::create(std::move(output), size);
    return output;
}

struct NativeGeoDistanceArgumentState {
    bool constant = false;
    Status parse_status = Status::OK();
    std::optional<WkbGeometry> geometry;
};

struct NativeGeoDistanceState {
    NativeGeoDistanceArgumentState arguments[2];
};

struct NativeGeoDistanceThreadState final : FunctionThreadState {
    std::optional<GeoPoint> points[2];
    std::optional<GeoSphericalLine> lines[2];
};

bool is_line_family(WkbGeometryType type) {
    return type == WkbGeometryType::LINESTRING || type == WkbGeometryType::MULTILINESTRING;
}

bool is_point_line_pair(const WkbGeometry& lhs, const WkbGeometry& rhs) {
    return (lhs.type == WkbGeometryType::POINT && is_line_family(rhs.type)) ||
           (rhs.type == WkbGeometryType::POINT && is_line_family(lhs.type));
}

bool is_empty_line_family(const WkbGeometry& geometry) {
    if (geometry.type == WkbGeometryType::LINESTRING) return geometry.empty;
    if (geometry.type != WkbGeometryType::MULTILINESTRING || geometry.empty) return geometry.empty;
    return std::all_of(geometry.children.begin(), geometry.children.end(),
                       [](const WkbGeometry& child) { return child.empty; });
}

StatusOr<const WkbGeometry*> distance_geometry(const GeoInput& input, size_t row, WkbCoordinateSemantics semantics,
                                               const NativeGeoDistanceArgumentState* prepared,
                                               std::optional<WkbGeometry>* constant_geometry,
                                               WkbGeometry* varying_geometry) {
    if (prepared != nullptr && prepared->constant) {
        RETURN_IF_ERROR(prepared->parse_status);
        return &prepared->geometry.value();
    }
    if (input.constant) {
        if (!constant_geometry->has_value()) {
            WkbGeometry parsed;
            RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &parsed, semantics));
            constant_geometry->emplace(std::move(parsed));
        }
        return &constant_geometry->value();
    }
    RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), varying_geometry, semantics));
    return varying_geometry;
}

template <LogicalType Type>
Status prepare_distance_constants(FunctionContext* context, NativeGeoDistanceState* state) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    for (size_t i = 0; i < 2; ++i) {
        if (!context->is_constant_column(i)) continue;
        auto& argument = state->arguments[i];
        argument.constant = true;
        const auto& column = context->get_constant_column(i);
        if (column->only_null()) continue;
        ASSIGN_OR_RETURN(auto input, geo_input<Type>(column));
        WkbGeometry geometry;
        argument.parse_status = WkbCodec::parse_wkb(input.wkb(0), &geometry, semantics);
        if (argument.parse_status.ok()) argument.geometry.emplace(std::move(geometry));
    }
    return Status::OK();
}

StatusOr<GeoSphericalLine> prepare_spherical_line_family(const WkbGeometry& line) {
    GeoSphericalLine result;
    const size_t component_count = line.type == WkbGeometryType::LINESTRING ? 1 : line.children.size();
    for (size_t component_index = 0; component_index < component_count; ++component_index) {
        const auto& component = line.type == WkbGeometryType::LINESTRING ? line : line.children[component_index];
        GeoCoordinateList coordinates;
        coordinates.list.reserve(component.coordinates.size());
        for (const auto& coordinate : component.coordinates) {
            coordinates.add({coordinate.x, coordinate.y});
        }
        if (result.add_component(coordinates) != GEO_PARSE_OK) {
            return Status::InvalidArgument(
                    "GEOGRAPHY line contains an exact or numerically ambiguous antipodal segment");
        }
    }
    return result;
}

long double planar_point_segment_distance(const WkbCoordinate& point, const WkbCoordinate& start,
                                          const WkbCoordinate& end) {
    const long double dx = static_cast<long double>(end.x) - start.x;
    const long double dy = static_cast<long double>(end.y) - start.y;
    const long double px = static_cast<long double>(point.x) - start.x;
    const long double py = static_cast<long double>(point.y) - start.y;
    if (dx == 0 && dy == 0) return std::hypotl(px, py);
    if (px * dx + py * dy <= 0) return std::hypotl(px, py);

    const long double qx = static_cast<long double>(point.x) - end.x;
    const long double qy = static_cast<long double>(point.y) - end.y;
    if (qx * -dx + qy * -dy <= 0) return std::hypotl(qx, qy);

    return std::abs(dx * py - dy * px) / std::hypotl(dx, dy);
}

long double planar_point_line_distance(const WkbCoordinate& point, const WkbGeometry& line) {
    long double minimum = std::numeric_limits<long double>::infinity();
    const size_t component_count = line.type == WkbGeometryType::LINESTRING ? 1 : line.children.size();
    for (size_t component_index = 0; component_index < component_count; ++component_index) {
        const auto& component = line.type == WkbGeometryType::LINESTRING ? line : line.children[component_index];
        for (size_t vertex = 1; vertex < component.coordinates.size(); ++vertex) {
            minimum = std::min(minimum, planar_point_segment_distance(point, component.coordinates[vertex - 1],
                                                                      component.coordinates[vertex]));
        }
    }
    return minimum;
}

template <LogicalType Type, bool DWithin>
StatusOr<ColumnPtr> geo_distance(FunctionContext* context, const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    constexpr const char* function_name = DWithin ? "ST_DWithin" : "ST_Distance";
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null() || (DWithin && columns[2]->only_null())) {
        return ColumnHelper::create_const_null_column(size);
    }
    ASSIGN_OR_RETURN(auto lhs, geo_input<Type>(columns[0]));
    ASSIGN_OR_RETURN(auto rhs, geo_input<Type>(columns[1]));
    if constexpr (Type == TYPE_GEOMETRY) {
        if (!is_geo_compute_compatible(lhs.data->descriptor(), rhs.data->descriptor())) {
            return Status::InvalidArgument(std::string(function_name) + " requires compatible GEOMETRY descriptors");
        }
    }
    std::optional<ColumnViewer<TYPE_DOUBLE>> threshold;
    if constexpr (DWithin) threshold.emplace(columns[2]);
    const bool constant = ColumnHelper::is_all_const(columns);
    const size_t rows = constant ? 1 : size;
    const auto* prepared_state = context == nullptr
                                         ? nullptr
                                         : reinterpret_cast<const NativeGeoDistanceState*>(
                                                   context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    NativeGeoDistanceThreadState* thread_state = nullptr;
    if constexpr (Type == TYPE_GEOGRAPHY) {
        if (prepared_state != nullptr) {
            thread_state = context->get_or_create_thread_state<NativeGeoDistanceThreadState>(
                    [] { return std::make_unique<NativeGeoDistanceThreadState>(); });
        }
    }
    std::optional<WkbGeometry> constant_geometry[2];
    std::optional<GeoPoint> constant_spherical_points[2];
    std::optional<GeoSphericalLine> constant_spherical_lines[2];
    ColumnBuilder<DWithin ? TYPE_BOOLEAN : TYPE_DOUBLE> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row) || (DWithin && threshold->is_null(row))) {
            result.append_null();
            continue;
        }
        double threshold_value = 0;
        if constexpr (DWithin) {
            threshold_value = threshold->value(row);
            if (!std::isfinite(threshold_value) || threshold_value < 0) {
                return Status::InvalidArgument("ST_DWithin requires a finite, nonnegative distance threshold");
            }
        }
        WkbGeometry varying_geometry[2];
        const auto* prepared_left = prepared_state == nullptr ? nullptr : &prepared_state->arguments[0];
        const auto* prepared_right = prepared_state == nullptr ? nullptr : &prepared_state->arguments[1];
        ASSIGN_OR_RETURN(const auto* left, distance_geometry(lhs, row, semantics, prepared_left, &constant_geometry[0],
                                                             &varying_geometry[0]));
        ASSIGN_OR_RETURN(const auto* right, distance_geometry(rhs, row, semantics, prepared_right,
                                                              &constant_geometry[1], &varying_geometry[1]));
        const bool point_point = left->type == WkbGeometryType::POINT && right->type == WkbGeometryType::POINT;
        if ((!DWithin && !point_point && !is_point_line_pair(*left, *right)) ||
            (DWithin && !is_point_line_pair(*left, *right))) {
            return Status::InvalidArgument(std::string(function_name) +
                                           (DWithin ? " supports only POINT with LINESTRING/MULTILINESTRING inputs"
                                                    : " supports POINT/POINT and POINT with "
                                                      "LINESTRING/MULTILINESTRING inputs"));
        }
        if (left->empty || right->empty || is_empty_line_family(*left) || is_empty_line_family(*right)) {
            if constexpr (DWithin) {
                result.append(false);
            } else {
                result.append_null();
            }
            continue;
        }
        if (point_point) {
            if constexpr (Type == TYPE_GEOGRAPHY) {
                double distance;
                if (!GeoPoint::st_distance_sphere(left->coordinates[0].x, left->coordinates[0].y,
                                                  right->coordinates[0].x, right->coordinates[0].y, &distance)) {
                    return Status::InvalidArgument("ST_Distance received invalid GEOGRAPHY coordinates");
                }
                result.append(distance);
            } else {
                result.append(std::hypot(right->coordinates[0].x - left->coordinates[0].x,
                                         right->coordinates[0].y - left->coordinates[0].y));
            }
            continue;
        }
        const size_t point_argument = left->type == WkbGeometryType::POINT ? 0 : 1;
        const size_t line_argument = 1 - point_argument;
        const auto& point_geometry = point_argument == 0 ? *left : *right;
        const auto& line_geometry = line_argument == 0 ? *left : *right;
        if constexpr (Type == TYPE_GEOGRAPHY) {
            GeoPoint local_point;
            const GeoPoint* point = &local_point;
            const bool point_constant = point_argument == 0 ? lhs.constant : rhs.constant;
            if (point_constant) {
                const bool plan_constant =
                        prepared_state != nullptr && prepared_state->arguments[point_argument].constant;
                auto* cached = plan_constant ? &thread_state->points[point_argument]
                                             : &constant_spherical_points[point_argument];
                if (!cached->has_value()) {
                    cached->emplace();
                    if (cached->value().from_coord(point_geometry.coordinates[0].x, point_geometry.coordinates[0].y) !=
                        GEO_PARSE_OK) {
                        return Status::InvalidArgument("Invalid GEOGRAPHY point coordinates");
                    }
                }
                point = &cached->value();
            } else if (local_point.from_coord(point_geometry.coordinates[0].x, point_geometry.coordinates[0].y) !=
                       GEO_PARSE_OK) {
                return Status::InvalidArgument("Invalid GEOGRAPHY point coordinates");
            }
            GeoSphericalLine local_line;
            const GeoSphericalLine* line = &local_line;
            const bool line_constant = line_argument == 0 ? lhs.constant : rhs.constant;
            if (line_constant) {
                const bool plan_constant =
                        prepared_state != nullptr && prepared_state->arguments[line_argument].constant;
                auto* cached =
                        plan_constant ? &thread_state->lines[line_argument] : &constant_spherical_lines[line_argument];
                if (!cached->has_value()) {
                    ASSIGN_OR_RETURN(auto prepared, prepare_spherical_line_family(line_geometry));
                    cached->emplace(std::move(prepared));
                }
                line = &cached->value();
            } else {
                ASSIGN_OR_RETURN(local_line, prepare_spherical_line_family(line_geometry));
            }
            if constexpr (DWithin) {
                result.append(line->dwithin(*point, threshold_value));
            } else {
                double distance;
                if (!line->distance(*point, &distance)) {
                    result.append_null();
                } else {
                    result.append(distance);
                }
            }
        } else {
            const auto distance = planar_point_line_distance(point_geometry.coordinates[0], line_geometry);
            if constexpr (DWithin) {
                result.append(distance <= static_cast<long double>(threshold_value));
            } else if (!std::isfinite(distance) || distance > std::numeric_limits<double>::max()) {
                return Status::InvalidArgument("ST_Distance result is outside the finite DOUBLE range");
            } else {
                result.append(static_cast<double>(distance));
            }
        }
    }
    auto output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

enum class PointPolygonRelation { OUTSIDE, BOUNDARY, INSIDE };

constexpr long double kPlanarBoundaryUlps = 32;

bool planar_point_on_segment(const WkbCoordinate& point, const WkbCoordinate& lhs, const WkbCoordinate& rhs) {
    const long double abx = static_cast<long double>(rhs.x) - lhs.x;
    const long double aby = static_cast<long double>(rhs.y) - lhs.y;
    const long double apx = static_cast<long double>(point.x) - lhs.x;
    const long double apy = static_cast<long double>(point.y) - lhs.y;
    const long double cross_lhs = abx * apy;
    const long double cross_rhs = aby * apx;
    const long double cross_scale = std::abs(cross_lhs) + std::abs(cross_rhs);
    const long double cross_tolerance = kPlanarBoundaryUlps * std::numeric_limits<double>::epsilon() * cross_scale;
    if (std::abs(cross_lhs - cross_rhs) > cross_tolerance) return false;

    const long double coordinate_scale = std::max({std::abs(abx), std::abs(aby), std::abs(apx), std::abs(apy)});
    const long double coordinate_tolerance =
            kPlanarBoundaryUlps * std::numeric_limits<double>::epsilon() * coordinate_scale;
    return apx >= std::min(0.0L, abx) - coordinate_tolerance && apx <= std::max(0.0L, abx) + coordinate_tolerance &&
           apy >= std::min(0.0L, aby) - coordinate_tolerance && apy <= std::max(0.0L, aby) + coordinate_tolerance;
}

PointPolygonRelation planar_ring_relation(const std::vector<WkbCoordinate>& ring, const WkbCoordinate& point) {
    bool inside = false;
    for (size_t i = 1; i < ring.size(); ++i) {
        const auto& lhs = ring[i - 1];
        const auto& rhs = ring[i];
        if (planar_point_on_segment(point, lhs, rhs)) return PointPolygonRelation::BOUNDARY;
        if ((lhs.y > point.y) != (rhs.y > point.y)) {
            const long double intersection_offset =
                    (static_cast<long double>(lhs.x) - point.x) + (static_cast<long double>(point.y) - lhs.y) *
                                                                          (static_cast<long double>(rhs.x) - lhs.x) /
                                                                          (static_cast<long double>(rhs.y) - lhs.y);
            if (intersection_offset > 0) inside = !inside;
        }
    }
    return inside ? PointPolygonRelation::INSIDE : PointPolygonRelation::OUTSIDE;
}

PointPolygonRelation planar_polygon_relation(const WkbGeometry& polygon, const WkbCoordinate& point) {
    auto relation = planar_ring_relation(polygon.rings[0], point);
    if (relation != PointPolygonRelation::INSIDE) return relation;
    for (size_t i = 1; i < polygon.rings.size(); ++i) {
        relation = planar_ring_relation(polygon.rings[i], point);
        if (relation == PointPolygonRelation::BOUNDARY) return relation;
        if (relation == PointPolygonRelation::INSIDE) return PointPolygonRelation::OUTSIDE;
    }
    return PointPolygonRelation::INSIDE;
}

PointPolygonRelation planar_polygon_family_relation(const WkbGeometry& polygon, const WkbCoordinate& point) {
    if (polygon.type == WkbGeometryType::POLYGON) return planar_polygon_relation(polygon, point);
    PointPolygonRelation result = PointPolygonRelation::OUTSIDE;
    for (const auto& child : polygon.children) {
        if (child.empty) continue;
        const auto relation = planar_polygon_relation(child, point);
        if (relation == PointPolygonRelation::BOUNDARY) return relation;
        if (relation == PointPolygonRelation::INSIDE) result = relation;
    }
    return result;
}

long double planar_orientation(const WkbCoordinate& a, const WkbCoordinate& b, const WkbCoordinate& c) {
    return (static_cast<long double>(b.x) - a.x) * (static_cast<long double>(c.y) - a.y) -
           (static_cast<long double>(b.y) - a.y) * (static_cast<long double>(c.x) - a.x);
}

bool planar_segments_intersect(const WkbCoordinate& a, const WkbCoordinate& b, const WkbCoordinate& c,
                               const WkbCoordinate& d) {
    if (planar_point_on_segment(a, c, d) || planar_point_on_segment(b, c, d) || planar_point_on_segment(c, a, b) ||
        planar_point_on_segment(d, a, b)) {
        return true;
    }
    const auto abc = planar_orientation(a, b, c);
    const auto abd = planar_orientation(a, b, d);
    const auto cda = planar_orientation(c, d, a);
    const auto cdb = planar_orientation(c, d, b);
    return (abc > 0) != (abd > 0) && (cda > 0) != (cdb > 0);
}

bool planar_polygon_intersects(const WkbGeometry& lhs, const WkbGeometry& rhs) {
    for (const auto& lhs_ring : lhs.rings) {
        for (const auto& rhs_ring : rhs.rings) {
            for (size_t i = 1; i < lhs_ring.size(); ++i) {
                for (size_t j = 1; j < rhs_ring.size(); ++j) {
                    if (planar_segments_intersect(lhs_ring[i - 1], lhs_ring[i], rhs_ring[j - 1], rhs_ring[j])) {
                        return true;
                    }
                }
            }
        }
    }
    return planar_polygon_relation(lhs, rhs.rings[0][0]) != PointPolygonRelation::OUTSIDE ||
           planar_polygon_relation(rhs, lhs.rings[0][0]) != PointPolygonRelation::OUTSIDE;
}

bool planar_polygon_family_intersects(const WkbGeometry& lhs, const WkbGeometry& rhs) {
    const size_t lhs_count = lhs.type == WkbGeometryType::POLYGON ? 1 : lhs.children.size();
    const size_t rhs_count = rhs.type == WkbGeometryType::POLYGON ? 1 : rhs.children.size();
    for (size_t i = 0; i < lhs_count; ++i) {
        const auto& lhs_polygon = lhs.type == WkbGeometryType::POLYGON ? lhs : lhs.children[i];
        if (lhs_polygon.empty) continue;
        for (size_t j = 0; j < rhs_count; ++j) {
            const auto& rhs_polygon = rhs.type == WkbGeometryType::POLYGON ? rhs : rhs.children[j];
            if (!rhs_polygon.empty && planar_polygon_intersects(lhs_polygon, rhs_polygon)) return true;
        }
    }
    return false;
}

Status prepare_spherical_polygon(const WkbGeometry& polygon, GeoPolygon* native_polygon) {
    GeoCoordinateListList rings;
    for (const auto& ring : polygon.rings) {
        auto* coordinates = new GeoCoordinateList();
        for (const auto& coordinate : ring) {
            coordinates->add({coordinate.x, coordinate.y});
        }
        rings.add(coordinates);
    }
    if (native_polygon->from_coords(rings) != GEO_PARSE_OK) {
        return Status::InvalidArgument("Invalid GEOGRAPHY polygon topology");
    }
    return Status::OK();
}

StatusOr<PointPolygonRelation> spherical_polygon_relation(const WkbGeometry& polygon, const GeoPoint& point,
                                                          std::unique_ptr<GeoPolygon>* prepared) {
    GeoPolygon local;
    GeoPolygon* native_polygon = &local;
    if (prepared != nullptr) {
        if (*prepared == nullptr) {
            auto cached_polygon = std::make_unique<GeoPolygon>();
            RETURN_IF_ERROR(prepare_spherical_polygon(polygon, cached_polygon.get()));
            *prepared = std::move(cached_polygon);
        }
        native_polygon = prepared->get();
    } else {
        RETURN_IF_ERROR(prepare_spherical_polygon(polygon, native_polygon));
    }
    switch (native_polygon->point_relation(point)) {
    case GeoPointPolygonRelation::BOUNDARY:
        return PointPolygonRelation::BOUNDARY;
    case GeoPointPolygonRelation::INSIDE:
        return PointPolygonRelation::INSIDE;
    case GeoPointPolygonRelation::OUTSIDE:
        return PointPolygonRelation::OUTSIDE;
    }
    __builtin_unreachable();
}

struct SphericalPolygonFamilyCache {
    std::vector<std::unique_ptr<GeoPolygon>> components;
};

struct NativeGeoContainmentArgumentState {
    bool constant = false;
    Status parse_status = Status::OK();
    std::optional<WkbGeometry> geometry;
};

struct NativeGeoContainmentState {
    NativeGeoContainmentArgumentState arguments[2];
};

struct NativeGeoContainmentThreadState final : FunctionThreadState {
    std::optional<GeoPoint> points[2];
    SphericalPolygonFamilyCache polygons[2];
};

StatusOr<PointPolygonRelation> spherical_polygon_family_relation(const WkbGeometry& polygon, const GeoPoint& point,
                                                                 SphericalPolygonFamilyCache* cache) {
    const size_t component_count = polygon.type == WkbGeometryType::POLYGON ? 1 : polygon.children.size();
    if (cache != nullptr && cache->components.empty()) cache->components.resize(component_count);
    PointPolygonRelation result = PointPolygonRelation::OUTSIDE;
    for (size_t i = 0; i < component_count; ++i) {
        const auto& component = polygon.type == WkbGeometryType::POLYGON ? polygon : polygon.children[i];
        if (component.empty) continue;
        auto* prepared = cache == nullptr ? nullptr : &cache->components[i];
        ASSIGN_OR_RETURN(auto relation, spherical_polygon_relation(component, point, prepared));
        if (relation == PointPolygonRelation::BOUNDARY) return relation;
        if (relation == PointPolygonRelation::INSIDE) result = relation;
    }
    return result;
}

StatusOr<GeoPolygon*> spherical_polygon(const WkbGeometry& polygon, std::unique_ptr<GeoPolygon>* prepared,
                                        GeoPolygon* local) {
    if (prepared == nullptr) {
        RETURN_IF_ERROR(prepare_spherical_polygon(polygon, local));
        return local;
    }
    if (*prepared == nullptr) {
        auto cached = std::make_unique<GeoPolygon>();
        RETURN_IF_ERROR(prepare_spherical_polygon(polygon, cached.get()));
        *prepared = std::move(cached);
    }
    return prepared->get();
}

StatusOr<bool> spherical_polygon_family_intersects(const WkbGeometry& lhs, const WkbGeometry& rhs,
                                                   SphericalPolygonFamilyCache* lhs_cache,
                                                   SphericalPolygonFamilyCache* rhs_cache) {
    const size_t lhs_count = lhs.type == WkbGeometryType::POLYGON ? 1 : lhs.children.size();
    const size_t rhs_count = rhs.type == WkbGeometryType::POLYGON ? 1 : rhs.children.size();
    if (lhs_cache != nullptr && lhs_cache->components.empty()) lhs_cache->components.resize(lhs_count);
    if (rhs_cache != nullptr && rhs_cache->components.empty()) rhs_cache->components.resize(rhs_count);
    for (size_t i = 0; i < lhs_count; ++i) {
        const auto& lhs_component = lhs.type == WkbGeometryType::POLYGON ? lhs : lhs.children[i];
        if (lhs_component.empty) continue;
        GeoPolygon local_lhs;
        auto* lhs_prepared = lhs_cache == nullptr ? nullptr : &lhs_cache->components[i];
        ASSIGN_OR_RETURN(auto* native_lhs, spherical_polygon(lhs_component, lhs_prepared, &local_lhs));
        for (size_t j = 0; j < rhs_count; ++j) {
            const auto& rhs_component = rhs.type == WkbGeometryType::POLYGON ? rhs : rhs.children[j];
            if (rhs_component.empty) continue;
            GeoPolygon local_rhs;
            auto* rhs_prepared = rhs_cache == nullptr ? nullptr : &rhs_cache->components[j];
            ASSIGN_OR_RETURN(auto* native_rhs, spherical_polygon(rhs_component, rhs_prepared, &local_rhs));
            if (native_lhs->intersects_inclusive(*native_rhs)) return true;
        }
    }
    return false;
}

StatusOr<const WkbGeometry*> containment_geometry(const GeoInput& input, size_t row, WkbCoordinateSemantics semantics,
                                                  const NativeGeoContainmentArgumentState* prepared,
                                                  std::optional<WkbGeometry>* constant_geometry,
                                                  WkbGeometry* varying_geometry) {
    if (prepared != nullptr && prepared->constant) {
        RETURN_IF_ERROR(prepared->parse_status);
        return &prepared->geometry.value();
    }
    if (input.constant) {
        if (!constant_geometry->has_value()) {
            WkbGeometry parsed;
            RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &parsed, semantics));
            constant_geometry->emplace(std::move(parsed));
        }
        return &constant_geometry->value();
    }
    RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), varying_geometry, semantics));
    return varying_geometry;
}

template <LogicalType Type>
Status prepare_containment_constants(FunctionContext* context, NativeGeoContainmentState* state) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    for (size_t i = 0; i < 2; ++i) {
        if (!context->is_constant_column(i)) continue;
        auto& argument = state->arguments[i];
        argument.constant = true;
        const auto& column = context->get_constant_column(i);
        if (column->only_null()) {
            continue;
        }
        ASSIGN_OR_RETURN(auto input, geo_input<Type>(column));
        WkbGeometry geometry;
        argument.parse_status = WkbCodec::parse_wkb(input.wkb(0), &geometry, semantics);
        if (argument.parse_status.ok()) argument.geometry.emplace(std::move(geometry));
    }
    return Status::OK();
}

template <LogicalType Type, bool PolygonFirst, bool IncludeBoundary>
StatusOr<ColumnPtr> geo_containment_predicate(FunctionContext* context, const Columns& columns,
                                              const char* function_name) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto lhs, geo_input<Type>(columns[0]));
    ASSIGN_OR_RETURN(auto rhs, geo_input<Type>(columns[1]));
    if constexpr (Type == TYPE_GEOMETRY) {
        if (!is_geo_compute_compatible(lhs.data->descriptor(), rhs.data->descriptor())) {
            return Status::InvalidArgument(std::string(function_name) + " requires compatible GEOMETRY descriptors");
        }
    }
    const bool constant = lhs.constant && rhs.constant;
    const size_t rows = constant ? 1 : size;
    ColumnBuilder<TYPE_BOOLEAN> result(rows);
    const auto* prepared_state = context == nullptr
                                         ? nullptr
                                         : reinterpret_cast<const NativeGeoContainmentState*>(
                                                   context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    NativeGeoContainmentThreadState* thread_state = nullptr;
    if constexpr (Type == TYPE_GEOGRAPHY) {
        if (prepared_state != nullptr) {
            thread_state = context->get_or_create_thread_state<NativeGeoContainmentThreadState>(
                    [] { return std::make_unique<NativeGeoContainmentThreadState>(); });
        }
    }
    std::optional<WkbGeometry> constant_left;
    std::optional<WkbGeometry> constant_right;
    std::optional<GeoPoint> constant_spherical_point;
    SphericalPolygonFamilyCache constant_spherical_polygon;
    for (size_t row = 0; row < rows; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry varying_left;
        WkbGeometry varying_right;
        const auto* prepared_left = prepared_state == nullptr ? nullptr : &prepared_state->arguments[0];
        const auto* prepared_right = prepared_state == nullptr ? nullptr : &prepared_state->arguments[1];
        ASSIGN_OR_RETURN(const auto* left,
                         containment_geometry(lhs, row, semantics, prepared_left, &constant_left, &varying_left));
        ASSIGN_OR_RETURN(const auto* right,
                         containment_geometry(rhs, row, semantics, prepared_right, &constant_right, &varying_right));
        const auto& polygon = PolygonFirst ? *left : *right;
        const auto& point = PolygonFirst ? *right : *left;
        if (point.type != WkbGeometryType::POINT ||
            (polygon.type != WkbGeometryType::POLYGON && polygon.type != WkbGeometryType::MULTIPOLYGON)) {
            return Status::InvalidArgument(std::string(function_name) +
                                           " supports only POINT with POLYGON/MULTIPOLYGON inputs");
        }
        if (point.empty || polygon.empty) {
            result.append(false);
            continue;
        }
        PointPolygonRelation relation;
        if constexpr (Type == TYPE_GEOGRAPHY) {
            constexpr size_t point_argument = PolygonFirst ? 1 : 0;
            constexpr size_t polygon_argument = PolygonFirst ? 0 : 1;
            GeoPoint local_point;
            GeoPoint* native_point = &local_point;
            if (PolygonFirst ? rhs.constant : lhs.constant) {
                const bool prepared_point =
                        prepared_state != nullptr && prepared_state->arguments[point_argument].constant;
                auto* cached_point = prepared_point ? &thread_state->points[point_argument] : &constant_spherical_point;
                if (!cached_point->has_value()) {
                    cached_point->emplace();
                    if (cached_point->value().from_coord(point.coordinates[0].x, point.coordinates[0].y) !=
                        GEO_PARSE_OK) {
                        return Status::InvalidArgument("Invalid GEOGRAPHY point coordinates");
                    }
                }
                native_point = &cached_point->value();
            } else if (native_point->from_coord(point.coordinates[0].x, point.coordinates[0].y) != GEO_PARSE_OK) {
                return Status::InvalidArgument("Invalid GEOGRAPHY point coordinates");
            }
            const bool prepared_polygon =
                    prepared_state != nullptr && prepared_state->arguments[polygon_argument].constant;
            auto* polygon_cache = (PolygonFirst ? lhs.constant : rhs.constant)
                                          ? (prepared_polygon ? &thread_state->polygons[polygon_argument]
                                                              : &constant_spherical_polygon)
                                          : nullptr;
            ASSIGN_OR_RETURN(relation, spherical_polygon_family_relation(polygon, *native_point, polygon_cache));
        } else {
            relation = planar_polygon_family_relation(polygon, point.coordinates[0]);
        }
        result.append(relation == PointPolygonRelation::INSIDE ||
                      (IncludeBoundary && relation == PointPolygonRelation::BOUNDARY));
    }
    auto output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

template <LogicalType Type>
StatusOr<ColumnPtr> geo_intersects(FunctionContext* context, const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto lhs, geo_input<Type>(columns[0]));
    ASSIGN_OR_RETURN(auto rhs, geo_input<Type>(columns[1]));
    if constexpr (Type == TYPE_GEOMETRY) {
        if (!is_geo_compute_compatible(lhs.data->descriptor(), rhs.data->descriptor())) {
            return Status::InvalidArgument("ST_Intersects requires compatible GEOMETRY descriptors");
        }
    }
    const bool constant = lhs.constant && rhs.constant;
    const size_t rows = constant ? 1 : size;
    ColumnBuilder<TYPE_BOOLEAN> result(rows);
    const auto* prepared_state = context == nullptr
                                         ? nullptr
                                         : reinterpret_cast<const NativeGeoContainmentState*>(
                                                   context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    NativeGeoContainmentThreadState* thread_state = nullptr;
    if constexpr (Type == TYPE_GEOGRAPHY) {
        if (prepared_state != nullptr) {
            thread_state = context->get_or_create_thread_state<NativeGeoContainmentThreadState>(
                    [] { return std::make_unique<NativeGeoContainmentThreadState>(); });
        }
    }
    std::optional<WkbGeometry> constant_geometry[2];
    SphericalPolygonFamilyCache constant_spherical_polygons[2];
    for (size_t row = 0; row < rows; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry varying_geometry[2];
        const auto* prepared_left = prepared_state == nullptr ? nullptr : &prepared_state->arguments[0];
        const auto* prepared_right = prepared_state == nullptr ? nullptr : &prepared_state->arguments[1];
        ASSIGN_OR_RETURN(const auto* left, containment_geometry(lhs, row, semantics, prepared_left,
                                                                &constant_geometry[0], &varying_geometry[0]));
        ASSIGN_OR_RETURN(const auto* right, containment_geometry(rhs, row, semantics, prepared_right,
                                                                 &constant_geometry[1], &varying_geometry[1]));
        const auto supported = [](const WkbGeometry& geometry) {
            return geometry.type == WkbGeometryType::POLYGON || geometry.type == WkbGeometryType::MULTIPOLYGON;
        };
        if (!supported(*left) || !supported(*right)) {
            return Status::InvalidArgument("ST_Intersects supports only POLYGON/MULTIPOLYGON inputs");
        }
        if (left->empty || right->empty) {
            result.append(false);
            continue;
        }
        if constexpr (Type == TYPE_GEOGRAPHY) {
            const bool prepared_left_constant = prepared_state != nullptr && prepared_state->arguments[0].constant;
            const bool prepared_right_constant = prepared_state != nullptr && prepared_state->arguments[1].constant;
            auto* left_cache = lhs.constant ? (prepared_left_constant ? &thread_state->polygons[0]
                                                                      : &constant_spherical_polygons[0])
                                            : nullptr;
            auto* right_cache = rhs.constant ? (prepared_right_constant ? &thread_state->polygons[1]
                                                                        : &constant_spherical_polygons[1])
                                             : nullptr;
            ASSIGN_OR_RETURN(auto intersects,
                             spherical_polygon_family_intersects(*left, *right, left_cache, right_cache));
            result.append(intersects);
        } else {
            result.append(planar_polygon_family_intersects(*left, *right));
        }
    }
    auto output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<MutableColumnPtr> create_geography_result(FunctionContext* context) {
    const auto& type = context->get_return_type();
    if (type.type != TYPE_GEOGRAPHY || !type.geo_type.has_value()) {
        return Status::NotSupported("GEOGRAPHY constructor requires semantic type metadata");
    }
    GeoColumnDescriptor descriptor{type.geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    auto data = GeoColumn::create(std::move(descriptor));
    RETURN_IF_ERROR(check_geography_boundary(*data));
    return NullableColumn::create(std::move(data), NullColumn::create());
}

StatusOr<MutableColumnPtr> create_geometry_result(FunctionContext* context) {
    const auto& type = context->get_return_type();
    if (type.type != TYPE_GEOMETRY || !type.geo_type.has_value()) {
        return Status::NotSupported("GEOMETRY constructor requires semantic type metadata");
    }
    GeoColumnDescriptor descriptor{type.geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    auto data = GeoColumn::create(std::move(descriptor));
    RETURN_IF_ERROR(check_geometry_boundary(*data));
    return NullableColumn::create(std::move(data), NullColumn::create());
}

Status overlay_checkpoint(FunctionContext* context) {
    if (context != nullptr && context->state() != nullptr) {
        RETURN_IF_CANCELLED(context->state());
        RETURN_IF_ERROR(context->state()->check_query_state("Polygon overlay"));
        RETURN_IF_ERROR(context->state()->check_mem_limit("Polygon overlay"));
    }
    return Status::OK();
}

struct NativeGeoOverlayArgument {
    bool constant = false;
    Status status = Status::OK();
    std::unique_ptr<PreparedGeoPolygon> polygon;
};

struct NativeGeoOverlayState {
    std::array<NativeGeoOverlayArgument, 2> arguments;
};

StatusOr<const PreparedGeoPolygon*> overlay_polygon(const GeoInput& input, size_t row,
                                                    const NativeGeoOverlayArgument* prepared,
                                                    std::unique_ptr<PreparedGeoPolygon>* batch_constant,
                                                    std::unique_ptr<PreparedGeoPolygon>* varying) {
    if (prepared != nullptr && prepared->constant) {
        RETURN_IF_ERROR(prepared->status);
        return prepared->polygon.get();
    }
    auto* target = input.constant ? batch_constant : varying;
    if (!input.constant || *target == nullptr) {
        ASSIGN_OR_RETURN(*target, PreparedGeoPolygon::prepare(input.wkb(row)));
    }
    return target->get();
}

StatusOr<ColumnPtr> geo_overlay(FunctionContext* context, const Columns& columns, GeoOverlayKind kind) {
    RETURN_IF_ERROR(overlay_checkpoint(context));
    if (context == nullptr) return Status::InvalidArgument("Polygon overlay requires a function context");
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto left, geo_input<TYPE_GEOMETRY>(columns[0]));
    ASSIGN_OR_RETURN(auto right, geo_input<TYPE_GEOMETRY>(columns[1]));
    if (!is_geo_compute_compatible(left.data->descriptor(), right.data->descriptor())) {
        return Status::InvalidArgument("Polygon overlay requires compatible GEOMETRY CRS descriptors");
    }
    ASSIGN_OR_RETURN(auto result, create_geometry_result(context));
    const auto* output =
            down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(result.get())->data_column().get());
    if (!is_geo_compute_compatible(left.data->descriptor(), output->descriptor())) {
        return Status::InvalidArgument("Polygon overlay return CRS does not match its inputs");
    }
    const auto* prepared = reinterpret_cast<const NativeGeoOverlayState*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::array<std::unique_ptr<PreparedGeoPolygon>, 2> batch_constants;
    const bool constant = left.constant && right.constant;
    const size_t rows = constant ? 1 : size;
    for (size_t row = 0; row < rows; ++row) {
        RETURN_IF_ERROR(overlay_checkpoint(context));
        if (left.is_null(row) || right.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        std::array<std::unique_ptr<PreparedGeoPolygon>, 2> varying;
        ASSIGN_OR_RETURN(const auto* a, overlay_polygon(left, row, prepared ? &prepared->arguments[0] : nullptr,
                                                        &batch_constants[0], &varying[0]));
        ASSIGN_OR_RETURN(const auto* b, overlay_polygon(right, row, prepared ? &prepared->arguments[1] : nullptr,
                                                        &batch_constants[1], &varying[1]));
        RETURN_IF_ERROR(overlay_checkpoint(context));
        ASSIGN_OR_RETURN(auto geometry, a->overlay(*b, kind, [&]() { return overlay_checkpoint(context); }));
        RETURN_IF_ERROR(overlay_checkpoint(context));
        std::string wkb;
        RETURN_IF_ERROR(WkbCodec::to_wkb(geometry, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        result->append_datum(Datum(Slice(wkb)));
        RETURN_IF_ERROR(overlay_checkpoint(context));
    }
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

struct NativeGeoBufferState {
    Status status = Status::OK();
    std::unique_ptr<PreparedGeoBuffer> input;
};

Status buffer_checkpoint(FunctionContext* context) {
    if (context != nullptr && context->state() != nullptr) {
        RETURN_IF_CANCELLED(context->state());
        RETURN_IF_ERROR(context->state()->check_query_state("ST_Buffer"));
        RETURN_IF_ERROR(context->state()->check_mem_limit("ST_Buffer"));
    }
    return Status::OK();
}

StatusOr<ColumnPtr> geo_buffer(FunctionContext* context, const Columns& columns) {
    if (context == nullptr || columns.size() != 2) {
        return Status::InvalidArgument("ST_Buffer requires a context and two arguments");
    }
    RETURN_IF_ERROR(buffer_checkpoint(context));
    const size_t size = columns[0]->size();
    if (columns[1]->size() != size) return Status::InvalidArgument("ST_Buffer argument sizes differ");
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<TYPE_GEOMETRY>(columns[0]));
    ASSIGN_OR_RETURN(auto result, create_geometry_result(context));
    const auto* output =
            down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(result.get())->data_column().get());
    if (!is_geo_compute_compatible(input.data->descriptor(), output->descriptor())) {
        return Status::InvalidArgument("ST_Buffer return CRS does not match its input");
    }
    ColumnViewer<TYPE_DOUBLE> distances(columns[1]);
    const auto* prepared =
            reinterpret_cast<const NativeGeoBufferState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::unique_ptr<PreparedGeoBuffer> batch_constant;
    const bool constant = input.constant && columns[1]->is_constant();
    const size_t rows = constant && size != 0 ? 1 : size;
    for (size_t row = 0; row < rows; ++row) {
        RETURN_IF_ERROR(buffer_checkpoint(context));
        if (input.is_null(row) || distances.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        const double distance = distances.value(row);
        if (!std::isfinite(distance)) return Status::InvalidArgument("ST_Buffer requires a finite distance");
        std::unique_ptr<PreparedGeoBuffer> varying;
        const PreparedGeoBuffer* model;
        if (prepared != nullptr) {
            RETURN_IF_ERROR(prepared->status);
            model = prepared->input.get();
            if (model == nullptr) return Status::InvalidArgument("ST_Buffer constant geometry is NULL");
        } else {
            auto* target = input.constant ? &batch_constant : &varying;
            if (!input.constant || *target == nullptr) {
                ASSIGN_OR_RETURN(*target, PreparedGeoBuffer::prepare(input.wkb(row),
                                                                     [&] { return buffer_checkpoint(context); }));
            }
            model = target->get();
        }
        ASSIGN_OR_RETURN(auto geometry, model->buffer(distance, [&] { return buffer_checkpoint(context); }));
        std::string wkb;
        RETURN_IF_ERROR(WkbCodec::to_wkb(geometry, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        result->append_datum(Datum(Slice(wkb)));
        RETURN_IF_ERROR(buffer_checkpoint(context));
    }
    if (constant && size != 0) return ConstColumn::create(std::move(result), size);
    return result;
}

template <LogicalType InputType>
StatusOr<ColumnPtr> construct_geography(FunctionContext* context, const Columns& columns, bool text) {
    const size_t size = columns[0]->size();
    const bool constant = ColumnHelper::is_all_const(columns);
    const size_t rows = constant ? 1 : size;
    ColumnViewer<InputType> input(columns[0]);
    std::optional<ColumnViewer<TYPE_INT>> srid;
    if (columns.size() == 2) srid.emplace(columns[1]);
    ASSIGN_OR_RETURN(auto result, create_geography_result(context));

    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row) || (srid && (srid->is_null(row) || srid->value(row) != kCrs84Srid))) {
            result->append_nulls(1);
            continue;
        }
        WkbGeometry geometry;
        const Slice value = input.value(row);
        Status status = text ? WkbCodec::parse_wkt(std::string_view(value.data, value.size), &geometry)
                             : WkbCodec::parse_wkb(value, &geometry);
        std::string wkb;
        if (status.ok()) status = WkbCodec::to_wkb(geometry, &wkb);
        if (!status.ok()) {
            result->append_nulls(1);
        } else {
            result->append_datum(Datum(Slice(wkb)));
        }
    }
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

template <LogicalType InputType>
StatusOr<ColumnPtr> construct_geometry(FunctionContext* context, const Columns& columns, bool text) {
    if (columns.size() != 2 || !columns[1]->is_constant()) {
        return Status::InvalidArgument("GEOMETRY constructor requires a constant CRS");
    }
    ColumnViewer<TYPE_VARCHAR> crs(columns[1]);
    if (crs.is_null(0)) return Status::InvalidArgument("GEOMETRY constructor requires a non-empty CRS");
    const Slice crs_value = crs.value(0);
    const auto& return_type = context->get_return_type();
    if (!return_type.geo_type.has_value() ||
        return_type.geo_type->crs != std::string_view(crs_value.data, crs_value.size)) {
        return Status::InvalidArgument("GEOMETRY constructor CRS does not match its return descriptor");
    }

    const size_t size = columns[0]->size();
    const bool constant = ColumnHelper::is_all_const(columns);
    const size_t rows = constant ? 1 : size;
    ColumnViewer<InputType> input(columns[0]);
    ASSIGN_OR_RETURN(auto result, create_geometry_result(context));
    constexpr auto semantics = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        WkbGeometry geometry;
        const Slice value = input.value(row);
        Status status = text ? WkbCodec::parse_wkt(std::string_view(value.data, value.size), &geometry, semantics)
                             : WkbCodec::parse_wkb(value, &geometry, semantics);
        std::string wkb;
        if (status.ok()) status = WkbCodec::to_wkb(geometry, &wkb, semantics);
        if (!status.ok()) {
            result->append_nulls(1);
        } else {
            result->append_datum(Datum(Slice(wkb)));
        }
    }
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

struct NativeGeoUnaryState {
    bool constant = false;
    Status parse_status = Status::OK();
    std::optional<WkbGeometry> geometry;
};

StatusOr<const WkbGeometry*> unary_geometry(const GeoInput& input, size_t row, WkbCoordinateSemantics semantics,
                                            const NativeGeoUnaryState* prepared,
                                            std::optional<WkbGeometry>* constant_geometry,
                                            WkbGeometry* varying_geometry) {
    if (prepared != nullptr && prepared->constant) {
        RETURN_IF_ERROR(prepared->parse_status);
        return &prepared->geometry.value();
    }
    if (input.constant) {
        if (!constant_geometry->has_value()) {
            WkbGeometry parsed;
            RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &parsed, semantics));
            constant_geometry->emplace(std::move(parsed));
        }
        return &constant_geometry->value();
    }
    RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), varying_geometry, semantics));
    return varying_geometry;
}

template <LogicalType Type, GeoMeasurementKind Kind>
StatusOr<ColumnPtr> geo_measure(FunctionContext* context, const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<Type>(columns[0]));
    const size_t rows = input.constant ? 1 : size;
    const auto* prepared = context == nullptr ? nullptr
                                              : reinterpret_cast<const NativeGeoUnaryState*>(
                                                        context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::optional<WkbGeometry> constant_geometry;
    ColumnBuilder<TYPE_DOUBLE> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry varying_geometry;
        ASSIGN_OR_RETURN(const auto* geometry,
                         unary_geometry(input, row, semantics, prepared, &constant_geometry, &varying_geometry));
        if constexpr (Type == TYPE_GEOGRAPHY) {
            ASSIGN_OR_RETURN(auto value, spherical_measurement(*geometry, Kind));
            result.append(value);
        } else {
            ASSIGN_OR_RETURN(auto value, planar_measurement(*geometry, Kind));
            result.append(value);
        }
    }
    auto output = result.build(false);
    if (input.constant) return ConstColumn::create(std::move(output), size);
    return output;
}

template <LogicalType Type>
StatusOr<ColumnPtr> geo_validity(FunctionContext* context, const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<Type>(columns[0]));
    const size_t rows = input.constant ? 1 : size;
    const auto* prepared = context == nullptr ? nullptr
                                              : reinterpret_cast<const NativeGeoUnaryState*>(
                                                        context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::optional<WkbGeometry> constant_geometry;
    ColumnBuilder<TYPE_BOOLEAN> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry varying_geometry;
        ASSIGN_OR_RETURN(const auto* geometry,
                         unary_geometry(input, row, semantics, prepared, &constant_geometry, &varying_geometry));
        if constexpr (Type == TYPE_GEOGRAPHY) {
            ASSIGN_OR_RETURN(auto valid, spherical_is_valid(*geometry));
            result.append(valid);
        } else {
            ASSIGN_OR_RETURN(auto valid, planar_is_valid(*geometry));
            result.append(valid);
        }
    }
    auto output = result.build(false);
    if (input.constant) return ConstColumn::create(std::move(output), size);
    return output;
}

template <LogicalType Type>
StatusOr<MutableColumnPtr> create_centroid_result(FunctionContext* context) {
    if (context == nullptr) return Status::InvalidArgument("ST_Centroid requires a function context");
    const auto& type = context->get_return_type();
    if (type.type != Type || !type.geo_type.has_value()) {
        return Status::NotSupported("ST_Centroid requires native GEO return metadata");
    }
    GeoColumnDescriptor descriptor{type.geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    auto data = GeoColumn::create(std::move(descriptor));
    if constexpr (Type == TYPE_GEOGRAPHY) {
        RETURN_IF_ERROR(check_geography_boundary(*data));
    } else {
        RETURN_IF_ERROR(check_geometry_compute_boundary(*data));
    }
    return NullableColumn::create(std::move(data), NullColumn::create());
}

template <LogicalType Type>
StatusOr<ColumnPtr> geo_centroid(FunctionContext* context, const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<Type>(columns[0]));
    const size_t rows = input.constant ? 1 : size;
    const auto* prepared =
            reinterpret_cast<const NativeGeoUnaryState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::optional<WkbGeometry> constant_geometry;
    ASSIGN_OR_RETURN(auto result, create_centroid_result<Type>(context));
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        WkbGeometry varying_geometry;
        ASSIGN_OR_RETURN(const auto* geometry,
                         unary_geometry(input, row, semantics, prepared, &constant_geometry, &varying_geometry));
        GeoCentroidResult centroid;
        if constexpr (Type == TYPE_GEOGRAPHY) {
            ASSIGN_OR_RETURN(centroid, spherical_centroid(*geometry));
        } else {
            ASSIGN_OR_RETURN(centroid, planar_centroid(*geometry));
        }
        WkbGeometry point;
        point.type = WkbGeometryType::POINT;
        point.empty = centroid.empty;
        if (!centroid.empty) point.coordinates.emplace_back(centroid.coordinate);
        std::string wkb;
        RETURN_IF_ERROR(WkbCodec::to_wkb(point, &wkb, semantics));
        result->append_datum(Datum(Slice(wkb)));
    }
    if (input.constant) return ConstColumn::create(std::move(result), size);
    return result;
}
template <LogicalType OutputType, LogicalType GeoType>
StatusOr<ColumnPtr> serialize_geo(const Columns& columns, bool text) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    const bool constant = columns[0]->is_constant();
    const size_t rows = constant ? 1 : size;
    const Column* source = columns[0].get();
    if (constant) source = down_cast<const ConstColumn*>(source)->data_column().get();
    const NullableColumn* nullable = source->is_nullable() ? down_cast<const NullableColumn*>(source) : nullptr;
    const auto* geo = down_cast<const GeoColumn*>(nullable ? nullable->data_column().get() : source);
    if constexpr (GeoType == TYPE_GEOGRAPHY) {
        RETURN_IF_ERROR(check_geography_boundary(*geo));
    } else {
        static_assert(GeoType == TYPE_GEOMETRY);
        RETURN_IF_ERROR(check_geometry_boundary(*geo));
    }
    constexpr auto semantics = GeoType == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                         : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;

    ColumnBuilder<OutputType> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (nullable != nullptr && nullable->is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(geo->get_wkb(row), &geometry, semantics));
        std::string value;
        RETURN_IF_ERROR(text ? WkbCodec::to_wkt(geometry, &value, semantics)
                             : WkbCodec::to_wkb(geometry, &value, semantics));
        result.append(Slice(value));
    }
    auto output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

struct NativeGeoCrsState {
    std::string source;
    std::string target;
};

constexpr double kPi = 3.141592653589793238462643383279502884;
constexpr double kWebMercatorRadius = 6378137.0;
constexpr double kWebMercatorMaxLatitude = 85.0511287798066;
constexpr double kWebMercatorMaxCoordinate = kWebMercatorRadius * kPi + 1e-7;

StatusOr<int32_t> supported_transform_srid(std::string_view crs) {
    if (crs == "EPSG:4326" || crs == "OGC:CRS84") return 4326;
    if (crs == "EPSG:3857") return 3857;
    return Status::NotSupported("ST_Transform supports only EPSG:4326 and EPSG:3857");
}

Status transform_coordinate(WkbCoordinate* point, int32_t source_srid, int32_t target_srid) {
    if (!std::isfinite(point->x) || !std::isfinite(point->y)) {
        return Status::InvalidArgument("ST_Transform coordinate is not finite");
    }
    if (source_srid == target_srid) return Status::OK();
    if (source_srid == 4326) {
        if (std::abs(point->x) > 180.0 || std::abs(point->y) > kWebMercatorMaxLatitude) {
            return Status::InvalidArgument("ST_Transform coordinate is outside the EPSG:4326 Web Mercator domain");
        }
        const double longitude = point->x * kPi / 180.0;
        const double latitude = point->y * kPi / 180.0;
        point->x = kWebMercatorRadius * longitude;
        point->y = kWebMercatorRadius * std::asinh(std::tan(latitude));
    } else {
        if (std::abs(point->x) > kWebMercatorMaxCoordinate || std::abs(point->y) > kWebMercatorMaxCoordinate) {
            return Status::InvalidArgument("ST_Transform coordinate is outside the EPSG:3857 Web Mercator domain");
        }
        point->x = point->x / kWebMercatorRadius * 180.0 / kPi;
        point->y = std::atan(std::sinh(point->y / kWebMercatorRadius)) * 180.0 / kPi;
        // Rounding at the Web Mercator edge must not make a valid inverse result
        // invalid as input to a subsequent forward transformation.
        point->x = std::clamp(point->x, -180.0, 180.0);
        point->y = std::clamp(point->y, -kWebMercatorMaxLatitude, kWebMercatorMaxLatitude);
    }
    if (!std::isfinite(point->x) || !std::isfinite(point->y)) {
        return Status::InvalidArgument("ST_Transform coordinate is outside the supported CRS domain");
    }
    return Status::OK();
}

Status transform_geometry_coordinates(WkbGeometry* geometry, int32_t source_srid, int32_t target_srid) {
    auto transform_point = [source_srid, target_srid](WkbCoordinate* point) -> Status {
        return transform_coordinate(point, source_srid, target_srid);
    };
    for (auto& coordinate : geometry->coordinates) RETURN_IF_ERROR(transform_point(&coordinate));
    for (auto& ring : geometry->rings) {
        for (auto& coordinate : ring) RETURN_IF_ERROR(transform_point(&coordinate));
    }
    for (auto& child : geometry->children) {
        RETURN_IF_ERROR(transform_geometry_coordinates(&child, source_srid, target_srid));
    }
    return Status::OK();
}

StatusOr<int32_t> checked_target_srid(FunctionContext* context, const Columns& columns) {
    if (context == nullptr || columns.size() != 2 || !columns[1]->is_constant() || columns[1]->only_null()) {
        return Status::InvalidArgument("Target SRID must be a non-NULL constant INT");
    }
    ColumnViewer<TYPE_INT> target(columns[1]);
    if (target.is_null(0) || target.value(0) <= 0) {
        return Status::InvalidArgument("Target SRID must be a positive INT");
    }
    const int32_t value = target.value(0);
    const auto& type = context->get_return_type();
    if (type.type != TYPE_GEOMETRY || !type.geo_type.has_value() ||
        type.geo_type->crs != "EPSG:" + std::to_string(value) || type.geo_type->srid != value) {
        return Status::InvalidArgument("Target SRID conflicts with GEOMETRY result descriptor");
    }
    return value;
}

} // namespace

struct StConstructState {
    StConstructState() = default;
    ~StConstructState() = default;

    bool is_null{false};
    std::string encoded_buf;
};

StatusOr<ColumnPtr> GeoFunctions::st_from_wkt_common(FunctionContext* ctx, const Columns& columns,
                                                     GeoShapeType shape_type) {
    ColumnViewer<TYPE_VARCHAR> wkt_viewer(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    auto* state = (StConstructState*)ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    if (state == nullptr) {
        for (int row = 0; row < size; ++row) {
            if (wkt_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            GeoParseStatus status;
            auto wkt_value = wkt_viewer.value(row);
            std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt_value.data, wkt_value.size, &status));
            if (shape == nullptr || (shape_type != GEO_SHAPE_ANY && shape->type() != shape_type)) {
                result.append_null();
                continue;
            }
            std::string buf;
            shape->encode_to(&buf);
            result.append(Slice(buf.data(), buf.size()));
        }

        return result.build(false);
    } else {
        if (state->is_null) {
            return ColumnHelper::create_const_null_column(size);
        } else {
            return ColumnHelper::create_const_column<TYPE_VARCHAR>(
                    Slice(state->encoded_buf.data(), state->encoded_buf.size()), size);
        }
    }
}

StatusOr<ColumnPtr> GeoFunctions::st_from_wkt(FunctionContext* context, const Columns& columns) {
    return st_from_wkt_common(context, columns, GEO_SHAPE_ANY);
}

StatusOr<ColumnPtr> GeoFunctions::st_line(FunctionContext* context, const Columns& columns) {
    return st_from_wkt_common(context, columns, GEO_SHAPE_LINE_STRING);
}

StatusOr<ColumnPtr> GeoFunctions::st_polygon(FunctionContext* context, const Columns& columns) {
    return st_from_wkt_common(context, columns, GEO_SHAPE_POLYGON);
}

Status GeoFunctions::st_from_wkt_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<StConstructState*>(context->get_function_state(scope));
        delete state;
    }

    return Status::OK();
}

Status GeoFunctions::st_circle_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!ctx->is_constant_column(0) || !ctx->is_constant_column(1) || !ctx->is_constant_column(2)) {
        return Status::OK();
    }

    auto state = new StConstructState();
    auto lng = ctx->get_constant_column(0);
    auto lat = ctx->get_constant_column(1);
    auto radius = ctx->get_constant_column(2);
    if (lng->only_null() || lat->only_null() || radius->only_null()) {
        state->is_null = true;
    } else {
        std::unique_ptr<GeoCircle> circle(new GeoCircle());
        auto lng_value = ColumnHelper::get_const_value<TYPE_DOUBLE>(lng);
        auto lat_value = ColumnHelper::get_const_value<TYPE_DOUBLE>(lat);
        auto radius_value = ColumnHelper::get_const_value<TYPE_DOUBLE>(radius);

        auto res = circle->init(lng_value, lat_value, radius_value);
        if (res != GEO_PARSE_OK) {
            state->is_null = true;
        } else {
            circle->encode_to(&state->encoded_buf);
        }
    }
    ctx->set_function_state(scope, state);

    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_circle(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> lng_viewer(columns[0]);
    ColumnViewer<TYPE_DOUBLE> lat_viewer(columns[1]);
    ColumnViewer<TYPE_DOUBLE> radius_viewer(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    auto* state = (StConstructState*)context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    if (state == nullptr) {
        for (int row = 0; row < size; ++row) {
            if (lng_viewer.is_null(row) || lat_viewer.is_null(row) || radius_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            GeoCircle circle;
            //std::unique_ptr<GeoCircle> circle(new GeoCircle());
            auto lng_value = lng_viewer.value(row);
            auto lat_value = lat_viewer.value(row);
            auto radius_value = radius_viewer.value(row);

            auto res = circle.init(lng_value, lat_value, radius_value);
            if (res != GEO_PARSE_OK) {
                result.append_null();
                continue;
            }
            std::string buf;
            circle.encode_to(&buf);
            result.append(Slice(buf.data(), buf.size()));
        }

        return result.build(false);
    } else {
        if (state->is_null) {
            return ColumnHelper::create_const_null_column(size);
        } else {
            return ColumnHelper::create_const_column<TYPE_VARCHAR>(
                    Slice(state->encoded_buf.data(), state->encoded_buf.size()), size);
        }
    }
}

StatusOr<ColumnPtr> GeoFunctions::st_point(FunctionContext* context, const Columns& columns) {
    auto x_column = ColumnViewer<TYPE_DOUBLE>(columns[0]);
    auto y_column = ColumnViewer<TYPE_DOUBLE>(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (x_column.is_null(row) || y_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto x_value = x_column.value(row);
        auto y_value = y_column.value(row);
        GeoPoint point;
        auto res = point.from_coord(x_value, y_value);
        if (res != GEO_PARSE_OK) {
            result.append_null();
            continue;
        }

        std::string buf;
        point.encode_to(&buf);
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> GeoFunctions::st_x(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> encode(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (encode.is_null(row)) {
            result.append_null();
            continue;
        }

        auto encode_value = encode.value(row);
        GeoPoint point;
        auto res = point.decode_from(encode_value.data, encode_value.size);
        if (!res) {
            result.append_null();
            continue;
        }

        result.append(point.x());
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> GeoFunctions::st_y(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> encode(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (encode.is_null(row)) {
            result.append_null();
            continue;
        }

        auto encode_value = encode.value(row);
        GeoPoint point;
        auto res = point.decode_from(encode_value.data, encode_value.size);
        if (!res) {
            result.append_null();
            continue;
        }

        result.append(point.y());
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> GeoFunctions::st_distance_sphere(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> x_lng(columns[0]);
    ColumnViewer<TYPE_DOUBLE> x_lat(columns[1]);
    ColumnViewer<TYPE_DOUBLE> y_lng(columns[2]);
    ColumnViewer<TYPE_DOUBLE> y_lat(columns[3]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> result(size);
    for (int row = 0; row < size; ++row) {
        if (x_lng.is_null(row) || x_lat.is_null(row) || y_lng.is_null(row) || y_lat.is_null(row)) {
            result.append_null();
            continue;
        }

        auto x_lng_value = x_lng.value(row);
        auto x_lat_value = x_lat.value(row);
        auto y_lng_value = y_lng.value(row);
        auto y_lat_value = y_lat.value(row);

        double dist_value;
        if (!GeoPoint::st_distance_sphere(x_lng_value, x_lat_value, y_lng_value, y_lat_value, &dist_value)) {
            result.append_null();
            continue;
        }

        result.append(dist_value);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> GeoFunctions::st_as_wkt(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> shape_viewer(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (shape_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto shape_value = shape_viewer.value(row);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(shape_value.data, shape_value.size));
        if (shape == nullptr) {
            result.append_null();
            continue;
        }

        auto wkt = shape->as_wkt();
        result.append(Slice(wkt.data(), wkt.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> GeoFunctions::st_geog_from_text(FunctionContext* context, const Columns& columns) {
    return construct_geography<TYPE_VARCHAR>(context, columns, true);
}

StatusOr<ColumnPtr> GeoFunctions::st_geog_from_wkb(FunctionContext* context, const Columns& columns) {
    return construct_geography<TYPE_VARBINARY>(context, columns, false);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_as_text(FunctionContext*, const Columns& columns) {
    return serialize_geo<TYPE_VARCHAR, TYPE_GEOGRAPHY>(columns, true);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_as_wkb(FunctionContext*, const Columns& columns) {
    return serialize_geo<TYPE_VARBINARY, TYPE_GEOGRAPHY>(columns, false);
}

StatusOr<ColumnPtr> GeoFunctions::st_geom_from_text(FunctionContext* context, const Columns& columns) {
    return construct_geometry<TYPE_VARCHAR>(context, columns, true);
}

StatusOr<ColumnPtr> GeoFunctions::st_geom_from_wkb(FunctionContext* context, const Columns& columns) {
    return construct_geometry<TYPE_VARBINARY>(context, columns, false);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_as_text(FunctionContext*, const Columns& columns) {
    return serialize_geo<TYPE_VARCHAR, TYPE_GEOMETRY>(columns, true);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_as_wkb(FunctionContext*, const Columns& columns) {
    return serialize_geo<TYPE_VARBINARY, TYPE_GEOMETRY>(columns, false);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_x(FunctionContext*, const Columns& columns) {
    return geo_coordinate<TYPE_GEOGRAPHY, true>(columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_y(FunctionContext*, const Columns& columns) {
    return geo_coordinate<TYPE_GEOGRAPHY, false>(columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_type(FunctionContext*, const Columns& columns) {
    return geo_type<TYPE_GEOGRAPHY>(columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_distance(FunctionContext* context, const Columns& columns) {
    return geo_distance<TYPE_GEOGRAPHY, false>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_dwithin(FunctionContext* context, const Columns& columns) {
    return geo_distance<TYPE_GEOGRAPHY, true>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_x(FunctionContext*, const Columns& columns) {
    return geo_coordinate<TYPE_GEOMETRY, true>(columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_y(FunctionContext*, const Columns& columns) {
    return geo_coordinate<TYPE_GEOMETRY, false>(columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_type(FunctionContext*, const Columns& columns) {
    return geo_type<TYPE_GEOMETRY>(columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_distance(FunctionContext* context, const Columns& columns) {
    return geo_distance<TYPE_GEOMETRY, false>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_dwithin(FunctionContext* context, const Columns& columns) {
    return geo_distance<TYPE_GEOMETRY, true>(context, columns);
}

Status GeoFunctions::native_geo_distance_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL ||
        (!context->is_constant_column(0) && !context->is_constant_column(1))) {
        return Status::OK();
    }
    const auto* left_type = context->get_arg_type(0);
    const auto* right_type = context->get_arg_type(1);
    if (left_type == nullptr || right_type == nullptr || left_type->type != right_type->type) {
        return Status::InvalidArgument("Native GEO distance requires two arguments of the same type");
    }
    auto state = std::make_unique<NativeGeoDistanceState>();
    if (left_type->type == TYPE_GEOGRAPHY) {
        RETURN_IF_ERROR(prepare_distance_constants<TYPE_GEOGRAPHY>(context, state.get()));
    } else if (left_type->type == TYPE_GEOMETRY) {
        RETURN_IF_ERROR(prepare_distance_constants<TYPE_GEOMETRY>(context, state.get()));
    } else {
        return Status::InvalidArgument("Native GEO distance requires GEOGRAPHY or GEOMETRY arguments");
    }
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::native_geo_distance_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<NativeGeoDistanceState*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

Status GeoFunctions::native_geo_containment_prepare(FunctionContext* context,
                                                    FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL ||
        (!context->is_constant_column(0) && !context->is_constant_column(1))) {
        return Status::OK();
    }
    const auto* left_type = context->get_arg_type(0);
    const auto* right_type = context->get_arg_type(1);
    if (left_type == nullptr || right_type == nullptr || left_type->type != right_type->type) {
        return Status::InvalidArgument("Native GEO containment requires two arguments of the same type");
    }
    auto state = std::make_unique<NativeGeoContainmentState>();
    if (left_type->type == TYPE_GEOGRAPHY) {
        RETURN_IF_ERROR(prepare_containment_constants<TYPE_GEOGRAPHY>(context, state.get()));
    } else if (left_type->type == TYPE_GEOMETRY) {
        RETURN_IF_ERROR(prepare_containment_constants<TYPE_GEOMETRY>(context, state.get()));
    } else {
        return Status::InvalidArgument("Native GEO containment requires GEOGRAPHY or GEOMETRY arguments");
    }
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::native_geo_containment_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<NativeGeoContainmentState*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_contains(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOGRAPHY, true, false>(context, columns, "ST_Contains");
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_contains(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOMETRY, true, false>(context, columns, "ST_Contains");
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_within(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOGRAPHY, false, false>(context, columns, "ST_Within");
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_within(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOMETRY, false, false>(context, columns, "ST_Within");
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_covers(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOGRAPHY, true, true>(context, columns, "ST_Covers");
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_covers(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOMETRY, true, true>(context, columns, "ST_Covers");
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_covered_by(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOGRAPHY, false, true>(context, columns, "ST_CoveredBy");
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_covered_by(FunctionContext* context, const Columns& columns) {
    return geo_containment_predicate<TYPE_GEOMETRY, false, true>(context, columns, "ST_CoveredBy");
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_intersects(FunctionContext* context, const Columns& columns) {
    return geo_intersects<TYPE_GEOGRAPHY>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_intersects(FunctionContext* context, const Columns& columns) {
    return geo_intersects<TYPE_GEOMETRY>(context, columns);
}
Status GeoFunctions::native_geo_unary_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || !context->is_constant_column(0)) return Status::OK();
    const auto* argument_type = context->get_arg_type(0);
    if (argument_type == nullptr || (argument_type->type != TYPE_GEOGRAPHY && argument_type->type != TYPE_GEOMETRY)) {
        return Status::InvalidArgument("Native GEO unary function requires GEOGRAPHY or GEOMETRY");
    }
    auto state = std::make_unique<NativeGeoUnaryState>();
    state->constant = true;
    const auto& column = context->get_constant_column(0);
    if (!column->only_null()) {
        if (argument_type->type == TYPE_GEOGRAPHY) {
            ASSIGN_OR_RETURN(auto input, geo_input<TYPE_GEOGRAPHY>(column));
            WkbGeometry geometry;
            state->parse_status = WkbCodec::parse_wkb(input.wkb(0), &geometry, WkbCoordinateSemantics::GEOGRAPHY_CRS84);
            if (state->parse_status.ok()) state->geometry.emplace(std::move(geometry));
        } else {
            ASSIGN_OR_RETURN(auto input, geo_input<TYPE_GEOMETRY>(column));
            WkbGeometry geometry;
            state->parse_status =
                    WkbCodec::parse_wkb(input.wkb(0), &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN);
            if (state->parse_status.ok()) state->geometry.emplace(std::move(geometry));
        }
    }
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::native_geo_unary_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<NativeGeoUnaryState*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_area(FunctionContext* context, const Columns& columns) {
    return geo_measure<TYPE_GEOGRAPHY, GeoMeasurementKind::AREA>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_area(FunctionContext* context, const Columns& columns) {
    return geo_measure<TYPE_GEOMETRY, GeoMeasurementKind::AREA>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_length(FunctionContext* context, const Columns& columns) {
    return geo_measure<TYPE_GEOGRAPHY, GeoMeasurementKind::LENGTH>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_length(FunctionContext* context, const Columns& columns) {
    return geo_measure<TYPE_GEOMETRY, GeoMeasurementKind::LENGTH>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_perimeter(FunctionContext* context, const Columns& columns) {
    return geo_measure<TYPE_GEOGRAPHY, GeoMeasurementKind::PERIMETER>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_perimeter(FunctionContext* context, const Columns& columns) {
    return geo_measure<TYPE_GEOMETRY, GeoMeasurementKind::PERIMETER>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_centroid(FunctionContext* context, const Columns& columns) {
    return geo_centroid<TYPE_GEOGRAPHY>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_centroid(FunctionContext* context, const Columns& columns) {
    return geo_centroid<TYPE_GEOMETRY>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geography_is_valid(FunctionContext* context, const Columns& columns) {
    return geo_validity<TYPE_GEOGRAPHY>(context, columns);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_is_valid(FunctionContext* context, const Columns& columns) {
    return geo_validity<TYPE_GEOMETRY>(context, columns);
}

struct StContainsState {
    StContainsState() = default;
    ~StContainsState() {
        delete shapes[0];
        delete shapes[1];
    }
    bool is_null{false};
    GeoShape* shapes[2]{nullptr, nullptr};
};

Status GeoFunctions::st_contains_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* contains_ctx = reinterpret_cast<StContainsState*>(ctx->get_function_state(scope));
        delete contains_ctx;
    }

    return Status::OK();
}

Status GeoFunctions::st_contains_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!ctx->is_constant_column(0) && !ctx->is_constant_column(1)) {
        return Status::OK();
    }

    auto contains_ctx = new StContainsState();
    for (int i = 0; !contains_ctx->is_null && i < 2; ++i) {
        if (ctx->is_constant_column(i)) {
            auto str_column = ctx->get_constant_column(i);
            if (str_column->only_null()) {
                contains_ctx->is_null = true;
            } else {
                auto str_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(str_column);
                contains_ctx->shapes[i] = GeoShape::from_encoded(str_value.data, str_value.size);
                if (contains_ctx->shapes[i] == nullptr) {
                    contains_ctx->is_null = true;
                }
            }
        }
    }

    ctx->set_function_state(scope, contains_ctx);
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_contains(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> lhs_viewer(columns[0]);
    ColumnViewer<TYPE_VARCHAR> rhs_viewer(columns[1]);

    const StContainsState* state =
            reinterpret_cast<StContainsState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state != nullptr && state->is_null) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        if (lhs_viewer.is_null(row) || rhs_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        GeoShape* shapes[2] = {nullptr, nullptr};
        auto lhs_value = lhs_viewer.value(row);
        auto rhs_value = rhs_viewer.value(row);
        const Slice* strs[2] = {&lhs_value, &rhs_value};
        // use this to delete new
        StContainsState local_state;
        int i;
        for (i = 0; i < 2; ++i) {
            if (state != nullptr && state->shapes[i] != nullptr) {
                shapes[i] = state->shapes[i];
            } else {
                shapes[i] = local_state.shapes[i] = GeoShape::from_encoded(strs[i]->data, strs[i]->size);
                if (shapes[i] == nullptr) {
                    result.append_null();
                    break;
                }
            }
        }

        if (i == 2) {
            result.append(shapes[0]->contains(shapes[1]));
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// from wkt
Status GeoFunctions::st_from_wkt_prepare_common(FunctionContext* ctx, FunctionContext::FunctionStateScope scope,
                                                GeoShapeType shape_type) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!ctx->is_constant_column(0)) {
        return Status::OK();
    }

    auto state = new StConstructState();
    auto str_column = ctx->get_constant_column(0);
    if (str_column->only_null()) {
        state->is_null = true;
    } else {
        auto str_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(str_column);
        GeoParseStatus status;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(str_value.data, str_value.size, &status));
        if (shape == nullptr || (shape_type != GEO_SHAPE_ANY && shape->type() != shape_type)) {
            state->is_null = true;
        } else {
            shape->encode_to(&state->encoded_buf);
        }
    }

    ctx->set_function_state(scope, state);
    return Status::OK();
}

Status GeoFunctions::st_from_wkt_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    return st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_ANY);
}

Status GeoFunctions::st_line_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    return st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_LINE_STRING);
}

Status GeoFunctions::st_polygon_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    return st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_POLYGON);
}

Status GeoFunctions::native_geo_transform_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) return Status::OK();
    if (context == nullptr || !context->is_constant_column(1)) {
        return Status::InvalidArgument("Native GEOMETRY CRS functions require a constant target SRID");
    }
    const auto* source_type = context->get_arg_type(0);
    const auto& result_type = context->get_return_type();
    if (source_type == nullptr || source_type->type != TYPE_GEOMETRY || !source_type->geo_type.has_value() ||
        result_type.type != TYPE_GEOMETRY || !result_type.geo_type.has_value()) {
        return Status::InvalidArgument("Native GEOMETRY CRS functions require semantic type metadata");
    }
    auto target_column = context->get_constant_column(1);
    if (target_column == nullptr || target_column->only_null()) {
        return Status::InvalidArgument("Target SRID must be a non-NULL constant INT");
    }
    ColumnViewer<TYPE_INT> target(target_column);
    if (target.is_null(0) || target.value(0) <= 0) {
        return Status::InvalidArgument("Target SRID must be a positive INT");
    }
    const std::string target_crs = "EPSG:" + std::to_string(target.value(0));
    if (result_type.geo_type->crs != target_crs || result_type.geo_type->srid != target.value(0)) {
        return Status::InvalidArgument("Target SRID conflicts with GEOMETRY result descriptor");
    }
    auto state = std::make_unique<NativeGeoCrsState>();
    state->source = source_type->geo_type->crs;
    state->target = target_crs;
    if (state->source.empty() || source_type->geo_type->srid != derive_srid(state->source)) {
        return Status::InvalidArgument("Source GEOMETRY CRS is missing or conflicts with SRID metadata");
    }
    ASSIGN_OR_RETURN(const int32_t source_srid, supported_transform_srid(state->source));
    ASSIGN_OR_RETURN(const int32_t target_srid, supported_transform_srid(state->target));
    (void)source_srid;
    (void)target_srid;
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::native_geo_transform_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<NativeGeoCrsState*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_srid(FunctionContext*, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    const bool constant = columns[0]->is_constant();
    const Column* source =
            constant ? down_cast<const ConstColumn*>(columns[0].get())->data_column().get() : columns[0].get();
    const auto* nullable = source->is_nullable() ? down_cast<const NullableColumn*>(source) : nullptr;
    const auto* geo = down_cast<const GeoColumn*>(nullable ? nullable->data_column().get() : source);
    RETURN_IF_ERROR(check_geometry_boundary(*geo));
    const size_t rows = constant ? 1 : size;
    ColumnBuilder<TYPE_INT> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if ((nullable != nullptr && nullable->is_null(row)) || !geo->descriptor().type.srid.has_value()) {
            result.append_null();
        } else {
            result.append(geo->descriptor().type.srid.value());
        }
    }
    auto output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_set_srid(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(const int32_t target_srid, checked_target_srid(context, columns));
    const auto* state =
            reinterpret_cast<const NativeGeoCrsState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state == nullptr || state->target != "EPSG:" + std::to_string(target_srid)) {
        return Status::InvalidArgument("ST_SetSRID requires prepared CRS metadata");
    }
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<TYPE_GEOMETRY>(columns[0]));
    if (input.data->descriptor().type.crs != state->source) {
        return Status::InvalidArgument("Source GEOMETRY CRS differs from prepared descriptor");
    }
    const bool constant = ColumnHelper::is_all_const(columns);
    const size_t rows = constant ? 1 : size;
    ASSIGN_OR_RETURN(auto result, create_geometry_result(context));
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        // Re-tag the existing WKB; ST_SetSRID never changes its coordinate bytes.
        result->append_datum(Datum(input.wkb(row)));
    }
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_transform(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(const int32_t target_srid, checked_target_srid(context, columns));
    const auto* state =
            reinterpret_cast<const NativeGeoCrsState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state == nullptr || state->target != "EPSG:" + std::to_string(target_srid)) {
        return Status::InvalidArgument("ST_Transform requires prepared CRS metadata");
    }
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto input, geo_input<TYPE_GEOMETRY>(columns[0]));
    if (input.data->descriptor().type.crs != state->source) {
        return Status::InvalidArgument("Source GEOMETRY CRS differs from prepared descriptor");
    }
    ASSIGN_OR_RETURN(const int32_t source_srid, supported_transform_srid(state->source));
    ASSIGN_OR_RETURN(const int32_t prepared_target_srid, supported_transform_srid(state->target));
    if (prepared_target_srid != target_srid) {
        return Status::InvalidArgument("Target SRID differs from prepared GEOMETRY CRS");
    }
    const bool constant = ColumnHelper::is_all_const(columns);
    const size_t rows = constant ? 1 : size;
    ASSIGN_OR_RETURN(auto result, create_geometry_result(context));
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(input.wkb(row), &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        RETURN_IF_ERROR(transform_geometry_coordinates(&geometry, source_srid, target_srid));
        std::string wkb;
        RETURN_IF_ERROR(WkbCodec::to_wkb(geometry, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        result->append_datum(Datum(Slice(wkb)));
    }
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

Status GeoFunctions::native_geo_buffer_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || !context->is_constant_column(0)) return Status::OK();
    RETURN_IF_ERROR(buffer_checkpoint(context));
    auto prepared = std::make_unique<NativeGeoBufferState>();
    const auto& column = context->get_constant_column(0);
    if (!column->only_null() && column->size() != 0) {
        // Keep plan-time errors lazy until a non-NULL row uses this geometry.
        auto input = geo_input<TYPE_GEOMETRY>(column);
        prepared->status = input.status();
        if (prepared->status.ok()) {
            auto model = PreparedGeoBuffer::prepare(input->wkb(0), [&] { return buffer_checkpoint(context); });
            prepared->status = model.status();
            if (prepared->status.ok()) prepared->input = std::move(model.value());
        }
    }
    RETURN_IF_ERROR(buffer_checkpoint(context));
    context->set_function_state(scope, prepared.release());
    return Status::OK();
}

Status GeoFunctions::native_geo_buffer_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<NativeGeoBufferState*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_buffer(FunctionContext* context, const Columns& columns) {
    return geo_buffer(context, columns);
}

Status GeoFunctions::native_geo_overlay_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL ||
        (!context->is_constant_column(0) && !context->is_constant_column(1)))
        return Status::OK();
    RETURN_IF_ERROR(overlay_checkpoint(context));
    auto state = std::make_unique<NativeGeoOverlayState>();
    for (size_t argument = 0; argument < 2; ++argument) {
        auto& prepared = state->arguments[argument];
        prepared.constant = context->is_constant_column(argument);
        if (!prepared.constant) continue;
        const auto& column = context->get_constant_column(argument);
        if (column->only_null()) continue;
        // Defer constant input errors until a non-NULL row actually uses this operand.
        auto input = geo_input<TYPE_GEOMETRY>(column);
        prepared.status = input.status();
        if (!prepared.status.ok()) continue;
        auto polygon = PreparedGeoPolygon::prepare(input->wkb(0));
        prepared.status = polygon.status();
        if (prepared.status.ok()) prepared.polygon = std::move(polygon.value());
        RETURN_IF_ERROR(overlay_checkpoint(context));
    }
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::native_geo_overlay_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<NativeGeoOverlayState*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_intersection(FunctionContext* context, const Columns& columns) {
    return geo_overlay(context, columns, GeoOverlayKind::INTERSECTION);
}

StatusOr<ColumnPtr> GeoFunctions::st_geometry_union(FunctionContext* context, const Columns& columns) {
    return geo_overlay(context, columns, GeoOverlayKind::UNION);
}
StatusOr<ColumnPtr> GeoFunctions::st_geometry_difference(FunctionContext* context, const Columns& columns) {
    return geo_overlay(context, columns, GeoOverlayKind::DIFFERENCE);
}
StatusOr<ColumnPtr> GeoFunctions::st_geometry_sym_difference(FunctionContext* context, const Columns& columns) {
    return geo_overlay(context, columns, GeoOverlayKind::SYMMETRIC_DIFFERENCE);
}

} // namespace starrocks

#include "gen_cpp/opcode/GeoFunctions.inc"
