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

#include <charconv>
#include <cmath>
#include <optional>
#include <string_view>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "geo/geo_types.h"
#include "geo/wkb.h"

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

template <LogicalType Type>
StatusOr<ColumnPtr> geo_distance(const Columns& columns) {
    constexpr auto semantics = Type == TYPE_GEOGRAPHY ? WkbCoordinateSemantics::GEOGRAPHY_CRS84
                                                      : WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(auto lhs, geo_input<Type>(columns[0]));
    ASSIGN_OR_RETURN(auto rhs, geo_input<Type>(columns[1]));
    if constexpr (Type == TYPE_GEOMETRY) {
        if (!is_geo_compute_compatible(lhs.data->descriptor(), rhs.data->descriptor())) {
            return Status::InvalidArgument("ST_Distance requires compatible GEOMETRY descriptors");
        }
    }
    const bool constant = lhs.constant && rhs.constant;
    const size_t rows = constant ? 1 : size;
    ColumnBuilder<TYPE_DOUBLE> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            result.append_null();
            continue;
        }
        WkbGeometry left;
        WkbGeometry right;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(lhs.wkb(row), &left, semantics));
        RETURN_IF_ERROR(WkbCodec::parse_wkb(rhs.wkb(row), &right, semantics));
        if (left.empty || right.empty) {
            result.append_null();
            continue;
        }
        if (left.type != WkbGeometryType::POINT || right.type != WkbGeometryType::POINT) {
            if constexpr (Type == TYPE_GEOGRAPHY) {
                return Status::InvalidArgument("ST_Distance requires POINT/POINT GEOGRAPHY inputs");
            } else {
                return Status::InvalidArgument("ST_Distance requires POINT/POINT GEOMETRY inputs");
            }
        }
        if constexpr (Type == TYPE_GEOGRAPHY) {
            double distance;
            if (!GeoPoint::st_distance_sphere(left.coordinates[0].x, left.coordinates[0].y, right.coordinates[0].x,
                                              right.coordinates[0].y, &distance)) {
                return Status::InvalidArgument("ST_Distance received invalid GEOGRAPHY coordinates");
            }
            result.append(distance);
        } else {
            result.append(std::hypot(right.coordinates[0].x - left.coordinates[0].x,
                                     right.coordinates[0].y - left.coordinates[0].y));
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

StatusOr<ColumnPtr> GeoFunctions::st_geography_distance(FunctionContext*, const Columns& columns) {
    return geo_distance<TYPE_GEOGRAPHY>(columns);
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

StatusOr<ColumnPtr> GeoFunctions::st_geometry_distance(FunctionContext*, const Columns& columns) {
    return geo_distance<TYPE_GEOMETRY>(columns);
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

} // namespace starrocks

#include "gen_cpp/opcode/GeoFunctions.inc"
