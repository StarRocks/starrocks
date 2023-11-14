// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/geo_functions.h"

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/logging.h"
#include "geo/geo_types.h"

namespace starrocks::vectorized {

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

struct StContainsState {
    StContainsState() : shapes{nullptr, nullptr} {}
    ~StContainsState() {
        delete shapes[0];
        delete shapes[1];
    }
    bool is_null{false};
    GeoShape* shapes[2];
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

} // namespace starrocks::vectorized
