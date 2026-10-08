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

#ifdef WITH_H3
#include <h3/h3api.h>
#endif

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "common/config_expr_fwd.h"
#include "geo/geo_measurements.h"
#include "geo/wkb.h"
#include "runtime/runtime_state.h"
#include "types/geo_wkb.h"

namespace starrocks {
#ifndef WITH_H3
namespace {
Status h3_unavailable() {
    return Status::NotSupported("H3 capability is unavailable in this BE build");
}
} // namespace
Status GeoFunctions::h3_prepare(FunctionContext*, FunctionContext::FunctionStateScope) {
    return Status::OK();
}
Status GeoFunctions::h3_close(FunctionContext*, FunctionContext::FunctionStateScope) {
    return Status::OK();
}
StatusOr<ColumnPtr> GeoFunctions::h3_from_geo(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
StatusOr<ColumnPtr> GeoFunctions::h3_grid_disk(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
StatusOr<ColumnPtr> GeoFunctions::h3_to_parent(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
StatusOr<ColumnPtr> GeoFunctions::h3_to_children(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
StatusOr<ColumnPtr> GeoFunctions::h3_resolution(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
StatusOr<ColumnPtr> GeoFunctions::h3_to_boundary(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
StatusOr<ColumnPtr> GeoFunctions::h3_polygon_to_cells(FunctionContext*, const Columns&) {
    return h3_unavailable();
}
#else
namespace {

constexpr double kRadiansPerDegree = 0.017453292519943295769236907684886;
constexpr double kDegreesPerRadian = 1.0 / kRadiansPerDegree;

struct H3Limits {
    int64_t cells;
    int32_t grid_disk_k;
    int64_t vertices;
    int32_t components;
    int64_t working_bytes;
    int64_t estimated_work;
};

StatusOr<H3Limits> h3_limits() {
    H3Limits limits{config::h3_max_cells_per_row,    config::h3_max_grid_disk_k,
                    config::h3_max_polygon_vertices, config::h3_max_polygon_components,
                    config::h3_max_working_bytes,    config::h3_max_estimated_work_per_row};
    if (limits.cells <= 0 || limits.grid_disk_k <= 0 || limits.vertices <= 0 || limits.components <= 0 ||
        limits.working_bytes <= 0 || limits.estimated_work <= 0) {
        return Status::InvalidArgument("All H3 resource limits must be positive");
    }
    return limits;
}

Status h3_checkpoint(FunctionContext* context) {
    if (context != nullptr && context->state() != nullptr) {
        RETURN_IF_CANCELLED(context->state());
        RETURN_IF_ERROR(context->state()->check_query_state("H3 function"));
        RETURN_IF_ERROR(context->state()->check_mem_limit("H3 function"));
    }
    return Status::OK();
}

Status h3_error(const char* name, H3Error code) {
    return Status::InvalidArgument(std::string(name) + ": " + describeH3Error(code));
}

StatusOr<H3Index> h3_cell(int64_t value) {
    if (value <= 0 || !isValidCell(static_cast<H3Index>(value))) {
        return Status::InvalidArgument("H3 requires a valid positive cell index");
    }
    return static_cast<H3Index>(value);
}

Status validate_h3_resolution(int value) {
    if (value < 0 || value > 15) {
        return Status::InvalidArgument("H3 resolution must be between 0 and 15");
    }
    return Status::OK();
}

Status h3_cells_limit(const H3Limits& limits, int64_t count, FunctionContext* context) {
    if (count < 0 || count > limits.cells) {
        return Status::InvalidArgument("h3_max_cells_per_row exceeded");
    }
    if (count > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument("H3 array offset limit exceeded");
    }
    if (context != nullptr && context->get_max_array_length() > 0 && count > context->get_max_array_length()) {
        return Status::InvalidArgument("max_array_length exceeded by H3 result");
    }
    return Status::OK();
}

Status h3_working_limit(const H3Limits& limits, uint64_t bytes) {
    if (bytes > static_cast<uint64_t>(limits.working_bytes)) {
        return Status::InvalidArgument("h3_max_working_bytes exceeded");
    }
    return Status::OK();
}

struct H3GeoInput {
    const GeoColumn* data;
    const NullableColumn* nullable;
    bool constant;
    size_t index(size_t row) const { return constant ? 0 : row; }
    bool is_null(size_t row) const { return nullable != nullptr && nullable->is_null(index(row)); }
    Slice wkb(size_t row) const { return data->get_wkb(index(row)); }
};

StatusOr<H3GeoInput> h3_geo_input(const ColumnPtr& column) {
    const bool constant = column->is_constant();
    const Column* source = constant ? down_cast<const ConstColumn*>(column.get())->data_column().get() : column.get();
    const auto* nullable = source->is_nullable() ? down_cast<const NullableColumn*>(source) : nullptr;
    const auto* geo = down_cast<const GeoColumn*>(nullable ? nullable->data_column().get() : source);
    const auto& descriptor = geo->descriptor();
    const auto& type = descriptor.type;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY || type.coordinate_system != GEO_COORDINATE_SYSTEM_SPHERICAL ||
        type.edge_algorithm != GEO_EDGE_ALGORITHM_SPHERICAL || type.crs != "OGC:CRS84" ||
        (type.srid.has_value() && type.srid.value() != 4326) || descriptor.storage.encoding != GEO_ENCODING_WKB ||
        descriptor.storage.dimension != GEO_DIMENSION_XY) {
        return Status::NotSupported("H3 requires XY CRS84 GEOGRAPHY with spherical edges");
    }
    return H3GeoInput{geo, nullable, constant};
}

StatusOr<WkbGeometry> h3_parse_geo(const H3GeoInput& input, size_t row, bool polygon, const H3Limits& limits) {
    const Slice wkb = input.wkb(row);
    ASSIGN_OR_RETURN(const auto info, inspect_geo_wkb(wkb));
    if (polygon && info.coordinates > limits.vertices) {
        return Status::InvalidArgument("h3_max_polygon_vertices exceeded");
    }
    if (polygon) {
        // Admit decode and spherical validation before either can allocate.
        const uint64_t preparation_bytes = static_cast<uint64_t>(wkb.size) +
                                           static_cast<uint64_t>(info.coordinates) * 128 +
                                           static_cast<uint64_t>(info.components) * 256;
        RETURN_IF_ERROR(h3_working_limit(limits, preparation_bytes));
    }
    if (info.dimension != GEO_DIMENSION_XY ||
        (polygon ? (info.geometry_type != 3 && info.geometry_type != 6) : info.geometry_type != 1)) {
        return Status::InvalidArgument(polygon ? "H3_PolygonToCells requires XY POLYGON or MULTIPOLYGON"
                                               : "H3_FromGeo requires XY POINT");
    }
    WkbGeometry geometry;
    RETURN_IF_ERROR(WkbCodec::parse_wkb(wkb, &geometry, WkbCoordinateSemantics::GEOGRAPHY_CRS84));
    return geometry;
}

struct H3PreparedGeo {
    bool constant = false;
    Status parse_status = Status::OK();
    std::optional<WkbGeometry> geometry;
};

StatusOr<const WkbGeometry*> h3_geometry(const H3GeoInput& input, size_t row, bool polygon, const H3Limits& limits,
                                         const H3PreparedGeo* prepared, std::optional<WkbGeometry>* local,
                                         WkbGeometry* varying) {
    if (prepared != nullptr && prepared->constant) {
        RETURN_IF_ERROR(prepared->parse_status);
        return &prepared->geometry.value();
    }
    if (input.constant) {
        if (!local->has_value()) {
            ASSIGN_OR_RETURN(auto parsed, h3_parse_geo(input, row, polygon, limits));
            local->emplace(std::move(parsed));
        }
        return &local->value();
    }
    ASSIGN_OR_RETURN(*varying, h3_parse_geo(input, row, polygon, limits));
    return varying;
}

struct H3ArrayBuilder {
    decltype(Int64Column::create()) values = Int64Column::create();
    decltype(UInt32Column::create()) offsets = UInt32Column::create();
    decltype(NullColumn::create()) nulls = NullColumn::create();

    H3ArrayBuilder() { offsets->append(0); }

    void append_null() {
        nulls->append(1);
        offsets->append(static_cast<uint32_t>(values->size()));
    }

    Status append(const std::vector<H3Index>& cells, FunctionContext* context = nullptr) {
        if (cells.size() > std::numeric_limits<uint32_t>::max() - values->size()) {
            return Status::InvalidArgument("H3 array offset limit exceeded");
        }
        for (size_t i = 0; i < cells.size(); ++i) {
            if ((i & 1023) == 0) RETURN_IF_ERROR(h3_checkpoint(context));
            values->append(static_cast<int64_t>(cells[i]));
        }
        nulls->append(0);
        offsets->append(static_cast<uint32_t>(values->size()));
        return Status::OK();
    }

    ColumnPtr build(size_t size, bool constant) {
        auto elements = NullableColumn::create(values, NullColumn::create(values->size(), 0));
        auto array = ArrayColumn::create(std::move(elements), offsets);
        ColumnPtr result = NullableColumn::create(std::move(array), nulls);
        if (constant) return ConstColumn::create(std::move(result), size);
        return result;
    }
};

// H3's pinned 4.5.0 C library calls these symbols for temporary allocations.
// The calling worker supplies a row-local budget; malloc remains under the
// normal StarRocks/jemalloc query tracker as well.
struct H3Budget {
    size_t limit = 0;
    size_t live = 0;
    bool exceeded = false;
};
thread_local H3Budget* active_h3_budget = nullptr;

struct H3BudgetScope {
    H3Budget* previous;
    explicit H3BudgetScope(H3Budget* budget) : previous(active_h3_budget) { active_h3_budget = budget; }
    ~H3BudgetScope() { active_h3_budget = previous; }
};

struct alignas(std::max_align_t) H3AllocHeader {
    size_t size;
    H3Budget* budget;
};

} // namespace

extern "C" void* starrocks_h3_malloc(size_t size) {
    if (size > std::numeric_limits<size_t>::max() - sizeof(H3AllocHeader)) return nullptr;
    H3Budget* budget = active_h3_budget;
    if (budget != nullptr && size > budget->limit - budget->live) {
        budget->exceeded = true;
        return nullptr;
    }
    auto* header = static_cast<H3AllocHeader*>(std::malloc(sizeof(H3AllocHeader) + size));
    if (header == nullptr) return nullptr;
    header->size = size;
    header->budget = budget;
    if (budget != nullptr) budget->live += size;
    return header + 1;
}

extern "C" void* starrocks_h3_calloc(size_t count, size_t size) {
    if (size != 0 && count > std::numeric_limits<size_t>::max() / size) return nullptr;
    const size_t bytes = count * size;
    void* ptr = starrocks_h3_malloc(bytes);
    if (ptr != nullptr) std::memset(ptr, 0, bytes);
    return ptr;
}

extern "C" void starrocks_h3_free(void* ptr) {
    if (ptr == nullptr) return;
    auto* header = static_cast<H3AllocHeader*>(ptr) - 1;
    if (header->budget != nullptr) header->budget->live -= header->size;
    std::free(header);
}

extern "C" void* starrocks_h3_realloc(void* ptr, size_t size) {
    if (ptr == nullptr) return starrocks_h3_malloc(size);
    if (size == 0) {
        starrocks_h3_free(ptr);
        return nullptr;
    }
    const auto* old = static_cast<H3AllocHeader*>(ptr) - 1;
    void* replacement = starrocks_h3_malloc(size);
    if (replacement == nullptr) return nullptr;
    std::memcpy(replacement, ptr, std::min(old->size, size));
    starrocks_h3_free(ptr);
    return replacement;
}

Status GeoFunctions::h3_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context == nullptr || context->get_num_args() == 0 ||
        !context->is_constant_column(0)) {
        return Status::OK();
    }
    const auto* type = context->get_arg_type(0);
    if (type == nullptr || type->type != TYPE_GEOGRAPHY) return Status::OK();
    auto state = std::make_unique<H3PreparedGeo>();
    state->constant = true;
    const auto& column = context->get_constant_column(0);
    if (column != nullptr && !column->only_null()) {
        auto input = h3_geo_input(column);
        if (!input.ok()) {
            state->parse_status = input.status();
        } else {
            auto limits = h3_limits();
            if (!limits.ok()) {
                state->parse_status = limits.status();
            } else {
                const bool polygon = context->get_return_type().type == TYPE_ARRAY;
                auto parsed = h3_parse_geo(input.value(), 0, polygon, limits.value());
                if (parsed.ok()) {
                    state->geometry.emplace(std::move(parsed.value()));
                } else {
                    state->parse_status = parsed.status();
                }
            }
        }
    }
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::h3_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<H3PreparedGeo*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::h3_from_geo(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(const auto limits, h3_limits());
    ASSIGN_OR_RETURN(const auto input, h3_geo_input(columns[0]));
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    ColumnBuilder<TYPE_BIGINT> result(constant ? 1 : size);
    const auto* prepared = context == nullptr ? nullptr
                                              : reinterpret_cast<const H3PreparedGeo*>(
                                                        context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::optional<WkbGeometry> local;
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        const int resolution = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(resolution));
        WkbGeometry varying;
        ASSIGN_OR_RETURN(const auto* point, h3_geometry(input, row, false, limits, prepared, &local, &varying));
        if (point->empty) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const bool valid, spherical_is_valid(*point));
        if (!valid) return Status::InvalidArgument("H3_FromGeo requires a valid GEOGRAPHY POINT");
        const LatLng latlng{point->coordinates[0].y * kRadiansPerDegree, point->coordinates[0].x * kRadiansPerDegree};
        H3Index cell = 0;
        const H3Error error = latLngToCell(&latlng, resolution, &cell);
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (error != E_SUCCESS) return h3_error("H3_FromGeo", error);
        if (!isValidCell(cell) || cell > static_cast<H3Index>(std::numeric_limits<int64_t>::max())) {
            return Status::InternalError("H3_FromGeo returned invalid cell");
        }
        result.append(static_cast<int64_t>(cell));
    }
    ColumnPtr output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::h3_resolution(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    const bool constant = columns[0]->is_constant();
    ColumnBuilder<TYPE_INT> result(constant ? 1 : size);
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        result.append(getResolution(cell));
    }
    ColumnPtr output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::h3_to_parent(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    ColumnBuilder<TYPE_BIGINT> result(constant ? 1 : size);
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        const int resolution = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(resolution));
        if (resolution > getResolution(cell))
            return Status::InvalidArgument("H3_ToParent resolution exceeds cell resolution");
        H3Index parent = 0;
        const H3Error error = cellToParent(cell, resolution, &parent);
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (error != E_SUCCESS) return h3_error("H3_ToParent", error);
        result.append(static_cast<int64_t>(parent));
    }
    ColumnPtr output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::h3_grid_disk(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(const auto limits, h3_limits());
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    ColumnViewer<TYPE_INT> ks(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    H3ArrayBuilder result;
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row) || ks.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        const int k = ks.value(row);
        if (k < 0 || k > limits.grid_disk_k) return Status::InvalidArgument("h3_max_grid_disk_k exceeded");
        int64_t count = 0;
        const H3Error size_error = maxGridDiskSize(k, &count);
        if (size_error != E_SUCCESS) return h3_error("H3_GridDisk", size_error);
        RETURN_IF_ERROR(h3_cells_limit(limits, count, context));
        RETURN_IF_ERROR(h3_working_limit(limits, static_cast<uint64_t>(count) * sizeof(H3Index) * 3));
        std::vector<H3Index> cells(static_cast<size_t>(count), 0);
        H3Budget budget{static_cast<size_t>(limits.working_bytes) - cells.size() * sizeof(H3Index)};
        H3Error error;
        {
            H3BudgetScope scope(&budget);
            error = gridDisk(cell, k, cells.data());
        }
        if (budget.exceeded) return Status::InvalidArgument("h3_max_working_bytes exceeded");
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (error != E_SUCCESS) return h3_error("H3_GridDisk", error);
        cells.erase(std::remove(cells.begin(), cells.end(), H3_NULL), cells.end());
        for (H3Index value : cells) {
            if (!isValidCell(value)) return Status::InternalError("H3_GridDisk returned invalid cell");
        }
        RETURN_IF_ERROR(result.append(cells, context));
    }
    return result.build(size, constant);
}

StatusOr<ColumnPtr> GeoFunctions::h3_to_children(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(const auto limits, h3_limits());
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    H3ArrayBuilder result;
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        const int resolution = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(resolution));
        if (resolution < getResolution(cell))
            return Status::InvalidArgument("H3_ToChildren resolution is below cell resolution");
        int64_t count = 0;
        const H3Error size_error = cellToChildrenSize(cell, resolution, &count);
        if (size_error != E_SUCCESS) return h3_error("H3_ToChildren", size_error);
        RETURN_IF_ERROR(h3_cells_limit(limits, count, context));
        RETURN_IF_ERROR(h3_working_limit(limits, static_cast<uint64_t>(count) * sizeof(H3Index) * 2));
        std::vector<H3Index> cells(static_cast<size_t>(count), 0);
        const H3Error error = cellToChildren(cell, resolution, cells.data());
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (error != E_SUCCESS) return h3_error("H3_ToChildren", error);
        for (H3Index value : cells) {
            if (!isValidCell(value)) return Status::InternalError("H3_ToChildren returned invalid cell");
        }
        RETURN_IF_ERROR(result.append(cells, context));
    }
    return result.build(size, constant);
}

StatusOr<ColumnPtr> GeoFunctions::h3_to_boundary(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    if (context == nullptr || context->get_return_type().type != TYPE_GEOGRAPHY ||
        !context->get_return_type().geo_type.has_value()) {
        return Status::NotSupported("H3_ToBoundary requires GEOGRAPHY return descriptor");
    }
    GeoColumnDescriptor descriptor{context->get_return_type().geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    const auto& type = descriptor.type;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY || type.coordinate_system != GEO_COORDINATE_SYSTEM_SPHERICAL ||
        type.edge_algorithm != GEO_EDGE_ALGORITHM_SPHERICAL || type.crs != "OGC:CRS84") {
        return Status::NotSupported("H3_ToBoundary requires CRS84 spherical GEOGRAPHY result");
    }
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    const bool constant = columns[0]->is_constant();
    auto result = NullableColumn::create(GeoColumn::create(std::move(descriptor)), NullColumn::create());
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row)) {
            result->append_nulls(1);
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        CellBoundary boundary{};
        const H3Error error = cellToBoundary(cell, &boundary);
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (error != E_SUCCESS) return h3_error("H3_ToBoundary", error);
        if (boundary.numVerts < 3 || boundary.numVerts > MAX_CELL_BNDRY_VERTS) {
            return Status::InternalError("H3_ToBoundary returned invalid vertex count");
        }
        WkbGeometry polygon;
        polygon.type = WkbGeometryType::POLYGON;
        auto& ring = polygon.rings.emplace_back();
        ring.reserve(static_cast<size_t>(boundary.numVerts) + 1);
        for (int i = 0; i < boundary.numVerts; ++i) {
            ring.push_back({boundary.verts[i].lng * kDegreesPerRadian, boundary.verts[i].lat * kDegreesPerRadian});
        }
        ring.push_back(ring.front());
        std::string wkb;
        RETURN_IF_ERROR(WkbCodec::to_wkb(polygon, &wkb, WkbCoordinateSemantics::GEOGRAPHY_CRS84));
        result->append_datum(Datum(Slice(wkb)));
    }
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

namespace {
struct H3PolygonComponent {
    std::vector<std::vector<LatLng> > vertices;
    std::vector< ::GeoLoop> holes;
    ::GeoPolygon polygon{};
    int64_t slots = 0;
    size_t vertex_count = 0;
};

StatusOr<std::unique_ptr<H3PolygonComponent> > h3_component(const WkbGeometry& geometry) {
    auto out = std::make_unique<H3PolygonComponent>();
    out->vertices.reserve(geometry.rings.size());
    for (const auto& ring : geometry.rings) {
        if (ring.size() < 4 || ring.front() != ring.back()) {
            return Status::InvalidArgument("H3_PolygonToCells requires closed rings");
        }
        auto& vertices = out->vertices.emplace_back();
        vertices.reserve(ring.size() - 1);
        for (size_t i = 0; i + 1 < ring.size(); ++i) {
            vertices.push_back({ring[i].y * kRadiansPerDegree, ring[i].x * kRadiansPerDegree});
        }
        out->vertex_count += ring.size();
    }
    if (out->vertices.empty()) return Status::InvalidArgument("H3_PolygonToCells requires an exterior ring");
    out->polygon.geoloop = {static_cast<int>(out->vertices[0].size()), out->vertices[0].data()};
    out->holes.reserve(out->vertices.size() - 1);
    for (size_t i = 1; i < out->vertices.size(); ++i) {
        out->holes.push_back({static_cast<int>(out->vertices[i].size()), out->vertices[i].data()});
    }
    out->polygon.numHoles = static_cast<int>(out->holes.size());
    out->polygon.holes = out->holes.data();
    return out;
}
} // namespace

StatusOr<ColumnPtr> GeoFunctions::h3_polygon_to_cells(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(const auto limits, h3_limits());
    ASSIGN_OR_RETURN(const auto input, h3_geo_input(columns[0]));
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    H3ArrayBuilder result;
    const auto* prepared = context == nullptr ? nullptr
                                              : reinterpret_cast<const H3PreparedGeo*>(
                                                        context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::optional<WkbGeometry> local;
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        RETURN_IF_ERROR(h3_checkpoint(context));
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        const int res = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(res));
        const Slice wkb = input.wkb(row);
        RETURN_IF_ERROR(h3_working_limit(limits, wkb.size));
        WkbGeometry varying;
        ASSIGN_OR_RETURN(const auto* geometry, h3_geometry(input, row, true, limits, prepared, &local, &varying));
        const size_t count = geometry->type == WkbGeometryType::POLYGON ? 1 : geometry->children.size();
        if (count > static_cast<size_t>(limits.components)) {
            return Status::InvalidArgument("h3_max_polygon_components exceeded");
        }
        if (geometry->empty) {
            RETURN_IF_ERROR(result.append({}));
            continue;
        }
        ASSIGN_OR_RETURN(const bool valid, spherical_is_valid(*geometry));
        if (!valid) return Status::InvalidArgument("H3_PolygonToCells requires valid spherical GEOGRAPHY topology");
        std::vector<std::unique_ptr<H3PolygonComponent> > components;
        components.reserve(count);
        uint64_t slots = 0;
        uint64_t work = 0;
        uint64_t vertices = 0;
        for (size_t i = 0; i < count; ++i) {
            RETURN_IF_ERROR(h3_checkpoint(context));
            const auto& poly = geometry->type == WkbGeometryType::POLYGON ? *geometry : geometry->children[i];
            if (poly.empty) continue;
            ASSIGN_OR_RETURN(auto component, h3_component(poly));
            vertices += component->vertex_count;
            if (vertices > static_cast<uint64_t>(limits.vertices)) {
                return Status::InvalidArgument("h3_max_polygon_vertices exceeded");
            }
            const uint64_t preflight_bytes = wkb.size + vertices * 64 + count * 256;
            RETURN_IF_ERROR(h3_working_limit(limits, preflight_bytes));
            H3Budget budget{static_cast<size_t>(limits.working_bytes - preflight_bytes)};
            H3Error error;
            {
                H3BudgetScope scope(&budget);
                error = maxPolygonToCellsSize(&component->polygon, res, 0, &component->slots);
            }
            if (budget.exceeded) return Status::InvalidArgument("h3_max_working_bytes exceeded");
            if (error != E_SUCCESS) return h3_error("H3_PolygonToCells size", error);
            if (component->slots < 0 || component->slots > limits.cells - static_cast<int64_t>(slots)) {
                return Status::InvalidArgument("h3_max_cells_per_row exceeded");
            }
            slots += static_cast<uint64_t>(component->slots);
            RETURN_IF_ERROR(h3_cells_limit(limits, static_cast<int64_t>(slots), context));
            const uint64_t factor = component->vertex_count + 1;
            if (static_cast<uint64_t>(component->slots) >
                (static_cast<uint64_t>(limits.estimated_work) - work) / factor) {
                return Status::InvalidArgument("h3_max_estimated_work_per_row exceeded");
            }
            work += static_cast<uint64_t>(component->slots) * factor;
            components.push_back(std::move(component));
        }
        const uint64_t external_bytes = wkb.size + vertices * 64 + count * 256 + slots * 128;
        RETURN_IF_ERROR(h3_working_limit(limits, external_bytes));
        std::vector<H3Index> cells;
        cells.reserve(static_cast<size_t>(slots));
        for (const auto& component : components) {
            RETURN_IF_ERROR(h3_checkpoint(context));
            std::vector<H3Index> scratch(static_cast<size_t>(component->slots), 0);
            H3Budget budget{static_cast<size_t>(limits.working_bytes - external_bytes)};
            H3Error error;
            {
                H3BudgetScope scope(&budget);
                error = polygonToCells(&component->polygon, res, 0, scratch.data());
            }
            if (budget.exceeded) return Status::InvalidArgument("h3_max_working_bytes exceeded");
            if (error != E_SUCCESS) return h3_error("H3_PolygonToCells", error);
            for (H3Index cell : scratch) {
                if (cell == H3_NULL) continue;
                if (!isValidCell(cell)) return Status::InternalError("H3_PolygonToCells returned invalid cell");
                cells.push_back(cell);
            }
        }
        std::unordered_set<H3Index> seen;
        seen.reserve(cells.size());
        size_t write = 0;
        for (size_t i = 0; i < cells.size(); ++i) {
            if ((i & 1023) == 0) RETURN_IF_ERROR(h3_checkpoint(context));
            if (seen.insert(cells[i]).second) cells[write++] = cells[i];
        }
        cells.resize(write);
        RETURN_IF_ERROR(result.append(cells, context));
    }
    return result.build(size, constant);
}

#endif // WITH_H3
} // namespace starrocks
