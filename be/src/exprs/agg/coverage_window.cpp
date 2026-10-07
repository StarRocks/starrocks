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

#include "exprs/agg/coverage_window.h"

#include <cmath>
#include <exception>
#include <limits>

#include "base/utility/defer_op.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "common/config_expr_fwd.h"
#include "exprs/function_context.h"
#include "runtime/memory/memory_allocator.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
Status checkpoint(void* pointer) {
    auto* state = static_cast<RuntimeState*>(pointer);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("ST_CoverageSimplify"));
    return state->check_mem_limit("ST_CoverageSimplify");
}

const Column* unwrap(const Column* column, size_t& position, bool& null) {
    null = column->only_null();
    if (column->is_constant()) {
        position = 0;
        column = down_cast<const ConstColumn*>(column)->data_column().get();
    }
    if (column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(column);
        null |= nullable->is_null(position);
        column = nullable->data_column().get();
    }
    return column;
}

StatusOr<std::optional<double>> tolerance(FunctionContext* ctx) {
    if (!ctx->is_constant_column(1))
        return Status::InvalidArgument("ST_CoverageSimplify tolerance must be plan-constant");
    size_t row = 0;
    bool null;
    const auto* column = unwrap(ctx->get_constant_column(1).get(), row, null);
    if (null) return std::optional<double>{};
    const auto* values = dynamic_cast<const DoubleColumn*>(column);
    if (!values || values->size() == 0)
        return Status::InvalidArgument("ST_CoverageSimplify tolerance requires a DOUBLE constant");
    return std::optional<double>{values->get_data()[row]};
}

StatusOr<std::optional<bool>> boundary(FunctionContext* ctx) {
    if (ctx->get_arg_types().size() == 2) return std::optional<bool>{true};
    if (!ctx->is_constant_column(2))
        return Status::InvalidArgument("ST_CoverageSimplify boundary must be plan-constant");
    size_t row = 0;
    bool null;
    const auto* column = unwrap(ctx->get_constant_column(2).get(), row, null);
    if (null) return std::optional<bool>{};
    const auto* values = dynamic_cast<const BooleanColumn*>(column);
    if (!values || values->size() == 0)
        return Status::InvalidArgument("ST_CoverageSimplify boundary requires a BOOLEAN constant");
    return std::optional<bool>{bool(values->get_data()[row])};
}
} // namespace

Status CoverageWindowFunction::initialize(FunctionContext* ctx, CoverageWindowState& state) const {
    const auto& args = ctx->get_arg_types();
    const auto& result = ctx->get_return_type();
    if ((args.size() != 2 && args.size() != 3) || args[1].type != TYPE_DOUBLE ||
        (args.size() == 3 && args[2].type != TYPE_BOOLEAN))
        return Status::InvalidArgument("ST_CoverageSimplify requires GEOMETRY, DOUBLE and optional BOOLEAN");
    auto planar = [](const TypeDescriptor& type) {
        return type.type == TYPE_GEOMETRY && type.geo_type &&
               type.geo_type->logical_type == GEO_LOGICAL_TYPE_GEOMETRY &&
               type.geo_type->coordinate_system == GEO_COORDINATE_SYSTEM_CARTESIAN &&
               type.geo_type->edge_algorithm == GEO_EDGE_ALGORITHM_PLANAR && !type.geo_type->crs.empty();
    };
    if (!planar(args[0]) || !planar(result) || !is_geo_semantically_compatible(*args[0].geo_type, *result.geo_type))
        return Status::InvalidArgument("ST_CoverageSimplify requires compatible planar GEOMETRY CRS descriptors");
    if (!ctx->state()) return Status::InvalidArgument("ST_CoverageSimplify requires a runtime state");
    if (ctx->state()->enable_spill())
        return Status::NotSupported("ST_CoverageSimplify partition state cannot spill; disable enable_spill");
    ASSIGN_OR_RETURN(state.tolerance, tolerance(ctx));
    ASSIGN_OR_RETURN(state.boundary, boundary(ctx));
    if (state.tolerance && state.boundary &&
        (!std::isfinite(*state.tolerance) || *state.tolerance < 0 ||
         !std::isfinite(*state.tolerance * *state.tolerance)))
        return Status::InvalidArgument(
                "ST_CoverageSimplify requires finite nonnegative tolerance and representable tolerance squared");
    const int64_t rows = config::geo_coverage_max_rows_per_partition;
    const int64_t vertices = config::geo_coverage_max_vertices_per_partition;
    const int64_t input = config::geo_coverage_max_input_bytes_per_partition;
    const int64_t working = config::geo_coverage_max_working_bytes_per_partition;
    if (rows <= 0 || vertices <= 0 || input <= 0 || working <= 0)
        return Status::InvalidArgument("ST_CoverageSimplify requires positive geo_coverage limits");
    state.limits = {size_t(rows), size_t(vertices), size_t(input), size_t(working)};
    RETURN_IF_ERROR(checkpoint(ctx->state()));
    state.initialized = true;
    return Status::OK();
}

void CoverageWindowFunction::create(FunctionContext* ctx, AggDataPtr state) const {
    WindowFunction<CoverageWindowState>::create(ctx, state);
    auto status = initialize(ctx, data(state));
    if (!status.ok()) ctx->set_error(status.to_string().c_str());
}

Status CoverageWindowFunction::prepare_partition(FunctionContext* ctx, CoverageWindowState& state, const Column* input,
                                                 int64_t start, int64_t end) const {
    if (!input || start < 0 || end < start || (!input->is_constant() && uint64_t(end) > input->size()))
        return Status::InvalidArgument("ST_CoverageSimplify invalid input partition");
    ASSIGN_OR_RETURN(state.core, GeoCoverageSimplify::create(memory::get_default_allocator(), state.limits,
                                                             {ctx->state(), checkpoint}));
    const bool skip = !state.tolerance || !state.boundary;
    for (int64_t row = start; row < end; ++row) {
        if ((row - start) % 128 == 0) RETURN_IF_ERROR(checkpoint(ctx->state()));
        size_t position = row;
        bool null;
        const auto* physical = unwrap(input, position, null);
        const auto* geo = dynamic_cast<const GeoColumn*>(physical);
        if ((!geo && !input->only_null()) ||
            (geo && (!is_geo_semantically_compatible(geo->descriptor().type, *ctx->get_return_type().geo_type) ||
                     geo->descriptor().storage.encoding != GEO_ENCODING_WKB ||
                     (geo->descriptor().storage.dimension != GEO_DIMENSION_XY &&
                      geo->descriptor().storage.dimension != GEO_DIMENSION_UNKNOWN &&
                      geo->descriptor().storage.dimension != GEO_DIMENSION_MIXED))))
            return Status::InvalidArgument("ST_CoverageSimplify requires compatible native planar XY GEOMETRY input");
        std::optional<Slice> bytes;
        if (!skip && !null) bytes = geo->get_wkb(position);
        RETURN_IF_ERROR(state.core->append(bytes));
    }
    RETURN_IF_ERROR(state.core->finish(state.tolerance, state.boundary));
    state.calls += state.core->kernel_calls();
    return Status::OK();
}

void CoverageWindowFunction::update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr state,
                                                                  const Column** columns, int64_t partition_start,
                                                                  int64_t partition_end, int64_t frame_start,
                                                                  int64_t frame_end) const {
    if (ctx->has_error()) return;
    auto& current = data(state);
    if (!current.initialized || current.core || !columns || frame_start != partition_start ||
        frame_end != partition_end) {
        ctx->set_error("ST_CoverageSimplify requires one complete partition");
        return;
    }
    auto status = prepare_partition(ctx, current, columns[0], frame_start, frame_end);
    if (!status.ok()) {
        current.core.reset();
        ctx->set_error(status.to_string().c_str());
    }
}

void CoverageWindowFunction::get_values(FunctionContext* ctx, ConstAggDataPtr state, Column* dst, size_t start,
                                        size_t end) const {
    if (end < start) {
        ctx->set_error("ST_CoverageSimplify invalid output range");
        return;
    }
    // Variable-width window results append end-start rows. Preserve that
    // invariant on errors too, before the caller can append dst to its chunk.
    const size_t expected_size = dst->size() + (end - start);
    auto pad = DeferOp([&] {
        // Allocation exceptions are handled by the caller's allocation scope.
        // Never inspect a partly appended nullable column or allocate on unwind.
        if (std::uncaught_exceptions() != 0) return;
        if (dst->size() < expected_size) dst->append_default(expected_size - dst->size());
    });
    if (ctx->has_error()) return;
    auto& current = data(const_cast<AggDataPtr>(state));
    auto* nullable = dynamic_cast<NullableColumn*>(dst);
    auto* geo = dynamic_cast<GeoColumn*>(nullable ? nullable->data_column()->as_mutable_raw_ptr() : dst);
    if (!current.core || !nullable || !geo || dst->size() != start || current.emitted > current.core->rows() ||
        end - start > current.core->rows() - current.emitted ||
        !is_geo_semantically_compatible(geo->descriptor().type, *ctx->get_return_type().geo_type)) {
        ctx->set_error("ST_CoverageSimplify positional window mapping mismatch");
        return;
    }
    // start/end are chunk-local destination positions. emitted is the ordinal
    // in this partition, independent of chunk boundaries and buffer contraction.
    // Reserve owned WKB storage once for this output range, without retaining
    // the kernel in the column or repeatedly growing the payload buffer.
    size_t bytes_to_reserve = geo->byte_size();
    const auto maximum = std::numeric_limits<uint32_t>::max();
    for (size_t index = 0; index < end - start; ++index) {
        if (index % 128 == 0) {
            auto status = checkpoint(ctx->state());
            if (!status.ok()) {
                ctx->set_error(status.to_string().c_str());
                return;
            }
        }
        auto result = current.core->result(current.emitted + index);
        if (!result.ok()) {
            ctx->set_error(result.status().to_string().c_str());
            return;
        }
        const size_t bytes = *result ? result->value().size : 0;
        if (bytes_to_reserve > maximum || bytes > maximum - bytes_to_reserve) {
            ctx->set_error("ST_CoverageSimplify output chunk exceeds native offset range");
            return;
        }
        bytes_to_reserve += bytes;
    }
    geo->reserve(end, bytes_to_reserve);
    for (size_t row = start; row < end; ++row) {
        if ((row - start) % 128 == 0) {
            auto status = checkpoint(ctx->state());
            if (!status.ok()) {
                ctx->set_error(status.to_string().c_str());
                return;
            }
        }
        auto result = current.core->result(current.emitted);
        if (!result.ok()) {
            ctx->set_error(result.status().to_string().c_str());
            return;
        }
        if (*result) {
            geo->append_wkb(result->value());
            nullable->null_column_data().push_back(0);
        } else {
            nullable->append_nulls(1);
        }
        ++current.emitted;
    }
    nullable->update_has_null();
}

void CoverageWindowFunction::reset(FunctionContext*, const Columns&, AggDataPtr state) const {
    auto& current = data(state);
    current.core.reset();
    current.emitted = 0;
}

size_t CoverageWindowFunction::kernel_calls(ConstAggDataPtr state) const {
    return data(state).calls;
}
} // namespace starrocks
