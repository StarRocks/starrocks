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

#include "exec/pipeline/nljoin/nljoin_runtime_filter.h"

#include <algorithm>
#include <cmath>

#include "column/chunk.h"
#include "column/column_viewer.h"
#include "exec/runtime_filter_compat/runtime_filter_port.h"
#include "exec_primitive/runtime_filter/runtime_filter_descriptor.h"
#include "exec_primitive/runtime_filter/runtime_filter_helper.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Opcodes_types.h"
#include "glog/logging.h"
#include "runtime/runtime_filter_factory.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks::pipeline {

namespace {

bool is_range_predicate(TExprOpcode::type op) {
    return op == TExprOpcode::GT || op == TExprOpcode::GE || op == TExprOpcode::LT || op == TExprOpcode::LE;
}

bool is_greater_predicate(TExprOpcode::type op) {
    return op == TExprOpcode::GT || op == TExprOpcode::GE;
}

bool is_boundary_supported_type(LogicalType type) {
    return type_dispatch_filter(type, false, []<LogicalType LT>() { return !lt_is_json<LT> && !lt_is_variant<LT>; });
}

bool is_range_filter_candidate(const std::vector<NLJoinRangeFilterCandidate>& candidates,
                               const RuntimeFilterBuildDescriptor* desc) {
    return std::any_of(candidates.begin(), candidates.end(),
                       [desc](const auto& candidate) { return candidate.desc == desc; });
}

} // namespace

ColumnPtr compute_min_max_boundary(LogicalType type, TExprOpcode::type op, const Columns& columns) {
    const bool is_greater = is_greater_predicate(op);
    return type_dispatch_filter(type, ColumnPtr(), [is_greater, &columns]<LogicalType LT>() -> ColumnPtr {
        using CppType = RunTimeCppType<LT>;
        bool has_boundary = false;
        CppType boundary{};
        for (const auto& column : columns) {
            ColumnViewer<LT> viewer(column);
            for (size_t row = 0; row < viewer.size(); row++) {
                if (viewer.is_null(row)) {
                    continue;
                }
                const auto value = viewer.value(row);
                if constexpr (lt_is_float<LT>) {
                    if (std::isnan(value)) {
                        continue;
                    }
                }
                if (!has_boundary || (is_greater ? value < boundary : boundary < value)) {
                    has_boundary = true;
                    boundary = value;
                }
            }
        }

        if (!has_boundary) {
            return nullptr;
        }
        auto result = RunTimeColumnType<LT>::create();
        result->append(boundary);
        return result;
    });
}

std::vector<NLJoinRangeFilterCandidate> make_range_filter_candidates(
        const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs, const std::vector<ExprContext*>& conjunct_ctxs) {
    std::vector<NLJoinRangeFilterCandidate> candidates;
    for (auto* desc : rf_descs) {
        DCHECK_LT(desc->build_expr_order(), conjunct_ctxs.size());
        auto* conjunct = conjunct_ctxs[desc->build_expr_order()];
        auto* root = conjunct->root();
        // Only range conjuncts have a min/max boundary to derive.
        if (!is_range_predicate(root->op())) {
            continue;
        }
        auto* build_expr = root->get_child(1);
        // JSON/VARIANT values have no ordering usable for min/max pruning.
        if (!is_boundary_supported_type(build_expr->type().type)) {
            continue;
        }

        NLJoinRangeFilterCandidate candidate;
        candidate.desc = desc;
        candidate.conjunct = conjunct;
        candidate.build_expr = build_expr;
        candidate.op = root->op();
        candidate.publish_global = desc->has_consumer() && desc->has_remote_targets();
        candidates.push_back(candidate);
    }
    return candidates;
}

Status compute_build_side_boundaries(std::vector<NLJoinRangeFilterCandidate>& candidates,
                                     const std::vector<ChunkPtr>& build_chunks, bool& is_build_chunk_invalid) {
    for (auto& candidate : candidates) {
        const LogicalType type = candidate.build_expr->type().type;
        for (const auto& chunk : build_chunks) {
            if (chunk == nullptr || chunk->is_empty()) {
                continue;
            }
            ASSIGN_OR_RETURN(auto column, candidate.conjunct->evaluate(candidate.build_expr, chunk.get()));
            Columns values;
            values.reserve(2);
            values.emplace_back(std::move(column));
            if (candidate.boundary != nullptr) {
                values.emplace_back(candidate.boundary);
            }
            candidate.boundary = compute_min_max_boundary(type, candidate.op, values);
        }
    }
    is_build_chunk_invalid = std::any_of(candidates.begin(), candidates.end(),
                                         [](const auto& candidate) { return candidate.boundary == nullptr; });
    return Status::OK();
}

StatusOr<std::list<ExprContext*>> build_local_range_filters(ObjectPool* pool,
                                                            const std::vector<NLJoinRangeFilterCandidate>& candidates) {
    std::list<ExprContext*> filters;
    for (const auto& candidate : candidates) {
        if (candidate.boundary == nullptr) {
            continue;
        }
        ASSIGN_OR_RETURN(auto expr, RuntimeFilterHelper::rewrite_runtime_filter_in_cross_join_node(
                                            pool, candidate.conjunct, candidate.boundary));
        filters.push_back(expr);
    }
    return filters;
}

StatusOr<std::list<ExprContext*>> build_local_non_range_filters(
        ObjectPool* pool, const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs,
        const std::vector<NLJoinRangeFilterCandidate>& candidates, Chunk* one_row_chunk,
        const std::vector<ExprContext*>& conjunct_ctxs) {
    std::list<ExprContext*> filters;
    for (auto* desc : rf_descs) {
        // Range conjuncts are already served by the boundary-based filters.
        if (is_range_filter_candidate(candidates, desc)) {
            continue;
        }
        DCHECK_LT(desc->build_expr_order(), conjunct_ctxs.size());
        auto* conjunct = conjunct_ctxs[desc->build_expr_order()];
        ASSIGN_OR_RETURN(auto value, conjunct->evaluate(conjunct->root()->get_child(1), one_row_chunk));
        ASSIGN_OR_RETURN(auto expr, RuntimeFilterHelper::rewrite_runtime_filter_in_cross_join_node(pool, conjunct,
                                                                                                   std::move(value)));
        filters.push_back(expr);
    }
    return filters;
}

Status publish_global_range_filters(RuntimeState* state, const std::vector<NLJoinRangeFilterCandidate>& candidates) {
    std::list<RuntimeFilterBuildDescriptor*> publish_descs;
    for (const auto& candidate : candidates) {
        if (!candidate.publish_global) {
            continue;
        }
        auto* filter = RuntimeFilterFactory::create_min_max_filter(
                state->obj_pool(), candidate.build_expr->type().type, is_greater_predicate(candidate.op),
                /*close_interval=*/true, candidate.boundary, candidate.desc->join_mode());
        DCHECK(filter != nullptr);
        candidate.desc->set_runtime_filter(filter);
        publish_descs.push_back(candidate.desc);
    }

    if (!publish_descs.empty()) {
        state->runtime_filter_port()->publish_runtime_filters(publish_descs);
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
