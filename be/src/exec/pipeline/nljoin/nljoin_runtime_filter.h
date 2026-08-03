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

#include <list>
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gen_cpp/Opcodes_types.h"
#include "types/logical_type.h"

namespace starrocks {
class Expr;
class ExprContext;
class ObjectPool;
class RuntimeFilterBuildDescriptor;
class RuntimeState;
} // namespace starrocks

namespace starrocks::pipeline {

// Derives NLJoin runtime filters: a range conjunct `probe OP build` is only
// satisfiable within the MIN (GT/GE) or MAX (LT/LE) of the build values.

// One range conjunct from which a min/max boundary can be derived.
struct NLJoinRangeFilterCandidate {
    RuntimeFilterBuildDescriptor* desc = nullptr;
    ExprContext* conjunct = nullptr;
    Expr* build_expr = nullptr;
    TExprOpcode::type op = TExprOpcode::INVALID_OPCODE;
    bool publish_global = false;
    ColumnPtr boundary = nullptr;
};

ColumnPtr compute_min_max_boundary(LogicalType type, TExprOpcode::type op, const Columns& columns);

std::vector<NLJoinRangeFilterCandidate> make_range_filter_candidates(
        const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs, const std::vector<ExprContext*>& conjunct_ctxs);

Status compute_build_side_boundaries(std::vector<NLJoinRangeFilterCandidate>& candidates,
                                     const std::vector<ChunkPtr>& build_chunks, bool& is_build_chunk_invalid);

StatusOr<std::list<ExprContext*>> build_local_range_filters(ObjectPool* pool,
                                                            const std::vector<NLJoinRangeFilterCandidate>& candidates);

StatusOr<std::list<ExprContext*>> build_local_non_range_filters(
        ObjectPool* pool, const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs,
        const std::vector<NLJoinRangeFilterCandidate>& candidates, Chunk* one_row_chunk,
        const std::vector<ExprContext*>& conjunct_ctxs);

Status publish_global_range_filters(RuntimeState* state, const std::vector<NLJoinRangeFilterCandidate>& candidates);

} // namespace starrocks::pipeline
