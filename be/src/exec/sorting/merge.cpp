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

#include "exec/sorting/merge.h"

#include "exec/sorting/sort_permute.h"

namespace starrocks {
StatusOr<MergedRun> MergedRun::build(ChunkUniquePtr&& chunk, const std::vector<ExprContext*>& exprs) {
    MergedRun run;
    DCHECK(chunk);
    if (!chunk->is_empty()) {
        for (auto& expr : exprs) {
            ASSIGN_OR_RETURN(auto column, expr->evaluate(chunk.get()));
            run.orderby.push_back(column);
        }
    }
    run.range = {0, chunk->num_rows()};
    run.chunk = std::move(chunk);
    return run;
}

ChunkPtr MergedRun::steal_chunk(size_t size) {
    if (empty()) {
        return {};
    }

    size_t reserved_rows = num_rows();

    if (size >= reserved_rows) {
        ChunkPtr res_chunk;
        Columns res_orderby;
        if (range.first == 0 && range.second == chunk->num_rows()) {
            res_chunk = std::move(chunk);

        } else {
            res_chunk = chunk->clone_empty(reserved_rows);
            res_chunk->append(*chunk, range.first, reserved_rows);
        }
        range.first = range.second = 0;
        chunk.reset();
        orderby.clear();
        return res_chunk;
    } else {
        size_t required_rows = std::min(size, reserved_rows);
        ChunkPtr res_chunk = chunk->clone_empty(required_rows);
        Columns res_orderby;
        res_chunk->append(*chunk, range.first, required_rows);
        range.first += required_rows;
        return res_chunk;
    }
}

size_t MergedRuns::num_rows() const {
    if (_num_rows.has_value()) return _num_rows.value();
    size_t res = 0;
    for (const auto& chunk : _runs) {
        res += chunk.num_rows();
    }
    _num_rows = res;
    return res;
}

int64_t MergedRuns::mem_usage() const {
    size_t res = 0;
    for (auto& chunk : _runs) {
        res += chunk.chunk->memory_usage();
    }
    return res;
}

} // namespace starrocks