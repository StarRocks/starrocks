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

#include <cstddef>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

class ExprContext;

class ChunkPredicateEvaluator {
public:
    static Status eval_conjuncts(const std::vector<ExprContext*>& ctxs, Chunk* chunk, FilterPtr* filter_ptr = nullptr,
                                 bool apply_filter = true);
    static StatusOr<size_t> eval_conjuncts_into_filter(const std::vector<ExprContext*>& ctxs, Chunk* chunk,
                                                       Filter* filter);
    static void eval_filter_null_values(Chunk* chunk, const std::vector<SlotId>& filter_null_value_columns);
};

} // namespace starrocks
