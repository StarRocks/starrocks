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

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/statusor.h"
#include "types/logical_type.h"

namespace starrocks {

class Chunk;
class Expr;
class ExprContext;
class RuntimeFilter;

class RuntimeFilterHelper {
public:
    static StatusOr<ExprContext*> rewrite_runtime_filter_in_cross_join_node(ObjectPool* pool, ExprContext* conjunct,
                                                                            Chunk* chunk);

    // create min/max predicate from filter.
    static void create_min_max_value_predicate(ObjectPool* pool, SlotId slot_id, LogicalType slot_type,
                                               const RuntimeFilter* filter, Expr** min_max_predicate);
};

} // namespace starrocks
