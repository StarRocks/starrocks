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

#include <map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"

namespace starrocks {

class ExprContext;
class ObjectPool;
class RuntimeState;

// Helper class for orchestrating ExprContext lifecycle.
class ExprExecutor {
public:
    static Status prepare(ExprContext* ctx, RuntimeState* state);
    static Status open(ExprContext* ctx, RuntimeState* state);
    static void close(ExprContext* ctx, RuntimeState* state);

    static Status prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state);
    static Status open(const std::vector<ExprContext*>& ctxs, RuntimeState* state);
    static void close(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

    static Status prepare(const std::map<SlotId, ExprContext*>& ctxs, RuntimeState* state);
    static Status open(const std::map<SlotId, ExprContext*>& ctxs, RuntimeState* state);
    static void close(const std::map<SlotId, ExprContext*>& ctxs, RuntimeState* state);

    static Status clone_if_not_exists(RuntimeState* state, ObjectPool* pool, const std::vector<ExprContext*>& ctxs,
                                      std::vector<ExprContext*>* new_ctxs);
};

} // namespace starrocks
