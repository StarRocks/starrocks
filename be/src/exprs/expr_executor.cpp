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

#include "exprs/expr_executor.h"

#include "exprs/expr_context.h"

namespace starrocks {

Status ExprExecutor::prepare(ExprContext* ctx, RuntimeState* state) {
    return ctx->prepare(state);
}

Status ExprExecutor::open(ExprContext* ctx, RuntimeState* state) {
    return ctx->open(state);
}

void ExprExecutor::close(ExprContext* ctx, RuntimeState* state) {
    if (ctx != nullptr) {
        ctx->close(state);
    }
}

Status ExprExecutor::prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (auto* ctx : ctxs) {
        RETURN_IF_ERROR(ctx->prepare(state));
    }
    return Status::OK();
}

Status ExprExecutor::open(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (auto* ctx : ctxs) {
        RETURN_IF_ERROR(ctx->open(state));
    }
    return Status::OK();
}

void ExprExecutor::close(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (auto* ctx : ctxs) {
        if (ctx != nullptr) {
            ctx->close(state);
        }
    }
}

Status ExprExecutor::prepare(const std::map<SlotId, ExprContext*>& ctxs, RuntimeState* state) {
    for (const auto& [_, ctx] : ctxs) {
        RETURN_IF_ERROR(ctx->prepare(state));
    }
    return Status::OK();
}

Status ExprExecutor::open(const std::map<SlotId, ExprContext*>& ctxs, RuntimeState* state) {
    for (const auto& [_, ctx] : ctxs) {
        RETURN_IF_ERROR(ctx->open(state));
    }
    return Status::OK();
}

void ExprExecutor::close(const std::map<SlotId, ExprContext*>& ctxs, RuntimeState* state) {
    for (const auto& [_, ctx] : ctxs) {
        if (ctx != nullptr) {
            ctx->close(state);
        }
    }
}

Status ExprExecutor::clone_if_not_exists(RuntimeState* state, ObjectPool* pool, const std::vector<ExprContext*>& ctxs,
                                         std::vector<ExprContext*>* new_ctxs) {
    if (new_ctxs == nullptr) {
        return Status::InvalidArgument("new_ctxs is null");
    }

    if (!new_ctxs->empty()) {
        if (new_ctxs->size() != ctxs.size()) {
            return Status::InvalidArgument("new_ctxs size does not match input ctxs size");
        }
        return Status::OK();
    }

    new_ctxs->resize(ctxs.size());
    for (size_t i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->clone(state, pool, &(*new_ctxs)[i]));
    }
    return Status::OK();
}

} // namespace starrocks
