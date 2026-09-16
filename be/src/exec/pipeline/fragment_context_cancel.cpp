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

#include "exec/pipeline/fragment_context_cancel.h"

#include "common/logging.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

void cancel_fragment_context(FragmentContext* fragment_ctx, const Status& status, bool cancelled_by_fe) {
    DCHECK(fragment_ctx != nullptr);
    auto* runtime_state = fragment_ctx->runtime_state();
    if (!status.ok() && runtime_state != nullptr && runtime_state->query_ctx() != nullptr) {
        runtime_state->query_ctx()->release_workgroup_token_once();
        if (cancelled_by_fe) {
            runtime_state->query_ctx()->set_cancelled_by_fe();
        }
    }

    fragment_ctx->cancel(status);
}

} // namespace starrocks::pipeline
