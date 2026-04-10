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

#include "exec/spill/executor.h"

#include "runtime/runtime_state.h"

namespace starrocks::spill {

TraceInfo::TraceInfo(RuntimeState* state) : query_id(state->query_id()), fragment_id(state->fragment_instance_id()) {}

std::weak_ptr<pipeline::QueryContext> spill_query_ctx_weak_ptr(RuntimeState* state) {
    return state->query_ctx()->weak_from_this();
}

} // namespace starrocks::spill
