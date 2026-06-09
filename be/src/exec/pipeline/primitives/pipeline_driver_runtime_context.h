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

#include "compute_env/pipeline/pipeline_timer_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks::pipeline {

// Non-owning runtime view for a pipeline driver. FragmentContext owns the lifetime
// of the referenced query, fragment, runtime state, and pipeline objects.
struct PipelineDriverRuntimeContext {
    QueryContext* query_ctx = nullptr;
    QueryRuntimeState* query_runtime_state = nullptr;
    FragmentContext* fragment_ctx = nullptr;
    FragmentRuntimeState* fragment_runtime_state = nullptr;
    RuntimeState* runtime_state = nullptr;
    Event* pipeline_event = nullptr;
    DriverObserver* driver_observer = nullptr;
    PipelineTimerContextPtr pipeline_timer_context = nullptr;
    QueryContextLifetimeWeakPtr query_ctx_lifetime;
};

} // namespace starrocks::pipeline
