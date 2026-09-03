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

#include <atomic>

#include "common/status.h"

namespace starrocks {

class ExecEnv;
class StreamLoadContext;

namespace orchestration {

class FragmentMgr;

class StreamLoadOrchestrator {
public:
    StreamLoadOrchestrator(ExecEnv* exec_env, FragmentMgr* fragment_mgr);

    // seq_cst orders this guard with the drain re-sample.
    void inc_load_inflight() { _load_inflight.fetch_add(1, std::memory_order_seq_cst); }
    void dec_load_inflight() { _load_inflight.fetch_sub(1, std::memory_order_seq_cst); }
    size_t load_inflight() const { return _load_inflight.load(std::memory_order_seq_cst); }

    class LoadInflightGuard {
    public:
        explicit LoadInflightGuard(StreamLoadOrchestrator* orchestrator) : _orchestrator(orchestrator) {
            _orchestrator->inc_load_inflight();
        }
        ~LoadInflightGuard() { _orchestrator->dec_load_inflight(); }

        LoadInflightGuard(const LoadInflightGuard&) = delete;
        LoadInflightGuard& operator=(const LoadInflightGuard&) = delete;

    private:
        StreamLoadOrchestrator* _orchestrator;
    };
    Status execute_plan_fragment(StreamLoadContext* ctx, bool admission_already_granted = false);

private:
    // Legacy (non-pipeline) BE-local execution via FragmentMgr + PlanFragmentExecutor.
    Status _execute_plan_fragment_by_legacy(StreamLoadContext* ctx);
    // Pipeline-engine BE-local execution (gated by FE Config.enable_pipeline_stream_load).
    Status _execute_plan_fragment_by_pipeline(StreamLoadContext* ctx);

    ExecEnv* _exec_env;
    [[maybe_unused]] FragmentMgr* _fragment_mgr;
    std::atomic<size_t> _load_inflight{0};
};

} // namespace orchestration
} // namespace starrocks
