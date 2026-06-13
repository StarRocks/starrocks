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

#include "exec/pipeline/adaptive/collect_stats_event.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <utility>

#include "base/failpoint/fail_point.h"
#include "base/utility/defer_op.h"
#include "common/status.h"
#include "common/thread/priority_thread_pool.hpp"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_cancel.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_instantiator.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "exec/pipeline/primitives/event.h"
#include "exec/pipeline/source_operator.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {

class CollectStatsSourceInitializeEvent final : public Event {
public:
    CollectStatsSourceInitializeEvent(DriverExecutor* executor, std::vector<Pipeline*>&& pipelines);

    ~CollectStatsSourceInitializeEvent() override = default;

    void process(RuntimeState* state) override;

    std::string name() const override { return "collect_stats_source_initialize_event"; }

private:
    DriverExecutor* const _executor;
    /// The pipelines should be in topo order, that is the upstream pipeline of a pipeline should be in front of it.
    std::vector<Pipeline*> _pipelines;
};

CollectStatsSourceInitializeEvent::CollectStatsSourceInitializeEvent(DriverExecutor* executor,
                                                                     std::vector<Pipeline*>&& pipelines)
        : _executor(executor), _pipelines(std::move(pipelines)) {}

DEFINE_FAIL_POINT(collect_stats_source_initialize_prepare_failed);

void CollectStatsSourceInitializeEvent::process(RuntimeState* state) {
    DeferOp defer_op([this, state] { finish(state); });
    auto* fragment_ctx = state->fragment_ctx();

    for (auto* pipeline : _pipelines) {
        pipeline->source_operator_factory()->adjust_dop();
        instantiate_pipeline_drivers(fragment_ctx, pipeline);
    }

    auto prepare_drivers = [state, &pipelines = _pipelines]() {
        size_t total_driver_size = 0;
        for (const auto& pipeline : pipelines) {
            for (const auto& driver : pipeline->drivers()) {
                FAIL_POINT_TRIGGER_RETURN(
                        collect_stats_source_initialize_prepare_failed,
                        Status::InternalError("injected collect_stats_source_initialize_prepare_failed"));
                RETURN_IF_ERROR(driver->prepare(state));
                total_driver_size++;
            }
        }

        auto* query_execution_services = state->query_execution_services();
        auto* prepare_thread_pool = query_execution_services->execution->pipeline_prepare_pool;
        DCHECK(prepare_thread_pool != nullptr);

        std::vector<DriverPtr> all_drivers;
        all_drivers.reserve(total_driver_size);
        for (const auto& pipeline : pipelines) {
            for (const auto& driver : pipeline->drivers()) {
                all_drivers.emplace_back(driver);
            }
        }

        if (all_drivers.empty()) {
            return Status::OK();
        }

        auto sync_ctx = std::make_shared<DriverPrepareSyncContext>();
        sync_ctx->pending_tasks = static_cast<int>(all_drivers.size());

        for (auto& driver : all_drivers) {
            auto task_fn = [&driver, runtime_state = state, sync_ctx]() {
                // hold mem tracker's shared ptr for thread safe
                auto mem_tracker = runtime_state->instance_mem_tracker_ptr();
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker.get());
                Status st = driver->prepare_local_state(runtime_state);
                if (!st.ok()) {
                    Status* expected = nullptr;
                    Status* new_error = new Status(std::move(st));
                    if (!sync_ctx->first_error.compare_exchange_strong(expected, new_error)) {
                        // Another thread already set the error, clean up our allocation
                        delete new_error;
                    }
                }

                if (sync_ctx->pending_tasks.fetch_sub(1) == 1) {
                    std::lock_guard<std::mutex> l(sync_ctx->mutex);
                    sync_ctx->cv.notify_all();
                }
            };

            bool submit_res = prepare_thread_pool->try_offer(task_fn);
            if (!submit_res) {
                task_fn();
            }
        }

        // Wait for all tasks to finish
        {
            std::unique_lock<std::mutex> lock(sync_ctx->mutex);
            sync_ctx->cv.wait(lock, [&sync_ctx] { return sync_ctx->pending_tasks.load() == 0; });
        }

        Status* error = sync_ctx->first_error.load();
        if (error != nullptr) {
            Status result = *error;
            return result;
        }

        return Status::OK();
    };

    if (const auto status = prepare_drivers(); !status.ok()) {
        LOG(WARNING) << "[ADAPTIVE DOP] failed to prepare pipeline drivers [status=" << status.message() << "]";
        cancel_fragment_context(state->fragment_ctx(), status);
        for (const auto& pipeline : _pipelines) {
            // The pipeline without driver indicates it has not been instantiated.
            // So we just count down the driver num once to trigger finalization of the pipeline.
            if (pipeline->drivers().empty()) {
                pipeline->on_driver_finished(state);
            } else {
                for (int i = 0; i < pipeline->drivers().size(); ++i) {
                    pipeline->on_driver_finished(state);
                }
            }
        }
    } else {
        for (const auto& pipeline : _pipelines) {
            for (const auto& driver : pipeline->drivers()) {
                _executor->submit(driver.get());
            }
        }
    }
}

EventPtr create_collect_stats_source_initialize_event(DriverExecutor* executor, std::vector<Pipeline*>&& pipelines) {
    return std::make_shared<CollectStatsSourceInitializeEvent>(executor, std::move(pipelines));
}

} // namespace starrocks::pipeline
