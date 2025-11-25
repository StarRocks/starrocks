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

#include "exec/pipeline/group_execution/execution_group.h"

#include <cstddef>

#include "common/logging.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/current_thread.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::pipeline {
// clang-format off
template <typename T>
concept DriverPtrCallable = std::invocable<T, const DriverPtr&> &&
        (std::same_as<std::invoke_result_t<T, const DriverPtr&>, void> ||
         std::same_as<std::invoke_result_t<T, const DriverPtr&>, Status>);
// clang-format on

void ExecutionGroup::clear_all_drivers(Pipelines& pipelines) {
    for (auto& pipeline : pipelines) {
        pipeline->clear_drivers();
    }
}

Status NormalExecutionGroup::prepare_pipelines(RuntimeState* state) {
    for (auto& pipeline : _pipelines) {
        RETURN_IF_ERROR(pipeline->prepare(state));
        _total_logical_dop += pipeline->degree_of_parallelism();
    }
    return Status::OK();
}

template <DriverPtrCallable Callable>
auto for_each_active_driver(PipelineRawPtrs& pipelines, Callable call) {
    using ReturnType = std::invoke_result_t<Callable, const DriverPtr&>;
    for (auto& pipeline : pipelines) {
        for (auto& driver : pipeline->drivers()) {
            auto* source_op = pipeline->source_operator_factory();
            if (!source_op->is_adaptive_group_initial_active()) {
                continue;
            }
            if constexpr (std::same_as<ReturnType, Status>) {
                RETURN_IF_ERROR(call(driver));
            } else {
                call(driver);
            }
        }
    }
    if constexpr (std::is_same_v<ReturnType, Status>) {
        return Status::OK();
    }
}

size_t ExecutionGroup::total_active_driver_size() {
    size_t total = 0;
    for_each_active_driver(_pipelines, [&total](const DriverPtr& driver) { total += 1; });
    return total;
}

void ExecutionGroup::prepare_active_drivers_parallel(RuntimeState* state,
                                                     std::shared_ptr<DriverPrepareSyncContext> sync_ctx) {
    auto pipeline_prepare_pool = state->exec_env()->pipeline_prepare_pool();

    for_each_active_driver(_pipelines, [&](const DriverPtr& driver) {
        bool submitted = pipeline_prepare_pool->try_offer([sync_ctx, &driver, runtime_state = state]() {
            // make sure mem tracker is instance level
            auto mem_tracker = runtime_state->instance_mem_tracker();
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker);
            // do the thread-safe prepare operation
            Status status = driver->prepare_local_state(runtime_state);

            if (!status.ok()) {
                Status* expected = nullptr;
                Status* new_error = new Status(status);
                if (!sync_ctx->first_error.compare_exchange_strong(expected, new_error)) {
                    // Another thread already set the error, clean up our allocation
                    delete new_error;
                }
            }

            if (sync_ctx->pending_tasks.fetch_sub(1) == 1) {
                std::lock_guard<std::mutex> lock(sync_ctx->mutex);
                sync_ctx->cv.notify_one();
            }
        });

        if (!submitted) {
            Status status = driver->prepare_local_state(state);
            if (!status.ok()) {
                Status* expected = nullptr;
                Status* new_error = new Status(status);
                if (!sync_ctx->first_error.compare_exchange_strong(expected, new_error)) {
                    // Another thread already set the error, clean up our allocation
                    delete new_error;
                }
            }

            if (sync_ctx->pending_tasks.fetch_sub(1) == 1) {
                std::lock_guard<std::mutex> lock(sync_ctx->mutex);
                sync_ctx->cv.notify_one();
            }
        }
    });
}

Status ExecutionGroup::prepare_active_drivers_sequentially(RuntimeState* state) {
    return for_each_active_driver(_pipelines, [&](const DriverPtr& driver) {
        RETURN_IF_ERROR(driver->prepare_local_state(state));
        return Status::OK();
    });
}

Status NormalExecutionGroup::prepare_drivers(RuntimeState* state) {
    return for_each_active_driver(_pipelines, [state](const DriverPtr& driver) { return driver->prepare(state); });
}

void NormalExecutionGroup::submit_active_drivers() {
    VLOG_QUERY << "submit_active_drivers:" << to_string();
    return for_each_active_driver(_pipelines, [this](const DriverPtr& driver) { _executor->submit(driver.get()); });
}

void NormalExecutionGroup::add_pipeline(PipelineRawPtr pipeline) {
    _pipelines.emplace_back(std::move(pipeline));
    _num_pipelines = _pipelines.size();
}

void NormalExecutionGroup::close(RuntimeState* state) {
    for (auto& pipeline : _pipelines) {
        pipeline->close(state);
    }
}

std::string NormalExecutionGroup::to_string() const {
    std::stringstream ss;
    ss << "NormalExecutionGroup: ";
    for (const auto& pipeline : _pipelines) {
        ss << pipeline->to_readable_string() << ",";
    }
    return ss.str();
}

Status ColocateExecutionGroup::prepare_pipelines(RuntimeState* state) {
    for (auto& pipeline : _pipelines) {
        RETURN_IF_ERROR(pipeline->prepare(state));
        _total_logical_dop = pipeline->degree_of_parallelism();
    }
    _submit_drivers = std::make_unique<std::atomic<int>[]>(_pipelines.size());
    return Status::OK();
}

Status ColocateExecutionGroup::prepare_drivers(RuntimeState* state) {
    return for_each_active_driver(_pipelines, [state](const DriverPtr& driver) { return driver->prepare(state); });
}

void ColocateExecutionGroup::submit_active_drivers() {
    VLOG_QUERY << "submit_active_drivers:" << to_string();
    // record the number of drivers to be submitted for each pipeline
    // Two-phase process to avoid race with submit_next_driver():
    // 1) publish all _submit_drivers[i]
    // 2) then submit initial drivers
    std::vector<size_t> pipeline_init_submit_drivers(_pipelines.size());

    for (size_t i = 0; i < _pipelines.size(); ++i) {
        const auto& pipeline = _pipelines[i];
        DCHECK_EQ(pipeline->drivers().size(), pipeline->degree_of_parallelism());
        const auto& drivers = pipeline->drivers();
        size_t init_submit_drivers = std::min(_physical_dop, drivers.size());
        _submit_drivers[i] = init_submit_drivers;
        pipeline_init_submit_drivers[i] = init_submit_drivers;
    }

    for (size_t i = 0; i < _pipelines.size(); ++i) {
        const auto& pipeline = _pipelines[i];
        const auto& drivers = pipeline->drivers();
        size_t init_submit_drivers = pipeline_init_submit_drivers[i];
        for (size_t j = 0; j < init_submit_drivers; ++j) {
            VLOG_QUERY << "submit_active_driver:" << j << ":" << drivers[j]->to_readable_string();
            _executor->submit(drivers[j].get());
        }
    }
}

void ColocateExecutionGroup::add_pipeline(PipelineRawPtr pipeline) {
    _pipelines.emplace_back(std::move(pipeline));
    _num_pipelines = _pipelines.size();
}

void ColocateExecutionGroup::close(RuntimeState* state) {
    for (auto& pipeline : _pipelines) {
        pipeline->close(state);
    }
}

std::string ColocateExecutionGroup::to_string() const {
    std::stringstream ss;
    ss << "ColocateExecutionGroup: ";
    for (const auto& pipeline : _pipelines) {
        ss << pipeline->to_readable_string() << ",";
    }
    return ss.str();
}

void ColocateExecutionGroup::submit_next_driver() {
    for (size_t i = 0; i < _pipelines.size(); ++i) {
        auto next_driver_idx = _submit_drivers[i].fetch_add(1);
        if (next_driver_idx >= _pipelines[i]->degree_of_parallelism()) {
            continue;
        }
        const auto& drivers = _pipelines[i]->drivers();
        VLOG_QUERY << "submit_next_drivers:" << next_driver_idx << ":"
                   << drivers[next_driver_idx]->to_readable_string();
        _executor->submit(drivers[next_driver_idx].get());
    }
}

} // namespace starrocks::pipeline