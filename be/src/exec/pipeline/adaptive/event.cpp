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

#include "exec/pipeline/adaptive/event.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <utility>

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "util/failpoint/fail_point.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::pipeline {

// ------------------------------------------------------------------------------------
// Event
// ------------------------------------------------------------------------------------

void Event::finish(RuntimeState* state) {
    if (bool expected_finished = false; !_finished.compare_exchange_strong(expected_finished, true)) {
        return;
    }

    for (auto& dependee_entry : _dependees) {
        auto dependee = dependee_entry.lock();
        if (dependee == nullptr) {
            continue;
        }
        dependee->finish_dependency(state);
    }
}

void Event::finish_dependency(RuntimeState* state) {
    if (_num_finished_dependencies.fetch_add(1) + 1 == _num_dependencies) {
        process(state);
    }
}

void Event::add_dependency(Event* event) {
    _num_dependencies++;
    event->_dependees.emplace_back(shared_from_this());
}

std::string Event::to_string() const {
    return std::string("Event{") + "name=" + name() + ",_num_dependencies=" + std::to_string(_num_dependencies) +
           ",_dependees" + std::to_string(_dependees.size()) +
           ",_num_finished_dependencies=" + std::to_string(_num_finished_dependencies.load()) +
           ",_finished=" + std::to_string(_finished.load()) + "}";
}

// ------------------------------------------------------------------------------------
// CollectStatsSourceInitializeEvent
// ------------------------------------------------------------------------------------

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

    for (auto* pipeline : _pipelines) {
        pipeline->source_operator_factory()->adjust_dop();
        pipeline->instantiate_drivers(state);
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

        auto* prepare_thread_pool = state->exec_env()->pipeline_prepare_pool();
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

        // Prepare operators' local state in parallel way
        // Use shared_ptr to manage the lifetime of synchronization primitives to avoid use-after-free.
        // The main thread may destroy local variables before all lambda functions complete execution.
        auto completion_mutex = std::make_shared<std::mutex>();
        auto completion_cv = std::make_shared<std::condition_variable>();
        // Use raw pointer with atomic to avoid std::atomic<std::shared_ptr<T>> compilation issue
        auto first_error = std::make_shared<std::atomic<Status*>>(nullptr);
        auto pending_tasks = std::make_shared<std::atomic<int>>(static_cast<int>(all_drivers.size()));

        for (auto& driver : all_drivers) {
            auto task_fn = [&driver, runtime_state = state, pending_tasks, completion_mutex, completion_cv,
                            first_error]() {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(runtime_state->instance_mem_tracker());
                Status st = driver->prepare_local_state(runtime_state);
                if (!st.ok()) {
                    auto* error_ptr = new Status(std::move(st));
                    Status* expected = nullptr;
                    // Only store the first error
                    (void)first_error->compare_exchange_strong(expected, error_ptr);
                    if (expected != nullptr) {
                        // If CAS failed, another thread already set the error, delete our copy
                        delete error_ptr;
                    }
                }

                if (pending_tasks->fetch_sub(1) == 1) {
                    std::lock_guard<std::mutex> l(*completion_mutex);
                    completion_cv->notify_all();
                }
            };

            bool submit_res = prepare_thread_pool->try_offer(task_fn);
            if (!submit_res) {
                task_fn();
            }
        }

        // Wait for all tasks to finish
        {
            std::unique_lock<std::mutex> lock(*completion_mutex);
            completion_cv->wait(lock, [pending_tasks] { return pending_tasks->load() == 0; });
        }

        Status* error = first_error->load();
        if (error != nullptr) {
            Status result = *error;
            delete error;
            return result;
        }

        return Status::OK();
    };

    if (const auto status = prepare_drivers(); !status.ok()) {
        LOG(WARNING) << "[ADAPTIVE DOP] failed to prepare pipeline drivers [status=" << status.message() << "]";
        state->fragment_ctx()->cancel(status);
        for (const auto& pipeline : _pipelines) {
            // The pipeline without driver indicates it has not been instantiated.
            // So we just count down the driver num once to trigger finalization of the pipeline.
            if (pipeline->drivers().empty()) {
                pipeline->count_down_driver(state);
            } else {
                for (int i = 0; i < pipeline->drivers().size(); ++i) {
                    pipeline->count_down_driver(state);
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

// ------------------------------------------------------------------------------------
// DependsAllEventt
// ------------------------------------------------------------------------------------

class DependsAllEvent final : public Event {
public:
    explicit DependsAllEvent(std::vector<EventPtr> events) : _events(std::move(events)) {}

    ~DependsAllEvent() override = default;

    void process(RuntimeState* state) override { finish(state); }

    std::string name() const override { return "depends_all_event"; }

private:
    std::vector<EventPtr> _events;
};

// ------------------------------------------------------------------------------------
// Event factory methods.
// ------------------------------------------------------------------------------------

EventPtr Event::create_event() {
    return std::make_shared<Event>();
}

EventPtr Event::create_collect_stats_source_initialize_event(DriverExecutor* executor,
                                                             std::vector<Pipeline*>&& pipelines) {
    return std::make_shared<CollectStatsSourceInitializeEvent>(executor, std::move(pipelines));
}

EventPtr Event::depends_all(const std::vector<EventPtr>& events) {
    EventPtr merged_event = std::make_shared<DependsAllEvent>(events);
    for (const auto& event : events) {
        merged_event->add_dependency(event.get());
    }
    return merged_event;
}

} // namespace starrocks::pipeline
