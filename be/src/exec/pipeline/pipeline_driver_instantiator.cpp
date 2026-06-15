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

#include "exec/pipeline/pipeline_driver_instantiator.h"

#include <memory>
#include <utility>

#include "base/uid_util.h"
#include "common/logging.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/morsel_queue_factory.h"
#include "exec/runtime/fragment_runtime_state.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

void instantiate_pipeline_drivers(FragmentContext* fragment_ctx, Pipeline* pipeline) {
    auto* state = fragment_ctx->runtime_state();
    auto* query_ctx = state->query_ctx();
    auto* query_runtime_state = state->query_runtime_state();
    auto* fragment_runtime_state = state->fragment_runtime_state();
    if (fragment_runtime_state == nullptr) {
        fragment_runtime_state = &fragment_ctx->fragment_runtime_state();
    }
    auto pipeline_timer_context = fragment_ctx->pipeline_timer_context();
    auto workgroup = fragment_ctx->workgroup();

    size_t dop = pipeline->degree_of_parallelism();

    VLOG_ROW << "Pipeline " << pipeline->to_readable_string() << " parallel=" << dop
             << " fragment_instance_id=" << print_id(fragment_ctx->fragment_instance_id());

    pipeline->setup_pipeline_profile(state);
    auto& drivers = pipeline->mutable_drivers();
    drivers.reserve(dop);
    for (size_t i = 0; i < dop; ++i) {
        auto&& operators = pipeline->create_operators(dop, i);
        DriverPtr driver = std::make_shared<PipelineDriver>(
                std::move(operators), query_runtime_state, fragment_runtime_state, pipeline->pipeline_event(), pipeline,
                pipeline_timer_context, fragment_ctx->next_driver_id());

        if (state->enable_event_scheduler()) {
            auto* scheduler = fragment_ctx->event_scheduler();
            DCHECK(scheduler != nullptr);
            driver->set_observer(scheduler->create_driver_observer(driver.get()));
            driver->assign_observer();
        }

        pipeline->setup_drivers_profile(driver);
        driver->set_workgroup(workgroup);
        drivers.emplace_back(std::move(driver));
    }

    if (!pipeline->source_operator_factory()->with_morsels()) {
        return;
    }

    auto* morsel_queue_factory = pipeline->source_operator_factory()->morsel_queue_factory();
    DCHECK(morsel_queue_factory != nullptr);
    DCHECK(dop == 1 || dop == morsel_queue_factory->size());
    for (size_t i = 0; i < dop; ++i) {
        auto& driver = drivers[i];
        driver->set_morsel_queue(morsel_queue_factory->create(i));
        if (auto* scan_operator = driver->source_driver_scan_operator()) {
            scan_operator->set_workgroup(workgroup);
            scan_operator->set_query_ctx(query_ctx->get_shared_ptr());
            if (scan_operator->sched_entity_type() == workgroup::ScanSchedEntityType::CONNECTOR) {
                scan_operator->set_scan_executor(workgroup->executors()->connector_scan_executor());
            } else {
                scan_operator->set_scan_executor(workgroup->executors()->scan_executor());
            }
        }
    }
}

} // namespace starrocks::pipeline
