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

#include "exec/runtime/pipeline.h"

#include "exec/pipeline/operator.h"
#include "exec/pipeline/primitives/event.h"
#include "exec/runtime/pipeline_driver.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Pipeline::Pipeline(uint32_t id, OpFactories op_factories, PipelineGroupRawPtr group)
        : _id(id), _op_factories(std::move(op_factories)), _pipeline_event(Event::create_event()), _group(group) {
    _runtime_profile = std::make_shared<RuntimeProfile>(strings::Substitute("Pipeline (id=$0)", _id));
}

size_t Pipeline::degree_of_parallelism() const {
    // DOP (degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
    return source_operator_factory()->degree_of_parallelism();
}

void Pipeline::on_driver_finished(RuntimeState* state) {
    size_t num_drivers = _drivers.size();
    bool all_drivers_finished = ++_num_finished_drivers >= num_drivers;
    if (all_drivers_finished) {
        _pipeline_event->finish(state);
        _group->count_down_pipeline();
    }
}

void Pipeline::clear_drivers() {
    _drivers.clear();
}

const Drivers& Pipeline::drivers() const {
    return _drivers;
}

Drivers& Pipeline::mutable_drivers() {
    return _drivers;
}

void Pipeline::setup_pipeline_profile(RuntimeState* runtime_state) {
    runtime_state->runtime_profile()->add_child(runtime_profile(), true, nullptr);
    // Set pipeline-level counters once here rather than redundantly in every setup_drivers_profile call.
    runtime_profile()->add_info_string("IsGroupExecution", _group->is_group_execution() ? "true" : "false");
    auto* dop_counter =
            ADD_COUNTER_SKIP_MERGE(runtime_profile(), "DegreeOfParallelism", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
    COUNTER_SET(dop_counter, static_cast<int64_t>(source_operator_factory()->degree_of_parallelism()));
    auto* total_dop_counter = ADD_COUNTER(runtime_profile(), "TotalDegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(total_dop_counter, COUNTER_VALUE(dop_counter));
}

void Pipeline::setup_drivers_profile(const DriverPtr& driver) {
    runtime_profile()->add_child(driver->runtime_profile(), true, nullptr);
    auto& operators = driver->operators();
    for (int32_t i = operators.size() - 1; i >= 0; --i) {
        auto& curr_op = operators[i];
        driver->runtime_profile()->add_child(curr_op->runtime_profile(), true, nullptr);
        if (curr_op->is_combinatorial_operator()) {
            curr_op->for_each_child_operator([&](Operator* child) {
                driver->runtime_profile()->add_child(child->runtime_profile(), true, nullptr);
            });
        }
    }
}

struct OutputAmplificationAddCalculator {
    size_t operator()(size_t accumulate, size_t value) const { return accumulate + value; }
};

struct OutputAmplificationMaxCalculator {
    size_t operator()(size_t accumulate, size_t value) const { return std::max(accumulate, value); }
};

template <typename Calculator>
size_t calculate_output_amplification(const Drivers& drivers) {
    Calculator calculator;
    size_t result = 0;
    for (const auto& driver : drivers) {
        result = calculator(result, driver->sink_operator()->output_amplification_factor());
    }
    return std::max<size_t>(1, result);
}

size_t Pipeline::output_amplification_factor() const {
    if (_drivers.empty()) {
        return 1;
    }

    auto* first_sink = _drivers[0]->sink_operator();
    switch (first_sink->intra_pipeline_amplification_type()) {
    case Operator::OutputAmplificationType::ADD:
        return calculate_output_amplification<OutputAmplificationAddCalculator>(_drivers);
    case Operator::OutputAmplificationType::MAX:
        return calculate_output_amplification<OutputAmplificationMaxCalculator>(_drivers);
    }

    return 1;
}

} // namespace starrocks::pipeline
