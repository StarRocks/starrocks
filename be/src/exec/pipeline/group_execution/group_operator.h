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

#include <utility>

#include "common/statusor.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/source_operator.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
// P1: SourceOp->ProjectOp->JoinProbeOp->ProjectOp->ExchangeSink
// =>
// P1: SourceOp->ProjectOp->JoinProbeOp->ProjectOp->GroupedSinkOp
// P2: GroupedSourceOp->ExchangeSink
class GroupedExecutionSinkOperator : public Operator {
public:
    GroupedExecutionSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                 const std::shared_ptr<LocalExchanger>& exchanger)
            : Operator(factory, id, "group_exchange_sink", plan_node_id, true, driver_sequence), _exchanger(exchanger) {
        _unique_metrics->add_info_string("Type", exchanger->name());
    }
    ~GroupedExecutionSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override { return _is_finished || _exchanger->is_all_sources_finished(); }
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return Status::InternalError("Not implemented"); }

private:
    std::atomic_bool _is_finished = false;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    const std::shared_ptr<LocalExchanger>& _exchanger;
};

class GroupedExecutionSinkFactory final : public OperatorFactory {
public:
    GroupedExecutionSinkFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<LocalExchanger> exchanger,
                                ExecutionGroupRawPtr exec_group)
            : OperatorFactory(id, "grouped_execution_sink", plan_node_id),
              _exec_group(exec_group),
              _exchanger(std::move(exchanger)) {}

    ~GroupedExecutionSinkFactory() override = default;
    bool support_event_scheduler() const override { return true; }

    Status prepare(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    void close(RuntimeState* state) override;
    void submit();

private:
    ExecutionGroupRawPtr _exec_group;
    std::shared_ptr<LocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline