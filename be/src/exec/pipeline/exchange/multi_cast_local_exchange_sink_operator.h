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

#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

class MultiCastLocalExchangeSinkOperator : public Operator {
public:
    MultiCastLocalExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                       const int32_t driver_sequence,
                                       std::shared_ptr<MultiCastLocalExchanger> exchanger)
            : Operator(factory, id, "multi_cast_local_exchange_sink", plan_node_id, true, driver_sequence),
              _exchanger(std::move(std::move(exchanger))) {}

    ~MultiCastLocalExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    bool releaseable() const override { return _exchanger->releaseable(); }

    void set_execute_mode(int performance_level) override { return _exchanger->enter_release_memory_mode(); }

    void update_exec_stats(RuntimeState* state) override {}

protected:
    bool _is_finished = false;
    const std::shared_ptr<MultiCastLocalExchanger> _exchanger;
};

class MultiCastLocalExchangeSinkOperatorFactory : public OperatorFactory {
public:
    MultiCastLocalExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                              std::shared_ptr<MultiCastLocalExchanger> exchanger)
            : OperatorFactory(id, "multi_cast_local_exchange_sink", plan_node_id),
              _exchanger(std::move(std::move(exchanger))) {}

    ~MultiCastLocalExchangeSinkOperatorFactory() override = default;

    bool support_event_scheduler() const override { return _exchanger->support_event_scheduler(); }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MultiCastLocalExchangeSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                    _exchanger);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<MultiCastLocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline