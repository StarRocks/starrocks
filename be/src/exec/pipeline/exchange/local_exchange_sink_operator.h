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

#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {
class LocalExchangeSinkOperator final : public Operator {
public:
    LocalExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                              const std::shared_ptr<LocalExchanger>& exchanger)
            : Operator(factory, id, "local_exchange_sink", plan_node_id, driver_sequence), _exchanger(exchanger) {
        _unique_metrics->add_info_string("Type", exchanger->name());
    }

    ~LocalExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    // _is_finished is true indicates that LocalExchangeSinkOperator is finished by its preceding operator.
    // _is_all_source_finished() returning true indicates that all its corresponding LocalExchangeSourceOperators
    // has finished.
    // In either case,  LocalExchangeSinkOperator is finished.
    bool is_finished() const override { return _is_finished || _exchanger->is_all_sources_finished(); }

    bool is_epoch_finished() const override { return _is_epoch_finished; }
    Status set_epoch_finishing(RuntimeState* state) override {
        _is_epoch_finished = true;
        return Status::OK();
    }
    Status set_epoch_finished(RuntimeState* state) override {
        _exchanger->epoch_finish(state);
        return Status::OK();
    }
    Status reset_epoch(RuntimeState* state) override {
        _is_epoch_finished = false;
        return Status::OK();
    }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    const std::shared_ptr<LocalExchanger>& _exchanger;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    // STREAM MV
    bool _is_epoch_finished = false;
};

class LocalExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    LocalExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<LocalExchanger> exchanger)
            : OperatorFactory(id, "local_exchange_sink", plan_node_id), _exchanger(std::move(exchanger)) {}

    ~LocalExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LocalExchangeSinkOperator>(this, _id, _plan_node_id, driver_sequence, _exchanger);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<LocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline
