// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    const std::shared_ptr<LocalExchanger>& _exchanger;
};

class LocalExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    LocalExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<LocalExchanger> exchanger)
            : OperatorFactory(id, "local_exchange_sink", plan_node_id), _exchanger(std::move(exchanger)) {}

    ~LocalExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LocalExchangeSinkOperator>(this, _id, _plan_node_id, driver_sequence, _exchanger);
    }

private:
    std::shared_ptr<LocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline
