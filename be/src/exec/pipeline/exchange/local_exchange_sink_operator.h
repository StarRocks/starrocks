// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {
class LocalExchangeSinkOperator final : public Operator {
public:
    LocalExchangeSinkOperator(int32_t id, const std::shared_ptr<LocalExchanger>& exchanger)
            : Operator(id, "local_exchange_sink", -1), _exchanger(exchanger) {}

    ~LocalExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    void finish(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    const std::shared_ptr<LocalExchanger>& _exchanger;
};

class LocalExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    LocalExchangeSinkOperatorFactory(int32_t id, const std::shared_ptr<LocalExchanger>& exchanger)
            : OperatorFactory(id, -1), _exchanger(exchanger) {}

    ~LocalExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LocalExchangeSinkOperator>(_id, _exchanger);
    }

private:
    std::shared_ptr<LocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline
