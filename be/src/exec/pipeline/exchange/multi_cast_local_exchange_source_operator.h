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
#include "exec/pipeline/source_operator.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"

namespace starrocks::pipeline {

class MultiCastLocalExchangeSourceOperator final : public SourceOperator {
public:
    MultiCastLocalExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, int32_t mcast_consumer_index,
                                         std::shared_ptr<MultiCastLocalExchanger> exchanger,
                                         const std::vector<ExprContext*>& conjunct_ctxs)
            : SourceOperator(factory, id, "multi_cast_local_exchange_source", plan_node_id, true, driver_sequence),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(std::move(exchanger)),
              _conjunct_ctxs(conjunct_ctxs) {}

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void update_exec_stats(RuntimeState* state) override {}

private:
    bool _is_finished = false;
    int32_t _mcast_consumer_index;
    std::shared_ptr<MultiCastLocalExchanger> _exchanger;
    const std::vector<ExprContext*>& _conjunct_ctxs;
};

class MultiCastLocalExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    MultiCastLocalExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id, int32_t mcast_consumer_index,
                                                std::shared_ptr<MultiCastLocalExchanger> exchanger,
                                                std::vector<ExprContext*>&& conjunct_ctxs = {})
            : SourceOperatorFactory(id, "multi_cast_local_exchange_source", plan_node_id),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(std::move(exchanger)),
              _conjunct_ctxs(std::move(conjunct_ctxs)) {}
    ~MultiCastLocalExchangeSourceOperatorFactory() override = default;
    bool support_event_scheduler() const override { return _exchanger->support_event_scheduler(); }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    void set_runtime_filter_collector(RuntimeFilterProbeCollector* collector);

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MultiCastLocalExchangeSourceOperator>(
                this, _id, _plan_node_id, driver_sequence, _mcast_consumer_index, _exchanger, _conjunct_ctxs);
    }

private:
    int32_t _mcast_consumer_index;
    std::shared_ptr<MultiCastLocalExchanger> _exchanger;
    std::vector<ExprContext*> _conjunct_ctxs;
};
} // namespace starrocks::pipeline
