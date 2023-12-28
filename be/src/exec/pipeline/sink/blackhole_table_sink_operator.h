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

#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

class BlackHoleTableSinkOperator final : public Operator {
public:
    BlackHoleTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "blackhole_table_sink", plan_node_id, false, driver_sequence) {}
    ~BlackHoleTableSinkOperator() override = default;

    bool has_output() const override { return false; }

    bool need_input() const override { return true; }

    bool is_finished() const override { return _is_finished.load(); }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::NotSupported("Not support pull chunk from BlackHoleTableSinkOperator");
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    std::atomic<bool> _is_finished = false;
};

class BlackHoleTableSinkOperatorFactory final : public OperatorFactory {
public:
    explicit BlackHoleTableSinkOperatorFactory(int32_t id)
            : OperatorFactory(id, "blackhole_table_sink_factory", Operator::s_pseudo_plan_node_id_for_final_sink) {}

    ~BlackHoleTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<BlackHoleTableSinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

} // namespace starrocks::pipeline
