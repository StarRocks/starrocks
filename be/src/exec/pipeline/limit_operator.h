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
class LimitOperator final : public Operator {
public:
    LimitOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                  std::atomic<int64_t>& limit)
            : Operator(factory, id, "limit", plan_node_id, driver_sequence), _limit(limit) {}

    ~LimitOperator() override = default;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return !_is_finished && _limit != 0 && _cur_chunk == nullptr; }

    bool is_finished() const override { return (_is_finished || _limit == 0) && _cur_chunk == nullptr; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    std::atomic<int64_t>& _limit;
    ChunkPtr _cur_chunk = nullptr;
};

class LimitOperatorFactory final : public OperatorFactory {
public:
    LimitOperatorFactory(int32_t id, int32_t plan_node_id, int64_t limit)
            : OperatorFactory(id, "limit", plan_node_id), _limit(limit) {}

    ~LimitOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LimitOperator>(this, _id, _plan_node_id, driver_sequence, _limit);
    }

private:
    std::atomic<int64_t> _limit;
};

} // namespace starrocks::pipeline
