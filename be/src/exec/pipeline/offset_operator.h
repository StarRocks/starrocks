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

class OffsetOperator final : public Operator {
public:
    OffsetOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                   std::atomic<int64_t>& offset)
            : Operator(factory, id, "offset", plan_node_id, false, driver_sequence), _offset(offset) {}

    ~OffsetOperator() override = default;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return !_is_finished && _cur_chunk == nullptr; }

    bool is_finished() const override { return _is_finished && _cur_chunk == nullptr; }

    Status set_finishing(RuntimeState*) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState*) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    std::atomic<int64_t>& _offset;
    ChunkPtr _cur_chunk = nullptr;
};

class OffsetOperatorFactory final : public OperatorFactory {
public:
    OffsetOperatorFactory(int32_t id, int32_t plan_node_id, int64_t offset)
            : OperatorFactory(id, "offset", plan_node_id), _offset(offset) {}

    ~OffsetOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<OffsetOperator>(this, _id, _plan_node_id, driver_sequence, _offset);
    }

private:
    std::atomic<int64_t> _offset;
};

} // namespace starrocks::pipeline
