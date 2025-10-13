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

#include "common/status.h"
#include "exec/pipeline/operator.h"
#include "exec/tablet_info.h"
#include "runtime/lookup_stream_mgr.h"

namespace starrocks::pipeline {

class FetchProcessor;
class FetchProcessorFactory;

class FetchSinkOperator final : public Operator {
public:
    FetchSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                      std::shared_ptr<FetchProcessor> processor);

    ~FetchSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override;
    bool pending_finish() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::NotSupported("FetchSinkOperator does not support pull_chunk");
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    friend class FetchProcessor;
    using PLookUpRequestPtr = std::shared_ptr<PLookUpRequest>;

    std::shared_ptr<FetchProcessor> _processor;
};

class FetchSinkOperatorFactory final : public OperatorFactory {
public:
    FetchSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<FetchProcessorFactory> processor_factory)
            : OperatorFactory(id, "fetch_sink", plan_node_id), _processor_factory(std::move(processor_factory)) {}

    ~FetchSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    std::shared_ptr<FetchProcessorFactory> _processor_factory;
};

} // namespace starrocks::pipeline