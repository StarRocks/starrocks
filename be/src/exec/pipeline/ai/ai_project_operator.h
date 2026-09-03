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

#include <memory>
#include <utility>
#include <vector>

#include "exec/pipeline/ai/ai_project_processor.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/source_operator.h"

namespace starrocks::pipeline {

class AISinkOperator final : public Operator {
public:
    AISinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                   int32_t degree_of_parallelism, std::shared_ptr<AIProjectProcessor> processor);

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override { return _is_finished; }
    bool pending_finish() const override;
    bool ignore_empty_eos() const override { return false; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;
    Status set_cancelled(RuntimeState* state) override;

private:
    Status _advance_pending() const;
    Status _record_status(const Status& status) const;
    Status _force_finish() const;

    const std::shared_ptr<AIProjectProcessor> _processor;
    const int32_t _degree_of_parallelism;
    mutable ChunkPtr _pending_chunk;
    mutable Status _status;
    mutable bool _is_finishing = false;
    mutable bool _is_finished = false;
    mutable bool _sink_eos = false;
};

class AISinkOperatorFactory final : public OperatorFactory {
public:
    AISinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<AIProjectProcessor> processor);

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    bool support_event_scheduler() const override { return true; }

private:
    const std::shared_ptr<AIProjectProcessor> _processor;
};

class AISourceOperator final : public SourceOperator {
public:
    AISourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                     int32_t degree_of_parallelism, std::shared_ptr<AIProjectProcessor> processor);

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;
    bool pending_finish() const override;
    bool ignore_empty_eos() const override { return false; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status set_finishing(RuntimeState* state) override { return Status::OK(); }
    Status set_finished(RuntimeState* state) override;
    Status set_cancelled(RuntimeState* state) override;

private:
    const std::shared_ptr<AIProjectProcessor> _processor;
    const int32_t _degree_of_parallelism;
    Status _finish_status;
    bool _is_finished = false;
};

class AISourceOperatorFactory final : public SourceOperatorFactory {
public:
    AISourceOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<AIProjectProcessor> processor);

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool support_event_scheduler() const override { return true; }

    void set_bucket_properties(std::vector<TBucketProperty> bucket_properties) {
        _bucket_properties = std::move(bucket_properties);
    }

private:
    const std::shared_ptr<AIProjectProcessor> _processor;
};

} // namespace starrocks::pipeline
