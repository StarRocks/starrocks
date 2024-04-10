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
#include <unordered_map>

#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/spill_process_channel.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
// similar with query_cache::MultilaneOperator but it only proxy one operator.
// MultiLane operator couldn't split to two operator and it couldn't used as source operator

struct BucketProcessContext {
    std::atomic_bool finished{};
    std::atomic_bool all_input_finishing{};
    std::atomic_bool current_bucket_sink_finished{};
    // The tokens BucketSink::set_finishing and BucketSource::pull_chunk may have races.
    // the party that gets the token performs the final state recovery.
    std::atomic_bool token{};
    // The final condition for reset_version and sink_complete_version needs to satisfy sink_complete_version = reset_version + 1.
    // This value is incremented every time operator->reset_state is executed.
    std::atomic_int reset_version{};
    // This value is incremented every time operator->set_finishing is executed.
    std::atomic_int sink_complete_version{};

    OperatorPtr source;
    OperatorPtr sink;
    SpillProcessChannelPtr spill_channel;

    Status reset_operator_state(RuntimeState* state);

    Status finish_current_sink(RuntimeState* state);
};
using BucketProcessContextPtr = std::shared_ptr<BucketProcessContext>;

class BucketProcessSinkOperator : public Operator {
public:
    BucketProcessSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                              BucketProcessContextPtr& ctx)
            : Operator(factory, id, "bucket_process_sink", plan_node_id, false, driver_sequence), _ctx(ctx) {}
    ~BucketProcessSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::NotSupported("un-support pull for sink operator");
    }
    Status set_finishing(RuntimeState* state) override;
    bool need_input() const override;
    bool has_output() const override { return false; }
    bool is_finished() const override;
    bool ignore_empty_eos() const override { return false; }
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override {
        CHECK(false) << "unreachable path plan node id:" << _plan_node_id;
        return Status::NotSupported("unsupport reset state for bucket process sink operator");
    }

    bool is_combinatorial_operator() const override { return true; }
    void for_each_child_operator(const std::function<void(Operator*)>& apply) override { apply(_ctx->sink.get()); }

private:
    BucketProcessContextPtr _ctx;
};

class BucketProcessSourceOperator : public SourceOperator {
public:
    BucketProcessSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                BucketProcessContextPtr& ctx)
            : SourceOperator(factory, id, "bucket_process_source", plan_node_id, false, driver_sequence), _ctx(ctx) {}
    ~BucketProcessSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;
    bool has_output() const override;
    bool is_finished() const override;
    Status set_finished(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool is_combinatorial_operator() const override { return true; }
    void for_each_child_operator(const std::function<void(Operator*)>& apply) override { apply(_ctx->source.get()); }

private:
    BucketProcessContextPtr _ctx;
};

class BucketProcessContextFactory {
public:
    BucketProcessContextPtr get_or_create(int32_t sequence) {
        if (sequence_to_contexts.find(sequence) == sequence_to_contexts.end()) {
            sequence_to_contexts.emplace(sequence, std::make_shared<BucketProcessContext>());
        }
        return sequence_to_contexts.at(sequence);
    }

private:
    std::unordered_map<int32_t, BucketProcessContextPtr> sequence_to_contexts;
};
using BucketProcessContextFactoryPtr = std::shared_ptr<BucketProcessContextFactory>;

class BucketProcessSinkOperatorFactory final : public OperatorFactory {
public:
    BucketProcessSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                     const BucketProcessContextFactoryPtr& context_factory,
                                     const OperatorFactoryPtr& factory);
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    OperatorFactoryPtr _factory;
    BucketProcessContextFactoryPtr _ctx_factory;
};

class BucketProcessSourceOperatorFactory final : public SourceOperatorFactory {
public:
    BucketProcessSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                       const BucketProcessContextFactoryPtr& context_factory,
                                       const OperatorFactoryPtr& factory);
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    OperatorFactoryPtr _factory;
    BucketProcessContextFactoryPtr _ctx_factory;
};

} // namespace starrocks::pipeline
