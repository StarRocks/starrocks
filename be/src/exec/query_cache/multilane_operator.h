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
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/pipeline/operator.h"
#include "exec/query_cache/lane_arbiter.h"
#include "runtime/runtime_state.h"

namespace starrocks::query_cache {
class MultilaneOperator;
using MultilaneOperatorRawPtr = MultilaneOperator*;
using MultilaneOperators = std::vector<MultilaneOperatorRawPtr>;
using MultilaneOperatorPtr = std::shared_ptr<MultilaneOperator>;
class MultilaneOperatorFactory;
using MultilaneOperatorFactoryRawPtr = MultilaneOperatorFactory*;
using MultilaneOperatorFactoryPtr = std::shared_ptr<MultilaneOperatorFactory>;

// MultilaneOperator is decorator introduced to support per-tablet computation, MultilaneOperator has several
// lanes the number of which is designated by _lane_arbiter->num_lanes(), each lane is a operator instance that
// MultilaneOperator decorates. The lane is acquired/released to/from the underlying tablet of morsels picked from
// MorselQueue dynamically.
class MultilaneOperator final : public pipeline::Operator {
public:
    struct Lane {
        pipeline::OperatorPtr processor;
        int64_t lane_owner{-1};
        int lane_id;
        bool last_chunk_received{false};
        bool eof_sent{false};
        Lane(pipeline::OperatorPtr&& op, int id) : processor(std::move(op)), lane_id(id) {}
        std::string to_debug_string() const {
            return strings::Substitute("Lane(lane_owner=$0, last_chunk_received=$1, eof_send=$2, operator=$3)",
                                       lane_owner, last_chunk_received, eof_sent, processor->get_name());
        }
    };

    MultilaneOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, size_t num_lanes,
                      pipeline::Operators&& processors, bool can_passthrough);

    ~MultilaneOperator() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;
    Status set_cancelled(RuntimeState* state) override;
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    void set_lane_arbiter(const LaneArbiterPtr& lane_arbiter) { _lane_arbiter = lane_arbiter; }

    Status reset_lane(RuntimeState* state, LaneOwnerType lane_id, const std::vector<ChunkPtr>& chunks);

    pipeline::OperatorPtr get_internal_op(size_t i);

    void set_precondition_ready(starrocks::RuntimeState* state) override;

private:
    StatusOr<ChunkPtr> _pull_chunk_from_lane(RuntimeState* state, Lane& lane, bool passthrough_mode);
    using FinishCallback = std::function<Status(pipeline::OperatorPtr&, RuntimeState*)>;
    Status _finish(RuntimeState* state, const FinishCallback& finish_cb);
    const size_t _num_lanes;
    LaneArbiterPtr _lane_arbiter = nullptr;
    std::vector<Lane> _lanes;
    std::unordered_map<int64_t, int> _owner_to_lanes;
    ChunkPtr _passthrough_chunk;
    const bool _can_passthrough;
    int _passthrough_lane_id = -1;
    bool _input_finished{false};
};

class MultilaneOperatorFactory final : public pipeline::OperatorFactory {
public:
    MultilaneOperatorFactory(int32_t id, const OperatorFactoryPtr& factory, size_t num_lanes);
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    // can_passthrough should be true for the operator that precedes cache_operator immediately.
    // because only this operator is computation-intensive, so its input chunks must be pass through
    // this operator if its computation imposes an unacceptable performance penalty on cache mechanism.
    void set_can_passthrough(bool on) { _can_passthrough = on; }

private:
    OperatorFactoryPtr _factory;
    const size_t _num_lanes;
    bool _can_passthrough = false;
};
} // namespace starrocks::query_cache
