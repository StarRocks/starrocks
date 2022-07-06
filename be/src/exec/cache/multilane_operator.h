// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/cache/lane_arbiter.h"
#include "exec/pipeline/operator.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace cache {
class MultilaneOperator;
using MultilaneOperatorRawPtr = MultilaneOperator*;
using MultilaneOperators = std::vector<MultilaneOperatorRawPtr>;
using MultilaneOperatorPtr = std::shared_ptr<MultilaneOperator>;
class MultilaneOperatorFactory;
using MultilaneOperatorFactoryRawPtr = MultilaneOperatorFactory*;
using MultilaneOperatorFactoryPtr = std::shared_ptr<MultilaneOperatorFactory>;

class MultilaneOperator final : public pipeline::Operator {
public:
    struct Lane {
        pipeline::OperatorPtr processor;
        int64_t lane_owner;
        int lane_id;
        bool last_chunk_received;
        bool eof_sent;
        Lane(pipeline::OperatorPtr&& op, int id)
                : processor(std::move(op)), lane_owner(-1), lane_id(id), last_chunk_received(false), eof_sent(false) {}
        std::string to_debug_string() const {
            return strings::Substitute("Lane(lane_owner=$0, last_chunk_received=$1, eof_send=$2, operator=$3)",
                                       lane_owner, last_chunk_received, eof_sent, processor->get_name());
        }
    };

    MultilaneOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, size_t num_lanes,
                      pipeline::Operators&& processors, bool can_passthrough);

    ~MultilaneOperator() = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;
    Status set_cancelled(RuntimeState* state) override;
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    void set_lane_arbiter(const LaneArbiterPtr& lane_arbiter) { _lane_arbiter = lane_arbiter; }

    Status reset_lane(LaneOwnerType lane_id, std::vector<vectorized::ChunkPtr>&& chunks);

    bool is_multilane() const override { return true; }

private:
    StatusOr<vectorized::ChunkPtr> _pull_chunk_from_lane(RuntimeState* state, Lane& lane, bool passthrough_mode);
    using FinishCallback = std::function<Status(pipeline::OperatorPtr&, RuntimeState*)>;
    Status _finish(RuntimeState* state, FinishCallback finish_cb);
    const size_t _num_lanes;
    LaneArbiterPtr _lane_arbiter = nullptr;
    std::vector<Lane> _lanes;
    std::unordered_map<int64_t, int> _owner_to_lanes;
    vectorized::ChunkPtr _passthrough_chunk;
    const bool _can_passthrough;
    int _passthrough_lane_id = -1;
    bool _input_finished{false};
};

class MultilaneOperatorFactory final : public pipeline::OperatorFactory {
public:
    MultilaneOperatorFactory(int32_t id, OperatorFactoryPtr factory, size_t num_lanes);
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    void set_can_passthrough(bool on) { _can_passthrough = on; }

private:
    OperatorFactoryPtr _factory;
    const size_t _num_lanes;
    bool _can_passthrough = false;
};
} // namespace cache
} // namespace starrocks
