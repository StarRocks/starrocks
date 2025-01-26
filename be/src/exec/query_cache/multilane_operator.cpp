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

#include "exec/query_cache/multilane_operator.h"

#include <glog/logging.h>

#include "util/defer_op.h"

namespace starrocks::query_cache {
MultilaneOperator::MultilaneOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, size_t num_lanes,
                                     pipeline::Operators&& processors, bool can_passthrough)
        : pipeline::Operator(factory, factory->id(), factory->get_raw_name(), factory->plan_node_id(), true,
                             driver_sequence),
          _num_lanes(num_lanes),
          _can_passthrough(can_passthrough) {
    DCHECK_EQ(processors.size(), _num_lanes);
    _lanes.reserve(_num_lanes);
    for (auto i = 0; i < _num_lanes; ++i) {
        _lanes.emplace_back(std::move(processors[i]), i);
    }
}

Status MultilaneOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(pipeline::Operator::prepare(state));
    for (auto& lane : _lanes) {
        RETURN_IF_ERROR(lane.processor->prepare(state));
    }
    return Status::OK();
}

void MultilaneOperator::close(RuntimeState* state) {
    for (auto& lane : _lanes) {
        lane.processor->close(state);
    }
    pipeline::Operator::close(state);
}

bool MultilaneOperator::need_input() const {
    if (_input_finished) {
        return false;
    }

    if (_lane_arbiter->in_passthrough_mode()) {
        if (_can_passthrough) {
            return _passthrough_chunk == nullptr;
        } else if (_passthrough_lane_id != -1) {
            auto& lane = _lanes[_passthrough_lane_id];
            DCHECK(!lane.processor->is_finished());
            return lane.processor->need_input();
        } else {
            return std::any_of(_lanes.begin(), _lanes.end(), [](const auto& lane) {
                return lane.processor->is_finished() || lane.processor->need_input();
            });
        }
    }

    // in non-passthrough mode, all lanes should be available, because we do not know which lane the chunk coming in the
    // future belongs to.
    return std::all_of(_owner_to_lanes.begin(), _owner_to_lanes.end(), [this](auto& pair) {
        auto lane_id = pair.second;
        auto& lane = _lanes[lane_id];
        return lane.last_chunk_received || lane.processor->need_input();
    });
}
bool MultilaneOperator::has_output() const {
    const auto passthrough_mode = _lane_arbiter->in_passthrough_mode();
    for (const auto& [_, lane_id] : _owner_to_lanes) {
        auto& lane = _lanes[lane_id];
        auto processor_is_finished = lane.processor->is_finished();
        auto need_send_eof_chunk = !passthrough_mode && processor_is_finished && !lane.eof_sent;
        auto need_send_chunk = !processor_is_finished && lane.processor->has_output();

        if (need_send_eof_chunk || need_send_chunk) {
            return true;
        }
    }
    if (passthrough_mode) {
        if (_can_passthrough) {
            return _passthrough_chunk != nullptr;
        } else if (_passthrough_lane_id != -1) {
            // _passthrough_lane is allocated
            return _lanes[_passthrough_lane_id].processor->has_output();
        } else {
            // _passthrough_lane is not allocated, so return false
            return false;
        }
    }
    return false;
}

bool MultilaneOperator::is_finished() const {
    if (!_input_finished) {
        return false;
    }
    auto passthrough_mode = _lane_arbiter->in_passthrough_mode();
    auto all_lanes_finished =
            std::all_of(_owner_to_lanes.begin(), _owner_to_lanes.end(), [passthrough_mode, this](auto& it) {
                auto& lane = _lanes[it.second];
                return lane.processor->is_finished() && (lane.eof_sent || passthrough_mode);
            });
    auto passthrough_finished = true;
    if (passthrough_mode) {
        if (_can_passthrough) {
            passthrough_finished = _passthrough_chunk == nullptr;
        } else if (_passthrough_lane_id != -1) {
            passthrough_finished = _lanes[_passthrough_lane_id].processor->is_finished();
        } else {
            passthrough_finished = true;
        }
    }
    return all_lanes_finished && passthrough_finished;
}

Status MultilaneOperator::_finish(starrocks::RuntimeState* state,
                                  const starrocks::query_cache::MultilaneOperator::FinishCallback& finish_cb) {
    _input_finished = true;
    auto status = Status::OK();
    for (auto it : _owner_to_lanes) {
        auto& lane = _lanes[it.second];
        lane.last_chunk_received = true;
        auto tmp_status = finish_cb(lane.processor, state);
        if (!tmp_status.ok()) {
            status = tmp_status;
        }
    }

    if (_lane_arbiter->in_passthrough_mode() && !_can_passthrough && _passthrough_lane_id != -1) {
        auto& lane = _lanes[_passthrough_lane_id];
        auto tmp_status = finish_cb(lane.processor, state);
        lane.last_chunk_received = true;
        if (!tmp_status.ok()) {
            status = tmp_status;
        }
    }
    return status;
}
Status MultilaneOperator::set_finishing(starrocks::RuntimeState* state) {
    return _finish(
            state, [](pipeline::OperatorPtr & op, RuntimeState * state) -> auto { return op->set_finishing(state); });
}
Status MultilaneOperator::set_finished(RuntimeState* state) {
    _passthrough_chunk = nullptr;
    return _finish(
            state, [](pipeline::OperatorPtr & op, RuntimeState * state) -> auto { return op->set_finished(state); });
}

Status MultilaneOperator::set_cancelled(RuntimeState* state) {
    _passthrough_chunk = nullptr;
    return _finish(
            state, [](pipeline::OperatorPtr & op, RuntimeState * state) -> auto { return op->set_cancelled(state); });
}

Status MultilaneOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK(chunk != nullptr);
    if (_lane_arbiter->in_passthrough_mode()) {
        if (chunk->is_empty()) {
            return Status::OK();
        }
        if (_can_passthrough) {
            DCHECK(_passthrough_chunk == nullptr);
            _passthrough_chunk = chunk;
            return Status::OK();
        }
        if (_passthrough_lane_id != -1) {
            auto& lane = _lanes[_passthrough_lane_id];
            return lane.processor->push_chunk(state, chunk);
        }
        auto lane_it = std::find_if(_lanes.begin(), _lanes.end(), [](const auto& lane) {
            return lane.processor->is_finished() || lane.processor->need_input();
        });
        DCHECK(lane_it != _lanes.end());
        _passthrough_lane_id = lane_it->lane_id;
        auto& lane = _lanes[_passthrough_lane_id];
        if (lane.processor->is_finished()) {
            RETURN_IF_ERROR(lane.processor->reset_state(state, {}));
        }
        lane.last_chunk_received = false;
        lane.eof_sent = false;
        DCHECK(!lane.processor->is_finished() && lane.processor->need_input());
        return lane.processor->push_chunk(state, chunk);
    }
    auto lane_owner = chunk->owner_info().owner_id();
    auto is_last_chunk = chunk->owner_info().is_last_chunk();

    DCHECK(_owner_to_lanes.count(lane_owner));
    auto& lane = _lanes[_owner_to_lanes[lane_owner]];

    lane.last_chunk_received = is_last_chunk;
    if (!chunk->is_empty()) {
        RETURN_IF_ERROR(lane.processor->push_chunk(state, chunk));
    }

    if (is_last_chunk) {
        RETURN_IF_ERROR(lane.processor->set_finishing(state));
    }
    return Status::OK();
}

StatusOr<ChunkPtr> MultilaneOperator::_pull_chunk_from_lane(RuntimeState* state, Lane& lane, bool passthrough_mode) {
    auto processor_is_finished = lane.processor->is_finished();
    auto need_send_eof = !passthrough_mode && processor_is_finished && !lane.eof_sent;
    auto need_send_chunk = !processor_is_finished && lane.processor->has_output();

    auto create_eof_chunk = [&lane]() -> auto {
        auto eof_chunk = std::make_shared<Chunk>();
        eof_chunk->owner_info().set_owner_id(lane.lane_owner, true);
        eof_chunk->owner_info().set_passthrough(false);
        lane.eof_sent = true;
        return eof_chunk;
    };

    if (need_send_eof) {
        return create_eof_chunk();
    }

    if (!need_send_chunk) {
        return nullptr;
    }

    ASSIGN_OR_RETURN(auto chunk, lane.processor->pull_chunk(state));

    processor_is_finished = lane.processor->is_finished();
    lane.eof_sent = processor_is_finished;

    if (chunk != nullptr) {
        chunk->owner_info().set_owner_id(lane.lane_owner, processor_is_finished);
        chunk->owner_info().set_passthrough(false);
        return chunk;
    }

    if (processor_is_finished) {
        return create_eof_chunk();
    }
    return nullptr;
}

StatusOr<ChunkPtr> MultilaneOperator::pull_chunk(RuntimeState* state) {
    const auto passthrough_mode = _lane_arbiter->in_passthrough_mode();
    auto preferred = _lane_arbiter->preferred_lane();
    if (preferred.has_value()) {
        auto& lane = _lanes[preferred.value()];
        auto chunk = _pull_chunk_from_lane(state, lane, passthrough_mode);
        if (!chunk.ok() || chunk.value() != nullptr) {
            return chunk;
        }
    }

    for (auto& lane : _lanes) {
        auto chunk = _pull_chunk_from_lane(state, lane, passthrough_mode);
        if (!chunk.ok() || chunk.value() != nullptr) {
            return chunk;
        }
    }

    if (_passthrough_chunk != nullptr) {
        DCHECK(passthrough_mode && _can_passthrough);
        _passthrough_chunk->owner_info().set_passthrough(true);
        return std::move(_passthrough_chunk);
    } else {
        return nullptr;
    }
}

Status MultilaneOperator::reset_lane(RuntimeState* state, LaneOwnerType lane_owner,
                                     const std::vector<ChunkPtr>& chunks) {
    auto lane_id = _lane_arbiter->must_acquire_lane(lane_owner);
    _owner_to_lanes[lane_owner] = lane_id;
    auto& lane = _lanes[lane_id];
    lane.lane_id = lane_id;
    _owner_to_lanes.erase(lane.lane_owner);
    lane.lane_owner = lane_owner;
    lane.last_chunk_received = false;
    lane.eof_sent = false;
    return lane.processor->reset_state(state, chunks);
}

pipeline::OperatorPtr MultilaneOperator::get_internal_op(size_t i) {
    DCHECK(i >= 0 && i < _lanes.size());
    return _lanes[i].processor;
}

const pipeline::LocalRFWaitingSet& MultilaneOperator::rf_waiting_set() const {
    return _lanes[0].processor->rf_waiting_set();
}

RuntimeFilterProbeCollector* MultilaneOperator::runtime_bloom_filters() {
    return _lanes[0].processor->runtime_bloom_filters();
}

const RuntimeFilterProbeCollector* MultilaneOperator::runtime_bloom_filters() const {
    return _lanes[0].processor->runtime_bloom_filters();
}

void MultilaneOperator::set_precondition_ready(RuntimeState* state) {
    for (auto& lane : _lanes) {
        lane.processor->set_precondition_ready(state);
    }
}

MultilaneOperatorFactory::MultilaneOperatorFactory(int32_t id, const OperatorFactoryPtr& factory, size_t num_lanes)
        : pipeline::OperatorFactory(id, strings::Substitute("ml_$0", factory->get_raw_name()), factory->plan_node_id()),
          _factory(factory),
          _num_lanes(num_lanes) {}

Status MultilaneOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(pipeline::OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_factory->prepare(state));
    return Status::OK();
}

void MultilaneOperatorFactory::close(RuntimeState* state) {
    _factory->close(state);
    pipeline::OperatorFactory::close(state);
}

pipeline::OperatorPtr MultilaneOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    pipeline::Operators processors;
    processors.reserve(_num_lanes);
    for (auto i = 0; i < _num_lanes; ++i) {
        processors.push_back(
                _factory->create(degree_of_parallelism * _num_lanes, i * degree_of_parallelism + driver_sequence));
    }
    for (auto& operators : processors) {
        operators->set_runtime_filter_probe_sequence(driver_sequence);
    }
    auto op = std::make_shared<MultilaneOperator>(this, driver_sequence, _num_lanes, std::move(processors),
                                                  _can_passthrough);
    return op;
}

} // namespace starrocks::query_cache
