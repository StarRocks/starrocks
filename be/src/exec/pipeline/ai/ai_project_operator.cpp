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

#include "exec/pipeline/ai/ai_project_operator.h"

#include <utility>

#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

AISinkOperator::AISinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                               int32_t degree_of_parallelism, std::shared_ptr<AIProjectProcessor> processor)
        : Operator(factory, id, "ai_sink", plan_node_id, false, driver_sequence),
          _processor(std::move(processor)),
          _degree_of_parallelism(degree_of_parallelism) {}

Status AISinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_processor->configure(_degree_of_parallelism));
    RETURN_IF_ERROR(_processor->configuration_status());
    if (!state->enable_event_scheduler()) {
        return Status::OK();
    }
    return _processor->input_buffer()->attach_sink_observer(_driver_sequence, state, observer());
}

void AISinkOperator::close(RuntimeState* state) {
    _pending_chunk.reset();
    Operator::close(state);
}

bool AISinkOperator::need_input() const {
    if (!_status.ok() || _is_finished) {
        return false;
    }
    static_cast<void>(_advance_pending());
    return _status.ok() && !_is_finishing && !_is_finished && _pending_chunk == nullptr;
}

bool AISinkOperator::pending_finish() const {
    // Async work captures the fragment-shared processor, never this sink.
    // The paired source owns the fragment-lifetime barrier.
    return false;
}

StatusOr<ChunkPtr> AISinkOperator::pull_chunk(RuntimeState*) {
    return Status::InternalError("Shouldn't pull chunk from AI sink operator");
}

Status AISinkOperator::push_chunk(RuntimeState*, const ChunkPtr& chunk) {
    if (!_status.ok()) {
        return _status;
    }
    if (_is_finishing || _is_finished) {
        return Status::InternalError("Cannot push a chunk after AI sink finishing");
    }
    if (chunk == nullptr) {
        return Status::InvalidArgument("Cannot push a null chunk into AI sink");
    }
    if (_pending_chunk != nullptr) {
        return Status::InternalError("Cannot push a chunk while AI sink has pending input");
    }

    auto admitted = _processor->input_buffer()->try_put(_driver_sequence, chunk);
    if (!admitted.ok()) {
        return _record_status(admitted.status());
    }
    if (admitted.value()) {
        return Status::OK();
    }

    auto source_finished = _processor->input_buffer()->lane_source_finished(_driver_sequence);
    if (!source_finished.ok()) {
        return _record_status(source_finished.status());
    }
    if (source_finished.value()) {
        _is_finished = true;
    } else {
        _pending_chunk = chunk;
    }
    return Status::OK();
}

Status AISinkOperator::set_finishing(RuntimeState*) {
    if (_is_finished) {
        return _status;
    }
    _is_finishing = true;
    return _advance_pending();
}

Status AISinkOperator::set_finished(RuntimeState*) {
    return _force_finish();
}

Status AISinkOperator::set_cancelled(RuntimeState*) {
    return _force_finish();
}

Status AISinkOperator::_advance_pending() const {
    if (!_status.ok() || _is_finished) {
        return _status;
    }

    if (_pending_chunk != nullptr) {
        auto admitted = _processor->input_buffer()->try_put(_driver_sequence, _pending_chunk);
        if (!admitted.ok()) {
            return _record_status(admitted.status());
        }
        if (admitted.value()) {
            _pending_chunk.reset();
        } else {
            auto source_finished = _processor->input_buffer()->lane_source_finished(_driver_sequence);
            if (!source_finished.ok()) {
                return _record_status(source_finished.status());
            }
            if (!source_finished.value()) {
                return Status::OK();
            }
            _pending_chunk.reset();
            _is_finished = true;
            return Status::OK();
        }
    }

    if (_is_finishing && !_sink_eos) {
        const Status status = _processor->input_buffer()->set_sink_eos(_driver_sequence);
        if (!status.ok()) {
            return _record_status(status);
        }
        _sink_eos = true;
        _is_finished = true;
    }
    return Status::OK();
}

Status AISinkOperator::_record_status(const Status& status) const {
    if (status.ok()) {
        return Status::OK();
    }
    _status.update(status);
    _pending_chunk.reset();
    _is_finished = true;
    static_cast<void>(_processor->set_status(_driver_sequence, status));
    return status;
}

Status AISinkOperator::_force_finish() const {
    if (_is_finished) {
        return _status;
    }
    _is_finishing = true;
    // set_finished/set_cancelled are forced shutdown paths. Unlike graceful
    // set_finishing(), they must not publish retained input after shutdown.
    _pending_chunk.reset();
    if (!_sink_eos) {
        const Status status = _processor->input_buffer()->set_sink_eos(_driver_sequence);
        if (!status.ok()) {
            return _record_status(status);
        }
        _sink_eos = true;
    }
    _is_finished = true;
    return Status::OK();
}

AISinkOperatorFactory::AISinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                             std::shared_ptr<AIProjectProcessor> processor)
        : OperatorFactory(id, "ai_sink", plan_node_id), _processor(std::move(processor)) {}

OperatorPtr AISinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<AISinkOperator>(this, _id, _plan_node_id, driver_sequence, degree_of_parallelism,
                                            _processor);
}

AISourceOperator::AISourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                   int32_t degree_of_parallelism, std::shared_ptr<AIProjectProcessor> processor)
        : SourceOperator(factory, id, "ai_source", plan_node_id, false, driver_sequence),
          _processor(std::move(processor)),
          _degree_of_parallelism(degree_of_parallelism) {}

Status AISourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_processor->prepare(state, _degree_of_parallelism));
    RETURN_IF_ERROR(_processor->configuration_status());
    if (!state->enable_event_scheduler()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_processor->attach_source_observer(_driver_sequence, state, observer()));
    return _processor->input_buffer()->attach_source_observer(_driver_sequence, state, observer());
}

void AISourceOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool AISourceOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    if (!_processor->status(_driver_sequence).ok()) {
        return true;
    }
    return _processor->has_output(_driver_sequence) || _processor->can_process(_driver_sequence);
}

bool AISourceOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    if (!_processor->status(_driver_sequence).ok()) {
        return false;
    }
    auto finished = _processor->lane_finished(_driver_sequence);
    return finished.ok() && finished.value();
}

bool AISourceOperator::pending_finish() const {
    return _processor->pending_finish(_driver_sequence);
}

StatusOr<ChunkPtr> AISourceOperator::pull_chunk(RuntimeState* state) {
    if (_is_finished) {
        return nullptr;
    }
    RETURN_IF_ERROR(_processor->status(_driver_sequence));
    if (_processor->has_output(_driver_sequence)) {
        return _processor->pull_chunk(state, _driver_sequence);
    }
    if (!_processor->can_process(_driver_sequence)) {
        return nullptr;
    }

    RETURN_IF_ERROR(_processor->try_process(state, _driver_sequence));
    RETURN_IF_ERROR(_processor->status(_driver_sequence));
    if (_processor->has_output(_driver_sequence)) {
        return _processor->pull_chunk(state, _driver_sequence);
    }
    return nullptr;
}

Status AISourceOperator::set_finished(RuntimeState*) {
    if (_is_finished) {
        return _finish_status;
    }
    _finish_status = _processor->set_source_finished(_driver_sequence);
    _is_finished = true;
    return _finish_status;
}

Status AISourceOperator::set_cancelled(RuntimeState* state) {
    return set_finished(state);
}

AISourceOperatorFactory::AISourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                 std::shared_ptr<AIProjectProcessor> processor)
        : SourceOperatorFactory(id, "ai_source", plan_node_id), _processor(std::move(processor)) {}

OperatorPtr AISourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<AISourceOperator>(this, _id, _plan_node_id, driver_sequence, degree_of_parallelism,
                                              _processor);
}

Status AISourceOperatorFactory::prepare(RuntimeState* state) {
    return SourceOperatorFactory::prepare(state);
}

void AISourceOperatorFactory::close(RuntimeState* state) {
    _processor->close(state);
    SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline
