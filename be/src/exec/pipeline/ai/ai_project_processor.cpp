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

#include "exec/pipeline/ai/ai_project_processor.h"

#include <algorithm>
#include <new>
#include <optional>
#include <utility>

#include "base/logging.h"
#include "base/testutil/sync_point.h"
#include "base/utility/scoped_cleanup.h"
#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

enum class AIProjectProcessor::TerminalKind : uint8_t { NONE, ROW_FAILURE, CANCELLED, DEADLINE, SHUTDOWN };

struct AIProjectProcessor::ResultCell {
    uint64_t task_id = 0;
    bool completed = false;
    std::optional<AITaskSuccess> success;
    std::unique_ptr<AIProjectTaskHandle> handle;
};

struct AIProjectProcessor::OutputState {
    SlotId slot_id = 0;
    bool replace_existing = false;
    std::vector<ResultCell> rows;
};

struct AIProjectProcessor::ActiveSubchunk {
    AIProjectPreparedSubchunk prepared;
    std::vector<OutputState> outputs;
    size_t remaining = 0;
    bool submissions_closed = false;
    bool ready = false;
};

struct AIProjectProcessor::Lane {
    mutable std::mutex mutex;
    ChunkPtr input_chunk;
    size_t input_offset = 0;
    std::shared_ptr<ActiveSubchunk> active;
    Status terminal_status;
    TerminalKind terminal_kind = TerminalKind::NONE;
    size_t outstanding_callbacks = 0;
    size_t submissions_in_progress = 0;
    bool building = false;
    bool source_finished = false;
    QueryContextLifetimeWeakPtr query_lifetime;
    PipeObservable observable;
};

struct AIProjectProcessor::Submission {
    size_t output_index = 0;
    size_t row_index = 0;
    uint64_t task_id = 0;
};

StatusOr<std::shared_ptr<AIProjectProcessor>> AIProjectProcessor::create(
        std::shared_ptr<AIChunkBuffer> input_buffer, std::shared_ptr<AIProjectProjection> projection,
        std::shared_ptr<AIProjectTaskSubmitter> submitter, AIRuntimeConfig config) {
    if (input_buffer == nullptr || projection == nullptr || submitter == nullptr) {
        return Status::InvalidArgument("AI project processor dependencies are required");
    }
    RETURN_IF_ERROR(config.validate());
    try {
        return std::shared_ptr<AIProjectProcessor>(new AIProjectProcessor(
                std::move(input_buffer), std::move(projection), std::move(submitter), std::move(config)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI project processor");
    }
}

AIProjectProcessor::AIProjectProcessor(std::shared_ptr<AIChunkBuffer> input_buffer,
                                       std::shared_ptr<AIProjectProjection> projection,
                                       std::shared_ptr<AIProjectTaskSubmitter> submitter, AIRuntimeConfig config)
        : _input_buffer(std::move(input_buffer)),
          _projection(std::move(projection)),
          _submitter(std::move(submitter)),
          _memory(_submitter->memory_context()),
          _config(std::move(config)) {}

Status AIProjectProcessor::configure(int32_t dop) {
    std::lock_guard lock(_configure_mutex);
    if (!_configuration_status.ok()) {
        return _configuration_status;
    }
    if (_configured_dop != 0) {
        if (_configured_dop == dop) {
            return Status::OK();
        }
        _configuration_status =
                Status::InternalError("AI project processor is already configured with a different DOP");
        return _configuration_status;
    }
    if (dop <= 0) {
        _configuration_status = Status::InvalidArgument("AI project processor DOP must be positive");
        return _configuration_status;
    }

    _configuration_status = _input_buffer->configure(dop);
    if (!_configuration_status.ok()) {
        return _configuration_status;
    }
    try {
        _lanes.reserve(dop);
        for (int32_t i = 0; i < dop; ++i) {
            _lanes.emplace_back(ai_allocate_shared<Lane>(_memory));
        }
    } catch (const std::bad_alloc&) {
        _lanes.clear();
        _configuration_status = Status::MemoryLimitExceeded("Failed to allocate AI project processor lanes");
        return _configuration_status;
    }
    _configured_dop = dop;
    return Status::OK();
}

Status AIProjectProcessor::configuration_status() const {
    std::lock_guard lock(_configure_mutex);
    return _configuration_status;
}

Status AIProjectProcessor::prepare(RuntimeState* state, int32_t dop) {
    if (state == nullptr) {
        return Status::InvalidArgument("AI project processor runtime state cannot be null");
    }

    std::lock_guard lock(_lifecycle_mutex);
    if (_closed) {
        return Status::InternalError("AI project processor is already closed");
    }
    if (_prepare_attempted) {
        if (_lifecycle_state != state || _lifecycle_dop != dop) {
            return Status::InternalError("AI project processor was prepared with a different runtime");
        }
        return _lifecycle_status;
    }
    if (!_lifecycle_status.ok()) {
        return _lifecycle_status;
    }

    _lifecycle_status = configure(dop);
    if (!_lifecycle_status.ok()) {
        return _lifecycle_status;
    }

    _prepare_attempted = true;
    _lifecycle_state = state;
    _lifecycle_dop = dop;
    try {
        _lifecycle_status = _projection->prepare(state, dop);
    } catch (const std::bad_alloc&) {
        _lifecycle_status = Status::MemoryLimitExceeded("Failed to prepare AI project projection");
    } catch (...) {
        _lifecycle_status = Status::InternalError("Failed to prepare AI project projection");
    }
    return _lifecycle_status;
}

void AIProjectProcessor::close(RuntimeState* state) {
    RuntimeState* lifecycle_state = nullptr;
    {
        std::lock_guard lock(_lifecycle_mutex);
        if (_closed) {
            return;
        }
        _closed = true;
        lifecycle_state = _lifecycle_state != nullptr ? _lifecycle_state : state;
    }
    try {
        _input_buffer->close();
    } catch (...) {
    }
    try {
        // The projection owns the transferred ExprContexts even if operator
        // instantiation fails before prepare(). ExprContext::close is safe for
        // an unprepared context, so teardown must not depend on prepare().
        _projection->close(lifecycle_state);
    } catch (...) {
    }
}

StatusOr<std::shared_ptr<AIProjectProcessor::Lane>> AIProjectProcessor::_lane(int32_t driver_sequence) const {
    std::lock_guard lock(_configure_mutex);
    if (_configured_dop == 0) {
        if (!_configuration_status.ok()) {
            return _configuration_status;
        }
        return Status::InternalError("AI project processor is not configured");
    }
    if (driver_sequence < 0 || driver_sequence >= _configured_dop) {
        return Status::InvalidArgument("AI project processor driver sequence is out of range");
    }
    return _lanes[driver_sequence];
}

Status AIProjectProcessor::try_process(RuntimeState* state, int32_t driver_sequence) {
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));

    ChunkPtr input;
    size_t offset = 0;
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE ||
            lane->active != nullptr || lane->building) {
            return Status::OK();
        }
        lane->building = true;
        input = lane->input_chunk;
        offset = lane->input_offset;
    }
    DeferOp clear_building([lane] {
        std::lock_guard lock(lane->mutex);
        lane->building = false;
    });

    Status process_status;
    TRY_CATCH_ALLOC_SCOPE_START();

    if (input == nullptr) {
        auto got = _input_buffer->try_get(driver_sequence, &input);
        if (!got.ok()) {
            std::lock_guard lock(lane->mutex);
            lane->terminal_status.update(got.status());
            return got.status();
        }
        if (!got.value()) {
            return Status::OK();
        }

        bool discard = false;
        {
            std::lock_guard lock(lane->mutex);
            if (lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE) {
                discard = true;
            } else {
                lane->input_chunk = input;
                lane->input_offset = 0;
                offset = 0;
            }
        }
        if (discard) {
            input.reset();
            return Status::OK();
        }
    }

    const size_t total_rows = input->num_rows();
    if (offset > total_rows) {
        std::lock_guard lock(lane->mutex);
        lane->terminal_status.update(Status::InternalError("AI project input offset exceeds chunk rows"));
        return lane->terminal_status;
    }
    const size_t rows = std::min(static_cast<size_t>(_config.sub_chunk_size), total_rows - offset);
    ChunkPtr slice = input->clone_empty_with_slot(rows);
    if (rows > 0) {
        slice->append(*input, offset, rows);
    }
    const bool final_slice = offset + rows == total_rows;
    slice->owner_info().set_owner_id(input->owner_info().owner_id(),
                                     input->owner_info().is_last_chunk() && final_slice);
    slice->owner_info().set_passthrough(input->owner_info().is_passthrough());

    ChunkPtr consumed_input;
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE) {
            return Status::OK();
        }
        lane->input_offset = offset + rows;
        if (final_slice) {
            consumed_input = std::move(lane->input_chunk);
            lane->input_offset = 0;
        }
    }
    consumed_input.reset();
    try {
        process_status = _prepare_and_submit(state, driver_sequence, lane, std::move(slice));
    } catch (const std::bad_alloc&) {
        process_status = Status::MemoryLimitExceeded("Failed to prepare AI project subchunk");
    } catch (const std::runtime_error&) {
        process_status = Status::RuntimeError("AI project projection raised a runtime error");
    } catch (...) {
        process_status = Status::InternalError("Unexpected AI project subchunk preparation failure");
    }
    if (!process_status.ok()) {
        std::lock_guard lock(lane->mutex);
        lane->terminal_status.update(process_status);
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return process_status;
}

Status AIProjectProcessor::_prepare_and_submit(RuntimeState* state, int32_t driver_sequence,
                                               const std::shared_ptr<Lane>& lane, ChunkPtr slice) {
    auto prepared_or = _projection->prepare_subchunk(state, driver_sequence, slice);
    if (!prepared_or.ok()) {
        std::lock_guard lock(lane->mutex);
        lane->building = false;
        lane->terminal_status.update(prepared_or.status());
        return prepared_or.status();
    }

    auto subchunk = std::make_shared<ActiveSubchunk>();
    subchunk->prepared = std::move(prepared_or).value();
    if (subchunk->prepared.output_chunk == nullptr) {
        std::lock_guard lock(lane->mutex);
        lane->building = false;
        lane->terminal_status.update(Status::InternalError("AI project projection returned a null output chunk"));
        return lane->terminal_status;
    }
    const size_t rows = slice->num_rows();
    if (subchunk->prepared.output_chunk->has_columns() && subchunk->prepared.output_chunk->num_rows() != rows) {
        std::lock_guard lock(lane->mutex);
        lane->building = false;
        lane->terminal_status.update(
                Status::InternalError("AI project projection returned an unexpected number of output rows"));
        return lane->terminal_status;
    }
    subchunk->prepared.output_chunk->owner_info() = slice->owner_info();

    std::vector<Submission> submissions;
    Status immediate_status;
    try {
        subchunk->outputs.reserve(subchunk->prepared.ai_outputs.size());
        for (size_t output_index = 0; output_index < subchunk->prepared.ai_outputs.size(); ++output_index) {
            const AIProjectPreparedOutput& prepared_output = subchunk->prepared.ai_outputs[output_index];
            const bool slot_exists = subchunk->prepared.output_chunk->is_slot_exist(prepared_output.slot_id);
            if (prepared_output.replace_existing && !slot_exists) {
                immediate_status = Status::InternalError("AI project output placeholder is missing");
                break;
            }
            if (!prepared_output.replace_existing && slot_exists) {
                immediate_status = Status::InternalError("AI project output slot conflicts with a projected slot");
                break;
            }
            for (size_t previous = 0; previous < output_index; ++previous) {
                if (subchunk->prepared.ai_outputs[previous].slot_id == prepared_output.slot_id) {
                    immediate_status = Status::InternalError("AI project contains duplicate AI output slots");
                    break;
                }
            }
            if (!immediate_status.ok()) {
                break;
            }
            if (prepared_output.input.rows.size() != rows) {
                immediate_status =
                        Status::InternalError("AI project expression returned an unexpected number of row inputs");
                break;
            }

            OutputState output;
            output.slot_id = prepared_output.slot_id;
            output.replace_existing = prepared_output.replace_existing;
            output.rows.resize(rows);
            for (size_t row_index = 0; row_index < rows; ++row_index) {
                ResultCell& cell = output.rows[row_index];
                const AIFunctionRowAction action = prepared_output.input.rows[row_index].action;
                if (action == AIFunctionRowAction::SQL_NULL) {
                    cell.completed = true;
                } else if (action == AIFunctionRowAction::TERMINAL_ROW_FAILURE) {
                    cell.completed = true;
                    if (_config.on_error != "ignore") {
                        immediate_status.update(_row_failure_status());
                    }
                } else {
                    cell.task_id = _next_task_id.fetch_add(1, std::memory_order_relaxed);
                    submissions.emplace_back(
                            Submission{.output_index = output_index, .row_index = row_index, .task_id = cell.task_id});
                    ++subchunk->remaining;
                }
            }
            subchunk->outputs.emplace_back(std::move(output));
        }
    } catch (const std::bad_alloc&) {
        immediate_status = Status::MemoryLimitExceeded("Failed to allocate AI project result slots");
    }

    {
        std::lock_guard lock(lane->mutex);
        lane->building = false;
        if (lane->source_finished) {
            return Status::OK();
        }
        if (!lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE) {
            return _terminal_status(lane->terminal_status, lane->terminal_kind);
        }
        if (!immediate_status.ok()) {
            lane->terminal_status = immediate_status;
            return immediate_status;
        }
        lane->active = subchunk;
        lane->outstanding_callbacks += subchunk->remaining;
        ++lane->submissions_in_progress;
    }

    size_t next_submission = 0;
    Status submission_status;
    try {
        for (; next_submission < submissions.size(); ++next_submission) {
            const Submission& submission = submissions[next_submission];
            bool should_submit = false;
            {
                std::lock_guard lock(lane->mutex);
                ResultCell& cell = subchunk->outputs[submission.output_index].rows[submission.row_index];
                if (!cell.completed && !lane->source_finished && lane->terminal_status.ok() &&
                    lane->terminal_kind == TerminalKind::NONE) {
                    should_submit = true;
                } else if (!cell.completed) {
                    cell.completed = true;
                    --subchunk->remaining;
                    --lane->outstanding_callbacks;
                }
            }
            if (!should_submit) {
                continue;
            }

            const AIProjectPreparedOutput& output = subchunk->prepared.ai_outputs[submission.output_index];
            const AIFunctionRowInput& row = output.input.rows[submission.row_index];
            AIProjectTaskRequest request{.task_id = submission.task_id,
                                         .model = row.model,
                                         .prompt = row.prompt,
                                         .options = output.input.options.get()};
            AITaskCallback callback;
            auto build_callback = [&] {
                // Lane is the process-scoped async completion state. It keeps
                // the barrier and observer valid without retaining the
                // processor or the active subchunk directly.
                callback = [lane, ignore_row_failures = _config.on_error == "ignore", task_id = submission.task_id,
                            output_index = submission.output_index,
                            row_index = submission.row_index](AITaskResult result) mutable noexcept {
                    _complete_task(lane, ignore_row_failures, task_id, output_index, row_index, std::move(result));
                };
            };
            _memory.run_in_physical_scope([](void* opaque) { (*static_cast<decltype(build_callback)*>(opaque))(); },
                                          &build_callback);
            SCOPED_CLEANUP({
                auto clear_callback = [&] { AITaskCallback().swap(callback); };
                _memory.run_in_physical_scope([](void* opaque) { (*static_cast<decltype(clear_callback)*>(opaque))(); },
                                              &clear_callback);
            });

            auto handle_or = _submitter->submit(request, std::move(callback));
            if (!handle_or.ok()) {
                _complete_submit_failure(lane, _config.on_error == "ignore", submission.task_id,
                                         submission.output_index, submission.row_index, handle_or.status());
                continue;
            }
            if (handle_or.value() == nullptr) {
                submission_status = Status::InternalError("AI task submitter returned a null handle");
                break;
            }

            std::unique_ptr<AIProjectTaskHandle> handle_to_cancel;
            {
                std::lock_guard lock(lane->mutex);
                ResultCell& cell = subchunk->outputs[submission.output_index].rows[submission.row_index];
                if (!cell.completed && (lane->source_finished || !lane->terminal_status.ok() ||
                                        lane->terminal_kind != TerminalKind::NONE)) {
                    handle_to_cancel = std::move(handle_or).value();
                } else {
                    cell.handle = std::move(handle_or).value();
                }
            }
            if (handle_to_cancel != nullptr) {
                handle_to_cancel->cancel();
            }
        }
    } catch (const std::bad_alloc&) {
        submission_status = Status::MemoryLimitExceeded("Failed to submit AI function task");
    } catch (const std::runtime_error&) {
        submission_status = Status::RuntimeError("AI function task submission raised a runtime error");
    } catch (...) {
        submission_status = Status::InternalError("Unexpected AI function task submission failure");
    }

    bool notify = false;
    {
        std::lock_guard lock(lane->mutex);
        if (!submission_status.ok()) {
            lane->terminal_status.update(submission_status);
            for (size_t index = next_submission; index < submissions.size(); ++index) {
                const Submission& submission = submissions[index];
                ResultCell& cell = subchunk->outputs[submission.output_index].rows[submission.row_index];
                if (!cell.completed) {
                    cell.completed = true;
                    DCHECK_GT(subchunk->remaining, 0);
                    DCHECK_GT(lane->outstanding_callbacks, 0);
                    --subchunk->remaining;
                    --lane->outstanding_callbacks;
                }
            }
        }
        subchunk->submissions_closed = true;
        if (lane->active == subchunk && !lane->source_finished &&
            (!lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE)) {
            notify = true;
        } else if (lane->active == subchunk && !lane->source_finished && lane->terminal_status.ok() &&
                   lane->terminal_kind == TerminalKind::NONE && subchunk->remaining == 0) {
            subchunk->ready = true;
            notify = true;
        }
    }
    if (!submission_status.ok()) {
        _dispose_subchunk_handles(lane, subchunk, true);
    }
    if (notify) {
        _notify_source(&lane->observable);
    }
    std::shared_ptr<QueryContextLifetime> drain_lifetime_pin;
    {
        std::lock_guard lock(lane->mutex);
        DCHECK_GT(lane->submissions_in_progress, 0);
        if (lane->source_finished && lane->submissions_in_progress == 1 && lane->outstanding_callbacks == 0) {
            drain_lifetime_pin = lane->query_lifetime.lock();
        }
        --lane->submissions_in_progress;
    }
    if (drain_lifetime_pin != nullptr) {
        _notify_source(&lane->observable);
    }
    return submission_status;
}

void AIProjectProcessor::_complete_submit_failure(const std::shared_ptr<Lane>& lane, bool ignore_row_failures,
                                                  uint64_t task_id, size_t output_index, size_t row_index,
                                                  const Status& status) {
    if (status.is_cancelled()) {
        _complete_task(lane, ignore_row_failures, task_id, output_index, row_index,
                       AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED});
        return;
    }
    if (status.is_time_out()) {
        _complete_task(lane, ignore_row_failures, task_id, output_index, row_index,
                       AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE});
        return;
    }
    if (status.is_shutdown() || status.is_service_unavailable()) {
        _complete_task(lane, ignore_row_failures, task_id, output_index, row_index,
                       AILifecycleCancelled{.reason = AILifecycleReason::SHUTDOWN});
        return;
    }

    AISanitizedFailureClass failure_class = AISanitizedFailureClass::LOCAL_REQUEST;
    if (status.is_mem_limit_exceeded() || status.is_resource_busy()) {
        failure_class = AISanitizedFailureClass::LOCAL_RESOURCE;
    }
    _complete_task(lane, ignore_row_failures, task_id, output_index, row_index,
                   AISanitizedRowFailure{.failure_class = failure_class});
}

void AIProjectProcessor::_complete_task(const std::shared_ptr<Lane>& lane, bool ignore_row_failures, uint64_t task_id,
                                        size_t output_index, size_t row_index, AITaskResult result) noexcept {
    bool notify = false;
    {
        // Destroy result-owned async memory before releasing the callback
        // barrier, including when result publication itself throws.
        AITaskResult local_result(std::move(result));
        try {
            std::lock_guard lock(lane->mutex);
            ActiveSubchunk* subchunk = lane->active.get();
            if (!lane->source_finished && subchunk != nullptr && output_index < subchunk->outputs.size() &&
                row_index < subchunk->outputs[output_index].rows.size()) {
                ResultCell& cell = subchunk->outputs[output_index].rows[row_index];
                if (!cell.completed && cell.task_id == task_id) {
                    TEST_SYNC_POINT("AIProjectProcessor::_complete_task:before_publish");
                    bool published_terminal = false;
                    if (auto* success = std::get_if<AITaskSuccess>(&local_result);
                        success != nullptr && lane->terminal_status.ok() && lane->terminal_kind == TerminalKind::NONE) {
                        cell.success.emplace(std::move(*success));
                    } else if (std::holds_alternative<AISanitizedRowFailure>(local_result)) {
                        if (!ignore_row_failures && lane->terminal_status.ok() &&
                            lane->terminal_kind == TerminalKind::NONE) {
                            lane->terminal_kind = TerminalKind::ROW_FAILURE;
                            published_terminal = true;
                        }
                    } else if (auto* lifecycle = std::get_if<AILifecycleCancelled>(&local_result);
                               lifecycle != nullptr && lane->terminal_status.ok() &&
                               lane->terminal_kind == TerminalKind::NONE) {
                        switch (lifecycle->reason) {
                        case AILifecycleReason::CANCELLED:
                            lane->terminal_kind = TerminalKind::CANCELLED;
                            break;
                        case AILifecycleReason::DEADLINE:
                            lane->terminal_kind = TerminalKind::DEADLINE;
                            break;
                        case AILifecycleReason::SHUTDOWN:
                            lane->terminal_kind = TerminalKind::SHUTDOWN;
                            break;
                        }
                        published_terminal = true;
                    }

                    // No operation after this marker may throw before the
                    // subchunk completion count is settled.
                    cell.completed = true;
                    DCHECK_GT(subchunk->remaining, 0);
                    --subchunk->remaining;

                    if (published_terminal) {
                        notify = true;
                    } else if (lane->terminal_status.ok() && lane->terminal_kind == TerminalKind::NONE &&
                               subchunk->submissions_closed && subchunk->remaining == 0) {
                        subchunk->ready = true;
                        notify = true;
                    }
                }
            }
        } catch (...) {
            LOG(WARNING) << "AI project task completion publication threw an exception";
            try {
                std::lock_guard lock(lane->mutex);
                ActiveSubchunk* subchunk = lane->active.get();
                if (!lane->source_finished && subchunk != nullptr && output_index < subchunk->outputs.size() &&
                    row_index < subchunk->outputs[output_index].rows.size()) {
                    ResultCell& cell = subchunk->outputs[output_index].rows[row_index];
                    if (!cell.completed && cell.task_id == task_id) {
                        cell.success.reset();
                        if (lane->terminal_status.ok() && lane->terminal_kind == TerminalKind::NONE) {
                            lane->terminal_kind = TerminalKind::ROW_FAILURE;
                        }
                        cell.completed = true;
                        DCHECK_GT(subchunk->remaining, 0);
                        --subchunk->remaining;
                        notify = true;
                    }
                }
            } catch (...) {
                LOG(WARNING) << "AI project task completion fallback failed";
            }
        }
    }

    if (notify) {
        _notify_source(&lane->observable);
    }
    std::shared_ptr<QueryContextLifetime> drain_lifetime_pin;
    try {
        std::lock_guard lock(lane->mutex);
        DCHECK_GT(lane->outstanding_callbacks, 0);
        if (lane->source_finished && lane->submissions_in_progress == 0 && lane->outstanding_callbacks == 1) {
            drain_lifetime_pin = lane->query_lifetime.lock();
        }
        --lane->outstanding_callbacks;
    } catch (...) {
        LOG(WARNING) << "AI project task completion barrier release failed";
        return;
    }
    if (drain_lifetime_pin != nullptr) {
        _notify_source(&lane->observable);
    }
}

bool AIProjectProcessor::has_output(int32_t driver_sequence) const {
    auto lane_or = _lane(driver_sequence);
    if (!lane_or.ok()) {
        return true;
    }
    const std::shared_ptr<Lane>& lane = lane_or.value();
    std::lock_guard lock(lane->mutex);
    if (lane->source_finished) {
        return false;
    }
    return !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE ||
           (lane->active != nullptr && lane->active->ready);
}

bool AIProjectProcessor::can_process(int32_t driver_sequence) const {
    auto lane_or = _lane(driver_sequence);
    if (!lane_or.ok()) {
        return true;
    }
    const std::shared_ptr<Lane>& lane = lane_or.value();
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE ||
            lane->active != nullptr || lane->building) {
            return false;
        }
        if (lane->input_chunk != nullptr) {
            return true;
        }
    }
    auto has_chunk = _input_buffer->lane_has_chunk(driver_sequence);
    return has_chunk.ok() ? has_chunk.value() : true;
}

StatusOr<ChunkPtr> AIProjectProcessor::pull_chunk(RuntimeState*, int32_t driver_sequence) {
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));

    std::shared_ptr<ActiveSubchunk> subchunk;
    Status driver_status;
    TerminalKind terminal_kind = TerminalKind::NONE;
    {
        std::lock_guard lock(lane->mutex);
        if (!lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE) {
            driver_status = lane->terminal_status;
            terminal_kind = lane->terminal_kind;
            subchunk = lane->active;
        } else if (lane->active == nullptr || !lane->active->ready) {
            return nullptr;
        } else {
            subchunk = lane->active;
        }
    }

    if (!driver_status.ok() || terminal_kind != TerminalKind::NONE) {
        _dispose_subchunk_handles(lane, subchunk, true);
        return _terminal_status(driver_status, terminal_kind);
    }

    auto output = _materialize(subchunk);
    if (!output.ok()) {
        {
            std::lock_guard lock(lane->mutex);
            lane->terminal_status.update(output.status());
        }
        _dispose_subchunk_handles(lane, subchunk, true);
        return output.status();
    }
    {
        std::lock_guard lock(lane->mutex);
        if (lane->active == subchunk) {
            lane->active.reset();
        }
    }
    _dispose_subchunk_handles(lane, subchunk, false);
    return std::move(output).value();
}

Status AIProjectProcessor::status(int32_t driver_sequence) const {
    auto lane_or = _lane(driver_sequence);
    if (!lane_or.ok()) {
        return lane_or.status();
    }
    const std::shared_ptr<Lane>& lane = lane_or.value();
    Status driver_status;
    TerminalKind terminal_kind;
    {
        std::lock_guard lock(lane->mutex);
        driver_status = lane->terminal_status;
        terminal_kind = lane->terminal_kind;
    }
    return _terminal_status(driver_status, terminal_kind);
}

Status AIProjectProcessor::set_status(int32_t driver_sequence, const Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));

    bool notify = false;
    {
        std::lock_guard lock(lane->mutex);
        if (!lane->source_finished && lane->terminal_status.ok() && lane->terminal_kind == TerminalKind::NONE) {
            lane->terminal_status = status;
            notify = true;
        }
    }
    if (notify) {
        _notify_source(&lane->observable);
    }
    return Status::OK();
}

StatusOr<bool> AIProjectProcessor::lane_finished(int32_t driver_sequence) const {
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished) {
            return true;
        }
        if (!lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE || lane->building ||
            lane->input_chunk != nullptr || lane->active != nullptr) {
            return false;
        }
    }
    return _input_buffer->lane_finished(driver_sequence);
}

bool AIProjectProcessor::pending_finish(int32_t driver_sequence) const {
    auto lane_or = _lane(driver_sequence);
    if (!lane_or.ok()) {
        return false;
    }
    const std::shared_ptr<Lane>& lane = lane_or.value();
    std::lock_guard lock(lane->mutex);
    return lane->submissions_in_progress > 0 || lane->outstanding_callbacks > 0;
}

Status AIProjectProcessor::set_source_finished(int32_t driver_sequence) {
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));

    ChunkPtr input_to_release;
    ChunkPtr output_to_release;
    std::shared_ptr<ActiveSubchunk> active_to_release;
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished) {
            return Status::OK();
        }
        lane->source_finished = true;
        lane->building = false;
        input_to_release = std::move(lane->input_chunk);
        active_to_release = std::move(lane->active);
        if (active_to_release != nullptr) {
            output_to_release = std::move(active_to_release->prepared.output_chunk);
        }
    }

    output_to_release.reset();
    Status buffer_status = _input_buffer->set_source_finished(driver_sequence);
    _dispose_subchunk_handles(lane, active_to_release, true);
    _release_subchunk_results(lane, active_to_release);
    active_to_release.reset();
    input_to_release.reset();
    return buffer_status;
}

Status AIProjectProcessor::attach_source_observer(int32_t driver_sequence, RuntimeState* state,
                                                  PipelineObserver* observer) {
    if (state == nullptr || observer == nullptr) {
        return Status::InvalidArgument("AI project source observer and runtime state cannot be null");
    }
    QueryContextLifetimeWeakPtr query_lifetime = state->query_ctx_lifetime();
    auto query_lifetime_pin = query_lifetime.lock();
    if (query_lifetime_pin == nullptr) {
        return Status::InvalidArgument("AI project source observer requires a live QueryContext");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));
    {
        std::lock_guard lock(lane->mutex);
        lane->query_lifetime = std::move(query_lifetime);
    }
    lane->observable.attach_source_observer(state, observer);
    return Status::OK();
}

StatusOr<ChunkPtr> AIProjectProcessor::_materialize(const std::shared_ptr<ActiveSubchunk>& subchunk) {
    ChunkPtr output;
    TRY_CATCH_ALLOC_SCOPE_START();
    output = subchunk->prepared.output_chunk;
    for (OutputState& result : subchunk->outputs) {
        auto values = BinaryColumn::create();
        auto nulls = NullColumn::create();
        values->reserve(result.rows.size());
        nulls->reserve(result.rows.size());
        for (ResultCell& cell : result.rows) {
            if (cell.success.has_value()) {
                values->append(cell.success->content());
                nulls->append(0);
            } else {
                values->append_default();
                nulls->append(1);
            }
        }
        ColumnPtr result_column = NullableColumn::create(std::move(values), std::move(nulls));
        if (result.replace_existing) {
            if (!output->is_slot_exist(result.slot_id)) {
                return Status::InternalError("AI project output placeholder is missing during materialization");
            }
            output->update_column(std::move(result_column), result.slot_id);
        } else {
            output->append_column(std::move(result_column), result.slot_id);
        }
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return output;
}

Status AIProjectProcessor::_terminal_status(const Status& driver_status, TerminalKind terminal_kind) {
    if (!driver_status.ok()) {
        return driver_status;
    }
    switch (terminal_kind) {
    case TerminalKind::NONE:
        return Status::OK();
    case TerminalKind::ROW_FAILURE:
        return _row_failure_status();
    case TerminalKind::CANCELLED:
        return Status::Cancelled("AI function request was cancelled");
    case TerminalKind::DEADLINE:
        return Status::TimedOut("AI function request deadline exceeded");
    case TerminalKind::SHUTDOWN:
        return Status::Shutdown("AI function runtime is shutting down");
    }
    return Status::InternalError("Unknown AI function terminal state");
}

Status AIProjectProcessor::_row_failure_status() {
    return Status::InternalError("AI function request failed");
}

void AIProjectProcessor::_dispose_subchunk_handles(const std::shared_ptr<Lane>& lane,
                                                   const std::shared_ptr<ActiveSubchunk>& subchunk,
                                                   bool cancel_unfinished) noexcept {
    if (lane == nullptr || subchunk == nullptr) {
        return;
    }
    for (OutputState& output : subchunk->outputs) {
        for (ResultCell& cell : output.rows) {
            std::unique_ptr<AIProjectTaskHandle> handle;
            bool should_cancel = false;
            {
                std::lock_guard lock(lane->mutex);
                should_cancel = cancel_unfinished && !cell.completed;
                handle = std::move(cell.handle);
            }
            if (handle != nullptr && should_cancel) {
                handle->cancel();
            }
            handle.reset();
        }
    }
}

void AIProjectProcessor::_release_subchunk_results(const std::shared_ptr<Lane>& lane,
                                                   const std::shared_ptr<ActiveSubchunk>& subchunk) noexcept {
    if (lane == nullptr || subchunk == nullptr) {
        return;
    }
    for (OutputState& output : subchunk->outputs) {
        for (ResultCell& cell : output.rows) {
            std::optional<AITaskSuccess> success;
            {
                std::lock_guard lock(lane->mutex);
                success.swap(cell.success);
            }
            success.reset();
        }
    }
}

void AIProjectProcessor::_notify_source(PipeObservable* observable) {
    // Pipeline observers follow the same no-throw trigger contract as the rest
    // of main's PipeObservable users. DeferOp deliberately invokes them from a
    // noexcept destructor.
    auto notify = observable->defer_notify_source();
}

} // namespace starrocks::pipeline
