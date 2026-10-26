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
#include "column/column_helper.h"
#include "compute_env/query/query_runtime_state.h"
#include "exec/pipeline/ai/ai_project_row_work.h"
#include "exprs/ai/ai_aggregate_state.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

namespace starrocks::pipeline {

enum class AIProjectProcessor::TerminalKind : uint8_t { NONE, ROW_FAILURE, CANCELLED, DEADLINE, SHUTDOWN };

struct AIProjectProcessor::ResultCell {
    std::optional<AIProjectRowWork> work;
    bool completed = false; // Driver only, unlike RequestSlot::publication_closed.
    std::optional<AITaskSuccess> success;
    std::unique_ptr<AIProjectTaskHandle> completed_handle;
};

struct AIProjectProcessor::OutputState {
    SlotId slot_id = 0;
    bool replace_existing = false;
    TypeDescriptor result_type;
    AIFunctionResultKind result_kind = AIFunctionResultKind::STRING;
    std::vector<ResultCell> rows;
};

struct AIProjectProcessor::ActiveSubchunk {
    AIProjectPreparedSubchunk prepared;
    std::vector<OutputState> outputs;
    size_t remaining = 0;
    size_t next_row = 0;
    bool runnable = true; // Published under lane mutex by the driver.
    bool ready = false;
};

struct AIProjectProcessor::RequestSlot {
    uint64_t task_id = 0;
    size_t output_index = 0;
    size_t row_index = 0;
    uint64_t ordinal = 0;
    bool used = false;
    bool aggregate = false;
    bool discarded = false;
    bool submission_returned = false;
    bool publication_closed = false;
    bool consumed = false;
    std::optional<AITaskResult> result;
    std::unique_ptr<AIProjectTaskHandle> handle;
};

struct AIProjectProcessor::Lane {
    explicit Lane(size_t slot_count) {
        // ai_allocate_shared constructs and destroys Lane in the async memory
        // scope, so its persistent slot storage must be allocated here too.
        TEST_SYNC_POINT("AIProjectProcessor::Lane:before_slots_allocation");
        slots.resize(slot_count);
    }

    mutable std::mutex mutex;
    ChunkPtr input_chunk;
    size_t input_offset = 0;
    std::shared_ptr<ActiveSubchunk> active;
    std::vector<RequestSlot> slots; // Bounded allocation before the first callback.
    Status terminal_status;
    TerminalKind terminal_kind = TerminalKind::NONE;
    size_t outstanding_callbacks = 0;
    size_t submissions_in_progress = 0;
    AIExecutionStatistics statistics;
    AIAggregateExecutionStatistics aggregate_statistics;
    bool building = false;
    bool source_finished = false;
    QueryContextLifetimeWeakPtr query_lifetime;
    PipeObservable observable;
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
            auto lane = ai_allocate_shared<Lane>(_memory, std::min<size_t>(_config.max_inflight, 64));
            _lanes.emplace_back(std::move(lane));
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
    std::shared_ptr<ActiveSubchunk> active;
    size_t offset = 0;
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE ||
            lane->building) {
            return Status::OK();
        }
        lane->building = true;
        active = lane->active;
        input = lane->input_chunk;
        offset = lane->input_offset;
    }
    DeferOp clear_building([lane] {
        std::lock_guard lock(lane->mutex);
        lane->building = false;
    });

    Status process_status;
    TRY_CATCH_ALLOC_SCOPE_START();

    if (active != nullptr) {
        RETURN_IF_ERROR(_advance(lane, active));
        return Status::OK();
    }

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

    for (size_t output_index = 0; output_index < subchunk->prepared.ai_outputs.size(); ++output_index) {
        auto& prepared_output = subchunk->prepared.ai_outputs[output_index];
        const bool slot_exists = subchunk->prepared.output_chunk->is_slot_exist(prepared_output.slot_id);
        if (prepared_output.replace_existing != slot_exists) {
            return Status::InternalError("AI project output slot conflicts with projection");
        }
        for (size_t previous = 0; previous < output_index; ++previous) {
            if (subchunk->prepared.ai_outputs[previous].slot_id == prepared_output.slot_id) {
                return Status::InternalError("AI project contains duplicate AI output slots");
            }
        }
        const auto* single = std::get_if<AIFunctionInputBatch>(&prepared_output.input);
        const auto* aggregate = std::get_if<AIAggregateReduceInputBatch>(&prepared_output.input);
        if ((single != nullptr && single->rows.size() != rows) ||
            (aggregate != nullptr && (aggregate->states == nullptr || aggregate->states->size() != rows))) {
            return Status::InternalError("AI project expression returned an unexpected number of row inputs");
        }
        OutputState output;
        output.slot_id = prepared_output.slot_id;
        output.replace_existing = prepared_output.replace_existing;
        output.result_type = prepared_output.result_type;
        output.result_kind = prepared_output.result_kind;
        output.rows.resize(rows);
        for (size_t row = 0; row < rows; ++row) {
            if (aggregate != nullptr) {
                // Aggregate decoding belongs to the bounded driver advance,
                // not eager preparation of every group in this subchunk.
                ++subchunk->remaining;
                continue;
            }
            auto work = AIProjectRowWork::create(prepared_output.input, row);
            // Preserve scalar preflight behavior, including on_error=ignore.
            if (!work.ok()) {
                if (single != nullptr && work.status().is_invalid_argument() && _config.on_error == "ignore") {
                    output.rows[row].completed = true;
                    continue;
                }
                return work.status();
            }
            output.rows[row].work.emplace(std::move(work).value());
            ++subchunk->remaining;
        }
        subchunk->outputs.emplace_back(std::move(output));
    }
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished) return Status::OK();
        if (!lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE) return Status::OK();
        lane->active = subchunk;
    }
    return _advance(lane, subchunk);
}

Status AIProjectProcessor::_advance(const std::shared_ptr<Lane>& lane,
                                    const std::shared_ptr<ActiveSubchunk>& subchunk) {
    const bool ignore = _config.on_error == "ignore";
    auto stopped = [&] {
        std::lock_guard lock(lane->mutex);
        return lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE;
    };
    auto fail_row = [&](size_t output, size_t row, const Status& failure) -> Status {
        if (!failure.is_invalid_argument() || !ignore) return failure;
        // Publish discard before cancellation, which is allowed to callback inline.
        {
            std::lock_guard lock(lane->mutex);
            for (auto& slot : lane->slots) {
                if (slot.used && slot.output_index == output && slot.row_index == row) slot.discarded = true;
            }
        }
        TEST_SYNC_POINT("AIProjectProcessor::_advance:after_row_discard");
        subchunk->outputs[output].rows[row].work->finish_null();
        for (size_t i = 0; i < lane->slots.size(); ++i) {
            std::unique_ptr<AIProjectTaskHandle> cancel;
            {
                std::lock_guard lock(lane->mutex);
                auto& slot = lane->slots[i];
                if (slot.used && slot.discarded && !slot.publication_closed) cancel = std::move(slot.handle);
            }
            if (cancel != nullptr) cancel->cancel();
        }
        return Status::OK();
    };
    auto advance = [&]() -> Status {
        size_t submitted = 0;
        size_t bytes = 0;
        size_t initialized_cells = 0;
        size_t decoded_bytes = 0;
        constexpr size_t kMaxInitializedCells = 64;
        constexpr size_t kMaxDecodedBytes = kAiAggregateStateMaxBytes;
        const size_t row_count = subchunk->outputs.empty() ? 0 : subchunk->outputs[0].rows.size();
        const size_t total = row_count * subchunk->outputs.size();
        for (;;) {
            if (stopped()) return Status::OK();
            // Completion is consumed only by the driver. Slots become reusable
            // after publication closes AND submit returns, independently of the
            // callback lifecycle barrier that still protects notification.
            for (size_t i = 0; i < lane->slots.size(); ++i) {
                std::optional<AITaskResult> result;
                std::unique_ptr<AIProjectTaskHandle> handle;
                size_t output = 0, row = 0;
                uint64_t ordinal = 0;
                bool discard = false;
                {
                    std::lock_guard lock(lane->mutex);
                    auto& slot = lane->slots[i];
                    if (!slot.used || !slot.publication_closed || slot.consumed) continue;
                    output = slot.output_index;
                    row = slot.row_index;
                    ordinal = slot.ordinal;
                    discard = slot.discarded;
                    result = std::move(slot.result);
                    slot.result.reset();
                    slot.consumed = true;
                    if (slot.submission_returned) {
                        handle = std::move(slot.handle);
                        slot.used = false;
                    }
                }
                if (discard || !result) continue;
                auto& cell = subchunk->outputs[output].rows[row];
                if (!cell.work->is_aggregate()) cell.completed_handle = std::move(handle);
                if (auto* success = std::get_if<AITaskSuccess>(&*result)) {
                    const auto status = cell.work->consume_success(ordinal, std::move(*success));
                    if (status.is_invalid_argument() && cell.work->is_aggregate()) {
                        std::lock_guard lock(lane->mutex);
                        ++lane->aggregate_statistics.local_budget_errors;
                    }
                    RETURN_IF_ERROR(fail_row(output, row, status));
                } else if (std::holds_alternative<AISanitizedRowFailure>(*result)) {
                    RETURN_IF_ERROR(fail_row(output, row, Status::InvalidArgument("AI function request failed")));
                }
            }
            bool runnable = false;
            for (auto& output : subchunk->outputs) {
                for (auto& cell : output.rows) {
                    if (cell.completed) continue;
                    if (!cell.work.has_value()) {
                        runnable = true;
                    } else if (cell.work->progress() == AIWorkProgress::COMPLETE) {
                        cell.success = cell.work->take_result();
                        cell.completed = true;
                        --subchunk->remaining;
                    } else {
                        runnable |= cell.work->progress() == AIWorkProgress::RUNNABLE;
                    }
                }
            }
            size_t free_slot = lane->slots.size();
            {
                std::lock_guard lock(lane->mutex);
                subchunk->runnable = runnable;
                subchunk->ready = subchunk->remaining == 0;
                for (size_t i = 0; i < lane->slots.size(); ++i) {
                    if (!lane->slots[i].used) {
                        free_slot = i;
                        break;
                    }
                }
            }
            if (!runnable || free_slot == lane->slots.size() || submitted >= 64 || bytes >= 256 * 1024) break;
            size_t output_index = 0, row_index = 0;
            bool found = false;
            for (size_t scan = 0; scan < total; ++scan) {
                const size_t index = subchunk->next_row++ % total;
                output_index = index / row_count;
                row_index = index % row_count;
                auto& cell = subchunk->outputs[output_index].rows[row_index];
                if (!cell.completed && (!cell.work.has_value() || cell.work->progress() == AIWorkProgress::RUNNABLE)) {
                    found = true;
                    break;
                }
            }
            if (!found) break;
            auto& cell = subchunk->outputs[output_index].rows[row_index];
            if (!cell.work.has_value()) {
                auto& input = subchunk->prepared.ai_outputs[output_index].input;
                const auto& aggregate = std::get<AIAggregateReduceInputBatch>(input);
                const size_t state_bytes =
                        aggregate.states->is_null(row_index) ? 0 : aggregate.states->get(row_index).get_slice().size;
                if (initialized_cells == kMaxInitializedCells ||
                    (initialized_cells != 0 && state_bytes > kMaxDecodedBytes - decoded_bytes)) {
                    // Keep this cell next in the round-robin order. An
                    // oversized first state still reaches the codec and fails,
                    // rather than yielding forever without validating it.
                    --subchunk->next_row;
                    break;
                }
                ++initialized_cells;
                ASSIGN_OR_RETURN(auto initialized, AIProjectRowWork::create(input, row_index));
                // Codec/memory failures above are query-fatal even with ignore.
                // A successful decode is at most kMaxDecodedBytes.
                decoded_bytes += state_bytes;
                cell.work.emplace(std::move(initialized));
                {
                    std::lock_guard lock(lane->mutex);
                    ++lane->aggregate_statistics.groups;
                    lane->aggregate_statistics.empty_groups += cell.work->progress() == AIWorkProgress::COMPLETE;
                }
                if (cell.work->progress() == AIWorkProgress::COMPLETE) continue;
            }
            auto& work = *cell.work;
            TEST_SYNC_POINT("AIProjectProcessor::_advance:before_request");
            auto next = work.next_request(256 * 1024 - bytes);
            if (!next.ok()) {
                if (work.is_aggregate() && next.status().is_invalid_argument()) {
                    std::lock_guard lock(lane->mutex);
                    ++lane->aggregate_statistics.local_budget_errors;
                }
                RETURN_IF_ERROR(fail_row(output_index, row_index, next.status()));
                continue;
            }
            if (!next.value()) break;
            AIWorkRequest request = std::move(*next.value());
            bytes += request.prompt.size();
            const uint64_t task_id = _next_task_id.fetch_add(1, std::memory_order_relaxed);
            AITaskCallback callback;
            auto build_callback = [&] {
                callback = [lane, ignore, task_id, free_slot](
                                   AITaskResult result, const AIExecutionStatistics& statistics) mutable noexcept {
                    _complete_task(lane, ignore, task_id, free_slot, std::move(result), statistics);
                };
            };
            _memory.run_in_physical_scope([](void* p) { (*static_cast<decltype(build_callback)*>(p))(); },
                                          &build_callback);
            SCOPED_CLEANUP({
                auto clear = [&] { AITaskCallback().swap(callback); };
                _memory.run_in_physical_scope([](void* p) { (*static_cast<decltype(clear)*>(p))(); }, &clear);
            });
            {
                std::lock_guard lock(lane->mutex);
                if (lane->source_finished || !lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE)
                    break;
                auto& slot = lane->slots[free_slot];
                slot = RequestSlot{};
                slot.used = true;
                slot.task_id = task_id;
                slot.output_index = output_index;
                slot.row_index = row_index;
                slot.ordinal = request.ordinal;
                slot.aggregate = work.is_aggregate();
                if (slot.aggregate) {
                    lane->aggregate_statistics.leaf_requests += request.aggregate_leaf;
                    lane->aggregate_statistics.rounds += request.first_in_round;
                }
                ++lane->outstanding_callbacks;
                ++lane->submissions_in_progress;
            }
            ++submitted;
            const auto& output = subchunk->prepared.ai_outputs[output_index];
            StatusOr<std::unique_ptr<AIProjectTaskHandle>> handle_or =
                    Status::InternalError("AI task submission failed");
            Status exception_status;
            try {
                handle_or = _submitter->submit(AIProjectTaskRequest{.task_id = task_id,
                                                                    .model = work.model(),
                                                                    .prompt = request.prompt,
                                                                    .options = request.options.get(),
                                                                    .model_config_id = output.model_config_id,
                                                                    .capability = output.capability,
                                                                    .media = request.media ? &*request.media : nullptr},
                                               std::move(callback));
            } catch (const std::bad_alloc&) {
                exception_status = Status::MemoryLimitExceeded("Failed to submit AI function task");
            } catch (const std::runtime_error&) {
                exception_status = Status::RuntimeError("AI function task submission raised a runtime error");
            } catch (...) {
                exception_status = Status::InternalError("Unexpected AI function task submission failure");
            }
            if (exception_status.ok() && handle_or.ok() && handle_or.value() == nullptr) {
                exception_status = Status::InternalError("AI task submitter returned a null handle");
            }
            if (!exception_status.ok()) {
                std::lock_guard lock(lane->mutex);
                lane->terminal_status.update(exception_status);
            }
            bool already_published;
            {
                std::lock_guard lock(lane->mutex);
                already_published = lane->slots[free_slot].publication_closed;
            }
            if (!already_published && (!handle_or.ok() || handle_or.value() == nullptr)) {
                _complete_submit_failure(lane, ignore, task_id, free_slot,
                                         handle_or.ok()
                                                 ? Status::InternalError("AI task submitter returned a null handle")
                                                 : handle_or.status());
            }
            std::unique_ptr<AIProjectTaskHandle> cancel;
            std::shared_ptr<QueryContextLifetime> drain_pin;
            bool notify = false;
            {
                std::lock_guard lock(lane->mutex);
                auto& slot = lane->slots[free_slot];
                slot.submission_returned = true;
                if (handle_or.ok()) {
                    if (lane->source_finished ||
                        (!slot.publication_closed && (slot.discarded || !lane->terminal_status.ok() ||
                                                      lane->terminal_kind != TerminalKind::NONE))) {
                        cancel = std::move(handle_or).value();
                    } else {
                        slot.handle = std::move(handle_or).value();
                    }
                }
                if (slot.consumed && slot.publication_closed) {
                    // Source finish may have discarded this result before
                    // submit returned. Keep its identity until the callback
                    // closes publication and releases its lifecycle barrier.
                    slot.used = false;
                    notify = !lane->source_finished;
                }
                if (lane->source_finished && lane->submissions_in_progress == 1 && lane->outstanding_callbacks == 0) {
                    drain_pin = lane->query_lifetime.lock();
                }
                --lane->submissions_in_progress;
            }
            if (cancel != nullptr) cancel->cancel();
            if (notify || drain_pin != nullptr) _notify_source(&lane->observable);
            if (!exception_status.ok()) return exception_status;
        }
        return Status::OK();
    };
    Status result;
    try {
        result = advance();
    } catch (const std::bad_alloc&) {
        result = Status::MemoryLimitExceeded("Failed to advance AI project work");
    } catch (...) {
        result = Status::InternalError("Failed to advance AI project work");
    }
    if (!result.ok()) {
        std::lock_guard lock(lane->mutex);
        lane->terminal_status.update(result);
    }
    // Driver errors cancel immediately. A callback-published terminal status
    // is consumed by pull_chunk, which retains ownership of completed handles
    // until that consumption. Source finish already disposes its own handles.
    if (!result.ok()) _dispose_request_handles(lane, true);
    return result;
}

void AIProjectProcessor::_complete_submit_failure(const std::shared_ptr<Lane>& lane, bool ignore, uint64_t task_id,
                                                  size_t slot_index, const Status& status) {
    if (status.is_cancelled() || status.is_time_out() || status.is_shutdown() || status.is_service_unavailable()) {
        const auto reason = status.is_cancelled()  ? AILifecycleReason::CANCELLED
                            : status.is_time_out() ? AILifecycleReason::DEADLINE
                                                   : AILifecycleReason::SHUTDOWN;
        _complete_task(lane, ignore, task_id, slot_index, AILifecycleCancelled{.reason = reason}, {});
    } else {
        const auto failure = status.is_mem_limit_exceeded() || status.is_resource_busy()
                                     ? AISanitizedFailureClass::LOCAL_RESOURCE
                                     : AISanitizedFailureClass::LOCAL_REQUEST;
        _complete_task(lane, ignore, task_id, slot_index, AISanitizedRowFailure{.failure_class = failure}, {});
    }
}

void AIProjectProcessor::_complete_task(const std::shared_ptr<Lane>& lane, bool ignore, uint64_t task_id,
                                        size_t slot_index, AITaskResult result,
                                        const AIExecutionStatistics& statistics) noexcept {
    bool notify = false;
    {
        AITaskResult local(std::move(result));
        std::lock_guard lock(lane->mutex);
        auto& slot = lane->slots[slot_index];
        if (!slot.used || slot.task_id != task_id || slot.publication_closed) return;
        lane->statistics.add(statistics);
        try {
            if (!lane->source_finished && !slot.discarded) {
                TEST_SYNC_POINT("AIProjectProcessor::_complete_task:before_publish");
                if (const auto* failure = std::get_if<AISanitizedRowFailure>(&local)) {
                    if (slot.aggregate && failure->failure_class == AISanitizedFailureClass::LOCAL_RESOURCE) {
                        lane->terminal_status.update(
                                Status::MemoryLimitExceeded("AI aggregate request resource limit exceeded"));
                    } else if (!ignore && lane->terminal_kind == TerminalKind::NONE)
                        lane->terminal_kind = TerminalKind::ROW_FAILURE;
                } else if (const auto* cancelled = std::get_if<AILifecycleCancelled>(&local)) {
                    if (lane->terminal_kind == TerminalKind::NONE) {
                        lane->terminal_kind =
                                cancelled->reason == AILifecycleReason::CANCELLED  ? TerminalKind::CANCELLED
                                : cancelled->reason == AILifecycleReason::DEADLINE ? TerminalKind::DEADLINE
                                                                                   : TerminalKind::SHUTDOWN;
                    }
                }
                slot.result.emplace(std::move(local));
                notify = true;
            } else {
                slot.consumed = true;
            }
        } catch (...) {
            lane->terminal_kind = TerminalKind::ROW_FAILURE;
            // Keep the slot owned until driver cleanup destroys its handle.
            slot.consumed = false;
            notify = !lane->source_finished;
        }
        slot.publication_closed = true;
        if (slot.consumed && slot.submission_returned) slot.used = false;
        // No slot access is permitted after this lock: the driver may reuse it
        // while this callback is still executing the observer notification.
    }
    if (notify) _notify_source(&lane->observable);
    std::shared_ptr<QueryContextLifetime> drain_pin;
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished && lane->submissions_in_progress == 0 && lane->outstanding_callbacks == 1) {
            drain_pin = lane->query_lifetime.lock();
        }
        --lane->outstanding_callbacks;
    }
    if (drain_pin != nullptr) _notify_source(&lane->observable);
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
    if (!lane->terminal_status.ok() || lane->terminal_kind != TerminalKind::NONE) return true;
    if (lane->active == nullptr) return false;
    if (lane->active->ready) return true;
    // Preserve one-shot scalar readiness: partial completions only make the
    // driver runnable. A completely published request wave can be consumed by
    // pull_chunk, including a reducer wave that has another round to schedule.
    if (lane->active->runnable) return false;
    bool published = false;
    for (const auto& slot : lane->slots) {
        if (!slot.used || slot.discarded) continue;
        if (!slot.publication_closed) return false;
        published |= !slot.consumed;
    }
    return published;
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
            lane->building) {
            return false;
        }
        if (lane->active != nullptr) {
            bool free = false;
            for (const auto& slot : lane->slots) {
                if (slot.used && slot.publication_closed && !slot.consumed) return true;
                free |= !slot.used;
            }
            return !lane->active->ready && lane->active->runnable && free;
        }
        if (lane->input_chunk != nullptr) return true;
    }
    auto has_chunk = _input_buffer->lane_has_chunk(driver_sequence);
    return has_chunk.ok() ? has_chunk.value() : true;
}

StatusOr<ChunkPtr> AIProjectProcessor::pull_chunk(RuntimeState* state, int32_t driver_sequence) {
    RETURN_IF_ERROR(try_process(state, driver_sequence));
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
        _dispose_request_handles(lane, true);
        return _terminal_status(driver_status, terminal_kind);
    }

    AIExecutionStatistics statistics;
    auto output = _materialize(subchunk, _config.on_error == "ignore", &statistics);
    if (!statistics.empty()) {
        // Typed validation runs after task completion. Publish only its additional
        // row errors before exposing output or returning a materialization failure.
        if (auto* query_state = state == nullptr ? nullptr : state->query_runtime_state(); query_state != nullptr) {
            query_state->add_ai_statistics(statistics);
        }
        std::lock_guard lock(lane->mutex);
        lane->statistics.add(statistics);
    }
    if (!output.ok()) {
        {
            std::lock_guard lock(lane->mutex);
            lane->terminal_status.update(output.status());
        }
        _dispose_request_handles(lane, true);
        return output.status();
    }
    {
        std::lock_guard lock(lane->mutex);
        if (lane->active == subchunk) {
            lane->active.reset();
        }
    }
    _dispose_request_handles(lane, false);
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

AIExecutionStatistics AIProjectProcessor::statistics(int32_t driver_sequence) const {
    auto lane_or = _lane(driver_sequence);
    if (!lane_or.ok()) return {};
    const std::shared_ptr<Lane>& lane = lane_or.value();
    std::lock_guard lock(lane->mutex);
    return lane->statistics;
}

AIAggregateExecutionStatistics AIProjectProcessor::aggregate_statistics(int32_t driver_sequence) const {
    auto lane_or = _lane(driver_sequence);
    if (!lane_or.ok()) return {};
    std::lock_guard lock(lane_or.value()->mutex);
    return lane_or.value()->aggregate_statistics;
}

Status AIProjectProcessor::set_source_finished(int32_t driver_sequence) {
    ASSIGN_OR_RETURN(std::shared_ptr<Lane> lane, _lane(driver_sequence));

    ChunkPtr input_to_release;
    std::shared_ptr<ActiveSubchunk> active_to_release;
    {
        std::lock_guard lock(lane->mutex);
        if (lane->source_finished) {
            return Status::OK();
        }
        lane->source_finished = true;
        input_to_release = std::move(lane->input_chunk);
        active_to_release = std::move(lane->active);
    }

    Status buffer_status = _input_buffer->set_source_finished(driver_sequence);
    _dispose_request_handles(lane, true);
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

StatusOr<ChunkPtr> AIProjectProcessor::_materialize(const std::shared_ptr<ActiveSubchunk>& subchunk,
                                                    bool ignore_row_failures, AIExecutionStatistics* statistics) {
    ChunkPtr output;
    TRY_CATCH_ALLOC_SCOPE_START();
    output = subchunk->prepared.output_chunk;
    for (OutputState& result : subchunk->outputs) {
        auto values = ColumnHelper::create_column(result.result_type, true);
        values->reserve(result.rows.size());
        for (ResultCell& cell : result.rows) {
            if (cell.success.has_value()) {
                const Status status =
                        append_ai_function_result(result.result_kind, cell.success->result(), values.get());
                if (status.ok()) {
                    continue;
                }
                if (!status.is_invalid_argument()) {
                    return status;
                }
                statistics->add({.error_count = 1});
                if (!ignore_row_failures) {
                    return _row_failure_status();
                }
            }
            values->append_nulls(1);
        }
        ColumnPtr result_column = std::move(values);
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

void AIProjectProcessor::_dispose_request_handles(const std::shared_ptr<Lane>& lane, bool cancel_unfinished) noexcept {
    if (lane == nullptr) return;
    for (size_t i = 0; i < lane->slots.size(); ++i) {
        std::unique_ptr<AIProjectTaskHandle> handle;
        std::optional<AITaskResult> result;
        bool cancel = false;
        {
            std::lock_guard lock(lane->mutex);
            auto& slot = lane->slots[i];
            cancel = cancel_unfinished && slot.used && !slot.publication_closed;
            // A discarded callback may have freed its slot before the cancel
            // sweep. Its completed handle still belongs to driver cleanup.
            handle = std::move(slot.handle);
            if (cancel_unfinished && slot.used) {
                slot.discarded = true;
                result = std::move(slot.result);
                slot.result.reset();
                slot.consumed = true;
                if (slot.publication_closed && slot.submission_returned) slot.used = false;
            }
        }
        if (handle != nullptr && cancel) handle->cancel();
    }
    // Row work, columns, and summaries are driver-owned. Source finish only
    // detaches its shared_ptr; an advancing driver is their final owner.
}

void AIProjectProcessor::_notify_source(PipeObservable* observable) {
    // Pipeline observers follow the same no-throw trigger contract as the rest
    // of main's PipeObservable users. DeferOp deliberately invokes them from a
    // noexcept destructor.
    auto notify = observable->defer_notify_source();
}

} // namespace starrocks::pipeline
