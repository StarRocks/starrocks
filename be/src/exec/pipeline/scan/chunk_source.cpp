// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/chunk_source.h"

#include "column/column_helper.h"
#include "common/statusor.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/workgroup/work_group.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

ChunkSource::ChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                         BalancedChunkBuffer& chunk_buffer, workgroup::ScanExecutorType executor_type)
        : _scan_operator_seq(scan_operator_id),
          _runtime_profile(runtime_profile),
          _morsel(std::move(morsel)),
          _chunk_buffer(chunk_buffer),
          _chunk_token(nullptr),
          _executor_type(executor_type) {}

Status ChunkSource::prepare(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _io_task_wait_timer = ADD_TIMER(_runtime_profile, "IOTaskWaitTime");
    _io_task_exec_timer = ADD_TIMER(_runtime_profile, "IOTaskExecTime");
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ChunkSource::get_next_chunk_from_buffer() {
    vectorized::ChunkPtr chunk = nullptr;
    _chunk_buffer.try_get(_scan_operator_seq, &chunk);
    return chunk;
}

bool ChunkSource::has_output() const {
    return !_chunk_buffer.empty(_scan_operator_seq);
}

bool ChunkSource::has_shared_output() const {
    return !_chunk_buffer.all_empty();
}

void ChunkSource::pin_chunk_token(ChunkBufferTokenPtr chunk_token) {
    _chunk_token = std::move(chunk_token);
}

void ChunkSource::unpin_chunk_token() {
    _chunk_token.reset(nullptr);
}

std::pair<Status, size_t> ChunkSource::buffer_next_batch_chunks_blocking(RuntimeState* state, size_t batch_size,
                                                                         const workgroup::WorkGroupPtr& running_wg,
                                                                         int worker_id) {
    using namespace vectorized;

    if (!_status.ok()) {
        return std::make_pair(_status, 0);
    }

    int64_t time_spent = 0;
    size_t num_read_chunks = 0;
    for (size_t i = 0; i < batch_size && !state->is_cancelled(); ++i) {
        {
            SCOPED_RAW_TIMER(&time_spent);

            if (_chunk_token == nullptr && (_chunk_token = _chunk_buffer.limiter()->pin(1)) == nullptr) {
                return std::make_pair(_status, num_read_chunks);
            }

            ChunkPtr chunk;
            _status = _read_chunk(state, &chunk);
            if (!_status.ok()) {
                // end of file is normal case, need process chunk
                if (_status.is_end_of_file()) {
                    ++num_read_chunks;
                    _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
                }
                break;
            }

            ++num_read_chunks;
            _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
        }

        if (time_spent >= YIELD_MAX_TIME_SPENT) {
            break;
        }

        if (running_wg != nullptr && time_spent >= YIELD_PREEMPT_MAX_TIME_SPENT &&
            workgroup::WorkGroupManager::instance()->should_yield_scan_worker(_executor_type, worker_id, running_wg)) {
            break;
        }
    }

    return std::make_pair(_status, num_read_chunks);
}

using namespace vectorized;
} // namespace starrocks::pipeline
