// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/chunk_source.h"

#include "column/column_helper.h"
#include "common/statusor.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/workgroup/work_group.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

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

Status ChunkSource::buffer_next_batch_chunks_blocking(size_t batch_size, RuntimeState* state) {
    if (!_status.ok()) {
        return _status;
    }
    using namespace vectorized;

    for (size_t i = 0; i < batch_size && !state->is_cancelled(); ++i) {
        ChunkPtr chunk;
        _status = _read_chunk(state, &chunk);
        if (!_status.ok()) {
            // end of file is normal case, need process chunk
            if (_status.is_end_of_file()) {
                _chunk_buffer.put(_scan_operator_seq, std::move(chunk));
            }
            break;
        }

        _chunk_buffer.put(_scan_operator_seq, std::move(chunk));
    }

    return _status;
}

Status ChunkSource::buffer_next_batch_chunks_blocking_for_workgroup(size_t batch_size, RuntimeState* state,
                                                                    size_t* num_read_chunks, int worker_id,
                                                                    workgroup::WorkGroupPtr running_wg) {
    if (!_status.ok()) {
        return _status;
    }

    using namespace vectorized;
    int64_t time_spent = 0;
    for (size_t i = 0; i < batch_size && !state->is_cancelled(); ++i) {
        {
            SCOPED_RAW_TIMER(&time_spent);

            ChunkPtr chunk;
            _status = _read_chunk(state, &chunk);
            if (!_status.ok()) {
                // end of file is normal case, need process chunk
                if (_status.is_end_of_file()) {
                    ++(*num_read_chunks);
                    _chunk_buffer.put(_scan_operator_seq, std::move(chunk));
                }
                break;
            }

            ++(*num_read_chunks);
            _chunk_buffer.put(_scan_operator_seq, std::move(chunk));
        }

        if (time_spent >= YIELD_MAX_TIME_SPENT) {
            break;
        }

        if (time_spent >= YIELD_PREEMPT_MAX_TIME_SPENT &&
            workgroup::WorkGroupManager::instance()->get_owners_of_scan_worker(workgroup::TypeOlapScanExecutor,
                                                                               worker_id, running_wg)) {
            break;
        }
    }

    return _status;
}

using namespace vectorized;
} // namespace starrocks::pipeline