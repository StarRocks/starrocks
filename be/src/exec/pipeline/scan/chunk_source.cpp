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

#include "exec/pipeline/scan/chunk_source.h"

#include <random>

#include "common/statusor.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group.h"
#include "runtime/runtime_state.h"
namespace starrocks::pipeline {

ChunkSource::ChunkSource(ScanOperator* scan_op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                         BalancedChunkBuffer& chunk_buffer)
        : _scan_op(scan_op),
          _scan_operator_seq(scan_op->get_driver_sequence()),
          _runtime_profile(runtime_profile),
          _morsel(std::move(morsel)),
          _chunk_buffer(chunk_buffer),
          _chunk_token(nullptr) {}

Status ChunkSource::prepare(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _io_task_wait_timer = ADD_TIMER(_runtime_profile, "IOTaskWaitTime");
    _io_task_exec_timer = ADD_TIMER(_runtime_profile, "IOTaskExecTime");
    return Status::OK();
}

void ChunkSource::pin_chunk_token(ChunkBufferTokenPtr chunk_token) {
    _chunk_token = std::move(chunk_token);
}

void ChunkSource::unpin_chunk_token() {
    _chunk_token.reset(nullptr);
}

Status ChunkSource::buffer_next_batch_chunks_blocking(RuntimeState* state, size_t batch_size,
                                                      const workgroup::WorkGroup* running_wg) {
    if (!_status.ok()) {
        return _status;
    }

    int64_t time_spent_ns = 0;
    auto [owner_id, version] = _morsel->get_lane_owner_and_version();
    for (size_t i = 0; i < batch_size && !state->is_cancelled(); ++i) {
        {
            SCOPED_RAW_TIMER(&time_spent_ns);

            if (_chunk_token == nullptr && (_chunk_token = _chunk_buffer.limiter()->pin(1)) == nullptr) {
                break;
            }

            ChunkPtr chunk;
            _status = _read_chunk(state, &chunk);
            // we always output a empty chunk instead of nullptr, because we need set tablet_id and is_last_chunk flag
            // in the chunk.
            if (chunk == nullptr) {
                chunk = std::make_shared<Chunk>();
            }
            if (_status.is_eagain()) {
                _status = Status::OK();
                continue;
            } else if (!_status.ok()) {
                // end of file is normal case, need process chunk
                if (_status.is_end_of_file()) {
                    chunk->owner_info().set_owner_id(owner_id, true);
                    _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
                } else if (_status.is_time_out()) {
                    chunk->owner_info().set_owner_id(owner_id, false);
                    _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
                    _status = Status::OK();
                } else if (_status.is_resource_busy()) {
                    // see ConnectorChunkSource::_read_chunk
                    // there are too much io tasks which leads to failure of memory allocaiton
                    _status = Status::OK();
                }
                break;
            }

            chunk->owner_info().set_owner_id(owner_id, false);
            _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
        }

        if (time_spent_ns >= workgroup::WorkGroup::YIELD_MAX_TIME_SPENT) {
            break;
        }

        if (running_wg != nullptr && time_spent_ns >= workgroup::WorkGroup::YIELD_PREEMPT_MAX_TIME_SPENT &&
            _scan_sched_entity(running_wg)->in_queue()->should_yield(running_wg, time_spent_ns)) {
            break;
        }
    }
    return _status;
}

} // namespace starrocks::pipeline