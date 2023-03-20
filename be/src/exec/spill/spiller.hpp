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

#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/spill/common.h"
#include "exec/spill/spiller.h"
#include "util/defer_op.h"

namespace starrocks {
namespace spill {
template <class TaskExecutor, class MemGuard>
Status Spiller::spill(RuntimeState* state, ChunkPtr chunk, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.spill_timer);
    RETURN_IF_ERROR(_spilled_task_status);
    DCHECK(!chunk->is_empty());
    DCHECK(!is_full());

    if (_mem_table == nullptr) {
        _mem_table = _acquire_mem_table_from_pool();
        DCHECK(_mem_table != nullptr);
    }

    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    _spilled_append_rows += chunk->num_rows();
    TRACE_SPILL_LOG << "spilled rows:" << chunk->num_rows() << ",cumulative:" << _spilled_append_rows
                    << ",spiller:" << this << "," << _mem_table.get();
    RETURN_IF_ERROR(_mem_table->append(std::move(chunk)));
    if (_mem_table->is_full()) {
        RETURN_IF_ERROR(flush(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard)));
    }

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status Spiller::flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    RETURN_IF_ERROR(_spilled_task_status);
    auto captured_mem_table = std::move(_mem_table);
    if (captured_mem_table == nullptr) {
        return Status::OK();
    }

    RETURN_IF_ERROR(captured_mem_table->done());
    _running_flush_tasks++;
    // TODO: handle spill queue
    auto task = [this, state, guard = guard, mem_table = std::move(captured_mem_table)]() {
        SCOPED_TIMER(_metrics.flush_timer);
        DCHECK_GT(_running_flush_tasks, 0);
        DCHECK(has_pending_data());
        guard.scoped_begin();
        //
        auto defer = DeferOp([&]() {
            {
                std::lock_guard _(_mutex);
                _mem_table_pool.emplace(std::move(mem_table));
            }

            _update_spilled_task_status(_decrease_running_flush_tasks());
        });

        _update_spilled_task_status(_run_flush_task(state, mem_table));
        guard.scoped_end();
    };
    // submit io task
    RETURN_IF_ERROR(executor.submit(std::move(task)));
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> Spiller::restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.restore_timer);
    DCHECK(has_output_data());

    RETURN_IF_ERROR(_get_spilled_task_status());
    RETURN_IF_ERROR(_acquire_input_stream(state));

    ChunkPtr chunk;
    // @TODO(silverbullet233): reuse ctx
    SerdeContext ctx;
    ASSIGN_OR_RETURN(chunk, _input_stream->get_next(ctx));
    COUNTER_UPDATE(_metrics.restore_rows, chunk->num_rows());
    _restore_read_rows += chunk->num_rows();
    RETURN_IF_ERROR(trigger_restore(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard)));
    return chunk;
}

template <class TaskExecutor, class MemGuard>
Status Spiller::trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    RETURN_IF_ERROR(_get_spilled_task_status());
    RETURN_IF_ERROR(_acquire_input_stream(state));

    DCHECK(_input_stream->enable_prefetch());
    // if all is well and input stream enable prefetch and not eof
    if (!_input_stream->eof()) {
        auto restore_task = [this, state, guard]() {
            _running_restore_tasks++;
            guard.scoped_begin();
            // @TODO(silverbullet233): reuse ctx
            SerdeContext ctx;
            auto res = _input_stream->prefetch(ctx);

            _update_spilled_task_status(res.is_end_of_file() ? Status::OK() : res);
            guard.scoped_end();
            _running_restore_tasks--;
            if (!res.ok()) {
                _finished_restore_tasks++;
                return;
            }
        };
        RETURN_IF_ERROR(executor.submit(std::move(restore_task)));
    }
    return Status::OK();
}
} // namespace spill
} // namespace starrocks