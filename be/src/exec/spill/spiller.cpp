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

#include "exec/spill/spiller.h"

#include <butil/iobuf.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/pipeline/query_context.h"
#include "exec/sort_exec_exprs.h"
#include "exec/spill/mem_table.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks {
namespace spill {
SpillProcessMetrics::SpillProcessMetrics(RuntimeProfile* profile) {
    spill_timer = ADD_TIMER(profile, "SpillTime");
    spill_rows = ADD_COUNTER(profile, "SpilledRows", TUnit::UNIT);
    flush_timer = ADD_TIMER(profile, "SpillFlushTimer");
    write_io_timer = ADD_TIMER(profile, "SpillWriteIOTimer");
    restore_rows = ADD_COUNTER(profile, "SpillRestoreRows", TUnit::UNIT);
    restore_timer = ADD_TIMER(profile, "SpillRestoreTimer");
}

Status Spiller::prepare(RuntimeState* state) {
    // prepare
    for (size_t i = 0; i < _opts.mem_table_pool_size; ++i) {
        if (_opts.is_unordered) {
            _mem_table_pool.push(
                    std::make_unique<UnorderedMemTable>(state, _opts.spill_file_size, state->instance_mem_tracker()));
        } else {
            _mem_table_pool.push(std::make_unique<OrderedMemTable>(&_opts.sort_exprs->lhs_ordering_expr_ctxs(),
                                                                   _opts.sort_desc, state, _opts.spill_file_size,
                                                                   state->instance_mem_tracker()));
        }
    }

    ASSIGN_OR_RETURN(_serde, spill::create_serde(&_opts));
    _block_group = std::make_shared<spill::BlockGroup>(_serde);
#ifndef BE_TEST
    _block_manager = state->query_ctx()->spill_manager()->block_manager();
#endif

    return Status::OK();
}

Status Spiller::_open(RuntimeState* state) {
    return Status::OK();
}

Status Spiller::_run_flush_task(RuntimeState* state, const MemTablePtr& mem_table) {
    if (state->is_cancelled()) {
        return Status::OK();
    }
    spill::AcquireBlockOptions opts;
    opts.query_id = state->query_id();
    opts.plan_node_id = _opts.plan_node_id;
    opts.name = _opts.name;
    ASSIGN_OR_RETURN(auto block, _block_manager->acquire_block(opts));

    {
        SCOPED_TIMER(_metrics.write_io_timer);
        // @TODO reuse context
        SerdeContext ctx;
        size_t num_rows_flushed = 0;
        RETURN_IF_ERROR(mem_table->flush([&](const auto& chunk) {
            num_rows_flushed += chunk->num_rows();
            RETURN_IF_ERROR(_serde->serialize(ctx, chunk, block));
            return Status::OK();
        }));
    }
    RETURN_IF_ERROR(block->flush());
    _block_manager->release_block(block);

    std::lock_guard<std::mutex> l(_mutex);
    _block_group->append(block);

    return Status::OK();
}

void Spiller::_update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

Status Spiller::_acquire_input_stream(RuntimeState* state) {
    std::lock_guard l(_mutex);
    if (_input_stream != nullptr) {
        return Status::OK();
    }
    if (_opts.is_unordered) {
        ASSIGN_OR_RETURN(_input_stream, _block_group->as_unordered_stream());
    } else {
        ASSIGN_OR_RETURN(_input_stream, _block_group->as_ordered_stream(state, _opts.sort_exprs, _opts.sort_desc));
    }
    return Status::OK();
}

Status Spiller::_decrease_running_flush_tasks() {
    if (_running_flush_tasks.fetch_sub(1) == 1) {
        if (_flush_all_callback) {
            RETURN_IF_ERROR(_flush_all_callback());
            if (_inner_flush_all_callback) {
                RETURN_IF_ERROR(_inner_flush_all_callback());
            }
        }
    }
    return Status::OK();
}
} // namespace spill
} // namespace starrocks