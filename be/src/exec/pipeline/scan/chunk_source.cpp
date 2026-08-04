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

#include "base/failpoint/fail_point.h"
#include "base/time/monotime.h"
#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "compute_env/workgroup/scan_task_queue.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/scan_node.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
DEFINE_FAIL_POINT(scan_chunk_sleep_after_read);

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

            // TODO: process when buffer full
            if (_chunk_token == nullptr && (_chunk_token = _chunk_buffer.limiter()->pin(1)) == nullptr) {
                break;
            }

            ChunkPtr chunk;
            _status = _read_chunk(state, &chunk);
            // Wake exactly the consumer driver that owns the chunk-buffer slot this chunk is
            // written to (the put() return value). In shared scan the round-robin buffer puts
            // the chunk into a sibling driver's slot, so the producer must wake that driver or
            // it hangs under event-based scheduling; routing the wakeup per-slot avoids the
            // notify storm of waking every sibling on every chunk.
            int put_index = -1;
            DeferOp notify([this, &put_index]() { _scan_op->notify_chunk_buffer_consumer(put_index); });
            // we always output a empty chunk instead of nullptr, because we need set tablet_id and is_last_chunk flag
            // in the chunk.
            if (chunk == nullptr) {
                chunk = std::make_shared<Chunk>();
            }
            if (chunk != nullptr && !chunk->is_empty()) {
                auto* scan_op_factory = down_cast<ScanOperatorFactory*>(_scan_op->get_factory());
                auto& slot_ids = scan_op_factory->scan_node()->get_heavy_expr_slot_ids();
                auto& expr_ctxs = scan_op_factory->scan_node()->get_heavy_expr_ctxs();
                const size_t num_rows = chunk->num_rows();
                for (auto k = 0; k < slot_ids.size(); ++k) {
                    ASSIGN_OR_RETURN(auto col, expr_ctxs[k]->evaluate(chunk.get()));
                    // The heavy expr result must follow the slot's declared type/nullability, the same
                    // way ProjectOperator normalizes its own evaluated columns. Without this the column
                    // implementation drifts across chunks -- a chunk whose rows all evaluate non-null
                    // yields a bare BinaryColumn while the next one yields NullableColumn<BinaryColumn>.
                    // ChunkPipelineAccumulator::push then appends one into the other, and
                    // BinaryColumnBase::append reinterprets the source through a release-mode down_cast.
                    const auto& type_desc = expr_ctxs[k]->root()->type();
                    if (col->only_null()) {
                        auto mutable_col = ColumnHelper::create_column(type_desc, true);
                        mutable_col->append_nulls(num_rows);
                        col = std::move(mutable_col);
                    } else if (col->is_constant()) {
                        MutableColumnPtr new_column = ColumnHelper::create_column(type_desc, false);
                        auto* const_column = down_cast<const ConstColumn*>(col.get());
                        new_column->append(*const_column->data_column(), 0, 1);
                        new_column->assign(num_rows, 0);
                        col = std::move(new_column);
                    }
                    if (expr_ctxs[k]->root()->is_nullable() && !col->is_nullable()) {
                        col = NullableColumn::create(col, NullColumn::create(num_rows, 0));
                    }
                    chunk->append_column(std::move(col), slot_ids[k]);
                }
            }
            if (!_status.ok()) {
                // end of file is normal case, need process chunk
                if (_status.is_end_of_file()) {
                    chunk->owner_info().set_owner_id(owner_id, true);
                    put_index = _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
                    break;
                } else if (_status.is_time_out()) {
                    chunk->owner_info().set_owner_id(owner_id, false);
                    put_index = _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
                    _status = Status::OK();
                    break;
                } else if (_status.is_eagain()) {
                    // EAGAIN is normal case, but sleep a while to avoid busy loop
                    SleepFor(MonoDelta::FromNanoseconds(workgroup::WorkGroup::YIELD_PREEMPT_MAX_TIME_SPENT));
                    _status = Status::OK();
                } else {
                    break;
                }
            }

            // schema won't be used by the computing layer, here we just reset it.
            chunk->reset_schema();
            chunk->owner_info().set_owner_id(owner_id, false);
            put_index = _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));

            FAIL_POINT_TRIGGER_EXECUTE(scan_chunk_sleep_after_read, { sleep(1); });
        }

        if (time_spent_ns >= workgroup::WorkGroup::YIELD_MAX_TIME_SPENT) {
            break;
        }

        if (running_wg != nullptr && time_spent_ns >= workgroup::WorkGroup::YIELD_PREEMPT_MAX_TIME_SPENT) {
            const auto* scan_sched_entity = _scan_sched_entity(running_wg);
            if (scan_sched_entity->in_queue()->should_yield(scan_sched_entity, time_spent_ns)) {
                break;
            }
        }
    }
    return _status;
}

const workgroup::WorkGroupScanSchedEntity* ChunkSource::_scan_sched_entity(const workgroup::WorkGroup* wg) const {
    DCHECK(wg != nullptr);
    if (_scan_op->sched_entity_type() == workgroup::ScanSchedEntityType::CONNECTOR) {
        return wg->connector_scan_sched_entity();
    } else {
        return wg->scan_sched_entity();
    }
}

} // namespace starrocks::pipeline
