// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks
// Limited.

#include "exec/pipeline/pipeline_driver.h"

#include <sstream>

#include "column/chunk.h"
#include "exec/pipeline/pipeline_driver_dispatcher.h"
#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status PipelineDriver::prepare(RuntimeState* runtime_state) {
    _runtime_state = runtime_state;

    // TotalTime is reserved name
    _total_timer = ADD_TIMER(_runtime_profile, "DriverTotalTime");
    _active_timer = ADD_TIMER(_runtime_profile, "ActiveTime");
    _overhead_timer = ADD_TIMER(_runtime_profile, "OverheadTime");
    _schedule_timer = ADD_TIMER(_runtime_profile, "ScheduleTime");
    _pending_timer = ADD_TIMER(_runtime_profile, "PendingTime");
    _precondition_block_timer = ADD_CHILD_TIMER(_runtime_profile, "PreconditionBlockTime", "PendingTime");
    _input_empty_timer = ADD_CHILD_TIMER(_runtime_profile, "InputEmptyTime", "PendingTime");
    _first_input_empty_timer = ADD_CHILD_TIMER(_runtime_profile, "FirstInputEmptyTime", "InputEmptyTime");
    _followup_input_empty_timer = ADD_CHILD_TIMER(_runtime_profile, "FollowupInputEmptyTime", "InputEmptyTime");
    _output_full_timer = ADD_CHILD_TIMER(_runtime_profile, "OutputFullTime", "PendingTime");
    _local_rf_waiting_set_counter = ADD_COUNTER(_runtime_profile, "LocalRfWaitingSet", TUnit::UNIT);

    _schedule_counter = ADD_COUNTER(_runtime_profile, "ScheduleCounter", TUnit::UNIT);
    _schedule_effective_counter = ADD_COUNTER(_runtime_profile, "ScheduleEffectiveCounter", TUnit::UNIT);
    _schedule_rows_per_chunk = ADD_COUNTER(_runtime_profile, "ScheduleAccumulatedRowsPerChunk", TUnit::UNIT);
    _schedule_accumulated_chunk_moved = ADD_COUNTER(_runtime_profile, "ScheduleAccumulatedChunkMoved", TUnit::UNIT);

    DCHECK(_state == DriverState::NOT_READY);
    // fill OperatorWithDependency instances into _dependencies from _operators.
    DCHECK(_dependencies.empty());
    _dependencies.reserve(_operators.size());
    LocalRFWaitingSet all_local_rf_set;
    for (auto& op : _operators) {
        if (auto* op_with_dep = dynamic_cast<DriverDependencyPtr>(op.get())) {
            _dependencies.push_back(op_with_dep);
        }

        const auto& rf_set = op->rf_waiting_set();
        all_local_rf_set.insert(rf_set.begin(), rf_set.end());

        const auto* global_rf_collector = op->runtime_bloom_filters();
        if (global_rf_collector != nullptr) {
            for (const auto& [_, desc] : global_rf_collector->descriptors()) {
                _global_rf_descriptors.emplace_back(desc);
            }

            _global_rf_wait_timeout_ns =
                    std::max(_global_rf_wait_timeout_ns, global_rf_collector->wait_timeout_ms() * 1000L * 1000L);
        }
    }
    _local_rf_waiting_set_counter->set((int64_t)all_local_rf_set.size());
    _local_rf_holders = fragment_ctx()->runtime_filter_hub()->gather_holders(all_local_rf_set);

    source_operator()->add_morsel_queue(_morsel_queue);
    for (auto& op : _operators) {
        RETURN_IF_ERROR(op->prepare(runtime_state));
        _operator_stages[op->get_id()] = OperatorStage::PREPARED;
    }

    // Driver has no dependencies always sets _all_dependencies_ready to true;
    _all_dependencies_ready = _dependencies.empty();
    // Driver has no local rf to wait for completion always sets _all_local_rf_ready to true;
    _all_local_rf_ready = _local_rf_holders.empty();
    // Driver has no global rf to wait for completion always sets _all_global_rf_ready_or_timeout to true;
    _all_global_rf_ready_or_timeout = _global_rf_descriptors.empty();
    set_driver_state(DriverState::READY);

    _total_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _pending_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _precondition_block_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _input_empty_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _output_full_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _total_timer_sw->start();
    _pending_timer_sw->start();
    _precondition_block_timer_sw->start();
    _input_empty_timer_sw->start();
    _output_full_timer_sw->start();

    return Status::OK();
}

StatusOr<DriverState> PipelineDriver::process(RuntimeState* runtime_state) {
    SCOPED_TIMER(_active_timer);
    set_driver_state(DriverState::RUNNING);
    size_t total_chunks_moved = 0;
    size_t total_rows_moved = 0;
    int64_t time_spent = 0;
    while (true) {
        RETURN_IF_LIMIT_EXCEEDED(runtime_state, "Pipeline");

        size_t num_chunk_moved = 0;
        bool should_yield = false;
        size_t num_operators = _operators.size();
        size_t new_first_unfinished = _first_unfinished;
        for (size_t i = _first_unfinished; i < num_operators - 1; ++i) {
            {
                SCOPED_RAW_TIMER(&time_spent);
                auto& curr_op = _operators[i];
                auto& next_op = _operators[i + 1];

                // Check curr_op finished firstly
                if (curr_op->is_finished()) {
                    if (i == 0) {
                        // For source operators
                        _mark_operator_finishing(curr_op, runtime_state);
                    }
                    _mark_operator_finishing(next_op, runtime_state);
                    new_first_unfinished = i + 1;
                    continue;
                }

                // try successive operator pairs
                if (!curr_op->has_output() || !next_op->need_input()) {
                    continue;
                }

                if (_check_fragment_is_canceled(runtime_state)) {
                    return _state;
                }

                // pull chunk from current operator and push the chunk onto next
                // operator
                StatusOr<vectorized::ChunkPtr> maybe_chunk;
                {
                    SCOPED_TIMER(curr_op->_pull_timer);
                    maybe_chunk = curr_op->pull_chunk(runtime_state);
                }
                auto status = maybe_chunk.status();
                if (!status.ok() && !status.is_end_of_file()) {
                    LOG(WARNING) << "pull_chunk returns not ok status " << status.to_string();
                    return status;
                }

                if (_check_fragment_is_canceled(runtime_state)) {
                    return _state;
                }

                if (status.ok()) {
                    COUNTER_UPDATE(curr_op->_pull_chunk_num_counter, 1);
                    if (maybe_chunk.value() && maybe_chunk.value()->num_rows() > 0) {
                        size_t row_num = maybe_chunk.value()->num_rows();
                        total_rows_moved += row_num;
                        {
                            SCOPED_TIMER(next_op->_push_timer);
                            status = next_op->push_chunk(runtime_state, maybe_chunk.value());
                        }

                        if (!status.ok() && !status.is_end_of_file()) {
                            LOG(WARNING) << "push_chunk returns not ok status " << status.to_string();
                            return status;
                        }

                        COUNTER_UPDATE(curr_op->_pull_row_num_counter, row_num);
                        COUNTER_UPDATE(next_op->_push_chunk_num_counter, 1);
                        COUNTER_UPDATE(next_op->_push_row_num_counter, row_num);
                    }
                    num_chunk_moved += 1;
                    total_chunks_moved += 1;
                }

                // Check curr_op finished again
                if (curr_op->is_finished()) {
                    if (i == 0) {
                        // For source operators
                        _mark_operator_finishing(curr_op, runtime_state);
                    }
                    _mark_operator_finishing(next_op, runtime_state);
                    new_first_unfinished = i + 1;
                    continue;
                }
            }
            // yield when total chunks moved or time spent on-core for evaluation
            // exceed the designated thresholds.
            if (total_chunks_moved >= _yield_max_chunks_moved || time_spent >= _yield_max_time_spent) {
                should_yield = true;
                break;
            }
        }
        // close finished operators and update _first_unfinished index
        for (auto i = _first_unfinished; i < new_first_unfinished; ++i) {
            _mark_operator_finished(_operators[i], runtime_state);
        }
        _first_unfinished = new_first_unfinished;

        if (sink_operator()->is_finished()) {
            finish_operators(runtime_state);
            set_driver_state(is_still_pending_finish() ? DriverState::PENDING_FINISH : DriverState::FINISH);
            return _state;
        }

        // no chunk moved in current round means that the driver is blocked.
        // should yield means that the CPU core is occupied the driver for a
        // very long time so that the driver should switch off the core and
        // give chance for another ready driver to run.
        if (num_chunk_moved == 0 || should_yield) {
            driver_acct().increment_schedule_times();
            driver_acct().update_last_chunks_moved(total_chunks_moved);
            driver_acct().update_accumulated_rows_moved(total_rows_moved);
            driver_acct().update_last_time_spent(time_spent);
            if (is_precondition_block()) {
                set_driver_state(DriverState::PRECONDITION_BLOCK);
            } else if (!sink_operator()->is_finished() && !sink_operator()->need_input()) {
                set_driver_state(DriverState::OUTPUT_FULL);
            } else if (!source_operator()->is_finished() && !source_operator()->has_output()) {
                set_driver_state(DriverState::INPUT_EMPTY);
            } else {
                set_driver_state(DriverState::READY);
            }
            return _state;
        }
    }
}

void PipelineDriver::check_short_circuit() {
    int last_finished = -1;
    for (int i = _first_unfinished; i < _operators.size() - 1; i++) {
        if (_operators[i]->is_finished()) {
            last_finished = i;
        }
    }

    if (last_finished == -1) {
        return;
    }

    _mark_operator_finishing(_operators[last_finished + 1], _runtime_state);
    for (auto i = _first_unfinished; i <= last_finished; ++i) {
        _mark_operator_finished(_operators[i], _runtime_state);
    }
    _first_unfinished = last_finished + 1;

    if (sink_operator()->is_finished()) {
        finish_operators(_runtime_state);
        set_driver_state(is_still_pending_finish() ? DriverState::PENDING_FINISH : DriverState::FINISH);
    }
}

void PipelineDriver::mark_precondition_not_ready() {
    for (auto& op : _operators) {
        _operator_stages[op->get_id()] = OperatorStage::PRECONDITION_NOT_READY;
    }
}

void PipelineDriver::mark_precondition_ready(RuntimeState* runtime_state) {
    for (auto& op : _operators) {
        op->set_precondition_ready(runtime_state);
        dispatch_operators();
    }
}

void PipelineDriver::dispatch_operators() {
    for (auto& op : _operators) {
        _operator_stages[op->get_id()] = OperatorStage::PROCESSING;
    }
}

void PipelineDriver::finish_operators(RuntimeState* runtime_state) {
    for (auto& op : _operators) {
        _mark_operator_finished(op, runtime_state);
    }
}

void PipelineDriver::cancel_operators(RuntimeState* runtime_state) {
    for (auto& op : _operators) {
        _mark_operator_cancelled(op, runtime_state);
    }
}

void PipelineDriver::_close_operators(RuntimeState* runtime_state) {
    for (auto& op : _operators) {
        _mark_operator_closed(op, runtime_state);
    }
}

void PipelineDriver::finalize(RuntimeState* runtime_state, DriverState state) {
    VLOG_ROW << "[Driver] finalize, driver=" << this;
    DCHECK(state == DriverState::FINISH || state == DriverState::CANCELED || state == DriverState::INTERNAL_ERROR);

    _close_operators(runtime_state);

    set_driver_state(state);

    COUNTER_UPDATE(_total_timer, _total_timer_sw->elapsed_time());
    COUNTER_UPDATE(_schedule_timer, _total_timer->value() - _active_timer->value() - _pending_timer->value());
    COUNTER_UPDATE(_schedule_counter, driver_acct().get_schedule_times());
    COUNTER_UPDATE(_schedule_effective_counter, driver_acct().get_schedule_effective_times());
    COUNTER_UPDATE(_schedule_rows_per_chunk, driver_acct().get_rows_per_chunk());
    COUNTER_UPDATE(_schedule_accumulated_chunk_moved, driver_acct().get_accumulated_chunk_moved());
    _update_overhead_timer();

    // last root driver cancel the all drivers' execution and notify FE the
    // fragment's completion but do not unregister the FragmentContext because
    // some non-root drivers maybe has pending io io tasks hold the reference to
    // object owned by FragmentContext.
    if (is_root()) {
        if (_fragment_ctx->count_down_root_drivers()) {
            _fragment_ctx->finish();
            auto status = _fragment_ctx->final_status();
            _fragment_ctx->runtime_state()->exec_env()->driver_dispatcher()->report_exec_state(_fragment_ctx, status,
                                                                                               true);
        }
    }
    // last finished driver notify FE the fragment's completion again and
    // unregister the FragmentContext.
    if (_fragment_ctx->count_down_drivers()) {
        _fragment_ctx->destroy_pass_through_chunk_buffer();
        auto status = _fragment_ctx->final_status();
        auto fragment_id = _fragment_ctx->fragment_instance_id();
        _query_ctx->count_down_fragments();
    }
}

void PipelineDriver::_update_overhead_timer() {
    int64_t overhead_time = _active_timer->value();
    RuntimeProfile* profile = _runtime_profile.get();
    std::vector<RuntimeProfile*> children;
    profile->get_children(&children);
    for (auto* child_profile : children) {
        auto* total_timer = child_profile->get_counter("OperatorTotalTime");
        if (total_timer != nullptr) {
            overhead_time -= total_timer->value();
        }
    }

    COUNTER_UPDATE(_overhead_timer, overhead_time);
}

std::string PipelineDriver::to_readable_string() const {
    std::stringstream ss;
    ss << "driver=" << this << ", status=" << ds_to_string(this->driver_state()) << ", operator-chain: [";
    for (size_t i = 0; i < _operators.size(); ++i) {
        if (i == 0) {
            ss << _operators[i]->get_name();
        } else {
            ss << " -> " << _operators[i]->get_name();
        }
    }
    ss << "]";
    return ss.str();
}

starrocks::workgroup::WorkGroup* PipelineDriver::workgroup() {
    DCHECK(_workgroup != nullptr);
    return _workgroup;
}

void PipelineDriver::set_workgroup(starrocks::workgroup::WorkGroup* wg) {
    this->_workgroup = wg;
}

bool PipelineDriver::_check_fragment_is_canceled(RuntimeState* runtime_state) {
    if (_fragment_ctx->is_canceled()) {
        cancel_operators(runtime_state);
        // If the fragment is cancelled after the source operator commits an i/o task to i/o threads,
        // the driver cannot be finished immediately and should wait for the completion of the pending i/o task.
        if (is_still_pending_finish()) {
            set_driver_state(DriverState::PENDING_FINISH);
        } else {
            set_driver_state(_fragment_ctx->final_status().ok() ? DriverState::FINISH : DriverState::CANCELED);
        }

        return true;
    }

    return false;
}

void PipelineDriver::_mark_operator_finishing(OperatorPtr& op, RuntimeState* state) {
    auto& op_state = _operator_stages[op->get_id()];
    if (op_state >= OperatorStage::FINISHING) {
        return;
    }

    VLOG_ROW << strings::Substitute("[Driver] finishing operator [driver=$0] [operator=$1]", to_readable_string(),
                                    op->get_name());
    {
        SCOPED_TIMER(op->_finishing_timer);
        op->set_finishing(state);
    }
    op_state = OperatorStage::FINISHING;
}

void PipelineDriver::_mark_operator_finished(OperatorPtr& op, RuntimeState* state) {
    _mark_operator_finishing(op, state);
    auto& op_state = _operator_stages[op->get_id()];
    if (op_state >= OperatorStage::FINISHED) {
        return;
    }

    VLOG_ROW << strings::Substitute("[Driver] finished operator [driver=$0] [operator=$1]", to_readable_string(),
                                    op->get_name());
    {
        SCOPED_TIMER(op->_finished_timer);
        op->set_finished(state);
    }
    op_state = OperatorStage::FINISHED;
}

void PipelineDriver::_mark_operator_cancelled(OperatorPtr& op, RuntimeState* state) {
    _mark_operator_finished(op, state);
    auto& op_state = _operator_stages[op->get_id()];
    if (op_state >= OperatorStage::CANCELLED) {
        return;
    }

    VLOG_ROW << strings::Substitute("[Driver] cancelled operator [driver=$0] [operator=$1]", to_readable_string(),
                                    op->get_name());
    op->set_cancelled(state);
    op_state = OperatorStage::CANCELLED;
}

void PipelineDriver::_mark_operator_closed(OperatorPtr& op, RuntimeState* state) {
    if (_fragment_ctx->is_canceled()) {
        _mark_operator_cancelled(op, state);
    } else {
        _mark_operator_finished(op, state);
    }

    auto& op_state = _operator_stages[op->get_id()];
    if (op_state >= OperatorStage::CLOSED) {
        return;
    }

    VLOG_ROW << strings::Substitute("[Driver] close operator [driver=$0] [operator=$1]", to_readable_string(),
                                    op->get_name());
    {
        SCOPED_TIMER(op->_close_timer);
        op->close(state);
    }
    COUNTER_UPDATE(op->_total_timer, op->_pull_timer->value() + op->_push_timer->value() +
                                             op->_finishing_timer->value() + op->_finished_timer->value() +
                                             op->_close_timer->value());
    op_state = OperatorStage::CLOSED;
}

} // namespace starrocks::pipeline
