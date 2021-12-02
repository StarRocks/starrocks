// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks
// Limited.

#include "exec/pipeline/pipeline_driver.h"

#include <sstream>

#include "column/chunk.h"
#include "exec/pipeline/source_operator.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status PipelineDriver::prepare(RuntimeState* runtime_state) {
    _total_timer = ADD_TIMER(_runtime_profile, "DriverTotalTime");
    _active_timer = ADD_TIMER(_runtime_profile, "DriverActiveTime");
    _pending_timer = ADD_TIMER(_runtime_profile, "DriverPendingTime");
    _precondition_block_timer = ADD_CHILD_TIMER(_runtime_profile, "DriverPreconditionBlockTime", "DriverPendingTime");
    _local_rf_waiting_set_counter = ADD_COUNTER(_runtime_profile, "LocalRfWaitingSet", TUnit::UNIT);

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
    _state = DriverState::READY;

    _total_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _pending_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _precondition_block_timer_sw = runtime_state->obj_pool()->add(new MonotonicStopWatch());
    _total_timer_sw->start();
    _pending_timer_sw->start();
    _precondition_block_timer_sw->start();

    return Status::OK();
}

StatusOr<DriverState> PipelineDriver::process(RuntimeState* runtime_state) {
    // VLOG_ROW << "[Driver] enter process: " << this->to_readable_string();
    SCOPED_TIMER(_active_timer);
    _state = DriverState::RUNNING;
    size_t total_chunks_moved = 0;
    int64_t time_spent = 0;
    while (true) {
        RETURN_IF_LIMIT_EXCEEDED(runtime_state, "Pipeline");

        size_t num_chunk_moved = 0;
        bool should_yield = false;
        size_t num_operators = _operators.size();
        size_t _new_first_unfinished = _first_unfinished;
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
                    _new_first_unfinished = i + 1;
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
                        //VLOG_ROW << "[Driver] Transfer chunk: num_rows=" << row_num << ", "
                        //         << this->to_readable_string();
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
                    _new_first_unfinished = i + 1;
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
        for (auto i = _first_unfinished; i < _new_first_unfinished; ++i) {
            _mark_operator_finished(_operators[i], runtime_state);
        }
        _first_unfinished = _new_first_unfinished;

        if (sink_operator()->is_finished()) {
            finish_operators(runtime_state);
            _state = is_still_pending_finish() ? DriverState::PENDING_FINISH : DriverState::FINISH;
            return _state;
        }

        // no chunk moved in current round means that the driver is blocked.
        // should yield means that the CPU core is occupied the driver for a
        // very long time so that the driver should switch off the core and
        // give chance for another ready driver to run.
        if (num_chunk_moved == 0 || should_yield) {
            driver_acct().increment_schedule_times();
            driver_acct().update_last_chunks_moved(total_chunks_moved);
            driver_acct().update_last_time_spent(time_spent);
            if (is_precondition_block()) {
                _state = DriverState::PRECONDITION_BLOCK;
                return DriverState::PRECONDITION_BLOCK;
            } else if (!sink_operator()->is_finished() && !sink_operator()->need_input()) {
                _state = DriverState::OUTPUT_FULL;
                return DriverState::OUTPUT_FULL;
            }
            if (!source_operator()->is_finished() && !source_operator()->has_output()) {
                _state = DriverState::INPUT_EMPTY;
                return DriverState::INPUT_EMPTY;
            }
            _state = DriverState::READY;
            return DriverState::READY;
        }
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
void PipelineDriver::_close_operators(RuntimeState* runtime_state) {
    for (auto& op : _operators) {
        _mark_operator_closed(op, runtime_state);
    }
}

void PipelineDriver::finalize(RuntimeState* runtime_state, DriverState state) {
    VLOG_ROW << "[Driver] finalize, driver=" << this;
    DCHECK(state == DriverState::FINISH || state == DriverState::CANCELED || state == DriverState::INTERNAL_ERROR);

    _close_operators(runtime_state);

    _state = state;

    // Calculate total time before report profile
    _total_timer->update(_total_timer_sw->elapsed_time());

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
        auto status = _fragment_ctx->final_status();
        auto fragment_id = _fragment_ctx->fragment_instance_id();
        // VLOG_ROW << "[Driver] Last driver finished: final_status=" << status.to_string();
        _query_ctx->count_down_fragments();
    }
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

bool PipelineDriver::_check_fragment_is_canceled(RuntimeState* runtime_state) {
    if (_fragment_ctx->is_canceled()) {
        finish_operators(runtime_state);
        // If the fragment is cancelled after the source operator commits an i/o task to i/o threads,
        // the driver cannot be finished immediately and should wait for the completion of the pending i/o task.
        if (is_still_pending_finish()) {
            _state = DriverState::PENDING_FINISH;
        } else {
            _state = _fragment_ctx->final_status().ok() ? DriverState::FINISH : DriverState::CANCELED;
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
    op->set_finishing(state);
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
    op->set_finished(state);
    op_state = OperatorStage::FINISHED;
}

void PipelineDriver::_mark_operator_closed(OperatorPtr& op, RuntimeState* state) {
    _mark_operator_finished(op, state);
    auto& op_state = _operator_stages[op->get_id()];
    if (op_state >= OperatorStage::CLOSED) {
        return;
    }

    VLOG_ROW << strings::Substitute("[Driver] close operator [driver=$0] [operator=$1]", to_readable_string(),
                                    op->get_name());
    op->close(state);
    op_state = OperatorStage::CLOSED;
}

} // namespace starrocks::pipeline
