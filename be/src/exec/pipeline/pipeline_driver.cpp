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
    if (_state == DriverState::NOT_READY) {
        source_operator()->add_morsel_queue(_morsel_queue);
        for (auto& op : _operators) {
            RETURN_IF_ERROR(op->prepare(runtime_state));
        }
        _state = DriverState::READY;
    }
    return Status::OK();
}

StatusOr<DriverState> PipelineDriver::process(RuntimeState* runtime_state) {
    _state = DriverState::RUNNING;
    size_t total_chunks_moved = 0;
    int64_t time_spent = 0;
    while (true) {
        size_t num_chunk_moved = 0;
        bool should_yield = false;
        size_t num_operators = _operators.size();
        size_t _new_first_unfinished = _first_unfinished;
        VLOG_ROW << "[Driver] " << to_debug_string() << ", driver=" << this;
        for (size_t i = _first_unfinished; i < num_operators - 1; ++i) {
            {
                SCOPED_RAW_TIMER(&time_spent);
                auto& curr_op = _operators[i];
                auto& next_op = _operators[i + 1];

                // Check curr_op finished firstly
                if (curr_op->is_finished()) {
                    if (i == 0) {
                        // For source operators
                        VLOG_ROW << "[Driver] " << curr_op->get_name() << " finish, driver=" << this;
                        curr_op->finish(runtime_state);
                    }
                    VLOG_ROW << "[Driver] " << next_op->get_name() << " finish, driver=" << this;
                    next_op->finish(runtime_state);
                    _new_first_unfinished = i + 1;
                    continue;
                }

                // try successive operator pairs
                if (!curr_op->has_output() || !next_op->need_input()) {
                    continue;
                }

                // check whether fragment is finished beforehand before pull_chunk
                if (_fragment_ctx->is_canceled()) {
                    return _fragment_ctx->final_status().ok() ? DriverState::FINISH : DriverState::CANCELED;
                }

                // pull chunk from current operator and push the chunk onto next
                // operator
                auto maybe_chunk = curr_op->pull_chunk(runtime_state);
                auto status = maybe_chunk.status();
                if (!status.ok() && !status.is_end_of_file()) {
                    LOG(WARNING) << " status " << status.to_string();
                    return status;
                }

                // check whether fragment is finished beforehand before push_chunk
                if (_fragment_ctx->is_canceled()) {
                    return _fragment_ctx->final_status().ok() ? DriverState::FINISH : DriverState::CANCELED;
                }

                if (status.ok()) {
                    if (maybe_chunk.value() && maybe_chunk.value()->num_rows() > 0) {
                        VLOG_ROW << "[Driver] transfer chunk(" << maybe_chunk.value()->num_rows() << ") from "
                                 << curr_op->get_name() << " to " << next_op->get_name() << ", driver=" << this;
                        next_op->push_chunk(runtime_state, maybe_chunk.value());
                    }
                    num_chunk_moved += 1;
                    total_chunks_moved += 1;
                }

                // Check curr_op finished again
                if (curr_op->is_finished()) {
                    if (i == 0) {
                        // For source operators
                        VLOG_ROW << "[Driver] " << curr_op->get_name() << " finish, driver=" << this;
                        curr_op->finish(runtime_state);
                    }
                    VLOG_ROW << "[Driver] " << next_op->get_name() << " finish, driver=" << this;
                    next_op->finish(runtime_state);
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
            VLOG_ROW << "[Driver] " << _operators[i]->get_name() << " finish, driver=" << this;
            _operators[i]->finish(runtime_state);
            RETURN_IF_ERROR(_operators[i]->close(runtime_state));
        }
        _first_unfinished = _new_first_unfinished;

        if (sink_operator()->is_finished()) {
            _state = source_operator()->pending_finish() ? DriverState::PENDING_FINISH : DriverState::FINISH;
            return _state;
        }

        // no chunk moved in current round means that the driver is blocked.
        // should yield means that the CPU core is occupied the driver for the
        // a very long time so that the driver should switch off the core and
        // give chance for another ready driver to run.
        if (num_chunk_moved == 0 || should_yield) {
            driver_acct().increment_schedule_times();
            driver_acct().update_last_chunks_moved(total_chunks_moved);
            driver_acct().update_last_time_spent(time_spent);
            if (!sink_operator()->is_finished() && !sink_operator()->need_input()) {
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

void PipelineDriver::cancel(RuntimeState* state) {
    for (auto i = _first_unfinished; i < _operators.size(); ++i) {
        _operators[i]->finish(state);
    }
}

void PipelineDriver::finalize(RuntimeState* runtime_state, DriverState state) {
    VLOG_ROW << "[Driver] finalize, driver=" << this;
    if (state == DriverState::FINISH || state == DriverState::CANCELED || state == DriverState::INTERNAL_ERROR) {
        auto num_operators = _operators.size();
        for (auto i = _first_unfinished; i < num_operators; ++i) {
            _operators[i]->finish(runtime_state);
            _operators[i]->close(runtime_state);
        }
    } else {
        DCHECK(false);
    }
    _state = state;

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
        VLOG_ROW << "[Driver] Last driver finished: final_status=" << status.to_string();
        _query_ctx->count_down_fragments();
    }
}

std::string PipelineDriver::to_debug_string() const {
    std::stringstream ss;
    ss << "operator-chain: [";
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
} // namespace starrocks::pipeline
