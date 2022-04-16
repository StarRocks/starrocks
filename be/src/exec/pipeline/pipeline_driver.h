// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <gutil/bits.h>

#include <atomic>

#include "common/statusor.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/morsel.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/phmap/phmap.h"

namespace starrocks {

namespace pipeline {

class PipelineDriver;
using DriverPtr = std::shared_ptr<PipelineDriver>;
using Drivers = std::vector<DriverPtr>;

enum DriverState : uint32_t {
    NOT_READY = 0,
    READY = 1,
    RUNNING = 2,
    INPUT_EMPTY = 3,
    OUTPUT_FULL = 4,
    PRECONDITION_BLOCK = 5,
    FINISH = 6,
    CANCELED = 7,
    INTERNAL_ERROR = 8,
    // PENDING_FINISH means a driver's SinkOperator has finished, but its SourceOperator still have a pending
    // io task executed by io threads synchronously, a driver turns to FINISH from PENDING_FINISH after the
    // pending io task's completion.
    PENDING_FINISH = 9,
};

static inline std::string ds_to_string(DriverState ds) {
    switch (ds) {
    case NOT_READY:
        return "NOT_READY";
    case READY:
        return "READY";
    case RUNNING:
        return "RUNNING";
    case INPUT_EMPTY:
        return "INPUT_EMPTY";
    case OUTPUT_FULL:
        return "OUTPUT_FULL";
    case PRECONDITION_BLOCK:
        return "PRECONDITION_BLOCK";
    case FINISH:
        return "FINISH";
    case CANCELED:
        return "CANCELED";
    case INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    case PENDING_FINISH:
        return "PENDING_FINISH";
    }
    DCHECK(false);
    return "UNKNOWN_STATE";
}

// DriverAcct is used to keep statistics of drivers' runtime information, such as time spent
// on core, number of chunks already processed, which are taken into consideration by DriverQueue
// for schedule.
class DriverAcct {
public:
    DriverAcct() {}
    //TODO:
    // get_level return a non-negative value that is a hint used by DriverQueue to choose
    // the target internal queue for put_back.
    int get_level() { return Bits::Log2Floor64(schedule_times + 1); }

    // get last elapsed time for process.
    int64_t get_last_time_spent() { return last_time_spent; }

    void update_last_time_spent(int64_t time_spent) {
        this->last_time_spent = time_spent;
        this->accumulated_time_spent += time_spent;
    }
    void update_last_chunks_moved(int64_t chunks_moved) {
        this->last_chunks_moved = chunks_moved;
        this->accumulated_chunks_moved += chunks_moved;
        this->schedule_effective_times += (chunks_moved > 0) ? 1 : 0;
    }
    void update_accumulated_rows_moved(int64_t rows_moved) { this->accumulated_rows_moved += rows_moved; }
    void increment_schedule_times() { this->schedule_times += 1; }

    int64_t get_schedule_times() { return schedule_times; }

    int64_t get_schedule_effective_times() { return schedule_effective_times; }

    int64_t get_rows_per_chunk() {
        if (accumulated_chunks_moved > 0) {
            return accumulated_rows_moved / accumulated_chunks_moved;
        } else {
            return 0;
        }
    }

    int64_t get_accumulated_chunks_moved() { return accumulated_chunks_moved; }

    int64_t get_accumulated_time_spent() const { return accumulated_time_spent; }

private:
    int64_t schedule_times{0};
    int64_t schedule_effective_times{0};
    int64_t last_time_spent{0};
    int64_t last_chunks_moved{0};
    int64_t accumulated_time_spent{0};
    int64_t accumulated_chunks_moved{0};
    int64_t accumulated_rows_moved{0};
};

// OperatorExecState is used to guarantee that some hooks of operator
// is called exactly one time during whole operator
enum OperatorStage {
    INIT = 0,
    PREPARED = 1,
    PRECONDITION_NOT_READY = 2,
    PROCESSING = 3,
    FINISHING = 4,
    FINISHED = 5,
    CANCELLED = 6,
    CLOSED = 7,
};

class PipelineDriver {
    friend class PipelineDriverPoller;

public:
    PipelineDriver(const Operators& operators, QueryContext* query_ctx, FragmentContext* fragment_ctx,
                   int32_t driver_id)
            : _operators(operators),
              _first_unfinished(0),
              _query_ctx(query_ctx),
              _fragment_ctx(fragment_ctx),
              _source_node_id(operators[0]->get_plan_node_id()),
              _driver_id(driver_id),
              _state(DriverState::NOT_READY) {
        _runtime_profile = std::make_shared<RuntimeProfile>(strings::Substitute("PipelineDriver (id=$0)", _driver_id));
        for (auto& op : _operators) {
            _operator_stages[op->get_id()] = OperatorStage::INIT;
        }
    }

    PipelineDriver(const PipelineDriver& driver)
            : PipelineDriver(driver._operators, driver._query_ctx, driver._fragment_ctx, driver._driver_id) {}

    ~PipelineDriver();

    QueryContext* query_ctx() { return _query_ctx; }
    FragmentContext* fragment_ctx() { return _fragment_ctx; }
    int32_t source_node_id() { return _source_node_id; }
    int32_t driver_id() const { return _driver_id; }
    DriverPtr clone() { return std::make_shared<PipelineDriver>(*this); }
    void set_morsel_queue(MorselQueuePtr morsel_queue) { _morsel_queue = std::move(morsel_queue); }
    Status prepare(RuntimeState* runtime_state);
    StatusOr<DriverState> process(RuntimeState* runtime_state, int worker_id);
    void finalize(RuntimeState* runtime_state, DriverState state);
    DriverAcct& driver_acct() { return _driver_acct; }
    DriverState driver_state() const { return _state; }

    void set_driver_state(DriverState state) {
        if (state == _state) {
            return;
        }

        switch (_state) {
        case DriverState::INPUT_EMPTY: {
            auto elapsed_time = _input_empty_timer_sw->elapsed_time();
            if (_first_input_empty_timer->value() == 0) {
                _first_input_empty_timer->update(elapsed_time);
            } else {
                _followup_input_empty_timer->update(elapsed_time);
            }
            _input_empty_timer->update(elapsed_time);
            break;
        }
        case DriverState::OUTPUT_FULL:
            _output_full_timer->update(_output_full_timer_sw->elapsed_time());
            break;
        case DriverState::PRECONDITION_BLOCK: {
            _precondition_block_timer->update(_precondition_block_timer_sw->elapsed_time());
            break;
        }
        default:
            break;
        }

        switch (state) {
        case DriverState::INPUT_EMPTY:
            _input_empty_timer_sw->reset();
            break;
        case DriverState::OUTPUT_FULL:
            _output_full_timer_sw->reset();
            break;
        case DriverState::PRECONDITION_BLOCK:
            _precondition_block_timer_sw->reset();
            break;
        default:
            break;
        }

        _state = state;
    }

    Operators& operators() { return _operators; }
    SourceOperator* source_operator() { return down_cast<SourceOperator*>(_operators.front().get()); }
    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    // drivers that waits for runtime filters' readiness must be marked PRECONDITION_NOT_READY and put into
    // PipelineDriverPoller.
    void mark_precondition_not_ready();

    // drivers in PRECONDITION_BLOCK state must be marked READY after its dependent runtime-filters or hash tables
    // are finished.
    void mark_precondition_ready(RuntimeState* runtime_state);
    void submit_operators();
    // Notify all the unfinished operators to be finished.
    // It is usually used when the sink operator is finished, or the fragment is cancelled or expired.
    void finish_operators(RuntimeState* runtime_state);
    void cancel_operators(RuntimeState* runtime_state);

    Operator* sink_operator() { return _operators.back().get(); }
    bool is_finished() {
        return _state == DriverState::FINISH || _state == DriverState::CANCELED ||
               _state == DriverState::INTERNAL_ERROR;
    }
    bool pending_finish() { return _state == DriverState::PENDING_FINISH; }
    bool is_still_pending_finish() { return source_operator()->pending_finish() || sink_operator()->pending_finish(); }
    // return false if all the dependencies are ready, otherwise return true.
    bool dependencies_block() {
        if (_all_dependencies_ready) {
            return false;
        }
        _all_dependencies_ready =
                std::all_of(_dependencies.begin(), _dependencies.end(), [](auto& dep) { return dep->is_ready(); });
        return !_all_dependencies_ready;
    }

    // return false if all the local runtime filters are ready, otherwise return false.
    bool local_rf_block() {
        if (_all_local_rf_ready) {
            return false;
        }
        _all_local_rf_ready = std::all_of(_local_rf_holders.begin(), _local_rf_holders.end(),
                                          [](auto* holder) { return holder->is_ready(); });
        return !_all_local_rf_ready;
    }

    bool global_rf_block() {
        if (_all_global_rf_ready_or_timeout) {
            return false;
        }

        _all_global_rf_ready_or_timeout =
                _precondition_block_timer_sw->elapsed_time() >= _global_rf_wait_timeout_ns || // Timeout,
                std::all_of(_global_rf_descriptors.begin(), _global_rf_descriptors.end(),
                            [](auto* rf_desc) { return rf_desc->runtime_filter() != nullptr; }); // or ready.

        return !_all_global_rf_ready_or_timeout;
    }

    // return true if either dependencies_block or local_rf_block return true, which means that the current driver
    // should wait for both hash table and local runtime filters' readiness.
    bool is_precondition_block() {
        if (!_wait_global_rf_ready) {
            if (dependencies_block() || local_rf_block()) {
                return true;
            }
            _wait_global_rf_ready = true;
            if (_global_rf_descriptors.empty()) {
                return false;
            }
            // wait global rf to be ready for at most _global_rf_wait_time_out_ns after
            // both dependencies_block and local_rf_block return false.
            _global_rf_wait_timeout_ns += _precondition_block_timer_sw->elapsed_time();
            return global_rf_block();
        } else {
            return global_rf_block();
        }
    }

    bool is_not_blocked() {
        // If the sink operator is finished, the rest operators of this driver needn't be executed anymore.
        if (sink_operator()->is_finished()) {
            return true;
        }

        // PRECONDITION_BLOCK
        if (_state == DriverState::PRECONDITION_BLOCK) {
            if (is_precondition_block()) {
                return false;
            }

            // TODO(trueeyu): This writing is to ensure that MemTracker will not be destructed before the thread ends.
            //  This writing method is a bit tricky, and when there is a better way, replace it
            mark_precondition_ready(_runtime_state);

            check_short_circuit();
            if (_state == DriverState::PENDING_FINISH) {
                return false;
            }
            // Driver state must be set to a state different from PRECONDITION_BLOCK bellow,
            // to avoid call mark_precondition_ready() and check_short_circuit() multiple times.
        }

        // OUTPUT_FULL
        if (!sink_operator()->need_input()) {
            set_driver_state(DriverState::OUTPUT_FULL);
            return false;
        }

        // INPUT_EMPTY
        if (!source_operator()->is_finished() && !source_operator()->has_output()) {
            set_driver_state(DriverState::INPUT_EMPTY);
            return false;
        }

        return true;
    }

    // Check whether an operator can be short-circuited, when is_precondition_block() becomes false from true.
    void check_short_circuit();

    std::string to_readable_string() const;

    workgroup::WorkGroup* workgroup();
    void set_workgroup(workgroup::WorkGroupPtr wg);

    size_t get_driver_queue_level() const { return _driver_queue_level; }
    void set_driver_queue_level(size_t driver_queue_level) { _driver_queue_level = driver_queue_level; }

private:
    // Yield PipelineDriver when maximum number of chunks has been moved in current execution round.
    static constexpr size_t YIELD_MAX_CHUNKS_MOVED = 100;
    // Yield PipelineDriver when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
    // Yield PipelineDriver when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT = 20'000'000L;

    // check whether fragment is cancelled. It is used before pull_chunk and push_chunk.
    bool _check_fragment_is_canceled(RuntimeState* runtime_state);
    Status _mark_operator_finishing(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_finished(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_cancelled(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_closed(OperatorPtr& op, RuntimeState* runtime_state);
    void _close_operators(RuntimeState* runtime_state);

    // Update metrics when the driver yields.
    void _update_statistics(size_t total_chunks_moved, size_t total_rows_moved, size_t time_spent) {
        driver_acct().increment_schedule_times();
        driver_acct().update_last_chunks_moved(total_chunks_moved);
        driver_acct().update_accumulated_rows_moved(total_rows_moved);
        driver_acct().update_last_time_spent(time_spent);
    }

    RuntimeState* _runtime_state = nullptr;
    void _update_overhead_timer();

    Operators _operators;

    DriverDependencies _dependencies;
    bool _all_dependencies_ready = false;

    mutable std::vector<RuntimeFilterHolder*> _local_rf_holders;
    bool _all_local_rf_ready = false;

    std::vector<vectorized::RuntimeFilterProbeDescriptor*> _global_rf_descriptors;
    bool _wait_global_rf_ready = false;
    bool _all_global_rf_ready_or_timeout = false;
    int64_t _global_rf_wait_timeout_ns = -1;

    size_t _first_unfinished;
    QueryContext* _query_ctx;
    FragmentContext* _fragment_ctx;
    // The default value -1 means no source
    int32_t _source_node_id = -1;
    int32_t _driver_id;
    DriverAcct _driver_acct;
    // The first one is source operator
    MorselQueuePtr _morsel_queue = nullptr;
    // _state must be set by set_driver_state() to record state timer.
    DriverState _state;
    std::shared_ptr<RuntimeProfile> _runtime_profile = nullptr;

    phmap::flat_hash_map<int32_t, OperatorStage> _operator_stages;

    workgroup::WorkGroupPtr _workgroup = nullptr;
    // The index of QuerySharedDriverQueue{WithoutLock}._queues which this driver belongs to.
    size_t _driver_queue_level = 0;

    // metrics
    RuntimeProfile::Counter* _total_timer = nullptr;
    RuntimeProfile::Counter* _active_timer = nullptr;
    RuntimeProfile::Counter* _overhead_timer = nullptr;
    RuntimeProfile::Counter* _schedule_timer = nullptr;
    RuntimeProfile::Counter* _pending_timer = nullptr;
    RuntimeProfile::Counter* _precondition_block_timer = nullptr;
    RuntimeProfile::Counter* _input_empty_timer = nullptr;
    RuntimeProfile::Counter* _first_input_empty_timer = nullptr;
    RuntimeProfile::Counter* _followup_input_empty_timer = nullptr;
    RuntimeProfile::Counter* _output_full_timer = nullptr;

    MonotonicStopWatch* _total_timer_sw = nullptr;
    MonotonicStopWatch* _pending_timer_sw = nullptr;
    MonotonicStopWatch* _precondition_block_timer_sw = nullptr;
    MonotonicStopWatch* _input_empty_timer_sw = nullptr;
    MonotonicStopWatch* _output_full_timer_sw = nullptr;
};

} // namespace pipeline
} // namespace starrocks
