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

#include <gutil/bits.h>

#include <atomic>
#include <chrono>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "fmt/printf.h"
#include "runtime/mem_tracker.h"
#include "util/phmap/phmap.h"

namespace starrocks {

namespace query_cache {
class MultilaneOperator;
using MultilaneOperatorRawPtr = MultilaneOperator*;
using MultilaneOperators = std::vector<MultilaneOperatorRawPtr>;
} // namespace query_cache

namespace pipeline {

class PipelineDriver;
using DriverPtr = std::shared_ptr<PipelineDriver>;
using Drivers = std::vector<DriverPtr>;
using IterateImmutableDriverFunc = std::function<void(DriverConstRawPtr)>;
using ImmutableDriverPredicateFunc = std::function<bool(DriverConstRawPtr)>;
class DriverQueue;

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
    EPOCH_PENDING_FINISH = 10,
    EPOCH_FINISH = 11,
    // In some cases, the output of SourceOperator::has_output may change frequently, it's better to wait
    // in the working thread other than moving the driver frequently between ready queue and pending queue, which
    // will lead to drastic performance deduction (the "ScheduleTime" in profile will be super high).
    // We can enable this optimization by overriding SourceOperator::is_mutable to return true.
    LOCAL_WAITING = 12
};

[[maybe_unused]] static inline std::string ds_to_string(DriverState ds) {
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
    case EPOCH_PENDING_FINISH:
        return "EPOCH_PENDING_FINISH";
    case EPOCH_FINISH:
        return "EPOCH_FINISH";
    case LOCAL_WAITING:
        return "LOCAL_WAITING";
    }
    DCHECK(false);
    return "UNKNOWN_STATE";
}

// DriverAcct is used to keep statistics of drivers' runtime information, such as time spent
// on core, number of chunks already processed, which are taken into consideration by DriverQueue
// for schedule.
class DriverAcct {
public:
    DriverAcct() = default;
    //TODO:
    // get_level return a non-negative value that is a hint used by DriverQueue to choose
    // the target internal queue for put_back.
    int get_level() { return Bits::Log2Floor64(schedule_times + 1); }

    // get last elapsed time for process.
    int64_t get_last_time_spent() { return last_time_spent; }

    void update_last_time_spent(int64_t time_spent) {
        this->last_time_spent = time_spent;
        this->accumulated_time_spent += time_spent;
        this->accumulated_local_wait_time_spent += time_spent;
    }
    // This method must be invoked when adding back to ready queue or pending queue.
    void clean_local_queue_infos() {
        this->accumulated_local_wait_time_spent = 0;
        this->enter_local_queue_timestamp = 0;
    }
    void update_enter_local_queue_timestamp() {
        enter_local_queue_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now().time_since_epoch())
                                              .count();
    }
    void update_last_chunks_moved(int64_t chunks_moved) {
        this->last_chunks_moved = chunks_moved;
        this->accumulated_chunks_moved += chunks_moved;
        this->schedule_effective_times += (chunks_moved > 0) ? 1 : 0;
    }
    void update_accumulated_rows_moved(int64_t rows_moved) { this->accumulated_rows_moved += rows_moved; }
    void increment_schedule_times() { this->schedule_times += 1; }

    int64_t get_accumulated_local_wait_time_spent() { return accumulated_local_wait_time_spent; }
    int64_t get_local_queue_time_spent() {
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now().time_since_epoch())
                                 .count();
        return now - enter_local_queue_timestamp;
    }
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
    int64_t enter_local_queue_timestamp{0};
    int64_t accumulated_time_spent{0};
    int64_t accumulated_local_wait_time_spent{0};
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
    EPOCH_FINISHING = 4,
    EPOCH_FINISHED = 5,
    FINISHING = 6,
    FINISHED = 7,
    CANCELLED = 8,
    CLOSED = 9,
};

class PipelineDriver {
    friend class PipelineDriverPoller;

public:
    PipelineDriver(const Operators& operators, QueryContext* query_ctx, FragmentContext* fragment_ctx,
                   Pipeline* pipeline, int32_t driver_id)
            : _operators(operators),
              _query_ctx(query_ctx),
              _fragment_ctx(fragment_ctx),
              _pipeline(pipeline),
              _source_node_id(operators[0]->get_plan_node_id()),
              _driver_id(driver_id) {
        _runtime_profile = std::make_shared<RuntimeProfile>(strings::Substitute("PipelineDriver (id=$0)", _driver_id));
        for (auto& op : _operators) {
            _operator_stages[op->get_id()] = OperatorStage::INIT;
        }
        _driver_name = fmt::sprintf("driver_%d_%d", _source_node_id, _driver_id);
    }

    PipelineDriver(const PipelineDriver& driver)
            : PipelineDriver(driver._operators, driver._query_ctx, driver._fragment_ctx, driver._pipeline,
                             driver._driver_id) {}

    virtual ~PipelineDriver() noexcept;
    void check_operator_close_states(std::string func_name);

    QueryContext* query_ctx() { return _query_ctx; }
    const QueryContext* query_ctx() const { return _query_ctx; }
    FragmentContext* fragment_ctx() { return _fragment_ctx; }
    const FragmentContext* fragment_ctx() const { return _fragment_ctx; }
    int32_t source_node_id() { return _source_node_id; }
    int32_t driver_id() const { return _driver_id; }
    DriverPtr clone() { return std::make_shared<PipelineDriver>(*this); }
    void set_morsel_queue(MorselQueue* morsel_queue) { _morsel_queue = morsel_queue; }
    [[nodiscard]] Status prepare(RuntimeState* runtime_state);
    [[nodiscard]] virtual StatusOr<DriverState> process(RuntimeState* runtime_state, int worker_id);
    void finalize(RuntimeState* runtime_state, DriverState state, int64_t schedule_count, int64_t execution_time);
    DriverAcct& driver_acct() { return _driver_acct; }
    DriverState driver_state() const { return _state; }

    void increment_schedule_times();

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
        case DriverState::PRECONDITION_BLOCK:
            _precondition_block_timer->update(_precondition_block_timer_sw->elapsed_time());
            break;
        case DriverState::PENDING_FINISH:
            _pending_finish_timer->update(_pending_finish_timer_sw->elapsed_time());
            break;
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
        case DriverState::PENDING_FINISH:
            _pending_finish_timer_sw->reset();
            break;
        default:
            break;
        }

        _state = state;
    }

    Operators& operators() { return _operators; }
    ScanOperator* source_scan_operator() {
        return _operators.empty() ? nullptr : dynamic_cast<ScanOperator*>(_operators.front().get());
    }
    SourceOperator* source_operator() {
        return _operators.empty() ? nullptr : down_cast<SourceOperator*>(_operators.front().get());
    }
    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    void update_peak_driver_queue_size_counter(size_t new_value);
    // drivers that waits for runtime filters' readiness must be marked PRECONDITION_NOT_READY and put into
    // PipelineDriverPoller.
    void mark_precondition_not_ready();

    // drivers in PRECONDITION_BLOCK state must be marked READY after its dependent runtime-filters or hash tables
    // are finished.
    void mark_precondition_ready(RuntimeState* runtime_state);
    void start_timers();
    void stop_timers();
    int64_t get_active_time() const { return _active_timer->value(); }
    void submit_operators();
    // Notify all the unfinished operators to be finished.
    // It is usually used when the sink operator is finished, or the fragment is cancelled or expired.
    void finish_operators(RuntimeState* runtime_state);
    void cancel_operators(RuntimeState* runtime_state);

    Operator* sink_operator() { return _operators.back().get(); }
    bool is_ready() {
        return _state == DriverState::READY || _state == DriverState::RUNNING || _state == DriverState::LOCAL_WAITING;
    }
    bool is_finished() {
        return _state == DriverState::FINISH || _state == DriverState::CANCELED ||
               _state == DriverState::INTERNAL_ERROR;
    }
    bool pending_finish() { return _state == DriverState::PENDING_FINISH; }
    bool is_still_pending_finish() { return source_operator()->pending_finish() || sink_operator()->pending_finish(); }
    // return false if all the dependencies are ready, otherwise return true.
    bool dependencies_block();

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
                std::all_of(_global_rf_descriptors.begin(), _global_rf_descriptors.end(), [](auto* rf_desc) {
                    return rf_desc->is_local() || rf_desc->runtime_filter() != nullptr;
                }); // or all the remote RFs are ready.

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

    std::string get_preconditions_block_reasons() {
        if (_state == DriverState::PRECONDITION_BLOCK) {
            return std::string(dependencies_block() ? "(dependencies," : "(") +
                   std::string(global_rf_block() ? "global runtime filter," : "") +
                   std::string(local_rf_block() ? "local runtime filter)" : ")");
        } else {
            return "";
        }
    }

    StatusOr<bool> is_not_blocked() {
        // If the sink operator is finished, the rest operators of this driver needn't be executed anymore.
        if (sink_operator()->is_finished()) {
            return true;
        }
        if (source_operator()->is_epoch_finished() || sink_operator()->is_epoch_finished()) {
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

            RETURN_IF_ERROR(check_short_circuit());
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
    [[nodiscard]] Status check_short_circuit();

    bool need_report_exec_state();
    void report_exec_state_if_necessary();
    void runtime_report_action();

    std::string to_readable_string() const;

    workgroup::WorkGroup* workgroup();
    const workgroup::WorkGroup* workgroup() const;
    void set_workgroup(workgroup::WorkGroupPtr wg);

    void set_in_queue(DriverQueue* in_queue) { _in_queue = in_queue; }
    size_t get_driver_queue_level() const { return _driver_queue_level; }
    void set_driver_queue_level(size_t driver_queue_level) { _driver_queue_level = driver_queue_level; }

    inline bool is_in_ready_queue() const { return _in_ready_queue.load(std::memory_order_acquire); }
    void set_in_ready_queue(bool v) { _in_ready_queue.store(v, std::memory_order_release); }

    inline std::string get_name() const { return strings::Substitute("PipelineDriver (id=$0)", _driver_id); }

    // Whether the query can be expirable or not.
    virtual bool is_query_never_expired() { return false; }
    // Whether the driver's state is already `EPOCH_FINISH`.
    bool is_epoch_finished() { return _state == DriverState::EPOCH_FINISH; }
    // Whether the driver's state is already `EPOCH_PENDING_FINISH`.
    bool is_epoch_finishing() { return _state == DriverState::EPOCH_PENDING_FINISH; }
    // Whether the driver is at finishing state in one epoch. when the driver is in `EPOCH_PENDING_FINISH` state,
    // use `is_still_epoch_finishing` method to check whether the driver has changed yet.
    bool is_still_epoch_finishing() {
        return source_operator()->is_epoch_finishing() || sink_operator()->is_epoch_finishing();
    }

protected:
    PipelineDriver()
            : _operators(),
              _query_ctx(nullptr),
              _fragment_ctx(nullptr),
              _pipeline(nullptr),
              _source_node_id(0),
              _driver_id(0) {}

    // Yield PipelineDriver when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT_NS = 100'000'000L;
    // Yield PipelineDriver when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT_NS = 5'000'000L;
    // Execution time exceed this is considered overloaded
    static constexpr int64_t OVERLOADED_MAX_TIME_SPEND_NS = 150'000'000L;

    // check whether fragment is cancelled. It is used before pull_chunk and push_chunk.
    bool _check_fragment_is_canceled(RuntimeState* runtime_state);
    [[nodiscard]] Status _mark_operator_finishing(OperatorPtr& op, RuntimeState* runtime_state);
    [[nodiscard]] Status _mark_operator_finished(OperatorPtr& op, RuntimeState* runtime_state);
    [[nodiscard]] Status _mark_operator_cancelled(OperatorPtr& op, RuntimeState* runtime_state);
    [[nodiscard]] Status _mark_operator_closed(OperatorPtr& op, RuntimeState* runtime_state);
    void _close_operators(RuntimeState* runtime_state);

    void _adjust_memory_usage(RuntimeState* state, MemTracker* tracker, OperatorPtr& op, const ChunkPtr& chunk);
    void _try_to_release_buffer(RuntimeState* state, OperatorPtr& op);

    // Update metrics when the driver yields.
    void _update_driver_acct(size_t total_chunks_moved, size_t total_rows_moved, size_t time_spent);
    void _update_statistics(RuntimeState* state, size_t total_chunks_moved, size_t total_rows_moved, size_t time_spent);
    void _update_scan_statistics(RuntimeState* state);
    void _update_driver_level_timer();

    RuntimeState* _runtime_state = nullptr;
    Operators _operators;
    DriverDependencies _dependencies;
    bool _all_dependencies_ready = false;

    mutable std::vector<RuntimeFilterHolder*> _local_rf_holders;
    bool _all_local_rf_ready = false;

    std::vector<RuntimeFilterProbeDescriptor*> _global_rf_descriptors;
    bool _wait_global_rf_ready = false;
    bool _all_global_rf_ready_or_timeout = false;
    int64_t _global_rf_wait_timeout_ns = -1;

    size_t _first_unfinished{0};
    QueryContext* _query_ctx;
    FragmentContext* _fragment_ctx;
    Pipeline* _pipeline;
    // The default value -1 means no source
    int32_t _source_node_id = -1;
    int32_t _driver_id;
    std::string _driver_name;
    DriverAcct _driver_acct;
    // The first one is source operator
    MorselQueue* _morsel_queue = nullptr;
    // _state must be set by set_driver_state() to record state timer.
    DriverState _state{DriverState::NOT_READY};
    std::shared_ptr<RuntimeProfile> _runtime_profile = nullptr;

    phmap::flat_hash_map<int32_t, OperatorStage> _operator_stages;

    workgroup::WorkGroupPtr _workgroup = nullptr;
    DriverQueue* _in_queue = nullptr;
    // The index of QuerySharedDriverQueue._queues which this driver belongs to.
    size_t _driver_queue_level = 0;
    std::atomic<bool> _in_ready_queue{false};

    // metrics
    RuntimeProfile::Counter* _total_timer = nullptr;
    RuntimeProfile::Counter* _active_timer = nullptr;
    RuntimeProfile::Counter* _overhead_timer = nullptr;
    RuntimeProfile::Counter* _schedule_timer = nullptr;

    // Schedule counters
    // Record global schedule count during this driver lifecycle
    RuntimeProfile::Counter* _schedule_counter = nullptr;
    RuntimeProfile::Counter* _yield_by_time_limit_counter = nullptr;
    RuntimeProfile::Counter* _yield_by_preempt_counter = nullptr;
    RuntimeProfile::Counter* _yield_by_local_wait_counter = nullptr;
    RuntimeProfile::Counter* _block_by_precondition_counter = nullptr;
    RuntimeProfile::Counter* _block_by_output_full_counter = nullptr;
    RuntimeProfile::Counter* _block_by_input_empty_counter = nullptr;

    RuntimeProfile::Counter* _pending_timer = nullptr;
    RuntimeProfile::Counter* _precondition_block_timer = nullptr;
    RuntimeProfile::Counter* _input_empty_timer = nullptr;
    RuntimeProfile::Counter* _first_input_empty_timer = nullptr;
    RuntimeProfile::Counter* _followup_input_empty_timer = nullptr;
    RuntimeProfile::Counter* _output_full_timer = nullptr;
    RuntimeProfile::Counter* _pending_finish_timer = nullptr;

    MonotonicStopWatch* _total_timer_sw = nullptr;
    MonotonicStopWatch* _pending_timer_sw = nullptr;
    MonotonicStopWatch* _precondition_block_timer_sw = nullptr;
    MonotonicStopWatch* _input_empty_timer_sw = nullptr;
    MonotonicStopWatch* _output_full_timer_sw = nullptr;
    MonotonicStopWatch* _pending_finish_timer_sw = nullptr;

    RuntimeProfile::HighWaterMarkCounter* _peak_driver_queue_size_counter = nullptr;
};

} // namespace pipeline
} // namespace starrocks
