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

#include <atomic>
#include <memory>

#include "base/phmap/phmap.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "compute_env/pipeline/pipeline_timer.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/primitives/driver_state.h"
#include "exec/pipeline/runtime_filter_hub.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/pipeline/schedule/common.h"
#include "exec/pipeline/schedule/pipeline_driver_observer.h"
#include "exec/pipeline/source_operator.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "fmt/printf.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks {

namespace query_cache {
class MultilaneOperator;
using MultilaneOperatorRawPtr = MultilaneOperator*;
using MultilaneOperators = std::vector<MultilaneOperatorRawPtr>;
} // namespace query_cache

namespace spill {
class OperatorMemoryResourceManager;
} // namespace spill

namespace pipeline {

class PipelineDriver;
using DriverPtr = std::shared_ptr<PipelineDriver>;
using Drivers = std::vector<DriverPtr>;
class DriverQueue;

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
    // used in event scheduler
    // If event_scheduler doesn't get the token, it proves that the observer has entered the critical zone,
    // and we don't know the real state of the driver, so we need to try scheduling the driver one more time.
    class ScheduleToken {
    public:
        ScheduleToken(DriverRawPtr driver, bool acquired) : _driver(driver), _acquired(acquired) {}
        ~ScheduleToken() {
            if (_acquired) {
                _driver->_schedule_token = true;
            }
        }

        ScheduleToken(const ScheduleToken&) = delete;
        void operator=(const ScheduleToken&) = delete;

        bool acquired() const { return _acquired; }

    private:
        DriverRawPtr _driver;
        bool _acquired;
    };

public:
    PipelineDriver(const Operators& operators, QueryContext* query_ctx, FragmentContext* fragment_ctx,
                   Pipeline* pipeline, DriverObserver* driver_observer, int32_t driver_id);

    PipelineDriver(const PipelineDriver& driver);

    virtual ~PipelineDriver() noexcept;
    void check_operator_close_states(const std::string& func_name);

    QueryContext* query_ctx() { return _query_ctx; }
    const QueryContext* query_ctx() const { return _query_ctx; }
    QueryRuntimeState* query_runtime_state() { return _query_runtime_state; }
    const QueryRuntimeState* query_runtime_state() const { return _query_runtime_state; }
    FragmentRuntimeState* fragment_runtime_state() { return _fragment_runtime_state; }
    const FragmentRuntimeState* fragment_runtime_state() const { return _fragment_runtime_state; }
    FragmentContext* fragment_ctx() { return _fragment_ctx; }
    const FragmentContext* fragment_ctx() const { return _fragment_ctx; }
    int32_t source_node_id() { return _source_node_id; }
    int32_t driver_id() const { return _driver_id; }
    DriverPtr clone() { return std::make_shared<PipelineDriver>(*this); }
    void set_morsel_queue(MorselQueue* morsel_queue) { _morsel_queue = morsel_queue; }
    Status prepare(RuntimeState* runtime_state);
    virtual StatusOr<DriverState> process(RuntimeState* runtime_state, int worker_id);
    void finalize(RuntimeState* runtime_state, DriverState state);
    DriverAcct& driver_acct() { return _driver_acct; }
    DriverState driver_state() const { return _state; }

    Status prepare_local_state(RuntimeState* runtime_state);

    void increment_schedule_times();

    void set_driver_state(DriverState state) {
        if (state == _state) {
            return;
        }

        switch (_state) {
        case DriverState::INPUT_EMPTY: {
            auto elapsed_time = _input_empty_timer_sw->elapsed_time();
            if (COUNTER_VALUE(_first_input_empty_timer) == 0) {
                COUNTER_UPDATE(_first_input_empty_timer, elapsed_time);
            } else {
                COUNTER_UPDATE(_followup_input_empty_timer, elapsed_time);
            }
            COUNTER_UPDATE(_input_empty_timer, elapsed_time);
            break;
        }
        case DriverState::OUTPUT_FULL:
            COUNTER_UPDATE(_output_full_timer, _output_full_timer_sw->elapsed_time());
            break;
        case DriverState::PRECONDITION_BLOCK:
            COUNTER_UPDATE(_precondition_block_timer, _precondition_block_timer_sw->elapsed_time());
            break;
        case DriverState::PENDING_FINISH:
            COUNTER_UPDATE(_pending_finish_timer, _pending_finish_timer_sw->elapsed_time());
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
            DCHECK_EQ(_state, DriverState::READY);
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
    void mark_precondition_ready();
    bool precondition_prepared() const { return _precondition_prepared; }
    void start_timers();
    void stop_timers();
    int64_t get_active_time() const { return COUNTER_VALUE(_active_timer); }
    void submit_operators();
    // Notify all the unfinished operators to be finished.
    // It is usually used when the sink operator is finished, or the fragment is cancelled or expired.
    void finish_operators(RuntimeState* runtime_state);
    void cancel_operators(RuntimeState* runtime_state);

    Operator* sink_operator() { return _operators.back().get(); }
    bool is_ready() const {
        return _state == DriverState::READY || _state == DriverState::RUNNING || _state == DriverState::LOCAL_WAITING;
    }
    bool is_finished() const {
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

    bool global_rf_block(std::string* rf_waiting_set = nullptr) {
        if (_all_global_rf_ready_or_timeout) {
            return false;
        }

        // timeout check
        if (_precondition_block_timer_sw->elapsed_time() >= _global_rf_wait_timeout_ns) {
            return false;
        }

        bool all_ready = true;
        // check if all the remote RFs are ready.
        for (auto* rf_desc : _global_rf_descriptors) {
            if (rf_desc->is_local() || rf_desc->runtime_filter(-1) != nullptr) {
                continue;
            }
            if (rf_waiting_set != nullptr) {
                rf_waiting_set->append(std::to_string(rf_desc->filter_id()) + ",");
            }
            all_ready = false;
        }
        _all_global_rf_ready_or_timeout = _all_global_rf_ready_or_timeout || all_ready;
        return !all_ready;
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
            _update_global_rf_timer();
            return global_rf_block();
        } else {
            return global_rf_block();
        }
    }

    void set_all_global_rf_timeout() { _all_global_rf_ready_or_timeout = true; }

    bool has_precondition() const {
        return !_local_rf_holders.empty() || !_dependencies.empty() || !_global_rf_descriptors.empty();
    }

    std::string get_preconditions_block_reasons() {
        if (_state == DriverState::PRECONDITION_BLOCK) {
            std::string rf_waiting_set;
            return std::string(dependencies_block() ? "(dependencies," : "(") +
                   std::string(global_rf_block(&rf_waiting_set) ? "global runtime filter:" + rf_waiting_set : "") +
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
        // PRECONDITION_BLOCK
        if (_state == DriverState::PRECONDITION_BLOCK) {
            if (is_precondition_block()) {
                return false;
            }

            mark_precondition_ready();

            RETURN_IF_ERROR(check_short_circuit());
            if (_state == DriverState::PENDING_FINISH) {
                return false;
            }
            // Driver state must be set to a state different from PRECONDITION_BLOCK bellow,
            // to avoid call mark_precondition_ready() and check_short_circuit() multiple times.
        }

        // OUTPUT_FULL
        if (!sink_operator()->need_input() && !sink_operator()->is_finished()) {
            set_driver_state(DriverState::OUTPUT_FULL);
            return false;
        }

        // INPUT_EMPTY
        if (!source_operator()->has_output() && !source_operator()->is_finished()) {
            set_driver_state(DriverState::INPUT_EMPTY);
            return false;
        }

        return true;
    }

    // used in event scheduler
    // check driver is ready for schedule
    // similar to is_not_blocked but without check short_circuit.
    bool check_is_ready() {
        // If the sink operator is finished, the rest operators of this driver needn't be executed anymore.
        if (sink_operator()->is_finished()) {
            return true;
        }
        if (_state == DriverState::PRECONDITION_BLOCK) {
            if (is_precondition_block()) {
                return false;
            }
            mark_precondition_ready();
            // In the event scheduler, we avoid calling check_short_circuit inside check_is_ready.
            // Because check_short_circuit may trigger cascading recursive calls such as set_finished.
            // It will increase scheduler complexity (like call set finished in unknown thread).
            // Instead, we directly return true after the precondition block state changes.
            // The check is performed in driver::process.
            return true;
        }

        // OUTPUT_FULL
        if (!sink_operator()->need_input() && !sink_operator()->is_finished()) {
            set_driver_state(DriverState::OUTPUT_FULL);
            return false;
        }

        // INPUT_EMPTY
        if (!source_operator()->has_output() && !source_operator()->is_finished()) {
            set_driver_state(DriverState::INPUT_EMPTY);
            return false;
        }

        return true;
    }

    // Check whether an operator can be short-circuited, when is_precondition_block() becomes false from true.
    Status check_short_circuit();

    bool need_report_exec_state();
    void report_exec_state_if_necessary();
    void runtime_report_action();

    std::string to_readable_string() const;
    std::string get_raw_string_name() const;

    workgroup::WorkGroup* workgroup();
    const workgroup::WorkGroup* workgroup() const;
    void set_workgroup(workgroup::WorkGroupPtr wg);

    void set_in_queue(DriverQueue* in_queue) { _in_queue = in_queue; }
    size_t get_driver_queue_level() const { return _driver_queue_level; }
    void set_driver_queue_level(size_t driver_queue_level) { _driver_queue_level = driver_queue_level; }

    inline bool is_in_ready() const { return _in_ready.load(std::memory_order_acquire); }
    void set_in_ready(bool v) {
        SCHEDULE_CHECK(!v || !is_in_ready());
        _in_ready.store(v, std::memory_order_release);
    }

    bool is_in_blocked() const { return _in_blocked.load(std::memory_order_acquire); }
    void set_in_blocked(bool v) {
        SCHEDULE_CHECK(!v || !is_in_blocked());
        SCHEDULE_CHECK(!is_in_ready());
        _in_blocked.store(v, std::memory_order_release);
    }

    ScheduleToken acquire_schedule_token() {
        bool val = false;
        return {this, _schedule_token.compare_exchange_strong(val, true)};
    }

    DECLARE_RACE_DETECTOR(schedule)

    bool need_check_reschedule() const { return _need_check_reschedule; }
    void set_need_check_reschedule(bool need_reschedule) { _need_check_reschedule = need_reschedule; }

    inline std::string get_name() const { return strings::Substitute("PipelineDriver (id=$0)", _driver_id); }

    // Whether the query can be expirable or not.
    virtual bool is_query_never_expired() { return false; }

    PipelineObserver* observer() { return &_observer; }
    void assign_observer();
    bool is_operator_cancelled() const { return _is_operator_cancelled; }

    bool local_prepare_is_done() const { return _local_prepare_is_done; }

protected:
    PipelineDriver();

    // Yield PipelineDriver when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT_NS = 100'000'000L;
    // Yield PipelineDriver when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT_NS = 5'000'000L;
    // Execution time exceed this is considered overloaded
    static constexpr int64_t OVERLOADED_MAX_TIME_SPEND_NS = 150'000'000L;

    // check whether fragment is cancelled. It is used before pull_chunk and push_chunk.
    bool _check_fragment_is_canceled(RuntimeState* runtime_state);
    Status _mark_operator_finishing(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_finished(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_cancelled(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_closed(size_t operator_idx, OperatorPtr& op, RuntimeState* runtime_state);
    void _close_operators(RuntimeState* runtime_state);

    Status _prepare_operator_mem_resource_manager(size_t operator_idx, RuntimeState* state);
    spill::OperatorMemoryResourceManager* _operator_mem_resource_manager(size_t operator_idx);

    void _adjust_memory_usage(RuntimeState* state, MemTracker* tracker, size_t operator_idx, OperatorPtr& op,
                              const ChunkPtr& chunk);
    void _try_to_release_buffer(RuntimeState* state, size_t operator_idx, OperatorPtr& op);

    // Update metrics when the driver yields.
    void _update_driver_acct(size_t total_chunks_moved, size_t total_rows_moved, size_t time_spent);
    void _update_statistics(RuntimeState* state, size_t total_chunks_moved, size_t total_rows_moved, size_t time_spent);
    void _update_scan_statistics(RuntimeState* state);
    void _update_driver_level_timer();

    // used in event scheduler
    void _update_global_rf_timer();

    // Helper function to build readable string with option to use raw operator names
    std::string _build_readable_string(bool use_raw_name) const;

    RuntimeState* _runtime_state = nullptr;
    PipelineDriverObserver _observer;
    // Keep this before _operators so driver teardown destroys operators first, then their managers.
    std::vector<std::unique_ptr<spill::OperatorMemoryResourceManager>> _operator_mem_resource_managers;
    Operators _operators;
    DriverDependencies _dependencies;
    bool _all_dependencies_ready = false;
    bool _precondition_prepared = false;

    mutable std::vector<RuntimeFilterHolder*> _local_rf_holders;
    bool _all_local_rf_ready = false;

    std::vector<RuntimeFilterProbeDescriptor*> _global_rf_descriptors;
    bool _wait_global_rf_ready = false;
    bool _all_global_rf_ready_or_timeout = false;
    int64_t _global_rf_wait_timeout_ns = -1;

    size_t _first_unfinished{0};
    QueryContext* _query_ctx;
    QueryRuntimeState* _query_runtime_state;
    FragmentRuntimeState* _fragment_runtime_state;
    FragmentContext* _fragment_ctx;
    Pipeline* _pipeline;
    DriverObserver* _driver_observer;
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

    phmap::flat_hash_map<int32_t, OperatorStage, StdHash<int32_t>> _operator_stages;

    workgroup::WorkGroupPtr _workgroup = nullptr;
    DriverQueue* _in_queue = nullptr;
    // The index of QuerySharedDriverQueue._queues which this driver belongs to.
    size_t _driver_queue_level = 0;
    // Indicates whether it is in a ready queue.
    std::atomic<bool> _in_ready{false};
    // Indicates whether it is in a block states. Only used when enable event scheduler mode.
    std::atomic<bool> _in_blocked{false};

    std::atomic<bool> _schedule_token{true};
    // Indicates if the block queue needs to be checked when it is added to the block queue. See EventScheduler for details.
    std::atomic<bool> _need_check_reschedule{false};

    std::atomic<bool> _has_log_cancelled{false};

    std::atomic<bool> _is_operator_cancelled{false};

    std::shared_ptr<PipelineTimerTask> _global_rf_timer;

    std::atomic<bool> _local_prepare_is_done{false};

protected:
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

private:
    void prepare_profile();
};

} // namespace pipeline
} // namespace starrocks
