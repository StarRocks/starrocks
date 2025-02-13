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

#include "exec/exec_node.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/topn_runtime_filter_back_pressure.h"
#include "exec/query_cache/cache_operator.h"
#include "exec/query_cache/lane_arbiter.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/race_detect.h"
#include "util/spinlock.h"

namespace starrocks {

class PriorityThreadPool;
class ScanNode;

namespace pipeline {

class ChunkBufferToken;
using ChunkBufferTokenPtr = std::unique_ptr<ChunkBufferToken>;
class PipelineDriver;
class ScanOperator : public SourceOperator {
public:
    ScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop, ScanNode* scan_node);

    ~ScanOperator() override;

    static size_t max_buffer_capacity() { return kIOTaskBatchSize; }

    Status prepare(RuntimeState* state) override;

    // The running I/O task committed by ScanOperator holds the reference of query context,
    // so it can prevent the scan operator from deconstructored, but cannot prevent it from closed.
    // Therefore, release resources used by the I/O task in ~ScanOperator and ScanOperatorFactory::close,
    // **NOT** in ScanOperator::close.
    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool pending_finish() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void update_metrics(RuntimeState* state) override { _merge_chunk_source_profiles(state); }

    virtual workgroup::ScanSchedEntityType sched_entity_type() const { return workgroup::ScanSchedEntityType::OLAP; }

    void set_scan_executor(workgroup::ScanExecutor* scan_executor) { _scan_executor = scan_executor; }

    void set_workgroup(workgroup::WorkGroupPtr wg) { _workgroup = std::move(wg); }

    int64_t global_rf_wait_timeout_ns() const override;

    /// interface for different scan node
    virtual Status do_prepare(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) = 0;

    int64_t get_last_scan_rows_num() { return _last_scan_rows_num.exchange(0); }
    int64_t get_last_scan_bytes() { return _last_scan_bytes.exchange(0); }

    void set_lane_arbiter(const query_cache::LaneArbiterPtr& lane_arbiter) { _lane_arbiter = lane_arbiter; }
    void set_cache_operator(const query_cache::CacheOperatorPtr& cache_operator) { _cache_operator = cache_operator; }
    void set_ticket_checker(query_cache::TicketCheckerPtr& ticket_checker) { _ticket_checker = ticket_checker; }

    void set_query_ctx(const QueryContextPtr& query_ctx);

    virtual int available_pickup_morsel_count() { return _io_tasks_per_scan_operator; }
    bool output_chunk_by_bucket() const { return _output_chunk_by_bucket; }
    void begin_pull_chunk(const ChunkPtr& res) {
        _op_pull_chunks += 1;
        _op_pull_rows += res->num_rows();
    }
    bool is_asc() const { return _is_asc; }
    void end_pull_chunk(int64_t time) { _op_running_time_ns += time; }
    virtual void begin_driver_process() {}
    virtual void end_driver_process(PipelineDriver* driver) {}
    virtual bool is_running_all_io_tasks() const;

    virtual int64_t get_scan_table_id() const { return -1; }

    void update_exec_stats(RuntimeState* state) override;

    bool has_full_events() { return get_chunk_buffer().limiter()->has_full_events(); }
    virtual bool need_notify_all() { return true; }

    template <class NotifyAll>
    auto defer_notify(NotifyAll notify_all) {
        return DeferOp([this, notify_all]() {
            if (notify_all()) {
                _source_factory()->observes().notify_source_observers();
            } else {
                _observable.notify_source_observers();
            }
        });
    }

protected:
    static constexpr size_t kIOTaskBatchSize = 64;

    // TODO: remove this to the base ScanContext.
    /// Shared scan
    virtual void attach_chunk_source(int32_t source_index) = 0;
    virtual void detach_chunk_source(int32_t source_index) {}
    virtual bool has_shared_chunk_source() const = 0;

    virtual BalancedChunkBuffer& get_chunk_buffer() const = 0;

    ChunkPtr get_chunk_from_buffer() {
        auto& chunk_buffer = get_chunk_buffer();
        ChunkPtr chunk = nullptr;
        if (chunk_buffer.try_get(_driver_sequence, &chunk)) {
            return chunk;
        }
        return nullptr;
    }

    size_t num_buffered_chunks() const { return get_chunk_buffer().size(_driver_sequence); }
    size_t buffer_size() const { return get_chunk_buffer().size(_driver_sequence); }
    size_t buffer_capacity() const { return get_chunk_buffer().limiter()->capacity(); }
    size_t buffer_memory_usage() const { return get_chunk_buffer().memory_usage(); }
    size_t default_buffer_capacity() const { return get_chunk_buffer().limiter()->default_capacity(); }
    ChunkBufferTokenPtr pin_chunk(int num_chunks) { return get_chunk_buffer().limiter()->pin(num_chunks); }
    bool is_buffer_full() const { return get_chunk_buffer().limiter()->is_full(); }
    void set_buffer_finished() { get_chunk_buffer().set_finished(_driver_sequence); }

    // This method is only invoked when current morsel is reached eof
    // and all cached chunk of this morsel has benn read out
    virtual Status _pickup_morsel(RuntimeState* state, int chunk_source_index);
    Status _trigger_next_scan(RuntimeState* state, int chunk_source_index);
    Status _try_to_trigger_next_scan(RuntimeState* state);
    virtual void _close_chunk_source_unlocked(RuntimeState* state, int index);
    void _close_chunk_source(RuntimeState* state, int index);
    virtual void _finish_chunk_source_task(RuntimeState* state, int chunk_source_index, int64_t cpu_time_ns,
                                           int64_t scan_rows, int64_t scan_bytes);
    void _detach_chunk_sources();

    void _merge_chunk_source_profiles(RuntimeState* state);
    size_t _buffer_unplug_threshold() const;

    // emit EOS chunk when we receive the last chunk of the tablet.
    std::tuple<int64_t, bool> _should_emit_eos(const ChunkPtr& chunk);

    inline void _set_scan_status(const Status& status) {
        std::lock_guard<SpinLock> l(_scan_status_mutex);
        if (_scan_status.ok()) {
            _scan_status = status;
        }
    }

    inline Status _get_scan_status() const {
        std::lock_guard<SpinLock> l(_scan_status_mutex);
        return _scan_status;
    }

    void evaluate_topn_runtime_filters(Chunk* chunk) {
        if (chunk == nullptr || chunk->is_empty() || !_topn_filter_back_pressure) {
            return;
        }
        if (auto* topn_runtime_filters = runtime_bloom_filters()) {
            auto input_num_rows = chunk->num_rows();
            _init_topn_runtime_filter_counters();
            topn_runtime_filters->evaluate(chunk, _topn_filter_eval_context);
            _topn_filter_back_pressure->inc_num_rows(chunk->num_rows());
            if (_topn_filter_eval_context.selectivity.empty()) {
                _topn_filter_back_pressure->update_selectivity(1.0);
            } else {
                double selectivity = _topn_filter_eval_context.selectivity.begin()->first;
                if (input_num_rows > 1024) {
                    _topn_filter_back_pressure->update_selectivity(selectivity);
                }
            }
        }
    }

    void _init_topn_runtime_filter_counters() {
        if (_topn_filter_eval_context.join_runtime_filter_timer == nullptr) {
            _topn_filter_eval_context.mode = RuntimeBloomFilterEvalContext::Mode::M_ONLY_TOPN;
            _topn_filter_eval_context.join_runtime_filter_timer = ADD_TIMER(_common_metrics, "TopnRuntimeFilterTime");
            _topn_filter_eval_context.join_runtime_filter_hash_timer =
                    ADD_TIMER(_common_metrics, "TopnRuntimeFilterHashTime");
            _topn_filter_eval_context.join_runtime_filter_input_counter =
                    ADD_COUNTER(_common_metrics, "TopnRuntimeFilterInputRows", TUnit::UNIT);
            _topn_filter_eval_context.join_runtime_filter_output_counter =
                    ADD_COUNTER(_common_metrics, "TopnRuntimeFilterOutputRows", TUnit::UNIT);
            _topn_filter_eval_context.join_runtime_filter_eval_counter =
                    ADD_COUNTER(_common_metrics, "TopnRuntimeFilterEvaluate", TUnit::UNIT);
            _topn_filter_eval_context.driver_sequence = _runtime_filter_probe_sequence;
        }
    }

    void eval_runtime_bloom_filters(Chunk* chunk) override {
        if (chunk == nullptr || chunk->is_empty()) {
            return;
        }

        if (auto* bloom_filters = runtime_bloom_filters()) {
            _init_rf_counters(true);
            if (_topn_filter_back_pressure) {
                _bloom_filter_eval_context.mode = RuntimeBloomFilterEvalContext::Mode::M_WITHOUT_TOPN;
            }
            bloom_filters->evaluate(chunk, _bloom_filter_eval_context);
        }
        ExecNode::eval_filter_null_values(chunk, filter_null_value_columns());
    }

protected:
    ScanNode* _scan_node = nullptr;
    const int32_t _dop;
    const bool _output_chunk_by_bucket;
    const int _io_tasks_per_scan_operator;
    const int _is_asc;
    // ScanOperator may do parallel scan, so each _chunk_sources[i] needs to hold
    // a profile indenpendently, to be more specificly, _chunk_sources[i] will go through
    // many ChunkSourcePtr in the entire life time, all these ChunkSources of _chunk_sources[i]
    // should share one profile because these ChunkSources are serial in timeline.
    // And all these parallel profiles will be merged to ScanOperator's profile at the end.
    std::vector<std::shared_ptr<RuntimeProfile>> _chunk_source_profiles;

    bool _is_finished = false;

    std::atomic<int> _num_running_io_tasks = 0;
    mutable std::shared_mutex _task_mutex; // Protects the chunk-source from concurrent close and read
    std::vector<std::atomic<bool>> _is_io_task_running;
    std::vector<ChunkSourcePtr> _chunk_sources;
    mutable bool _unpluging = false;

    std::atomic_int64_t _last_scan_rows_num = 0;
    std::atomic_int64_t _last_scan_bytes = 0;

    // The number of morsels picked up by this scan operator.
    // A tablet may be divided into multiple morsels.
    RuntimeProfile::Counter* _morsels_counter = nullptr;
    RuntimeProfile::Counter* _submit_task_counter = nullptr;

    int64_t _op_pull_chunks = 0;
    int64_t _op_pull_rows = 0;
    int64_t _op_running_time_ns = 0;

    // ticket_checker is used to count down the EOS generated by SplitMorsels from the identical original ScanMorsel.
    query_cache::TicketCheckerPtr _ticket_checker = nullptr;

private:
    int32_t _io_task_retry_cnt = 0;
    workgroup::ScanExecutor* _scan_executor = nullptr;

    int32_t _chunk_source_idx = -1;
    mutable SpinLock _scan_status_mutex;
    Status _scan_status;
    // we should hold a weak ptr because query context may be released before running io task
    std::weak_ptr<QueryContext> _query_ctx;

    workgroup::WorkGroupPtr _workgroup = nullptr;

    query_cache::LaneArbiterPtr _lane_arbiter = nullptr;
    query_cache::CacheOperatorPtr _cache_operator = nullptr;

    RuntimeProfile::Counter* _default_buffer_capacity_counter = nullptr;
    RuntimeProfile::Counter* _buffer_capacity_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_size_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_scan_task_queue_size_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_memory_usage = nullptr;
    // The total number of the original tablets in this fragment instance.
    RuntimeProfile::Counter* _tablets_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_io_tasks_counter = nullptr;

    RuntimeProfile::Counter* _prepare_chunk_source_timer = nullptr;
    RuntimeProfile::Counter* _submit_io_task_timer = nullptr;

    RuntimeBloomFilterEvalContext _topn_filter_eval_context;
    std::unique_ptr<TopnRfBackPressure> _topn_filter_back_pressure = nullptr;

    DECLARE_RACE_DETECTOR(race_pull_chunk)
};

class ScanOperatorFactory : public SourceOperatorFactory {
public:
    ScanOperatorFactory(int32_t id, ScanNode* scan_node);

    ~ScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // interface for different scan node
    virtual Status do_prepare(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual OperatorPtr do_create(int32_t dop, int32_t driver_sequence) = 0;

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

    std::shared_ptr<workgroup::ScanTaskGroup> scan_task_group() const { return _scan_task_group; }

protected:
    ScanNode* const _scan_node;
    std::shared_ptr<workgroup::ScanTaskGroup> _scan_task_group;
};

inline auto scan_defer_notify(ScanOperator* scan_op) {
    return scan_op->defer_notify([scan_op]() -> bool { return scan_op->need_notify_all(); });
}

pipeline::OpFactories decompose_scan_node_to_pipeline(std::shared_ptr<ScanOperatorFactory> factory, ScanNode* scan_node,
                                                      pipeline::PipelineBuilderContext* context);

} // namespace pipeline
} // namespace starrocks
