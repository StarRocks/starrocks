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

#include <functional>

#include "column/vectorized_fwd.h"
#include "common/runtime_profile.h"
#include "common/statusor.h"
#include "exec/pipeline/primitives/operator_runtime_access.h"
#include "exec/pipeline/runtime_filter_core_types.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
class Expr;
class ExprContext;
class RuntimeProfile;
class RuntimeState;
using RuntimeFilterProbeCollector = starrocks::RuntimeFilterProbeCollector;

namespace pipeline {
class Operator;
class OperatorFactory;
class PipelineObserver;
class RuntimeFilterHolder;
class RuntimeFilterHub;
using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;
using LocalRFWaitingSet = std::set<TPlanNodeId>;

class Operator {
    friend class PipelineDriver;

public:
    Operator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id, bool is_subordinate,
             int32_t driver_sequence, OperatorRuntimeAccess* runtime_access = nullptr);
    virtual ~Operator() = default;

    // prepare is used to do the initialization work
    // It's one of the stages of the operator life cycle（prepare -> finishing -> finished -> [cancelled] -> closed)
    // This method will be exactly invoked once in the whole life cycle
    // do not add heavy task in this method!
    virtual Status prepare(RuntimeState* state);

    // thread-safe operation in prepare can be moved into prepare_local_state
    virtual Status prepare_local_state(RuntimeState* state);

    // Notifies the operator that no more input chunk will be added.
    // The operator should finish processing.
    // The method should be idempotent, because it may be triggered
    // multiple times in the entire life cycle
    // finish function is used to finish the following operator of the current operator that encounters its EOS
    // and has no data to push into its following operator, but the operator is not finished until its buffered
    // data inside is processed.
    // It's one of the stages of the operator life cycle（prepare -> finishing -> finished -> [cancelled] -> closed)
    // This method will be exactly invoked once in the whole life cycle
    virtual Status set_finishing(RuntimeState* state) { return Status::OK(); }

    // set_finished is used to shutdown both input and output stream of a operator and after its invocation
    // buffered data inside the operator is cleared.
    // This function is used to shutdown preceding operators of the current operator if it is finished in advance,
    // when the query or fragment instance is canceled, set_finished is also called to shutdown unfinished operators.
    // A complex source operator that interacts with the corresponding sink operator in its preceding drivers via
    // an implementation-specific context should override set_finished function, such as LocalExchangeSourceOperator.
    // For an ordinary operator, set_finished function is trivial and just has the same implementation with
    // set_finishing function.
    // It's one of the stages of the operator life cycle（prepare -> finishing -> finished -> [cancelled] -> closed)
    // This method will be exactly invoked once in the whole life cycle
    virtual Status set_finished(RuntimeState* state) { return Status::OK(); }

    // It's one of the stages of the operator life cycle（prepare -> finishing -> finished -> [cancelled] -> closed)
    // - When the fragment exits abnormally, the stage operator will become to CANCELLED between FINISHED and CLOSE.
    // - When the fragment exits normally, there isn't CANCELLED stage for the drivers.
    // Sometimes, the operator need to realize it is cancelled to stop earlier than normal, such as ExchangeSink.
    virtual Status set_cancelled(RuntimeState* state) { return Status::OK(); }

    // when local runtime filters are ready, the operator should bound its corresponding runtime in-filters.
    virtual void set_precondition_ready(RuntimeState* state);

    // close is used to do the cleanup work
    // It's one of the stages of the operator life cycle（prepare -> finishing -> finished -> [cancelled] -> closed)
    // This method will be exactly invoked once in the whole life cycle
    virtual void close(RuntimeState* state);

    // Whether we could pull chunk from this operator
    virtual bool has_output() const = 0;

    // return true if operator should ignore eos chunk
    virtual bool ignore_empty_eos() const { return true; }

    // Whether we could push chunk to this operator
    virtual bool need_input() const = 0;

    // Is this operator completely finished processing and no more
    // output chunks will be produced
    virtual bool is_finished() const = 0;

    // pending_finish returns whether this operator still has reference to the object owned by the operator or FragmentContext.
    // It can ONLY be called after calling set_finished().
    // When a driver's sink operator is finished, the driver should wait for pending i/o task completion.
    // Otherwise, pending tasks shall reference to destructed objects in the operator or FragmentContext,
    // since FragmentContext is unregistered prematurely after all the drivers are finalized.
    // Only source and sink operator may return true, and other operators always return false.
    virtual bool pending_finish() const { return false; }

    // Pull chunk from this operator
    // Use shared_ptr, because in some cases (local broadcast exchange),
    // the chunk need to be shared
    virtual StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) = 0;

    // Push chunk to this operator
    virtual Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) = 0;

    // reset_state is used by MultilaneOperator in cache mechanism, because lanes in MultilaneOperator are
    // re-used by tablets, before the lane serves for the current tablet, it must invoke reset_state to re-prepare
    // the operators (such as: Project, ChunkAccumulate, DictDecode, Aggregate) that is decorated by MultilaneOperator
    // and clear the garbage that previous tablet has produced.
    //
    // In multi-version cache, when cache is hit partially, the partial-hit cache value should be refilled back to the
    // pre-cache operator(e.g. pre-cache Agg operator) that precedes CacheOperator immediately, the Rowsets of delta
    // version and the partial-hit cache value will be merged in this pre-cache operator.
    //
    // which operators should override this functions?
    // 1. operators not decorated by MultiOperator: not required
    // 2. operators decorated by MultilaneOperator and it precedes CacheOperator immediately: required, and must refill back
    // partial-hit cache value via the `chunks` parameter, e.g.
    //  MultilaneOperator<ConjugateOperator<AggregateBlockingSinkOperator, AggregateBlockingSourceOperator>>
    // 3. operators decorated by MultilaneOperator except case 2: e.g. ProjectOperator, Chunk AccumulateOperator and etc.
    virtual Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) { return Status::OK(); }

    // Some operator's metrics are updated in the finishing stage, which is not suitable to the runtime profile mechanism.
    // So we add this function for manual updation of metrics when reporting the runtime profile.
    virtual void update_metrics(RuntimeState* state) {}

    virtual size_t output_amplification_factor() const { return 1; }
    enum class OutputAmplificationType { ADD, MAX };
    virtual OutputAmplificationType intra_pipeline_amplification_type() const { return OutputAmplificationType::MAX; }

    int32_t get_id() const { return _id; }

    int32_t get_plan_node_id() const { return _plan_node_id; }

    MemTracker* mem_tracker() const { return _mem_tracker.get(); }

    virtual std::string get_name() const {
        return strings::Substitute("$0_$1_$2($3)", _name, _plan_node_id, this, is_finished() ? "X" : "O");
    }

    std::string get_raw_name() const { return _name; }

    std::vector<ExprContext*>& runtime_in_filters();

    virtual int64_t global_rf_wait_timeout_ns() const;

    // equal to ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, chunk), is used to apply in-filters to Operators.
    Status eval_conjuncts_and_in_filters(const std::vector<ExprContext*>& conjuncts, Chunk* chunk,
                                         FilterPtr* filter = nullptr, bool apply_filter = true);
    // evaluate no eq join runtime in filters
    // The no-eq join runtime filter does not have a companion bloom filter.
    // This function only executes these filters to avoid the overhead of executing an additional runtime in filter.
    Status eval_no_eq_join_runtime_in_filters(Chunk* chunk);

    // Evaluate conjuncts without cache
    Status eval_conjuncts(const std::vector<ExprContext*>& conjuncts, Chunk* chunk, FilterPtr* filter = nullptr);

    // equal to ExecNode::eval_join_runtime_filters, is used to apply bloom-filters to Operators.
    virtual void eval_runtime_bloom_filters(Chunk* chunk);

    // Pseudo plan_node_id for final sink, such as result_sink, table_sink
    static const int32_t s_pseudo_plan_node_id_for_final_sink;

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    RuntimeProfile* common_metrics() { return _common_metrics.get(); }
    RuntimeProfile* unique_metrics() { return _unique_metrics.get(); }

    // The different operators have their own independent logic for calculating Cost
    virtual int64_t get_last_growth_cpu_time_ns() {
        int64_t res = _last_growth_cpu_time_ns;
        _last_growth_cpu_time_ns = 0;
        return res;
    }

    void set_prepare_time(int64_t cost_ns);
    void set_local_prepare_time(int64_t cost_ns);

    // Adjusts the execution mode of the operator after spill memory heuristics request low-memory mode.
    virtual void set_execute_mode(int performance_level) {}
    // @TODO(silverbullet233): for an operator, the way to reclaim memory is either spill
    // or push the buffer data to the downstream operator.
    // Maybe we don’t need to have the concepts of spillable and releasable, and we can use reclaimable instead.
    // Later, we need to refactor here.
    virtual bool spillable() const { return false; }
    // Operator can free memory/buffer early
    virtual bool releaseable() const { return false; }
    virtual void enter_release_memory_mode() {}

    // the memory that can be freed by the current operator
    size_t revocable_mem_bytes() { return _revocable_mem_bytes; }
    void set_revocable_mem_bytes(size_t bytes) {
        _revocable_mem_bytes = bytes;
        if (_peak_revocable_mem_bytes) {
            COUNTER_SET(_peak_revocable_mem_bytes, _revocable_mem_bytes);
        }
    }
    int32_t get_driver_sequence() const { return _driver_sequence; }
    void set_runtime_filter_probe_sequence(int32_t probe_sequence) {
        this->_runtime_filter_probe_sequence = probe_sequence;
    }
    OperatorFactory* get_factory() const { return _factory; }

    // memory to be reserved before executing push_chunk
    virtual size_t estimated_memory_reserved(const ChunkPtr& chunk) {
        if (chunk && !chunk->is_empty()) {
            return chunk->memory_usage();
        }
        return 0;
    }

    // memory to be reserved before executing set_finishing
    virtual size_t estimated_memory_reserved() { return 0; }

    // if return true it means the operator has child operators
    virtual bool is_combinatorial_operator() const { return false; }
    // apply operation for each child operator
    virtual void for_each_child_operator(const std::function<void(Operator*)>& apply) {}

    virtual void update_exec_stats(RuntimeState* state);

    void set_observer(PipelineObserver* observer) { _observer = observer; }
    PipelineObserver* observer() const { return _observer; }

    void _init_rf_counters(bool init_bloom);

protected:
    OperatorFactory* _factory;
    OperatorRuntimeAccess* _runtime_access;
    const int32_t _id;
    const std::string _name;
    // Which plan node this operator belongs to
    const int32_t _plan_node_id;
    const bool _is_subordinate;
    const int32_t _driver_sequence;
    int32_t _runtime_filter_probe_sequence;
    // _common_metrics and _unique_metrics are the only children of _runtime_profile
    // _common_metrics contains the common metrics of Operator, including counters and sub profiles,
    // e.g. OperatorTotalTime/PushChunkNum/PullChunkNum etc.
    // _unique_metrics contains the unique metrics, incluing counters and sub profiles,
    // e.g. ExchangeSinkOperator have some counters to describe the transmission' speed and throughput.
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::shared_ptr<RuntimeProfile> _common_metrics;
    std::shared_ptr<RuntimeProfile> _unique_metrics;

    bool _conjuncts_and_in_filters_is_cached = false;
    std::vector<ExprContext*> _cached_conjuncts_and_in_filters;

    RuntimeMembershipFilterEvalContext _bloom_filter_eval_context;

    // the memory that can be released by this operator
    size_t _revocable_mem_bytes = 0;

    // Common metrics
    RuntimeProfile::Counter* _total_timer = nullptr;
    RuntimeProfile::Counter* _push_timer = nullptr;
    RuntimeProfile::Counter* _pull_timer = nullptr;
    RuntimeProfile::Counter* _finishing_timer = nullptr;
    RuntimeProfile::Counter* _finished_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _local_prepare_timer = nullptr;
    RuntimeProfile::Counter* _global_prepare_timer = nullptr;
    int64_t _global_prepare_time_ns = 0;

    RuntimeProfile::Counter* _push_chunk_num_counter = nullptr;
    RuntimeProfile::Counter* _push_row_num_counter = nullptr;
    RuntimeProfile::Counter* _pull_chunk_num_counter = nullptr;
    RuntimeProfile::Counter* _pull_row_num_counter = nullptr;
    RuntimeProfile::Counter* _pull_chunk_bytes_counter = nullptr;
    RuntimeProfile::Counter* _runtime_in_filter_num_counter = nullptr;
    RuntimeProfile::Counter* _runtime_bloom_filter_num_counter = nullptr;
    RuntimeProfile::Counter* _conjuncts_timer = nullptr;
    RuntimeProfile::Counter* _conjuncts_input_counter = nullptr;
    RuntimeProfile::Counter* _conjuncts_output_counter = nullptr;

    // only used in spillable operator to record peak revocable memory bytes,
    // each operator should initialize it before use
    RuntimeProfile::HighWaterMarkCounter* _peak_revocable_mem_bytes = nullptr;

    // Some extra cpu cost of this operator that not accounted by pipeline driver,
    // such as OlapScanOperator( use separated IO thread to execute the IO task)
    std::atomic_int64_t _last_growth_cpu_time_ns = 0;

    PipelineObserver* _observer = nullptr;

private:
    void _init_conjuct_counters();

    std::shared_ptr<MemTracker> _mem_tracker;
    std::vector<ExprContext*> _runtime_in_filters;
};

} // namespace pipeline
} // namespace starrocks
