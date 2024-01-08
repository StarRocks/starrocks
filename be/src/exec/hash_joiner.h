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

#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/hash_join_components.h"
#include "exec/hash_join_node.h"
#include "exec/join_hash_map.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/spill/spiller.h"
#include "exprs/in_const_predicate.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;
class HashJoinProber;
class HashJoinBuilder;

class ColumnRef;
class RuntimeFilterBuildDescriptor;

class HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;

// HashJoiner works in four consecutive phases, each phase has its own allowed operations.
// 1.BUILD: building ht from right child is outstanding, and probe operations is disallowed. EOS from right child
//   indicates that building ht should be finalized.
// 2.PROBE: building ht is done, and probe operations can be conducted as chunks is pulled one by one from left child.
//   EOS from left child indicates that PROBE phase is done.
// 3.POST_PROBE: for RIGHT ANTI-JOIN, RIGHT SEMI-JOIN, FULL OUTER-JOIN, probe-missing/probe-hitting tuples should be
//   processed.
// 4.DONE: all input streams have been processed.
//
enum HashJoinPhase {
    BUILD = 0,
    PROBE = 1,
    POST_PROBE = 2,
    EOS = 4,
};
struct HashJoinerParam {
    HashJoinerParam(ObjectPool* pool, const THashJoinNode& hash_join_node, TPlanNodeId node_id,
                    TPlanNodeType::type node_type, std::vector<bool> is_null_safes,
                    std::vector<ExprContext*> build_expr_ctxs, std::vector<ExprContext*> probe_expr_ctxs,
                    std::vector<ExprContext*> other_join_conjunct_ctxs, std::vector<ExprContext*> conjunct_ctxs,
                    const RowDescriptor& build_row_descriptor, const RowDescriptor& probe_row_descriptor,
                    const RowDescriptor& row_descriptor, TPlanNodeType::type build_node_type,
                    TPlanNodeType::type probe_node_type, bool build_conjunct_ctxs_is_empty,
                    std::list<RuntimeFilterBuildDescriptor*> build_runtime_filters, std::set<SlotId> build_output_slots,
                    std::set<SlotId> probe_output_slots, const TJoinDistributionMode::type distribution_mode,
                    bool mor_reader_mode)
            : _pool(pool),
              _hash_join_node(hash_join_node),
              _node_id(node_id),
              _node_type(node_type),
              _is_null_safes(std::move(is_null_safes)),
              _build_expr_ctxs(std::move(build_expr_ctxs)),
              _probe_expr_ctxs(std::move(probe_expr_ctxs)),
              _other_join_conjunct_ctxs(std::move(other_join_conjunct_ctxs)),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _build_row_descriptor(build_row_descriptor),
              _probe_row_descriptor(probe_row_descriptor),
              _row_descriptor(row_descriptor),
              _build_node_type(build_node_type),
              _probe_node_type(probe_node_type),
              _build_conjunct_ctxs_is_empty(build_conjunct_ctxs_is_empty),
              _build_runtime_filters(std::move(build_runtime_filters)),
              _build_output_slots(std::move(build_output_slots)),
              _probe_output_slots(std::move(probe_output_slots)),
              _distribution_mode(distribution_mode),
              _mor_reader_mode(mor_reader_mode) {}

    HashJoinerParam(HashJoinerParam&&) = default;
    HashJoinerParam(HashJoinerParam&) = default;
    ~HashJoinerParam() = default;

    ObjectPool* _pool;
    const THashJoinNode& _hash_join_node;
    TPlanNodeId _node_id;
    TPlanNodeType::type _node_type;
    const std::vector<bool> _is_null_safes;
    const std::vector<ExprContext*> _build_expr_ctxs;
    const std::vector<ExprContext*> _probe_expr_ctxs;
    const std::vector<ExprContext*> _other_join_conjunct_ctxs;
    const std::vector<ExprContext*> _conjunct_ctxs;
    const RowDescriptor _build_row_descriptor;
    const RowDescriptor _probe_row_descriptor;
    const RowDescriptor _row_descriptor;
    TPlanNodeType::type _build_node_type;
    TPlanNodeType::type _probe_node_type;
    bool _build_conjunct_ctxs_is_empty;
    std::list<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
    std::set<SlotId> _build_output_slots;
    std::set<SlotId> _probe_output_slots;

    const TJoinDistributionMode::type _distribution_mode;
    const bool _mor_reader_mode;
};

inline bool could_short_circuit(TJoinOp::type join_type) {
    return join_type == TJoinOp::INNER_JOIN || join_type == TJoinOp::LEFT_SEMI_JOIN ||
           join_type == TJoinOp::RIGHT_SEMI_JOIN || join_type == TJoinOp::RIGHT_ANTI_JOIN ||
           join_type == TJoinOp::RIGHT_OUTER_JOIN;
}

inline bool has_post_probe(TJoinOp::type join_type) {
    return join_type == TJoinOp::RIGHT_OUTER_JOIN || join_type == TJoinOp::RIGHT_ANTI_JOIN ||
           join_type == TJoinOp::FULL_OUTER_JOIN;
}

inline bool is_spillable(TJoinOp::type join_type) {
    return join_type == TJoinOp::LEFT_SEMI_JOIN || join_type == TJoinOp::INNER_JOIN ||
           join_type == TJoinOp::LEFT_ANTI_JOIN || join_type == TJoinOp::LEFT_OUTER_JOIN ||
           join_type == TJoinOp::RIGHT_OUTER_JOIN || join_type == TJoinOp::RIGHT_ANTI_JOIN ||
           join_type == TJoinOp::RIGHT_SEMI_JOIN || join_type == TJoinOp::FULL_OUTER_JOIN;
}

struct HashJoinProbeMetrics {
    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* output_tuple_column_timer = nullptr;
    RuntimeProfile::Counter* probe_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* other_join_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* where_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;

    void prepare(RuntimeProfile* runtime_profile);
};

struct HashJoinBuildMetrics {
    RuntimeProfile::Counter* build_ht_timer = nullptr;
    RuntimeProfile::Counter* copy_right_table_chunk_timer = nullptr;
    RuntimeProfile::Counter* build_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* build_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* build_buckets_counter = nullptr;
    RuntimeProfile::Counter* runtime_filter_num = nullptr;

    void prepare(RuntimeProfile* runtime_profile);
};

class HashJoiner final : public pipeline::ContextWithDependency {
public:
    explicit HashJoiner(const HashJoinerParam& param);

    ~HashJoiner() override {
        if (_runtime_state != nullptr) {
            close(_runtime_state);
        }
    }

    Status prepare_builder(RuntimeState* state, RuntimeProfile* runtime_profile);
    Status prepare_prober(RuntimeState* state, RuntimeProfile* runtime_profile);
    void close(RuntimeState* state) override;

    bool need_input() const;
    bool has_output() const;
    bool is_build_done() const { return _phase != HashJoinPhase::BUILD; }
    bool is_done() const { return _phase == HashJoinPhase::EOS; }

    void enter_probe_phase() {
        _short_circuit_break();

        auto old_phase = HashJoinPhase::BUILD;
        _phase.compare_exchange_strong(old_phase, HashJoinPhase::PROBE);
    }
    void enter_post_probe_phase() {
        HashJoinPhase old_phase = HashJoinPhase::PROBE;
        if (!_phase.compare_exchange_strong(old_phase, HashJoinPhase::POST_PROBE)) {
            old_phase = HashJoinPhase::BUILD;
            // HashJoinProbeOperator finishes prematurely on runtime error or fragment's cancellation.
            _phase.compare_exchange_strong(old_phase, HashJoinPhase::EOS);
        }
    }

    void update_build_rows(size_t num_rows) { _hash_table_build_rows += num_rows; }

    void enter_eos_phase() { _phase = HashJoinPhase::EOS; }
    // build phase
    Status append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk);

    Status append_chunk_to_spill_buffer(RuntimeState* state, const ChunkPtr& chunk);

    Status append_spill_task(RuntimeState* state, std::function<StatusOr<ChunkPtr>()>& spill_task);

    Status build_ht(RuntimeState* state);
    // probe phase
    void push_chunk(RuntimeState* state, ChunkPtr&& chunk);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);

    pipeline::RuntimeInFilters& get_runtime_in_filters() { return _runtime_in_filters; }
    pipeline::RuntimeBloomFilters& get_runtime_bloom_filters() { return _build_runtime_filters; }
    pipeline::OptRuntimeBloomFilterBuildParams& get_runtime_bloom_filter_build_params() {
        return _runtime_bloom_filter_build_params;
    }

    size_t get_ht_row_count() { return _hash_join_builder->hash_table_row_count(); }

    HashJoinBuilder* hash_join_builder() { return _hash_join_builder; }

    Status create_runtime_filters(RuntimeState* state);

    void reference_hash_table(HashJoiner* src_join_builder);

    // These two methods are used only by the hash join builder.
    void set_prober_finished();
    void incr_prober() { ++_num_probers; }
    void decr_prober(RuntimeState* state);
    bool has_referenced_hash_table() const { return _has_referenced_hash_table; }

    Columns string_key_columns() { return _string_key_columns; }
    Status reset_probe(RuntimeState* state);

    size_t avg_keys_perf_bucket() const;

    const HashJoinBuildMetrics& build_metrics() { return *_build_metrics; }
    const HashJoinProbeMetrics& probe_metrics() { return *_probe_metrics; }

    size_t runtime_in_filter_row_limit() const { return 1024; }

    size_t runtime_bloom_filter_row_limit() const {
        uint64_t runtime_join_filter_pushdown_limit = 1024000;
        if (_runtime_state->query_options().__isset.runtime_join_filter_pushdown_limit) {
            runtime_join_filter_pushdown_limit = _runtime_state->query_options().runtime_join_filter_pushdown_limit;
        }
        return runtime_join_filter_pushdown_limit;
    }

    // hash table param.
    // this function only valid in hash_joiner_builder
    const HashTableParam& hash_table_param() const { return _hash_table_param; }

    void set_spiller(std::shared_ptr<spill::Spiller> spiller) { _spiller = std::move(spiller); }
    void set_spill_channel(SpillProcessChannelPtr channel) { _spill_channel = std::move(channel); }
    const auto& spiller() { return _spiller; }
    const SpillProcessChannelPtr& spill_channel() { return _spill_channel; }
    auto& io_executor() { return *spill_channel()->io_executor(); }
    void set_spill_strategy(spill::SpillStrategy strategy) { _spill_strategy = strategy; }
    spill::SpillStrategy spill_strategy() { return _spill_strategy; }

    void prepare_probe_key_columns(Columns* key_columns, const ChunkPtr& chunk) {
        SCOPED_TIMER(probe_metrics().probe_conjunct_evaluate_timer);
        _prepare_key_columns(*key_columns, chunk, _probe_expr_ctxs);
    }

    void prepare_build_key_columns(Columns* key_columns, const ChunkPtr& chunk) {
        SCOPED_TIMER(build_metrics().build_conjunct_evaluate_timer);
        _prepare_key_columns(*key_columns, chunk, _build_expr_ctxs);
    }

    const std::vector<ExprContext*> probe_expr_ctxs() { return _probe_expr_ctxs; }

    HashJoinProber* new_prober(ObjectPool* pool) { return _hash_join_prober->clone_empty(pool); }
    HashJoinBuilder* new_builder(ObjectPool* pool) { return _hash_join_builder->clone_empty(pool); }

    Status filter_probe_output_chunk(ChunkPtr& chunk, JoinHashTable& hash_table) {
        // Probe in JoinHashMap is divided into probe with other_conjuncts and without other_conjuncts.
        // Probe without other_conjuncts directly labels the hash table as hit, while _process_other_conjunct()
        // only remains the rows which are not hit the hash table before. Therefore, _process_other_conjunct can
        // not be called when other_conjuncts is empty.
        if (chunk && !chunk->is_empty() && !_other_join_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(_process_other_conjunct(&chunk, hash_table));
        }

        // TODO(satanson): _conjunct_ctxs shouldn't include local runtime in-filters.
        if (chunk && !chunk->is_empty() && !_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(_process_where_conjunct(&chunk));
        }

        return Status::OK();
    }

    Status filter_post_probe_output_chunk(ChunkPtr& chunk) {
        // Post probe needn't process _other_join_conjunct_ctxs, because they
        // are `ON` predicates, which need to be processed only on probe phase.
        if (chunk && !chunk->is_empty() && !_conjunct_ctxs.empty()) {
            // TODO(satanson): _conjunct_ctxs should including local runtime in-filters.
            RETURN_IF_ERROR(_process_where_conjunct(&chunk));
        }
        return Status::OK();
    }

    const TJoinOp::type& join_type() const { return _join_type; }

private:
    static bool _has_null(const ColumnPtr& column);

    void _init_hash_table_param(HashTableParam* param);

    Status _prepare_key_columns(Columns& key_columns, const ChunkPtr& chunk, const vector<ExprContext*>& expr_ctxs) {
        key_columns.resize(0);
        for (auto& expr_ctx : expr_ctxs) {
            ASSIGN_OR_RETURN(auto column_ptr, expr_ctx->evaluate(chunk.get()));
            if (column_ptr->only_null()) {
                ColumnPtr column = ColumnHelper::create_column(expr_ctx->root()->type(), true);
                column->append_nulls(chunk->num_rows());
                key_columns.emplace_back(column);
            } else if (column_ptr->is_constant()) {
                auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(column_ptr);
                const_column->data_column()->assign(chunk->num_rows(), 0);
                key_columns.emplace_back(const_column->data_column());
            } else {
                key_columns.emplace_back(column_ptr);
            }
        }
        return Status::OK();
    }

    bool _need_post_probe() const {
        return _join_type == TJoinOp::RIGHT_OUTER_JOIN || _join_type == TJoinOp::RIGHT_ANTI_JOIN ||
               _join_type == TJoinOp::FULL_OUTER_JOIN;
    }

    void _short_circuit_break() {
        if (_phase == HashJoinPhase::EOS) {
            return;
        }
        size_t row_count = _hash_table_build_rows;

        // special cases of short-circuit break.
        if (row_count == 0 && could_short_circuit(_join_type)) {
            _phase = HashJoinPhase::EOS;
            return;
        }

        auto& ht = _hash_join_builder->hash_table();

        if (row_count > 0) {
            if (_join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && ht.get_key_columns().size() == 1 &&
                _has_null(ht.get_key_columns()[0]) && _other_join_conjunct_ctxs.empty()) {
                // The current implementation of HashTable will reserve a row for judging the end of the linked list.
                // When performing expression calculations (such as cast string to int),
                // it is possible that this reserved row will generate Null,
                // so Column::has_null() cannot be used to judge whether there is Null in the right table.
                // TODO: This reserved field will be removed in the implementation mechanism in the future.
                // at that time, you can directly use Column::has_null() to judge
                _phase = HashJoinPhase::EOS;
            }
        }
    }

    Status _build(RuntimeState* state);

    StatusOr<ChunkPtr> _pull_probe_output_chunk(RuntimeState* state);

    Status _calc_filter_for_other_conjunct(ChunkPtr* chunk, Filter& filter, bool& filter_all, bool& hit_all);
    static void _process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                bool filter_all, bool hit_all, const Filter& filter);

    Status _process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                   JoinHashTable& hash_table);
    Status _process_semi_join_with_other_conjunct(ChunkPtr* chunk, JoinHashTable& hash_table);
    Status _process_right_anti_join_with_other_conjunct(ChunkPtr* chunk, JoinHashTable& hash_table);
    Status _process_other_conjunct(ChunkPtr* chunk, JoinHashTable& hash_table);
    Status _process_where_conjunct(ChunkPtr* chunk);

    Status _create_runtime_in_filters(RuntimeState* state);

    Status _create_runtime_bloom_filters(RuntimeState* state, int64_t limit);

private:
    const THashJoinNode& _hash_join_node;
    ObjectPool* _pool;

    RuntimeState* _runtime_state = nullptr;

    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    std::atomic<HashJoinPhase> _phase = HashJoinPhase::BUILD;
    bool _is_closed = false;

    const std::vector<bool>& _is_null_safes;
    // Equal conjuncts in Join On.
    const std::vector<ExprContext*>& _build_expr_ctxs;
    // Equal conjuncts in Join On.
    const std::vector<ExprContext*>& _probe_expr_ctxs;
    // Conjuncts in Join On except equal conjuncts.
    const std::vector<ExprContext*>& _other_join_conjunct_ctxs;
    // Conjuncts in Join followed by a filter predicate, usually in Where and Having.
    const std::vector<ExprContext*>& _conjunct_ctxs;
    const RowDescriptor& _build_row_descriptor;
    const RowDescriptor& _probe_row_descriptor;
    const RowDescriptor& _row_descriptor;
    const TPlanNodeType::type _build_node_type;
    const TPlanNodeType::type _probe_node_type;
    const bool _build_conjunct_ctxs_is_empty;
    const std::set<SlotId>& _build_output_slots;
    const std::set<SlotId>& _probe_output_slots;

    pipeline::RuntimeInFilters _runtime_in_filters;
    pipeline::RuntimeBloomFilters _build_runtime_filters;
    pipeline::OptRuntimeBloomFilterBuildParams _runtime_bloom_filter_build_params;
    bool _build_runtime_filters_from_planner;

    bool _is_push_down = false;

    // lifetime of string-typed key columns must exceed HashJoiner's lifetime, because slices in the hash of runtime
    // in-filter constructed from string-typed key columns reference the memory of this column, and the in-filter's
    // lifetime can last beyond HashJoiner.
    Columns _string_key_columns;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    // hash table doesn't have reserved data
    // bool _ht_has_remain = false;
    // right table have not output data for right outer join/right semi join/right anti join/full outer join

    bool _has_referenced_hash_table = false;

    // These two fields are used only by the hash join builder.
    size_t _num_probers = 0;
    std::atomic<size_t> _num_finished_probers = 0;
    std::atomic<size_t> _num_closed_probers = 0;

    HashJoinProber* _hash_join_prober;
    HashJoinBuilder* _hash_join_builder;

    HashTableParam _hash_table_param;

    std::shared_ptr<spill::Spiller> _spiller;
    std::shared_ptr<SpillProcessChannel> _spill_channel;
    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;

    HashJoinBuildMetrics* _build_metrics;
    HashJoinProbeMetrics* _probe_metrics;
    size_t _hash_table_build_rows{};
    bool _mor_reader_mode = false;
};

} // namespace starrocks
