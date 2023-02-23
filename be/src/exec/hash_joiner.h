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

#include <utility>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/hash_join_node.h"
#include "exec/join_hash_map.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/in_const_predicate.hpp"
#include "util/phmap/phmap.h"

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;

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
                    std::list<RuntimeFilterBuildDescriptor*> build_runtime_filters, std::set<SlotId> output_slots,
                    const TJoinDistributionMode::type distribution_mode)
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
              _output_slots(std::move(output_slots)),
              _distribution_mode(distribution_mode) {}

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
    std::set<SlotId> _output_slots;

    const TJoinDistributionMode::type _distribution_mode;
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
    void enter_eos_phase() { _phase = HashJoinPhase::EOS; }
    // build phase
    Status append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk);
    Status build_ht(RuntimeState* state);
    // probe phase
    void push_chunk(RuntimeState* state, ChunkPtr&& chunk);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);

    pipeline::RuntimeInFilters& get_runtime_in_filters() { return _runtime_in_filters; }
    pipeline::RuntimeBloomFilters& get_runtime_bloom_filters() { return _build_runtime_filters; }
    pipeline::OptRuntimeBloomFilterBuildParams& get_runtime_bloom_filter_build_params() {
        return _runtime_bloom_filter_build_params;
    }
    size_t get_ht_row_count() { return _ht.get_row_count(); }

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

private:
    static bool _has_null(const ColumnPtr& column);

    void _init_hash_table_param(HashTableParam* param);

    void _prepare_key_columns(Columns& key_columns, const ChunkPtr& chunk, const vector<ExprContext*>& expr_ctxs) {
        key_columns.resize(0);
        for (auto& expr_ctx : expr_ctxs) {
            ColumnPtr column_ptr = EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), chunk.get());
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
    }

    void _prepare_probe_key_columns() {
        SCOPED_TIMER(_probe_conjunct_evaluate_timer);
        _prepare_key_columns(_key_columns, _probe_input_chunk, _probe_expr_ctxs);
    }

    bool _need_post_probe() const {
        return _join_type == TJoinOp::RIGHT_OUTER_JOIN || _join_type == TJoinOp::RIGHT_ANTI_JOIN ||
               _join_type == TJoinOp::FULL_OUTER_JOIN;
    }

    void _short_circuit_break() {
        if (_phase == HashJoinPhase::EOS) {
            return;
        }

        // special cases of short-circuit break.
        if (_ht.get_row_count() == 0 &&
            (_join_type == TJoinOp::INNER_JOIN || _join_type == TJoinOp::LEFT_SEMI_JOIN ||
             _join_type == TJoinOp::RIGHT_SEMI_JOIN || _join_type == TJoinOp::RIGHT_ANTI_JOIN ||
             _join_type == TJoinOp::RIGHT_OUTER_JOIN)) {
            _phase = HashJoinPhase::EOS;
            return;
        }

        if (_ht.get_row_count() > 0) {
            if (_join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _ht.get_key_columns().size() == 1 &&
                _has_null(_ht.get_key_columns()[0]) && _other_join_conjunct_ctxs.empty()) {
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
    Status _probe(RuntimeState* state, ScopedTimer<MonotonicStopWatch>& probe_timer, ChunkPtr* chunk, bool& eos);

    StatusOr<ChunkPtr> _pull_probe_output_chunk(RuntimeState* state);

    Status _calc_filter_for_other_conjunct(ChunkPtr* chunk, Filter& filter, bool& filter_all, bool& hit_all);
    static void _process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                bool filter_all, bool hit_all, const Filter& filter);

    Status _process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count);
    Status _process_semi_join_with_other_conjunct(ChunkPtr* chunk);
    Status _process_right_anti_join_with_other_conjunct(ChunkPtr* chunk);
    Status _process_other_conjunct(ChunkPtr* chunk);
    Status _process_where_conjunct(ChunkPtr* chunk);

    Status _filter_probe_output_chunk(ChunkPtr& chunk) {
        // Probe in JoinHashMap is divided into probe with other_conjuncts and without other_conjuncts.
        // Probe without other_conjuncts directly labels the hash table as hit, while _process_other_conjunct()
        // only remains the rows which are not hit the hash table before. Therefore, _process_other_conjunct can
        // not be called when other_conjuncts is empty.
        if (chunk && !chunk->is_empty() && !_other_join_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(_process_other_conjunct(&chunk));
        }

        // TODO(satanson): _conjunct_ctxs shouldn't include local runtime in-filters.
        if (chunk && !chunk->is_empty() && !_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(_process_where_conjunct(&chunk));
        }

        return Status::OK();
    }

    Status _filter_post_probe_output_chunk(ChunkPtr& chunk) {
        // Post probe needn't process _other_join_conjunct_ctxs, because they
        // are `ON` predicates, which need to be processed only on probe phase.
        if (chunk && !chunk->is_empty() && !_conjunct_ctxs.empty()) {
            // TODO(satanson): _conjunct_ctxs should including local runtime in-filters.
            RETURN_IF_ERROR(_process_where_conjunct(&chunk));
        }
        return Status::OK();
    }

    Status _create_runtime_in_filters(RuntimeState* state) {
        SCOPED_TIMER(_build_runtime_filter_timer);

        if (_ht.get_row_count() > 1024) {
            return Status::OK();
        }

        if (_ht.get_row_count() > 0) {
            // there is a bug (DSDB-3860) in old planner if probe_expr is not slot-ref, and this fix is workaround.
            size_t size = _build_expr_ctxs.size();
            std::vector<bool> to_build(size, true);
            for (int i = 0; i < size; i++) {
                ExprContext* expr_ctx = _probe_expr_ctxs[i];
                to_build[i] = (expr_ctx->root()->is_slotref());
            }

            for (size_t i = 0; i < size; i++) {
                if (!to_build[i]) continue;
                ColumnPtr column = _ht.get_key_columns()[i];
                Expr* probe_expr = _probe_expr_ctxs[i]->root();
                // create and fill runtime in filter.
                VectorizedInConstPredicateBuilder builder(state, _pool, probe_expr);
                builder.set_eq_null(_is_null_safes[i]);
                builder.use_as_join_runtime_filter();
                Status st = builder.create();
                if (!st.ok()) {
                    _runtime_in_filters.push_back(nullptr);
                    continue;
                }
                if (probe_expr->type().is_string_type()) {
                    _string_key_columns.emplace_back(column);
                }
                builder.add_values(column, kHashJoinKeyColumnOffset);
                _runtime_in_filters.push_back(builder.get_in_const_predicate());
            }
        }

        COUNTER_UPDATE(_runtime_filter_num, static_cast<int64_t>(_runtime_in_filters.size()));
        return Status::OK();
    }

    Status _create_runtime_bloom_filters(RuntimeState* state, int64_t limit) {
        for (auto* rf_desc : _build_runtime_filters) {
            rf_desc->set_is_pipeline(true);
            // skip if it does not have consumer.
            if (!rf_desc->has_consumer()) {
                _runtime_bloom_filter_build_params.emplace_back();
                continue;
            }
            if (!rf_desc->has_remote_targets() && _ht.get_row_count() > limit) {
                _runtime_bloom_filter_build_params.emplace_back();
                continue;
            }

            int expr_order = rf_desc->build_expr_order();
            ColumnPtr column = _ht.get_key_columns()[expr_order];
            bool eq_null = _is_null_safes[expr_order];
            _runtime_bloom_filter_build_params.emplace_back(pipeline::RuntimeBloomFilterBuildParam(eq_null, column));
        }
        return Status::OK();
    }

private:
    const THashJoinNode& _hash_join_node;
    ObjectPool* _pool;

    RuntimeState* _runtime_state = nullptr;

    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    std::atomic<HashJoinPhase> _phase = HashJoinPhase::BUILD;
    bool _is_closed = false;

    ChunkPtr _probe_input_chunk;

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
    const std::set<SlotId>& _output_slots;

    pipeline::RuntimeInFilters _runtime_in_filters;
    pipeline::RuntimeBloomFilters _build_runtime_filters;
    pipeline::OptRuntimeBloomFilterBuildParams _runtime_bloom_filter_build_params;
    bool _build_runtime_filters_from_planner;

    bool _is_push_down = false;

    JoinHashTable _ht;

    Columns _key_columns;
    // lifetime of string-typed key columns must exceed HashJoiner's lifetime, because slices in the hash of runtime
    // in-filter constructed from string-typed key columns reference the memory of this column, and the in-filter's
    // lifetime can last beyond HashJoiner.
    Columns _string_key_columns;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    // hash table doesn't have reserved data
    bool _ht_has_remain = false;
    // right table have not output data for right outer join/right semi join/right anti join/full outer join

    bool _has_referenced_hash_table = false;

    // These two fields are used only by the hash join builder.
    size_t _num_probers = 0;
    std::atomic<size_t> _num_finished_probers = 0;
    std::atomic<size_t> _num_closed_probers = 0;

    // Profile for hash join builder.
    RuntimeProfile::Counter* _build_ht_timer = nullptr;
    RuntimeProfile::Counter* _copy_right_table_chunk_timer = nullptr;
    RuntimeProfile::Counter* _build_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* _output_build_column_timer = nullptr;
    RuntimeProfile::Counter* _build_buckets_counter = nullptr;
    RuntimeProfile::Counter* _runtime_filter_num = nullptr;

    // Profile for hash join prober.
    RuntimeProfile::Counter* _search_ht_timer = nullptr;
    RuntimeProfile::Counter* _output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* _output_tuple_column_timer = nullptr;
    RuntimeProfile::Counter* _build_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _probe_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _other_join_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _where_conjunct_evaluate_timer = nullptr;
};

} // namespace starrocks
