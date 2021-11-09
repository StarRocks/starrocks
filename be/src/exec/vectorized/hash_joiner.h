// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/vectorized/hash_join_node.h"
#include "exec/vectorized/join_hash_map.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;

namespace vectorized {
class ColumnRef;
class RuntimeFilterBuildDescriptor;

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
class HashJoiner {
public:
    HashJoiner(const THashJoinNode& hash_join_node, TPlanNodeId node_id, TPlanNodeType::type node_type, int64_t limit,
               std::vector<bool>&& is_null_safes, std::vector<ExprContext*>&& build_expr_ctxs,
               std::vector<ExprContext*>&& probe_expr_ctxs, std::vector<ExprContext*>&& other_join_conjunct_ctxs,
               std::vector<ExprContext*>&& conjunct_ctxs, const RowDescriptor& build_row_descriptor,
               const RowDescriptor& probe_row_descriptor, const RowDescriptor& row_descriptor);

    ~HashJoiner() = default;
    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);
    bool need_input() const;
    bool has_output() const;
    bool is_build_done() const { return _phase != HashJoinPhase::BUILD; }
    bool is_done() const { return _phase == HashJoinPhase::EOS; }
    void enter_post_probe_phase() {
        HashJoinPhase old_phase = HashJoinPhase::PROBE;
        if (!_phase.compare_exchange_strong(old_phase, HashJoinPhase::POST_PROBE)) {
            old_phase = HashJoinPhase::BUILD;
            // HashJoinProbeOperator finishes prematurely on runtime error or fragment's cancellation.
            _phase.compare_exchange_strong(old_phase, HashJoinPhase::EOS);
        }
    }
    // build phase
    Status append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk);
    Status build_ht(RuntimeState* state);
    // probe phase
    void push_chunk(RuntimeState* state, ChunkPtr&& chunk);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);

private:
    static bool _has_null(const ColumnPtr& column);

    StatusOr<ChunkPtr> _pull_probe_output_chunk(RuntimeState* state);

    void _init_hash_table_param(HashTableParam* param);

    bool _reached_limit() { return _limit != -1 && _num_rows_returned >= _limit; }
    static void merge_chunk(RuntimeState* state, ChunkPtr& buffered_chunk, ChunkPtr& result_chunk, ChunkPtr&& chunk) {
        const size_t half_vector_chunk_size = config::vector_chunk_size >> 1;
        if (!chunk || chunk->is_empty()) {
            return;
        }
        // try to pile chunk with buffered_chunk to form a large enough chunk.
        if (!buffered_chunk) {
            buffered_chunk = std::move(chunk);
        } else if (buffered_chunk->num_rows() + chunk->num_rows() <= config::vector_chunk_size) {
            Columns& dst_columns = buffered_chunk->columns();
            Columns& src_columns = chunk->columns();
            size_t num_rows = chunk->num_rows();
            // copy the new read chunk to the reserved
            for (size_t i = 0; i < dst_columns.size(); i++) {
                dst_columns[i]->append(*src_columns[i], 0, num_rows);
            }
        } else if (chunk->num_rows() >= half_vector_chunk_size) {
            result_chunk = std::move(chunk);
        } else {
            result_chunk = std::move(buffered_chunk);
            buffered_chunk = std::move(chunk);
        }
        // _buffered_chunk is piled up to exceed half of vector_chunk_size, it's bigger enough so that it can be moved
        // into result_chunk.
        if (buffered_chunk && !result_chunk && buffered_chunk->num_rows() >= half_vector_chunk_size) {
            result_chunk = std::move(buffered_chunk);
        }
    }

    void _merge_probe_input_chunk(RuntimeState* state, ChunkPtr&& chunk) {
        merge_chunk(state, _buffered_probe_input_chunk, _probe_input_chunk, std::move(chunk));
    }

    void _merge_probe_output_chunk(RuntimeState* state, ChunkPtr&& chunk) {
        merge_chunk(state, _buffered_probe_output_chunk, _probe_output_chunk, std::move(chunk));
    }

    void _filter_probe_output_chunk(ChunkPtr& chunk) {
        _process_other_conjunct(&chunk);
        if (chunk && !chunk->is_empty()) {
            ExecNode::eval_conjuncts(_conjunct_ctxs, chunk.get());
        }
    }

    void _prepare_key_columns(Columns& key_columns, const ChunkPtr& chunk, const vector<ExprContext*>& expr_ctxs) {
        key_columns.resize(0);
        for (auto& expr_ctx : expr_ctxs) {
            ColumnPtr column_ptr = expr_ctx->evaluate(chunk.get());
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

    void _prepare_build_key_columns() {
        _prepare_key_columns(_ht.get_key_columns(), _ht.get_build_chunk(), _build_expr_ctxs);
    }

    void _prepare_probe_key_columns() { _prepare_key_columns(_key_columns, _probe_input_chunk, _probe_expr_ctxs); }

    bool _need_post_probe() const {
        return _join_type == TJoinOp::RIGHT_OUTER_JOIN || _join_type == TJoinOp::RIGHT_ANTI_JOIN ||
               _join_type == TJoinOp::FULL_OUTER_JOIN;
    }

    Status _post_probe(RuntimeState* state) {
        DCHECK(_phase == HashJoinPhase::POST_PROBE);
        if (!_need_post_probe()) {
            _phase = HashJoinPhase::EOS;
            _ht.close();
            return Status::OK();
        }
        DCHECK(!_probe_output_chunk);
        while (true) {
            bool has_remain = true;
            auto chunk = std::make_shared<Chunk>();
            RETURN_IF_ERROR(_ht.probe_remain(&chunk, &has_remain));
            _merge_probe_output_chunk(state, std::move(chunk));
            if (!has_remain) {
                _phase = HashJoinPhase::EOS;
                _ht.close();
                return Status::OK();
            }
            if (_probe_output_chunk) {
                return Status::OK();
            }
        }
    }

    void _short_circuit_break() {
        // special cases of short-circuit break.
        if (_ht.get_row_count() == 0 && (_join_type == TJoinOp::INNER_JOIN || _join_type == TJoinOp::LEFT_SEMI_JOIN)) {
            _phase = HashJoinPhase::EOS;
        }

        if (_ht.get_row_count() > 0) {
            if (_join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _ht.get_key_columns().size() == 1 &&
                _has_null(_ht.get_key_columns()[0])) {
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

    void _calc_filter_for_other_conjunct(ChunkPtr* chunk, Column::Filter& filter, bool& filter_all, bool& hit_all);
    static void _process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                bool filter_all, bool hit_all, const Column::Filter& filter);

    void _process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count);
    void _process_other_conjunct_and_remove_duplicate_index(ChunkPtr* chunk);
    void _process_right_anti_join_with_other_conjunct(ChunkPtr* chunk);
    void _process_other_conjunct(ChunkPtr* chunk);

    static std::string _get_join_type_str(TJoinOp::type join_type);

    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    const int64_t _limit; // -1: no limit
    int64_t _num_rows_returned;
    std::atomic<HashJoinPhase> _phase = HashJoinPhase::BUILD;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    bool _is_closed = false;

    ChunkPtr _buffered_probe_input_chunk;
    ChunkPtr _probe_input_chunk;

    ChunkPtr _buffered_probe_output_chunk;
    ChunkPtr _probe_output_chunk;

    std::vector<bool> _is_null_safes;
    std::vector<ExprContext*> _build_expr_ctxs;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _other_join_conjunct_ctxs;
    std::vector<ExprContext*> _conjunct_ctxs;
    RowDescriptor _build_row_descriptor;
    RowDescriptor _probe_row_descriptor;
    RowDescriptor _row_descriptor;

    std::list<ExprContext*> _runtime_in_filters;
    std::list<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
    bool _build_runtime_filters_from_planner;

    bool _is_push_down = false;

    JoinHashTable _ht;

    Columns _key_columns;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;
    size_t _probe_chunk_count = 0;
    size_t _output_chunk_count = 0;

    bool _eos = false;
    // hash table doesn't have reserved data
    bool _ht_has_remain = false;
    // right table have not output data for right outer join/right semi join/right anti join/full outer join

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _build_ht_timer = nullptr;
    RuntimeProfile::Counter* _copy_right_table_chunk_timer = nullptr;
    RuntimeProfile::Counter* _build_push_down_expr_timer = nullptr;
    RuntimeProfile::Counter* _merge_input_chunk_timer = nullptr;
    RuntimeProfile::Counter* _probe_timer = nullptr;
    RuntimeProfile::Counter* _search_ht_timer = nullptr;
    RuntimeProfile::Counter* _output_build_column_timer = nullptr;
    RuntimeProfile::Counter* _output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* _output_tuple_column_timer = nullptr;
    RuntimeProfile::Counter* _build_rows_counter = nullptr;
    RuntimeProfile::Counter* _probe_rows_counter = nullptr;
    RuntimeProfile::Counter* _build_buckets_counter = nullptr;
    RuntimeProfile::Counter* _push_down_expr_num = nullptr;
    RuntimeProfile::Counter* _avg_input_probe_chunk_size = nullptr;
    RuntimeProfile::Counter* _avg_output_chunk_size = nullptr;
    RuntimeProfile::Counter* _build_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _probe_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _other_join_conjunct_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _where_conjunct_evaluate_timer = nullptr;
};

} // namespace vectorized
} // namespace starrocks
