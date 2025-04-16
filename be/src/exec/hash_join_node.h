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

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exec/join_hash_map.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;

class ColumnRef;
class RuntimeFilterBuildDescriptor;

class HashJoinNode final : public ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    void close(RuntimeState* state) override;
    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
    bool can_generate_global_runtime_filter() const;
    TJoinDistributionMode::type distribution_mode() const;
    const std::list<RuntimeFilterBuildDescriptor*>& build_runtime_filters() const;
    void push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) override;

private:
    template <class HashJoinerFactory, class HashJoinBuilderFactory, class HashJoinProbeFactory>
    pipeline::OpFactories _decompose_to_pipeline(pipeline::PipelineBuilderContext* context);

    static bool _has_null(const ColumnPtr& column);

    void _init_hash_table_param(HashTableParam* param);
    // local join includes: broadcast join and colocate join.
    Status _create_implicit_local_join_runtime_filters(RuntimeState* state);
    void _final_update_profile() {
        if (_probe_chunk_count > 0) {
            COUNTER_SET(_avg_input_probe_chunk_size, int64_t(_probe_rows_counter->value() / _probe_chunk_count));
        } else {
            COUNTER_SET(_avg_input_probe_chunk_size, int64_t(0));
        }
        if (_output_chunk_count > 0) {
            COUNTER_SET(_avg_output_chunk_size, int64_t(_num_rows_returned / _output_chunk_count));
        } else {
            COUNTER_SET(_avg_output_chunk_size, int64_t(0));
        }
    }
    Status _build(RuntimeState* state);
    Status _probe(RuntimeState* state, ScopedTimer<MonotonicStopWatch>& probe_timer, ChunkPtr* chunk, bool& eos);
    Status _probe_remain(ChunkPtr* chunk, bool& eos);

    Status _evaluate_build_keys(const ChunkPtr& chunk);

    Status _calc_filter_for_other_conjunct(ChunkPtr* chunk, Filter& filter, bool& filter_all, bool& hit_all);
    static void _process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                bool filter_all, bool hit_all, const Filter& filter);

    Status _process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count);
    Status _process_semi_join_with_other_conjunct(ChunkPtr* chunk);
    Status _process_right_anti_join_with_other_conjunct(ChunkPtr* chunk);
    Status _process_other_conjunct(ChunkPtr* chunk);

    Status _do_publish_runtime_filters(RuntimeState* state, int64_t limit);
    Status _push_down_in_filter(RuntimeState* state);

    friend ExecNode;
    // _hash_join_node is used to construct HashJoiner, the reference is sound since
    // it's only used in FragmentExecutor::prepare function.
    const THashJoinNode& _hash_join_node;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
    std::vector<ExprContext*> _other_join_conjunct_ctxs;
    std::vector<bool> _is_null_safes;

    // If distribution type is SHUFFLE_HASH_BUCKET, local shuffle can use the
    // equivalence of ExchagneNode's partition colums
    std::vector<ExprContext*> _probe_equivalence_partition_expr_ctxs;
    std::vector<ExprContext*> _build_equivalence_partition_expr_ctxs;

    std::list<ExprContext*> _runtime_in_filters;
    std::list<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
    bool _build_runtime_filters_from_planner;

    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    TJoinDistributionMode::type _distribution_mode = TJoinDistributionMode::NONE;
    std::set<SlotId> _output_slots;

    bool _is_push_down = false;
    bool _enable_late_materialization = false;

    bool _enable_partition_hash_join = false;

    bool _is_skew_join = false;

    JoinHashTable _ht;

    ChunkPtr _cur_left_input_chunk = nullptr;
    ChunkPtr _pre_left_input_chunk = nullptr;
    ChunkPtr _probing_chunk = nullptr;

    Columns _key_columns;
    size_t _output_probe_column_count = 0;
    size_t _output_build_column_count = 0;
    size_t _probe_chunk_count = 0;
    size_t _output_chunk_count = 0;

    bool _eos = false;
    // hash table doesn't have reserved data
    bool _ht_has_remain = false;
    // right table have not output data for right outer join/right semi join/right anti join/full outer join
    bool _right_table_has_remain = true;
    bool _probe_eos = false; // probe table scan finished;
    size_t _runtime_join_filter_pushdown_limit = 1024000;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _build_ht_timer = nullptr;
    RuntimeProfile::Counter* _copy_right_table_chunk_timer = nullptr;
    RuntimeProfile::Counter* _build_push_down_expr_timer = nullptr;
    RuntimeProfile::Counter* _merge_input_chunk_timer = nullptr;
    RuntimeProfile::Counter* _probe_timer = nullptr;
    RuntimeProfile::Counter* _search_ht_timer = nullptr;
    RuntimeProfile::Counter* _probe_counter = nullptr;
    RuntimeProfile::Counter* _output_build_column_timer = nullptr;
    RuntimeProfile::Counter* _output_probe_column_timer = nullptr;
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

} // namespace starrocks
