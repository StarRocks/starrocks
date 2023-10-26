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

#include <algorithm>
#include <mutex>
#include <set>

#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_layout.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
class RowDescriptor;
class MemTracker;
class ExecEnv;
class RuntimeProfile;

class HashJoinNode;
class RuntimeFilterProbeCollector;
class RuntimeFilterHelper {
public:
    // ==================================
    // serialization and deserialization.
    static size_t max_runtime_filter_serialized_size(const JoinRuntimeFilter* rf);
    static size_t serialize_runtime_filter(RuntimeState* state, const JoinRuntimeFilter* rf, uint8_t* data);
    static size_t serialize_runtime_filter(int serialize_version, const JoinRuntimeFilter* rf, uint8_t* data);
    static int deserialize_runtime_filter(ObjectPool* pool, JoinRuntimeFilter** rf, const uint8_t* data, size_t size);
    static JoinRuntimeFilter* create_join_runtime_filter(ObjectPool* pool, LogicalType type);

    // ====================================
    static JoinRuntimeFilter* create_runtime_bloom_filter(ObjectPool* pool, LogicalType type);
    static Status fill_runtime_bloom_filter(const ColumnPtr& column, LogicalType type, JoinRuntimeFilter* filter,
                                            size_t column_offset, bool eq_null);

    static StatusOr<ExprContext*> rewrite_runtime_filter_in_cross_join_node(ObjectPool* pool, ExprContext* conjunct,
                                                                            Chunk* chunk);

    static bool filter_zonemap_with_min_max(LogicalType type, const JoinRuntimeFilter* filter, const Column* min_column,
                                            const Column* max_column);

    // create min/max predicate from filter.
    static void create_min_max_value_predicate(ObjectPool* pool, SlotId slot_id, LogicalType slot_type,
                                               const JoinRuntimeFilter* filter, Expr** min_max_predicate);
};

// how to generate & publish this runtime filter
// it only happens in hash join node.
class RuntimeFilterBuildDescriptor : public WithLayoutMixin {
public:
    RuntimeFilterBuildDescriptor() = default;
    Status init(ObjectPool* pool, const TRuntimeFilterDescription& desc, RuntimeState* state);
    int32_t filter_id() const { return _filter_id; }
    ExprContext* build_expr_ctx() { return _build_expr_ctx; }
    LogicalType build_expr_type() const { return _build_expr_ctx->root()->type().type; }
    int build_expr_order() const { return _build_expr_order; }
    const TUniqueId& sender_finst_id() const { return _sender_finst_id; }
    const std::unordered_set<TUniqueId>& broadcast_grf_senders() const { return _broadcast_grf_senders; }
    const std::vector<TRuntimeFilterDestination>& broadcast_grf_destinations() const {
        return _broadcast_grf_destinations;
    }
    bool has_remote_targets() const { return _has_remote_targets; }
    bool has_consumer() const { return _has_consumer; }
    const std::vector<TNetworkAddress>& merge_nodes() const { return _merge_nodes; }
    void set_runtime_filter(JoinRuntimeFilter* rf) { _runtime_filter = rf; }
    void set_or_intersect_filter(JoinRuntimeFilter* rf) {
        std::lock_guard guard(_mutex);
        if (_runtime_filter) {
            _runtime_filter->intersect(rf);
        } else {
            _runtime_filter = rf;
        }
    }

    JoinRuntimeFilter* runtime_filter() { return _runtime_filter; }
    void set_is_pipeline(bool flag) { _is_pipeline = flag; }
    bool is_pipeline() const { return _is_pipeline; }
    int8_t join_mode() const { return _join_mode; };

private:
    friend class HashJoinNode;
    friend class HashJoiner;
    int32_t _filter_id;

    ExprContext* _build_expr_ctx = nullptr;
    int _build_expr_order;
    bool _has_remote_targets;
    bool _has_consumer;
    int8_t _join_mode;
    TUniqueId _sender_finst_id;
    std::unordered_set<TUniqueId> _broadcast_grf_senders;
    std::vector<TRuntimeFilterDestination> _broadcast_grf_destinations;
    std::vector<TNetworkAddress> _merge_nodes;
    JoinRuntimeFilter* _runtime_filter = nullptr;
    bool _is_pipeline = false;

    std::mutex _mutex;
};

class RuntimeFilterProbeDescriptor : public WithLayoutMixin {
public:
    RuntimeFilterProbeDescriptor() = default;
    Status init(ObjectPool* pool, const TRuntimeFilterDescription& desc, TPlanNodeId node_id, RuntimeState* state);
    // for testing.
    Status init(int32_t filter_id, ExprContext* probe_expr_ctx);
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, RuntimeProfile* p);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);
    int32_t filter_id() const { return _filter_id; }
    bool skip_wait() const { return _skip_wait; }
    bool is_topn_filter() const { return _is_topn_filter; }
    ExprContext* probe_expr_ctx() { return _probe_expr_ctx; }
    const JoinRuntimeFilter* runtime_filter() const { return _runtime_filter.load(); }
    void set_runtime_filter(const JoinRuntimeFilter* rf);
    void set_shared_runtime_filter(const std::shared_ptr<const JoinRuntimeFilter>& rf);
    bool is_bound(const std::vector<TupleId>& tuple_ids) const { return _probe_expr_ctx->root()->is_bound(tuple_ids); }
    // Disable pushing down runtime filters when:
    //  - partition_by_exprs have multi columns;
    //  - partition_by_exprs only one column but differ with probe_expr;
    // When pushing down runtime filters(probe_exprs) but partition_by_exprs are not changed
    // which may cause wrong results.
    bool can_push_down_runtime_filter() { return _partition_by_exprs_contexts.empty(); }
    bool is_probe_slot_ref(SlotId* slot_id) const {
        Expr* probe_expr = _probe_expr_ctx->root();
        if (!probe_expr->is_slotref()) return false;
        auto* slot_ref = down_cast<ColumnRef*>(probe_expr);
        *slot_id = slot_ref->slot_id();
        return true;
    }
    LogicalType probe_expr_type() const { return _probe_expr_ctx->root()->type().type; }
    void replace_probe_expr_ctx(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* new_probe_expr_ctx);
    std::string debug_string() const;
    JoinRuntimeFilter::RunningContext* runtime_filter_ctx() { return &_runtime_filter_ctx; }
    bool is_local() const { return _is_local; }
    TPlanNodeId build_plan_node_id() const { return _build_plan_node_id; }
    TPlanNodeId probe_plan_node_id() const { return _probe_plan_node_id; }
    void set_probe_plan_node_id(TPlanNodeId id) { _probe_plan_node_id = id; }
    int8_t join_mode() const { return _join_mode; };
    const std::vector<ExprContext*>* partition_by_expr_contexts() const { return &_partition_by_exprs_contexts; }

private:
    friend class HashJoinNode;
    friend class hashJoiner;
    friend class RuntimeFilterTest;
    int32_t _filter_id;
    ExprContext* _probe_expr_ctx = nullptr;
    bool _is_local;
    TPlanNodeId _build_plan_node_id;
    TPlanNodeId _probe_plan_node_id;
    std::atomic<const JoinRuntimeFilter*> _runtime_filter = nullptr;
    std::shared_ptr<const JoinRuntimeFilter> _shared_runtime_filter = nullptr;
    JoinRuntimeFilter::RunningContext _runtime_filter_ctx;
    // we want to measure when this runtime filter is applied since it's opened.
    RuntimeProfile::Counter* _latency_timer = nullptr;
    int64_t _open_timestamp = 0;
    int64_t _ready_timestamp = 0;
    int8_t _join_mode;
    bool _is_topn_filter = false;
    bool _skip_wait = false;
    std::vector<ExprContext*> _partition_by_exprs_contexts;
};

// RuntimeFilterProbeCollector::do_evaluate function apply runtime bloom filter to Operators to filter chunk.
// this function is non-reentrant, variables inside RuntimeFilterProbeCollector that hinder reentrancy is moved
// into RuntimeBloomFilterEvalContext and make do_evaluate function can be called concurrently.
struct RuntimeBloomFilterEvalContext {
    RuntimeBloomFilterEvalContext() = default;

    std::map<double, RuntimeFilterProbeDescriptor*> selectivity;
    size_t input_chunk_nums = 0;
    int run_filter_nums = 0;
    JoinRuntimeFilter::RunningContext running_context;
    RuntimeProfile::Counter* join_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_hash_timer = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_input_counter = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_output_counter = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_eval_counter = nullptr;
};

// The collection of `RuntimeFilterProbeDescriptor`
class RuntimeFilterProbeCollector {
public:
    RuntimeFilterProbeCollector();
    RuntimeFilterProbeCollector(RuntimeFilterProbeCollector&& that) noexcept;
    size_t size() const { return _descriptors.size(); }
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, RuntimeProfile* p);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);

    void compute_hash_values(Chunk* chunk, Column* column, RuntimeFilterProbeDescriptor* rf_desc,
                             RuntimeBloomFilterEvalContext& eval_context);
    void evaluate(Chunk* chunk);
    void evaluate(Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context);
    void add_descriptor(RuntimeFilterProbeDescriptor* desc);
    // accept RuntimeFilterCollector from parent node
    // which means parent node to push down runtime filter.
    void push_down(RuntimeFilterProbeCollector* parent, const std::vector<TupleId>& tuple_ids,
                   std::set<TPlanNodeId>& rf_waiting_set);
    std::map<int32_t, RuntimeFilterProbeDescriptor*>& descriptors() { return _descriptors; }
    const std::map<int32_t, RuntimeFilterProbeDescriptor*>& descriptors() const { return _descriptors; }

    void set_wait_timeout_ms(int v) { _wait_timeout_ms = v; }
    int wait_timeout_ms() const { return _wait_timeout_ms; }
    void set_scan_wait_timeout_ms(int v) { _scan_wait_timeout_ms = v; }
    long scan_wait_timeout_ms() const { return _scan_wait_timeout_ms; }
    // wait for all runtime filters are ready.
    void wait(bool on_scan_node);

    std::string debug_string() const;
    bool empty() const { return _descriptors.empty(); }
    void init_counter();
    void set_plan_node_id(int id) { _plan_node_id = id; }
    int plan_node_id() { return _plan_node_id; }
    bool has_topn_filter() const {
        return std::any_of(_descriptors.begin(), _descriptors.end(),
                           [](const auto& entry) { return entry.second->is_topn_filter(); });
    }

private:
    void update_selectivity(Chunk* chunk);
    void update_selectivity(Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context);
    // TODO: return a funcion call status
    void do_evaluate(Chunk* chunk);
    void do_evaluate(Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context);
    // mapping from filter id to runtime filter descriptor.
    std::map<int32_t, RuntimeFilterProbeDescriptor*> _descriptors;
    int _wait_timeout_ms = 0;
    long _scan_wait_timeout_ms = 0L;
    double _early_return_selectivity = 0.05;
    RuntimeProfile* _runtime_profile = nullptr;
    RuntimeBloomFilterEvalContext _eval_context;
    int _plan_node_id = -1;
    RuntimeState* _runtime_state = nullptr;
};

} // namespace starrocks
