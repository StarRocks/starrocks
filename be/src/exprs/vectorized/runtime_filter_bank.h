// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <mutex>

#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/runtime_filter.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
class RowDescriptor;
class MemTracker;
class ExecEnv;
class RuntimeProfile;

namespace vectorized {
class HashJoinNode;
class RuntimeFilterProbeCollector;

class RuntimeFilterHelper {
public:
    // ==================================
    // serialization and deserialization.
    static size_t max_runtime_filter_serialized_size(const JoinRuntimeFilter* rf);
    static size_t serialize_runtime_filter(const JoinRuntimeFilter* rf, uint8_t* data);
    static void deserialize_runtime_filter(ObjectPool* pool, JoinRuntimeFilter** rf, const uint8_t* data, size_t size);
    static JoinRuntimeFilter* create_join_runtime_filter(ObjectPool* pool, PrimitiveType type);

    // =================================
    // create and fill runtime IN filter
    static ExprContext* create_runtime_in_filter(RuntimeState* state, ObjectPool* pool, Expr* probe_expr, bool eq_null);
    static Status fill_runtime_in_filter(const ColumnPtr& column, Expr* probe_expr, ExprContext* filter);

    // ====================================
    static JoinRuntimeFilter* create_runtime_bloom_filter(ObjectPool* pool, PrimitiveType type);
    static Status fill_runtime_bloom_filter(const ColumnPtr& column, PrimitiveType type, JoinRuntimeFilter* filter);
};

// how to generate & publish this runtime filter
// it only happens in hash join node.
class RuntimeFilterBuildDescriptor {
public:
    RuntimeFilterBuildDescriptor() = default;
    Status init(ObjectPool* pool, const TRuntimeFilterDescription& desc);
    int32_t filter_id() const { return _filter_id; }
    ExprContext* build_expr_ctx() { return _build_expr_ctx; }
    PrimitiveType build_expr_type() const { return _build_expr_ctx->root()->type().type; }
    int build_expr_order() const { return _build_expr_order; }
    const TUniqueId& sender_finst_id() const { return _sender_finst_id; }
    bool has_remote_targets() const { return _has_remote_targets; }
    bool has_consumer() const { return _has_consumer; }
    int8_t join_mode() const { return _join_mode; }
    const std::vector<TNetworkAddress>& merge_nodes() const { return _merge_nodes; }
    void set_runtime_filter(JoinRuntimeFilter* rf) { _runtime_filter = rf; }
    JoinRuntimeFilter* runtime_filter() { return _runtime_filter; }

private:
    friend class HashJoinNode;
    int32_t _filter_id;

    ExprContext* _build_expr_ctx = nullptr;
    int _build_expr_order;
    bool _has_remote_targets;
    bool _has_consumer;
    int8_t _join_mode;
    TUniqueId _sender_finst_id;
    std::vector<TNetworkAddress> _merge_nodes;
    JoinRuntimeFilter* _runtime_filter = nullptr;
};

class RuntimeFilterProbeDescriptor {
public:
    RuntimeFilterProbeDescriptor() = default;
    Status init(ObjectPool* pool, const TRuntimeFilterDescription& desc, TPlanNodeId node_id);
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, MemTracker* tracker, RuntimeProfile* p);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);
    int32_t filter_id() const { return _filter_id; }
    ExprContext* probe_expr_ctx() { return _probe_expr_ctx; }
    const JoinRuntimeFilter* runtime_filter() const { return _runtime_filter.load(); }
    void set_runtime_filter(const JoinRuntimeFilter* rf);
    void set_shared_runtime_filter(const std::shared_ptr<const JoinRuntimeFilter>& rf);
    bool is_bound(const std::vector<TupleId>& tuple_ids) const { return _probe_expr_ctx->root()->is_bound(tuple_ids); }
    bool is_probe_slot_ref(SlotId* slot_id) const {
        Expr* probe_expr = _probe_expr_ctx->root();
        if (!probe_expr->is_slotref()) return false;
        ColumnRef* slot_ref = down_cast<ColumnRef*>(probe_expr);
        *slot_id = slot_ref->slot_id();
        return true;
    }
    PrimitiveType probe_expr_type() const { return _probe_expr_ctx->root()->type().type; }
    void replace_probe_expr_ctx(RuntimeState* state, const RowDescriptor& row_desc, MemTracker* tracker,
                                ExprContext* new_probe_expr_ctx);
    std::string debug_string() const;
    JoinRuntimeFilter::RunningContext* runtime_filter_ctx() { return &_runtime_filter_ctx; }

private:
    friend class HashJoinNode;
    int32_t _filter_id;
    ExprContext* _probe_expr_ctx = nullptr;
    std::atomic<const JoinRuntimeFilter*> _runtime_filter;
    std::shared_ptr<const JoinRuntimeFilter> _shared_runtime_filter;
    JoinRuntimeFilter::RunningContext _runtime_filter_ctx;
    // we want to measure when this runtime filter is applied since it's opened.
    RuntimeProfile::Counter* _latency_timer = nullptr;
    int64_t _open_timestamp = 0;
    int64_t _ready_timestamp = 0;
};

// The collection of `RuntimeFilterProbeDescriptor`
class RuntimeFilterProbeCollector {
public:
    RuntimeFilterProbeCollector();
    RuntimeFilterProbeCollector(RuntimeFilterProbeCollector&& that) noexcept;
    size_t size() const { return _descriptors.size(); }
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, MemTracker* tracker, RuntimeProfile* p);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);
    void evaluate(vectorized::Chunk* chunk);
    void add_descriptor(RuntimeFilterProbeDescriptor* desc);
    // accept RuntimeFilterCollector from parent node
    // which means parent node to push down runtime filter.
    void push_down(RuntimeFilterProbeCollector* parent, const std::vector<TupleId>& tuple_ids);
    std::map<int32_t, RuntimeFilterProbeDescriptor*>& descriptors() { return _descriptors; }
    const std::map<int32_t, RuntimeFilterProbeDescriptor*>& descriptors() const { return _descriptors; }
    void set_wait_timeout_ms(int v) { _wait_timeout_ms = v; }
    // wait for all runtime filters are ready.
    void wait();
    std::string debug_string() const;
    bool empty() const { return _descriptors.empty(); }

private:
    void update_selectivity(vectorized::Chunk* chunk);
    void do_evaluate(vectorized::Chunk* chunk);
    void init_counter();
    // mapping from filter id to runtime filter descriptor.
    std::map<int32_t, RuntimeFilterProbeDescriptor*> _descriptors;
    std::map<double, RuntimeFilterProbeDescriptor*> _selectivity;
    size_t _input_chunk_nums = 0;
    int _run_filter_nums = 0;
    int _wait_timeout_ms = 0;

    RuntimeProfile* _runtime_profile = nullptr;
    RuntimeProfile::Counter* _join_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* _join_runtime_filter_input_counter = nullptr;
    RuntimeProfile::Counter* _join_runtime_filter_output_counter = nullptr;
    RuntimeProfile::Counter* _join_runtime_filter_eval_counter = nullptr;
};

} // namespace vectorized
} // namespace starrocks