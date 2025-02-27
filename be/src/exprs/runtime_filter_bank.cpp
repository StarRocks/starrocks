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

#include "exprs/runtime_filter_bank.h"

#include <memory>
#include <thread>

#include "column/column.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/literal.h"
#include "exprs/min_max_predicate.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_layout.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "util/time.h"

namespace starrocks {

struct FilterBuilder {
    template <LogicalType ltype>
    RuntimeFilter* operator()(int8_t join_mode) {
        auto rf = new ComposedRuntimeFilter<ltype>();
        rf->get_bloom_filter()->set_join_mode(join_mode);
        return rf;
    }
};

RuntimeFilter* RuntimeFilterHelper::create_join_runtime_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    RuntimeFilter* filter = type_dispatch_filter(type, (RuntimeFilter*)nullptr, FilterBuilder(), join_mode);
    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

size_t RuntimeFilterHelper::max_runtime_filter_serialized_size(const RuntimeFilter* rf) {
    size_t size = RF_VERSION_SZ;
    size += rf->max_serialized_size();
    return size;
}

size_t RuntimeFilterHelper::serialize_runtime_filter(int rf_version, const RuntimeFilter* rf, uint8_t* data) {
    size_t offset = 0;
    // put version at the head.
    memcpy(data + offset, &rf_version, RF_VERSION_SZ);
    offset += RF_VERSION_SZ;
    offset += rf->serialize(rf_version, data + offset);
    return offset;
}

size_t RuntimeFilterHelper::serialize_runtime_filter(RuntimeState* state, const RuntimeFilter* rf, uint8_t* data) {
    int32_t rf_version = RF_VERSION;
    if (state->func_version() >= TFunctionVersion::RUNTIME_FILTER_SERIALIZE_VERSION_2) {
        rf_version = RF_VERSION_V2;
    }
    return serialize_runtime_filter(rf_version, rf, data);
}

int RuntimeFilterHelper::deserialize_runtime_filter(ObjectPool* pool, RuntimeFilter** rf, const uint8_t* data,
                                                    size_t size) {
    *rf = nullptr;

    size_t offset = 0;

    // read version first.
    uint8_t version = 0;
    memcpy(&version, data, sizeof(version));
    offset += sizeof(version);
    if (version != RF_VERSION_V2) {
        // version mismatch and skip this chunk.
        LOG(WARNING) << "unrecognized version:" << version;
        return 0;
    }

    // peek logical type.
    TPrimitiveType::type type;
    memcpy(&type, data + offset, sizeof(type));
    LogicalType ltype = thrift_to_type(type);

    RuntimeFilter* filter = create_join_runtime_filter(pool, ltype, TJoinDistributionMode::NONE);
    DCHECK(filter != nullptr);
    if (filter != nullptr) {
        offset += filter->deserialize(version, data + offset);
        DCHECK(offset == size);
        *rf = filter;
    }
    return version;
}

struct FilterIniter {
    template <LogicalType ltype>
    auto operator()(const ColumnPtr& column, size_t column_offset, RuntimeFilter* expr, bool eq_null) {
        auto* filter = down_cast<ComposedRuntimeFilter<ltype>*>(expr);

        if (column->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);
            const auto& data_array = GetContainer<ltype>::get_data(nullable_column->data_column().get());
            for (size_t j = column_offset; j < data_array.size(); j++) {
                if (!nullable_column->is_null(j)) {
                    filter->insert(data_array[j]);
                } else {
                    if (eq_null) {
                        filter->insert_null();
                    }
                }
            }
        } else {
            const auto& data_array = GetContainer<ltype>::get_data(column.get());
            for (size_t j = column_offset; j < data_array.size(); j++) {
                filter->insert(data_array[j]);
            }
        }
        return nullptr;
    }
};

Status RuntimeFilterHelper::fill_runtime_bloom_filter(const ColumnPtr& column, LogicalType type, RuntimeFilter* filter,
                                                      size_t column_offset, bool eq_null) {
    if (column->has_large_column()) {
        return Status::NotSupported("unsupported build runtime filter for large binary column");
    }
    type_dispatch_filter(type, nullptr, FilterIniter(), column, column_offset, filter, eq_null);
    return Status::OK();
}

Status RuntimeFilterHelper::fill_runtime_bloom_filter(const std::vector<ColumnPtr>& columns, LogicalType type,
                                                      RuntimeFilter* filter, size_t column_offset, bool eq_null) {
    for (const auto& column : columns) {
        RETURN_IF_ERROR(fill_runtime_bloom_filter(column, type, filter, column_offset, eq_null));
    }
    return Status::OK();
}

Status RuntimeFilterHelper::fill_runtime_bloom_filter(const starrocks::pipeline::RuntimeBloomFilterBuildParam& param,
                                                      LogicalType type, RuntimeFilter* filter, size_t column_offset) {
    return fill_runtime_bloom_filter(param.columns, type, filter, column_offset, param.eq_null);
}

StatusOr<ExprContext*> RuntimeFilterHelper::rewrite_runtime_filter_in_cross_join_node(ObjectPool* pool,
                                                                                      ExprContext* conjunct,
                                                                                      Chunk* chunk) {
    auto left_child = conjunct->root()->get_child(0);
    auto right_child = conjunct->root()->get_child(1);
    // all of the child(1) in expr is in build chunk
    ASSIGN_OR_RETURN(auto res, conjunct->evaluate(right_child, chunk));
    DCHECK_EQ(res->size(), 1);
    ColumnPtr col;
    if (res->is_constant()) {
        col = res;
    } else if (res->is_nullable()) {
        if (res->is_null(0)) {
            col = ColumnHelper::create_const_null_column(1);
        } else {
            auto data_col = down_cast<NullableColumn*>(res.get())->data_column();
            col = std::make_shared<ConstColumn>(data_col, 1);
        }
    } else {
        col = std::make_shared<ConstColumn>(res, 1);
    }

    auto literal = pool->add(new VectorizedLiteral(std::move(col), right_child->type()));
    auto new_expr = conjunct->root()->clone(pool);
    auto new_left = left_child->clone(pool);
    new_expr->clear_children();
    new_expr->add_child(new_left);
    new_expr->add_child(literal);
    auto expr = pool->add(new ExprContext(new_expr));
    expr->set_build_from_only_in_filter(true);
    return expr;
}

Status RuntimeFilterBuildDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc,
                                          RuntimeState* state) {
    _filter_id = desc.filter_id;
    _build_expr_order = desc.expr_order;
    _has_remote_targets = desc.has_remote_targets;

    if (desc.__isset.runtime_filter_merge_nodes) {
        _merge_nodes = desc.runtime_filter_merge_nodes;
    }
    _has_consumer = false;
    _join_mode = desc.build_join_mode;
    if (desc.__isset.plan_node_id_to_target_expr && desc.plan_node_id_to_target_expr.size() != 0) {
        _has_consumer = true;
    }
    if (!desc.__isset.build_expr) {
        return Status::NotFound("build_expr not found");
    }
    if (desc.__isset.sender_finst_id) {
        _sender_finst_id = desc.sender_finst_id;
    }
    if (desc.__isset.broadcast_grf_senders) {
        _broadcast_grf_senders.insert(desc.broadcast_grf_senders.begin(), desc.broadcast_grf_senders.end());
    }
    if (desc.__isset.broadcast_grf_destinations) {
        _broadcast_grf_destinations = desc.broadcast_grf_destinations;
    }
    WithLayoutMixin::init(desc);
    RETURN_IF_ERROR(Expr::create_expr_tree(pool, desc.build_expr, &_build_expr_ctx, state));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc, TPlanNodeId node_id,
                                          RuntimeState* state) {
    _filter_id = desc.filter_id;
    _is_local = !desc.has_remote_targets;
    _build_plan_node_id = desc.build_plan_node_id;
    _runtime_filter.store(nullptr);
    _join_mode = desc.build_join_mode;
    _is_topn_filter = desc.__isset.filter_type && desc.filter_type == TRuntimeFilterBuildType::TOPN_FILTER;
    _skip_wait = _is_topn_filter;
    _is_group_colocate_rf = desc.__isset.build_from_group_execution && desc.build_from_group_execution;

    bool not_found = true;
    if (desc.__isset.plan_node_id_to_target_expr) {
        const auto& it = const_cast<TRuntimeFilterDescription&>(desc).plan_node_id_to_target_expr.find(node_id);
        if (it != desc.plan_node_id_to_target_expr.end()) {
            not_found = false;
            RETURN_IF_ERROR(Expr::create_expr_tree(pool, it->second, &_probe_expr_ctx, state));
        }
    }

    WithLayoutMixin::init(desc);

    if (desc.__isset.plan_node_id_to_partition_by_exprs) {
        const auto& it = const_cast<TRuntimeFilterDescription&>(desc).plan_node_id_to_partition_by_exprs.find(node_id);
        // TODO(lishuming): maybe reuse probe exprs because partition_by_exprs and probe_expr
        // must be overlapped.
        if (it != desc.plan_node_id_to_partition_by_exprs.end()) {
            RETURN_IF_ERROR(Expr::create_expr_trees(pool, it->second, &_partition_by_exprs_contexts, state));
        }
    }

    if (not_found) {
        return Status::NotFound("plan node id not found. node_id = " + std::to_string(node_id));
    }
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::init(int32_t filter_id, ExprContext* probe_expr_ctx) {
    _filter_id = filter_id;
    _probe_expr_ctx = probe_expr_ctx;
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::prepare(RuntimeState* state, RuntimeProfile* p) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->prepare(state));
    }
    for (auto* partition_by_expr : _partition_by_exprs_contexts) {
        RETURN_IF_ERROR(partition_by_expr->prepare(state));
    }
    _open_timestamp = UnixMillis();
    _latency_timer = ADD_COUNTER(p, strings::Substitute("JoinRuntimeFilter/$0/latency", _filter_id), TUnit::TIME_NS);
    // not set yet.
    _latency_timer->set((int64_t)(-1));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::open(RuntimeState* state) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->open(state));
    }
    for (auto* partition_by_expr : _partition_by_exprs_contexts) {
        RETURN_IF_ERROR(partition_by_expr->open(state));
    }
    return Status::OK();
}

void RuntimeFilterProbeDescriptor::close(RuntimeState* state) {
    if (_probe_expr_ctx != nullptr) {
        _probe_expr_ctx->close(state);
    }
    for (auto* partition_by_expr : _partition_by_exprs_contexts) {
        partition_by_expr->close(state);
    }
}

void RuntimeFilterProbeDescriptor::replace_probe_expr_ctx(RuntimeState* state, const RowDescriptor& row_desc,
                                                          ExprContext* new_probe_expr_ctx) {
    // close old probe expr
    _probe_expr_ctx->close(state);
    // create new probe expr and open it.
    _probe_expr_ctx = state->obj_pool()->add(new ExprContext(new_probe_expr_ctx->root()));
    WARN_IF_ERROR(_probe_expr_ctx->prepare(state), "prepare probe expr failed");
    WARN_IF_ERROR(_probe_expr_ctx->open(state), "open probe expr failed");
}

std::string RuntimeFilterProbeDescriptor::debug_string() const {
    std::stringstream ss;
    ss << "RFDptr(filter_id=" << _filter_id << ", probe_expr=";
    if (_probe_expr_ctx != nullptr) {
        ss << "(addr = " << _probe_expr_ctx << ", expr = " << _probe_expr_ctx->root()->debug_string() << ")";
    } else {
        ss << "nullptr";
    }
    ss << ", is_local=" << _is_local;
    ss << ", is_topn=" << _is_topn_filter;
    ss << ", rf=";
    const RuntimeFilter* rf = _runtime_filter.load();
    if (rf != nullptr) {
        ss << rf->debug_string();
    } else {
        ss << "nullptr";
    }
    ss << ")";
    return ss.str();
}

static const int default_runtime_filter_wait_timeout_ms = 1000;

RuntimeFilterProbeCollector::RuntimeFilterProbeCollector() : _wait_timeout_ms(default_runtime_filter_wait_timeout_ms) {}

RuntimeFilterProbeCollector::RuntimeFilterProbeCollector(RuntimeFilterProbeCollector&& that) noexcept
        : _descriptors(std::move(that._descriptors)),
          _wait_timeout_ms(that._wait_timeout_ms),
          _scan_wait_timeout_ms(that._scan_wait_timeout_ms),
          _eval_context(that._eval_context),
          _plan_node_id(that._plan_node_id) {}

Status RuntimeFilterProbeCollector::prepare(RuntimeState* state, RuntimeProfile* profile) {
    _runtime_profile = profile;
    _runtime_state = state;
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->prepare(state, profile));
    }
    if (state != nullptr) {
        const TQueryOptions& options = state->query_options();
        if (options.__isset.runtime_filter_early_return_selectivity) {
            _early_return_selectivity = options.runtime_filter_early_return_selectivity;
        }
    }
    return Status::OK();
}
Status RuntimeFilterProbeCollector::open(RuntimeState* state) {
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->open(state));
    }
    return Status::OK();
}
void RuntimeFilterProbeCollector::close(RuntimeState* state) {
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        rf_desc->close(state);
    }
}

// do_evaluate is reentrant, can be called concurrently by multiple operators that shared the same
// RuntimeFilterProbeCollector.
void RuntimeFilterProbeCollector::do_evaluate(Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context) {
    if (eval_context.mode == RuntimeBloomFilterEvalContext::Mode::M_ONLY_TOPN) {
        update_selectivity(chunk, eval_context);
        return;
    } else {
        if ((eval_context.input_chunk_nums++ & 31) == 0) {
            update_selectivity(chunk, eval_context);
            return;
        }
    }

    auto& seletivity_map = eval_context.selectivity;
    if (seletivity_map.empty()) {
        return;
    }

    auto& selection = eval_context.running_context.selection;
    eval_context.running_context.use_merged_selection = false;
    eval_context.running_context.compatibility =
            _runtime_state->func_version() <= 3 || !_runtime_state->enable_pipeline_engine();

    for (auto& kv : seletivity_map) {
        RuntimeFilterProbeDescriptor* rf_desc = kv.second;
        const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
        bool skip_topn = eval_context.mode == RuntimeBloomFilterEvalContext::Mode::M_WITHOUT_TOPN;
        if ((skip_topn && rf_desc->is_topn_filter()) || filter == nullptr || filter->always_true()) {
            continue;
        }
        if (rf_desc->has_push_down_to_storage()) {
            continue;
        }

        auto* ctx = rf_desc->probe_expr_ctx();
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(ctx, ctx->root(), chunk);

        // for colocate grf
        compute_hash_values(chunk, column.get(), rf_desc, eval_context);

        filter->evaluate(column.get(), &eval_context.running_context);

        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;

        if (true_count == 0) {
            chunk->set_num_rows(0);
            return;
        } else {
            chunk->filter(selection);
        }
    }
}

void RuntimeFilterProbeCollector::do_evaluate_partial_chunk(Chunk* partial_chunk,
                                                            RuntimeBloomFilterEvalContext& eval_context) {
    auto& selection = eval_context.running_context.selection;
    eval_context.running_context.use_merged_selection = false;
    eval_context.running_context.compatibility =
            _runtime_state->func_version() <= 3 || !_runtime_state->enable_pipeline_engine();

    // since partial chunk is currently very lightweight (a bunch of const columns), use every runtime filter if possible
    // without computing each rf's selectivity
    for (auto kv : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = kv.second;
        const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
        if (filter == nullptr || filter->always_true()) {
            continue;
        }

        auto only_reference_existent_slots = [&](ExprContext* expr) {
            std::vector<SlotId> slot_ids;
            int n = expr->root()->get_slot_ids(&slot_ids);
            DCHECK(slot_ids.size() == n);

            // do not allow struct subfield
            if (expr->root()->get_subfields(nullptr) > 0) {
                return false;
            }

            for (auto slot_id : slot_ids) {
                if (!partial_chunk->is_slot_exist(slot_id)) {
                    return false;
                }
            }

            return true;
        };

        auto* probe_expr = rf_desc->probe_expr_ctx();
        auto* partition_by_exprs = rf_desc->partition_by_expr_contexts();

        bool can_use_rf_on_partial_chunk = only_reference_existent_slots(probe_expr);
        for (auto* part_by_expr : *partition_by_exprs) {
            can_use_rf_on_partial_chunk &= only_reference_existent_slots(part_by_expr);
        }

        // skip runtime filter that references a non-existent column for the partial chunk
        if (!can_use_rf_on_partial_chunk) {
            continue;
        }

        ColumnPtr column = EVALUATE_NULL_IF_ERROR(probe_expr, probe_expr->root(), partial_chunk);
        // for colocate grf
        compute_hash_values(partial_chunk, column.get(), rf_desc, eval_context);
        filter->evaluate(column.get(), &eval_context.running_context);

        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;

        if (true_count == 0) {
            partial_chunk->set_num_rows(0);
            return;
        } else {
            partial_chunk->filter(selection);
        }
    }
}

void RuntimeFilterProbeCollector::init_counter() {
    _eval_context.join_runtime_filter_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterTime");
    _eval_context.join_runtime_filter_hash_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterHashTime");
    _eval_context.join_runtime_filter_input_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterInputRows", TUnit::UNIT);
    _eval_context.join_runtime_filter_output_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
    _eval_context.join_runtime_filter_eval_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterEvaluate", TUnit::UNIT);
}

void RuntimeFilterProbeCollector::evaluate(Chunk* chunk) {
    if (_descriptors.empty()) return;
    if (_eval_context.join_runtime_filter_timer == nullptr) {
        init_counter();
    }
    evaluate(chunk, _eval_context);
}

void RuntimeFilterProbeCollector::evaluate(Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context) {
    if (_descriptors.empty()) return;
    size_t before = chunk->num_rows();
    if (before == 0) return;

    {
        SCOPED_TIMER(eval_context.join_runtime_filter_timer);
        eval_context.join_runtime_filter_input_counter->update(before);
        eval_context.run_filter_nums = 0;
        do_evaluate(chunk, eval_context);
        size_t after = chunk->num_rows();
        eval_context.join_runtime_filter_output_counter->update(after);
        eval_context.join_runtime_filter_eval_counter->update(eval_context.run_filter_nums);
    }
}

void RuntimeFilterProbeCollector::evaluate_partial_chunk(Chunk* partial_chunk,
                                                         RuntimeBloomFilterEvalContext& eval_context) {
    if (_descriptors.empty()) return;
    size_t before = partial_chunk->num_rows();
    if (before == 0) return;

    {
        SCOPED_TIMER(eval_context.join_runtime_filter_timer);
        eval_context.join_runtime_filter_input_counter->update(before);
        eval_context.run_filter_nums = 0;
        do_evaluate_partial_chunk(partial_chunk, eval_context);
        size_t after = partial_chunk->num_rows();
        eval_context.join_runtime_filter_output_counter->update(after);
        eval_context.join_runtime_filter_eval_counter->update(eval_context.run_filter_nums);
    }
}

void RuntimeFilterProbeCollector::compute_hash_values(Chunk* chunk, Column* column,
                                                      RuntimeFilterProbeDescriptor* rf_desc,
                                                      RuntimeBloomFilterEvalContext& eval_context) {
    // TODO: Hash values will be computed multi times for runtime filters with the same partition_by_exprs.
    SCOPED_TIMER(eval_context.join_runtime_filter_hash_timer);
    const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
    DCHECK(filter);
    if (filter->num_hash_partitions() == 0) {
        return;
    }

    if (rf_desc->partition_by_expr_contexts()->empty()) {
        filter->compute_partition_index(rf_desc->layout(), {column}, &eval_context.running_context);
    } else {
        // Used to hold generated columns
        std::vector<ColumnPtr> column_holders;
        std::vector<const Column*> partition_by_columns;
        for (auto& partition_ctx : *(rf_desc->partition_by_expr_contexts())) {
            ColumnPtr partition_column = EVALUATE_NULL_IF_ERROR(partition_ctx, partition_ctx->root(), chunk);
            partition_by_columns.push_back(partition_column.get());
            column_holders.emplace_back(std::move(partition_column));
        }
        filter->compute_partition_index(rf_desc->layout(), partition_by_columns, &eval_context.running_context);
    }
}

void RuntimeFilterProbeCollector::update_selectivity(Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context) {
    size_t chunk_size = chunk->num_rows();
    auto& merged_selection = eval_context.running_context.merged_selection;
    auto& use_merged_selection = eval_context.running_context.use_merged_selection;
    eval_context.running_context.compatibility =
            _runtime_state->func_version() <= 3 || !_runtime_state->enable_pipeline_engine();
    auto& seletivity_map = eval_context.selectivity;
    use_merged_selection = true;

    seletivity_map.clear();
    for (auto& kv : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = kv.second;
        const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
        bool should_use =
                eval_context.mode == RuntimeBloomFilterEvalContext::Mode::M_ONLY_TOPN && rf_desc->is_topn_filter();
        if (filter == nullptr || (!should_use && filter->always_true())) {
            continue;
        }
        if (eval_context.mode == RuntimeBloomFilterEvalContext::Mode::M_WITHOUT_TOPN && rf_desc->is_topn_filter()) {
            continue;
        } else if (eval_context.mode == RuntimeBloomFilterEvalContext::Mode::M_ONLY_TOPN &&
                   !rf_desc->is_topn_filter()) {
            continue;
        }

        if (rf_desc->has_push_down_to_storage()) {
            continue;
        }
        auto& selection = eval_context.running_context.use_merged_selection
                                  ? eval_context.running_context.merged_selection
                                  : eval_context.running_context.selection;
        auto ctx = rf_desc->probe_expr_ctx();
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(ctx, ctx->root(), chunk);
        // for colocate grf
        compute_hash_values(chunk, column.get(), rf_desc, eval_context);
        // true count is not accummulated, it is evaluated for each RF respectively
        filter->evaluate(column.get(), &eval_context.running_context);
        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;
        double selectivity = true_count * 1.0 / chunk_size;
        if (selectivity <= 0.5) {                          // useful filter
            if (selectivity < _early_return_selectivity) { // very useful filter, could early return
                seletivity_map.clear();
                seletivity_map.emplace(selectivity, rf_desc);
                chunk->filter(selection);
                return;
            }

            // Only choose three most selective runtime filters
            if (seletivity_map.size() < 3) {
                seletivity_map.emplace(selectivity, rf_desc);
            } else {
                auto it = seletivity_map.end();
                it--;
                if (selectivity < it->first) {
                    seletivity_map.erase(it);
                    seletivity_map.emplace(selectivity, rf_desc);
                }
            }

            if (use_merged_selection) {
                use_merged_selection = false;
            } else {
                uint8_t* dest = merged_selection.data();
                const uint8_t* src = selection.data();
                for (size_t j = 0; j < chunk_size; ++j) {
                    dest[j] = src[j] & dest[j];
                }
            }
        } else if (rf_desc->is_topn_filter() && eval_context.mode == RuntimeBloomFilterEvalContext::Mode::M_ONLY_TOPN) {
            seletivity_map.emplace(selectivity, rf_desc);
        }
    }
    if (!seletivity_map.empty()) {
        chunk->filter(merged_selection);
    }
}

static bool contains_dict_mapping_expr(Expr* expr) {
    if (typeid(*expr) == typeid(DictMappingExpr)) {
        return true;
    }

    return std::any_of(expr->children().begin(), expr->children().end(),
                       [](Expr* child) { return contains_dict_mapping_expr(child); });
}

static bool contains_dict_mapping_expr(RuntimeFilterProbeDescriptor* probe_desc) {
    auto* probe_expr_ctx = probe_desc->probe_expr_ctx();
    if (probe_expr_ctx == nullptr) {
        return false;
    }
    return contains_dict_mapping_expr(probe_expr_ctx->root());
}

void RuntimeFilterProbeCollector::push_down(const RuntimeState* state, TPlanNodeId target_plan_node_id,
                                            RuntimeFilterProbeCollector* parent, const std::vector<TupleId>& tuple_ids,
                                            std::set<TPlanNodeId>& local_rf_waiting_set) {
    if (this == parent) return;
    auto iter = parent->_descriptors.begin();
    while (iter != parent->_descriptors.end()) {
        RuntimeFilterProbeDescriptor* desc = iter->second;
        if (!desc->can_push_down_runtime_filter()) {
            ++iter;
            continue;
        }
        if (desc->is_bound(tuple_ids) &&
            !(state->broadcast_join_right_offsprings().contains(target_plan_node_id) &&
              state->non_broadcast_rf_ids().contains(desc->filter_id())) &&
            !contains_dict_mapping_expr(desc)) {
            add_descriptor(desc);
            if (desc->is_local()) {
                local_rf_waiting_set.insert(desc->build_plan_node_id());
            }
            iter = parent->_descriptors.erase(iter);
        } else {
            ++iter;
        }
    }
}

std::string RuntimeFilterProbeCollector::debug_string() const {
    std::stringstream ss;
    ss << "RFColl(";
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* desc = it.second;
        if (desc != nullptr) {
            ss << "[" << desc->debug_string() << "]";
        }
    }
    ss << ")";
    return ss.str();
}

void RuntimeFilterProbeCollector::add_descriptor(RuntimeFilterProbeDescriptor* desc) {
    _descriptors[desc->filter_id()] = desc;
}

// only used in non-pipeline mode
void RuntimeFilterProbeCollector::wait(bool on_scan_node) {
    if (_descriptors.empty()) return;

    std::list<RuntimeFilterProbeDescriptor*> wait_list;
    for (auto& it : _descriptors) {
        auto* rf = it.second;
        int filter_id = rf->filter_id();
        VLOG_FILE << "RuntimeFilterCollector::wait start. filter_id = " << filter_id
                  << ", plan_node_id = " << _plan_node_id << ", finst_id = " << _runtime_state->fragment_instance_id();
        wait_list.push_back(it.second);
    }

    int wait_time = _wait_timeout_ms;
    if (on_scan_node) {
        wait_time = _scan_wait_timeout_ms;
    }
    const int wait_interval = 5;
    auto wait_duration = std::chrono::milliseconds(wait_interval);
    while (wait_time >= 0 && !wait_list.empty()) {
        auto it = wait_list.begin();
        while (it != wait_list.end()) {
            auto* rf = (*it)->runtime_filter(-1);
            // find runtime filter in cache.
            if (rf == nullptr) {
                RuntimeFilterPtr t = _runtime_state->exec_env()->runtime_filter_cache()->get(_runtime_state->query_id(),
                                                                                             (*it)->filter_id());
                if (t != nullptr) {
                    VLOG_FILE << "RuntimeFilterCollector::wait: rf found in cache. filter_id = " << (*it)->filter_id()
                              << ", plan_node_id = " << _plan_node_id
                              << ", finst_id  = " << _runtime_state->fragment_instance_id();
                    (*it)->set_shared_runtime_filter(t);
                    rf = t.get();
                }
            }
            if (rf != nullptr) {
                it = wait_list.erase(it);
            } else {
                ++it;
            }
        }
        if (wait_list.empty()) break;
        std::this_thread::sleep_for(wait_duration);
        wait_time -= wait_interval;
    }

    if (_descriptors.size() != 0) {
        for (const auto& it : _descriptors) {
            auto* rf = it.second;
            int filter_id = rf->filter_id();
            bool ready = (rf->runtime_filter(-1) != nullptr);
            VLOG_FILE << "RuntimeFilterCollector::wait start. filter_id = " << filter_id
                      << ", plan_node_id = " << _plan_node_id
                      << ", finst_id = " << _runtime_state->fragment_instance_id()
                      << ", ready = " << std::to_string(ready);
        }
    }
}

void RuntimeFilterProbeDescriptor::set_runtime_filter(const RuntimeFilter* rf) {
    auto notify = DeferOp([this]() { _observable.notify_source_observers(); });
    const RuntimeFilter* expected = nullptr;
    _runtime_filter.compare_exchange_strong(expected, rf, std::memory_order_seq_cst, std::memory_order_seq_cst);
    if (_ready_timestamp == 0 && rf != nullptr && _latency_timer != nullptr) {
        _ready_timestamp = UnixMillis();
        _latency_timer->set((_ready_timestamp - _open_timestamp) * 1000);
    }
}

void RuntimeFilterProbeDescriptor::set_shared_runtime_filter(const std::shared_ptr<const RuntimeFilter>& rf) {
    std::shared_ptr<const RuntimeFilter> old_value = nullptr;
    if (std::atomic_compare_exchange_strong(&_shared_runtime_filter, &old_value, rf)) {
        set_runtime_filter(_shared_runtime_filter.get());
    }
}

void RuntimeFilterHelper::create_min_max_value_predicate(ObjectPool* pool, SlotId slot_id, LogicalType slot_type,
                                                         const RuntimeFilter* filter, Expr** min_max_predicate) {
    *min_max_predicate = nullptr;
    if (filter == nullptr) return;
    // TODO, if you want to enable it for string, pls adapt for low-cardinality string
    if (slot_type == TYPE_CHAR || slot_type == TYPE_VARCHAR) return;
    auto res = type_dispatch_filter(slot_type, (Expr*)nullptr, MinMaxPredicateBuilder(pool, slot_id, filter));
    *min_max_predicate = res;
}

} // namespace starrocks
