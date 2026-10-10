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

#include "compute_env/query/partition_scan_range_pruner.h"

#include <algorithm>
#include <span>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/runtime_type_traits.h"
#include "common/object_pool.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/literal.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

StatusOr<ColumnPtr> build_partition_col_values(const SlotDescriptor* slot_desc, const TKeyRange& column_range,
                                               ObjectPool* obj_pool, RuntimeState* state) {
    if (column_range.__isset.list_values && !column_range.list_values.empty()) {
        std::vector<ExprContext*> ctxs;
        for (const auto& obj : column_range.list_values) {
            RETURN_IF_ERROR(ExprFactory::create_expr_tree(obj_pool, obj, &ctxs.emplace_back(), state));
            DCHECK(ctxs.back()->root()->is_constant());
        }
        RETURN_IF_ERROR(ExprExecutor::prepare(ctxs, state));
        RETURN_IF_ERROR(ExprExecutor::open(ctxs, state));

        auto col = ColumnHelper::create_column(slot_desc->type(), true, false, column_range.list_values.size(), false);
        for (auto* ctx : ctxs) {
            ASSIGN_OR_RETURN(ColumnPtr v, ctx->root()->evaluate_const(ctx));
            if (v->only_null()) {
                col->append_nulls(1);
                continue;
            }
            auto cv = ColumnHelper::unpack_and_duplicate_const_column(1, v);
            col->append(*cv, 0, 1);
        }
        ExprExecutor::close(ctxs, state);
        return col;
    } else if (column_range.__isset.begin_key && column_range.__isset.end_key) {
        if (slot_desc->type().is_date_type()) {
            auto lower_julian = date::from_date_literal(column_range.begin_key);
            auto upper_julian = date::from_date_literal(column_range.end_key);

            auto col =
                    ColumnHelper::create_column(slot_desc->type(), true, false, upper_julian - lower_julian + 1, false);
            for (JulianDate date = lower_julian; date <= upper_julian; date++) {
                col->append_datum(Datum(DateValue{date}));
            }
            if (column_range.__isset.has_null && column_range.has_null) {
                col->append_nulls(1);
            }
            return col;
        } else if (slot_desc->type().is_integer_type()) {
            size_t size = column_range.end_key - column_range.begin_key + 1;
            auto col = ColumnHelper::create_column(slot_desc->type(), true, false, size, false);
#define M(TYPE)                                                                    \
    if (slot_desc->type().type == TYPE) {                                          \
        for (int64_t v = column_range.begin_key; v <= column_range.end_key; v++) { \
            col->append_datum(Datum((RunTimeTypeTraits<TYPE>::CppType)v));         \
        }                                                                          \
    }
            APPLY_FOR_ALL_INT_TYPE(M)
#undef M
            if (column_range.__isset.has_null && column_range.has_null) {
                col->append_nulls(1);
            }
            return col;
        } else {
            DCHECK(false) << "Unsupported partition column range, column name: " << column_range.column_name;
            return Status::InternalError("Unsupported partition column range");
        }
    } else {
        DCHECK(false) << "Unsupported partition column range, column name: " << column_range.column_name;
        return Status::InternalError("Unsupported partition column range");
    }
}

Status prune_scan_ranges_by_partition_conjuncts(RuntimeState* state, const TupleDescriptor* tuple_desc,
                                                const std::vector<ExprContext*>& partition_conjunct_ctxs,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                std::vector<TScanRangeParams>* pruned_scan_ranges) {
    if (partition_conjunct_ctxs.empty() || tuple_desc == nullptr) {
        *pruned_scan_ranges = scan_ranges;
        return Status::OK();
    }

    phmap::flat_hash_map<std::string, SlotDescriptor*> column_name_to_slot;
    for (auto* slot : tuple_desc->slots()) {
        column_name_to_slot[slot->col_name()] = slot;
    }

    ObjectPool obj_pool;
    std::vector<TScanRangeParams> temp;
    temp.reserve(scan_ranges.size());
    for (const auto& scan_range : scan_ranges) {
        const auto& internal_range = scan_range.scan_range.internal_scan_range;
        if (!internal_range.__isset.partition_column_ranges || internal_range.partition_column_ranges.empty()) {
            temp.emplace_back(scan_range);
            continue;
        }

        bool is_pruned = false;
        for (const auto& partition_column_range : internal_range.partition_column_ranges) {
            auto it = column_name_to_slot.find(partition_column_range.column_name);
            if (it == column_name_to_slot.end()) {
                continue;
            }
            auto* slot = it->second;
            ASSIGN_OR_RETURN(auto col, build_partition_col_values(slot, partition_column_range, &obj_pool, state));

            Chunk partition_cols_chunk;
            Filter filter(col->size(), 1);
            partition_cols_chunk.append_column(std::move(col), slot->id());

            std::vector<SlotId> slot_ids;
            for (auto* ctx : partition_conjunct_ctxs) {
                slot_ids.clear();
                if (ctx->root()->get_slot_ids(&slot_ids) != 1 || slot_ids[0] != slot->id()) {
                    continue;
                }
                ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(&partition_cols_chunk, filter.data()));
                size_t true_count = ColumnHelper::count_true_with_notnull(column);
                if (true_count == column->size()) {
                    continue;
                } else if (0 == true_count) {
                    is_pruned = true;
                    break;
                } else {
                    bool all_zero = false;
                    ColumnHelper::merge_two_filters(column, &filter, &all_zero);
                    if (all_zero) {
                        is_pruned = true;
                        break;
                    }
                }
            }
            if (is_pruned) {
                break;
            }
        }

        if (!is_pruned) {
            temp.emplace_back(scan_range);
        }
    }
    pruned_scan_ranges->swap(temp);
    return Status::OK();
}

// Decode integers directly and DATE from yyyymmdd.
static ColumnPtr build_rf_partition_int_values(const SlotDescriptor* slot_desc, std::span<const int64_t> values) {
    auto column = ColumnHelper::create_column(slot_desc->type(), true);
    column->reserve(values.size());
    if (slot_desc->type().is_date_type()) {
        for (int64_t value : values) {
            DateValue date;
            date.from_date_literal(value);
            column->append_datum(Datum(date));
        }
    } else if (slot_desc->type().is_integer_type()) {
#define M(LT)                                                                    \
    if (slot_desc->type().type == LT) {                                          \
        for (int64_t value : values) {                                           \
            column->append_datum(Datum(static_cast<RunTimeCppType<LT>>(value))); \
        }                                                                        \
    }
        APPLY_FOR_ALL_INT_TYPE(M)
#undef M
    } else {
        return nullptr;
    }
    return column;
}

static ColumnPtr build_rf_partition_literal_values(const SlotDescriptor* slot_desc, std::span<const TExpr> literals) {
    auto column = ColumnHelper::create_column(slot_desc->type(), true);
    column->reserve(literals.size());
    for (const auto& expr : literals) {
        VectorizedLiteral literal(expr.nodes[0]);
        column->append_datum(literal.value()->get(0));
    }
    return column;
}

RuntimeFilterPartitionBoundaryMap parse_partition_boundaries(const TupleDescriptor* tuple_desc,
                                                             const std::vector<TPartitionBoundary>& thrift_boundaries) {
    RuntimeFilterPartitionBoundaryMap boundaries;
    for (const auto& thrift_boundary : thrift_boundaries) {
        auto* slot = tuple_desc->get_slot_by_id(thrift_boundary.slot_id);
        if (slot == nullptr) {
            continue;
        }

        RuntimeFilterPartitionBoundary boundary;
        boundary.physical_partition_ids = thrift_boundary.physical_partition_ids;
        boundary.slot_id = thrift_boundary.slot_id;
        boundary.column_type = slot->type();
        boundary.contains_null = thrift_boundary.contains_null;
        boundary.upper_bound_closed = thrift_boundary.range_upper_closed;
        // LIST uses a value list; RANGE uses endpoints.
        if (thrift_boundary.__isset.list_int_values) {
            boundary.list_values = build_rf_partition_int_values(slot, thrift_boundary.list_int_values);
        } else if (thrift_boundary.__isset.list_values) {
            boundary.list_values = build_rf_partition_literal_values(slot, thrift_boundary.list_values);
        } else {
            if (thrift_boundary.__isset.range_lower_int) {
                boundary.lower_bound = build_rf_partition_int_values(slot, {&thrift_boundary.range_lower_int, 1});
            } else if (thrift_boundary.__isset.range_lower) {
                boundary.lower_bound = build_rf_partition_literal_values(slot, {&thrift_boundary.range_lower, 1});
            }
            if (thrift_boundary.__isset.range_upper_int) {
                boundary.upper_bound = build_rf_partition_int_values(slot, {&thrift_boundary.range_upper_int, 1});
            } else if (thrift_boundary.__isset.range_upper) {
                boundary.upper_bound = build_rf_partition_literal_values(slot, {&thrift_boundary.range_upper, 1});
            }
        }
        // Skip boundaries with no decoded values or endpoints.
        if (boundary.list_values == nullptr && boundary.lower_bound == nullptr && boundary.upper_bound == nullptr) {
            continue;
        }
        boundaries[boundary.slot_id].emplace_back(std::move(boundary));
    }
    return boundaries;
}

static bool parse_join_runtime_in_filter(const Expr& root, bool need_values, ColumnPtr* values, bool* matches_null) {
    // The join build side instantiates the predicate on the probe slot's own type (no CHAR->VARCHAR
    // mapping, unlike the static IN factory), so the cast target must follow that type exactly.
    return type_dispatch_filter(root.get_child(0)->type().type, false, [&]<LogicalType LT>() {
        const auto* predicate = dynamic_cast<const VectorizedInConstPredicate<LT>*>(&root);
        if (predicate == nullptr || !predicate->is_join_runtime_filter()) {
            return false;
        }
        if (need_values) {
            *values = predicate->get_all_values();
        }
        *matches_null = predicate->null_in_set() && predicate->is_eq_null();
        return true;
    });
}

RuntimeFilterPartitionPruner::RuntimeFilterPartitionPruner(RuntimeFilterPartitionBoundaryMap boundaries)
        : _boundaries(std::move(boundaries)) {
    // Boundaries of different partition columns may name the same physical partition; count each once.
    std::unordered_set<int64_t> partition_ids;
    for (const auto& [slot_id, slot_boundaries] : _boundaries) {
        for (const auto& boundary : slot_boundaries) {
            partition_ids.insert(boundary.physical_partition_ids.begin(), boundary.physical_partition_ids.end());
        }
    }
    _candidate_partition_count = static_cast<int64_t>(partition_ids.size());
}

void RuntimeFilterPartitionPruner::publish_pruned_partitions(const PrunedPartitions& current,
                                                             const PrunedPartitions& newly_pruned_partitions) {
    if (newly_pruned_partitions.empty()) {
        return;
    }
    auto published = std::make_shared<PrunedPartitions>(current);
    published->insert(newly_pruned_partitions.begin(), newly_pruned_partitions.end());
    const bool all_pruned = published->size() == _candidate_partition_count;
    _pruned_partitions.store(std::move(published), std::memory_order_release);
    if (all_pruned) {
        // Publish completion after the snapshot.
        _bloom_pruning_complete.store(true, std::memory_order_release);
    }
}

void RuntimeFilterPartitionPruner::prune_by_in_filters(const std::vector<ExprContext*>& runtime_in_filters) {
    std::lock_guard<std::mutex> update_guard(_update_mutex);
    auto current = _pruned_partitions.load(std::memory_order_acquire);
    PrunedPartitions newly_pruned_partitions;
    for (auto* filter : runtime_in_filters) {
        const Expr* root = filter->root();
        if (root->get_num_children() == 0 || !root->get_child(0)->is_slotref()) {
            continue;
        }
        auto slot_boundaries = _boundaries.find(down_cast<const ColumnRef*>(root->get_child(0))->slot_id());
        if (slot_boundaries == _boundaries.end()) {
            continue;
        }
        ColumnPtr values;
        bool matches_null = false;
        if (!parse_join_runtime_in_filter(*root, slot_boundaries->second.front().is_range(), &values, &matches_null)) {
            continue;
        }
        for (const auto& boundary : slot_boundaries->second) {
            // Preserve NULL partitions for null-safe joins.
            if (boundary.contains_null && matches_null) {
                continue;
            }
            bool match = false;
            if (boundary.is_range()) {
                // RANGE: does any IN value fall inside the partition interval?
                for (size_t i = 0; i < values->size(); ++i) {
                    if (values->is_null(i)) {
                        continue;
                    }
                    if (boundary.lower_bound != nullptr && values->compare_at(i, 0, *boundary.lower_bound, -1) < 0) {
                        continue;
                    }
                    if (boundary.upper_bound == nullptr) {
                        match = true;
                        break;
                    }
                    const int comparison = values->compare_at(i, 0, *boundary.upper_bound, -1);
                    if (comparison < 0 || (comparison == 0 && boundary.upper_bound_closed)) {
                        match = true;
                        break;
                    }
                }
            } else if (!boundary.list_values->empty()) {
                // LIST: run the IN predicate itself over the partition value set.
                Chunk chunk;
                chunk.append_column(boundary.list_values, boundary.slot_id);
                auto result = filter->evaluate(&chunk);
                if (result.ok()) {
                    ColumnViewer<TYPE_BOOLEAN> viewer(result.value());
                    for (size_t i = 0; i < viewer.size(); ++i) {
                        if (!viewer.is_null(i) && viewer.value(i)) {
                            match = true;
                            break;
                        }
                    }
                } else {
                    match = true;
                }
            }
            if (!match) {
                newly_pruned_partitions.insert(boundary.physical_partition_ids.begin(),
                                               boundary.physical_partition_ids.end());
            }
        }
    }
    publish_pruned_partitions(*current, newly_pruned_partitions);
}

void RuntimeFilterPartitionPruner::prune_by_bloom_filters(RuntimeFilterProbeCollector& runtime_bloom_filters,
                                                          RuntimeState* state) {
    if (_bloom_pruning_complete.load(std::memory_order_acquire)) {
        return;
    }
    std::unique_lock<std::mutex> update_guard(_update_mutex, std::try_to_lock);
    if (!update_guard.owns_lock() || _bloom_pruning_complete.load(std::memory_order_acquire)) {
        return;
    }

    auto current = _pruned_partitions.load(std::memory_order_acquire);
    PrunedPartitions newly_pruned_partitions;
    for (const auto& entry : runtime_bloom_filters.descriptors()) {
        const int32_t filter_id = entry.first;
        const auto& descriptor = entry.second;
        if (_processed_filter_ids.count(filter_id) != 0) {
            continue;
        }
        SlotId slot_id;
        if (descriptor->is_stream_build_filter() || !descriptor->can_push_down_runtime_filter() ||
            !descriptor->is_probe_slot_ref(&slot_id)) {
            _processed_filter_ids.emplace(filter_id);
            continue;
        }
        auto slot_boundaries = _boundaries.find(slot_id);
        if (slot_boundaries == _boundaries.end()) {
            _processed_filter_ids.emplace(filter_id);
            continue;
        }
        const RuntimeFilter* filter = descriptor->runtime_filter(0);
        if (filter == nullptr) {
            // The filter has not arrived yet.
            continue;
        }

        if (filter->always_true()) {
            _processed_filter_ids.emplace(filter_id);
            continue;
        }
        // Reuse temporary buffers across this filter's boundaries.
        RuntimeFilter::RunningContext context;
        context.use_merged_selection = false;
        context.compatibility = state->func_version() <= 3 || !state->enable_pipeline_engine();
        context.exchange_hash_function_version = state->query_options().exchange_hash_function_version;
        for (const auto& boundary : slot_boundaries->second) {
            const auto& ids = boundary.physical_partition_ids;
            // Preserve NULL partitions for null-safe joins.
            if (boundary.contains_null && filter->has_null()) {
                continue;
            }
            bool match = true;
            if (boundary.is_range()) {
                match = type_dispatch_filter(boundary.column_type.type, true, [&]<LogicalType LT>() {
                    const auto* min_max = down_cast<const MinMaxRuntimeFilter<LT>*>(filter->get_min_max_filter());
                    if (min_max->is_empty_range()) {
                        return false;
                    }
                    if (boundary.upper_bound != nullptr) {
                        ColumnViewer<LT> upper(boundary.upper_bound);
                        if (min_max->min() > upper.value(0) ||
                            (min_max->min() == upper.value(0) && !boundary.upper_bound_closed)) {
                            return false;
                        }
                    }
                    if (boundary.lower_bound != nullptr) {
                        ColumnViewer<LT> lower(boundary.lower_bound);
                        if (min_max->max() < lower.value(0) ||
                            (min_max->max() == lower.value(0) && !min_max->right_close_interval())) {
                            return false;
                        }
                    }
                    return true;
                });
            } else {
                // Global filters route each value to its hash partition.
                context.selection.assign(boundary.list_values->size(), 1);
                if (filter->num_hash_partitions() > 0) {
                    filter->compute_partition_index(descriptor->layout(), {boundary.list_values.get()}, &context);
                }
                filter->evaluate(boundary.list_values.get(), &context);
                match = std::any_of(context.selection.begin(), context.selection.end(),
                                    [](uint8_t selected) { return selected != 0; });
            }
            if (!match) {
                newly_pruned_partitions.insert(ids.begin(), ids.end());
            }
        }
        _processed_filter_ids.emplace(filter_id);
    }

    publish_pruned_partitions(*current, newly_pruned_partitions);
    if (_processed_filter_ids.size() == runtime_bloom_filters.descriptors().size()) {
        // Publish completion only after the covered verdicts are visible.
        _bloom_pruning_complete.store(true, std::memory_order_release);
    }
}
} // namespace starrocks
