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

#include "exec/olap_scan_prepare.h"

#include "column/column_helper.h"
#include "column/runtime_type_traits.h"
#include "common/object_pool.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/date_value.h"
#include "types/logical_type.h"

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
                    // all hit, skip
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

} // namespace starrocks
