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

#include "exec/hdfs_scanner/hdfs_scanner_context.h"

#include <algorithm>
#include <map>
#include <utility>

#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "column/runtime_type_traits.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "storage/primitive/zone_map_detail.h"
#include "storage/runtime_range_pruner.hpp"
#include "storage/type_info_allocator_adapter.h"
#include "storage/types.h"
#include "types/timestamp_value.h"

namespace starrocks {

static const std::string kCountOptColumnName = "___count___";

static Status fill_default_value_for_not_existed_slot(SlotDescriptor* slot_desc, const std::string& default_value,
                                                      size_t row_count, Column* column) {
    auto type_info = get_type_info(slot_desc->type());
    if (type_info == nullptr) {
        return Status::InternalError(fmt::format("failed to get type info for slot {}", slot_desc->col_name()));
    }
    if (default_value.empty() && !slot_desc->type().is_string_type()) {
        return Status::InvalidArgument(fmt::format(
                "empty default value is only supported for string-like columns, col_name={}", slot_desc->col_name()));
    }

    // Parse default value into Datum using TypeInfo::from_string
    // This handles all basic types: INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING, DATE, TIMESTAMP
    MemPool mem_pool;
    TypeInfoAllocator type_info_allocator = make_type_info_allocator(&mem_pool);
    Datum datum;
    RETURN_IF_ERROR(datum_from_string(type_info.get(), &datum, default_value, &type_info_allocator));

    // Fill column with the default value
    for (size_t i = 0; i < row_count; ++i) {
        column->append_datum(datum);
    }

    return Status::OK();
}

bool HdfsScannerContext::is_lazy_materialization_slot(SlotId slot_id) const {
    // if there are no conjuncts at all, every slot must be read eagerly.
    if (conjuncts.by_slot.empty() && conjuncts.scanner_ctxs.empty()) {
        return false;
    }
    // slots directly filtered by a by_slot predicate are active (eagerly decoded).
    if (conjuncts.by_slot.count(slot_id)) {
        return false;
    }
    // slots referenced by any conjunct must be eagerly decoded too.
    if (conjuncts.slots_in_conjunct.count(slot_id)) {
        return false;
    }
    return true;
}

bool HdfsScannerContext::can_use_count_optimization() const {
    return options.use_count_opt && can_use_file_record_count;
}

bool HdfsScannerContext::can_use_min_max_optimization() const {
    // @TODO for iceberg _row_id column, we can support min/max optimization in the future
    return options.use_min_max_opt && materialized_columns.empty() && reserved_field_slots.empty();
}

void HdfsScannerContext::update_with_none_existed_slot(SlotDescriptor* slot) {
    not_existed_slots.push_back(slot);
    SlotId slot_id = slot->id();
    if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
        for (ExprContext* expr_ctx : conjunct_ctxs_by_slot[slot_id]) {
            conjunct_ctxs_of_non_existed_slots.emplace_back(expr_ctx);
        }
        conjunct_ctxs_by_slot.erase(slot_id);
    }
}

void HdfsScannerContext::update_return_count_columns() {
    // special handling for ___count__ optimization.
    // this is different from `can_use_count_optimization` ,  which uses iceberg metadata to return count value
    // this optimizaton is to fill with `count` rows of default value from parquet/orc header
    std::vector<FormatColumnInfo> updated_columns;
    for (auto& column : materialized_columns) {
        if (column.name() == kCountOptColumnName) {
            update_with_none_existed_slot(column.slot_desc);
        } else {
            updated_columns.emplace_back(column);
        }
    }
    materialized_columns.swap(updated_columns);
}

void HdfsScannerContext::update_min_max_columns() {
    if (!options.use_min_max_opt) {
        return;
    }
    std::vector<FormatColumnInfo> updated_columns;
    const std::map<int32_t, TExprMinMaxValue>& min_max_values = scan_range->min_max_values;
    for (auto& column : materialized_columns) {
        if (min_max_values.find(column.slot_id()) != min_max_values.end()) {
            // This column has file-level min/max statistics.  Move it to
            // not_existed_slots so that append_or_update_min_max_column_to_chunk()
            // fills the column with the statistics values instead of reading the
            // actual data from the file.
            update_with_none_existed_slot(column.slot_desc);
        } else if (options.can_use_any_column) {
            // This column has no min/max statistics (e.g. STRING or TIMESTAMP type
            // which are not yet supported, or a placeholder column injected by
            // PruneHDFSScanColumnRule when every queried column is a partition column).
            // Because can_use_any_column is set we know its value is irrelevant to the
            // query result, so fill it with a default value and skip reading the file.
            update_with_none_existed_slot(column.slot_desc);
        } else {
            // This column genuinely needs to be read from the data file.
            updated_columns.emplace_back(column);
        }
    }
    // When can_use_any_column is set, also drain reserved_field_slots (e.g. _pos, _row_id)
    // into not_existed_slots.  reserved_field_slots are meta/hidden columns whose
    // values are irrelevant to the min/max query result, so filling them with defaults
    // is safe and allows can_use_min_max_optimization() to return true.
    if (options.can_use_any_column) {
        for (SlotDescriptor* slot_desc : reserved_field_slots) {
            update_with_none_existed_slot(slot_desc);
        }
        reserved_field_slots.clear();
    }
    materialized_columns.swap(updated_columns);
}

Status HdfsScannerContext::update_materialized_columns(const std::unordered_set<std::string>& names) {
    std::vector<FormatColumnInfo> updated_columns;
    for (auto& column : materialized_columns) {
        auto col_name = column.formatted_name(options.case_sensitive);
        if (names.find(col_name) == names.end()) {
            update_with_none_existed_slot(column.slot_desc);
        } else {
            updated_columns.emplace_back(column);
        }
    }
    materialized_columns.swap(updated_columns);
    return Status::OK();
}

Status HdfsScannerContext::append_or_update_not_existed_columns_to_chunk(ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.empty()) return Status::OK();

    ChunkPtr& ck = (*chunk);

    if (options.use_min_max_opt) {
        append_or_update_min_max_column_to_chunk(chunk, row_count);
    }

    for (auto* slot_desc : not_existed_slots) {
        if (options.use_min_max_opt &&
            scan_range->min_max_values.find(slot_desc->id()) != scan_range->min_max_values.end()) {
            // handled in min max column
            continue;
        }

        auto col = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        if (row_count > 0) {
            if (slot_desc->col_name() == kCountOptColumnName) {
                TypeDescriptor desc;
                desc.type = TYPE_BIGINT;
                col = ColumnHelper::create_column(desc, slot_desc->is_nullable());
                col->append_datum(int64_t(1));
                col->assign(row_count, 0);
            } else if (auto it = materialize_slot_default_values.find(slot_desc->id());
                       it != materialize_slot_default_values.end()) {
                RETURN_IF_ERROR(fill_default_value_for_not_existed_slot(slot_desc, it->second, row_count, col.get()));
            } else {
                col->append_default(row_count);
            }
        }
        ck->append_or_update_column(std::move(col), slot_desc->id());
    }
    ck->set_num_rows(row_count);
    return Status::OK();
}

void HdfsScannerContext::append_or_update_count_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.empty() || row_count < 0) return;
    ChunkPtr& ck = (*chunk);
    auto* slot_desc = not_existed_slots[0];
    TypeDescriptor desc;
    desc.type = TYPE_BIGINT;
    auto col = ColumnHelper::create_column(desc, slot_desc->is_nullable());
    col->append_datum(int64_t(row_count));
    ck->append_or_update_column(std::move(col), slot_desc->id());
    ck->set_num_rows(1);
}

void HdfsScannerContext::append_or_update_min_max_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    for (SlotDescriptor* slot_desc : not_existed_slots) {
        auto it = scan_range->min_max_values.find(slot_desc->id());
        if (it == scan_range->min_max_values.end()) {
            continue;
        }
        const TExprMinMaxValue& min_max_value = it->second;
        MutableColumnPtr col = create_min_max_value_column(slot_desc, min_max_value, row_count);
        (*chunk)->append_or_update_column(std::move(col), slot_desc->id());
    }
}

MutableColumnPtr HdfsScannerContext::create_min_max_value_column(SlotDescriptor* slot_desc,
                                                                 const TExprMinMaxValue& value, size_t row_count) {
    auto col = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
    std::vector<Datum> data;
    if (value.has_null) {
        data.emplace_back(kNullDatum);
    }
    if (value.type != TExprNodeType::NULL_LITERAL) {
        switch (slot_desc->type().type) {
#define HANDLE_INT_TYPE(T)                                         \
    case T: {                                                      \
        data.emplace_back((RunTimeCppType<T>)value.min_int_value); \
        data.emplace_back((RunTimeCppType<T>)value.max_int_value); \
        break;                                                     \
    }
#define HANDLE_FLOAT_TYPE(T)                                         \
    case T: {                                                        \
        data.emplace_back((RunTimeCppType<T>)value.min_float_value); \
        data.emplace_back((RunTimeCppType<T>)value.max_float_value); \
        break;                                                       \
    }
            HANDLE_INT_TYPE(TYPE_BOOLEAN);
            HANDLE_INT_TYPE(TYPE_TINYINT);
            HANDLE_INT_TYPE(TYPE_SMALLINT);
            HANDLE_INT_TYPE(TYPE_INT);
            HANDLE_INT_TYPE(TYPE_BIGINT);
            HANDLE_FLOAT_TYPE(TYPE_FLOAT);
            HANDLE_FLOAT_TYPE(TYPE_DOUBLE);
#undef HANDLE_INT_TYPE
#undef HANDLE_FLOAT_TYPE
            // https://iceberg.apache.org/spec/#binary-single-value-serialization
        case TYPE_DATE:
            data.emplace_back(DateValue::from_days_since_unix_epoch(value.min_int_value));
            data.emplace_back(DateValue::from_days_since_unix_epoch(value.max_int_value));
            break;
        case TYPE_TIME:
            data.emplace_back((double)value.min_int_value * 1e-6);
            data.emplace_back((double)value.max_int_value * 1e-6);
            break;
        case TYPE_DATETIME: {
            auto to_ts = [](int64_t micros) {
                constexpr int64_t kMicrosPerSecond = 1000000L;
                TimestampValue ts;
                int64_t seconds = micros / kMicrosPerSecond;
                int64_t microseconds = micros % kMicrosPerSecond;
                if (microseconds < 0) {
                    microseconds += kMicrosPerSecond;
                    --seconds;
                }
                ts.from_unix_second(seconds, microseconds);
                return ts;
            };
            data.emplace_back(to_ts(value.min_int_value));
            data.emplace_back(to_ts(value.max_int_value));
            break;
        }
        default:
            break;
        }
    }

    // if this is the first split, we use null/min/max order
    // otherwise, we reverse it. In that way, we can make sure
    // null/min/max values all output from this file.
    if (!is_first_split) {
        std::reverse(data.begin(), data.end());
    }
    for (int i = 0; i < data.size() && row_count > 0; i++) {
        row_count -= 1;
        if (data[i].is_null()) {
            col->append_nulls(1);
        } else {
            col->append_datum(data[i]);
        }
    }
    if (row_count > 0) {
        if (!value.all_null) {
            // the rest values does not matter, so we just copy the first value.
            // it's noted that we can not use `append_default` here, we can only put null(maybe)/min/max
            auto col_tail = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            // if not all null values, then data[1] is the non-null value for sure.
            col_tail->append_datum(data[1]);
            col_tail->assign(row_count, 0);
            col->append(*col_tail);
        } else {
            col->append_nulls(row_count);
        }
    }
    return col;
}

bool HdfsScannerContext::decode_min_max_endpoint(const TypeDescriptor& type, const TExprMinMaxValue& value,
                                                 Datum* min_out, Datum* max_out) {
    if (value.type == TExprNodeType::NULL_LITERAL) {
        return false;
    }
    switch (type.type) {
#define DECODE_INT_ENDPOINT(T)                                    \
    case T:                                                       \
        *min_out = Datum((RunTimeCppType<T>)value.min_int_value); \
        *max_out = Datum((RunTimeCppType<T>)value.max_int_value); \
        return true;
        DECODE_INT_ENDPOINT(TYPE_BOOLEAN)
        DECODE_INT_ENDPOINT(TYPE_TINYINT)
        DECODE_INT_ENDPOINT(TYPE_SMALLINT)
        DECODE_INT_ENDPOINT(TYPE_INT)
        DECODE_INT_ENDPOINT(TYPE_BIGINT)
#undef DECODE_INT_ENDPOINT
    case TYPE_DATE:
        *min_out = Datum(DateValue::from_days_since_unix_epoch(value.min_int_value));
        *max_out = Datum(DateValue::from_days_since_unix_epoch(value.max_int_value));
        return true;
    case TYPE_DATETIME: {
        // Same micros->TimestampValue decode as create_min_max_value_column.
        auto to_ts = [](int64_t micros) {
            constexpr int64_t kMicrosPerSecond = 1000000L;
            TimestampValue ts;
            int64_t seconds = micros / kMicrosPerSecond;
            int64_t microseconds = micros % kMicrosPerSecond;
            if (microseconds < 0) {
                microseconds += kMicrosPerSecond;
                --seconds;
            }
            ts.from_unix_second(seconds, microseconds);
            return ts;
        };
        *min_out = Datum(to_ts(value.min_int_value));
        *max_out = Datum(to_ts(value.max_int_value));
        return true;
    }
    default:
        // float/double (NaN, not eligible), time, decimal/string (absent from min_max_values).
        return false;
    }
}

StatusOr<bool> HdfsScannerContext::should_skip_scan_range_by_topn_min_max() {
    const int32_t slot_id = options.topn_reorder_slot_id;
    if (slot_id < 0 || predicates.runtime_filter_scan_range_pruner == nullptr || scan_range == nullptr) {
        return false;
    }
    if (!scan_range->__isset.min_max_values) {
        return false;
    }
    auto it = scan_range->min_max_values.find(slot_id);
    if (it == scan_range->min_max_values.end()) {
        return false;
    }
    const TExprMinMaxValue& value = it->second;

    const SlotDescriptor* reorder_slot = nullptr;
    if (tuple_desc != nullptr) {
        for (SlotDescriptor* s : tuple_desc->slots()) {
            if (s->id() == slot_id) {
                reorder_slot = s;
                break;
            }
        }
    }
    if (reorder_slot == nullptr) {
        return false;
    }

    // NULL handling. An all-null file has no numeric bound, so never skip it. With NULLS FIRST a
    // file that has any null may hold top-k rows, so never skip it either.
    if (value.all_null) {
        return false;
    }
    if (value.has_null && options.topn_reorder_nulls_first) {
        return false;
    }

    Datum file_min;
    Datum file_max;
    if (!decode_min_max_endpoint(reorder_slot->type(), value, &file_min, &file_max)) {
        return false; // type without a comparable bound -> no skip
    }
    ZoneMapDetail detail(file_min, file_max, value.has_null);

    bool skip = false;

    // (a) An RF that already arrived when the scanner context was built is folded into the predicate
    //     tree as a min/max range on the sort column, not into the pruner. Use only the root-level
    //     (immediate) predicates: they form a true AND, so testing the file's zone map against each of
    //     them is safe. The full map also flattens leaves out of OR nodes (a < 5 OR a > 1000 becomes
    //     [a < 5, a > 1000]); AND-testing those would drop a file that matches through one arm, which
    //     is a wrong result. OR predicates are left to the footer skip, which keeps their structure.
    //     For ConnectorPredicateParser the predicate cid equals the slot id.
    {
        const auto& cid_to_preds = predicates.predicate_tree.get_immediate_column_predicate_map();
        if (auto it_preds = cid_to_preds.find(slot_id); it_preds != cid_to_preds.end()) {
            for (const auto* pred : it_preds->second) {
                if (!pred->zone_map_filter(detail)) {
                    skip = true;
                    break;
                }
            }
        }
    }

    // (b) An RF that arrives later sits in the pruner. Probe a throwaway copy, not the scanner's own
    //     pruner: update_range_if_arrived changes its arrived/version state, and the Parquet footer
    //     path copies that pruner for its own row-group skip. Mutating the original here would mark
    //     this RF version as consumed and suppress that skip.
    if (!skip) {
        RuntimeScanRangePruner probe = *predicates.runtime_filter_scan_range_pruner;
        Status st = probe.update_range_if_arrived(
                global_dictmaps,
                [&](int cid, const std::vector<const ColumnPredicate*>& preds) -> Status {
                    if (cid != slot_id) {
                        return Status::OK();
                    }
                    // AND zone-map test, same as PredicateFilterEvaluatorUtils::zonemap_satisfy but
                    // without the parquet GroupReader visitor.
                    for (const auto* pred : preds) {
                        if (!pred->zone_map_filter(detail)) {
                            skip = true;
                            break;
                        }
                    }
                    return Status::OK();
                },
                /*force=*/true, /*raw_read_rows=*/0);
        if (st.is_end_of_file()) {
            return true; // RF is always-false -> drop the whole file
        }
        RETURN_IF_ERROR(st);
    }
    return skip;
}

Status HdfsScannerContext::evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter) {
    size_t chunk_size = (*chunk)->num_rows();
    if (conjunct_ctxs_by_slot.size()) {
        filter->assign(chunk_size, 1);
        for (auto& it : conjunct_ctxs_by_slot) {
            ASSIGN_OR_RETURN(chunk_size,
                             ChunkPredicateEvaluator::eval_conjuncts_into_filter(it.second, chunk->get(), filter));
            if (chunk_size == 0) {
                (*chunk)->set_num_rows(0);
                return Status::OK();
            }
        }
        if (chunk_size != 0 && chunk_size != (*chunk)->num_rows()) {
            (*chunk)->filter(*filter);
        }
    }
    return Status::OK();
}

StatusOr<bool> HdfsScannerContext::should_skip_by_evaluating_not_existed_slots() {
    if (not_existed_slots.size() == 0) return false;

    // build chunk for evaluation.
    ChunkPtr chunk = std::make_shared<Chunk>();
    RETURN_IF_ERROR(append_or_update_not_existed_columns_to_chunk(&chunk, 1));
    // do evaluation.
    {
        SCOPED_RAW_TIMER(&stats->expr_filter_ns);
        RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(conjunct_ctxs_of_non_existed_slots, chunk.get()));
    }
    return !(chunk->has_rows());
}

void HdfsScannerContext::append_or_update_partition_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    append_or_update_column_to_chunk(chunk, row_count, partition_columns, partition_values);
}

void HdfsScannerContext::append_or_update_extended_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    append_or_update_column_to_chunk(chunk, row_count, extended_columns, extended_values);
}

void HdfsScannerContext::append_or_update_column_to_chunk(ChunkPtr* chunk, size_t row_count,
                                                          const std::vector<FormatColumnInfo>& columns,
                                                          const Columns& values) {
    if (columns.size() == 0) return;

    ChunkPtr& ck = (*chunk);
    for (size_t i = 0; i < columns.size(); i++) {
        SlotDescriptor* slot_desc = columns[i].slot_desc;
        DCHECK(values[i]->is_constant());
        auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(values[i]);
        ColumnPtr data_column = const_column->data_column();
        auto chunk_column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());

        if (row_count > 0) {
            if (data_column->is_nullable()) {
                chunk_column->append_nulls(1);
            } else {
                chunk_column->append(*data_column, 0, 1);
            }
            chunk_column->assign(row_count, 0);
        }
        ck->append_or_update_column(std::move(chunk_column), slot_desc->id());
    }
    ck->set_num_rows(row_count);
}

bool HdfsScannerContext::can_use_dict_filter_on_slot(SlotDescriptor* slot) const {
    if (!slot->type().is_string_type()) {
        return false;
    }
    SlotId slot_id = slot->id();
    if (conjunct_ctxs_by_slot.find(slot_id) == conjunct_ctxs_by_slot.end()) {
        return false;
    }
    for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
        const Expr* root_expr = ctx->root();
        if (root_expr->node_type() == TExprNodeType::FUNCTION_CALL) {
            std::string is_null_str;
            if (root_expr->is_null_scalar_function(is_null_str)) {
                return false;
            }
        }
    }
    return true;
}

void HdfsScannerContext::merge_split_tasks() {
    DCHECK(predicates.conjuncts_manager != nullptr);
    auto& split_tasks = this->split.split_tasks;
    if (split_tasks.size() < 2) return;

    // NOTE: the prerequisites of `split_tasks` are
    // 1. all ranges in it are sorted
    // 2. and none of them is overlapped.
    std::vector<HdfsSplitContextPtr> new_split_tasks;

    auto do_merge = [&](size_t start, size_t end) {
        auto start_ctx = split_tasks[start].get();
        auto end_ctx = split_tasks[end].get();
        auto new_ctx = start_ctx->clone();
        new_ctx->split_start = start_ctx->split_start;
        new_ctx->split_end = end_ctx->split_end;
        new_split_tasks.emplace_back(std::move(new_ctx));
    };

    size_t head = 0;
    for (size_t i = 1; i < split_tasks.size(); i++) {
        bool cut = false;

        auto prev_ctx = split_tasks[i - 1].get();
        auto ctx = split_tasks[i].get();
        auto head_ctx = split_tasks[head].get();

        if ((ctx->split_start != prev_ctx->split_end) ||
            (ctx->split_end - head_ctx->split_start > options.connector_max_split_size)) {
            cut = true;
        }

        if (cut) {
            do_merge(head, i - 1);
            head = i;
        }
    }
    do_merge(head, split_tasks.size() - 1);

    // handle the tail stripe, if it's small and consecutive, merge it to the last one.
    size_t new_size = new_split_tasks.size();
    if (new_size >= 2) {
        auto tail_ctx = new_split_tasks[new_size - 1].get();
        size_t tail_size = (tail_ctx->split_end - tail_ctx->split_start);
        if ((tail_size * 2) < options.connector_max_split_size) {
            auto last_ctx = new_split_tasks[new_size - 2].get();
            if (last_ctx->split_end == tail_ctx->split_start) {
                last_ctx->split_end = tail_ctx->split_end;
                new_split_tasks.pop_back();
            }
        }
    }

    split_tasks.swap(new_split_tasks);
}

} // namespace starrocks
