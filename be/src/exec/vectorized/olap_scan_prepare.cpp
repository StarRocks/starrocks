
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/olap_scan_prepare.h"

#include <variant>

#include "column/type_traits.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/dictmapping_expr.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "gutil/map_util.h"
#include "runtime/date_value.hpp"
#include "storage/vectorized/column_predicate.h"
#include "storage/vectorized/predicate_parser.h"

namespace starrocks {
namespace vectorized {

static bool ignore_cast(const SlotDescriptor& slot, const Expr& expr) {
    if (slot.type().is_date_type() && expr.type().is_date_type()) {
        return true;
    }
    return slot.type().is_string_type() && expr.type().is_string_type();
}

static Expr* get_root_expr(ExprContext* ctx) {
    if (dynamic_cast<DictMappingExpr*>(ctx->root())) {
        return ctx->root()->get_child(1);
    }
    return ctx->root();
}

template <typename ValueType>
static bool check_decimal_overflow(int precision, const ValueType& value) {
    if constexpr (is_decimal<ValueType>) {
        return -get_scale_factor<ValueType>(precision) < value && value < get_scale_factor<ValueType>(precision);
    } else {
        return false;
    }
}

template <typename ValueType>
static bool get_predicate_value(ObjectPool* obj_pool, const SlotDescriptor& slot, const Expr* expr, ExprContext* ctx,
                                ValueType* value, SQLFilterOp* op, Status* status) {
    if (expr->get_num_children() != 2) {
        return false;
    }

    Expr* l = expr->get_child(0);
    Expr* r = expr->get_child(1);

    // 1. ensure |l| points to a slot ref and |r| points to a const expression.
    bool reverse_op = false;
    if (!r->is_constant()) {
        reverse_op = true;
        std::swap(l, r);
    }

    // TODO(zhuming): DATE column may be casted to double.
    if (l->type().type != slot.type().type && !ignore_cast(slot, *l)) {
        return false;
    }

    // when query on a `DATE` column with predicate, both the `DATE`
    // column and the operand of predicate will be casted to timestamp.
    if constexpr (std::is_same_v<ValueType, DateValue>) {
        if (l->op() == TExprOpcode::CAST) {
            l = l->get_child(0);
        }
    }

    if (!l->is_slotref() || !r->is_constant()) {
        return false;
    }

    std::vector<SlotId> slot_ids;
    if (l->get_slot_ids(&slot_ids) != 1 || slot_ids[0] != slot.id()) {
        return false;
    }

    // 3. extract the const value from |r|.
    ColumnPtr column_ptr = ctx->evaluate(r, nullptr);
    if (column_ptr == nullptr) {
        return false;
    }

    DCHECK_EQ(1u, column_ptr->size());
    if (column_ptr->only_null() || column_ptr->is_null(0)) {
        return false;
    }

    // check column type, as not all exprs return a const column.
    ColumnPtr data = column_ptr;
    if (column_ptr->is_nullable()) {
        data = down_cast<NullableColumn*>(column_ptr.get())->data_column();
    } else if (column_ptr->is_constant()) {
        data = down_cast<ConstColumn*>(column_ptr.get())->data_column();
    } else { // defensive check.
        DCHECK(false) << "unreachable path: unknown column type of expr evaluate result";
        return false;
    }

    if (expr->op() == TExprOpcode::EQ || expr->op() == TExprOpcode::NE) {
        *op = to_olap_filter_type(expr->op(), false);
    } else {
        *op = to_olap_filter_type(expr->op(), reverse_op);
    }

    if constexpr (std::is_same_v<ValueType, DateValue>) {
        if (data->is_timestamp()) {
            TimestampValue ts = down_cast<TimestampColumn*>(data.get())->get(0).get_timestamp();
            *value = implicit_cast<DateValue>(ts);
            if (implicit_cast<TimestampValue>(*value) != ts) {
                // |ts| has nonzero time, rewrite predicate.
                switch (*op) {
                case FILTER_LARGER_OR_EQUAL:
                    // rewrite (c >= '2020-01-01 01:00:00') to (c > '2020-01-01').
                    *op = FILTER_LARGER;
                    break;
                case FILTER_LESS:
                    // rewrite (c < '2020-01-01 01:00:00') to (c <= '2020-01-01').
                    *op = FILTER_LESS_OR_EQUAL;
                    break;
                case FILTER_LARGER:
                    [[fallthrough]];
                case FILTER_LESS_OR_EQUAL:
                    // Just ignore the time value.
                    break;
                case FILTER_IN:
                    *status = Status::EndOfFile("predicate for date always false");
                    return false;
                case FILTER_NOT_IN:
                    // TODO(zhuming): Should be rewrote to `NOT NULL`.
                    return false;
                }
            }
        } else {
            DCHECK(data->is_date());
            *value = down_cast<DateColumn*>(data.get())->get(0).get_date();
        }
    } else if constexpr (std::is_same_v<ValueType, Slice>) {
        // |column_ptr| will be released after this method return, have to ensure that
        // the corresponding external storage will not be deallocated while the slice
        // still been used.
        const Slice* slice = reinterpret_cast<const Slice*>(data->raw_data());
        std::string* str = obj_pool->add(new std::string(slice->data, slice->size));
        *value = *str;
    } else {
        *value = *reinterpret_cast<const ValueType*>(data->raw_data());
        if (r->type().is_decimalv3_type()) {
            return check_decimal_overflow<ValueType>(r->type().precision, *value);
        }
    }
    return true;
}

template <PrimitiveType SlotType, typename RangeValueType>
void OlapScanConjunctsManager::normalize_in_or_equal_predicate(const SlotDescriptor& slot,
                                                               ColumnValueRange<RangeValueType>* range) {
    Status status;
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);
    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) {
            continue;
        }

        const Expr* root_expr = get_root_expr(conjunct_ctxs[i]);

        // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
        if (TExprOpcode::FILTER_IN == root_expr->op()) {
            const Expr* l = root_expr->get_child(0);

            if ((!l->is_slotref()) || (l->type().type != slot.type().type && !ignore_cast(slot, *l))) {
                continue;
            }
            std::vector<SlotId> slot_ids;
            if (1 == l->get_slot_ids(&slot_ids) && slot_ids[0] == slot.id()) {
                const auto* pred = down_cast<const VectorizedInConstPredicate<SlotType>*>(root_expr);
                // join in runtime filter  will handle by `_normalize_join_runtime_filter`
                if (pred->is_join_runtime_filter()) {
                    continue;
                }

                if (pred->is_not_in() || pred->null_in_set() ||
                    pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                    continue;
                }

                std::set<RangeValueType> values;
                for (const auto& value : pred->hash_set()) {
                    values.insert(value);
                }
                if (range->add_fixed_values(FILTER_IN, values).ok()) {
                    normalized_conjuncts[i] = true;
                }
            }
        }

        // 2. Normalize eq conjuncts like 'where col = value'
        if (TExprNodeType::BINARY_PRED == root_expr->node_type() &&
            FILTER_IN == to_olap_filter_type(root_expr->op(), false)) {
            using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;
            SQLFilterOp op;
            ValueType value;
            bool ok = get_predicate_value(obj_pool, slot, root_expr, conjunct_ctxs[i], &value, &op, &status);
            if (ok && range->add_fixed_values(FILTER_IN, std::set<RangeValueType>{value}).ok()) {
                normalized_conjuncts[i] = true;
            }
        }
    }
    return;
}

// explicit specialization for DATE.
template <>
void OlapScanConjunctsManager::normalize_in_or_equal_predicate<starrocks::TYPE_DATE, DateValue>(
        const SlotDescriptor& slot, ColumnValueRange<DateValue>* range) {
    Status status;
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);

    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) {
            continue;
        }

        const Expr* root_expr = get_root_expr(conjunct_ctxs[i]);

        // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
        if (TExprOpcode::FILTER_IN == root_expr->op()) {
            const Expr* l = root_expr->get_child(0);
            // TODO(zhuming): DATE column may be casted to double.
            if (l->type().type != starrocks::TYPE_DATE && l->type().type != starrocks::TYPE_DATETIME) {
                continue;
            }

            PrimitiveType pred_type = l->type().type;
            // ignore the cast on DATE.
            if (l->op() == TExprOpcode::CAST) {
                l = l->get_child(0);
            }
            if (!l->is_slotref()) {
                continue;
            }
            std::vector<SlotId> slot_ids;
            if (1 == l->get_slot_ids(&slot_ids) && slot_ids[0] == slot.id()) {
                std::set<DateValue> values;

                if (pred_type == starrocks::TYPE_DATETIME) {
                    const auto* pred =
                            down_cast<const VectorizedInConstPredicate<starrocks::TYPE_DATETIME>*>(root_expr);
                    // join in runtime filter  will handle by `_normalize_join_runtime_filter`
                    if (pred->is_join_runtime_filter()) {
                        continue;
                    }

                    if (pred->is_not_in() || pred->null_in_set() ||
                        pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                        continue;
                    }

                    for (const TimestampValue& ts : pred->hash_set()) {
                        DateValue date = implicit_cast<DateValue>(ts);
                        if (implicit_cast<TimestampValue>(date) == ts) {
                            values.insert(date);
                        }
                    }
                } else if (pred_type == starrocks::TYPE_DATE) {
                    const auto* pred = down_cast<const VectorizedInConstPredicate<starrocks::TYPE_DATE>*>(root_expr);
                    if (pred->is_not_in() || pred->null_in_set() ||
                        pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                        continue;
                    }
                    for (const DateValue& date : pred->hash_set()) {
                        values.insert(date);
                    }
                }
                if (values.empty()) {
                    status = Status::EndOfFile("const false predicate result");
                    continue;
                }
                if (range->add_fixed_values(FILTER_IN, values).ok()) {
                    normalized_conjuncts[i] = true;
                }
            }
        }

        // 2. Normalize eq conjuncts like 'where col = value'
        if (TExprNodeType::BINARY_PRED == root_expr->node_type() &&
            FILTER_IN == to_olap_filter_type(root_expr->op(), false)) {
            SQLFilterOp op;
            DateValue value{0};
            bool ok = get_predicate_value(obj_pool, slot, root_expr, conjunct_ctxs[i], &value, &op, &status);
            if (ok && range->add_fixed_values(FILTER_IN, std::set<DateValue>{value}).ok()) {
                normalized_conjuncts[i] = true;
            }
        }
    }
}

template <PrimitiveType SlotType, typename RangeValueType>
void OlapScanConjunctsManager::normalize_binary_predicate(const SlotDescriptor& slot,
                                                          ColumnValueRange<RangeValueType>* range) {
    Status status;
    DCHECK((SlotType == slot.type().type) || (SlotType == TYPE_VARCHAR && slot.type().type == TYPE_CHAR));
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);

    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) {
            continue;
        }

        Expr* root_expr = get_root_expr(conjunct_ctxs[i]);
        if (TExprNodeType::BINARY_PRED != root_expr->node_type()) {
            continue;
        }

        using ValueType = typename vectorized::RunTimeTypeTraits<SlotType>::CppType;

        SQLFilterOp op;
        ValueType value;
        bool ok = get_predicate_value(obj_pool, slot, root_expr, conjunct_ctxs[i], &value, &op, &status);
        if (ok && range->add_range(op, static_cast<RangeValueType>(value)).ok()) {
            normalized_conjuncts[i] = true;
        }
    }
    return;
}

template <PrimitiveType SlotType, typename RangeValueType>
void OlapScanConjunctsManager::normalize_join_runtime_filter(const SlotDescriptor& slot,
                                                             ColumnValueRange<RangeValueType>* range) {
    // in runtime filter
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);

    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) {
            continue;
        }

        const Expr* root_expr = get_root_expr(conjunct_ctxs[i]);
        if (TExprOpcode::FILTER_IN == root_expr->op()) {
            const Expr* l = root_expr->get_child(0);
            if (!l->is_slotref() || (l->type().type != slot.type().type && !ignore_cast(slot, *l))) {
                continue;
            }
            std::vector<SlotId> slot_ids;
            if (1 == l->get_slot_ids(&slot_ids) && slot_ids[0] == slot.id()) {
                const auto* pred = down_cast<const VectorizedInConstPredicate<SlotType>*>(root_expr);

                if (!pred->is_join_runtime_filter()) {
                    continue;
                }

                // Ensure we don't compute this conjuncts again in olap scanner
                normalized_conjuncts[i] = true;

                if (pred->is_not_in() || pred->null_in_set() ||
                    pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                    continue;
                }

                std::set<RangeValueType> values;
                for (const auto& value : pred->hash_set()) {
                    values.insert(value);
                }
                range->add_fixed_values(FILTER_IN, values);
            }
        }
    }

    // bloom runtime filter
    for (const auto it : runtime_filters->descriptors()) {
        const RuntimeFilterProbeDescriptor* desc = it.second;
        const JoinRuntimeFilter* rf = desc->runtime_filter();
        using ValueType = typename vectorized::RunTimeTypeTraits<SlotType>::CppType;
        SlotId slot_id;

        // runtime filter existed and does not have null.
        if (rf == nullptr || rf->has_null()) continue;
        // probe expr is slot ref and slot id matches.
        if (!desc->is_probe_slot_ref(&slot_id) || slot_id != slot.id()) continue;

        const RuntimeBloomFilter<SlotType>* filter = down_cast<const RuntimeBloomFilter<SlotType>*>(rf);
        // For some cases such as in bucket shuffle, some hash join node may not have any input chunk from right table.
        // Runtime filter does not have any min/max values in this case.
        if (!filter->has_min_max()) continue;
        // If this column doesn't have other filter, we use join runtime filter
        // to fast comput row range in storage engine
        if (range->is_init_state()) {
            range->set_index_filter_only(true);
        }

        SQLFilterOp min_op = to_olap_filter_type(TExprOpcode::GE, false);
        ValueType min_value = filter->min_value();
        range->add_range(min_op, static_cast<RangeValueType>(min_value));

        SQLFilterOp max_op = to_olap_filter_type(TExprOpcode::LE, false);
        ValueType max_value = filter->max_value();
        range->add_range(max_op, static_cast<RangeValueType>(max_value));
    }
}

template <PrimitiveType SlotType, typename RangeValueType>
void OlapScanConjunctsManager::normalize_not_in_or_not_equal_predicate(const SlotDescriptor& slot,
                                                                       ColumnValueRange<RangeValueType>* range) {
    Status status;
    DCHECK((SlotType == slot.type().type) || (SlotType == TYPE_VARCHAR && slot.type().type == TYPE_CHAR));
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);

    using ValueType = typename vectorized::RunTimeTypeTraits<SlotType>::CppType;
    // handle not equal.
    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) {
            continue;
        }
        Expr* root_expr = get_root_expr(conjunct_ctxs[i]);
        // handle not equal
        if (root_expr->node_type() == TExprNodeType::BINARY_PRED && root_expr->op() == TExprOpcode::NE) {
            SQLFilterOp op;
            ValueType value;
            bool ok = get_predicate_value(obj_pool, slot, root_expr, conjunct_ctxs[i], &value, &op, &status);
            if (ok && range->add_fixed_values(FILTER_NOT_IN, std::set<RangeValueType>{value}).ok()) {
                normalized_conjuncts[i] = true;
            }
        }

        // handle not in
        if (root_expr->node_type() == TExprNodeType::IN_PRED && root_expr->op() == TExprOpcode::FILTER_NOT_IN) {
            const Expr* l = root_expr->get_child(0);
            if (!l->is_slotref() || (l->type().type != slot.type().type && !ignore_cast(slot, *l))) {
                continue;
            }
            std::vector<SlotId> slot_ids;

            if (1 == l->get_slot_ids(&slot_ids) && slot_ids[0] == slot.id()) {
                const auto* pred = down_cast<const VectorizedInConstPredicate<SlotType>*>(root_expr);
                // RTF won't generate not in predicate
                DCHECK(!pred->is_join_runtime_filter());

                if (!pred->is_not_in() || pred->null_in_set() ||
                    pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                    continue;
                }

                std::set<RangeValueType> values;
                for (const auto& value : pred->hash_set()) {
                    values.insert(value);
                }
                if (range->add_fixed_values(FILTER_NOT_IN, values).ok()) {
                    normalized_conjuncts[i] = true;
                }
            }
        }
    }
}

void OlapScanConjunctsManager::normalize_is_null_predicate(const SlotDescriptor& slot) {
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);

    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) {
            continue;
        }
        Expr* root_expr = get_root_expr(conjunct_ctxs[i]);
        if (TExprNodeType::FUNCTION_CALL == root_expr->node_type()) {
            std::string is_null_str;
            if (root_expr->is_null_scalar_function(is_null_str)) {
                Expr* e = root_expr->get_child(0);
                if (!e->is_slotref()) {
                    continue;
                }
                std::vector<SlotId> slot_ids;
                if (1 != e->get_slot_ids(&slot_ids) || slot_ids[0] != slot.id()) {
                    continue;
                }
                TCondition is_null;
                is_null.column_name = slot.col_name();
                is_null.condition_op = "is";
                is_null.condition_values.push_back(is_null_str);
                is_null_vector.push_back(is_null);
                normalized_conjuncts[i] = true;
            }
        }
    }
}

template <PrimitiveType SlotType, typename RangeValueType>
void OlapScanConjunctsManager::normalize_predicate(const SlotDescriptor& slot,
                                                   ColumnValueRange<RangeValueType>* range) {
    normalize_in_or_equal_predicate<SlotType, RangeValueType>(slot, range);
    normalize_binary_predicate<SlotType, RangeValueType>(slot, range);
    normalize_not_in_or_not_equal_predicate<SlotType, RangeValueType>(slot, range);
    normalize_is_null_predicate(slot);
    // Must handle join runtime filter last
    normalize_join_runtime_filter<SlotType, RangeValueType>(slot, range);
}

Status OlapScanConjunctsManager::normalize_conjuncts() {
    // Note: _normalized_conjuncts size must be equal to _conjunct_ctxs size,
    // but HashJoinNode will push down predicate to OlapScanNode's _conjunct_ctxs,
    // So _conjunct_ctxs will change after OlapScanNode prepare,
    // So we couldn't resize _normalized_conjuncts when OlapScanNode init or prepare
    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);
    normalized_conjuncts.assign(conjunct_ctxs.size(), false);

    // TODO(zhuming): if any of the normalized column range is empty, we can know that
    // no row will be selected anymore and can return EOF directly.
    for (auto& slot : tuple_desc->decoded_slots()) {
        const std::string& col_name = slot->col_name();
        PrimitiveType type = slot->type().type;
        switch (type) {
        case TYPE_TINYINT: {
            // TYPE_TINYINT use int32_t to present
            // because it's easy to be converted to string when building Olap fetch Query
            using RangeType = ColumnValueRange<int32_t>;
            RangeType full_range(col_name, type, std::numeric_limits<int8_t>::lowest(),
                                 std::numeric_limits<int8_t>::max());
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<int32_t>>(v);
            normalize_predicate<TYPE_TINYINT, int32_t>(*slot, &range);
            break;
        }
        case TYPE_BOOLEAN: {
            // TYPE_BOOLEAN use int32_t to present
            // because it's easy to be converted to string when building Olap fetch Query
            using RangeType = ColumnValueRange<int32_t>;
            RangeType full_range(col_name, type, 0, 1);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<int32_t>>(v);
            normalize_predicate<TYPE_BOOLEAN, int32_t>(*slot, &range);
            break;
        }
        case TYPE_SMALLINT: {
            using RangeType = ColumnValueRange<int16_t>;
            RangeType full_range(col_name, type, std::numeric_limits<int16_t>::lowest(),
                                 std::numeric_limits<int16_t>::max());
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<int16_t>>(v);
            normalize_predicate<TYPE_SMALLINT, int16_t>(*slot, &range);
            break;
        }
        case TYPE_INT: {
            using RangeType = ColumnValueRange<int32_t>;
            RangeType full_range(col_name, type, std::numeric_limits<int32_t>::lowest(),
                                 std::numeric_limits<int32_t>::max());
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<int32_t>>(v);
            normalize_predicate<TYPE_INT, int32_t>(*slot, &range);
            break;
        }
        case TYPE_BIGINT: {
            using RangeType = ColumnValueRange<int64_t>;
            RangeType full_range(col_name, type, std::numeric_limits<int64_t>::lowest(),
                                 std::numeric_limits<int64_t>::max());
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<int64_t>>(v);
            normalize_predicate<TYPE_BIGINT, int64_t>(*slot, &range);
            break;
        }
        case TYPE_LARGEINT: {
            using RangeType = ColumnValueRange<int128_t>;
            RangeType full_range(col_name, type, MIN_INT128, MAX_INT128);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<int128_t>>(v);
            normalize_predicate<TYPE_LARGEINT, int128_t>(*slot, &range);
            break;
        }
        case TYPE_CHAR:
            // for a CHAR column, its `in` predicate will be represented as a
            // `InConstPredicate<PrimitiveType::TYPE_VARCHAR>`, so here we mapping CHAR as VARCHAR.
            [[fallthrough]];
        case TYPE_VARCHAR: {
            using RangeType = ColumnValueRange<Slice>;
            static char min_char = 0x00;
            static char max_char = (char)0xff;
            RangeType full_range(col_name, type, Slice(&min_char, 0), Slice(&max_char, 1));
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<Slice>>(v);
            normalize_predicate<TYPE_VARCHAR, Slice>(*slot, &range);
            break;
        }
        case TYPE_DATE: {
            using RangeType = ColumnValueRange<DateValue>;
            RangeType full_range(col_name, type, DateValue::MIN_DATE_VALUE, DateValue::MAX_DATE_VALUE);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<DateValue>>(v);
            normalize_predicate<TYPE_DATE, DateValue>(*slot, &range);
            break;
        }
        case TYPE_DATETIME: {
            using RangeType = ColumnValueRange<TimestampValue>;
            RangeType full_range(col_name, type, TimestampValue::MIN_TIMESTAMP_VALUE,
                                 TimestampValue::MAX_TIMESTAMP_VALUE);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<TimestampValue>>(v);
            normalize_predicate<TYPE_DATETIME, TimestampValue>(*slot, &range);
            break;
        }
        case TYPE_DECIMALV2: {
            using RangeType = ColumnValueRange<DecimalV2Value>;
            RangeType full_range(col_name, type, DecimalV2Value::get_min_decimal(), DecimalV2Value::get_max_decimal());
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<ColumnValueRange<DecimalV2Value>>(v);
            normalize_predicate<TYPE_DECIMALV2, DecimalV2Value>(*slot, &range);
            break;
        }
        case TYPE_DECIMAL32: {
            using DecimalValueType = int32_t;
            using RangeType = ColumnValueRange<DecimalValueType>;
            RangeType full_range(col_name, type, get_min_decimal<DecimalValueType>(),
                                 get_max_decimal<DecimalValueType>());
            full_range.set_precision(slot->type().precision);
            full_range.set_scale(slot->type().scale);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<RangeType>(v);
            range.set_precision(slot->type().precision);
            range.set_scale(slot->type().scale);
            normalize_predicate<TYPE_DECIMAL32, DecimalValueType>(*slot, &range);
            break;
        }
        case TYPE_DECIMAL64: {
            using DecimalValueType = int64_t;
            using RangeType = ColumnValueRange<DecimalValueType>;
            RangeType full_range(col_name, type, get_min_decimal<DecimalValueType>(),
                                 get_max_decimal<DecimalValueType>());
            full_range.set_precision(slot->type().precision);
            full_range.set_scale(slot->type().scale);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<RangeType>(v);
            range.set_precision(slot->type().precision);
            range.set_scale(slot->type().scale);
            normalize_predicate<TYPE_DECIMAL64, DecimalValueType>(*slot, &range);
            break;
        }
        case TYPE_DECIMAL128: {
            using DecimalValueType = int128_t;
            using RangeType = ColumnValueRange<DecimalValueType>;
            RangeType full_range(col_name, type, get_min_decimal<DecimalValueType>(),
                                 get_max_decimal<DecimalValueType>());
            full_range.set_precision(slot->type().precision);
            full_range.set_scale(slot->type().scale);
            ColumnValueRangeType& v = LookupOrInsert(&column_value_ranges, col_name, full_range);
            RangeType& range = boost::get<RangeType>(v);
            range.set_precision(slot->type().precision);
            range.set_scale(slot->type().scale);
            normalize_predicate<TYPE_DECIMAL128, DecimalValueType>(*slot, &range);
            break;
        }
        case INVALID_TYPE:
        case TYPE_NULL:
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
        case TYPE_BINARY:
        case TYPE_DECIMAL:
        case TYPE_STRUCT:
        case TYPE_ARRAY:
        case TYPE_MAP:
        case TYPE_HLL:
        case TYPE_TIME:
        case TYPE_OBJECT:
        case TYPE_PERCENTILE:
            break;
        }
    }
    return Status::OK();
}

Status OlapScanConjunctsManager::build_olap_filters() {
    olap_filters.clear();

    for (auto iter : column_value_ranges) {
        std::vector<TCondition> filters;
        boost::apply_visitor([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);
        bool empty_range = boost::apply_visitor([](auto&& range) { return range.is_empty_value_range(); }, iter.second);
        if (empty_range) {
            return Status::EndOfFile("EOF, Filter by always false condition");
        }

        for (auto& filter : filters) {
            olap_filters.emplace_back(std::move(filter));
        }
    }

    return Status::OK();
}

// Try to convert the ranges predicates applied on key columns to in predicates to increase
// the scan concurrency, i.e, the number of OlapScanners.
// For example, if the original query is `select * from t where c0 between 1 and 3 and c1 between 12 and 13`,
// where c0 is the first key column and c1 is the second key column, this routine will convert the predicates
// to `where c0 in (1,2,3) and c1 in (12,13)`, which is equivalent to the following disjunctive predicates:
// `where (c0=1 and c1=12)
//     OR (c0=1 and c1=13)
//     OR (c0=2 and c1=12)
//     OR (c0=2 and c1=13)
//     OR (c0=3 and c1=12)
//     OR (c0=3 and c1=13)
// `.
// By doing so, we can then create six instances of OlapScanner and assign each one with one of the disjunctive
// predicates and run the OlapScanners concurrently.

class ExtendScanKeyVisitor : public boost::static_visitor<Status> {
public:
    ExtendScanKeyVisitor(OlapScanKeys* scan_keys, int32_t max_scan_key_num)
            : _scan_keys(scan_keys), _max_scan_key_num(max_scan_key_num) {}

    template <class T>
    Status operator()(T& v) {
        return _scan_keys->extend_scan_key(v, _max_scan_key_num);
    }

private:
    OlapScanKeys* _scan_keys;
    int32_t _max_scan_key_num;
};

Status OlapScanConjunctsManager::build_scan_keys(bool unlimited, int32_t max_scan_key_num) {
    int conditional_key_columns = 0;
    scan_keys.set_is_convertible(unlimited);
    const std::vector<std::string>& ref_key_column_names = *key_column_names;

    for (const auto& key_column_name : ref_key_column_names) {
        if (column_value_ranges.count(key_column_name) == 0) {
            break;
        }
        conditional_key_columns++;
    }
    if (conditional_key_columns > 1) {
        for (int i = 0; i < conditional_key_columns && !scan_keys.has_range_value(); ++i) {
            ExtendScanKeyVisitor visitor(&scan_keys, max_scan_key_num);
            if (!boost::apply_visitor(visitor, column_value_ranges[ref_key_column_names[i]]).ok()) {
                break;
            }
        }
    }
    return Status::OK();
}

Status OlapScanConjunctsManager::get_column_predicates(PredicateParser* parser,
                                                       std::vector<std::unique_ptr<ColumnPredicate>>* preds) {
    for (auto& f : olap_filters) {
        std::unique_ptr<ColumnPredicate> p(parser->parse_thrift_cond(f));
        RETURN_IF(!p, Status::RuntimeError("invalid filter"));
        p->set_index_filter_only(f.is_index_filter_only);
        preds->emplace_back(std::move(p));
    }
    for (auto& f : is_null_vector) {
        std::unique_ptr<ColumnPredicate> p(parser->parse_thrift_cond(f));
        RETURN_IF(!p, Status::RuntimeError("invalid filter"));
        preds->emplace_back(std::move(p));
    }

    const auto& slots = tuple_desc->decoded_slots();
    for (auto& iter : slot_index_to_expr_ctxs) {
        int slot_index = iter.first;
        auto& expr_ctxs = iter.second;
        const SlotDescriptor* slot_desc = slots[slot_index];
        for (ExprContext* ctx : expr_ctxs) {
            std::unique_ptr<ColumnPredicate> p(parser->parse_expr_ctx(*slot_desc, runtime_state, ctx));
            if (p == nullptr) {
                std::stringstream ss;
                ss << "invalid filter, slot=" << slot_desc->debug_string();
                if (ctx != nullptr) {
                    ss << ", expr=" << ctx->root()->debug_string();
                }
                LOG(WARNING) << ss.str();
                return Status::RuntimeError("invalid filter");
            } else {
                preds->emplace_back(std::move(p));
            }
        }
    }
    return Status::OK();
}

void OlapScanConjunctsManager::eval_const_conjuncts(const std::vector<ExprContext*>& conjunct_ctxs, Status* status) {
    *status = Status::OK();
    for (const auto& ctx_iter : conjunct_ctxs) {
        // if conjunct is constant, compute direct and set eos = true
        if (ctx_iter->root()->is_constant()) {
            ColumnPtr value = ctx_iter->root()->evaluate_const(ctx_iter);

            if (value == nullptr || value->only_null() || value->is_null(0)) {
                *status = Status::EndOfFile("conjuncts evaluated to null");
                break;
            } else if (value->is_constant() && !ColumnHelper::get_const_value<TYPE_BOOLEAN>(value)) {
                *status = Status::EndOfFile("conjuncts evaluated to false");
                break;
            }
        }
    }
}

Status OlapScanConjunctsManager::get_key_ranges(std::vector<std::unique_ptr<OlapScanRange>>* key_ranges) {
    RETURN_IF_ERROR(scan_keys.get_key_range(key_ranges));
    if (key_ranges->empty()) {
        key_ranges->emplace_back(std::make_unique<OlapScanRange>());
    }
    return Status::OK();
}

void OlapScanConjunctsManager::get_not_push_down_conjuncts(std::vector<ExprContext*>* predicates) {
    DCHECK_EQ(conjunct_ctxs_ptr->size(), normalized_conjuncts.size());
    for (size_t i = 0; i < normalized_conjuncts.size(); i++) {
        if (!normalized_conjuncts[i]) {
            predicates->push_back(conjunct_ctxs_ptr->at(i));
        }
    }
}

void OlapScanConjunctsManager::build_column_expr_predicates() {
    std::map<SlotId, int> slot_id_to_index;
    const auto& slots = tuple_desc->decoded_slots();
    for (int i = 0; i < slots.size(); i++) {
        const SlotDescriptor* slot_desc = slots[i];
        SlotId slot_id = slot_desc->id();
        slot_id_to_index.insert(std::make_pair(slot_id, i));
    }

    const auto& conjunct_ctxs = (*conjunct_ctxs_ptr);
    for (size_t i = 0; i < conjunct_ctxs.size(); i++) {
        if (normalized_conjuncts[i]) continue;

        ExprContext* ctx = conjunct_ctxs[i];
        std::vector<SlotId> slot_ids;
        ctx->root()->get_slot_ids(&slot_ids);
        if (slot_ids.size() != 1) continue;
        int index = -1;
        {
            auto iter = slot_id_to_index.find(slot_ids[0]);
            if (iter == slot_id_to_index.end()) continue;
            index = iter->second;
        }
        // note(yan): we only handles scalar type now to avoid complex type mismatch.
        // otherwise we don't need this limitation.
        const SlotDescriptor* slot_desc = slots[index];
        PrimitiveType ptype = slot_desc->type().type;
        if (!is_scalar_primitive_type(ptype)) continue;
        // disable on float/double type because min/max value may lose precision
        // The fix should be on storage layer, and this is just a temporary fix.
        if (ptype == PrimitiveType::TYPE_FLOAT || ptype == PrimitiveType::TYPE_DOUBLE) continue;
        {
            auto iter = slot_index_to_expr_ctxs.find(index);
            if (iter == slot_index_to_expr_ctxs.end()) {
                slot_index_to_expr_ctxs.insert(make_pair(index, std::vector<ExprContext*>{}));
                iter = slot_index_to_expr_ctxs.find(index);
            }
            iter->second.emplace_back(ctx);
        }
        normalized_conjuncts[i] = true;
    }
}

Status OlapScanConjunctsManager::parse_conjuncts(bool scan_keys_unlimited, int32_t max_scan_key_num,
                                                 bool enable_column_expr_predicate) {
    normalize_conjuncts();
    RETURN_IF_ERROR(build_olap_filters());
    build_scan_keys(scan_keys_unlimited, max_scan_key_num);
    if (enable_column_expr_predicate) {
        VLOG_FILE << "OlapScanConjunctsManager: enable_column_expr_predicate = true. push down column expr predicates";
        build_column_expr_predicates();
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace starrocks
