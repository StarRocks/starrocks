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

#include <variant>

#include "column/type_traits.h"
#include "exprs/binary_predicate.h"
#include "exprs/compound_predicate.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/is_null_predicate.h"
#include "gutil/map_util.h"
#include "runtime/descriptors.h"
#include "storage/column_predicate.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/olap_runtime_range_pruner.hpp"
#include "storage/predicate_parser.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "types/date_value.hpp"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// Util methods.
// ------------------------------------------------------------------------------------

static bool ignore_cast(const SlotDescriptor& slot, const Expr& expr) {
    if (slot.type().is_date_type() && expr.type().is_date_type()) {
        return true;
    }
    return slot.type().is_string_type() && expr.type().is_string_type();
}

static Expr* get_root_expr(Expr* root) {
    if (dynamic_cast<DictMappingExpr*>(root)) {
        return root->get_child(1);
    }
    return root;
}

static Expr* get_root_expr(ExprContext* ctx) {
    return get_root_expr(ctx->root());
}

template <typename ValueType>
static bool check_decimal_overflow(int precision, const ValueType& value) {
    if constexpr (is_decimal<ValueType>) {
        return -get_scale_factor<ValueType>(precision) < value && value < get_scale_factor<ValueType>(precision);
    } else {
        return false;
    }
}

template <bool Negative, typename ValueType>
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
    ColumnPtr column_ptr = EVALUATE_NULL_IF_ERROR(ctx, r, nullptr);
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
        *op = to_olap_filter_type<Negative>(expr->op(), false);
    } else {
        *op = to_olap_filter_type<Negative>(expr->op(), reverse_op);
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
        const auto* slice = reinterpret_cast<const Slice*>(data->raw_data());
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

static std::vector<BoxedExprContext> build_expr_context_containers(const std::vector<ExprContext*>& expr_contexts) {
    std::vector<BoxedExprContext> containers;
    for (auto* expr_ctx : expr_contexts) {
        containers.emplace_back(expr_ctx);
    }
    return containers;
}

static std::vector<BoxedExpr> build_raw_expr_containers(const std::vector<Expr*>& exprs) {
    std::vector<BoxedExpr> containers;
    for (auto* expr : exprs) {
        containers.emplace_back(expr);
    }
    return containers;
}

template <bool Negative>
static TExprOpcode::type maybe_invert_in_and_equal_op(const TExprOpcode::type op) {
    if constexpr (!Negative) {
        return op;
    } else {
        switch (op) {
        case TExprOpcode::FILTER_IN:
            return TExprOpcode::FILTER_NOT_IN;
        case TExprOpcode::FILTER_NOT_IN:
            return TExprOpcode::FILTER_IN;
        case TExprOpcode::EQ:
            return TExprOpcode::NE;
        case TExprOpcode::NE:
            return TExprOpcode::EQ;

        default:
            return op;
        }
    }
}

// ------------------------------------------------------------------------------------
// ChunkPredicateBuilder
// ------------------------------------------------------------------------------------

BoxedExpr::BoxedExpr(Expr* root_expr) : root_expr(root_expr) {}
Expr* BoxedExpr::root() const {
    return get_root_expr(root_expr);
}
StatusOr<ExprContext*> BoxedExpr::expr_context(ObjectPool* obj_pool, RuntimeState* state) const {
    if (new_expr_ctx == nullptr) {
        // Copy expr to prevent two ExprContexts from owning the same Expr, which will cause the same Expr to be
        // closed twice.
        // - The ExprContext in the `original _opts.conjunct_ctxs_ptr` will own an Expr and all its children.
        // - The newly created ExprContext here will also own this Expr.
        auto* new_expr = Expr::copy(obj_pool, root_expr);
        new_expr_ctx = obj_pool->add(new ExprContext(new_expr));
        RETURN_IF_ERROR(new_expr_ctx->prepare(state));
        RETURN_IF_ERROR(new_expr_ctx->open(state));
    }
    return new_expr_ctx;
}

BoxedExprContext::BoxedExprContext(ExprContext* expr_ctx) : expr_ctx(expr_ctx) {}
Expr* BoxedExprContext::root() const {
    return get_root_expr(expr_ctx);
}
StatusOr<ExprContext*> BoxedExprContext::expr_context(ObjectPool* obj_pool, RuntimeState* state) const {
    return expr_ctx;
}

template <BoxedExprType E, CompoundNodeType Type>
ChunkPredicateBuilder<E, Type>::ChunkPredicateBuilder(const ScanConjunctsManagerOptions& opts, std::vector<E> exprs,
                                                      bool is_root_builder)
        : _opts(opts), _exprs(std::move(exprs)), _is_root_builder(is_root_builder), _normalized_exprs(_exprs.size()) {}

template <BoxedExprType E, CompoundNodeType Type>
StatusOr<bool> ChunkPredicateBuilder<E, Type>::parse_conjuncts() {
    RETURN_IF_ERROR(normalize_expressions());
    RETURN_IF_ERROR(build_olap_filters());

    // Only the root builder builds scan keys.
    if (_is_root_builder && _opts.is_olap_scan) {
        RETURN_IF_ERROR(build_scan_keys(_opts.scan_keys_unlimited, _opts.max_scan_key_num));
    }

    if (_opts.enable_column_expr_predicate) {
        VLOG_FILE << "OlapScanConjunctsManager: enable_column_expr_predicate = true. push down column expr predicates";
        RETURN_IF_ERROR(build_column_expr_predicates());
    }

    ASSIGN_OR_RETURN(auto normalized, _normalize_compound_predicates());
    if (_is_root_builder) {
        return normalized;
    }
    // Non-root builder return true only when all the child predicates are normalized.
    return normalized && !SIMD::contain_zero(_normalized_exprs);
}

template <BoxedExprType E, CompoundNodeType Type>
StatusOr<bool> ChunkPredicateBuilder<E, Type>::_normalize_compound_predicates() {
    const size_t num_preds = _exprs.size();
    for (size_t i = 0; i < num_preds; i++) {
        if (_normalized_exprs[i]) {
            continue;
        }

        ASSIGN_OR_RETURN(const bool normalized, _normalize_compound_predicate(_exprs[i].root()));
        if (!normalized && !_is_root_builder) {
            return false;
        }
        _normalized_exprs[i] = normalized;
    }

    return true;
}

template <BoxedExprType E, CompoundNodeType Type>
StatusOr<bool> ChunkPredicateBuilder<E, Type>::_normalize_compound_predicate(const Expr* root_expr) {
    auto process = [&]<CompoundNodeType ChildType>() -> StatusOr<bool> {
        ChunkPredicateBuilder<BoxedExpr, ChildType> child_builder(
                _opts, build_raw_expr_containers(root_expr->children()), false);
        ASSIGN_OR_RETURN(const bool normalized, child_builder.parse_conjuncts());
        if (normalized) {
            _child_builders.emplace_back(std::move(child_builder));
        }
        return normalized;
    };

    if (TExprOpcode::COMPOUND_OR == root_expr->op()) {
        if (!_opts.pred_tree_params.enable_or) {
            return false;
        }
        return process.template operator()<CompoundNodeType::OR>();
    }

    if (TExprOpcode::COMPOUND_AND == root_expr->op()) {
        return process.template operator()<CompoundNodeType::AND>();
    }

    return false;
}

template <BoxedExprType E, CompoundNodeType Type>
StatusOr<PredicateCompoundNode<Type>> ChunkPredicateBuilder<E, Type>::get_predicate_tree_root(
        PredicateParser* parser, ColumnPredicatePtrs& col_preds_owner) {
    auto compound_node = PredicateCompoundNode<Type>{};

    const size_t start_i = col_preds_owner.size();
    RETURN_IF_ERROR(_get_column_predicates(parser, col_preds_owner));
    for (size_t i = start_i; i < col_preds_owner.size(); i++) {
        compound_node.add_child(PredicateColumnNode{col_preds_owner[i].get()});
    }

    for (auto& child_builder : _child_builders) {
        RETURN_IF_ERROR(std::visit(
                [&](auto& c) {
                    ASSIGN_OR_RETURN(auto child_node, c.get_predicate_tree_root(parser, col_preds_owner));
                    compound_node.add_child(std::move(child_node));
                    return Status::OK();
                },
                child_builder));
    }

    return compound_node;
}

template <bool Negative>
static bool is_not_in(const auto* pred) {
    if constexpr (Negative) {
        return !pred->is_not_in();
    } else {
        return pred->is_not_in();
    }
};

// clang-format off
template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, typename RangeValueType, bool Negative>
requires(!lt_is_date<SlotType>) Status ChunkPredicateBuilder<E, Type>::normalize_in_or_equal_predicate(
        const SlotDescriptor& slot, ColumnValueRange<RangeValueType>* range) {
    // clang-format on

    Status status;

    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) {
            continue;
        }

        const Expr* root_expr = _exprs[i].root();

        // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
        if (TExprOpcode::FILTER_IN == maybe_invert_in_and_equal_op<Negative>(root_expr->op())) {
            const Expr* l = root_expr->get_child(0);

            if (!l->is_slotref() || (l->type().type != slot.type().type && !ignore_cast(slot, *l))) {
                continue;
            }
            std::vector<SlotId> slot_ids;
            if (1 == l->get_slot_ids(&slot_ids) && slot_ids[0] == slot.id()) {
                const auto* pred = down_cast<const VectorizedInConstPredicate<SlotType>*>(root_expr);
                // join in runtime filter  will handle by `_normalize_join_runtime_filter`
                if (pred->is_join_runtime_filter()) {
                    continue;
                }

                if (is_not_in<Negative>(pred) || pred->null_in_set() ||
                    pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                    continue;
                }

                std::set<RangeValueType> values;
                for (const auto& value : pred->hash_set()) {
                    values.insert(value);
                }
                if (range->add_fixed_values(FILTER_IN, values).ok()) {
                    _normalized_exprs[i] = true;
                }
            }
        }

        // 2. Normalize eq conjuncts like 'where col = value'
        if (TExprNodeType::BINARY_PRED == root_expr->node_type() &&
            FILTER_IN == to_olap_filter_type<Negative>(root_expr->op(), false)) {
            using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;
            SQLFilterOp op;
            ValueType value;
            ASSIGN_OR_RETURN(auto* expr_context, _exprs[i].expr_context(_opts.obj_pool, _opts.runtime_state));
            bool ok =
                    get_predicate_value<Negative>(_opts.obj_pool, slot, root_expr, expr_context, &value, &op, &status);
            if (ok && range->add_fixed_values(FILTER_IN, std::set<RangeValueType>{value}).ok()) {
                _normalized_exprs[i] = true;
            }
        }
    }

    return Status::OK();
}

// clang-format off
// explicit specialization for DATE.
template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, typename RangeValueType, bool Negative>
requires lt_is_date<SlotType> Status ChunkPredicateBuilder<E, Type>::normalize_in_or_equal_predicate(
        const SlotDescriptor& slot, ColumnValueRange<RangeValueType>* range) {
    // clang-format on
    Status status;

    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) {
            continue;
        }

        const Expr* root_expr = _exprs[i].root();

        // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
        if (TExprOpcode::FILTER_IN == maybe_invert_in_and_equal_op<Negative>(root_expr->op())) {
            const Expr* l = root_expr->get_child(0);
            // TODO(zhuming): DATE column may be casted to double.
            if (l->type().type != starrocks::TYPE_DATE && l->type().type != starrocks::TYPE_DATETIME) {
                continue;
            }

            LogicalType pred_type = l->type().type;
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

                    if (is_not_in<Negative>(pred) || pred->null_in_set() ||
                        pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                        continue;
                    }

                    for (const TimestampValue& ts : pred->hash_set()) {
                        auto date = implicit_cast<DateValue>(ts);
                        if (implicit_cast<TimestampValue>(date) == ts) {
                            values.insert(date);
                        }
                    }
                } else if (pred_type == starrocks::TYPE_DATE) {
                    const auto* pred = down_cast<const VectorizedInConstPredicate<starrocks::TYPE_DATE>*>(root_expr);

                    if (is_not_in<Negative>(pred) || pred->null_in_set() ||
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
                    _normalized_exprs[i] = true;
                }
            }
        }

        // 2. Normalize eq conjuncts like 'where col = value'
        if (TExprNodeType::BINARY_PRED == root_expr->node_type() &&
            FILTER_IN == to_olap_filter_type<Negative>(root_expr->op(), false)) {
            SQLFilterOp op;
            DateValue value{0};
            ASSIGN_OR_RETURN(auto* expr_context, _exprs[i].expr_context(_opts.obj_pool, _opts.runtime_state));
            bool ok =
                    get_predicate_value<Negative>(_opts.obj_pool, slot, root_expr, expr_context, &value, &op, &status);
            if (ok && range->add_fixed_values(FILTER_IN, std::set<DateValue>{value}).ok()) {
                _normalized_exprs[i] = true;
            }
        }
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, typename RangeValueType, bool Negative>
Status ChunkPredicateBuilder<E, Type>::normalize_binary_predicate(const SlotDescriptor& slot,
                                                                  ColumnValueRange<RangeValueType>* range) {
    Status status;
    DCHECK((SlotType == slot.type().type) || (SlotType == TYPE_VARCHAR && slot.type().type == TYPE_CHAR));

    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) {
            continue;
        }

        const Expr* root_expr = _exprs[i].root();

        if (TExprNodeType::BINARY_PRED != root_expr->node_type()) {
            continue;
        }

        using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;

        SQLFilterOp op;
        ValueType value;
        ASSIGN_OR_RETURN(auto* expr_context, _exprs[i].expr_context(_opts.obj_pool, _opts.runtime_state));
        bool ok = get_predicate_value<Negative>(_opts.obj_pool, slot, root_expr, expr_context, &value, &op, &status);
        if (ok && range->add_range(op, static_cast<RangeValueType>(value)).ok()) {
            _normalized_exprs[i] = true;
        }
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, LogicalType MappingType, template <class> class Decoder, class... Args>
void ChunkPredicateBuilder<E, Type>::normalized_rf_with_null(const JoinRuntimeFilter* rf, Expr* col_ref,
                                                             Args&&... args) {
    DCHECK(Type == CompoundNodeType::AND);

    ObjectPool* pool = _opts.obj_pool;

    const auto* filter = down_cast<const RuntimeBloomFilter<MappingType>*>(rf);
    using DecoderType = Decoder<typename RunTimeTypeTraits<MappingType>::CppType>;
    DecoderType decoder(std::forward<Args>(args)...);
    detail::RuntimeColumnPredicateBuilder::MinMaxParser<RuntimeBloomFilter<MappingType>, DecoderType> parser(filter,
                                                                                                             &decoder);
    const TypeDescriptor& col_type = col_ref->type();

    ColumnPtr const_min_col = parser.template min_const_column<SlotType>(col_type);
    ColumnPtr const_max_col = parser.template max_const_column<SlotType>(col_type);
    VectorizedLiteral* min_literal = pool->add(new VectorizedLiteral(std::move(const_min_col), col_type));
    VectorizedLiteral* max_literal = pool->add(new VectorizedLiteral(std::move(const_max_col), col_type));

    Expr* left_expr = _gen_min_binary_pred(col_ref, min_literal, filter->left_close_interval());
    Expr* right_expr = _gen_max_binary_pred(col_ref, max_literal, filter->right_close_interval());
    Expr* is_null_expr = _gen_is_null_pred(col_ref);
    Expr* and_expr = _gen_and_pred(left_expr, right_expr);

    std::vector<BoxedExpr> containers;
    containers.emplace_back(and_expr);
    containers.emplace_back(is_null_expr);
    ChunkPredicateBuilder<BoxedExpr, CompoundNodeType::OR> child_builder(_opts, containers, false);

    auto normalized = child_builder.parse_conjuncts();
    if (!normalized.ok()) {
    } else if (normalized.value()) {
        _child_builders.emplace_back(child_builder);
    }
}

template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, typename RangeValueType, bool Negative>
Status ChunkPredicateBuilder<E, Type>::normalize_join_runtime_filter(const SlotDescriptor& slot,
                                                                     ColumnValueRange<RangeValueType>* range) {
    if (!_is_root_builder) {
        return Status::OK();
    }
    DCHECK(!Negative);

    // in runtime filter
    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) {
            continue;
        }

        const Expr* root_expr = _exprs[i].root();
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
                _normalized_exprs[i] = true;

                if (pred->is_not_in() || pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                    continue;
                }

                if (pred->null_in_set()) {
                    std::vector<BoxedExpr> containers;
                    auto* new_in_pred =
                            down_cast<VectorizedInConstPredicate<SlotType>*>(root_expr->clone(_opts.obj_pool));
                    const auto& childs = root_expr->children();
                    for (const auto& child : childs) {
                        new_in_pred->add_child(child);
                    }
                    new_in_pred->set_null_in_set(false);
                    new_in_pred->set_is_join_runtime_filter(false);

                    auto* is_null_pred = _gen_is_null_pred(childs[0]);

                    containers.emplace_back(new_in_pred);
                    containers.emplace_back(is_null_pred);

                    ChunkPredicateBuilder<BoxedExpr, CompoundNodeType::OR> child_builder(_opts, containers, false);
                    auto normalized = child_builder.parse_conjuncts();
                    if (!normalized.ok()) {
                        continue;
                    } else if (normalized.value()) {
                        _child_builders.emplace_back(child_builder);
                    }
                } else {
                    std::set<RangeValueType> values;
                    for (const auto& value : pred->hash_set()) {
                        values.insert(value);
                    }
                    (void)range->add_fixed_values(FILTER_IN, values);
                }
            }
        }
    }

    // bloom runtime filter
    for (const auto& it : _opts.runtime_filters->descriptors()) {
        RuntimeFilterProbeDescriptor* desc = it.second;
        const JoinRuntimeFilter* rf = desc->runtime_filter(_opts.driver_sequence);
        using RangeType = ColumnValueRange<RangeValueType>;
        using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;
        SlotId slot_id;

        // probe expr is slot ref and slot id matches.
        if (!desc->is_probe_slot_ref(&slot_id) || slot_id != slot.id()) continue;

        // runtime filter existed and does not have null.
        if (rf == nullptr) {
            rt_ranger_params.add_unarrived_rf(desc, &slot, _opts.driver_sequence);
            continue;
        }

        // If this column doesn't have other filter, we use join runtime filter
        // to fast comput row range in storage engine
        if (range->is_init_state()) {
            range->set_index_filter_only(true);
        }

        // if we have multi-scanners
        // If a scanner has finished building a runtime filter,
        // the rest of the runtime filters will be normalized here

        auto& global_dicts = _opts.runtime_state->get_query_global_dict_map();
        if constexpr (SlotType == TYPE_VARCHAR) {
            if (auto iter = global_dicts.find(slot_id); iter != global_dicts.end()) {
                if (rf->has_null()) {
                    normalized_rf_with_null<SlotType, LowCardDictType,
                                            detail::RuntimeColumnPredicateBuilder::GlobalDictCodeDecoder>(
                            rf, desc->probe_expr_ctx()->root(), &iter->second.first);
                } else {
                    detail::RuntimeColumnPredicateBuilder::build_minmax_range<
                            RangeType, SlotType, LowCardDictType,
                            detail::RuntimeColumnPredicateBuilder::GlobalDictCodeDecoder>(*range, rf,
                                                                                          &iter->second.first);
                }
            } else {
                if (rf->has_null()) {
                    normalized_rf_with_null<SlotType, SlotType, detail::RuntimeColumnPredicateBuilder::DummyDecoder>(
                            rf, desc->probe_expr_ctx()->root(), nullptr);
                } else {
                    detail::RuntimeColumnPredicateBuilder::build_minmax_range<
                            RangeType, SlotType, SlotType, detail::RuntimeColumnPredicateBuilder::DummyDecoder>(
                            *range, rf, nullptr);
                }
            }
        } else {
            if (rf->has_null()) {
                normalized_rf_with_null<SlotType, SlotType, detail::RuntimeColumnPredicateBuilder::DummyDecoder>(
                        rf, desc->probe_expr_ctx()->root(), nullptr);
            } else {
                detail::RuntimeColumnPredicateBuilder::build_minmax_range<
                        RangeType, SlotType, SlotType, detail::RuntimeColumnPredicateBuilder::DummyDecoder>(*range, rf,
                                                                                                            nullptr);
            }
        }
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, typename RangeValueType, bool Negative>
Status ChunkPredicateBuilder<E, Type>::normalize_not_in_or_not_equal_predicate(
        const SlotDescriptor& slot, ColumnValueRange<RangeValueType>* range) {
    Status status;
    DCHECK((SlotType == slot.type().type) || (SlotType == TYPE_VARCHAR && slot.type().type == TYPE_CHAR));

    using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;
    // handle not equal.
    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) {
            continue;
        }
        const Expr* root_expr = _exprs[i].root();

        // handle not equal
        if (root_expr->node_type() == TExprNodeType::BINARY_PRED &&
            maybe_invert_in_and_equal_op<Negative>(root_expr->op()) == TExprOpcode::NE) {
            SQLFilterOp op;
            ValueType value;
            ASSIGN_OR_RETURN(auto* expr_context, _exprs[i].expr_context(_opts.obj_pool, _opts.runtime_state));
            bool ok =
                    get_predicate_value<Negative>(_opts.obj_pool, slot, root_expr, expr_context, &value, &op, &status);
            if (ok && range->add_fixed_values(FILTER_NOT_IN, std::set<RangeValueType>{value}).ok()) {
                _normalized_exprs[i] = true;
            }
        }

        // handle not in
        if (root_expr->node_type() == TExprNodeType::IN_PRED &&
            maybe_invert_in_and_equal_op<Negative>(root_expr->op()) == TExprOpcode::FILTER_NOT_IN) {
            const Expr* l = root_expr->get_child(0);
            if (!l->is_slotref() || (l->type().type != slot.type().type && !ignore_cast(slot, *l))) {
                continue;
            }
            std::vector<SlotId> slot_ids;

            if (1 == l->get_slot_ids(&slot_ids) && slot_ids[0] == slot.id()) {
                const auto* pred = down_cast<const VectorizedInConstPredicate<SlotType>*>(root_expr);

                if (pred->is_join_runtime_filter()) {
                    continue;
                }

                if (!is_not_in<Negative>(pred) || pred->null_in_set() ||
                    pred->hash_set().size() > config::max_pushdown_conditions_per_column) {
                    continue;
                }

                std::set<RangeValueType> values;
                for (const auto& value : pred->hash_set()) {
                    values.insert(value);
                }
                if (range->add_fixed_values(FILTER_NOT_IN, values).ok()) {
                    _normalized_exprs[i] = true;
                }
            }
        }
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::normalize_is_null_predicate(const SlotDescriptor& slot) {
    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) {
            continue;
        }
        const Expr* root_expr = _exprs[i].root();
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
                _normalized_exprs[i] = true;
            }
        }
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
template <LogicalType SlotType, typename RangeValueType>
Status ChunkPredicateBuilder<E, Type>::normalize_predicate(const SlotDescriptor& slot,
                                                           ColumnValueRange<RangeValueType>* range) {
    constexpr bool Negative = Type == CompoundNodeType::OR;
    RETURN_IF_ERROR((normalize_in_or_equal_predicate<SlotType, RangeValueType, Negative>(slot, range)));
    RETURN_IF_ERROR((normalize_binary_predicate<SlotType, RangeValueType, Negative>(slot, range)));
    // Execute normalize_not_in_or_not_equal_predicate after normalize_binary_predicate,
    // because the range generated by not in predicates cannot be merged with the range generated by binary predicates.
    RETURN_IF_ERROR((normalize_not_in_or_not_equal_predicate<SlotType, RangeValueType, Negative>(slot, range)));
    RETURN_IF_ERROR(normalize_is_null_predicate(slot));
    // Must handle join runtime filter last
    RETURN_IF_ERROR((normalize_join_runtime_filter<SlotType, RangeValueType, Negative>(slot, range)));

    return Status::OK();
}

struct ColumnRangeBuilder {
    template <LogicalType ltype, typename E, CompoundNodeType Type>
    Status operator()(ChunkPredicateBuilder<E, Type>* parent, const SlotDescriptor* slot,
                      std::map<std::string, ColumnValueRangeType>* column_value_ranges) {
        if constexpr (ltype == TYPE_TIME || ltype == TYPE_NULL || ltype == TYPE_JSON || lt_is_float<ltype> ||
                      lt_is_binary<ltype>) {
            return Status::OK();
        } else {
            // Treat tinyint and boolean as int
            constexpr LogicalType limit_type = ltype == TYPE_TINYINT || ltype == TYPE_BOOLEAN ? TYPE_INT : ltype;
            // Map TYPE_CHAR to TYPE_VARCHAR
            constexpr LogicalType mapping_type = ltype == TYPE_CHAR ? TYPE_VARCHAR : ltype;
            using value_type = typename RunTimeTypeLimits<limit_type>::value_type;
            using RangeType = ColumnValueRange<value_type>;

            const std::string& col_name = slot->col_name();
            RangeType full_range(col_name, ltype, RunTimeTypeLimits<ltype>::min_value(),
                                 RunTimeTypeLimits<ltype>::max_value());
            if constexpr (lt_is_decimal<limit_type>) {
                full_range.set_precision(slot->type().precision);
                full_range.set_scale(slot->type().scale);
            }
            ColumnValueRangeType& v = LookupOrInsert(column_value_ranges, col_name, full_range);
            auto& range = std::get<ColumnValueRange<value_type>>(v);
            if constexpr (lt_is_decimal<limit_type>) {
                range.set_precision(slot->type().precision);
                range.set_scale(slot->type().scale);
            }

            return parent->template normalize_predicate<mapping_type, value_type>(*slot, &range);
        }
    }
};

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::normalize_expressions() {
    // Note: _normalized_exprs size must be equal to _conjunct_ctxs size,
    // but HashJoinNode will push down predicate to OlapScanNode's _conjunct_ctxs,
    // So _conjunct_ctxs will change after OlapScanNode prepare,
    // So we couldn't resize _normalized_exprs when OlapScanNode init or prepare

    // TODO(zhuming): if any of the normalized column range is empty, we can know that
    // no row will be selected anymore and can return EOF directly.
    for (auto& slot : _opts.tuple_desc->decoded_slots()) {
        RETURN_IF_ERROR(type_dispatch_predicate<Status>(slot->type().type, false, ColumnRangeBuilder(), this, slot,
                                                        &column_value_ranges));
    }
    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::build_olap_filters() {
    constexpr bool Negative = Type == CompoundNodeType::OR;
    olap_filters.clear();

    // False alert from clang-tidy-14
    // NOLINTNEXTLINE(performance-for-range-copy)
    for (auto iter : column_value_ranges) {
        std::vector<TCondition> filters;
        std::visit([&](auto&& range) { range.template to_olap_filter<Negative>(filters); }, iter.second);
        const bool empty_range = std::visit([](auto&& range) { return range.is_empty_value_range(); }, iter.second);
        if (empty_range) {
            if constexpr (!Negative) {
                return Status::EndOfFile("EOF, Filter by always false condition");
            } else {
                auto not_null_filter =
                        std::visit([&](auto&& range) { return range.to_olap_not_null_filter(); }, iter.second);
                filters.clear();
                filters.emplace_back(std::move(not_null_filter));
            }
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

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::build_scan_keys(bool unlimited, int32_t max_scan_key_num) {
    int conditional_key_columns = 0;
    scan_keys.set_is_convertible(unlimited);
    const std::vector<std::string>& ref_key_column_names = *_opts.key_column_names;

    for (const auto& key_column_name : ref_key_column_names) {
        if (column_value_ranges.count(key_column_name) == 0) {
            break;
        }
        conditional_key_columns++;
    }
    if (config::enable_short_key_for_one_column_filter || conditional_key_columns > 1) {
        for (int i = 0; i < conditional_key_columns && !scan_keys.has_range_value(); ++i) {
            ExtendScanKeyVisitor visitor(&scan_keys, max_scan_key_num);
            if (!std::visit(visitor, column_value_ranges[ref_key_column_names[i]]).ok()) {
                break;
            }
        }
    }
    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::_get_column_predicates(PredicateParser* parser,
                                                              ColumnPredicatePtrs& col_preds_owner) {
    for (auto& f : olap_filters) {
        std::unique_ptr<ColumnPredicate> p(parser->parse_thrift_cond(f));
        RETURN_IF(!p, Status::RuntimeError("invalid filter"));
        p->set_index_filter_only(f.is_index_filter_only);
        col_preds_owner.emplace_back(std::move(p));
    }
    for (auto& f : is_null_vector) {
        std::unique_ptr<ColumnPredicate> p(parser->parse_thrift_cond(f));
        RETURN_IF(!p, Status::RuntimeError("invalid filter"));
        col_preds_owner.emplace_back(std::move(p));
    }

    const auto& slots = _opts.tuple_desc->decoded_slots();
    for (auto& [slot_index, expr_ctxs] : slot_index_to_expr_ctxs) {
        const SlotDescriptor* slot_desc = slots[slot_index];
        for (ExprContext* ctx : expr_ctxs) {
            ASSIGN_OR_RETURN(auto tmp, parser->parse_expr_ctx(*slot_desc, _opts.runtime_state, ctx));
            std::unique_ptr<ColumnPredicate> p(std::move(tmp));
            if (p == nullptr) {
                std::stringstream ss;
                ss << "invalid filter, slot=" << slot_desc->debug_string();
                if (ctx != nullptr) {
                    ss << ", expr=" << ctx->root()->debug_string();
                }
                LOG(WARNING) << ss.str();
                return Status::RuntimeError("invalid filter");
            } else {
                col_preds_owner.emplace_back(std::move(p));
            }
        }
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::get_key_ranges(std::vector<std::unique_ptr<OlapScanRange>>* key_ranges) {
    RETURN_IF_ERROR(scan_keys.get_key_range(key_ranges));
    if (key_ranges->empty()) {
        key_ranges->emplace_back(std::make_unique<OlapScanRange>());
    }
    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
bool ChunkPredicateBuilder<E, Type>::is_pred_normalized(size_t index) const {
    return index < _normalized_exprs.size() && _normalized_exprs[index];
}

template <BoxedExprType E, CompoundNodeType Type>
Status ChunkPredicateBuilder<E, Type>::build_column_expr_predicates() {
    std::map<SlotId, int> slot_id_to_index;
    const auto& slots = _opts.tuple_desc->decoded_slots();
    for (int i = 0; i < slots.size(); i++) {
        const SlotDescriptor* slot_desc = slots[i];
        SlotId slot_id = slot_desc->id();
        slot_id_to_index.insert(std::make_pair(slot_id, i));
    }

    for (size_t i = 0; i < _exprs.size(); i++) {
        if (_normalized_exprs[i]) continue;

        const Expr* root_expr = _exprs[i].root();
        std::vector<SlotId> slot_ids;
        root_expr->get_slot_ids(&slot_ids);
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
        LogicalType ltype = slot_desc->type().type;
        if (!support_column_expr_predicate(ltype)) {
            continue;
        }

        auto iter = slot_index_to_expr_ctxs.find(index);
        if (iter == slot_index_to_expr_ctxs.end()) {
            slot_index_to_expr_ctxs.insert(make_pair(index, std::vector<ExprContext*>{}));
            iter = slot_index_to_expr_ctxs.find(index);
        }
        ASSIGN_OR_RETURN(auto* expr_ctx, _exprs[i].expr_context(_opts.obj_pool, _opts.runtime_state));
        iter->second.emplace_back(expr_ctx);
        _normalized_exprs[i] = true;
    }

    return Status::OK();
}

template <BoxedExprType E, CompoundNodeType Type>
Expr* ChunkPredicateBuilder<E, Type>::_gen_min_binary_pred(Expr* col_ref, VectorizedLiteral* min_literal,
                                                           bool is_close_interval) {
    TExprNode node;
    node.node_type = TExprNodeType::BINARY_PRED;
    node.type = TypeDescriptor(TYPE_BOOLEAN).to_thrift();
    node.child_type = to_thrift(col_ref->type().type);
    if (is_close_interval) {
        node.__set_opcode(TExprOpcode::GE);
    } else {
        node.__set_opcode(TExprOpcode::GT);
    }

    Expr* expr = _opts.obj_pool->add(VectorizedBinaryPredicateFactory::from_thrift(node));
    expr->add_child(col_ref);
    expr->add_child(min_literal);
    return expr;
}

template <BoxedExprType E, CompoundNodeType Type>
Expr* ChunkPredicateBuilder<E, Type>::_gen_max_binary_pred(Expr* col_ref, VectorizedLiteral* max_literal,
                                                           bool is_close_interval) {
    TExprNode node;
    node.node_type = TExprNodeType::BINARY_PRED;
    node.type = TypeDescriptor(TYPE_BOOLEAN).to_thrift();
    node.child_type = to_thrift(col_ref->type().type);
    if (is_close_interval) {
        node.__set_opcode(TExprOpcode::LE);
    } else {
        node.__set_opcode(TExprOpcode::LT);
    }

    Expr* expr = _opts.obj_pool->add(VectorizedBinaryPredicateFactory::from_thrift(node));
    expr->add_child(col_ref);
    expr->add_child(max_literal);
    return expr;
}

template <BoxedExprType E, CompoundNodeType Type>
Expr* ChunkPredicateBuilder<E, Type>::_gen_is_null_pred(Expr* col_ref) {
    TExprNode null_pred_node;
    null_pred_node.node_type = TExprNodeType::FUNCTION_CALL;
    TFunction fn;
    fn.name.function_name = "is_null_pred";
    null_pred_node.__set_fn(fn);
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    type_node.__set_scalar_type(scalar_type);
    null_pred_node.type.types.emplace_back(type_node);

    Expr* expr = _opts.obj_pool->add(VectorizedIsNullPredicateFactory::from_thrift(null_pred_node));
    expr->add_child(col_ref);
    return expr;
}

template <BoxedExprType E, CompoundNodeType Type>
Expr* ChunkPredicateBuilder<E, Type>::_gen_and_pred(Expr* left, Expr* right) {
    TExprNode and_pred_node;
    and_pred_node.node_type = TExprNodeType::COMPOUND_PRED;
    and_pred_node.num_children = 2;
    and_pred_node.is_nullable = true;
    and_pred_node.__set_opcode(TExprOpcode::COMPOUND_AND);
    and_pred_node.__set_child_type(TPrimitiveType::BOOLEAN);
    and_pred_node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());

    Expr* expr = _opts.obj_pool->add(VectorizedCompoundPredicateFactory::from_thrift(and_pred_node));
    expr->add_child(left);
    expr->add_child(right);
    return expr;
}

// ------------------------------------------------------------------------------------
// OlapScanConjunctsManager
// ------------------------------------------------------------------------------------

ScanConjunctsManager::ScanConjunctsManager(ScanConjunctsManagerOptions&& opts)
        : _opts(opts), _root_builder(_opts, build_expr_context_containers(*_opts.conjunct_ctxs_ptr), true) {}

Status ScanConjunctsManager::parse_conjuncts() {
    return _root_builder.parse_conjuncts().status();
}

Status ScanConjunctsManager::eval_const_conjuncts(const std::vector<ExprContext*>& conjunct_ctxs, Status* status) {
    *status = Status::OK();
    for (const auto& ctx : conjunct_ctxs) {
        // if conjunct is constant, compute direct and set eos = true
        if (ctx->root()->is_constant()) {
            ASSIGN_OR_RETURN(ColumnPtr value, ctx->root()->evaluate_const(ctx));

            if (value == nullptr || value->only_null() || value->is_null(0)) {
                *status = Status::EndOfFile("conjuncts evaluated to null");
                break;
            } else if (value->is_constant() && !ColumnHelper::get_const_value<TYPE_BOOLEAN>(value)) {
                *status = Status::EndOfFile("conjuncts evaluated to false");
                break;
            }
        }
    }
    return Status::OK();
}

StatusOr<PredicateTree> ScanConjunctsManager::get_predicate_tree(PredicateParser* parser,
                                                                 ColumnPredicatePtrs& col_preds_owner) {
    ASSIGN_OR_RETURN(auto pred_root, _root_builder.get_predicate_tree_root(parser, col_preds_owner));
    return PredicateTree::create(std::move(pred_root));
}

Status ScanConjunctsManager::get_key_ranges(std::vector<std::unique_ptr<OlapScanRange>>* key_ranges) {
    return _root_builder.get_key_ranges(key_ranges);
}

void ScanConjunctsManager::get_not_push_down_conjuncts(std::vector<ExprContext*>* predicates) {
    // DCHECK_EQ(_opts.conjunct_ctxs_ptr->size(), _normalized_exprs.size());
    const size_t num_preds = _opts.conjunct_ctxs_ptr->size();
    for (size_t i = 0; i < num_preds; i++) {
        if (!_root_builder.is_pred_normalized(i)) {
            predicates->push_back(_opts.conjunct_ctxs_ptr->at(i));
        }
    }
}

const UnarrivedRuntimeFilterList& ScanConjunctsManager::unarrived_runtime_filters() {
    return _root_builder.unarrived_runtime_filters();
}

template class ChunkPredicateBuilder<BoxedExprContext, CompoundNodeType::AND>;
} // namespace starrocks
