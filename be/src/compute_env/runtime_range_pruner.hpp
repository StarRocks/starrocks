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

#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "column/global_dict/config.h"
#include "compute_env/runtime_range_pruner.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "exprs/binary_predicate.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_in_filter.h"
#include "storage_primitive/column_and_predicate.h"
#include "storage_primitive/column_or_predicate.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/column_value_range.h"
#include "storage_primitive/filter_condition.h"
#include "storage_primitive/predicate_parser.h"
#include "types/datum.h"

namespace starrocks {
namespace detail {
// The offset of a probe expression shaped `slot + c` / `slot - c` / `c + slot`, or nullopt.
// Only these invert: multiplication is not injective under wrapping (wrap(2a) collides a with
// a + 2^63) and `c - slot` reverses the order. Non-integral value types are excluded too -- the
// checked builtins do not apply to them.
template <typename CppType>
std::optional<CppType> probe_expr_offset(const Expr* expr, SlotId slot_id, ExprContext* ctx) {
    if constexpr (std::is_integral_v<CppType>) {
        if (expr->node_type() != TExprNodeType::ARITHMETIC_EXPR) return std::nullopt;
        if (expr->op() != TExprOpcode::ADD && expr->op() != TExprOpcode::SUBTRACT) return std::nullopt;
        if (expr->get_num_children() != 2) return std::nullopt;

        auto is_the_slot = [&](const Expr* e) {
            return e->node_type() == TExprNodeType::SLOT_REF && e->is_slotref() &&
                   down_cast<const ColumnRef*>(e)->slot_id() == slot_id;
        };
        // evaluate_const() dereferences the context it is given, so it gets the one owning this tree.
        auto literal_value = [&](const Expr* e, CppType* out) {
            if (e->node_type() != TExprNodeType::INT_LITERAL) return false;
            auto res = const_cast<Expr*>(e)->evaluate_const(ctx);
            if (!res.ok()) return false;
            ColumnPtr column = std::move(res).value();
            if (column == nullptr || column->size() < 1 || column->is_null(0)) return false;
            *out = column->get(0).get<CppType>();
            return true;
        };

        CppType c{};
        if (is_the_slot(expr->get_child(0)) && literal_value(expr->get_child(1), &c)) {
            if (expr->op() == TExprOpcode::ADD) return c;
            CppType negated{};
            // `slot - CppType_MIN` has no representable offset.
            if (__builtin_sub_overflow(CppType{0}, c, &negated)) return std::nullopt;
            return negated;
        }
        if (expr->op() == TExprOpcode::ADD && is_the_slot(expr->get_child(1)) &&
            literal_value(expr->get_child(0), &c)) {
            return c;
        }
    }
    return std::nullopt;
}

struct RuntimeColumnPredicateBuilder {
    template <LogicalType ltype>
    StatusOr<std::vector<const ColumnPredicate*>> operator()(const ColumnIdToGlobalDictMap* global_dictmaps,
                                                             PredicateParser* parser,
                                                             const RuntimeFilterProbeDescriptor* desc,
                                                             const SlotDescriptor* slot, int32_t driver_sequence,
                                                             ObjectPool* pool) {
        // Unsupported probe expression types fall back to the original runtime filter.
        if constexpr (ltype == TYPE_TIME || ltype == TYPE_NULL || ltype == TYPE_JSON || ltype == TYPE_VARIANT ||
                      lt_is_float<ltype> || lt_is_binary<ltype>) {
            return std::vector<const ColumnPredicate*>{};
        } else {
            std::vector<const ColumnPredicate*> preds;
            constexpr LogicalType mapping_type = ltype == TYPE_CHAR ? TYPE_VARCHAR : ltype;
            const RuntimeFilter* rf = desc->runtime_filter(driver_sequence);
            ExprContext* probe_expr_ctx = desc->probe_expr_ctx();
            Expr* probe_expr = probe_expr_ctx->root();

            // Offset of an additive probe expression, in the column's own value space; zero for a
            // plain slot ref. Carrying it into the range built below is all the inversion takes.
            typename RunTimeTypeLimits<ltype>::value_type key_offset{};
            bool has_key_offset = false;

            if (!probe_expr->is_slotref()) {
                if (!probe_expr->is_monotonic()) return preds;

                // The FE marks arithmetic monotonic from the expression's shape alone, but machine
                // integers wrap, and zone_map_filter skips a segment from the expression's value at
                // the zone's two ends -- one row at BIGINT_MIN makes `a - 4000` positive there and
                // takes the whole segment with it (#76720, d3002abafa9). Invert the bounds onto the
                // column instead, and only for shapes computed in the column's own width: ltype is
                // the expression's type (see _get_predicates()), so a promotion means the join
                // wraps in a width the column does not have.
                if (auto offset = probe_expr_offset<typename RunTimeTypeLimits<ltype>::value_type>(
                            probe_expr, slot->id(), probe_expr_ctx);
                    offset.has_value() && slot->type().type == ltype) {
                    key_offset = *offset;
                    has_key_offset = true;
                }
            }

            if (!probe_expr->is_slotref() && !has_key_offset) {
                // year() keeps the pushdown: total, and its 0..9999 result cannot overflow.
                if (probe_expr->node_type() != TExprNodeType::FUNCTION_CALL) return preds;

                // Skip index filtering if the column referenced by the expr is dict encoded.
                if (global_dictmaps != nullptr &&
                    global_dictmaps->find(parser->column_id(*slot)) != global_dictmaps->end()) {
                    return preds;
                }

                const auto* filter = down_cast<const MinMaxRuntimeFilter<mapping_type>*>(rf->get_min_max_filter());
                if (filter == nullptr || filter->is_empty_range() || rf->has_null()) return preds;

                using DecoderType = DummyDecoder<typename RunTimeTypeTraits<mapping_type>::CppType>;
                DecoderType decoder(nullptr);
                MinMaxParser<MinMaxRuntimeFilter<mapping_type>, DecoderType> minmax_parser(filter, &decoder);

                RuntimeState* state = probe_expr_ctx->runtime_state();
                const TypeDescriptor& probe_type = probe_expr->type();

                auto add_predicate = [&](ColumnPtr bound, TExprOpcode::type opcode) -> Status {
                    TExprNode node;
                    node.node_type = TExprNodeType::BINARY_PRED;
                    node.type = TypeDescriptor(TYPE_BOOLEAN).to_thrift();
                    node.child_type = to_thrift(ltype);
                    node.__set_opcode(opcode);

                    Expr* root = pool->add(VectorizedBinaryPredicateFactory::from_thrift(node));
                    root->add_child(Expr::copy(pool, probe_expr));
                    root->add_child(pool->add(new VectorizedLiteral(std::move(bound), probe_type)));
                    root->set_monotonic(true);

                    auto* expr_ctx = pool->add(new ExprContext(root));
                    RETURN_IF_ERROR(expr_ctx->prepare(state));
                    RETURN_IF_ERROR(expr_ctx->open(state));
                    ASSIGN_OR_RETURN(auto* predicate, parser->parse_expr_ctx(*slot, state, expr_ctx));
                    if (predicate == nullptr) return Status::OK();

                    predicate = pool->add(predicate);
                    predicate->set_index_filter_only(true);
                    preds.emplace_back(predicate);
                    return Status::OK();
                };

                RETURN_IF_ERROR(add_predicate(minmax_parser.template min_const_column<ltype>(probe_type, pool),
                                              filter->left_close_interval() ? TExprOpcode::GE : TExprOpcode::GT));
                RETURN_IF_ERROR(add_predicate(minmax_parser.template max_const_column<ltype>(probe_type, pool),
                                              filter->right_close_interval() ? TExprOpcode::LE : TExprOpcode::LT));
                return preds;
            }

            // Treat tinyint and boolean as int
            constexpr LogicalType limit_type = ltype == TYPE_TINYINT || ltype == TYPE_BOOLEAN ? TYPE_INT : ltype;

            using value_type = typename RunTimeTypeLimits<limit_type>::value_type;
            using RangeType = ColumnValueRange<value_type>;

            const auto col_name = std::string(slot->col_name());
            RangeType full_range(col_name, ltype, RunTimeTypeLimits<ltype>::min_value(),
                                 RunTimeTypeLimits<ltype>::max_value());
            if constexpr (lt_is_decimal<limit_type>) {
                full_range.set_precision(slot->type().precision);
                full_range.set_scale(slot->type().scale);
            }

            RangeType& range = full_range;
            range.set_index_filter_only(true);

            // process agg in runtime-filter
            auto* in_filter = rf->get_in_filter();
            if (in_filter && !has_key_offset) { // the IN values would need shifting as well
                if constexpr (ltype == TYPE_VARCHAR) {
                    auto cid = parser->column_id(*slot);
                    if (global_dictmaps == nullptr || global_dictmaps->find(cid) == global_dictmaps->end()) {
                        build_in_range<RangeType, limit_type, mapping_type>(range, rf, pool);
                    }
                } else {
                    build_in_range<RangeType, limit_type, mapping_type>(range, rf, pool);
                }
            }

            // applied global-dict optimized column
            auto* minmax = rf->get_min_max_filter();
            if (minmax) {
                if constexpr (ltype == TYPE_VARCHAR) {
                    auto cid = parser->column_id(*slot);
                    if (auto iter = global_dictmaps->find(cid); iter != global_dictmaps->end()) {
                        build_minmax_range<RangeType, limit_type, LowCardDictType, GlobalDictCodeDecoder>(
                                range, minmax, pool, {}, iter->second);
                    } else {
                        build_minmax_range<RangeType, limit_type, mapping_type, DummyDecoder>(range, minmax, pool, {},
                                                                                              nullptr);
                    }
                } else {
                    build_minmax_range<RangeType, limit_type, mapping_type, DummyDecoder>(
                            range, minmax, pool,
                            static_cast<typename RunTimeTypeTraits<limit_type>::CppType>(key_offset), nullptr);
                }
            }

            std::vector<OlapCondition> filters;
            range.to_olap_filter(filters);

            // if runtime filter generate an empty range we could return directly
            if (range.is_empty_value_range()) {
                if (rf->has_null()) {
                    std::vector<const ColumnPredicate*> new_preds;
                    TypeInfoPtr type = get_type_info(limit_type, slot->type().precision, slot->type().scale);
                    auto column_id = parser->column_id(*slot);
                    ColumnPredicate* null_pred = pool->add(new_column_null_predicate(type, column_id, true));
                    new_preds.emplace_back(null_pred);
                    return new_preds;
                } else {
                    return Status::EndOfFile("EOF, Filter by always false runtime filter");
                }
            }

            for (auto& f : filters) {
                ASSIGN_OR_RETURN(auto p, parser->parse_thrift_cond(f));
                p = pool->add(p);
                VLOG(2) << "build runtime predicate:" << p->debug_string();
                p->set_index_filter_only(f.is_index_filter_only);
                preds.emplace_back(p);
            }

            if (rf->has_null() && !preds.empty()) {
                std::vector<const ColumnPredicate*> new_preds;
                auto type = preds[0]->type_info_ptr();
                auto column_id = preds[0]->column_id();

                ColumnAndPredicate* and_pred = pool->add(new ColumnAndPredicate(type, column_id));
                and_pred->add_child(preds.begin(), preds.end());

                ColumnPredicate* null_pred = pool->add(new_column_null_predicate(type, column_id, true));

                ColumnOrPredicate* or_pred = pool->add(new ColumnOrPredicate(type, column_id));
                or_pred->add_child(and_pred);
                or_pred->add_child(null_pred);
                new_preds.emplace_back(or_pred);

                return new_preds;
            } else {
                return preds;
            }
        }
    }

    template <class InputType>
    struct DummyDecoder {
        DummyDecoder(std::nullptr_t) {}
        auto decode(InputType input) const { return input; }
    };

    template <class InputType>
    struct GlobalDictCodeDecoder {
        GlobalDictCodeDecoder(const GlobalDictMap* dict_map) : _dict_map(dict_map) {}
        Slice decode(DictId input) const {
            for (const auto& [k, v] : *_dict_map) {
                if (v == input) {
                    return k;
                }
            }
            if (input < 0) {
                return Slice::min_value();
            } else {
                return Slice::max_value();
            }
        }

    private:
        const GlobalDictMap* _dict_map;
    };

    template <class RuntimeFilter, class Decoder>
    struct MinMaxParser {
        MinMaxParser(const RuntimeFilter* runtime_filter_, Decoder* decoder)
                : runtime_filter(runtime_filter_), decoder(decoder) {}
        auto min_value(ObjectPool* pool) {
            auto code = runtime_filter->min_value(pool);
            return decoder->decode(code);
        }
        auto max_value(ObjectPool* pool) {
            auto code = runtime_filter->max_value(pool);
            return decoder->decode(code);
        }

        template <LogicalType Type>
        ColumnPtr min_const_column(const TypeDescriptor& col_type, ObjectPool* pool) {
            auto min_decode_value = min_value(pool);
            if constexpr (lt_is_decimal<Type>) {
                return ColumnHelper::create_const_decimal_column<Type>(min_decode_value, col_type.precision,
                                                                       col_type.scale, 1);
            } else {
                return ColumnHelper::create_const_column<Type>(min_decode_value, 1);
            }
        }

        template <LogicalType Type>
        ColumnPtr max_const_column(const TypeDescriptor& col_type, ObjectPool* pool) {
            auto max_decode_value = max_value(pool);
            if constexpr (lt_is_decimal<Type>) {
                return ColumnHelper::create_const_decimal_column<Type>(max_decode_value, col_type.precision,
                                                                       col_type.scale, 1);
            } else {
                return ColumnHelper::create_const_column<Type>(max_decode_value, 1);
            }
        }

    private:
        const RuntimeFilter* runtime_filter;
        const Decoder* decoder;
    };

    template <class Range, LogicalType SlotType, LogicalType mapping_type>
    static void build_in_range(Range& range, const RuntimeFilter* rf, ObjectPool* pool) {
        auto* filter = down_cast<const InRuntimeFilter<mapping_type>*>(rf->get_in_filter());
        if (filter == nullptr) return;
        auto hash_set = filter->get_set(pool);
        boost::container::flat_set<typename Range::RangeValueType> values(hash_set.begin(), hash_set.end());
        (void)range.add_fixed_values(FILTER_IN, values);
    }

    // `offset` moves the filter's bounds out of the probe key's space and back onto the column, for
    // a probe expression of `slot + offset`; it is zero for a plain slot ref. The shift is checked:
    // when it overflows the preimage wraps around the type and is no longer an interval, so neither
    // bound is added and the range stays open. Pruning on a wrapped preimage would drop rows the
    // join really does match -- the join compares wrapped values too.
    template <class Range, LogicalType SlotType, LogicalType mapping_type, template <class> class Decoder,
              class... Args>
    static void build_minmax_range(Range& range, const RuntimeFilter* rf, ObjectPool* pool,
                                   typename RunTimeTypeTraits<SlotType>::CppType offset, Args&&... args) {
        using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;

        auto* filter = down_cast<const MinMaxRuntimeFilter<mapping_type>*>(rf->get_min_max_filter());
        if (filter == nullptr) return;
        using DecoderType = Decoder<typename RunTimeTypeTraits<mapping_type>::CppType>;
        DecoderType decoder(std::forward<Args>(args)...);
        MinMaxParser<MinMaxRuntimeFilter<mapping_type>, DecoderType> parser(filter, &decoder);
        if (filter->is_empty_range()) {
            range.clear_to_empty();
            return;
        }

        SQLFilterOp min_op;
        if (filter->left_close_interval()) {
            min_op = to_olap_filter_type(TExprOpcode::GE, false);
        } else {
            min_op = to_olap_filter_type(TExprOpcode::GT, false);
        }
        auto min_value = static_cast<ValueType>(parser.min_value(pool));

        SQLFilterOp max_op;
        if (filter->right_close_interval()) {
            max_op = to_olap_filter_type(TExprOpcode::LE, false);
        } else {
            max_op = to_olap_filter_type(TExprOpcode::LT, false);
        }

        auto max_value = static_cast<ValueType>(parser.max_value(pool));

        if constexpr (std::is_integral_v<ValueType>) {
            // Both or neither: with one bound shifted the range would describe the wrong half of a
            // preimage that has wrapped into two pieces.
            ValueType shifted_min{};
            ValueType shifted_max{};
            if (__builtin_sub_overflow(min_value, offset, &shifted_min)) return;
            if (__builtin_sub_overflow(max_value, offset, &shifted_max)) return;
            min_value = shifted_min;
            max_value = shifted_max;
        }

        (void)range.add_range(min_op, min_value);
        (void)range.add_range(max_op, max_value);
    }
};
} // namespace detail

inline Status RuntimeScanRangePruner::_update(const ColumnIdToGlobalDictMap* global_dictmaps,
                                              RuntimeFilterArrivedCallBack&& updater, bool force,
                                              size_t raw_read_rows) {
    if (_arrived_runtime_filters_masks.empty()) {
        return Status::OK();
    }
    for (size_t i = 0; i < _arrived_runtime_filters_masks.size(); ++i) {
        // 1. runtime filter arrived
        // 2. runtime filter updated and read rows greater than rf_update_threhold
        // we will filter by index
        if (auto rf = _unarrived_runtime_filters[i]->runtime_filter(_driver_sequence)) {
            size_t rf_version = rf->rf_version();
            if (!_arrived_runtime_filters_masks[i] ||
                (rf_version > _rf_versions[i] && (force || raw_read_rows - _raw_read_rows > rf_update_threshold))) {
                ObjectPool pool;

                ASSIGN_OR_RETURN(auto predicates, _get_predicates(global_dictmaps, i, &pool));
                if (!predicates.empty()) {
                    RETURN_IF_ERROR(updater(predicates.front()->column_id(), predicates));
                }
                _arrived_runtime_filters_masks[i] = true;
                _rf_versions[i] = rf_version;
                _raw_read_rows = raw_read_rows;
            }
        }
    }

    return Status::OK();
}

inline auto RuntimeScanRangePruner::_get_predicates(const ColumnIdToGlobalDictMap* global_dictmaps, size_t idx,
                                                    ObjectPool* pool) -> StatusOr<PredicatesRawPtrs> {
    // convert to olap filter
    auto slot_desc = _slot_descs[idx];
    auto* desc = _unarrived_runtime_filters[idx];
    // Slot-ref probes dispatch on the slot type: a dict encoded column's probe expr is typed
    // as dict code, while dict decoding lives in the string-type branch.
    const LogicalType dispatch_type =
            desc->probe_expr_ctx()->root()->is_slotref() ? slot_desc->type().type : desc->probe_expr_type();
    return type_dispatch_predicate<StatusOr<PredicatesRawPtrs>>(
            dispatch_type, false, detail::RuntimeColumnPredicateBuilder(), global_dictmaps, _parser, desc, slot_desc,
            _driver_sequence, pool);
}

inline void RuntimeScanRangePruner::_init(const UnarrivedRuntimeFilterList& params) {
    for (size_t i = 0; i < params.slot_descs.size(); ++i) {
        if (_parser->can_pushdown(params.slot_descs[i])) {
            _unarrived_runtime_filters.emplace_back(params.unarrived_runtime_filters[i]);
            _slot_descs.emplace_back(params.slot_descs[i]);
            _arrived_runtime_filters_masks.emplace_back();
            _rf_versions.emplace_back();
            _driver_sequence = params.driver_sequence;
        }
    }
}

} // namespace starrocks
