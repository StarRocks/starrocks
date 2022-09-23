// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "exec/olap_common.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/predicate_parser.h"
#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {
namespace detail {
struct RuntimeColumnPredicateBuilder {
    template <PrimitiveType ptype>
    StatusOr<std::vector<std::unique_ptr<ColumnPredicate>>> operator()(PredicateParser* parser,
                                                                       const RuntimeFilterProbeDescriptor* desc,
                                                                       const SlotDescriptor* slot) {
        if constexpr (ptype == TYPE_TIME || ptype == TYPE_NULL || ptype == TYPE_JSON || pt_is_float<ptype>) {
            CHECK(false) << "unreachable path";
            return Status::NotSupported("unreachable path");
        } else {
            std::vector<std::unique_ptr<ColumnPredicate>> preds;

            // Treat tinyint and boolean as int
            constexpr PrimitiveType limit_type = ptype == TYPE_TINYINT || ptype == TYPE_BOOLEAN ? TYPE_INT : ptype;
            // Map TYPE_CHAR to TYPE_VARCHAR
            constexpr PrimitiveType mapping_type = ptype == TYPE_CHAR ? TYPE_VARCHAR : ptype;

            using value_type = typename RunTimeTypeLimits<limit_type>::value_type;
            using RangeType = ColumnValueRange<value_type>;

            const std::string& col_name = slot->col_name();
            RangeType full_range(col_name, ptype, RunTimeTypeLimits<ptype>::min_value(),
                                 RunTimeTypeLimits<ptype>::max_value());
            if constexpr (pt_is_decimal<limit_type>) {
                full_range.set_precision(slot->type().precision);
                full_range.set_scale(slot->type().scale);
            }

            RangeType& range = full_range;
            range.set_index_filter_only(true);

            const JoinRuntimeFilter* rf = desc->runtime_filter();

            const RuntimeBloomFilter<mapping_type>* filter = down_cast<const RuntimeBloomFilter<mapping_type>*>(rf);

            using ValueType = typename vectorized::RunTimeTypeTraits<mapping_type>::CppType;
            SQLFilterOp min_op = to_olap_filter_type(TExprOpcode::GE, false);
            ValueType min_value = filter->min_value();
            range.add_range(min_op, static_cast<value_type>(min_value));

            SQLFilterOp max_op = to_olap_filter_type(TExprOpcode::LE, false);
            ValueType max_value = filter->max_value();
            range.add_range(max_op, static_cast<value_type>(max_value));

            std::vector<TCondition> filters;
            range.to_olap_filter(filters);

            for (auto& f : filters) {
                std::unique_ptr<ColumnPredicate> p(parser->parse_thrift_cond(f));
                p->set_index_filter_only(f.is_index_filter_only);
                preds.emplace_back(std::move(p));
            }

            return preds;
        }
    }
};
} // namespace detail

inline Status OlapRuntimeScanRangePruner::_update(RuntimeFilterArrivedCallBack&& updater) {
    if (_arrived_runtime_filters_masks.empty()) {
        return Status::OK();
    }
    size_t cnt = 0;
    for (size_t i = 0; i < _arrived_runtime_filters_masks.size(); ++i) {
        if (_arrived_runtime_filters_masks[i] == 0 && _unarrived_runtime_filters[i]->runtime_filter()) {
            ASSIGN_OR_RETURN(auto predicates, _get_predicates(i));
            auto raw_predicates = _as_raw_predicates(predicates);
            if (!raw_predicates.empty()) {
                RETURN_IF_ERROR(updater(raw_predicates.front()->column_id(), raw_predicates));
            }
            _arrived_runtime_filters_masks[i] = true;
        }
        cnt += _arrived_runtime_filters_masks[i];
    }

    // all filters arrived
    if (cnt == _arrived_runtime_filters_masks.size()) {
        _arrived_runtime_filters_masks.clear();
    }
    return Status::OK();
}

inline auto OlapRuntimeScanRangePruner::_get_predicates(size_t idx) -> StatusOr<PredicatesPtrs> {
    auto rf = _unarrived_runtime_filters[idx]->runtime_filter();
    if (rf->has_null()) return PredicatesPtrs{};
    // convert to olap filter
    auto slot_desc = _slot_descs[idx];
    return type_dispatch_predicate<StatusOr<PredicatesPtrs>>(slot_desc->type().type, false,
                                                             detail::RuntimeColumnPredicateBuilder(), _parser,
                                                             _unarrived_runtime_filters[idx], slot_desc);
}

inline auto OlapRuntimeScanRangePruner::_as_raw_predicates(
        const std::vector<std::unique_ptr<ColumnPredicate>>& predicates) -> PredicatesRawPtrs {
    PredicatesRawPtrs res;
    for (auto& predicate : predicates) {
        res.push_back(predicate.get());
    }
    return res;
}

inline void OlapRuntimeScanRangePruner::_init(const UnarrivedRuntimeFilterList& params) {
    for (size_t i = 0; i < params.slot_descs.size(); ++i) {
        if (_parser->can_pushdown(params.slot_descs[i])) {
            _unarrived_runtime_filters.emplace_back(params.unarrived_runtime_filters[i]);
            _slot_descs.emplace_back(params.slot_descs[i]);
            _arrived_runtime_filters_masks.emplace_back();
        }
    }
}

} // namespace starrocks::vectorized
