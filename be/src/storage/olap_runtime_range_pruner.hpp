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

#include <memory>

#include "exec/olap_common.h"
#include "exprs/runtime_filter_bank.h"
#include "storage/column_predicate.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/predicate_parser.h"

namespace starrocks {
namespace detail {
struct RuntimeColumnPredicateBuilder {
    template <LogicalType ptype>
    StatusOr<std::vector<std::unique_ptr<ColumnPredicate>>> operator()(PredicateParser* parser,
                                                                       const RuntimeFilterProbeDescriptor* desc,
                                                                       const SlotDescriptor* slot) {
        // keep consistent with ColumnRangeBuilder
        if constexpr (ptype == TYPE_TIME || ptype == TYPE_NULL || ptype == TYPE_JSON || pt_is_float<ptype> ||
                      pt_is_binary<ptype>) {
            CHECK(false) << "unreachable path";
            return Status::NotSupported("unreachable path");
        } else {
            std::vector<std::unique_ptr<ColumnPredicate>> preds;

            // Treat tinyint and boolean as int
            constexpr LogicalType limit_type = ptype == TYPE_TINYINT || ptype == TYPE_BOOLEAN ? TYPE_INT : ptype;
            // Map TYPE_CHAR to TYPE_VARCHAR
            constexpr LogicalType mapping_type = ptype == TYPE_CHAR ? TYPE_VARCHAR : ptype;

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

            using ValueType = typename RunTimeTypeTraits<mapping_type>::CppType;
            SQLFilterOp min_op;
            if (filter->left_open_interval()) {
                min_op = to_olap_filter_type(TExprOpcode::GE, false);
            } else {
                min_op = to_olap_filter_type(TExprOpcode::GT, false);
            }
            ValueType min_value = filter->min_value();
            range.add_range(min_op, static_cast<value_type>(min_value));

            SQLFilterOp max_op;
            if (filter->right_open_interval()) {
                max_op = to_olap_filter_type(TExprOpcode::LE, false);
            } else {
                max_op = to_olap_filter_type(TExprOpcode::LT, false);
            }
            ValueType max_value = filter->max_value();
            range.add_range(max_op, static_cast<value_type>(max_value));

            std::vector<TCondition> filters;
            range.to_olap_filter(filters);

            // if runtime filter generate an empty range we could return directly
            if (range.is_empty_value_range()) {
                return Status::EndOfFile("EOF, Filter by always false runtime filter");
            }

            for (auto& f : filters) {
                std::unique_ptr<ColumnPredicate> p(parser->parse_thrift_cond(f));
                VLOG(1) << "build runtime predicate:" << p->debug_string();
                p->set_index_filter_only(f.is_index_filter_only);
                preds.emplace_back(std::move(p));
            }

            return preds;
        }
    }
};
} // namespace detail

inline Status OlapRuntimeScanRangePruner::_update(RuntimeFilterArrivedCallBack&& updater, size_t raw_read_rows) {
    if (_arrived_runtime_filters_masks.empty()) {
        return Status::OK();
    }
    for (size_t i = 0; i < _arrived_runtime_filters_masks.size(); ++i) {
        if (auto rf = _unarrived_runtime_filters[i]->runtime_filter()) {
            size_t rf_version = rf->rf_version();
            if (_arrived_runtime_filters_masks[i] == 0 ||
                (rf_version > _rf_versions[i] && raw_read_rows - _raw_read_rows > rf_update_threhold)) {
                ASSIGN_OR_RETURN(auto predicates, _get_predicates(i));
                auto raw_predicates = _as_raw_predicates(predicates);
                if (!raw_predicates.empty()) {
                    RETURN_IF_ERROR(updater(raw_predicates.front()->column_id(), raw_predicates));
                }
                _arrived_runtime_filters_masks[i] = true;
                _rf_versions[i] = rf_version;
                _raw_read_rows = raw_read_rows;
            }
        }
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
            _rf_versions.emplace_back();
        }
    }
}

} // namespace starrocks
