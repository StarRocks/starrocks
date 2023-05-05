// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/group_concat.h"
#include "exprs/agg/percentile_cont.h"
#include "runtime/primitive_type.h"
#include "util/percentile_value.h"

namespace starrocks::vectorized {

struct PercentileDiscDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_datetime<pt> || pt_is_date<pt> || pt_is_arithmetic<pt> || pt_is_string<pt> ||
                      pt_is_decimal_of_any_version<pt>) {
            resolver->add_aggregate_mapping_variadic<pt, pt, PercentileState<pt>>(
                    "percentile_disc", false, AggregateFactory::MakePercentileDiscAggregateFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_others() {
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_DOUBLE>("percentile_approx", false,
                                                            AggregateFactory::MakePercentileApproxAggregateFunction());
    add_aggregate_mapping_notnull<TYPE_DOUBLE, TYPE_DOUBLE>("percentile_approx", false,
                                                            AggregateFactory::MakePercentileApproxAggregateFunction());
    add_aggregate_mapping<TYPE_PERCENTILE, TYPE_PERCENTILE, PercentileValue>(
            "percentile_union", false, AggregateFactory::MakePercentileUnionAggregateFunction());

    add_aggregate_mapping_variadic<TYPE_DOUBLE, TYPE_DOUBLE, PercentileState<TYPE_DOUBLE>>(
            "percentile_cont", false, AggregateFactory::MakePercentileContAggregateFunction<TYPE_DOUBLE>());
    add_aggregate_mapping_variadic<TYPE_DATETIME, TYPE_DATETIME, PercentileState<TYPE_DATETIME>>(
            "percentile_cont", false, AggregateFactory::MakePercentileContAggregateFunction<TYPE_DATETIME>());
    add_aggregate_mapping_variadic<TYPE_DATE, TYPE_DATE, PercentileState<TYPE_DATE>>(
            "percentile_cont", false, AggregateFactory::MakePercentileContAggregateFunction<TYPE_DATE>());

    for (auto type : sortable_types()) {
        type_dispatch_all(type, PercentileDiscDispatcher(), this);
    }

    add_aggregate_mapping_variadic<TYPE_CHAR, TYPE_VARCHAR, GroupConcatAggregateState>(
            "group_concat", false, AggregateFactory::MakeGroupConcatAggregateFunction<TYPE_CHAR>());
    add_aggregate_mapping_variadic<TYPE_VARCHAR, TYPE_VARCHAR, GroupConcatAggregateState>(
            "group_concat", false, AggregateFactory::MakeGroupConcatAggregateFunction<TYPE_VARCHAR>());

    add_array_mapping<TYPE_ARRAY, TYPE_VARCHAR>("dict_merge");
    add_array_mapping<TYPE_ARRAY, TYPE_ARRAY>("retention");

    // sum, avg, distinct_sum use decimal128 as intermediate or result type to avoid overflow
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("decimal_multi_distinct_sum");

    // This first type is the 4th type input of windowfunnel.
    // And the 1st type is BigInt, 2nd is datetime, 3rd is mode(default 0).
    add_array_mapping<TYPE_INT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_BIGINT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATETIME, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATE, TYPE_INT>("window_funnel");
}

} // namespace starrocks::vectorized
