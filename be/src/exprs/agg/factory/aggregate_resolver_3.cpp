// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/distinct.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/sum.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using DistinctStateTrait = DistinctAggregateState<pt, SumResultPT<pt>>;
template <PrimitiveType pt>
inline constexpr PrimitiveType CountResult = TYPE_BIGINT;

template <PrimitiveType pt>
struct DistinctBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeCountDistinctAggregateFunction<pt>(); }
};
template <PrimitiveType pt>
struct DistinctSumBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeSumDistinctAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
using DistinctState2Trait = DistinctAggregateStateV2<pt, SumResultPT<pt>>;

template <PrimitiveType pt>
struct DistinctBuilder2 {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeCountDistinctAggregateFunctionV2<pt>(); }
};
template <PrimitiveType pt>
struct DistinctSumBuilder2 {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeSumDistinctAggregateFunctionV2<pt>(); }
};

template <PrimitiveType pt>
using SumStateTrait = SumAggregateState<RunTimeCppType<SumResultPT<pt>>>;
template <PrimitiveType pt>
using SumResultTrait = RunTimeCppType<SumResultPT<pt>>;
template <PrimitiveType pt>
struct SumBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeSumAggregateFunction<pt>(); }
};

void AggregateFuncResolver::register_3() {
    AGGREGATE_ALL_TYPE_FROM_TRAIT("multi_distinct_count", false, CountResult, DistinctStateTrait, DistinctBuilder);
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                          false);

    AGGREGATE_ALL_TYPE_FROM_TRAIT("multi_distinct_count2", false, CountResult, DistinctState2Trait, DistinctBuilder2);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                            false);
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                         false);

    AGGREGATE_ALL_TYPE_FROM_TRAIT("multi_distinct_sum", false, SumResultPT, DistinctStateTrait, DistinctSumBuilder);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                              false);

    AGGREGATE_ALL_TYPE_FROM_TRAIT("multi_distinct_sum2", false, SumResultPT, DistinctState2Trait, DistinctSumBuilder2);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                                false);

    AGGREGATE_NUMERIC1_TYPE_FROM_TRAIT("sum", true, SumResultPT, SumStateTrait, SumBuilder);
    add_aggregate_mapping<TYPE_DECIMAL32, SumResultPT<TYPE_DECIMAL32>, SumStateTrait, SumBuilder>("sum", true);
    add_aggregate_mapping<TYPE_DECIMAL64, SumResultPT<TYPE_DECIMAL64>, SumStateTrait, SumBuilder>("sum", true);
    add_aggregate_mapping<TYPE_DECIMAL128, SumResultPT<TYPE_DECIMAL128>, SumStateTrait, SumBuilder>("sum", true);
}
} // namespace starrocks::vectorized