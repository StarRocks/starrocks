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
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                          false);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                          false);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                           false);
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                           false);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                          false);
    add_aggregate_mapping<TYPE_DATETIME, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                           false);
    add_aggregate_mapping<TYPE_DATE, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count", false);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                            false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                            false);
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                            false);
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_BIGINT, DistinctStateTrait, DistinctBuilder>("multi_distinct_count",
                                                                                             false);

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                            false);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                            false);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                             false);
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2", false);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                           false);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                             false);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                          false);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                           false);
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                         false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                            false);
    add_aggregate_mapping<TYPE_DATETIME, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                             false);
    add_aggregate_mapping<TYPE_DATE, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                         false);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                              false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                              false);
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                              false);
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_BIGINT, DistinctState2Trait, DistinctBuilder2>("multi_distinct_count2",
                                                                                               false);

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                             false);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                             false);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                              false);
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum", false);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                            false);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                                false);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum", false);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                            false);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                              false);
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                                false);
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum", false);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                                  false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                                   false);
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, DistinctStateTrait, DistinctSumBuilder>("multi_distinct_sum",
                                                                                                   false);
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, DistinctStateTrait, DistinctSumBuilder>(
            "multi_distinct_sum", false);

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                               false);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                               false);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                                false);
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                           false);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                              false);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                                  false);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                             false);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                              false);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                                false);
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2",
                                                                                                  false);
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE, DistinctState2Trait, DistinctSumBuilder2>("multi_distinct_sum2", false);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, DistinctState2Trait, DistinctSumBuilder2>(
            "multi_distinct_sum2", false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, DistinctState2Trait, DistinctSumBuilder2>(
            "multi_distinct_sum2", false);
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL32, DistinctState2Trait, DistinctSumBuilder2>(
            "multi_distinct_sum2", false);
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL32, DistinctState2Trait, DistinctSumBuilder2>(
            "multi_distinct_sum2", false);

    std::string name = "sum";
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64, SumStateTrait, SumBuilder>(name, false);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, SumStateTrait, SumBuilder>(name, false);
}
} // namespace starrocks::vectorized