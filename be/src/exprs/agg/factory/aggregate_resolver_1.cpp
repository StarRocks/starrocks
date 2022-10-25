// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using AvgStateTrait = AvgAggregateState<AvgResultTrait<pt>>;

template <PrimitiveType pt>
struct AvgBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeAvgAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
struct ArrayAvgBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeArrayAggAggregateFunction<pt>(); }
};

void AggregateFuncResolver::register_1() {
    // TODO(murphy) simplify the register procedure
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64, AvgStateTrait, AvgBuilder>("avg", true);
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, AvgStateTrait, AvgBuilder>("avg", true);

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_INT, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_BIGINT, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_DATETIME, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_DATE, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_CHAR, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
}

} // namespace starrocks::vectorized