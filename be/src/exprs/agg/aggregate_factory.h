// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exprs/agg/aggregate.h"
#include "runtime/primitive_type.h"

namespace starrocks {
namespace vectorized {

class AggregateFactory {
public:
    // The function should be placed by alphabetical order
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeAvgAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

    static AggregateFunctionPtr MakeBitmapIntersectAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeIntersectCountAggregateFunction();

    static AggregateFunctionPtr MakeCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunction();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunctionV2();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeGroupConcatAggregateFunction();

    static AggregateFunctionPtr MakeCountNullableAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeMaxAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeMinAggregateFunction();

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(AggregateFunctionPtr nested_function);

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function);

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeSumAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeVarianceAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeStddevAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumDistinctAggregateFunction();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumDistinctAggregateFunctionV2();

    static AggregateFunctionPtr MakeDictMergeAggregateFunction();

    // Hyperloglog functions:
    static AggregateFunctionPtr MakeHllUnionAggregateFunction();

    static AggregateFunctionPtr MakeHllUnionCountAggregateFunction();

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeHllNdvAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxAggregateFunction();

    static AggregateFunctionPtr MakePercentileUnionAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeFirstValueWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLastValueWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLeadLagWindowFunction();
};

extern const AggregateFunction* get_aggregate_function(const std::string& name, PrimitiveType arg_type,
                                                       PrimitiveType return_type, bool is_null,
                                                       int agg_func_set_version = 1);

} // namespace vectorized
} // namespace starrocks
