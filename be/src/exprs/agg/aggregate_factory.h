// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
    static AggregateFunctionPtr MakeDecimalAvgAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction();

    template <PrimitiveType LT>
    static AggregateFunctionPtr MakeBitmapAggAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

    static AggregateFunctionPtr MakeBitmapIntersectAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeWindowfunnelAggregateFunction();

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

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeAnyValueAggregateFunction();

    template <typename NestedState, bool IgnoreNull = true>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(AggregateFunctionPtr nested_function);

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function);

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeDecimalSumAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeVarianceAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeStddevAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumDistinctAggregateFunction();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumDistinctAggregateFunctionV2();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeDecimalSumDistinctAggregateFunction();

    static AggregateFunctionPtr MakeDictMergeAggregateFunction();
    static AggregateFunctionPtr MakeRetentionAggregateFunction();

    // Hyperloglog functions:
    static AggregateFunctionPtr MakeHllUnionAggregateFunction();

    static AggregateFunctionPtr MakeHllUnionCountAggregateFunction();

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeHllNdvAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxAggregateFunction();

    static AggregateFunctionPtr MakePercentileUnionAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakePercentileContAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeArrayAggAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    static AggregateFunctionPtr MakeNtileWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeFirstValueWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLastValueWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLeadLagWindowFunction();
};

const AggregateFunction* get_aggregate_function(const std::string& name, PrimitiveType arg_type,
                                                PrimitiveType return_type, bool is_null,
                                                TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                                                int func_version = 1);

const AggregateFunction* get_window_function(const std::string& name, PrimitiveType arg_type, PrimitiveType return_type,
                                             bool is_null,
                                             TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                                             int func_version = 1);

} // namespace vectorized
} // namespace starrocks
