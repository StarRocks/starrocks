// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <tuple>
#include <unordered_map>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/bitmap_agg.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/bitmap_union.h"
#include "exprs/agg/bitmap_union_count.h"
#include "exprs/agg/bitmap_union_int.h"
#include "exprs/agg/count.h"
#include "exprs/agg/covariance.h"
#include "exprs/agg/distinct.h"
#include "exprs/agg/exchange_perf.h"
#include "exprs/agg/group_concat.h"
#include "exprs/agg/histogram.h"
#include "exprs/agg/hll_ndv.h"
#include "exprs/agg/hll_union.h"
#include "exprs/agg/hll_union_count.h"
#include "exprs/agg/intersect_count.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/maxmin_by.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/percentile_approx.h"
#include "exprs/agg/percentile_cont.h"
#include "exprs/agg/percentile_union.h"
#include "exprs/agg/retention.h"
#include "exprs/agg/sum.h"
#include "exprs/agg/variance.h"
#include "exprs/agg/window.h"
#include "exprs/agg/window_funnel.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks::vectorized {

// TODO(murphy) refactor the factory into a shim style
class AggregateFactory {
public:
    // The function should be placed by alphabetical order
    template <PrimitiveType PT>
    static auto MakeAvgAggregateFunction() {
        return std::make_shared<AvgAggregateFunction<PT>>();
    }

    template <PrimitiveType PT>
    static auto MakeDecimalAvgAggregateFunction() {
        return std::make_shared<DecimalAvgAggregateFunction<PT>>();
    }

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction() {
        return std::make_shared<BitmapUnionIntAggregateFunction<PT>>();
    }

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

    template <PrimitiveType LT>
    static AggregateFunctionPtr MakeBitmapAggAggregateFunction();

    static AggregateFunctionPtr MakeBitmapIntersectAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeWindowfunnelAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeIntersectCountAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunction();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunctionV2();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeGroupConcatAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountNullableAggregateFunction();

    template <AggExchangePerfType PerfType>
    static AggregateFunctionPtr MakeExchangePerfAggregateFunction() {
        return std::make_shared<ExchangePerfAggregateFunction<PerfType>>();
    }

    template <PrimitiveType PT>
    static auto MakeMaxAggregateFunction();

    template <PrimitiveType PT>
    static auto MakeMaxByAggregateFunction();

    template <PrimitiveType PT>
    static auto MakeMinAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeAnyValueAggregateFunction();

    template <typename NestedState, bool IsWindowFunc, bool IgnoreNull = true,
              typename NestedFunctionPtr = AggregateFunctionPtr>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function);

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function);

    template <PrimitiveType PT>
    static auto MakeSumAggregateFunction();

    template <PrimitiveType PT>
    static auto MakeDecimalSumAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeVarianceAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeStddevAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeCovarianceAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCorelationAggregateFunction();

    template <PrimitiveType PT>
    static auto MakeSumDistinctAggregateFunction();
    template <PrimitiveType PT>
    static auto MakeSumDistinctAggregateFunctionV2();
    template <PrimitiveType PT>
    static auto MakeDecimalSumDistinctAggregateFunction();

    static AggregateFunctionPtr MakeDictMergeAggregateFunction();
    static AggregateFunctionPtr MakeRetentionAggregateFunction();

    // Hyperloglog functions:
    static AggregateFunctionPtr MakeHllUnionAggregateFunction();

    static AggregateFunctionPtr MakeHllUnionCountAggregateFunction();

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeHllNdvAggregateFunction();

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeHllRawAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxAggregateFunction();

    static AggregateFunctionPtr MakePercentileUnionAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakePercentileContAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakePercentileDiscAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    static AggregateFunctionPtr MakeNtileWindowFunction();

    template <PrimitiveType PT, bool ignoreNulls>
    static AggregateFunctionPtr MakeFirstValueWindowFunction() {
        return std::make_shared<FirstValueWindowFunction<PT, ignoreNulls>>();
    }

    template <PrimitiveType PT, bool ignoreNulls>
    static AggregateFunctionPtr MakeLastValueWindowFunction() {
        return std::make_shared<LastValueWindowFunction<PT, ignoreNulls>>();
    }

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLeadLagWindowFunction() {
        return std::make_shared<LeadLagWindowFunction<PT>>();
    }

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeHistogramAggregationFunction() {
        return std::make_shared<HistogramAggregationFunction<PT>>();
    }
};

// The function should be placed by alphabetical order

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeIntersectCountAggregateFunction() {
    return std::make_shared<IntersectCountAggregateFunction<PT>>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountAggregateFunction() {
    return std::make_shared<CountAggregateFunction<IsWindowFunc>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeWindowfunnelAggregateFunction() {
    return std::make_shared<WindowFunnelAggregateFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeCountDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<PT, AggDistinctType::COUNT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeCountDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<PT, AggDistinctType::COUNT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeGroupConcatAggregateFunction() {
    return std::make_shared<GroupConcatAggregateFunction<PT>>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountNullableAggregateFunction() {
    return std::make_shared<CountNullableAggregateFunction<IsWindowFunc>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeMaxAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunction<PT, MaxAggregateData<PT>, MaxElement<PT, MaxAggregateData<PT>>>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeMaxByAggregateFunction() {
    return std::make_shared<
            MaxByAggregateFunction<PT, MaxByAggregateData<PT>, MaxByElement<PT, MaxByAggregateData<PT>>>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeMinAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunction<PT, MinAggregateData<PT>, MinElement<PT, MinAggregateData<PT>>>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeAnyValueAggregateFunction() {
    return std::make_shared<
            AnyValueAggregateFunction<PT, AnyValueAggregateData<PT>, AnyValueElement<PT, AnyValueAggregateData<PT>>>>();
}

template <typename NestedState, bool IsWindowFunc, bool IgnoreNull, typename NestedFunctionPtr>
AggregateFunctionPtr AggregateFactory::MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function) {
    using AggregateDataType = NullableAggregateFunctionState<NestedState, IsWindowFunc>;
    return std::make_shared<
            NullableAggregateFunctionUnary<NestedFunctionPtr, AggregateDataType, IsWindowFunc, IgnoreNull>>(
            nested_function);
}

template <typename NestedState>
AggregateFunctionPtr AggregateFactory::MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function) {
    using AggregateDataType = NullableAggregateFunctionState<NestedState, false>;
    return std::make_shared<NullableAggregateFunctionVariadic<AggregateDataType>>(nested_function);
}

template <PrimitiveType PT>
auto AggregateFactory::MakeSumAggregateFunction() {
    return std::make_shared<SumAggregateFunction<PT>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeDecimalSumAggregateFunction() {
    return std::make_shared<DecimalSumAggregateFunction<PT>>();
}

template <PrimitiveType PT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeVarianceAggregateFunction() {
    return std::make_shared<VarianceAggregateFunction<PT, is_sample>>();
}

template <PrimitiveType PT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeStddevAggregateFunction() {
    return std::make_shared<StddevAggregateFunction<PT, is_sample>>();
}

template <PrimitiveType PT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeCovarianceAggregateFunction() {
    return std::make_shared<CorVarianceAggregateFunction<PT, is_sample>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeCorelationAggregateFunction() {
    return std::make_shared<CorelationAggregateFunction<PT>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeSumDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<PT, AggDistinctType::SUM>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeSumDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<PT, AggDistinctType::SUM>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeDecimalSumDistinctAggregateFunction() {
    return std::make_shared<DecimalDistinctAggregateFunction<PT, AggDistinctType::SUM>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeHllNdvAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<PT, false>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeHllRawAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<PT, true>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakePercentileContAggregateFunction() {
    return std::make_shared<PercentileContAggregateFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakePercentileDiscAggregateFunction() {
    return std::make_shared<PercentileDiscAggregateFunction<PT>>();
}

<<<<<<< HEAD
template <PrimitiveType LT>
AggregateFunctionPtr AggregateFactory::MakeBitmapAggAggregateFunction() {
    return std::make_shared<BitmapAggAggregateFunction<LT>>();
}

} // namespace starrocks::vectorized
=======
} // namespace starrocks::vectorized
>>>>>>> branch-2.5-mrs
