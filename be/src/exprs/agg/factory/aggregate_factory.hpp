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
#include <tuple>
#include <unordered_map>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/array_agg.h"
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
#include "exprs/agg/stream/retract_maxmin.h"
#include "exprs/agg/sum.h"
#include "exprs/agg/variance.h"
#include "exprs/agg/window.h"
#include "exprs/agg/window_funnel.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks {

// TODO(murphy) refactor the factory into a shim style
class AggregateFactory {
public:
    // The function should be placed by alphabetical order
    template <LogicalType LT>
    static auto MakeAvgAggregateFunction() {
        return std::make_shared<AvgAggregateFunction<LT>>();
    }

    template <LogicalType LT>
    static auto MakeDecimalAvgAggregateFunction() {
        return std::make_shared<DecimalAvgAggregateFunction<LT>>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction() {
        return std::make_shared<BitmapUnionIntAggregateFunction<LT>>();
    }

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeBitmapAggAggregateFunction();

    static AggregateFunctionPtr MakeBitmapIntersectAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionCountAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeWindowfunnelAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeIntersectCountAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunction();
    template <LogicalType LT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunctionV2();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeGroupConcatAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountNullableAggregateFunction();

    template <AggExchangePerfType PerfType>
    static AggregateFunctionPtr MakeExchangePerfAggregateFunction() {
        return std::make_shared<ExchangePerfAggregateFunction<PerfType>>();
    }

    static AggregateFunctionPtr MakeArrayAggAggregateFunctionV2() {
        return std::make_shared<ArrayAggAggregateFunctionV2>();
    }

    template <LogicalType LT>
    static auto MakeMaxAggregateFunction();

    template <LogicalType LT>
    static auto MakeMaxByAggregateFunction();

    template <LogicalType LT>
    static auto MakeMinByAggregateFunction();

    template <LogicalType LT>
    static auto MakeMinAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeAnyValueAggregateFunction();

    template <typename NestedState, bool IsWindowFunc, bool IgnoreNull = true,
              typename NestedFunctionPtr = AggregateFunctionPtr>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function);

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function);

    template <LogicalType LT>
    static auto MakeSumAggregateFunction();

    template <LogicalType LT>
    static auto MakeDecimalSumAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static AggregateFunctionPtr MakeVarianceAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static AggregateFunctionPtr MakeStddevAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static AggregateFunctionPtr MakeCovarianceAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeCorelationAggregateFunction();

    template <LogicalType LT>
    static auto MakeSumDistinctAggregateFunction();
    template <LogicalType LT>
    static auto MakeSumDistinctAggregateFunctionV2();
    template <LogicalType LT>
    static auto MakeDecimalSumDistinctAggregateFunction();

    static AggregateFunctionPtr MakeDictMergeAggregateFunction();
    static AggregateFunctionPtr MakeRetentionAggregateFunction();

    // Hyperloglog functions:
    static AggregateFunctionPtr MakeHllUnionAggregateFunction();

    static AggregateFunctionPtr MakeHllUnionCountAggregateFunction();

    template <LogicalType T>
    static AggregateFunctionPtr MakeHllNdvAggregateFunction();

    template <LogicalType T>
    static AggregateFunctionPtr MakeHllRawAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxAggregateFunction();

    static AggregateFunctionPtr MakePercentileUnionAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakePercentileContAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakePercentileDiscAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    static AggregateFunctionPtr MakeCumeDistWindowFunction();

    static AggregateFunctionPtr MakePercentRankWindowFunction();

    static AggregateFunctionPtr MakeNtileWindowFunction();

    template <LogicalType LT, bool ignoreNulls>
    static AggregateFunctionPtr MakeFirstValueWindowFunction() {
        return std::make_shared<FirstValueWindowFunction<LT, ignoreNulls>>();
    }

    template <LogicalType LT, bool ignoreNulls>
    static AggregateFunctionPtr MakeLastValueWindowFunction() {
        return std::make_shared<LastValueWindowFunction<LT, ignoreNulls>>();
    }

    template <LogicalType LT, bool ignoreNulls, bool isLag>
    static AggregateFunctionPtr MakeLeadLagWindowFunction() {
        return std::make_shared<LeadLagWindowFunction<LT, ignoreNulls, isLag>>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeHistogramAggregationFunction() {
        return std::make_shared<HistogramAggregationFunction<LT>>();
    }

    // Stream MV Retractable Agg Functions
    template <LogicalType LT>
    static auto MakeRetractMinAggregateFunction();

    template <LogicalType LT>
    static auto MakeRetractMaxAggregateFunction();
};

// The function should be placed by alphabetical order

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeIntersectCountAggregateFunction() {
    return std::make_shared<IntersectCountAggregateFunction<LT>>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountAggregateFunction() {
    return std::make_shared<CountAggregateFunction<IsWindowFunc>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeWindowfunnelAggregateFunction() {
    return std::make_shared<WindowFunnelAggregateFunction<LT>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeCountDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<LT, AggDistinctType::COUNT>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeCountDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<LT, AggDistinctType::COUNT>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeGroupConcatAggregateFunction() {
    return std::make_shared<GroupConcatAggregateFunction<LT>>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountNullableAggregateFunction() {
    return std::make_shared<CountNullableAggregateFunction<IsWindowFunc>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeMaxAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunction<LT, MaxAggregateData<LT>, MaxElement<LT, MaxAggregateData<LT>>>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeMaxByAggregateFunction() {
    return std::make_shared<
            MaxMinByAggregateFunction<LT, MaxByAggregateData<LT>, MaxByElement<LT, MaxByAggregateData<LT>>>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeMinByAggregateFunction() {
    return std::make_shared<
            MaxMinByAggregateFunction<LT, MinByAggregateData<LT>, MinByElement<LT, MinByAggregateData<LT>>>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeMinAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunction<LT, MinAggregateData<LT>, MinElement<LT, MinAggregateData<LT>>>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeAnyValueAggregateFunction() {
    return std::make_shared<
            AnyValueAggregateFunction<LT, AnyValueAggregateData<LT>, AnyValueElement<LT, AnyValueAggregateData<LT>>>>();
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

template <LogicalType LT>
auto AggregateFactory::MakeSumAggregateFunction() {
    return std::make_shared<SumAggregateFunction<LT>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeDecimalSumAggregateFunction() {
    return std::make_shared<DecimalSumAggregateFunction<LT>>();
}

template <LogicalType LT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeVarianceAggregateFunction() {
    return std::make_shared<VarianceAggregateFunction<LT, is_sample>>();
}

template <LogicalType LT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeStddevAggregateFunction() {
    return std::make_shared<StddevAggregateFunction<LT, is_sample>>();
}

template <LogicalType LT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeCovarianceAggregateFunction() {
    return std::make_shared<CorVarianceAggregateFunction<LT, is_sample>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeCorelationAggregateFunction() {
    return std::make_shared<CorelationAggregateFunction<LT>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeSumDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<LT, AggDistinctType::SUM>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeSumDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<LT, AggDistinctType::SUM>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeDecimalSumDistinctAggregateFunction() {
    return std::make_shared<DecimalDistinctAggregateFunction<LT, AggDistinctType::SUM>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeHllNdvAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<LT, false>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeHllRawAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<LT, true>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakePercentileContAggregateFunction() {
    return std::make_shared<PercentileContAggregateFunction<LT>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakePercentileDiscAggregateFunction() {
    return std::make_shared<PercentileDiscAggregateFunction<PT>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeBitmapAggAggregateFunction() {
    return std::make_shared<BitmapAggAggregateFunction<LT>>();
}

// Stream MV Retractable Aggregate Functions
template <LogicalType LT>
auto AggregateFactory::MakeRetractMinAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunctionRetractable<LT, MinAggregateDataRetractable<LT>,
                                                               MinElement<LT, MinAggregateDataRetractable<LT>>>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeRetractMaxAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunctionRetractable<LT, MaxAggregateDataRetractable<LT>,
                                                               MaxElement<LT, MaxAggregateDataRetractable<LT>>>>();
}

} // namespace starrocks
