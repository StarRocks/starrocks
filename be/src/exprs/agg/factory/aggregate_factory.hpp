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
#include "exprs/agg/avg.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/bitmap_union.h"
#include "exprs/agg/bitmap_union_count.h"
#include "exprs/agg/bitmap_union_int.h"
#include "exprs/agg/count.h"
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
    template <LogicalType PT>
    static auto MakeAvgAggregateFunction() {
        return std::make_shared<AvgAggregateFunction<PT>>();
    }

    template <LogicalType PT>
    static auto MakeDecimalAvgAggregateFunction() {
        return std::make_shared<DecimalAvgAggregateFunction<PT>>();
    }

    template <LogicalType PT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction() {
        return std::make_shared<BitmapUnionIntAggregateFunction<PT>>();
    }

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

    static AggregateFunctionPtr MakeBitmapIntersectAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionCountAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakeWindowfunnelAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakeIntersectCountAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunction();
    template <LogicalType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunctionV2();

    template <LogicalType PT>
    static AggregateFunctionPtr MakeGroupConcatAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountNullableAggregateFunction();

    template <AggExchangePerfType PerfType>
    static AggregateFunctionPtr MakeExchangePerfAggregateFunction() {
        return std::make_shared<ExchangePerfAggregateFunction<PerfType>>();
    }

    template <LogicalType PT>
    static auto MakeMaxAggregateFunction();

    template <LogicalType PT>
    static auto MakeMaxByAggregateFunction();

    template <LogicalType PT>
    static auto MakeMinAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakeAnyValueAggregateFunction();

    template <typename NestedState, bool IsWindowFunc, bool IgnoreNull = true,
              typename NestedFunctionPtr = AggregateFunctionPtr>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function);

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function);

    template <LogicalType PT>
    static auto MakeSumAggregateFunction();

    template <LogicalType PT>
    static auto MakeDecimalSumAggregateFunction();

    template <LogicalType PT, bool is_sample>
    static AggregateFunctionPtr MakeVarianceAggregateFunction();

    template <LogicalType PT, bool is_sample>
    static AggregateFunctionPtr MakeStddevAggregateFunction();

    template <LogicalType PT>
    static auto MakeSumDistinctAggregateFunction();
    template <LogicalType PT>
    static auto MakeSumDistinctAggregateFunctionV2();
    template <LogicalType PT>
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

    template <LogicalType PT>
    static AggregateFunctionPtr MakePercentileContAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    static AggregateFunctionPtr MakeNtileWindowFunction();

    template <LogicalType PT, bool ignoreNulls>
    static AggregateFunctionPtr MakeFirstValueWindowFunction() {
        return std::make_shared<FirstValueWindowFunction<PT, ignoreNulls>>();
    }

    template <LogicalType PT, bool ignoreNulls>
    static AggregateFunctionPtr MakeLastValueWindowFunction() {
        return std::make_shared<LastValueWindowFunction<PT, ignoreNulls>>();
    }

    template <LogicalType PT, bool ignoreNulls, bool isLag>
    static AggregateFunctionPtr MakeLeadLagWindowFunction() {
        return std::make_shared<LeadLagWindowFunction<PT, ignoreNulls, isLag>>();
    }

    template <LogicalType PT>
    static AggregateFunctionPtr MakeHistogramAggregationFunction() {
        return std::make_shared<HistogramAggregationFunction<PT>>();
    }

    // Stream MV Retractable Agg Functions
    template <LogicalType PT>
    static auto MakeRetractMinAggregateFunction();

    template <LogicalType PT>
    static auto MakeRetractMaxAggregateFunction();
};

// The function should be placed by alphabetical order

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeIntersectCountAggregateFunction() {
    return std::make_shared<IntersectCountAggregateFunction<PT>>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountAggregateFunction() {
    return std::make_shared<CountAggregateFunction<IsWindowFunc>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeWindowfunnelAggregateFunction() {
    return std::make_shared<WindowFunnelAggregateFunction<PT>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeCountDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<PT, AggDistinctType::COUNT>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeCountDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<PT, AggDistinctType::COUNT>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeGroupConcatAggregateFunction() {
    return std::make_shared<GroupConcatAggregateFunction<PT>>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountNullableAggregateFunction() {
    return std::make_shared<CountNullableAggregateFunction<IsWindowFunc>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeMaxAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunction<PT, MaxAggregateData<PT>, MaxElement<PT, MaxAggregateData<PT>>>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeMaxByAggregateFunction() {
    return std::make_shared<
            MaxByAggregateFunction<PT, MaxByAggregateData<PT>, MaxByElement<PT, MaxByAggregateData<PT>>>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeMinAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunction<PT, MinAggregateData<PT>, MinElement<PT, MinAggregateData<PT>>>>();
}

template <LogicalType PT>
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

template <LogicalType PT>
auto AggregateFactory::MakeSumAggregateFunction() {
    return std::make_shared<SumAggregateFunction<PT>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeDecimalSumAggregateFunction() {
    return std::make_shared<DecimalSumAggregateFunction<PT>>();
}

template <LogicalType PT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeVarianceAggregateFunction() {
    return std::make_shared<VarianceAggregateFunction<PT, is_sample>>();
}

template <LogicalType PT, bool is_sample>
AggregateFunctionPtr AggregateFactory::MakeStddevAggregateFunction() {
    return std::make_shared<StddevAggregateFunction<PT, is_sample>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeSumDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<PT, AggDistinctType::SUM>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeSumDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<PT, AggDistinctType::SUM>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeDecimalSumDistinctAggregateFunction() {
    return std::make_shared<DecimalDistinctAggregateFunction<PT, AggDistinctType::SUM>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeHllNdvAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<PT, false>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeHllRawAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<PT, true>>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakePercentileContAggregateFunction() {
    return std::make_shared<PercentileContAggregateFunction<PT>>();
}

// Stream MV Retractable Aggregate Functions
template <LogicalType PT>
auto AggregateFactory::MakeRetractMinAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunctionRetractable<PT, MinAggregateDataRetractable<PT>,
                                                               MinElement<PT, MinAggregateDataRetractable<PT>>>>();
}

template <LogicalType PT>
auto AggregateFactory::MakeRetractMaxAggregateFunction() {
    return std::make_shared<MaxMinAggregateFunctionRetractable<PT, MaxAggregateDataRetractable<PT>,
                                                               MaxElement<PT, MaxAggregateDataRetractable<PT>>>>();
}

} // namespace starrocks
