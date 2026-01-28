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

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/approx_top_k.h"
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
#include "exprs/agg/ds_hll_count_distinct.h"
#include "exprs/agg/ds_theta_count_distinct.h"
#include "exprs/agg/exchange_perf.h"
#include "exprs/agg/group_concat.h"
#include "exprs/agg/histogram.h"
#include "exprs/agg/histogram_hll_ndv.h"
#include "exprs/agg/hll_ndv.h"
#include "exprs/agg/hll_union.h"
#include "exprs/agg/hll_union_count.h"
#include "exprs/agg/intersect_count.h"
#include "exprs/agg/mann_whitney.h"
#include "exprs/agg/map_agg.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/maxmin_by.h"
#include "exprs/agg/minmax_n.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/percentile_approx.h"
#include "exprs/agg/percentile_cont.h"
#include "exprs/agg/percentile_union.h"
#include "exprs/agg/retention.h"
#include "exprs/agg/stream/retract_maxmin.h"
#include "exprs/agg/sum.h"
#include "exprs/agg/sum_map.h"
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
        return new AvgAggregateFunction<LT>();
    }

    template <LogicalType LT>
    static auto MakeDecimalAvgAggregateFunction() {
        return new DecimalAvgAggregateFunction<LT>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction() {
        return new BitmapUnionIntAggregateFunction<LT>();
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
    static auto MakeCountDistinctAggregateFunction();
    template <LogicalType LT>
    static auto MakeCountDistinctAggregateFunctionV2();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeGroupConcatAggregateFunction();

    template <bool IsWindowFunc>
    static AggregateFunctionPtr MakeCountNullableAggregateFunction();

    template <AggExchangePerfType PerfType>
    static AggregateFunctionPtr MakeExchangePerfAggregateFunction() {
        return new ExchangePerfAggregateFunction<PerfType>();
    }

    template <typename ArrayAggState>
    static AggregateFunctionPtr MakeArrayAggAggregateFunctionV2() {
        return new ArrayAggAggregateFunctionV2<ArrayAggState>();
    }

    static AggregateFunctionPtr MakeGroupConcatAggregateFunctionV2() { return new GroupConcatAggregateFunctionV2(); }

    static auto MakeMannWhitneyUTestAggregateFunction() { return new MannWhitneyUTestAggregateFunction(); }

    template <LogicalType LT>
    static auto MakeMaxAggregateFunction();

    template <LogicalType LT, bool not_filter_nulls>
    static auto MakeMaxByAggregateFunction();

    template <LogicalType LT, bool not_filter_nulls>
    static auto MakeMinByAggregateFunction();

    template <LogicalType LT>
    static auto MakeMinAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeAnyValueAggregateFunction();

    static AggregateFunctionPtr MakeAnyValueSemiAggregateFunction() { return new AnyValueSemiAggregateFunction(); }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeMinNAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeMaxNAggregateFunction();

    template <typename NestedState, bool IsWindowFunc, bool IgnoreNull = true,
              typename NestedFunctionPtr = AggregateFunctionPtr,
              IsAggNullPred<NestedState> AggNullPred = AggNonNullPred<NestedState>>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function,
                                                                   AggNullPred null_pred = AggNullPred());

    template <typename NestedState, IsAggNullPred<NestedState> AggNullPredType = AggNonNullPred<NestedState>>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function,
                                                                      AggNullPredType null_pred = AggNullPredType());

    template <LogicalType LT>
    static auto MakeSumAggregateFunction();

    template <LogicalType LT>
    static auto MakeDecimalSumAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static auto MakeVarianceAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static auto MakeStddevAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static auto MakeCovarianceAggregateFunction();

    template <LogicalType LT>
    static auto MakeCorelationAggregateFunction();

    template <LogicalType LT>
    static auto MakeSumDistinctAggregateFunction();
    template <LogicalType LT>
    static auto MakeSumDistinctAggregateFunctionV2();
    template <LogicalType LT>
    static auto MakeDecimalSumDistinctAggregateFunction();
    template <LogicalType LT, int compute_bits>
    static auto MakeFusedMultiDistinctAggregateFunction();

    static AggregateFunctionPtr MakeDictMergeAggregateFunction();
    static AggregateFunctionPtr MakeRetentionAggregateFunction();

    // Hyperloglog functions:
    static AggregateFunctionPtr MakeHllUnionAggregateFunction();

    static AggregateFunctionPtr MakeHllUnionCountAggregateFunction();

    template <LogicalType T>
    static AggregateFunctionPtr MakeHllNdvAggregateFunction();

    template <LogicalType T>
    static AggregateFunctionPtr MakeHllSketchAggregateFunction();

    template <LogicalType T>
    static AggregateFunctionPtr MakeThetaSketchAggregateFunction();

    template <LogicalType T>
    static AggregateFunctionPtr MakeHllRawAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxArrayAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxWeightedAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxWeightedArrayAggregateFunction();

    static AggregateFunctionPtr MakePercentileUnionAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakePercentileContAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakePercentileDiscAggregateFunction();

    template <LogicalType LT>
    static AggregateFunctionPtr MakeLowCardPercentileBinAggregateFunction();

    template <LogicalType PT>
    static AggregateFunctionPtr MakeLowCardPercentileCntAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    static AggregateFunctionPtr MakeCumeDistWindowFunction();

    static AggregateFunctionPtr MakePercentRankWindowFunction();

    static AggregateFunctionPtr MakeNtileWindowFunction();

    template <LogicalType LT, bool ignoreNulls>
    static AggregateFunctionPtr MakeFirstValueWindowFunction() {
        return new FirstValueWindowFunction<LT, ignoreNulls>();
    }

    template <LogicalType LT, bool ignoreNulls>
    static AggregateFunctionPtr MakeLastValueWindowFunction() {
        return new LastValueWindowFunction<LT, ignoreNulls>();
    }

    template <LogicalType LT, bool ignoreNulls, bool isLag>
    static AggregateFunctionPtr MakeLeadLagWindowFunction() {
        return new LeadLagWindowFunction<LT, ignoreNulls, isLag>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeSessionNumberWindowFunction() {
        return new SessionNumberWindowFunction<LT>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeApproxTopKAggregateFunction() {
        return new ApproxTopKAggregateFunction<LT>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeHistogramAggregationFunction() {
        return new HistogramAggregationFunction<LT>();
    }

    template <LogicalType LT>
    static AggregateFunctionPtr MakeHistogramHllNdvAggregationFunction() {
        return new HistogramHllNdvAggregateFunction<LT>();
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
    return new IntersectCountAggregateFunction<LT>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountAggregateFunction() {
    return new CountAggregateFunction<IsWindowFunc>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeWindowfunnelAggregateFunction() {
    return new WindowFunnelAggregateFunction<LT>();
}

template <LogicalType LT>
auto AggregateFactory::MakeCountDistinctAggregateFunction() {
    return new DistinctAggregateFunction<LT, AggDistinctType::COUNT>();
}

template <LogicalType LT>
auto AggregateFactory::MakeCountDistinctAggregateFunctionV2() {
    return new DistinctAggregateFunctionV2<LT, AggDistinctType::COUNT>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeGroupConcatAggregateFunction() {
    return new GroupConcatAggregateFunction<LT>();
}

template <bool IsWindowFunc>
AggregateFunctionPtr AggregateFactory::MakeCountNullableAggregateFunction() {
    return new CountNullableAggregateFunction<IsWindowFunc>();
}

template <LogicalType LT>
auto AggregateFactory::MakeMaxAggregateFunction() {
    return new MaxMinAggregateFunction<LT, MaxAggregateData<LT>, MaxElement<LT, MaxAggregateData<LT>>>();
}

template <LogicalType LT, bool not_filter_nulls>
auto AggregateFactory::MakeMaxByAggregateFunction() {
    using AggData = MaxByAggregateData<LT, not_filter_nulls>;
    return new MaxMinByAggregateFunction<LT, AggData, MaxByElement<LT, AggData>>();
}

template <LogicalType LT, bool not_filter_nulls>
auto AggregateFactory::MakeMinByAggregateFunction() {
    using AggData = MinByAggregateData<LT, not_filter_nulls>;
    return new MaxMinByAggregateFunction<LT, AggData, MinByElement<LT, AggData>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeMinAggregateFunction() {
    return new MaxMinAggregateFunction<LT, MinAggregateData<LT>, MinElement<LT, MinAggregateData<LT>>>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeMinNAggregateFunction() {
    return new MinMaxNAggregateFunction<LT, true>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeMaxNAggregateFunction() {
    return new MinMaxNAggregateFunction<LT, false>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeAnyValueAggregateFunction() {
    return new AnyValueAggregateFunction<LT, AnyValueAggregateData<LT>,
                                         AnyValueElement<LT, AnyValueAggregateData<LT>>>();
}

template <typename NestedState, bool IsWindowFunc, bool IgnoreNull, typename NestedFunctionPtr,
          IsAggNullPred<NestedState> AggNullPred>
AggregateFunctionPtr AggregateFactory::MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function,
                                                                          AggNullPred null_pred) {
    using AggregateDataType = NullableAggregateFunctionState<NestedState, IsWindowFunc>;
    return new NullableAggregateFunctionUnary<NestedFunctionPtr, AggregateDataType, IsWindowFunc, IgnoreNull,
                                              AggNullPred>(nested_function, std::move(null_pred));
}

template <typename NestedState, IsAggNullPred<NestedState> AggNullPredType>
AggregateFunctionPtr AggregateFactory::MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function,
                                                                             AggNullPredType null_pred) {
    using AggregateDataType = NullableAggregateFunctionState<NestedState, false>;
    return new NullableAggregateFunctionVariadic<AggregateDataType, AggNullPredType>(nested_function,
                                                                                     std::move(null_pred));
}

template <LogicalType LT>
auto AggregateFactory::MakeSumAggregateFunction() {
    return new SumAggregateFunction<LT>();
}

template <LogicalType LT>
auto AggregateFactory::MakeDecimalSumAggregateFunction() {
    return new DecimalSumAggregateFunction<LT>();
}

template <LogicalType LT, bool is_sample>
auto AggregateFactory::MakeVarianceAggregateFunction() {
    return new VarianceAggregateFunction<LT, is_sample>();
}

template <LogicalType LT, bool is_sample>
auto AggregateFactory::MakeStddevAggregateFunction() {
    return new StddevAggregateFunction<LT, is_sample>();
}

template <LogicalType LT, bool is_sample>
auto AggregateFactory::MakeCovarianceAggregateFunction() {
    return new CorVarianceAggregateFunction<LT, is_sample>();
}

template <LogicalType LT>
auto AggregateFactory::MakeCorelationAggregateFunction() {
    return new CorelationAggregateFunction<LT>();
}

template <LogicalType LT>
auto AggregateFactory::MakeSumDistinctAggregateFunction() {
    return new DistinctAggregateFunction<LT, AggDistinctType::SUM>();
}

template <LogicalType LT>
auto AggregateFactory::MakeSumDistinctAggregateFunctionV2() {
    return new DistinctAggregateFunctionV2<LT, AggDistinctType::SUM>();
}

template <LogicalType LT>
auto AggregateFactory::MakeDecimalSumDistinctAggregateFunction() {
    return new DecimalDistinctAggregateFunction<LT, AggDistinctType::SUM>();
}

template <LogicalType LT, int compute_bits>
auto AggregateFactory::MakeFusedMultiDistinctAggregateFunction() {
    return new FusedMultiDistinctFunction<LT, compute_bits>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeHllNdvAggregateFunction() {
    return new HllNdvAggregateFunction<LT, false>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeHllSketchAggregateFunction() {
    return new HllSketchAggregateFunction<LT>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeThetaSketchAggregateFunction() {
    return new ThetaSketchAggregateFunction<LT>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeHllRawAggregateFunction() {
    return new HllNdvAggregateFunction<LT, true>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakePercentileContAggregateFunction() {
    return new PercentileContAggregateFunction<LT>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakePercentileDiscAggregateFunction() {
    return new PercentileDiscAggregateFunction<PT>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeLowCardPercentileBinAggregateFunction() {
    return new LowCardPercentileBinAggregateFunction<PT>();
}

template <LogicalType PT>
AggregateFunctionPtr AggregateFactory::MakeLowCardPercentileCntAggregateFunction() {
    return new LowCardPercentileCntAggregateFunction<PT>();
}

template <LogicalType LT>
AggregateFunctionPtr AggregateFactory::MakeBitmapAggAggregateFunction() {
    return new BitmapAggAggregateFunction<LT>();
}

// Stream MV Retractable Aggregate Functions
template <LogicalType LT>
auto AggregateFactory::MakeRetractMinAggregateFunction() {
    return new MaxMinAggregateFunctionRetractable<LT, MinAggregateDataRetractable<LT>,
                                                  MinElement<LT, MinAggregateDataRetractable<LT>>>();
}

template <LogicalType LT>
auto AggregateFactory::MakeRetractMaxAggregateFunction() {
    return new MaxMinAggregateFunctionRetractable<LT, MaxAggregateDataRetractable<LT>,
                                                  MaxElement<LT, MaxAggregateDataRetractable<LT>>>();
}

} // namespace starrocks
