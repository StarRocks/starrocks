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
    static const AggregateFunction* MakeAvgAggregateFunction() {
        return new AvgAggregateFunction<LT>();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeDecimalAvgAggregateFunction() {
        return new DecimalAvgAggregateFunction<LT>();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeBitmapUnionIntAggregateFunction() {
        return new BitmapUnionIntAggregateFunction<LT>();
    }

    static const AggregateFunction* MakeBitmapUnionAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeBitmapAggAggregateFunction();

    static const AggregateFunction* MakeBitmapIntersectAggregateFunction();

    static const AggregateFunction* MakeBitmapUnionCountAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeWindowfunnelAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeIntersectCountAggregateFunction();

    template <bool IsWindowFunc>
    static const AggregateFunction* MakeCountAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeCountDistinctAggregateFunction();
    template <LogicalType LT>
    static const AggregateFunction* MakeCountDistinctAggregateFunctionV2();

    template <LogicalType LT>
    static const AggregateFunction* MakeGroupConcatAggregateFunction();

    template <bool IsWindowFunc>
    static const AggregateFunction* MakeCountNullableAggregateFunction();

    template <AggExchangePerfType PerfType>
    static const AggregateFunction* MakeExchangePerfAggregateFunction() {
        return new ExchangePerfAggregateFunction<PerfType>();
    }

    template <typename ArrayAggState>
    static const AggregateFunction* MakeArrayAggAggregateFunctionV2() {
        return new ArrayAggAggregateFunctionV2<ArrayAggState>();
    }

    static const AggregateFunction* MakeGroupConcatAggregateFunctionV2() {
        return new GroupConcatAggregateFunctionV2();
    }

    static const AggregateFunction* MakeMannWhitneyUTestAggregateFunction() {
        return new MannWhitneyUTestAggregateFunction();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeMaxAggregateFunction();

    template <LogicalType LT, bool not_filter_nulls>
    static const AggregateFunction* MakeMaxByAggregateFunction();

    template <LogicalType LT, bool not_filter_nulls>
    static const AggregateFunction* MakeMinByAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeMinAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeAnyValueAggregateFunction();

    static const AggregateFunction* MakeAnyValueSemiAggregateFunction() { return new AnyValueSemiAggregateFunction(); }

    template <typename NestedState, bool IsWindowFunc, bool IgnoreNull = true,
              typename NestedFunctionPtr = const AggregateFunction*,
              IsAggNullPred<NestedState> AggNullPred = AggNonNullPred<NestedState>>
    static const AggregateFunction* MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function,
                                                                       AggNullPred null_pred = AggNullPred());

    template <typename NestedState, IsAggNullPred<NestedState> AggNullPredType = AggNonNullPred<NestedState>>
    static const AggregateFunction* MakeNullableAggregateFunctionVariadic(
            const AggregateFunction* nested_function, AggNullPredType null_pred = AggNullPredType());

    template <LogicalType LT>
    static const AggregateFunction* MakeSumAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeDecimalSumAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static const AggregateFunction* MakeVarianceAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static const AggregateFunction* MakeStddevAggregateFunction();

    template <LogicalType LT, bool is_sample>
    static const AggregateFunction* MakeCovarianceAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeCorelationAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeSumDistinctAggregateFunction();
    template <LogicalType LT>
    static const AggregateFunction* MakeSumDistinctAggregateFunctionV2();
    template <LogicalType LT>
    static const AggregateFunction* MakeDecimalSumDistinctAggregateFunction();
    template <LogicalType LT, int compute_bits>
    static const AggregateFunction* MakeFusedMultiDistinctAggregateFunction();

    static const AggregateFunction* MakeDictMergeAggregateFunction();
    static const AggregateFunction* MakeRetentionAggregateFunction();

    // Hyperloglog functions:
    static const AggregateFunction* MakeHllUnionAggregateFunction();

    static const AggregateFunction* MakeHllUnionCountAggregateFunction();

    template <LogicalType T>
    static const AggregateFunction* MakeHllNdvAggregateFunction();

    template <LogicalType T>
    static const AggregateFunction* MakeHllSketchAggregateFunction();

    template <LogicalType T>
    static const AggregateFunction* MakeThetaSketchAggregateFunction();

    template <LogicalType T>
    static const AggregateFunction* MakeHllRawAggregateFunction();

    static const AggregateFunction* MakePercentileApproxAggregateFunction();

    static const AggregateFunction* MakePercentileApproxArrayAggregateFunction();

    static const AggregateFunction* MakePercentileApproxWeightedAggregateFunction();

    static const AggregateFunction* MakePercentileApproxWeightedArrayAggregateFunction();

    static const AggregateFunction* MakePercentileUnionAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakePercentileContAggregateFunction();

    template <LogicalType PT>
    static const AggregateFunction* MakePercentileDiscAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeLowCardPercentileBinAggregateFunction();

    template <LogicalType PT>
    static const AggregateFunction* MakeLowCardPercentileCntAggregateFunction();

    // Windows functions:
    static const AggregateFunction* MakeDenseRankWindowFunction();

    static const AggregateFunction* MakeRankWindowFunction();

    static const AggregateFunction* MakeRowNumberWindowFunction();

    static const AggregateFunction* MakeCumeDistWindowFunction();

    static const AggregateFunction* MakePercentRankWindowFunction();

    static const AggregateFunction* MakeNtileWindowFunction();

    template <LogicalType LT, bool ignoreNulls>
    static const AggregateFunction* MakeFirstValueWindowFunction() {
        return new FirstValueWindowFunction<LT, ignoreNulls>();
    }

    template <LogicalType LT, bool ignoreNulls>
    static const AggregateFunction* MakeLastValueWindowFunction() {
        return new LastValueWindowFunction<LT, ignoreNulls>();
    }

    template <LogicalType LT, bool ignoreNulls, bool isLag>
    static const AggregateFunction* MakeLeadLagWindowFunction() {
        return new LeadLagWindowFunction<LT, ignoreNulls, isLag>();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeSessionNumberWindowFunction() {
        return new SessionNumberWindowFunction<LT>();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeApproxTopKAggregateFunction() {
        return new ApproxTopKAggregateFunction<LT>();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeHistogramAggregationFunction() {
        return new HistogramAggregationFunction<LT>();
    }

    template <LogicalType LT>
    static const AggregateFunction* MakeHistogramHllNdvAggregationFunction() {
        return new HistogramHllNdvAggregateFunction<LT>();
    }

    // Stream MV Retractable Agg Functions
    template <LogicalType LT>
    static const AggregateFunction* MakeRetractMinAggregateFunction();

    template <LogicalType LT>
    static const AggregateFunction* MakeRetractMaxAggregateFunction();
};

// The function should be placed by alphabetical order

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeIntersectCountAggregateFunction() {
    return new IntersectCountAggregateFunction<LT>();
}

template <bool IsWindowFunc>
const AggregateFunction* AggregateFactory::MakeCountAggregateFunction() {
    return new CountAggregateFunction<IsWindowFunc>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeWindowfunnelAggregateFunction() {
    return new WindowFunnelAggregateFunction<LT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeCountDistinctAggregateFunction() {
    return new DistinctAggregateFunction<LT, AggDistinctType::COUNT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeCountDistinctAggregateFunctionV2() {
    return new DistinctAggregateFunctionV2<LT, AggDistinctType::COUNT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeGroupConcatAggregateFunction() {
    return new GroupConcatAggregateFunction<LT>();
}

template <bool IsWindowFunc>
const AggregateFunction* AggregateFactory::MakeCountNullableAggregateFunction() {
    return new CountNullableAggregateFunction<IsWindowFunc>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeMaxAggregateFunction() {
    return new MaxMinAggregateFunction<LT, MaxAggregateData<LT>, MaxElement<LT, MaxAggregateData<LT>>>();
}

template <LogicalType LT, bool not_filter_nulls>
const AggregateFunction* AggregateFactory::MakeMaxByAggregateFunction() {
    using AggData = MaxByAggregateData<LT, not_filter_nulls>;
    return new MaxMinByAggregateFunction<LT, AggData, MaxByElement<LT, AggData>>();
}

template <LogicalType LT, bool not_filter_nulls>
const AggregateFunction* AggregateFactory::MakeMinByAggregateFunction() {
    using AggData = MinByAggregateData<LT, not_filter_nulls>;
    return new MaxMinByAggregateFunction<LT, AggData, MinByElement<LT, AggData>>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeMinAggregateFunction() {
    return new MaxMinAggregateFunction<LT, MinAggregateData<LT>, MinElement<LT, MinAggregateData<LT>>>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeAnyValueAggregateFunction() {
    return new AnyValueAggregateFunction<LT, AnyValueAggregateData<LT>,
                                         AnyValueElement<LT, AnyValueAggregateData<LT>>>();
}

template <typename NestedState, bool IsWindowFunc, bool IgnoreNull, typename NestedFunctionPtr,
          IsAggNullPred<NestedState> AggNullPred>
const AggregateFunction* AggregateFactory::MakeNullableAggregateFunctionUnary(NestedFunctionPtr nested_function,
                                                                              AggNullPred null_pred) {
    using AggregateDataType = NullableAggregateFunctionState<NestedState, IsWindowFunc>;
    return new NullableAggregateFunctionUnary<NestedFunctionPtr, AggregateDataType, IsWindowFunc, IgnoreNull,
                                              AggNullPred>(nested_function, std::move(null_pred));
}

template <typename NestedState, IsAggNullPred<NestedState> AggNullPredType>
const AggregateFunction* AggregateFactory::MakeNullableAggregateFunctionVariadic(
        const AggregateFunction* nested_function, AggNullPredType null_pred) {
    using AggregateDataType = NullableAggregateFunctionState<NestedState, false>;
    return new NullableAggregateFunctionVariadic<const AggregateFunction*, AggregateDataType, AggNullPredType>(
            nested_function, std::move(null_pred));
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeSumAggregateFunction() {
    return new SumAggregateFunction<LT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeDecimalSumAggregateFunction() {
    return new DecimalSumAggregateFunction<LT>();
}

template <LogicalType LT, bool is_sample>
const AggregateFunction* AggregateFactory::MakeVarianceAggregateFunction() {
    return new VarianceAggregateFunction<LT, is_sample>();
}

template <LogicalType LT, bool is_sample>
const AggregateFunction* AggregateFactory::MakeStddevAggregateFunction() {
    return new StddevAggregateFunction<LT, is_sample>();
}

template <LogicalType LT, bool is_sample>
const AggregateFunction* AggregateFactory::MakeCovarianceAggregateFunction() {
    return new CorVarianceAggregateFunction<LT, is_sample>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeCorelationAggregateFunction() {
    return new CorelationAggregateFunction<LT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeSumDistinctAggregateFunction() {
    return new DistinctAggregateFunction<LT, AggDistinctType::SUM>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeSumDistinctAggregateFunctionV2() {
    return new DistinctAggregateFunctionV2<LT, AggDistinctType::SUM>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeDecimalSumDistinctAggregateFunction() {
    return new DecimalDistinctAggregateFunction<LT, AggDistinctType::SUM>();
}

template <LogicalType LT, int compute_bits>
const AggregateFunction* AggregateFactory::MakeFusedMultiDistinctAggregateFunction() {
    return new FusedMultiDistinctFunction<LT, compute_bits>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeHllNdvAggregateFunction() {
    return new HllNdvAggregateFunction<LT, false>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeHllSketchAggregateFunction() {
    return new HllSketchAggregateFunction<LT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeThetaSketchAggregateFunction() {
    return new ThetaSketchAggregateFunction<LT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeHllRawAggregateFunction() {
    return new HllNdvAggregateFunction<LT, true>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakePercentileContAggregateFunction() {
    return new PercentileContAggregateFunction<LT>();
}

template <LogicalType PT>
const AggregateFunction* AggregateFactory::MakePercentileDiscAggregateFunction() {
    return new PercentileDiscAggregateFunction<PT>();
}

template <LogicalType PT>
const AggregateFunction* AggregateFactory::MakeLowCardPercentileBinAggregateFunction() {
    return new LowCardPercentileBinAggregateFunction<PT>();
}

template <LogicalType PT>
const AggregateFunction* AggregateFactory::MakeLowCardPercentileCntAggregateFunction() {
    return new LowCardPercentileCntAggregateFunction<PT>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeBitmapAggAggregateFunction() {
    return new BitmapAggAggregateFunction<LT>();
}

// Stream MV Retractable Aggregate Functions
template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeRetractMinAggregateFunction() {
    return new MaxMinAggregateFunctionRetractable<LT, MinAggregateDataRetractable<LT>,
                                                  MinElement<LT, MinAggregateDataRetractable<LT>>>();
}

template <LogicalType LT>
const AggregateFunction* AggregateFactory::MakeRetractMaxAggregateFunction() {
    return new MaxMinAggregateFunctionRetractable<LT, MaxAggregateDataRetractable<LT>,
                                                  MaxElement<LT, MaxAggregateDataRetractable<LT>>>();
}

} // namespace starrocks
