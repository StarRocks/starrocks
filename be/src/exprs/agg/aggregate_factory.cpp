// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"

#include <memory>
#include <tuple>
#include <unordered_map>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/bitmap_union.h"
#include "exprs/agg/bitmap_union_count.h"
#include "exprs/agg/bitmap_union_int.h"
#include "exprs/agg/count.h"
#include "exprs/agg/distinct.h"
#include "exprs/agg/group_concat.h"
#include "exprs/agg/histogram.h"
#include "exprs/agg/hll_ndv.h"
#include "exprs/agg/hll_union.h"
#include "exprs/agg/hll_union_count.h"
#include "exprs/agg/intersect_count.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/percentile_approx.h"
#include "exprs/agg/percentile_cont.h"
#include "exprs/agg/retention.h"
#include "exprs/agg/sum.h"
#include "exprs/agg/variance.h"
#include "exprs/agg/window.h"
#include "exprs/agg/window_funnel.h"
#include "percentile_union.h"
#include "runtime/primitive_type.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks::vectorized {
class AggregateFactory {
public:
    // The function should be placed by alphabetical order
    template <PrimitiveType PT>
    static auto MakeAvgAggregateFunction();

    template <PrimitiveType PT>
    static auto MakeDecimalAvgAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

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

    template <PrimitiveType PT>
    static auto MakeMaxAggregateFunction();

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

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeHllRawAggregateFunction();

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

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeHistogramAggregationFunction();
};

// The function should be placed by alphabetical order

template <PrimitiveType PT>
auto AggregateFactory::MakeAvgAggregateFunction() {
    return std::make_shared<AvgAggregateFunction<PT>>();
}

template <PrimitiveType PT>
auto AggregateFactory::MakeDecimalAvgAggregateFunction() {
    return std::make_shared<DecimalAvgAggregateFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeBitmapUnionIntAggregateFunction() {
    return std::make_shared<BitmapUnionIntAggregateFunction<PT>>();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapUnionAggregateFunction() {
    return std::make_shared<BitmapUnionAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapIntersectAggregateFunction() {
    return std::make_shared<BitmapIntersectAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapUnionCountAggregateFunction() {
    return std::make_shared<BitmapUnionCountAggregateFunction>();
}

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
AggregateFunctionPtr AggregateFactory::MakeDecimalSumAggregateFunction() {
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

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeSumDistinctAggregateFunction() {
    return std::make_shared<DistinctAggregateFunction<PT, AggDistinctType::SUM>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeSumDistinctAggregateFunctionV2() {
    return std::make_shared<DistinctAggregateFunctionV2<PT, AggDistinctType::SUM>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeDecimalSumDistinctAggregateFunction() {
    return std::make_shared<DecimalDistinctAggregateFunction<PT, AggDistinctType::SUM>>();
}

AggregateFunctionPtr AggregateFactory::MakeDictMergeAggregateFunction() {
    return std::make_shared<DictMergeAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeRetentionAggregateFunction() {
    return std::make_shared<RetentionAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeHllUnionAggregateFunction() {
    return std::make_shared<HllUnionAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeHllUnionCountAggregateFunction() {
    return std::make_shared<HllUnionCountAggregateFunction>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeHllNdvAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<PT, false>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeHllRawAggregateFunction() {
    return std::make_shared<HllNdvAggregateFunction<PT, true>>();
}

AggregateFunctionPtr AggregateFactory::MakePercentileApproxAggregateFunction() {
    return std::make_shared<PercentileApproxAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakePercentileUnionAggregateFunction() {
    return std::make_shared<PercentileUnionAggregateFunction>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakePercentileContAggregateFunction() {
    return std::make_shared<PercentileContAggregateFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeArrayAggAggregateFunction() {
    return std::make_shared<ArrayAggAggregateFunction<PT>>();
}

// Windows functions:

AggregateFunctionPtr AggregateFactory::MakeDenseRankWindowFunction() {
    return std::make_shared<DenseRankWindowFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeRankWindowFunction() {
    return std::make_shared<RankWindowFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeRowNumberWindowFunction() {
    return std::make_shared<RowNumberWindowFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeNtileWindowFunction() {
    return std::make_shared<NtileWindowFunction>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeFirstValueWindowFunction() {
    return std::make_shared<FirstValueWindowFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeLastValueWindowFunction() {
    return std::make_shared<LastValueWindowFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeLeadLagWindowFunction() {
    return std::make_shared<LeadLagWindowFunction<PT>>();
}

template <PrimitiveType PT>
AggregateFunctionPtr AggregateFactory::MakeHistogramAggregationFunction() {
    return std::make_shared<HistogramAggregationFunction<PT>>();
}

// ----------------------------------------------------------------------------------------------
// ----------------------------------------------------------------------------------------------

// 1. name
// 2. arg primitive type
// 3. return primitive type
// 4. is_window_function
// 5. is_nullable
typedef std::tuple<std::string, int, int, bool, bool> AggregateFuncKey;

struct AggregateFuncMapHash {
    size_t operator()(const AggregateFuncKey& key) const {
        std::hash<std::string> hasher;
        return hasher(std::get<0>(key)) ^ std::get<1>(key) ^ std::get<2>(key) ^ std::get<3>(key) ^ std::get<4>(key);
    }
};

class AggregateFuncResolver {
    DECLARE_SINGLETON(AggregateFuncResolver);

public:
    const AggregateFunction* get_aggregate_info(const std::string& name, const PrimitiveType arg_type,
                                                const PrimitiveType return_type, const bool is_window_function,
                                                const bool is_null) const {
        auto pair = _infos_mapping.find(std::make_tuple(name, arg_type, return_type, is_window_function, is_null));
        if (pair == _infos_mapping.end()) {
            return nullptr;
        }
        return pair->second.get();
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false>
    void add_aggregate_mapping(std::string&& name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false),
                               create_function<ArgPT, ResultPT, false, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true),
                               create_function<ArgPT, ResultPT, false, true>(name));
        if constexpr (AddWindowVersion) {
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, false),
                                   create_function<ArgPT, ResultPT, true, false>(name));
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, true),
                                   create_function<ArgPT, ResultPT, true, true>(name));
        }
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false>
    void add_object_mapping(std::string&& name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false),
                               create_object_function<ArgPT, ResultPT, false, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true),
                               create_object_function<ArgPT, ResultPT, false, true>(name));
        if constexpr (AddWindowVersion) {
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, false),
                                   create_object_function<ArgPT, ResultPT, true, false>(name));
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, true),
                                   create_object_function<ArgPT, ResultPT, true, true>(name));
        }
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT>
    void add_array_mapping(std::string&& name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false),
                               create_array_function<ArgPT, ResultPT, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true),
                               create_array_function<ArgPT, ResultPT, true>(name));
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false>
    void add_decimal_mapping(std::string&& name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false),
                               create_decimal_function<ArgPT, ResultPT, false, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true),
                               create_decimal_function<ArgPT, ResultPT, false, true>(name));
        if constexpr (AddWindowVersion) {
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, false),
                                   create_decimal_function<ArgPT, ResultPT, true, false>(name));
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, true),
                                   create_decimal_function<ArgPT, ResultPT, true, true>(name));
        }
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool IsWindowFunc, bool IsNull>
    AggregateFunctionPtr create_object_function(std::string& name) {
        if constexpr (IsNull) {
            if (name == "hll_raw_agg" || name == "hll_union") {
                auto hll_union = AggregateFactory::MakeHllUnionAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<HyperLogLog, IsWindowFunc>(hll_union);
            } else if (name == "hll_union_agg") {
                auto hll_union_count = AggregateFactory::MakeHllUnionCountAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<HyperLogLog, IsWindowFunc>(hll_union_count);
            } else if (name == "bitmap_union") {
                auto bitmap = AggregateFactory::MakeBitmapUnionAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<BitmapValue, IsWindowFunc>(bitmap);
            } else if (name == "bitmap_intersect") {
                auto bitmap = AggregateFactory::MakeBitmapIntersectAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<BitmapValuePacked, IsWindowFunc>(bitmap);
            } else if (name == "bitmap_union_count") {
                auto bitmap = AggregateFactory::MakeBitmapUnionCountAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<BitmapValue, IsWindowFunc>(bitmap);
            } else if (name == "intersect_count") {
                auto bitmap = AggregateFactory::MakeIntersectCountAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionVariadic<
                        BitmapIntersectAggregateState<BitmapRuntimeCppType<ArgPT>>>(bitmap);
            } else if (name == "ndv" || name == "approx_count_distinct") {
                auto ndv = AggregateFactory::MakeHllNdvAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<HyperLogLog, IsWindowFunc>(ndv);
            } else if (name == "percentile_union") {
                auto percentile = AggregateFactory::MakePercentileUnionAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<PercentileValue, IsWindowFunc>(percentile);
            } else if (name == "hll_raw") {
                auto hll_raw = AggregateFactory::MakeHllRawAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<HyperLogLog, IsWindowFunc>(hll_raw);
            }
        } else {
            if (name == "hll_raw_agg" || name == "hll_union") {
                return AggregateFactory::MakeHllUnionAggregateFunction();
            } else if (name == "hll_union_agg") {
                return AggregateFactory::MakeHllUnionCountAggregateFunction();
            } else if (name == "bitmap_union") {
                return AggregateFactory::MakeBitmapUnionAggregateFunction();
            } else if (name == "bitmap_intersect") {
                return AggregateFactory::MakeBitmapIntersectAggregateFunction();
            } else if (name == "bitmap_union_count") {
                return AggregateFactory::MakeBitmapUnionCountAggregateFunction();
            } else if (name == "intersect_count") {
                return AggregateFactory::MakeIntersectCountAggregateFunction<ArgPT>();
            } else if (name == "ndv" || name == "approx_count_distinct") {
                return AggregateFactory::MakeHllNdvAggregateFunction<ArgPT>();
            } else if (name == "percentile_union") {
                return AggregateFactory::MakePercentileUnionAggregateFunction();
            } else if (name == "hll_raw") {
                return AggregateFactory::MakeHllRawAggregateFunction<ArgPT>();
            }
        }

        //MakeNullableAggregateFunctionUnary only support deal with single parameter aggregation function,
        //so here are the separate processing function percentile_approx
        if (name == "percentile_approx") {
            return AggregateFactory::MakePercentileApproxAggregateFunction();
        }

        return nullptr;
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool IsNull>
    AggregateFunctionPtr create_array_function(std::string& name) {
        if constexpr (IsNull) {
            if (name == "dict_merge") {
                auto dict_merge = AggregateFactory::MakeDictMergeAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<DictMergeState, false>(dict_merge);
            } else if (name == "retention") {
                auto retentoin = AggregateFactory::MakeRetentionAggregateFunction();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<RetentionState, false>(retentoin);
            } else if (name == "window_funnel") {
                if constexpr (ArgPT == TYPE_INT || ArgPT == TYPE_BIGINT || ArgPT == TYPE_DATE ||
                              ArgPT == TYPE_DATETIME) {
                    auto windowfunnel = AggregateFactory::MakeWindowfunnelAggregateFunction<ArgPT>();
                    return AggregateFactory::MakeNullableAggregateFunctionVariadic<WindowFunnelState<ArgPT>>(
                            windowfunnel);
                }
            }
        } else {
            if (name == "dict_merge") {
                return AggregateFactory::MakeDictMergeAggregateFunction();
            } else if (name == "retention") {
                return AggregateFactory::MakeRetentionAggregateFunction();
            } else if (name == "window_funnel") {
                if constexpr (ArgPT == TYPE_INT || ArgPT == TYPE_BIGINT || ArgPT == TYPE_DATE ||
                              ArgPT == TYPE_DATETIME) {
                    return AggregateFactory::MakeWindowfunnelAggregateFunction<ArgPT>();
                }
            }
        }

        return nullptr;
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool IsWindowFunc, bool IsNull>
    std::enable_if_t<isArithmeticPT<ArgPT>, AggregateFunctionPtr> create_decimal_function(std::string& name) {
        static_assert(pt_is_decimal128<ResultPT>);
        if constexpr (IsNull) {
            using ResultType = RunTimeCppType<ResultPT>;
            if (name == "decimal_avg") {
                auto avg = AggregateFactory::MakeDecimalAvgAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AvgAggregateState<ResultType>,
                                                                            IsWindowFunc>(avg);
            } else if (name == "decimal_sum") {
                auto sum = AggregateFactory::MakeDecimalSumAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AvgAggregateState<ResultType>,
                                                                            IsWindowFunc>(sum);
            } else if (name == "decimal_multi_distinct_sum") {
                auto distinct_sum = AggregateFactory::MakeDecimalSumDistinctAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<DistinctAggregateState<ArgPT, ResultPT>,
                                                                            IsWindowFunc>(distinct_sum);
            }
        } else {
            if (name == "decimal_avg") {
                return AggregateFactory::MakeDecimalAvgAggregateFunction<ArgPT>();
            } else if (name == "decimal_sum") {
                return AggregateFactory::MakeDecimalSumAggregateFunction<ArgPT>();
            } else if (name == "decimal_multi_distinct_sum") {
                return AggregateFactory::MakeDecimalSumDistinctAggregateFunction<ArgPT>();
            }
        }
        return nullptr;
    }

    // TODO(kks): simplify create_function method
    template <PrimitiveType ArgPT, PrimitiveType ReturnPT, bool IsWindowFunc, bool IsNull>
    std::enable_if_t<isArithmeticPT<ArgPT>, AggregateFunctionPtr> create_function(std::string& name) {
        if constexpr (IsNull) {
            if (name == "count") {
                return AggregateFactory::MakeCountNullableAggregateFunction<IsWindowFunc>();
            } else if (name == "sum") {
                auto sum = AggregateFactory::MakeSumAggregateFunction<ArgPT>();
                using ResultType = RunTimeCppType<SumResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<SumAggregateState<ResultType>,
                                                                            IsWindowFunc>(sum);
            } else if (name == "variance" || name == "variance_pop" || name == "var_pop") {
                auto variance = AggregateFactory::MakeVarianceAggregateFunction<ArgPT, false>();
                using ResultType = RunTimeCppType<DevFromAveResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<DevFromAveAggregateState<ResultType>,
                                                                            IsWindowFunc>(variance);
            } else if (name == "variance_samp" || name == "var_samp") {
                auto variance = AggregateFactory::MakeVarianceAggregateFunction<ArgPT, true>();
                using ResultType = RunTimeCppType<DevFromAveResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<DevFromAveAggregateState<ResultType>,
                                                                            IsWindowFunc>(variance);
            } else if (name == "std" || name == "stddev" || name == "stddev_pop") {
                auto stddev = AggregateFactory::MakeStddevAggregateFunction<ArgPT, false>();
                using ResultType = RunTimeCppType<DevFromAveResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<DevFromAveAggregateState<ResultType>,
                                                                            IsWindowFunc>(stddev);
            } else if (name == "stddev_samp") {
                auto stddev = AggregateFactory::MakeStddevAggregateFunction<ArgPT, true>();
                using ResultType = RunTimeCppType<DevFromAveResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<DevFromAveAggregateState<ResultType>,
                                                                            IsWindowFunc>(stddev);
            } else if (name == "bitmap_union_int") {
                auto bitmap = AggregateFactory::MakeBitmapUnionIntAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<BitmapValue, IsWindowFunc>(bitmap);
            } else if (name == "max") {
                auto max = AggregateFactory::MakeMaxAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<MaxAggregateData<ArgPT>, IsWindowFunc>(max);
            } else if (name == "min") {
                auto min = AggregateFactory::MakeMinAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<MinAggregateData<ArgPT>, IsWindowFunc>(min);
            } else if (name == "avg") {
                auto avg = AggregateFactory::MakeAvgAggregateFunction<ArgPT>();
                using ResultType = RunTimeCppType<ImmediateAvgResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AvgAggregateState<ResultType>,
                                                                            IsWindowFunc>(avg);
            } else if (name == "multi_distinct_count") {
                auto distinct = AggregateFactory::MakeCountDistinctAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<
                        DistinctAggregateState<ArgPT, SumResultPT<ArgPT>>, IsWindowFunc>(distinct);
            } else if (name == "multi_distinct_count2") {
                auto distinct = AggregateFactory::MakeCountDistinctAggregateFunctionV2<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<
                        DistinctAggregateStateV2<ArgPT, SumResultPT<ArgPT>>, IsWindowFunc>(distinct);
            } else if (name == "multi_distinct_sum") {
                auto distinct = AggregateFactory::MakeSumDistinctAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<
                        DistinctAggregateState<ArgPT, SumResultPT<ArgPT>>, IsWindowFunc>(distinct);
            } else if (name == "multi_distinct_sum2") {
                auto distinct = AggregateFactory::MakeSumDistinctAggregateFunctionV2<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<
                        DistinctAggregateStateV2<ArgPT, SumResultPT<ArgPT>>, IsWindowFunc>(distinct);
            } else if (name == "group_concat") {
                auto group_count = AggregateFactory::MakeGroupConcatAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionVariadic<GroupConcatAggregateState>(group_count);
            } else if (name == "any_value") {
                auto any_value = AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AnyValueAggregateData<ArgPT>, IsWindowFunc>(
                        any_value);
            } else if (name == "array_agg") {
                auto array_agg = AggregateFactory::MakeArrayAggAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<ArrayAggAggregateState<ArgPT>, IsWindowFunc,
                                                                            false>(array_agg);
            } else if (name == "percentile_cont") {
                auto percentile_cont = AggregateFactory::MakePercentileContAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionVariadic<PercentileContState<ArgPT>>(
                        percentile_cont);
            }
        } else {
            if (name == "count") {
                return AggregateFactory::MakeCountAggregateFunction<IsWindowFunc>();
            } else if (name == "sum") {
                return AggregateFactory::MakeSumAggregateFunction<ArgPT>();
            } else if (name == "variance" || name == "variance_pop" || name == "var_pop") {
                return AggregateFactory::MakeVarianceAggregateFunction<ArgPT, false>();
            } else if (name == "variance_samp" || name == "var_samp") {
                return AggregateFactory::MakeVarianceAggregateFunction<ArgPT, true>();
            } else if (name == "std" || name == "stddev" || name == "stddev_pop") {
                return AggregateFactory::MakeStddevAggregateFunction<ArgPT, false>();
            } else if (name == "stddev_samp") {
                return AggregateFactory::MakeStddevAggregateFunction<ArgPT, true>();
            } else if (name == "bitmap_union_int") {
                return AggregateFactory::MakeBitmapUnionIntAggregateFunction<ArgPT>();
            } else if (name == "max") {
                return AggregateFactory::MakeMaxAggregateFunction<ArgPT>();
            } else if (name == "min") {
                return AggregateFactory::MakeMinAggregateFunction<ArgPT>();
            } else if (name == "avg") {
                return AggregateFactory::MakeAvgAggregateFunction<ArgPT>();
            } else if (name == "multi_distinct_count") {
                return AggregateFactory::MakeCountDistinctAggregateFunction<ArgPT>();
            } else if (name == "multi_distinct_count2") {
                return AggregateFactory::MakeCountDistinctAggregateFunctionV2<ArgPT>();
            } else if (name == "multi_distinct_sum") {
                return AggregateFactory::MakeSumDistinctAggregateFunction<ArgPT>();
            } else if (name == "multi_distinct_sum2") {
                return AggregateFactory::MakeSumDistinctAggregateFunctionV2<ArgPT>();
            } else if (name == "group_concat") {
                return AggregateFactory::MakeGroupConcatAggregateFunction<ArgPT>();
            } else if (name == "any_value") {
                return AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
            } else if (name == "array_agg") {
                return AggregateFactory::MakeArrayAggAggregateFunction<ArgPT>();
            } else if (name == "percentile_cont") {
                return AggregateFactory::MakePercentileContAggregateFunction<ArgPT>();
            }
        }

        if (name == "lead" || name == "lag") {
            return AggregateFactory::MakeLeadLagWindowFunction<ArgPT>();
        } else if (name == "first_value") {
            return AggregateFactory::MakeFirstValueWindowFunction<ArgPT>();
        } else if (name == "last_value") {
            return AggregateFactory::MakeLastValueWindowFunction<ArgPT>();
        } else if (name == "dense_rank") {
            return AggregateFactory::MakeDenseRankWindowFunction();
        } else if (name == "rank") {
            return AggregateFactory::MakeRankWindowFunction();
        } else if (name == "row_number") {
            return AggregateFactory::MakeRowNumberWindowFunction();
        } else if (name == "ntile") {
            return AggregateFactory::MakeNtileWindowFunction();
        } else if (name == "histogram") {
            return AggregateFactory::MakeHistogramAggregationFunction<ArgPT>();
        }
        return nullptr;
    }

    template <PrimitiveType ArgPT, PrimitiveType ReturnPT, bool IsWindowFunc, bool IsNull>
    std::enable_if_t<pt_is_json<ArgPT>, AggregateFunctionPtr> create_function(std::string& name) {
        // TODO: support more functions for JSON type
        if constexpr (IsNull) {
            if (name == "any_value") {
                auto any_value = AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AnyValueAggregateData<ArgPT>, IsWindowFunc>(
                        any_value);
                return AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
            }
        } else {
            if (name == "any_value") {
                return AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
            }
        }
        return nullptr;
    }

    template <PrimitiveType ArgPT, PrimitiveType ReturnPT, bool IsWindowFunc, bool IsNull>
    std::enable_if_t<!isArithmeticPT<ArgPT> && !pt_is_json<ArgPT>, AggregateFunctionPtr> create_function(
            std::string& name) {
        using ArgType = RunTimeCppType<ArgPT>;
        if constexpr (IsNull) {
            if (name == "avg") {
                auto avg = AggregateFactory::MakeAvgAggregateFunction<ArgPT>();
                using ResultType = RunTimeCppType<ImmediateAvgResultPT<ArgPT>>;
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AvgAggregateState<ResultType>,
                                                                            IsWindowFunc>(avg);
            } else if (name == "max") {
                auto max = AggregateFactory::MakeMaxAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<MaxAggregateData<ArgPT>, IsWindowFunc>(max);
            } else if (name == "min") {
                auto min = AggregateFactory::MakeMinAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<MinAggregateData<ArgPT>, IsWindowFunc>(min);
            } else if (name == "multi_distinct_count") {
                auto distinct = AggregateFactory::MakeCountDistinctAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<
                        DistinctAggregateState<ArgPT, SumResultPT<ArgPT>>, IsWindowFunc>(distinct);
            } else if (name == "multi_distinct_count2") {
                auto distinct = AggregateFactory::MakeCountDistinctAggregateFunctionV2<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<
                        DistinctAggregateStateV2<ArgPT, SumResultPT<ArgPT>>, IsWindowFunc>(distinct);
            } else if (name == "group_concat") {
                auto group_count = AggregateFactory::MakeGroupConcatAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionVariadic<GroupConcatAggregateState>(group_count);
            } else if (name == "any_value") {
                auto any_value = AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<AnyValueAggregateData<ArgPT>, IsWindowFunc>(
                        any_value);
            } else if (name == "array_agg") {
                auto array_agg_value = AggregateFactory::MakeArrayAggAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionUnary<ArrayAggAggregateState<ArgPT>, IsWindowFunc,
                                                                            false>(array_agg_value);
            } else if (name == "percentile_cont") {
                auto percentile_cont = AggregateFactory::MakePercentileContAggregateFunction<ArgPT>();
                return AggregateFactory::MakeNullableAggregateFunctionVariadic<PercentileContState<ArgPT>>(
                        percentile_cont);
            }
        } else {
            if (name == "avg") {
                return AggregateFactory::MakeAvgAggregateFunction<ArgPT>();
            } else if (name == "max") {
                return AggregateFactory::MakeMaxAggregateFunction<ArgPT>();
            } else if (name == "min") {
                return AggregateFactory::MakeMinAggregateFunction<ArgPT>();
            } else if (name == "multi_distinct_count") {
                return AggregateFactory::MakeCountDistinctAggregateFunction<ArgPT>();
            } else if (name == "multi_distinct_count2") {
                return AggregateFactory::MakeCountDistinctAggregateFunctionV2<ArgPT>();
            } else if (name == "group_concat") {
                return AggregateFactory::MakeGroupConcatAggregateFunction<ArgPT>();
            } else if (name == "any_value") {
                return AggregateFactory::MakeAnyValueAggregateFunction<ArgPT>();
            } else if (name == "array_agg") {
                return AggregateFactory::MakeArrayAggAggregateFunction<ArgPT>();
            } else if (name == "percentile_cont") {
                return AggregateFactory::MakePercentileContAggregateFunction<ArgPT>();
            }
        }

        if (name == "lead" || name == "lag") {
            return AggregateFactory::MakeLeadLagWindowFunction<ArgPT>();
        } else if (name == "first_value") {
            return AggregateFactory::MakeFirstValueWindowFunction<ArgPT>();
        } else if (name == "last_value") {
            return AggregateFactory::MakeLastValueWindowFunction<ArgPT>();
        } else if (name == "histogram") {
            return AggregateFactory::MakeHistogramAggregationFunction<ArgPT>();
        }
        return nullptr;
    }

private:
    std::unordered_map<AggregateFuncKey, AggregateFunctionPtr, AggregateFuncMapHash> _infos_mapping;
    AggregateFuncResolver(const AggregateFuncResolver&) = delete;
    const AggregateFuncResolver& operator=(const AggregateFuncResolver&) = delete;
};

#define ADD_ALL_TYPE(FUNCTIONNAME, ADD_WINDOW_VERSION)                                       \
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BOOLEAN, ADD_WINDOW_VERSION>(FUNCTIONNAME);     \
    add_aggregate_mapping<TYPE_TINYINT, TYPE_TINYINT, ADD_WINDOW_VERSION>(FUNCTIONNAME);     \
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_SMALLINT, ADD_WINDOW_VERSION>(FUNCTIONNAME);   \
    add_aggregate_mapping<TYPE_INT, TYPE_INT, ADD_WINDOW_VERSION>(FUNCTIONNAME);             \
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, ADD_WINDOW_VERSION>(FUNCTIONNAME);       \
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, ADD_WINDOW_VERSION>(FUNCTIONNAME);   \
    add_aggregate_mapping<TYPE_FLOAT, TYPE_FLOAT, ADD_WINDOW_VERSION>(FUNCTIONNAME);         \
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, ADD_WINDOW_VERSION>(FUNCTIONNAME);       \
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, ADD_WINDOW_VERSION>(FUNCTIONNAME);     \
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, ADD_WINDOW_VERSION>(FUNCTIONNAME);           \
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, ADD_WINDOW_VERSION>(FUNCTIONNAME); \
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, ADD_WINDOW_VERSION>(FUNCTIONNAME);   \
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE, ADD_WINDOW_VERSION>(FUNCTIONNAME);           \
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, ADD_WINDOW_VERSION>(FUNCTIONNAME); \
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64, ADD_WINDOW_VERSION>(FUNCTIONNAME); \
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, ADD_WINDOW_VERSION>(FUNCTIONNAME);

AggregateFuncResolver::AggregateFuncResolver() {
    // The function should be placed by alphabetical order

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, true>("avg");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, true>("avg");
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE, true>("avg");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("avg");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("avg");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("avg");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_INT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_CHAR, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DATE, TYPE_ARRAY>("array_agg");
    // TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128 is not supported now for array_agg

    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("bitmap_union_int");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("bitmap_union_int");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("bitmap_union_int");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("bitmap_union_int");

    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("count");

    ADD_ALL_TYPE("max", true);
    ADD_ALL_TYPE("min", true);
    ADD_ALL_TYPE("any_value", true);
    add_aggregate_mapping<TYPE_JSON, TYPE_JSON>("any_value");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DATE, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("multi_distinct_count");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("multi_distinct_sum");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DATE, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("multi_distinct_count2");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("multi_distinct_sum2");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT, true>("sum");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, true>("sum");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, true>("sum");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, true>("sum");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, true>("sum");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("sum");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, true>("sum");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, true>("sum");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, true>("sum");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64, true>("sum");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64, true>("sum");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("sum");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("variance");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("variance");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("variance");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("variance_pop");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("variance_pop");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("variance_pop");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("var_pop");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("var_pop");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("var_pop");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("variance_samp");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("variance_samp");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("variance_samp");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("var_samp");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("var_samp");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("var_samp");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("std");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("std");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("std");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("stddev");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("stddev");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("stddev");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("stddev_pop");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("stddev_pop");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("stddev_pop");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("stddev_samp");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("stddev_samp");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("stddev_samp");

    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("dense_rank");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("rank");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("row_number");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("ntile");

    add_aggregate_mapping<TYPE_CHAR, TYPE_VARCHAR>("group_concat");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR>("group_concat");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_INT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DATE, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_CHAR, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR>("histogram");

    ADD_ALL_TYPE("first_value", true);
    ADD_ALL_TYPE("last_value", true);
    ADD_ALL_TYPE("lead", true);
    ADD_ALL_TYPE("lag", true);

    add_object_mapping<TYPE_HLL, TYPE_HLL>("hll_union");
    add_object_mapping<TYPE_HLL, TYPE_HLL>("hll_raw_agg");
    add_object_mapping<TYPE_HLL, TYPE_BIGINT>("hll_union_agg");

    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT>("bitmap_union");
    add_object_mapping<TYPE_OBJECT, TYPE_BIGINT, true>("bitmap_union_count");

    // This first type is the second type input of intersect_count.
    // And the first type is Bitmap.
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_INT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT>("intersect_count");

    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT>("bitmap_intersect");

    add_object_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_INT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("ndv");

    add_object_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_INT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("approx_count_distinct");

    add_object_mapping<TYPE_BOOLEAN, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_TINYINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_SMALLINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_INT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_BIGINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_LARGEINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_FLOAT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DOUBLE, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_CHAR, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_VARCHAR, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMALV2, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DATETIME, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DATE, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMAL32, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMAL64, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMAL128, TYPE_HLL>("hll_raw");

    add_object_mapping<TYPE_BIGINT, TYPE_DOUBLE>("percentile_approx");
    add_object_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("percentile_approx");

    add_object_mapping<TYPE_PERCENTILE, TYPE_PERCENTILE>("percentile_union");

    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("percentile_cont");
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE>("percentile_cont");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME>("percentile_cont");

    add_array_mapping<TYPE_ARRAY, TYPE_VARCHAR>("dict_merge");
    add_array_mapping<TYPE_ARRAY, TYPE_ARRAY>("retention");

    // sum, avg, distinct_sum use decimal128 as intermediate or result type to avoid overflow
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_sum");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_sum");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_sum");
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    // This first type is the 4th type input of windowfunnel.
    // And the 1st type is BigInt, 2nd is datetime, 3rd is mode(default 0).
    add_array_mapping<TYPE_INT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_BIGINT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATETIME, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATE, TYPE_INT>("window_funnel");
}

#undef ADD_ALL_TYPE

AggregateFuncResolver::~AggregateFuncResolver() = default;

static const AggregateFunction* get_function(const std::string& name, PrimitiveType arg_type, PrimitiveType return_type,
                                             bool is_window_function, bool is_null,
                                             TFunctionBinaryType::type binary_type, int func_version) {
    std::string func_name = name;
    if (func_version > 1) {
        if (name == "multi_distinct_sum") {
            func_name = "multi_distinct_sum2";
        } else if (name == "multi_distinct_count") {
            func_name = "multi_distinct_count2";
        }
    }

    auto is_decimal_type = [](PrimitiveType pt) {
        return pt == TYPE_DECIMAL32 || pt == TYPE_DECIMAL64 || pt == TYPE_DECIMAL128;
    };
    if (func_version > 2 && is_decimal_type(arg_type)) {
        if (name == "sum") {
            func_name = "decimal_sum";
        } else if (name == "avg") {
            func_name = "decimal_avg";
        } else if (name == "multi_distinct_sum") {
            func_name = "decimal_multi_distinct_sum";
        }
    }

    if (binary_type == TFunctionBinaryType::BUILTIN) {
        return AggregateFuncResolver::instance()->get_aggregate_info(func_name, arg_type, return_type,
                                                                     is_window_function, is_null);
    } else if (binary_type == TFunctionBinaryType::SRJAR) {
        return getJavaUDAFFunction(is_null);
    }
    return nullptr;
}

const AggregateFunction* get_aggregate_function(const std::string& name, PrimitiveType arg_type,
                                                PrimitiveType return_type, bool is_null,
                                                TFunctionBinaryType::type binary_type, int func_version) {
    return get_function(name, arg_type, return_type, false, is_null, binary_type, func_version);
}

const AggregateFunction* get_window_function(const std::string& name, PrimitiveType arg_type, PrimitiveType return_type,
                                             bool is_null, TFunctionBinaryType::type binary_type, int func_version) {
    if (binary_type == TFunctionBinaryType::BUILTIN) {
        return get_function(name, arg_type, return_type, true, is_null, binary_type, func_version);
    } else if (binary_type == TFunctionBinaryType::SRJAR) {
        return getJavaWindowFunction();
    }
    return nullptr;
}

} // namespace starrocks::vectorized
