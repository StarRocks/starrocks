// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <tuple>
#include <unordered_map>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks::vectorized {

// 1. name
// 2. arg primitive type
// 3. return primitive type
// 4. is_window_function
// 5. is_nullable
using AggregateFuncKey = std::tuple<std::string, int, int, bool, bool>;

struct AggregateFuncMapHash {
    size_t operator()(const AggregateFuncKey& key) const {
        std::hash<std::string> hasher;
        return hasher(std::get<0>(key)) ^ std::get<1>(key) ^ std::get<2>(key) ^ std::get<3>(key) ^ std::get<4>(key);
    }
};

// Infer ResultType from ArgType
template <template <PrimitiveType PT, typename = guard::Guard> class ResultTypeTrait>
struct ResultTypeDispatcher {
    template <PrimitiveType pt>
    PrimitiveType operator()() {
        return ResultTypeTrait<pt>::value;
    }
};

// Build the aggregate function from the function template
template <template <PrimitiveType arg_type, typename = guard::Guard> class ResultTrait,
          template <class T> class StateTypeTrait,
          template <PrimitiveType arg_type, typename T = RunTimeCppType<arg_type>,
                    PrimitiveType res_type = PrimitiveType::TYPE_NULL, typename ResultType = RunTimeCppType<res_type>>
          class FunImpl>
struct AggFunctionDispatcher {
    template <PrimitiveType pt>
    AggregateFunctionPtr operator()(bool nullable, bool is_window) {
        auto agg = std::make_shared<FunImpl<pt>>();
        if (!nullable) {
            return agg;
        }

        using ArgCppType = RunTimeCppType<pt>;
        constexpr PrimitiveType res_type = ResultTrait<pt>::value;
        using ResultCppType = RunTimeCppType<res_type>;
        using StateType = StateTypeTrait<ResultCppType>;
        if (is_window) {
            return AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true>(agg);
        } else {
            return AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, false>(agg);
        }
    }
};

// TODO(murphy) refactor the function creator
class AggregateFuncResolver {
    DECLARE_SINGLETON(AggregateFuncResolver);

public:
    void register_1();
    void register_2();
    void register_3();
    void register_4();
    void register_5();
    void register_6();
    void register_7();

    const AggregateFunction* get_aggregate_info(const std::string& name, const PrimitiveType arg_type,
                                                const PrimitiveType return_type, const bool is_window_function,
                                                const bool is_null) const {
        auto pair = _infos_mapping.find(std::make_tuple(name, arg_type, return_type, is_window_function, is_null));
        if (pair == _infos_mapping.end()) {
            return nullptr;
        }
        return pair->second.get();
    }

    template <class AggData>
    void add_aggregate_mapping(const std::string& name, PrimitiveType arg, PrimitiveType ret, bool is_window,
                               AggregateFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, arg, ret, false, false), fun);
        auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<AggData, false>(fun);
        _infos_mapping.emplace(std::make_tuple(name, arg, ret, false, true), nullable_agg);

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, arg, ret, true, false), fun);
            auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<AggData, true>(fun);
            _infos_mapping.emplace(std::make_tuple(name, arg, ret, true, true), nullable_agg);
        }
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false>
    void add_aggregate_mapping(std::string name) {
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
    void add_object_mapping(std::string name) {
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
    void add_array_mapping(std::string name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false),
                               create_array_function<ArgPT, ResultPT, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true),
                               create_array_function<ArgPT, ResultPT, true>(name));
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false>
    void add_decimal_mapping(std::string name) {
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
    AggregateFunctionPtr create_object_function(std::string name) {
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
        } else if (name == "lead" || name == "lag") {
            return AggregateFactory::MakeLeadLagWindowFunction<ArgPT>();
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
        } else if (name == "max_by") {
            return AggregateFactory::MakeMaxByAggregateFunction<ArgPT>();
        } else if (name == "exchange_bytes") {
            return AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::BYTES>();
        } else if (name == "exchange_speed") {
            return AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::SPEED>();
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
        } else if (name == "max_by") {
            return AggregateFactory::MakeMaxByAggregateFunction<ArgPT>();
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

#define ADD_ALL_TYPE1(FUNCTIONNAME, RET_TYPE)                            \
    add_aggregate_mapping<TYPE_BOOLEAN, RET_TYPE, true>(FUNCTIONNAME);   \
    add_aggregate_mapping<TYPE_TINYINT, RET_TYPE, true>(FUNCTIONNAME);   \
    add_aggregate_mapping<TYPE_SMALLINT, RET_TYPE, true>(FUNCTIONNAME);  \
    add_aggregate_mapping<TYPE_INT, RET_TYPE, true>(FUNCTIONNAME);       \
    add_aggregate_mapping<TYPE_BIGINT, RET_TYPE, true>(FUNCTIONNAME);    \
    add_aggregate_mapping<TYPE_LARGEINT, RET_TYPE, true>(FUNCTIONNAME);  \
    add_aggregate_mapping<TYPE_FLOAT, RET_TYPE, true>(FUNCTIONNAME);     \
    add_aggregate_mapping<TYPE_DOUBLE, RET_TYPE, true>(FUNCTIONNAME);    \
    add_aggregate_mapping<TYPE_VARCHAR, RET_TYPE, true>(FUNCTIONNAME);   \
    add_aggregate_mapping<TYPE_CHAR, RET_TYPE, true>(FUNCTIONNAME);      \
    add_aggregate_mapping<TYPE_DECIMALV2, RET_TYPE, true>(FUNCTIONNAME); \
    add_aggregate_mapping<TYPE_DATETIME, RET_TYPE, true>(FUNCTIONNAME);  \
    add_aggregate_mapping<TYPE_DATE, RET_TYPE, true>(FUNCTIONNAME);      \
    add_aggregate_mapping<TYPE_DECIMAL32, RET_TYPE, true>(FUNCTIONNAME); \
    add_aggregate_mapping<TYPE_DECIMAL64, RET_TYPE, true>(FUNCTIONNAME); \
    add_aggregate_mapping<TYPE_DECIMAL128, RET_TYPE, true>(FUNCTIONNAME);

// #undef ADD_ALL_TYPE

} // namespace starrocks::vectorized
