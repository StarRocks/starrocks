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
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks {

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

class AggregateFuncResolver {
    DECLARE_SINGLETON(AggregateFuncResolver);

public:
    void register_avg();
    void register_bitmap();
    void register_minmaxany();
    void register_sumcount();
    void register_distinct();
    void register_variance();
    void register_window();
    void register_utility();
    void register_approx();
    void register_others();
    void register_retract_functions();

    const std::vector<LogicalType>& aggregate_types() const {
        const static std::vector<LogicalType> kTypes{
                TYPE_BOOLEAN,   TYPE_TINYINT,   TYPE_SMALLINT,  TYPE_INT,        TYPE_BIGINT, TYPE_LARGEINT,
                TYPE_FLOAT,     TYPE_DOUBLE,    TYPE_VARCHAR,   TYPE_CHAR,       TYPE_DATE,   TYPE_DATETIME,
                TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_HLL,    TYPE_OBJECT};
        return kTypes;
    }

    const AggregateFunction* get_aggregate_info(const std::string& name, const LogicalType arg_type,
                                                const LogicalType return_type, const bool is_window_function,
                                                const bool is_null) const {
        auto pair = _infos_mapping.find(std::make_tuple(name, arg_type, return_type, is_window_function, is_null));
        if (pair == _infos_mapping.end()) {
            return nullptr;
        }
        return pair->second.get();
    }

    template <LogicalType ArgType, LogicalType RetType, typename SpecificAggFunctionPtr = AggregateFunctionPtr>
    void add_aggregate_mapping_notnull(const std::string& name, bool is_window, SpecificAggFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), fun);
        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), fun);
        }
    }

    template <LogicalType ArgType, LogicalType RetType, class StateType,
              typename SpecificAggFunctionPtr = AggregateFunctionPtr, bool IgnoreNull = true>
    void add_aggregate_mapping(const std::string& name, bool is_window, SpecificAggFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, false, IgnoreNull>(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), nullable_agg);

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true, IgnoreNull>(fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), nullable_agg);
        }
    }

    template <LogicalType ArgType, LogicalType RetType, class StateType,
              typename SpecificAggFunctionPtr = AggregateFunctionPtr>
    void add_aggregate_mapping_variadic(const std::string& name, bool is_window, SpecificAggFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        auto variadic_agg = AggregateFactory::MakeNullableAggregateFunctionVariadic<StateType>(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), variadic_agg);

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            auto variadic_agg = AggregateFactory::MakeNullableAggregateFunctionVariadic<StateType>(fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), variadic_agg);
        }
    }

    template <LogicalType ArgPT, LogicalType ResultPT>
    void add_array_mapping(std::string name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false),
                               create_array_function<ArgPT, ResultPT, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true),
                               create_array_function<ArgPT, ResultPT, true>(name));
    }

    template <LogicalType ArgPT, LogicalType ResultPT, bool AddWindowVersion = false>
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

    template <LogicalType ArgPT, LogicalType ResultPT, bool IsNull>
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

    template <LogicalType ArgPT, LogicalType ResultPT, bool IsWindowFunc, bool IsNull>
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

    AggregateFuncResolver(const AggregateFuncResolver&) = delete;
    const AggregateFuncResolver& operator=(const AggregateFuncResolver&) = delete;

private:
    std::unordered_map<AggregateFuncKey, AggregateFunctionPtr, AggregateFuncMapHash> _infos_mapping;
};

} // namespace starrocks
