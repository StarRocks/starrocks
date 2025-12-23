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
#include <unordered_set>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "types/logical_type.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks {

// 1. name
// 2. arg logical type
// 3. return logical type
// 4. is_window_function
// 5. is_nullable
using AggregateFuncKey = std::tuple<std::string, int, int, bool, bool>;

// 1. name
// 2. is_window_function
// 3. is_nullable
using GeneralFuncKey = std::tuple<std::string, bool, bool>;

struct AggregateFuncMapHash {
    size_t operator()(const AggregateFuncKey& key) const {
        std::hash<std::string> hasher;
        return hasher(std::get<0>(key)) ^ std::get<1>(key) ^ std::get<2>(key) ^ std::get<3>(key) ^ std::get<4>(key);
    }
};

struct GeneralFuncMapHash {
    size_t operator()(const GeneralFuncKey& key) const {
        std::hash<std::string> hasher;
        return hasher(std::get<0>(key)) ^ std::get<1>(key) ^ std::get<2>(key);
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
    void register_hypothesis_testing();
    void register_boolean();

    const std::vector<LogicalType>& aggregate_types() const {
        const static std::vector<LogicalType> kTypes{TYPE_BOOLEAN,    TYPE_TINYINT,   TYPE_SMALLINT,  TYPE_INT,
                                                     TYPE_BIGINT,     TYPE_LARGEINT,  TYPE_FLOAT,     TYPE_DOUBLE,
                                                     TYPE_VARCHAR,    TYPE_CHAR,      TYPE_DATE,      TYPE_DATETIME,
                                                     TYPE_DECIMALV2,  TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128,
                                                     TYPE_DECIMAL256, TYPE_HLL,       TYPE_OBJECT,    TYPE_ARRAY};
        return kTypes;
    }

    const AggregateFunction* get_aggregate_info(const std::string& name, const LogicalType arg_type,
                                                const LogicalType return_type, const bool is_window_function,
                                                const bool is_null) const {
        auto pair = _infos_mapping.find(std::make_tuple(name, arg_type, return_type, is_window_function, is_null));
        if (pair == _infos_mapping.end()) {
            return nullptr;
        }
        return pair->second;
    }

    const AggregateFunction* get_general_info(const std::string& name, const bool is_window_function,
                                              const bool is_null) const {
        auto pair = _general_mapping.find(std::make_tuple(name, is_window_function, is_null));
        if (pair == _general_mapping.end()) {
            return nullptr;
        }
        return pair->second;
    }

    void add_general_mapping_notnull(const std::string& name, bool is_window, const AggregateFunction* fun) {
        track_function(fun);
        _general_mapping.emplace(std::make_tuple(name, false, false), fun);
        _general_mapping.emplace(std::make_tuple(name, false, true), fun);
        if (is_window) {
            _general_mapping.emplace(std::make_tuple(name, true, false), fun);
            _general_mapping.emplace(std::make_tuple(name, true, true), fun);
        }
    }

    void add_general_window_mapping_notnull(const std::string& name, const AggregateFunction* fun) {
        track_function(fun);
        _general_mapping.emplace(std::make_tuple(name, true, false), fun);
        _general_mapping.emplace(std::make_tuple(name, true, true), fun);
    }

    template <class StateType, bool IgnoreNull = true>
    void add_general_mapping(const std::string& name, bool is_window, const AggregateFunction* fun) {
        track_function(fun);
        _general_mapping.emplace(std::make_tuple(name, false, false), fun);
        auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, false, IgnoreNull>(fun);
        _general_mapping.emplace(std::make_tuple(name, false, true), track_function(nullable_agg));

        if (is_window) {
            _general_mapping.emplace(std::make_tuple(name, true, false), fun);
            auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true, IgnoreNull>(fun);
            _general_mapping.emplace(std::make_tuple(name, true, true), track_function(nullable_agg));
        }
    }

    template <LogicalType ArgType, LogicalType RetType>
    void add_aggregate_mapping_notnull(const std::string& name, bool is_window, const AggregateFunction* fun) {
        track_function(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), fun);
        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), fun);
        }
    }

    template <LogicalType ArgType, LogicalType RetType, class StateType, bool IgnoreNull = true,
              IsAggNullPred<StateType> AggNullPred = AggNonNullPred<StateType>>
    void add_aggregate_mapping(const std::string& name, bool is_window, const AggregateFunction* fun,
                               AggNullPred null_pred = AggNullPred()) {
        track_function(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        auto nullable_agg =
                AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, false, IgnoreNull>(fun, null_pred);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), track_function(nullable_agg));

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true, IgnoreNull>(
                    fun, std::move(null_pred));
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), track_function(nullable_agg));
        }
    }

    template <LogicalType ArgType, LogicalType RetType, class StateType, bool IgnoreNull = true,
              IsAggNullPred<StateType> AggNullPred = AggNonNullPred<StateType>>
    void add_window_mapping(const std::string& name, const AggregateFunction* fun,
                            AggNullPred null_pred = AggNullPred()) {
        track_function(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
        auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true, IgnoreNull>(
                fun, std::move(null_pred));
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), track_function(nullable_agg));
    }

    template <LogicalType ArgType, LogicalType RetType, class StateType,
              IsAggNullPred<StateType> AggNullPred = AggNonNullPred<StateType>>
    void add_aggregate_mapping_variadic(const std::string& name, bool is_window, const AggregateFunction* fun,
                                        AggNullPred null_pred = AggNullPred()) {
        track_function(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        auto variadic_agg = AggregateFactory::MakeNullableAggregateFunctionVariadic<StateType>(fun, null_pred);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), track_function(variadic_agg));

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            auto variadic_agg =
                    AggregateFactory::MakeNullableAggregateFunctionVariadic<StateType>(fun, std::move(null_pred));
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), track_function(variadic_agg));
        }
    }

    template <LogicalType ArgLT, LogicalType ResultLT>
    void add_array_mapping(std::string name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgLT, ResultLT, false, false),
                               create_array_function<ArgLT, ResultLT, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgLT, ResultLT, false, true),
                               create_array_function<ArgLT, ResultLT, true>(name));
    }

    template <LogicalType ArgLT, LogicalType ResultLT, bool AddWindowVersion = false>
    void add_decimal_mapping(std::string name) {
        _infos_mapping.emplace(std::make_tuple(name, ArgLT, ResultLT, false, false),
                               create_decimal_function<ArgLT, ResultLT, false, false>(name));
        _infos_mapping.emplace(std::make_tuple(name, ArgLT, ResultLT, false, true),
                               create_decimal_function<ArgLT, ResultLT, false, true>(name));
        if constexpr (AddWindowVersion) {
            _infos_mapping.emplace(std::make_tuple(name, ArgLT, ResultLT, true, false),
                                   create_decimal_function<ArgLT, ResultLT, true, false>(name));
            _infos_mapping.emplace(std::make_tuple(name, ArgLT, ResultLT, true, true),
                                   create_decimal_function<ArgLT, ResultLT, true, true>(name));
        }
    }

    template <LogicalType ArgLT, LogicalType ResultLT, bool IsNull>
    const AggregateFunction* create_array_function(std::string& name) {
        if constexpr (IsNull) {
            if (name == "dict_merge") {
                auto dict_merge = track_function(AggregateFactory::MakeDictMergeAggregateFunction());
                return track_function(
                        AggregateFactory::MakeNullableAggregateFunctionUnary<DictMergeState, false>(dict_merge));
            } else if (name == "retention") {
                auto retentoin = track_function(AggregateFactory::MakeRetentionAggregateFunction());
                return track_function(
                        AggregateFactory::MakeNullableAggregateFunctionUnary<RetentionState, false>(retentoin));
            } else if (name == "window_funnel") {
                if constexpr (ArgLT == TYPE_INT || ArgLT == TYPE_BIGINT || ArgLT == TYPE_DATE ||
                              ArgLT == TYPE_DATETIME) {
                    auto windowfunnel = track_function(AggregateFactory::MakeWindowfunnelAggregateFunction<ArgLT>());
                    return track_function(
                            AggregateFactory::MakeNullableAggregateFunctionVariadic<WindowFunnelState<ArgLT>>(
                                    windowfunnel));
                }
            }
        } else {
            if (name == "dict_merge") {
                return track_function(AggregateFactory::MakeDictMergeAggregateFunction());
            } else if (name == "retention") {
                return track_function(AggregateFactory::MakeRetentionAggregateFunction());
            } else if (name == "window_funnel") {
                if constexpr (ArgLT == TYPE_INT || ArgLT == TYPE_BIGINT || ArgLT == TYPE_DATE ||
                              ArgLT == TYPE_DATETIME) {
                    return track_function(AggregateFactory::MakeWindowfunnelAggregateFunction<ArgLT>());
                }
            }
        }

        return nullptr;
    }

    template <LogicalType ArgLT, LogicalType ResultLT, bool IsWindowFunc, bool IsNull>
    std::enable_if_t<isArithmeticLT<ArgLT>, const AggregateFunction*> create_decimal_function(std::string& name) {
        static_assert(lt_is_decimal128<ResultLT> || lt_is_decimal256<ResultLT>);
        if constexpr (IsNull) {
            using ResultType = RunTimeCppType<ResultLT>;
            if (name == "decimal_avg") {
                auto avg = track_function(AggregateFactory::MakeDecimalAvgAggregateFunction<ArgLT>());
                return track_function(
                        AggregateFactory::MakeNullableAggregateFunctionUnary<AvgAggregateState<ResultType>,
                                                                             IsWindowFunc>(avg));
            } else if (name == "decimal_sum") {
                auto sum = track_function(AggregateFactory::MakeDecimalSumAggregateFunction<ArgLT>());
                return track_function(
                        AggregateFactory::MakeNullableAggregateFunctionUnary<AvgAggregateState<ResultType>,
                                                                             IsWindowFunc>(sum));
            } else if (name == "decimal_multi_distinct_sum") {
                auto distinct_sum = track_function(AggregateFactory::MakeDecimalSumDistinctAggregateFunction<ArgLT>());
                return track_function(
                        AggregateFactory::MakeNullableAggregateFunctionUnary<DistinctAggregateState<ArgLT, ResultLT>,
                                                                             IsWindowFunc>(distinct_sum));
            }
        } else {
            if (name == "decimal_avg") {
                return track_function(AggregateFactory::MakeDecimalAvgAggregateFunction<ArgLT>());
            } else if (name == "decimal_sum") {
                return track_function(AggregateFactory::MakeDecimalSumAggregateFunction<ArgLT>());
            } else if (name == "decimal_multi_distinct_sum") {
                return track_function(AggregateFactory::MakeDecimalSumDistinctAggregateFunction<ArgLT>());
            }
        }
        return nullptr;
    }

    AggregateFuncResolver(const AggregateFuncResolver&) = delete;
    const AggregateFuncResolver& operator=(const AggregateFuncResolver&) = delete;

    template <typename T>
    const T* track_function(const T* func) {
        if (func != nullptr) {
            _functions.insert(func);
        }
        return func;
    }

private:
    std::unordered_map<AggregateFuncKey, const AggregateFunction*, AggregateFuncMapHash> _infos_mapping;
    std::unordered_map<GeneralFuncKey, const AggregateFunction*, GeneralFuncMapHash> _general_mapping;
    std::unordered_set<const AggregateFunction*> _functions;
};

} // namespace starrocks
