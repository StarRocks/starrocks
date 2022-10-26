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

    template <PrimitiveType ArgType, PrimitiveType RetType>
    void add_aggregate_mapping_notnull(const std::string& name, bool is_window, AggregateFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), fun);
        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), fun);
        }
    }

    template <PrimitiveType ArgType, PrimitiveType RetType, template <PrimitiveType> class FuncBuilder>
    void add_aggregate_mapping_notnull(const std::string& name, bool is_window) {
        auto fun = FuncBuilder<ArgType>()();
        add_aggregate_mapping_notnull<ArgType, RetType>(name, is_window, fun);
    }

    template <PrimitiveType ArgType>
    void add_aggregate_window(const std::string& name, AggregateFunctionPtr fun) {
        using ArgCppType = RunTimeCppType<ArgType>;
        _infos_mapping.emplace(std::make_tuple(name, ArgType, ArgType, false, false), fun);
    }

    template <PrimitiveType ArgType, PrimitiveType RetType, class StateType>
    void add_aggregate_mapping(const std::string& name, bool is_window, AggregateFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, false>(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), nullable_agg);

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true>(fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), nullable_agg);
        }
    }

    template <PrimitiveType ArgType, PrimitiveType RetType, class StateType>
    void add_aggregate_mapping_variadic(const std::string& name, bool is_window, AggregateFunctionPtr fun) {
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, false), fun);
        auto variadic_agg = AggregateFactory::MakeNullableAggregateFunctionVariadic<StateType>(fun);
        _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, false, true), variadic_agg);

        if (is_window) {
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, false), fun);
            auto variadic_agg = AggregateFactory::MakeNullableAggregateFunctionVariadic<StateType>(fun);
            _infos_mapping.emplace(std::make_tuple(name, ArgType, RetType, true, true), variadic_agg);
        }
    }

    template <PrimitiveType ArgType, template <PrimitiveType, typename = guard::Guard> class StateTrait>
    void add_aggregate_mapping(const std::string& name, bool is_window, AggregateFunctionPtr fun) {
        add_aggregate_mapping<ArgType, ArgType, StateTrait<ArgType>>(name, is_window, fun);
    }

    template <PrimitiveType ArgType, PrimitiveType RetType, template <class> class StateTrait>
    void add_aggregate_mapping(const std::string& name, bool is_window, AggregateFunctionPtr fun) {
        using ArgCppType = RunTimeCppType<ArgType>;
        using StateType = StateTrait<ArgCppType>;
        add_aggregate_mapping<ArgType, RetType, StateType>(name, is_window, fun);
    }

    template <PrimitiveType ArgType, PrimitiveType RetType, template <PrimitiveType> class StateTrait,
              template <PrimitiveType> class FuncBuilder>
    void add_aggregate_mapping(const std::string& name, bool is_window) {
        using StateType = StateTrait<ArgType>;
        AggregateFunctionPtr fun = FuncBuilder<ArgType>()();
        add_aggregate_mapping<ArgType, RetType, StateType>(name, is_window, fun);
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

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false, class StateType>
    void add_object_mapping(std::string name, AggregateFunctionPtr func) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false), func);
        auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, false>(func);
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true), nullable_agg);

        if constexpr (AddWindowVersion) {
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, false), func);
            auto nullable_agg = AggregateFactory::MakeNullableAggregateFunctionUnary<StateType, true>(func);
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, true), nullable_agg);
        }
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, template <PrimitiveType> class StateTrait,
              template <PrimitiveType> class FuncBuilder>
    void add_object_mapping(std::string name, bool is_window) {
        auto func = FuncBuilder<ArgPT>()();
        using StateType = StateTrait<ArgPT>;
        if (is_window) {
            add_object_mapping<ArgPT, ResultPT, true, StateType>(name, func);
        } else {
            add_object_mapping<ArgPT, ResultPT, false, StateType>(name, func);
        }
    }

    template <PrimitiveType ArgPT, PrimitiveType ResultPT, bool AddWindowVersion = false>
    void add_object_mapping(std::string name, AggregateFunctionPtr func) {
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, false), func);
        _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, false, true), func);
        if constexpr (AddWindowVersion) {
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, false), func);
            _infos_mapping.emplace(std::make_tuple(name, ArgPT, ResultPT, true, true), func);
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

    template <PrimitiveType ArgPT, PrimitiveType ReturnPT, bool IsWindowFunc, bool IsNull>
    AggregateFunctionPtr create_function(std::string& name) {
        if (name == "max_by") {
            return AggregateFactory::MakeMaxByAggregateFunction<ArgPT>();
        } else if (name == "count") {
            if constexpr (IsNull) {
                return AggregateFactory::MakeCountNullableAggregateFunction<IsWindowFunc>();
            } else {
                return AggregateFactory::MakeCountAggregateFunction<IsWindowFunc>();
            }
        }
        return nullptr;
    }

private:
    std::unordered_map<AggregateFuncKey, AggregateFunctionPtr, AggregateFuncMapHash> _infos_mapping;
    AggregateFuncResolver(const AggregateFuncResolver&) = delete;
    const AggregateFuncResolver& operator=(const AggregateFuncResolver&) = delete;
};

// TODO(murphy) refactor it into a type dispatch
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

// TODO(murphy) refactor it into a type dispatch
#define AGGREGATE_ALL_TYPE_FROM_TRAIT(FUNCTIONNAME, WINDOW, RESULT_TRAIT, STATE_TRAIT, FUNC_BUILDER)                  \
    add_aggregate_mapping<TYPE_BOOLEAN, RESULT_TRAIT<TYPE_BOOLEAN>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping<TYPE_TINYINT, RESULT_TRAIT<TYPE_TINYINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping<TYPE_SMALLINT, RESULT_TRAIT<TYPE_SMALLINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,        \
                                                                                                 WINDOW);             \
    add_aggregate_mapping<TYPE_INT, RESULT_TRAIT<TYPE_INT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);         \
    add_aggregate_mapping<TYPE_BIGINT, RESULT_TRAIT<TYPE_BIGINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping<TYPE_LARGEINT, RESULT_TRAIT<TYPE_LARGEINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,        \
                                                                                                 WINDOW);             \
    add_aggregate_mapping<TYPE_FLOAT, RESULT_TRAIT<TYPE_FLOAT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_aggregate_mapping<TYPE_DOUBLE, RESULT_TRAIT<TYPE_DOUBLE>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping<TYPE_DECIMALV2, RESULT_TRAIT<TYPE_DECIMALV2>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,      \
                                                                                                   WINDOW);           \
    add_aggregate_mapping<TYPE_DATETIME, RESULT_TRAIT<TYPE_DATETIME>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,        \
                                                                                                 WINDOW);             \
    add_aggregate_mapping<TYPE_DATE, RESULT_TRAIT<TYPE_DATE>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);       \
    add_aggregate_mapping<TYPE_DECIMAL32, RESULT_TRAIT<TYPE_DECIMAL32>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,      \
                                                                                                   WINDOW);           \
    add_aggregate_mapping<TYPE_DECIMAL64, RESULT_TRAIT<TYPE_DECIMAL64>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,      \
                                                                                                   WINDOW);           \
    add_aggregate_mapping<TYPE_DECIMAL128, RESULT_TRAIT<TYPE_DECIMAL128>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,    \
                                                                                                     WINDOW);

// TODO(murphy) refactor it into a type dispatch
#define AGGREGATE_ALL_OBJECT_TYPE_FROM_TRAIT(FUNCTIONNAME, WINDOW, RESULT_TRAIT, STATE_TRAIT, FUNC_BUILDER)            \
    add_object_mapping<TYPE_BOOLEAN, RESULT_TRAIT<TYPE_BOOLEAN>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_object_mapping<TYPE_TINYINT, RESULT_TRAIT<TYPE_TINYINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_object_mapping<TYPE_SMALLINT, RESULT_TRAIT<TYPE_SMALLINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_object_mapping<TYPE_INT, RESULT_TRAIT<TYPE_INT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);             \
    add_object_mapping<TYPE_BIGINT, RESULT_TRAIT<TYPE_BIGINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);       \
    add_object_mapping<TYPE_LARGEINT, RESULT_TRAIT<TYPE_LARGEINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_object_mapping<TYPE_FLOAT, RESULT_TRAIT<TYPE_FLOAT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);         \
    add_object_mapping<TYPE_DOUBLE, RESULT_TRAIT<TYPE_DOUBLE>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);       \
    add_object_mapping<TYPE_CHAR, RESULT_TRAIT<TYPE_CHAR>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);           \
    add_object_mapping<TYPE_VARCHAR, RESULT_TRAIT<TYPE_VARCHAR>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_object_mapping<TYPE_DECIMALV2, RESULT_TRAIT<TYPE_DECIMALV2>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_object_mapping<TYPE_DATETIME, RESULT_TRAIT<TYPE_DATETIME>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_object_mapping<TYPE_DATE, RESULT_TRAIT<TYPE_DATE>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);           \
    add_object_mapping<TYPE_DECIMAL32, RESULT_TRAIT<TYPE_DECIMAL32>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_object_mapping<TYPE_DECIMAL64, RESULT_TRAIT<TYPE_DECIMAL64>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_object_mapping<TYPE_DECIMAL128, RESULT_TRAIT<TYPE_DECIMAL128>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);

// TODO(murphy) refactor it into a type dispatch
#define AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT(FUNCTIONNAME, WINDOW, RESULT_TRAIT, FUNC_BUILDER)                      \
    add_aggregate_mapping_notnull<TYPE_BOOLEAN, RESULT_TRAIT<TYPE_BOOLEAN>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_aggregate_mapping_notnull<TYPE_TINYINT, RESULT_TRAIT<TYPE_TINYINT>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_aggregate_mapping_notnull<TYPE_SMALLINT, RESULT_TRAIT<TYPE_SMALLINT>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping_notnull<TYPE_INT, RESULT_TRAIT<TYPE_INT>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);             \
    add_aggregate_mapping_notnull<TYPE_BIGINT, RESULT_TRAIT<TYPE_BIGINT>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);       \
    add_aggregate_mapping_notnull<TYPE_LARGEINT, RESULT_TRAIT<TYPE_LARGEINT>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping_notnull<TYPE_FLOAT, RESULT_TRAIT<TYPE_FLOAT>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);         \
    add_aggregate_mapping_notnull<TYPE_DOUBLE, RESULT_TRAIT<TYPE_DOUBLE>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);       \
    add_aggregate_mapping_notnull<TYPE_DATETIME, RESULT_TRAIT<TYPE_DATETIME>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping_notnull<TYPE_DATE, RESULT_TRAIT<TYPE_DATE>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);           \
    add_aggregate_mapping_notnull<TYPE_CHAR, RESULT_TRAIT<TYPE_CHAR>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);           \
    add_aggregate_mapping_notnull<TYPE_VARCHAR, RESULT_TRAIT<TYPE_VARCHAR>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_aggregate_mapping_notnull<TYPE_DECIMALV2, RESULT_TRAIT<TYPE_DECIMALV2>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping_notnull<TYPE_DECIMAL32, RESULT_TRAIT<TYPE_DECIMAL32>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping_notnull<TYPE_DECIMAL64, RESULT_TRAIT<TYPE_DECIMAL64>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping_notnull<TYPE_DECIMAL128, RESULT_TRAIT<TYPE_DECIMAL128>, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);

// TODO(murphy) refactor it into a type dispatch
#define AGGREGATE_NUMERIC1_TYPE_FROM_TRAIT(FUNCTIONNAME, WINDOW, RESULT_TRAIT, STATE_TRAIT, FUNC_BUILDER)             \
    add_aggregate_mapping<TYPE_BOOLEAN, RESULT_TRAIT<TYPE_BOOLEAN>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping<TYPE_TINYINT, RESULT_TRAIT<TYPE_TINYINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW); \
    add_aggregate_mapping<TYPE_SMALLINT, RESULT_TRAIT<TYPE_SMALLINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,        \
                                                                                                 WINDOW);             \
    add_aggregate_mapping<TYPE_INT, RESULT_TRAIT<TYPE_INT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);         \
    add_aggregate_mapping<TYPE_BIGINT, RESULT_TRAIT<TYPE_BIGINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping<TYPE_LARGEINT, RESULT_TRAIT<TYPE_LARGEINT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,        \
                                                                                                 WINDOW);             \
    add_aggregate_mapping<TYPE_FLOAT, RESULT_TRAIT<TYPE_FLOAT>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);     \
    add_aggregate_mapping<TYPE_DOUBLE, RESULT_TRAIT<TYPE_DOUBLE>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME, WINDOW);   \
    add_aggregate_mapping<TYPE_DECIMALV2, RESULT_TRAIT<TYPE_DECIMALV2>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,      \
                                                                                                   WINDOW);           \
    add_aggregate_mapping<TYPE_DECIMAL128, RESULT_TRAIT<TYPE_DECIMAL128>, STATE_TRAIT, FUNC_BUILDER>(FUNCTIONNAME,    \
                                                                                                     WINDOW);

// TODO(murphy) refactor it into a type dispatch
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
