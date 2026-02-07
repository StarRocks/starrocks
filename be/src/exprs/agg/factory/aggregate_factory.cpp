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

#include "exprs/agg/aggregate_factory.h"

#include <memory>

#include "base/failpoint/fail_point.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks {

DEFINE_FAIL_POINT(not_exist_agg_function);

AggregateFuncResolver::AggregateFuncResolver() {
    register_avg();
    register_minmaxany();
    register_bitmap();
    register_sumcount();
    register_distinct();
    register_variance();
    register_window();
    register_utility();
    register_approx();
    register_others();
    register_retract_functions();
    register_hypothesis_testing();
    register_boolean();
}

AggregateFuncResolver::~AggregateFuncResolver() {
    for (const auto* func : _functions) {
        delete func;
    }
}

AggregateFunctionPtr AggregateFactory::MakeBitmapUnionAggregateFunction() {
    return new BitmapUnionAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapIntersectAggregateFunction() {
    return new BitmapIntersectAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapUnionCountAggregateFunction() {
    return new BitmapUnionCountAggregateFunction();
}
AggregateFunctionPtr AggregateFactory::MakeDictMergeAggregateFunction() {
    return new DictMergeAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakeRetentionAggregateFunction() {
    return new RetentionAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakeHllUnionAggregateFunction() {
    return new HllUnionAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakeHllUnionCountAggregateFunction() {
    return new HllUnionCountAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakePercentileApproxAggregateFunction() {
    return new PercentileApproxAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakePercentileApproxArrayAggregateFunction() {
    return new PercentileApproxArrayAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakePercentileApproxWeightedAggregateFunction() {
    return new PercentileApproxWeightedAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakePercentileApproxWeightedArrayAggregateFunction() {
    return new PercentileApproxWeightedArrayAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakePercentileUnionAggregateFunction() {
    return new PercentileUnionAggregateFunction();
}

AggregateFunctionPtr AggregateFactory::MakeDenseRankWindowFunction() {
    return new DenseRankWindowFunction();
}

AggregateFunctionPtr AggregateFactory::MakeRankWindowFunction() {
    return new RankWindowFunction();
}

AggregateFunctionPtr AggregateFactory::MakeRowNumberWindowFunction() {
    return new RowNumberWindowFunction();
}

AggregateFunctionPtr AggregateFactory::MakeCumeDistWindowFunction() {
    return new CumeDistWindowFunction();
}

AggregateFunctionPtr AggregateFactory::MakePercentRankWindowFunction() {
    return new PercentRankWindowFunction();
}

AggregateFunctionPtr AggregateFactory::MakeNtileWindowFunction() {
    return new NtileWindowFunction();
}

static AggregateFunctionPtr get_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                         bool is_window_function, bool is_null, TFunctionBinaryType::type binary_type,
                                         int func_version) {
    std::string func_name = name;
    if (func_version > 1) {
        if (name == "multi_distinct_sum") {
            func_name = "multi_distinct_sum2";
        } else if (name == "multi_distinct_count") {
            func_name = "multi_distinct_count2";
        }
    }

    auto is_decimal_type = [](LogicalType lt) {
        return lt == TYPE_DECIMAL32 || lt == TYPE_DECIMAL64 || lt == TYPE_DECIMAL128 || lt == TYPE_DECIMAL256;
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

    if (func_version > 5) {
        if (name == "array_agg") {
            func_name = "array_agg2";
        }
    }

    if (func_version > 6) {
        if (name == "group_concat") {
            func_name = "group_concat2";
        }
    }

    if (binary_type == TFunctionBinaryType::BUILTIN) {
        auto func = AggregateFuncResolver::instance()->get_aggregate_info(func_name, arg_type, return_type,
                                                                          is_window_function, is_null);
        if (func != nullptr) {
            return func;
        }
        return AggregateFuncResolver::instance()->get_general_info(func_name, is_window_function, is_null);
    } else if (binary_type == TFunctionBinaryType::SRJAR) {
        return getJavaUDAFFunction(is_null);
    }
    return nullptr;
}

AggregateFunctionPtr get_aggregate_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                            bool is_null, TFunctionBinaryType::type binary_type, int func_version) {
    FAIL_POINT_TRIGGER_RETURN(not_exist_agg_function, nullptr);
    return get_function(name, arg_type, return_type, false, is_null, binary_type, func_version);
}

AggregateFunctionPtr get_window_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                         bool is_null, TFunctionBinaryType::type binary_type, int func_version) {
    if (binary_type == TFunctionBinaryType::BUILTIN) {
        return get_function(name, arg_type, return_type, true, is_null, binary_type, func_version);
    } else if (binary_type == TFunctionBinaryType::SRJAR) {
        return getJavaWindowFunction();
    }
    return nullptr;
}

AggregateFunctionPtr get_aggregate_function(const std::string& agg_func_name, const TypeDescriptor& return_type,
                                            const std::vector<TypeDescriptor>& arg_types, bool is_result_nullable,
                                            TFunctionBinaryType::type binary_type, int func_version) {
    // get function
    if (agg_func_name == "count") {
        return get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, is_result_nullable);
    } else {
        DCHECK_GE(arg_types.size(), 1);
        TypeDescriptor arg_type = arg_types[0];
        TypeDescriptor ret_type = return_type;
        // Because intersect_count have two input types.
        // And intersect_count's first argument's type is alwasy Bitmap,
        // so we use its second arguments type as input.
        if (agg_func_name == "intersect_count") {
            arg_type = arg_types[1];
        }

        // Because max_by and min_by function have two input types,
        // so we use its second arguments type as input.
        if (agg_func_name == "max_by" || agg_func_name == "min_by" || agg_func_name == "max_by_v2" ||
            agg_func_name == "min_by_v2") {
            arg_type = arg_types[1];
        }

        // Because windowfunnel have more two input types.
        // functions registry use 2th args(datetime/date).
        if (agg_func_name == "window_funnel") {
            arg_type = arg_types[1];
        }

        // hack for accepting various arguments
        if (agg_func_name == "exchange_bytes" || agg_func_name == "exchange_speed") {
            arg_type = TypeDescriptor(TYPE_BIGINT);
        }

        if (agg_func_name == "array_union_agg" || agg_func_name == "array_unique_agg") {
            // NOTE: Do not assign from `arg_type.children[0]` directly.
            // `TypeDescriptor::operator=` will destroy `arg_type.children` first, which would also
            // destroy the RHS object (a child element) and cause heap-use-after-free.
            DCHECK_GE(arg_type.children.size(), 1);
            TypeDescriptor child_type = arg_type.children[0];
            arg_type = std::move(child_type);
        }

        if (agg_func_name == "sum_map") {
            ret_type = arg_type.children[1];
            arg_type = arg_type.children[0];
        }

        return get_aggregate_function(agg_func_name, arg_type.type, ret_type.type, is_result_nullable, binary_type,
                                      func_version);
    }
}

} // namespace starrocks
