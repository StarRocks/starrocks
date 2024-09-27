// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"

#include <memory>
#include <tuple>
#include <unordered_map>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks::vectorized {

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
}

AggregateFuncResolver::~AggregateFuncResolver() = default;

AggregateFunctionPtr AggregateFactory::MakeBitmapUnionAggregateFunction() {
    return std::make_shared<BitmapUnionAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapIntersectAggregateFunction() {
    return std::make_shared<BitmapIntersectAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeBitmapUnionCountAggregateFunction() {
    return std::make_shared<BitmapUnionCountAggregateFunction>();
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

AggregateFunctionPtr AggregateFactory::MakePercentileApproxAggregateFunction() {
    return std::make_shared<PercentileApproxAggregateFunction>();
}

AggregateFunctionPtr AggregateFactory::MakePercentileUnionAggregateFunction() {
    return std::make_shared<PercentileUnionAggregateFunction>();
}
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

<<<<<<< HEAD
} // namespace starrocks::vectorized
=======
const AggregateFunction* get_aggregate_function(const std::string& agg_func_name, const TypeDescriptor& return_type,
                                                const std::vector<TypeDescriptor>& arg_types, bool is_result_nullable,
                                                TFunctionBinaryType::type binary_type, int func_version) {
    // get function
    if (agg_func_name == "count") {
        return get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, is_result_nullable);
    } else {
        DCHECK_GE(arg_types.size(), 1);
        TypeDescriptor arg_type = arg_types[0];
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
            arg_type = arg_type.children[0];
        }
        return get_aggregate_function(agg_func_name, arg_type.type, return_type.type, is_result_nullable, binary_type,
                                      func_version);
    }
}

} // namespace starrocks
>>>>>>> 9398edd4af ([BugFix] MaxBy/MinBy not filter nulls (#51354))
