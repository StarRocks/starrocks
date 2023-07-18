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
#include <tuple>
#include <unordered_map>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "udf/java/java_function_fwd.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {

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

AggregateFunctionPtr AggregateFactory::MakeCumeDistWindowFunction() {
    return std::make_shared<CumeDistWindowFunction>();
}

AggregateFunctionPtr AggregateFactory::MakePercentRankWindowFunction() {
    return std::make_shared<PercentRankWindowFunction>();
}

AggregateFunctionPtr AggregateFactory::MakeNtileWindowFunction() {
    return std::make_shared<NtileWindowFunction>();
}

static const AggregateFunction* get_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                             bool is_window_function, bool is_null,
                                             TFunctionBinaryType::type binary_type, int func_version) {
    FAIL_POINT_TRIGGER_RETURN(rand_error_during_prepare, nullptr);
    std::string func_name = name;
    if (func_version > 1) {
        if (name == "multi_distinct_sum") {
            func_name = "multi_distinct_sum2";
        } else if (name == "multi_distinct_count") {
            func_name = "multi_distinct_count2";
        }
    }

    auto is_decimal_type = [](LogicalType lt) {
        return lt == TYPE_DECIMAL32 || lt == TYPE_DECIMAL64 || lt == TYPE_DECIMAL128;
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

const AggregateFunction* get_aggregate_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                                bool is_null, TFunctionBinaryType::type binary_type, int func_version) {
    return get_function(name, arg_type, return_type, false, is_null, binary_type, func_version);
}

const AggregateFunction* get_window_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                             bool is_null, TFunctionBinaryType::type binary_type, int func_version) {
    if (binary_type == TFunctionBinaryType::BUILTIN) {
        return get_function(name, arg_type, return_type, true, is_null, binary_type, func_version);
    } else if (binary_type == TFunctionBinaryType::SRJAR) {
        return getJavaWindowFunction();
    }
    return nullptr;
}

} // namespace starrocks
