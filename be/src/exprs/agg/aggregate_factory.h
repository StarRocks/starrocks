// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/agg/aggregate.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

const AggregateFunction* get_aggregate_function(const std::string& name, PrimitiveType arg_type,
                                                PrimitiveType return_type, bool is_null,
                                                TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                                                int func_version = 1);

const AggregateFunction* get_window_function(const std::string& name, PrimitiveType arg_type, PrimitiveType return_type,
                                             bool is_null,
                                             TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                                             int func_version = 1);

} // namespace starrocks::vectorized
