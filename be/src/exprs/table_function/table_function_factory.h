// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/table_function/table_function.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

class TableFunctionFactory {
public:
};

extern const TableFunction* get_table_function(const std::string& name, const std::vector<PrimitiveType>& arg_type,
                                               const std::vector<PrimitiveType>& return_type,
                                               TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN);

} // namespace starrocks::vectorized