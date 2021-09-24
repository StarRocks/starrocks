// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exprs/table_function/table_function.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

class TableFunctionFactory {
public:
    static TableFunctionPtr MakeUnnest();
};

extern const TableFunction* get_table_function(const std::string& name, const std::vector<PrimitiveType>& arg_type,
                                               const std::vector<PrimitiveType>& return_type);
} // namespace starrocks::vectorized