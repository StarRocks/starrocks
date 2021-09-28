// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>
#include <utility>

#include "column/column.h"
#include "common/status.h"
#include "udf/udf.h"

namespace starrocks {
namespace vectorized {

using PrepareFunction = Status (*)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

using CloseFunction = Status (*)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

using ScalarFunction = ColumnPtr (*)(FunctionContext* context, const Columns& columns);

struct FunctionDescriptor {
    std::string name;

    uint8_t args_nums;

    ScalarFunction scalar_function;

    PrepareFunction prepare_function;

    CloseFunction close_function;

    FunctionDescriptor(std::string nm, uint8_t args, ScalarFunction sf, PrepareFunction pf, CloseFunction cf)
            : name(std::move(nm)), args_nums(args), scalar_function(sf), prepare_function(pf), close_function(cf) {}

    FunctionDescriptor(std::string nm, uint8_t args, ScalarFunction sf)
            : name(std::move(nm)),
              args_nums(args),
              scalar_function(sf),
              prepare_function(nullptr),
              close_function(nullptr) {}
};

class BuiltinFunctions {
    using FunctionTables = std::unordered_map<uint64_t, FunctionDescriptor>;

public:
    static const FunctionDescriptor* find_builtin_function(uint64_t id) {
        if (auto iter = _fn_tables.find(id); iter != _fn_tables.end()) {
            return &iter->second;
        }
        return nullptr;
    };

private:
    static FunctionTables _fn_tables;
};

} // namespace vectorized
} // namespace starrocks
