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

#include <string>
#include <utility>

#include "column/column.h"
#include "common/status.h"
#include "exprs/function_context.h"

namespace starrocks {

using PrepareFunction = Status (*)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

using CloseFunction = Status (*)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

using ScalarFunction = StatusOr<ColumnPtr> (*)(FunctionContext* context, const Columns& columns);

struct FunctionDescriptor {
    std::string name;

    uint8_t args_nums;

    ScalarFunction scalar_function;

    PrepareFunction prepare_function;

    CloseFunction close_function;

    bool exception_safe;

    FunctionDescriptor(std::string nm, uint8_t args, ScalarFunction sf, PrepareFunction pf, CloseFunction cf,
                       bool exception_safe_)
            : name(std::move(nm)),
              args_nums(args),
              scalar_function(sf),
              prepare_function(pf),
              close_function(cf),
              exception_safe(exception_safe_) {}

    FunctionDescriptor(std::string nm, uint8_t args, ScalarFunction sf, bool exception_safe_)
            : name(std::move(nm)),
              args_nums(args),
              scalar_function(sf),
              prepare_function(nullptr),
              close_function(nullptr),
              exception_safe(exception_safe_) {}
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

} // namespace starrocks
