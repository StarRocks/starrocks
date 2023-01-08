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

#include "exprs/agg/aggregate.h"
#include "types/logical_type.h"

namespace starrocks {

const AggregateFunction* get_aggregate_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                                bool is_null,
                                                TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                                                int func_version = 1);

const AggregateFunction* get_window_function(const std::string& name, LogicalType arg_type, LogicalType return_type,
                                             bool is_null,
                                             TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN,
                                             int func_version = 1);

} // namespace starrocks
