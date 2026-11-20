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

#include <memory>
#include <string>

#include "column/column.h"
#include "exprs/function_context.h"
#include "types/type_descriptor.h"

namespace starrocks {
class MemTracker;

// Arrow and Paimon objects remain in the optional SDK shim. One calculator is owned by
// each function execution thread and released before its query memory tracker.
class PaimonBucketCalculator {
public:
    virtual ~PaimonBucketCalculator() = default;
    virtual StatusOr<ColumnPtr> calculate(const ColumnPtr& keys) = 0;
};

#define STARROCKS_PAIMON_BUCKET_CALCULATOR_SYMBOL "starrocks_paimon_bucket_calculator_v1"
using CreatePaimonBucketCalculator = StatusOr<std::unique_ptr<PaimonBucketCalculator>> (*)(const std::string&,
                                                                                           const TypeDescriptor&,
                                                                                           MemTracker*);

extern "C" StatusOr<std::unique_ptr<PaimonBucketCalculator>> starrocks_paimon_bucket_calculator_v1(
        const std::string&, const TypeDescriptor&, MemTracker*);

class PaimonFunctions {
public:
    static Status prepare(FunctionContext*, FunctionContext::FunctionStateScope);
    static Status close(FunctionContext*, FunctionContext::FunctionStateScope);
    static StatusOr<ColumnPtr> bucket(FunctionContext*, const Columns&);
};
} // namespace starrocks
