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

#include <vector>

#include "common/global_types.h"

namespace starrocks {
class ExprContext;
class RuntimeFilterProbeCollector;
class RuntimeState;

namespace pipeline {

class OperatorRuntimeAccess {
public:
    virtual ~OperatorRuntimeAccess() = default;

    virtual void bind_runtime_in_filters(RuntimeState* state, int32_t driver_sequence,
                                         std::vector<ExprContext*>* runtime_in_filters) = 0;
    virtual RuntimeFilterProbeCollector* get_runtime_bloom_filters() = 0;
    virtual const RuntimeFilterProbeCollector* get_runtime_bloom_filters() const = 0;
    virtual const std::vector<SlotId>& get_filter_null_value_columns() const = 0;
};

} // namespace pipeline
} // namespace starrocks
