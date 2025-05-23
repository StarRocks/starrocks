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

#include <atomic>
#include <memory>

#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
class Aggregator;

class AggInRuntimeFilterBuilder {
public:
    AggInRuntimeFilterBuilder(RuntimeFilterBuildDescriptor* build_desc, LogicalType type)
            : _build_desc(build_desc), _type(type) {}
    RuntimeFilter* build(Aggregator* aggretator, ObjectPool* pool);

private:
    RuntimeFilterBuildDescriptor* _build_desc;
    LogicalType _type{};
};

class AggInRuntimeFilterMerger {
public:
    AggInRuntimeFilterMerger(size_t dop) : _merged(dop), _target_filters(dop) {}
    bool merge(size_t sequence, RuntimeFilterBuildDescriptor* desc, RuntimeFilter* in_rf);
    bool always_true() const { return _always_true.load(std::memory_order_acquire); }
    RuntimeFilter* merged_runtime_filter() { return _target_filters[0]; }

private:
    std::atomic<size_t> _merged;
    std::vector<RuntimeFilter*> _target_filters;
    std::atomic<bool> _always_true = false;
};

} // namespace starrocks