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

#include <cstdint>
#include <functional>
#include <shared_mutex>

#include "base/phmap/phmap.h"

namespace starrocks {

// GlobalLateMaterilizationContext is used to describe the context information required
// for global late materialization. Each data source owns its concrete implementation.
class GlobalLateMaterilizationContext {
public:
    virtual ~GlobalLateMaterilizationContext() = default;
};

// Manage all global late materialization contexts for different data sources.
class GlobalLateMaterilizationContextMgr {
public:
    GlobalLateMaterilizationContext* get_ctx(int64_t scan_node_id) const;
    GlobalLateMaterilizationContext* get_or_create_ctx(
            int64_t scan_node_id, const std::function<GlobalLateMaterilizationContext*()>& ctor_func);

    using MutexType = std::shared_mutex;
    // scan_node_id -> GlobalLateMaterilizationContext*
    using ContextMap = phmap::parallel_flat_hash_map<int64_t, GlobalLateMaterilizationContext*, phmap::Hash<int64_t>,
                                                     phmap::EqualTo<int64_t>, phmap::Allocator<int64_t>, 4, MutexType>;

    ContextMap _ctx_map;
};

} // namespace starrocks
