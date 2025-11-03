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

#include "cache/cache_options.h"
#include "common/object_pool.h"
#include "gen_cpp/PlanNodes_types.h"
#include "util/phmap/phmap.h"

namespace starrocks::pipeline {

// GlobalLateMaterilizationContext is used to describe the context information required
// for global late materialization.
// Each data source needs to have its own implementation.
class GlobalLateMaterilizationContext {
public:
    virtual ~GlobalLateMaterilizationContext() = default;
};

class IcebergGlobalLateMaterilizationContext : public GlobalLateMaterilizationContext {
public:
    int32_t assign_scan_range_id(const THdfsScanRange& scan_range) {
        std::unique_lock lock(_mutex);
        hdfs_scan_ranges.push_back(scan_range);
        return hdfs_scan_ranges.size() - 1;
    }

    const THdfsScanRange& get_hdfs_scan_range(int32_t scan_range_id) const {
        std::shared_lock lock(_mutex);
        DCHECK(scan_range_id < hdfs_scan_ranges.size())
                << "scan_range_id: " << scan_range_id << ", size: " << hdfs_scan_ranges.size();
        return hdfs_scan_ranges[scan_range_id];
    }

    mutable std::shared_mutex _mutex;
    std::vector<THdfsScanRange> hdfs_scan_ranges;
    THdfsScanNode hdfs_scan_node;
    TPlanNode plan_node;
};

// manage all global late materialization contexts for different data sources
class GlobalLateMaterilizationContextMgr {
public:
    void add_ctx(int32_t row_source_slot_id, GlobalLateMaterilizationContext* ctx);
    GlobalLateMaterilizationContext* get_ctx(int32_t row_source_slot_id) const;
    GlobalLateMaterilizationContext* get_or_create_ctx(
            int32_t row_source_slot_id, const std::function<GlobalLateMaterilizationContext*()>& ctor_func);

    using MutexType = std::shared_mutex;
    using ContextMap = phmap::parallel_flat_hash_map<int32_t, GlobalLateMaterilizationContext*, phmap::Hash<int32_t>,
                                                     phmap::EqualTo<int32_t>, phmap::Allocator<int32_t>, 4, MutexType>;

    ContextMap _ctx_map;
};
} // namespace starrocks::pipeline