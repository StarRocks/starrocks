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
#include <unordered_map>

#include "base/phmap/phmap.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/rowset.h"

namespace starrocks {
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
} // namespace starrocks

namespace starrocks {

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

class OlapScanLazyMaterializationContext : public GlobalLateMaterilizationContext {
public:
    void capture_rowsets(int32_t tablet_id, int64_t version, const std::vector<RowsetSharedPtr>& rowsets);

    RowsetSharedPtr get_rowset(int32_t tablet_id, int32_t dynamic_rssid, int32_t* segment_idx) const;

    using RowsetIdToDRSSId = phmap::parallel_flat_hash_map<RowsetId, uint32_t, HashOfRowsetId>;
    RowsetIdToDRSSId get_rowset_id_to_drssid(int32_t tablet_id) const;

    int64_t get_rowsets_version(int32_t tablet_id) const {
        std::shared_lock lock(_mutex);
        return _versions.at(tablet_id);
    }

    const TOlapScanNode& scan_node() const { return *_thrift_olap_scan_node; }
    void set_scan_node(const TOlapScanNode& node);

private:
    mutable std::shared_mutex _mutex;
    using RowsetInfo = std::tuple<RowsetSharedPtr, uint32_t /*dynamic rss id*/>;
    // tablet_id -> rowset infos
    std::unordered_map<int32_t, std::vector<RowsetInfo>> _rowsets;
    std::unordered_map<int32_t, int64_t> _versions;
    std::optional<TOlapScanNode> _thrift_olap_scan_node;
    size_t _dynamic_rss_id_allocator = 0;
};

class LakeScanLazyMaterializationContext : public GlobalLateMaterilizationContext {
public:
    void capture_rowsets(int32_t tablet_id, int64_t version, const std::vector<BaseRowsetSharedPtr>& rowsets);

    lake::RowsetPtr get_rowset(int32_t tablet_id, int32_t dynamic_rssid, int32_t* segment_idx) const;
    lake::RowsetPtr get_rowset(const std::vector<lake::RowsetPtr>& rowsets, int32_t drssid, int32_t* segment_idx) const;

    int64_t get_rowsets_version(int32_t tablet_id) const {
        std::shared_lock lock(_mutex);
        return _versions.at(tablet_id);
    }

    const TLakeScanNode& scan_node() const { return *_thrift_lake_scan_node; }
    void set_scan_node(const TLakeScanNode& node);

private:
    mutable std::shared_mutex _mutex;
    std::unordered_map<int32_t, std::vector<lake::RowsetPtr>> _rowsets;
    std::unordered_map<int32_t, int64_t> _versions;
    std::optional<TLakeScanNode> _thrift_lake_scan_node;
};

// manage all global late materialization contexts for different data sources
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