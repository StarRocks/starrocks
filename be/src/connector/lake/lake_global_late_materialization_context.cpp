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

#include "connector/lake/lake_global_late_materialization_context.h"

#include <mutex>

#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"

namespace starrocks {

static_assert(sizeof(lake::Rowset) > 0);

lake::RowsetPtr LakeScanLazyMaterializationContext::get_rowset(int32_t tablet_id, int32_t rssid,
                                                               int32_t* segment_idx) const {
    std::shared_lock lock(_mutex);
    if (!_rowsets.contains(tablet_id)) {
        return nullptr;
    }

    const auto& tablet_rowsets = _rowsets.at(tablet_id);
    return get_rowset(tablet_rowsets, rssid, segment_idx);
}

lake::RowsetPtr LakeScanLazyMaterializationContext::get_rowset(const std::vector<lake::RowsetPtr>& rowsets,
                                                               int32_t rssid, int32_t* segment_idx) const {
    lake::RowsetPtr target;
    int32_t segment_id = 0;

    for (const auto& rowset : rowsets) {
        const auto& rowset_meta = rowset->metadata();
        const uint32_t rssid_base = rowset_meta.id();
        size_t num_segment = rowset_meta.segment_metas_size();
        if (rssid_base <= rssid && rssid < rssid_base + num_segment) {
            segment_id = rssid - rssid_base;
            target = rowset;
            break;
        }
    }

    *segment_idx = segment_id;
    return target;
}

void LakeScanLazyMaterializationContext::capture_rowsets(int32_t tablet_id, int64_t version,
                                                         const std::vector<BaseRowsetSharedPtr>& rowsets) {
    std::unique_lock lock(_mutex);
    std::vector<lake::RowsetPtr> lake_rowsets;
    lake_rowsets.reserve(rowsets.size());
    for (const auto& rowset : rowsets) {
        auto lake_rowset = std::dynamic_pointer_cast<lake::Rowset>(rowset);
        if (lake_rowset != nullptr) {
            lake_rowsets.emplace_back(std::move(lake_rowset));
        }
    }
    _rowsets[tablet_id] = std::move(lake_rowsets);
    _versions[tablet_id] = version;
}

void LakeScanLazyMaterializationContext::set_scan_node(const TLakeScanNode& node) {
    std::unique_lock lock(_mutex);
    if (!_thrift_lake_scan_node.has_value()) {
        _thrift_lake_scan_node = node;
    }
}

} // namespace starrocks
