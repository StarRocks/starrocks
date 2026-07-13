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

#include "exec/pipeline/scan/glm_manager.h"

#include "storage/rowset/rowset.h"

namespace starrocks {

RowsetSharedPtr OlapScanLazyMaterializationContext::get_rowset(int32_t tablet_id, int32_t drssid,
                                                               int32_t* segment_idx) const {
    std::shared_lock lock(_mutex);
    if (!_rowsets.contains(tablet_id)) {
        return nullptr;
    }

    const auto& tablet_rowsets = _rowsets.at(tablet_id);

    RowsetSharedPtr target;
    int32_t segment_id = 0;

    for (const auto& [rowset, drssid_base] : tablet_rowsets) {
        const int32_t num_segment = rowset->num_segments();
        if (drssid_base <= drssid && drssid < drssid_base + num_segment) {
            segment_id = drssid - drssid_base;
            target = rowset;
            break;
        }
    }

    *segment_idx = segment_id;
    return target;
}

auto OlapScanLazyMaterializationContext::get_rowset_id_to_drssid(int32_t tablet_id) const -> RowsetIdToDRSSId {
    RowsetIdToDRSSId map;
    std::shared_lock lock(_mutex);
    if (!_rowsets.contains(tablet_id)) {
        return map;
    }
    for (const auto& [rowset, drssid_base] : _rowsets.at(tablet_id)) {
        map[rowset->rowset_id()] = drssid_base;
    }
    return map;
}

void OlapScanLazyMaterializationContext::capture_rowsets(int32_t tablet_id, int64_t version,
                                                         const std::vector<RowsetSharedPtr>& rowsets) {
    std::unique_lock lock(_mutex);
    auto& tablet_rowsets = this->_rowsets[tablet_id];
    for (const auto& rowset : rowsets) {
        tablet_rowsets.emplace_back(std::make_tuple(rowset, _dynamic_rss_id_allocator));
        // + 1 for empty rowset id, which will be used for segment with no rows, and the drssid of this empty rowset is always -1.
        _dynamic_rss_id_allocator += rowset->num_segments() + 1;
    }
    _versions[tablet_id] = version;
}

void OlapScanLazyMaterializationContext::set_scan_node(const TOlapScanNode& node) {
    std::unique_lock lock(_mutex);
    if (!_thrift_olap_scan_node.has_value()) {
        _thrift_olap_scan_node = node;
    }
}

} // namespace starrocks
