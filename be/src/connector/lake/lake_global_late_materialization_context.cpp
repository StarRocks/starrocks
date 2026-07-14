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

    // Resolve an rssid (rowset id + a segment's segment_idx) back to its rowset and the segment's
    // PHYSICAL position in segment_metas. segment_idx is NOT necessarily dense over [0, num_segments):
    // a tablet split keeps only the segments a child owns while preserving each kept segment's
    // ORIGINAL segment_idx (so the inherited cloud-native PK-index rssids stay stable), which leaves
    // gaps -- e.g. a child that drops the low-key segment keeps segments with segment_idx {1,2} while
    // segment_metas_size()==2. Matching by segment_idx and returning the physical position is therefore
    // required: the old dense `rssid - rowset.id()` offset would fail to resolve the top segment
    // (rssid == id+2 falls outside [id, id+2)), surfacing as "not found lake rssid", and would resolve a
    // surviving lower rssid to the wrong physical segment. See issue #75993.
    const auto urssid = static_cast<uint32_t>(rssid);
    for (const auto& rowset : rowsets) {
        const auto& rowset_meta = rowset->metadata();
        const uint32_t rssid_base = rowset_meta.id();
        // Fast reject: the rssid must fall in this rowset's owned rssid span
        // [id, id + get_rowset_id_step) = [id, id + max_segment_idx + 1).
        if (urssid < rssid_base || urssid >= rssid_base + lake::get_rowset_id_step(rowset_meta)) {
            continue;
        }
        for (int i = 0; i < rowset_meta.segment_metas_size(); ++i) {
            if (rssid_base + lake::get_segment_idx(rowset_meta, i) == urssid) {
                segment_id = i;
                target = rowset;
                break;
            }
        }
        if (target != nullptr) {
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
