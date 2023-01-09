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

#include "runtime/stream_epoch_manager.h"

#include <fmt/format.h>

namespace starrocks {

// Start the new epoch from input epoch info
Status StreamEpochManager::update_epoch(
        const EpochInfo& epoch_info,
        std::unordered_map<TUniqueId, NodeId2ScanRanges>& fragment_id_to_node_id_scan_ranges) {
    std::unique_lock<std::shared_mutex> l(_epoch_lock);
    if (!_fragment_id_to_node_id_scan_ranges.empty() &&
        _fragment_id_to_node_id_scan_ranges.size() != fragment_id_to_node_id_scan_ranges.size()) {
        return Status::InternalError(fmt::format("Update epoch failed: {} is not equal to origin's size {}",
                                                 fragment_id_to_node_id_scan_ranges.size(),
                                                 _fragment_id_to_node_id_scan_ranges.size()));
    }

    // reset state
    _epoch_info = epoch_info;
    _fragment_id_to_node_id_scan_ranges = fragment_id_to_node_id_scan_ranges;
    return Status::OK();
}

Status StreamEpochManager::update_binlog_offset(const TUniqueId& fragment_instance_id, int64_t scan_node_id,
                                                int64_t tablet_id, BinlogOffset binlog_offset) {
    std::unique_lock<std::shared_mutex> l(_epoch_lock);
    auto node_id_to_scan_ranges_iter = _fragment_id_to_node_id_scan_ranges.find(fragment_instance_id);
    if (node_id_to_scan_ranges_iter == _fragment_id_to_node_id_scan_ranges.end()) {
        return Status::InternalError(fmt::format("Update binlog offset failed: fragment_instance_id {} is not found.",
                                                 print_id(fragment_instance_id)));
    }
    auto& node_id_to_scan_ranges = node_id_to_scan_ranges_iter->second;
    auto iter = node_id_to_scan_ranges.find(scan_node_id);
    if (iter == node_id_to_scan_ranges.end()) {
        return Status::InternalError(
                fmt::format("Update binlog offset failed: scan_node_id {} is not found.", scan_node_id));
    }
    auto& scan_range_mapping = iter->second;
    auto scan_range_iter = scan_range_mapping.find(tablet_id);
    if (scan_range_iter == scan_range_mapping.end()) {
        return Status::InternalError(fmt::format("Update binlog offset failed: tablet_id {} is not found.", tablet_id));
    }
    // update binlog to report the final status
    scan_range_iter->second = binlog_offset;
    return Status::OK();
}

const BinlogOffset* StreamEpochManager::get_binlog_offset(const TUniqueId& fragment_instance_id, int64_t scan_node_id,
                                                          int64_t tablet_id) const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);

    auto node_id_to_scan_ranges_iter = _fragment_id_to_node_id_scan_ranges.find(fragment_instance_id);
    if (node_id_to_scan_ranges_iter == _fragment_id_to_node_id_scan_ranges.end()) {
        return nullptr;
    }
    auto& node_id_to_scan_ranges = node_id_to_scan_ranges_iter->second;
    auto iter = node_id_to_scan_ranges.find(scan_node_id);
    if (iter == node_id_to_scan_ranges.end()) {
        return nullptr;
    }
    return _get_epoch_unlock(iter->second, tablet_id);
}

const EpochInfo& StreamEpochManager::epoch_info() const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);
    return _epoch_info;
}

const std::unordered_map<TUniqueId, NodeId2ScanRanges>& StreamEpochManager::fragment_id_to_node_id_scan_ranges() const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);
    return _fragment_id_to_node_id_scan_ranges;
}

const BinlogOffset* StreamEpochManager::_get_epoch_unlock(const TabletId2BinlogOffset& tablet_id_scan_ranges_mapping,
                                                          int64_t tablet_id) const {
    auto iter = tablet_id_scan_ranges_mapping.find(tablet_id);
    if (iter != tablet_id_scan_ranges_mapping.end()) {
        return &(iter->second);
    } else {
        return nullptr;
    }
}

} // namespace starrocks
