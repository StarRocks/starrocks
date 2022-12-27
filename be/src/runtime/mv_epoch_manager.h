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

#include "column/stream_chunk.h"

namespace starrocks {

/**
 * `MVEpochManager` is used to manage the binlog source operators' start or final epoch, and
 *  can be used to interact with RuntimeState which can be controlled by FE.
 */
class MVEpochManager {
public:
    using TabletId2BinlogOffset = std::unordered_map<int64_t, BinlogOffset>;

    MVEpochManager() {}
    ~MVEpochManager() = default;

    // Start the new epoch from input epoch info
    Status update_epoch(const TUniqueId& fragment_instance_id, const EpochInfo& epoch_info,
                        const std::unordered_map<int64_t, TabletId2BinlogOffset>& input_epoch_infos) {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        // check
        if (!_node_id_to_scan_ranges.empty() && _node_id_to_scan_ranges.size() != input_epoch_infos.size()) {
            return Status::InternalError("MV Epoch ScanNode's ranges should not change.");
        }

        // reset state
        _fragment_instance_id = fragment_instance_id;
        _epoch_info = epoch_info;
        _node_id_to_scan_ranges = input_epoch_infos;
        return Status::OK();
    }

    Status update_binlog_offset(int64_t scan_node_id, int64_t tablet_id, BinlogOffset binlog_offset) {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        auto iter = _node_id_to_scan_ranges.find(scan_node_id);
        if (iter == _node_id_to_scan_ranges.end()) {
            return Status::InternalError("MV Epoch scan_node_id cannot be found.");
        }
        auto& scan_range_mapping = iter->second;
        auto scan_range_iter = scan_range_mapping.find(tablet_id);
        if (scan_range_iter == scan_range_mapping.end()) {
            return Status::InternalError("MV Epoch tablet_id cannot be found.");
        }
        // update binlog to report the final status
        scan_range_iter->second = binlog_offset;
        return Status::OK();
    }

    const BinlogOffset* get_binlog_offset(int64_t scan_node_id, int64_t tablet_id) const {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        auto iter = _node_id_to_scan_ranges.find(scan_node_id);
        if (iter == _node_id_to_scan_ranges.end()) {
            return nullptr;
        }
        return get_epoch_unlock(iter->second, tablet_id);
    }

    const TUniqueId& fragment_instance_id() const {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        return _fragment_instance_id;
    }
    const EpochInfo& epoch_info() const {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        return _epoch_info;
    }
    const std::unordered_map<int64_t, TabletId2BinlogOffset>& node_id_to_scan_ranges() const {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        return _node_id_to_scan_ranges;
    }

    bool is_finished() const { return _is_finished.load(std::memory_order_acquire); }
    void set_is_finished(bool v) { _is_finished.store(v, std::memory_order_release); }

private:
    const BinlogOffset* get_epoch_unlock(const TabletId2BinlogOffset& tablet_id_scan_ranges_mapping,
                                         int64_t tablet_id) const {
        auto iter = tablet_id_scan_ranges_mapping.find(tablet_id);
        if (iter != tablet_id_scan_ranges_mapping.end()) {
            return &(iter->second);
        } else {
            return nullptr;
        }
    }

    // TODO: Maybe use a bucket for each source operator later.
    mutable std::shared_mutex _epoch_lock;
    std::atomic_bool _is_finished{false};
    TUniqueId _fragment_instance_id;
    EpochInfo _epoch_info;
    std::unordered_map<int64_t, TabletId2BinlogOffset> _node_id_to_scan_ranges;
};

} // namespace starrocks
