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
 * `StreamEpochManager` is used to manage the binlog source operators' start or final epoch, and
 *  can be used to interact with RuntimeState which can be controlled by FE.
 * 
 * `StreamEpochManager` manages all fragment instances in one BE, operators in all fragment
 * instances may interact with it, so methods should be thread-safe in MVEpochMangaer.
 */
using TabletId2BinlogOffset = std::unordered_map<int64_t, BinlogOffset>;
using NodeId2ScanRanges = std::unordered_map<int64_t, TabletId2BinlogOffset>;
class StreamEpochManager {
public:
    StreamEpochManager() = default;
    ~StreamEpochManager() = default;

    // Start the new epoch from input epoch info
    Status update_epoch(const EpochInfo& epoch_info,
                        std::unordered_map<TUniqueId, NodeId2ScanRanges>& fragment_id_to_node_id_scan_ranges);
    Status update_binlog_offset(const TUniqueId& fragment_instance_id, int64_t scan_node_id, int64_t tablet_id,
                                BinlogOffset binlog_offset);

    const BinlogOffset* get_binlog_offset(const TUniqueId& fragment_instance_id, int64_t scan_node_id,
                                          int64_t tablet_id) const;
    const EpochInfo& epoch_info() const;
    const std::unordered_map<TUniqueId, NodeId2ScanRanges>& fragment_id_to_node_id_scan_ranges() const;

    bool is_finished() const { return _is_finished.load(std::memory_order_acquire); }
    void set_is_finished(bool v) { _is_finished.store(v, std::memory_order_release); }

private:
    const BinlogOffset* _get_epoch_unlock(const TabletId2BinlogOffset& tablet_id_scan_ranges_mapping,
                                          int64_t tablet_id) const;

private:
    mutable std::shared_mutex _epoch_lock;
    std::atomic_bool _is_finished{false};
    EpochInfo _epoch_info;
    std::unordered_map<TUniqueId, NodeId2ScanRanges> _fragment_id_to_node_id_scan_ranges;
};

} // namespace starrocks
