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
    MVEpochManager() {}
    ~MVEpochManager() = default;

    // Start the new epoch from input epoch info
    Status start_epoch(const std::unordered_map<int64_t, EpochInfo>& input_epoch_infos) {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        // reset state
        _source_operator_epoch_infos_mapping = input_epoch_infos;
        return Status::OK();
    }

    const EpochInfo* get_epoch(int64_t tablet_id) const {
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        return get_epoch_unlock(tablet_id);
    }

    bool is_finished() const { return _is_finished.load(std::memory_order_acquire); }
    void set_is_finished(bool v) { _is_finished.store(v, std::memory_order_release); }

private:
    const EpochInfo* get_epoch_unlock(int64_t tablet_id) const {
        auto iter = _source_operator_epoch_infos_mapping.find(tablet_id);
        if (iter != _source_operator_epoch_infos_mapping.end()) {
            return &(iter->second);
        } else {
            return nullptr;
        }
    }

    // TODO: Maybe use a bucket for each source operator later.
    mutable std::shared_mutex _epoch_lock;
    std::atomic_bool _is_finished{false};
    std::unordered_map<int64_t, EpochInfo> _source_operator_epoch_infos_mapping;
};

} // namespace starrocks
