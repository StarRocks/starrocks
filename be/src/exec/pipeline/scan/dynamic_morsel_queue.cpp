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

#include "exec/pipeline/scan/dynamic_morsel_queue.h"

#include <iterator>

namespace starrocks::pipeline {

MorselQueuePtr create_empty_morsel_queue() {
    // instead of creating FixedMorselQueue, DynamicMorselQueue permits to add scan ranges dynamically
    // because if we have incremental scan ranges delivery, some driver maybe does not have any scan ranges at first
    // but in the next round, it will have scan ranges to process.
    return std::make_unique<DynamicMorselQueue>(std::vector<MorselPtr>{}, true);
}

StatusOr<MorselPtr> DynamicMorselQueue::try_get() {
    std::lock_guard<std::mutex> _l(_mutex);
    if (_size == 0) return nullptr;
    _size -= 1;
    MorselPtr ret = std::move(_queue.front());
    _queue.pop_front();
    if (_ticket_checker != nullptr && ret->has_owner_id() && !ret->is_ticket_checker_entered()) {
        ret->set_ticket_checker_entered(true);
        _ticket_checker->enter(ret->owner_id(), ret->is_last_split());
    }
    return std::move(ret);
}

void DynamicMorselQueue::unget(MorselPtr&& morsel) {
    std::lock_guard<std::mutex> _l(_mutex);
    _size += 1;
    _queue.emplace_front(std::move(morsel));
}

Status DynamicMorselQueue::append_morsels(std::vector<MorselPtr>&& morsels) {
    std::lock_guard<std::mutex> _l(_mutex);
    _size += morsels.size();
    // add split morsels to front of this queue.
    // so this new morsels share same owner_id with recently processed morsel.
    _queue.insert(_queue.begin(), std::make_move_iterator(morsels.begin()), std::make_move_iterator(morsels.end()));
    return Status::OK();
}

} // namespace starrocks::pipeline
