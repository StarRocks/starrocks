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

#include "exec/pipeline/scan/bucket_sequence_morsel_queue.h"

#include <fmt/compile.h>

#include "gutil/casts.h"

namespace starrocks::pipeline {

BucketSequenceMorselQueue::BucketSequenceMorselQueue(MorselQueuePtr&& morsel_queue)
        : _morsel_queue(std::move(morsel_queue)) {}

std::vector<TInternalScanRange*> BucketSequenceMorselQueue::prepare_olap_scan_ranges() const {
    return _olap_morsel_queue()->prepare_olap_scan_ranges();
}

bool BucketSequenceMorselQueue::empty() const {
    return _unget_morsel == nullptr && _morsel_queue->empty();
}

StatusOr<MorselPtr> BucketSequenceMorselQueue::try_get() {
    std::lock_guard guard(_mutex);
    if (_unget_morsel != nullptr) {
        return std::move(_unget_morsel);
    }
    if (_morsel_queue->empty()) {
        return nullptr;
    }
    ASSIGN_OR_RETURN(auto morsel, _morsel_queue->try_get());
    auto* m = down_cast<ScanMorsel*>(morsel.get());
    if (m == nullptr) {
        return nullptr;
    }
    DCHECK(m->has_owner_id());
    auto owner_id = m->owner_id();
    ASSIGN_OR_RETURN(int64_t next_owner_id, _peek_sequence_id());
    _ticket_checker->enter(owner_id, next_owner_id != owner_id);
    _current_sequence = owner_id;
    return morsel;
}

std::string BucketSequenceMorselQueue::name() const {
    return fmt::format("partition_morsel_queue({})", _morsel_queue->name());
}

StatusOr<bool> BucketSequenceMorselQueue::ready_for_next() const {
    std::lock_guard guard(_mutex);
    if (_current_sequence < 0) {
        return true;
    }

    ASSIGN_OR_RETURN(int64_t next_sequence_id, _peek_sequence_id());
    if (next_sequence_id == _current_sequence) {
        return true;
    }

    if (_ticket_checker->are_all_left(_current_sequence)) {
        return true;
    }

    return false;
}

StatusOr<int64_t> BucketSequenceMorselQueue::_peek_sequence_id() const {
    int64_t next_owner_id = -1;
    if (!_morsel_queue->empty()) {
        ASSIGN_OR_RETURN(auto next_morsel, _morsel_queue->try_get());
        if (auto* next_scan_morsel = down_cast<ScanMorsel*>(next_morsel.get())) {
            next_owner_id = next_scan_morsel->owner_id();
            _morsel_queue->unget(std::move(next_morsel));
        }
    }
    return next_owner_id;
}

} // namespace starrocks::pipeline
