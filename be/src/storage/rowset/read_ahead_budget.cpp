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

#include "storage/rowset/read_ahead_budget.h"

#include <algorithm>

#include "common/config_lake_fwd.h"
#include "storage/storage_metrics.h"

namespace starrocks {

ReadAheadBudget::Grant& ReadAheadBudget::Grant::operator=(Grant&& other) noexcept {
    if (this != &other) {
        release();
        _budget = other._budget;
        _blocks = other._blocks;
        other._blocks = 0;
    }
    return *this;
}

void ReadAheadBudget::Grant::release() {
    if (_blocks > 0) {
        _budget->give_back(_blocks);
        _blocks = 0;
    }
}

ReadAheadBudget::Member& ReadAheadBudget::Member::operator=(Member&& other) noexcept {
    if (this != &other) {
        leave();
        _budget = other._budget;
        other._budget = nullptr;
    }
    return *this;
}

void ReadAheadBudget::Member::leave() {
    if (_budget != nullptr) {
        _budget->_members.fetch_sub(1, std::memory_order_relaxed);
        if (_budget->reports_metrics()) {
            StorageMetrics::instance()->lake_scan_read_ahead_windows.increment(-1);
        }
        _budget = nullptr;
    }
}

ReadAheadBudget* ReadAheadBudget::instance() {
    static ReadAheadBudget budget;
    return &budget;
}

int64_t ReadAheadBudget::limit() const {
    return _fixed_limit > 0 ? _fixed_limit : config::lake_scan_read_ahead_max_blocks;
}

ReadAheadBudget::Member ReadAheadBudget::join() {
    _members.fetch_add(1, std::memory_order_relaxed);
    if (reports_metrics()) {
        StorageMetrics::instance()->lake_scan_read_ahead_windows.increment(1);
    }
    return Member(this);
}

size_t ReadAheadBudget::share(size_t own_limit) const {
    const int64_t limit = this->limit();
    if (limit <= 0) {
        return own_limit;
    }
    const int64_t members = std::max<int64_t>(1, _members.load(std::memory_order_relaxed));
    return std::min(own_limit, static_cast<size_t>(std::max<int64_t>(1, limit / members)));
}

ReadAheadBudget::Grant ReadAheadBudget::acquire(size_t want, size_t held, size_t own_limit) {
    if (want == 0) {
        return {};
    }
    const int64_t limit = this->limit();
    const int64_t room = static_cast<int64_t>(share(own_limit)) - static_cast<int64_t>(held);
    const int64_t ask = std::min(static_cast<int64_t>(want), room);
    int64_t got = 0;
    if (ask > 0) {
        if (limit <= 0) {
            _held.fetch_add(ask, std::memory_order_relaxed);
            got = ask;
        } else {
            int64_t cur = _held.load(std::memory_order_relaxed);
            do {
                got = std::min(ask, limit - cur);
                if (got <= 0) {
                    got = 0;
                    break;
                }
            } while (!_held.compare_exchange_weak(cur, cur + got, std::memory_order_relaxed));
        }
    }
    if (got < static_cast<int64_t>(want)) {
        count_capped();
    }
    if (got == 0) {
        return {};
    }
    if (reports_metrics()) {
        StorageMetrics::instance()->lake_scan_read_ahead_blocks.increment(got);
    }
    return Grant(this, static_cast<size_t>(got));
}

bool ReadAheadBudget::has_room() {
    const int64_t limit = this->limit();
    if (limit <= 0 || held_blocks() < limit) {
        return true;
    }
    count_capped();
    return false;
}

void ReadAheadBudget::give_back(size_t blocks) {
    _held.fetch_sub(static_cast<int64_t>(blocks), std::memory_order_relaxed);
    if (reports_metrics()) {
        StorageMetrics::instance()->lake_scan_read_ahead_blocks.increment(-static_cast<int64_t>(blocks));
    }
}

void ReadAheadBudget::count_capped() {
    if (reports_metrics()) {
        StorageMetrics::instance()->lake_scan_read_ahead_capped_total.increment(1);
    }
}

} // namespace starrocks
