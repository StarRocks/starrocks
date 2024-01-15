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

#include <atomic>
#include <functional>
#include <ostream>

#include "common/status.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/versioned_tablet.h"

namespace starrocks::lake {

class Rowset;

class CompactionTask {
public:
    class Progress {
    public:
        int value() const { return _value.load(std::memory_order_acquire); }

        void update(int value) { _value.store(value, std::memory_order_release); }

    private:
        std::atomic<int> _value{0};
    };

    // CancelFunc is a function that used to tell the compaction task whether the task
    // should be cancelled.
    using CancelFunc = std::function<bool()>;

    explicit CompactionTask(int64_t txn_id, VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets);
    virtual ~CompactionTask() = default;

    virtual Status execute(Progress* stats, CancelFunc cancel_func, ThreadPool* flush_pool = nullptr) = 0;

    inline static const CancelFunc kNoCancelFn = []() { return false; };
    inline static const CancelFunc kCancelledFn = []() { return true; };

protected:
    int64_t _txn_id;
    VersionedTablet _tablet;
    std::vector<std::shared_ptr<Rowset>> _input_rowsets;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
};

} // namespace starrocks::lake
