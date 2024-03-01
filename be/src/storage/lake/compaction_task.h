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
#include "compaction_task_context.h"
#include "runtime/mem_tracker.h"

namespace starrocks::lake {

class Rowset;
class Tablet;

class CompactionTask {
public:
    // CancelFunc is a function that used to tell the compaction task whether the task
    // should be cancelled.
    using CancelFunc = std::function<bool()>;

<<<<<<< HEAD
    explicit CompactionTask(int64_t txn_id, int64_t version, std::shared_ptr<Tablet> tablet,
                            std::vector<std::shared_ptr<Rowset>> input_rowsets);
    virtual ~CompactionTask() = default;

    virtual Status execute(Progress* stats, CancelFunc cancel_func) = 0;
=======
    explicit CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                            CompactionTaskContext* context);
    virtual ~CompactionTask() = default;

    virtual Status execute(CancelFunc cancel_func, ThreadPool* flush_pool = nullptr) = 0;
>>>>>>> 60da7ccc83 ([Enhancement] Print compaction task statistics logs for cloud native table (#37616))

    inline static const CancelFunc kNoCancelFn = []() { return false; };
    inline static const CancelFunc kCancelledFn = []() { return true; };

protected:
    int64_t _txn_id;
    int64_t _version;
    std::shared_ptr<Tablet> _tablet = nullptr;
    std::vector<std::shared_ptr<Rowset>> _input_rowsets;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    CompactionTaskContext* _context;
};

} // namespace starrocks::lake
