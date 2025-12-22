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

#include "common/status.h"
#include "util/threadpool.h"

namespace starrocks {

class ChunkIterator;
class MemTracker;
class Schema;

namespace lake {

class TabletWriter;

struct QuitFlag {
    std::atomic<bool> quit{false};
};

class TabletInternalParallelMergeTask : public Runnable {
public:
    TabletInternalParallelMergeTask(TabletWriter* writer, ChunkIterator* block_iterator, MemTracker* merge_mem_tracker,
                                    Schema* schema, int32_t task_index, QuitFlag* quit_flag);

    ~TabletInternalParallelMergeTask();

    void run() override;

    void cancel() override;

    void update_status(const Status& st);

    const Status& status() const { return _status; }

private:
    TabletWriter* _writer = nullptr;
    ChunkIterator* _block_iterator = nullptr;
    MemTracker* _merge_mem_tracker = nullptr;
    Schema* _schema = nullptr;
    int32_t _task_index = 0;
    Status _status;
    QuitFlag* _quit_flag = nullptr;
};

} // namespace lake
} // namespace starrocks