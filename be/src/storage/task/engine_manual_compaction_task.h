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

#include "gen_cpp/AgentService_types.h"
#include "storage/olap_define.h"
#include "storage/task/engine_task.h"

namespace starrocks {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineManualCompactionTask : public EngineTask {
public:
    Status execute() override;

    EngineManualCompactionTask(MemTracker* mem_tracker, TTabletId tablet_id, bool base_compaction);

    ~EngineManualCompactionTask() override = default;

private:
    Status _manual_compaction();

    std::unique_ptr<MemTracker> _mem_tracker;

    TTabletId _tablet_id;
    bool _base_compaction;
};

} // namespace starrocks
