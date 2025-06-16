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

#include "storage/task/engine_compaction_control_task.h"

#include <memory>

#include "runtime/exec_env.h"
#include "storage/compaction_manager.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"

namespace starrocks {

EngineCompactionControlTask::EngineCompactionControlTask(const std::map<TTableId, int64_t>& table_to_disable_deadline)
        : _table_to_disable_deadline(std::move(table_to_disable_deadline)) {}

Status EngineCompactionControlTask::execute() {
    CompactionManager* compaction_manager = StorageEngine::instance()->compaction_manager();
    for (auto& entry : _table_to_disable_deadline) {
        compaction_manager->disable_table_compaction(entry.first, entry.second);
    }
    return Status::OK();
}

} // namespace starrocks
