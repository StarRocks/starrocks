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

#include "storage/lake/compaction_task.h"

#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

CompactionTask::CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                               CompactionTaskContext* context)
        : _txn_id(context->txn_id),
          _tablet(std::move(tablet)),
          _input_rowsets(std::move(input_rowsets)),
          _mem_tracker(std::make_unique<MemTracker>(MemTracker::COMPACTION, -1,
                                                    "Compaction-" + std::to_string(_tablet.metadata()->id()),
                                                    GlobalEnv::GetInstance()->compaction_mem_tracker())),
          _context(context) {}

Status CompactionTask::execute_index_major_compaction(TxnLogPB* txn_log) {
    if (_tablet.get_schema()->keys_type() == KeysType::PRIMARY_KEYS) {
        auto metadata = _tablet.metadata();
        if (metadata->enable_persistent_index() &&
            metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE) {
            return _tablet.tablet_manager()->update_mgr()->execute_index_major_compaction(_tablet.metadata()->id(),
                                                                                          *metadata, txn_log);
        }
    }
    return Status::OK();
}

} // namespace starrocks::lake
