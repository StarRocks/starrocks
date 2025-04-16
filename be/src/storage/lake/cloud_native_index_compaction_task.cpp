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

#include "storage/lake/cloud_native_index_compaction_task.h"

#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {

Status CloudNativeIndexCompactionTask::execute(CancelFunc cancel_func, ThreadPool* flush_pool) {
    auto txn_log = std::make_shared<TxnLog>();
    auto op_compaction = txn_log->mutable_op_compaction();
    txn_log->set_tablet_id(_tablet.id());
    txn_log->set_txn_id(_txn_id);
    op_compaction->set_compact_version(_tablet.metadata()->version());
    RETURN_IF_ERROR(cancel_func());
    RETURN_IF_ERROR(execute_index_major_compaction(txn_log.get()));
    _context->progress.update(100);
    RETURN_IF_ERROR(_tablet.tablet_manager()->put_txn_log(txn_log));
    VLOG(2) << "CloudNative Index compaction finished. tablet: " << _tablet.id() << ", txn_id: " << _txn_id
            << ", statistics: " << _context->stats->to_json_stats();
    return Status::OK();
}

} // namespace starrocks::lake