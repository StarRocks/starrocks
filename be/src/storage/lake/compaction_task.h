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
#include "storage/lake/versioned_tablet.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class RowsetMetadataPB;
class TxnLogPB;
} // namespace starrocks

namespace starrocks::lake {

class Rowset;
class RowsetSplitContext;

class CompactionTask {
public:
    using RowsetMetadataPtr = std::shared_ptr<RowsetMetadataPB>;
    using RowsetPtr = std::shared_ptr<Rowset>;
    using RowsetList = std::vector<RowsetPtr>;

    // CancelFunc is a function that used to tell the compaction task whether the task
    // should be cancelled.
    using CancelFunc = std::function<bool()>;

    explicit CompactionTask(VersionedTablet tablet, RowsetList input_rowsets, CompactionTaskContext* context);
    virtual ~CompactionTask() = default;

    Status execute(const CancelFunc& cancel_func, ThreadPool* flush_pool = nullptr);

    Status execute_index_major_compaction(TxnLogPB* txn_log);

    inline static const CancelFunc kNoCancelFn = []() { return false; };
    inline static const CancelFunc kCancelledFn = []() { return true; };

protected:
    RowsetPtr build_rowset_from_metadata(RowsetMetadataPB* metadata, int32_t index);

    virtual StatusOr<RowsetPtr> compact(const RowsetList& input_rowsets, const CancelFunc& cancel_func,
                                        ThreadPool* flush_pool) = 0;

    int64_t _txn_id;
    VersionedTablet _tablet;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    CompactionTaskContext* _context;
    TabletSchemaCSPtr _tablet_schema;

private:
    StatusOr<RowsetPtr> execute(const RowsetList& input_rowsets, const CancelFunc& cancel_func, ThreadPool* flush_pool);
    void delete_rowsets(const RowsetList& rowsets);
    StatusOr<std::vector<RowsetList>> split_rowsets(const RowsetList& input_rowsets, int64_t max_merge_way);
    StatusOr<RowsetPtr> split(RowsetSplitContext* split_context, int64_t max_merge_way);

    RowsetList _input_rowsets;
};

} // namespace starrocks::lake
