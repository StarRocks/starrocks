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

#include <memory>
#include <vector>

#include "storage/lake/compaction_task.h"

namespace starrocks::lake {

class CloudNativeIndexCompactionTask : public CompactionTask {
public:
    explicit CloudNativeIndexCompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                                            CompactionTaskContext* context,
                                            std::shared_ptr<const TabletSchema> tablet_schema)
            : CompactionTask(std::move(tablet), std::move(input_rowsets), context, std::move(tablet_schema)) {}

    ~CloudNativeIndexCompactionTask() override = default;

    Status execute(CancelFunc cancel_func, ThreadPool* flush_pool = nullptr) override;
};

} // namespace starrocks::lake