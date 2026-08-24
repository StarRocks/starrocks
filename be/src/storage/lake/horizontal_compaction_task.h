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

namespace starrocks {
class Chunk;
class ChunkIterator;
} // namespace starrocks

namespace starrocks::lake {

class TabletWriter;

class HorizontalCompactionTask : public CompactionTask {
public:
    explicit HorizontalCompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                                      CompactionTaskContext* context, std::shared_ptr<const TabletSchema> tablet_schema)
            : CompactionTask(std::move(tablet), std::move(input_rowsets), context, std::move(tablet_schema)) {}

    ~HorizontalCompactionTask() override = default;

    Status execute(CancelFunc cancel_func, ThreadPool* flush_pool = nullptr) override;

private:
    StatusOr<int32_t> calculate_chunk_size();

    // Snapshot of config::lake_compaction_hold_input_segments, taken once at the top of execute().
    // The config is mutable, and the chunk-size pass and the read pass must agree on it: a flip in
    // between would leave the read pass with neither the held segments nor a filled metadata cache.
    bool _hold_input_segments = false;
};

} // namespace starrocks::lake
