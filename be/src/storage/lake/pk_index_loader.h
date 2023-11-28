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

#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "storage/lake/types_fwd.h"

namespace starrocks::lake {

class MetaFileBuilder;

struct ChunkInfo {
    ChunkUniquePtr chunk;
    uint64_t rssid;
    std::vector<uint32_t> rowids;
};

struct PkSegmentScanTaskContext {
    std::queue<std::shared_ptr<ChunkInfo>> chunk_infos;
    uint64_t subtask_num;
    std::condition_variable cv;
    Status status;

    std::shared_ptr<ChunkInfo> get_chunk_info() {
        auto chunk_info = chunk_infos.front();
        chunk_infos.pop();
        return chunk_info;
    }
};

class PkIndexLoader {
public:
    PkIndexLoader() = default;

    ~PkIndexLoader();

    Status load(Tablet* tablet, const std::vector<RowsetPtr>& rowsets, const Schema& schema, int64_t version,
                const MetaFileBuilder* builder);

    void add_chunk(int64_t tablet_id, const std::shared_ptr<ChunkInfo>& chunk_info);

    StatusOr<std::shared_ptr<ChunkInfo>> get_chunk(int64_t tablet_id);

    void finish_subtask(int64_t tablet_id, const Status& status);

private:
    std::mutex _mutex;
    std::unordered_map<int64_t, std::shared_ptr<PkSegmentScanTaskContext>> _contexts;
};

} // namespace starrocks::lake
