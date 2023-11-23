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
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>

#include "common/statusor.h"
#include "storage/lake/types_fwd.h"

namespace starrocks {
class Schema;
class ThreadPool;
} // namespace starrocks

namespace starrocks::lake {

class MetaFileBuilder;
class Tablet;
class LakePrimaryIndex;

struct LoadStats {
    std::mutex mutex;
    uint64_t io_cost = 0;
    uint64_t insert_cost = 0;
    uint64_t lock_cost = 0;

    const std::string to_string() const {
        return "io cost(ms): " + std::to_string(io_cost / 1000000) +
               " insert cost(ms): " + std::to_string(insert_cost / 1000000) +
               " lock cost(ms): " + std::to_string(lock_cost / 1000000);
    }
};

class PkIndexLoader {
public:
    PkIndexLoader() = default;

    ~PkIndexLoader();

    Status init();

    std::future<Status> load(Tablet* tablet, const std::vector<RowsetPtr>& rowsets, const Schema& schema,
                             int64_t version, const MetaFileBuilder* builder, LakePrimaryIndex* index);

    void finish_subtask(int64_t tablet_id, const Status& status);

private:
    std::mutex _mutex;
    std::unordered_map<int64_t, std::unique_ptr<std::promise<Status>>> _promises;
    std::unordered_map<int64_t, uint64_t> _subtask_nums;
    std::unordered_map<int64_t, std::shared_ptr<LoadStats>> _stats;
    std::unique_ptr<ThreadPool> _load_thread_pool;
};

} // namespace starrocks::lake
