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

#include "exec/spill/block.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/dir_manager.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

namespace starrocks {
namespace spill {

class LogBlockContainer;
using LogBlockContainerPtr = std::shared_ptr<LogBlockContainer>;

class LogBlockManager: public BlockManager {
public:
    LogBlockManager(TUniqueId query_id):
        _query_id(query_id) {}
    ~LogBlockManager();

    virtual Status open() override;
    virtual void close() override;

    virtual StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) override;
    virtual Status release_block(const BlockPtr& block) override;

private:
    StatusOr<LogBlockContainerPtr> get_or_create_container(Dir* dir, int32_t plan_node_id, const std::string& plan_node_name);

private:
    typedef std::unordered_map<uint64_t, LogBlockContainerPtr> ContainerMap;
    typedef std::queue<LogBlockContainerPtr> ContainerQueue;
    typedef std::shared_ptr<ContainerQueue> ContainerQueuePtr;

    TUniqueId _query_id;

    std::atomic<uint64_t> _next_container_id = 0;
    std::mutex _mutex;

    typedef std::unordered_map<int32_t, ContainerQueuePtr> PlanNodeContainerMap;
    typedef std::unordered_map<TUniqueId, std::shared_ptr<PlanNodeContainerMap>> QueryContainerMap;
    typedef std::unordered_map<std::string, std::shared_ptr<QueryContainerMap>> DirContainerMap;

    std::unordered_map<Dir*, std::shared_ptr<PlanNodeContainerMap>> _available_containers;

    std::vector<LogBlockContainerPtr> _full_containers;
};
}
}