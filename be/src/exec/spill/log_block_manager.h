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
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "exec/spill/block_manager.h"
#include "exec/spill/dir_manager.h"

namespace starrocks::spill {

class LogBlockContainer;
using LogBlockContainerPtr = std::shared_ptr<LogBlockContainer>;

// LogBlockManager is an implementations of BlockManager that attempts to reduce the number of spill files,
// the basic idea is to cluster multiple Blocks into serveral large files.
// Each large file is called a LogBlockContainer.
// LogBlockContainer begins empty and is written to sequentially, block by block, just like log writing,
// this is also the origin of the LogBlockManager name.

// LogBlockManager contains multiple LogBlockContainers,
// when a new block application arrives, it will first determine the directory to place the Block through DirManager,
// and then try to select one of the existing available containers to place the Block,
// if there is no avaiable container, it will create a new one.

// When Block writing is completed, the upper layer returns the Block to LogBlockManager by calling `release_block`.
// If the corresponding container is not full, LogBlockManager will save it for reuse.
// LogBlockContainer does not guarantee thread safety, and only one thread can write at a time,
// so theoretically, the number of containers being written at the same time will be equivalent to the number of io threads
class LogBlockManager : public BlockManager {
public:
    LogBlockManager(TUniqueId query_id, DirManager* dir_mgr);
    ~LogBlockManager() override;

    Status open() override;
    void close() override;

    StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) override;
    Status release_block(const BlockPtr& block) override;

#ifdef BE_TEST
    void set_dir_manager(DirManager* dir_mgr) { _dir_mgr = dir_mgr; }
#endif

private:
    StatusOr<LogBlockContainerPtr> get_or_create_container(DirPtr dir, TUniqueId fragment_instance_id,
                                                           int32_t plan_node_id, const std::string& plan_node_name,
                                                           bool direct_io);

private:
    typedef std::unordered_map<uint64_t, LogBlockContainerPtr> ContainerMap;
    typedef std::queue<LogBlockContainerPtr> ContainerQueue;
    typedef std::shared_ptr<ContainerQueue> ContainerQueuePtr;

    TUniqueId _query_id;
    int64_t _max_container_bytes;

    std::atomic<uint64_t> _next_container_id = 0;
    std::mutex _mutex;

    typedef std::unordered_map<int32_t, ContainerQueuePtr> PlanNodeContainerMap;
    typedef std::unordered_map<TUniqueId, std::shared_ptr<PlanNodeContainerMap>> QueryContainerMap;
    typedef std::unordered_map<std::string, std::shared_ptr<QueryContainerMap>> DirContainerMap;

    std::unordered_map<Dir*, std::shared_ptr<PlanNodeContainerMap>> _available_containers;

    std::vector<LogBlockContainerPtr> _full_containers;
    DirManager* _dir_mgr = nullptr;
    const static int64_t kDefaultMaxContainerBytes = 10L * 1024 * 1024 * 1024; // 10GB
};
} // namespace starrocks::spill