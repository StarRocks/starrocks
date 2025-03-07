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

#include "exec/spill/block_manager.h"
#include "exec/spill/dir_manager.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::spill {
class FileBlockContainer;
using FileBlockContainerPtr = std::shared_ptr<FileBlockContainer>;

// FileBlockManager is an implementation of BlockManager.
// Unlike LogBlockManager, it does not aggregate multiple blocks into one file.
// Instead, each block is stored in a separate file.

// For some distributed storage systems, they may not support append semantics, such as s3.
// An object is not visible to others before close, and cannot be appended after close.
// Therefore, blocks placed on such storage systems cannot be managed by LogBlockManager.
// FileBlockManager is designed to solve this problem.

class FileBlockManager : public BlockManager {
public:
    FileBlockManager(const TUniqueId& query_id, DirManager* dir_manager);
    ~FileBlockManager() override = default;

    Status open() override;
    void close() override;
    StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) override;
    Status release_block(BlockPtr block) override;

private:
    StatusOr<FileBlockContainerPtr> get_or_create_container(const DirPtr& dir, const TUniqueId& fragment_instance_id,
                                                            int32_t plan_node_id, const std::string& plan_node_name,
                                                            size_t block_size);

    TUniqueId _query_id;
    std::atomic<uint64_t> _next_container_id = 0;
    std::string _last_created_container_dir;

    DirManager* _dir_mgr = nullptr;
};

} // namespace starrocks::spill