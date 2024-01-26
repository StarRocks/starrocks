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

#include "exec/spill/hybird_block_manager.h"

namespace starrocks::spill {

HyBirdBlockManager::HyBirdBlockManager(const TUniqueId& query_id, std::unique_ptr<BlockManager> local_block_manager,
                                       std::unique_ptr<BlockManager> remote_block_manager)
        : _local_block_manager(std::move(local_block_manager)),
          _remote_block_manager(std::move(remote_block_manager)) {}

HyBirdBlockManager::~HyBirdBlockManager() {
    _local_block_manager.reset();
    _remote_block_manager.reset();
}

Status HyBirdBlockManager::open() {
    RETURN_IF_ERROR(_local_block_manager->open());
    RETURN_IF_ERROR(_remote_block_manager->open());
    return Status::OK();
}

void HyBirdBlockManager::close() {
    _local_block_manager->close();
    _remote_block_manager->close();
}

StatusOr<BlockPtr> HyBirdBlockManager::acquire_block(const AcquireBlockOptions& opts) {
    if (rand() % 10 < 2) {
        auto local_block = _local_block_manager->acquire_block(opts);
        if (local_block.ok()) {
            return local_block;
        }
    }
    ASSIGN_OR_RETURN(auto remote_block, _remote_block_manager->acquire_block(opts));
    remote_block->set_is_remote(true);
    return remote_block;
}

Status HyBirdBlockManager::release_block(const BlockPtr& block) {
    if (block->is_remote()) {
        return _remote_block_manager->release_block(block);
    }
    return _local_block_manager->release_block(block);
}
} // namespace starrocks::spill