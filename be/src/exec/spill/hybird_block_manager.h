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

namespace starrocks::spill {

// HybirdManager contains two independent BlockManagers, which manage local blocks and remote blocks respectively.
// If the local disk capacity is large enough, we will allocate blocks from the local disk first,
// otherwise allocate them from the remote storage.
class HyBirdBlockManager : public BlockManager {
public:
    HyBirdBlockManager(const TUniqueId& query_id, std::unique_ptr<BlockManager> local_block_manager,
                       std::unique_ptr<BlockManager> remote_block_manager);
    ~HyBirdBlockManager() override;

    Status open() override;
    void close() override;
    StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) override;
    Status release_block(BlockPtr block) override;

private:
    std::unique_ptr<BlockManager> _local_block_manager;
    std::unique_ptr<BlockManager> _remote_block_manager;
};

} // namespace starrocks::spill