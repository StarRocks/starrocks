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

#include "common/status.h"
#include "common/statusor.h"
#include "exec/spill/block.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {
namespace spill {

struct AcquireBlockOptions {
    TUniqueId query_id;
    int32_t plan_node_id;
    std::string name;
};

// virtual class, use to allocate block
// BlockManager holds lots of BlockContainer,
// when CreateBlock, should choose one unused container first and acquire an block from container
// one query has one BlockManager?
class BlockManager {
public:
    virtual ~BlockManager() = default;
    virtual Status open() = 0;
    virtual void close() = 0;
    // create a block with option, if cannot create return an error
    virtual StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) = 0;
    virtual Status release_block(const BlockPtr& block) = 0;
};
} // namespace spill
} // namespace starrocks