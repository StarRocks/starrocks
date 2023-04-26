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

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/spill/block_manager.h"
#include "gen_cpp/types.pb.h"
#include "util/raw_container.h"

namespace starrocks::spill {
class ChunkBuilder;

enum class SerdeType {
    BY_COLUMN,
};

struct SerdeContext {
    std::string serialize_buffer;
};
class Spiller;
// Serde is used to serialize and deserialize spilled data.
class Serde;
using SerdePtr = std::shared_ptr<Serde>;
class Serde {
public:
    Serde(Spiller* parent) : _parent(parent) {}
    virtual ~Serde() = default;

    virtual Status prepare() = 0;
    // serialize chunk and append the serialized data into block
    virtual Status serialize(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) = 0;
    // deserialize data from block, return the chunk after deserialized
    virtual StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) = 0;

    static StatusOr<SerdePtr> create_serde(Spiller* parent);

protected:
    Spiller* _parent = nullptr;
};

} // namespace starrocks::spill