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
#include "runtime/runtime_state.h"
#include "util/slice.h"

namespace starrocks::spill {
class Spiller;
class BlockGroup;
class BlockManager;

class SpillOutputDataStream {
public:
    virtual ~SpillOutputDataStream() = default;
    virtual Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size) = 0;
    virtual Status flush() = 0;
    virtual bool is_remote() const = 0;
};
using SpillOutputDataStreamPtr = std::shared_ptr<SpillOutputDataStream>;
SpillOutputDataStreamPtr create_spill_output_stream(Spiller* spiller, BlockGroup* block_group,
                                                    BlockManager* block_manager);

} // namespace starrocks::spill