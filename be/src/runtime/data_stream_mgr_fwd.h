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

#include <memory>
#include <vector>

#include "column/vectorized_fwd.h"

namespace starrocks {

// To manage pass through chunks between sink/sources in the same process.
struct ChunkPassThroughItem {
    ChunkPassThroughItem() = default;
    ChunkPassThroughItem(ChunkUniquePtr chunk_, int32_t driver_sequence_, size_t chunk_bytes_, int64_t physical_bytes_)
            : chunk(std::move(chunk_)),
              driver_sequence(driver_sequence_),
              chunk_bytes(chunk_bytes_),
              physical_bytes(physical_bytes_) {}
    ChunkUniquePtr chunk;
    int32_t driver_sequence;
    size_t chunk_bytes;
    int64_t physical_bytes;
};

using ChunkPassThroughVector = std::vector<ChunkPassThroughItem>;
using ChunkPassThroughVectorPtr = std::unique_ptr<ChunkPassThroughVector>;

class DataStreamMgr;

} // namespace starrocks