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

#include <cstddef>
#include <deque>

#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

class Chunk;

bool check_json_schema_compatibility(const Chunk* one, const Chunk* two);

// Accumulate small chunk into desired size
class ChunkAccumulator {
public:
    // Avoid accumulate too many chunks in case that chunks' selectivity is very low
    static inline size_t kAccumulateLimit = 64;

    ChunkAccumulator() = default;
    ChunkAccumulator(size_t desired_size);
    void set_desired_size(size_t desired_size);
    void reset();
    void finalize();
    bool empty() const;
    bool reach_limit() const;
    Status push(ChunkPtr&& chunk);
    ChunkPtr pull();

private:
    size_t _desired_size;
    ChunkPtr _tmp_chunk;
    std::deque<ChunkPtr> _output;
    size_t _accumulate_count = 0;
};

} // namespace starrocks
