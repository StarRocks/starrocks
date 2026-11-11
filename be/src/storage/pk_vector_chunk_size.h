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

#include <cstdint>

#include "common/config.h"
#include "common/constexpr.h"

namespace starrocks {

// Chunk size for reading segments while a primary key table applies a load or a compaction.
// config::pk_vector_chunk_size is mutable and unchecked, but the segment iterator never advances with a chunk
// size of 0 and counts the rows of a chunk in a uint16_t. So a value <= 0 falls back to DEFAULT_CHUNK_SIZE and a
// value above MAX_CHUNK_SIZE is capped.
inline int32_t get_pk_vector_chunk_size() {
    const int32_t configured = config::pk_vector_chunk_size;
    if (configured <= 0) {
        return DEFAULT_CHUNK_SIZE;
    }
    return configured < MAX_CHUNK_SIZE ? configured : MAX_CHUNK_SIZE;
}

} // namespace starrocks
