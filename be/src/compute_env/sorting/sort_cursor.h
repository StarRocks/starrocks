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

#include <functional>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

class ExprContext;

using ChunkConsumer = std::function<Status(ChunkUniquePtr)>;
using ChunkProvider = std::function<bool(ChunkUniquePtr*, bool*)>;

// SimpleChunkCursor a simple cursor over the SenderQueue, avoid copy the chunk
//
// Example:
// while (!cursor.is_eos()) {
//      ChunkUniquePtr chunk = cursor.try_get_next();
//      if (!!chunk)) {
//          usage(chunk);
//      }
// }
class SimpleChunkSortCursor {
public:
    SimpleChunkSortCursor() = delete;
    SimpleChunkSortCursor(const SimpleChunkSortCursor& rhs) = delete;
    SimpleChunkSortCursor(ChunkProvider chunk_provider, const std::vector<ExprContext*>* sort_exprs);
    ~SimpleChunkSortCursor() = default;

    // Check has any data
    bool is_data_ready();

    // Try to get next chunk, return a Chunk a available,
    // return a nullptr if data temporarily not avaiable or end of stream
    std::pair<ChunkUniquePtr, Columns> try_get_next();

    // Check if is the end of stream
    bool is_eos();

    const std::vector<ExprContext*>* get_sort_exprs() const { return _sort_exprs; }

private:
    bool _data_ready = false;
    bool _eos = false;

    ChunkProvider _chunk_provider;
    const std::vector<ExprContext*>* _sort_exprs;
};

} // namespace starrocks
