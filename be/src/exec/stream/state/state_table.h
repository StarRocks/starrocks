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
#include "exec/stream/stream_fdw.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/tablet.h"

namespace starrocks::stream {

using ChunkIteratorPtrOr = StatusOr<ChunkIteratorPtr>;
using ChunkPtrOr = StatusOr<ChunkPtr>;

/**
 * `StateTable` is used in Incremental MV, stateful operators will use`StateTable` to keep its
 * intermediate state, eg `StreamAgg` use `StateTable` to keep its agg state which can be used later.
 * `StateTable` offers seek/flush apis to interact with the `StateTable` behind.
 */
class StateTable {
public:
    virtual ~StateTable() = default;

    virtual Status init() = 0;
    virtual Status prepare(RuntimeState* state) = 0;
    virtual Status open(RuntimeState* state) = 0;

    // The queried key must be the primary keys of state table which may contain multi columns.
    // `seek` must only return one row for the primary keys.
    virtual ChunkPtrOr seek(const DatumRow& key) const = 0;

    // The batch api of `seek` which all the keys must be primary keys of state table.
    // NOTE: The count of the result of `seek` must be exactly same to the input keys' count.
    virtual std::vector<ChunkPtrOr> seek(const std::vector<DatumRow>& keys) const = 0;

    // The queried key must be the prefix of the primary keys. Result may contain multi rows, so
    // use ChunkIterator to fetch all results.
    virtual ChunkIteratorPtrOr prefix_scan(const DatumRow& key) const = 0;

    // The batch api of `prefix_scan`.
    // NOTE: The count of the result of `prefix_scan` must be exactly same to the input keys' count.
    virtual std::vector<ChunkIteratorPtrOr> prefix_scan(const std::vector<DatumRow>& keys) const = 0;

    // Flush the input chunk into the state table: StreamChunk contains the ops columns.
    virtual Status flush(RuntimeState* state, StreamChunk* chunk) = 0;

    // Commit the flushed state data to be used in the later transaction.
    virtual Status commit(RuntimeState* state) = 0;
};

} // namespace starrocks::stream
