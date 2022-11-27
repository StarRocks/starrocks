// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/status.h"
#include "exec/stream/stream_fdw.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/tablet.h"

namespace starrocks::stream {

using ChunkIteratorPtrOr = StatusOr<vectorized::ChunkIteratorPtr>;
using ChunkPtrOr = StatusOr<vectorized::ChunkPtr>;

class StateTable {
public:
    virtual ~StateTable() = default;

    virtual Status init() = 0;
    virtual Status prepare(RuntimeState* state) = 0;
    virtual Status open(RuntimeState* state) = 0;

    // The queried key must be the primary keys of state table which may contain multi columns.
    // `seek_key` must only return one row for the primary keys.
    virtual ChunkPtrOr seek_key(const DatumRow& key) const = 0;

    // The batch api of `seek_key` which all the keys must be primary keys of state table.
    // NOTE: The count of the result of `seek_keys` must be exactly same to the input keys' count.
    virtual std::vector<ChunkPtrOr> seek_keys(const std::vector<DatumRow>& keys) const = 0;

    // The queried key must be the prefix of the primary keys. Result may contain multi rows, so
    // use ChunkIterator to fetch all results.
    virtual ChunkIteratorPtrOr prefix_scan_key(const DatumRow& key) const = 0;

    // The batch api of `prefix_scan_key`.
    // NOTE: The count of the result of `seek_keys` must be exactly same to the input keys' count.
    virtual std::vector<ChunkIteratorPtrOr> prefix_scan_keys(const std::vector<DatumRow>& keys) const = 0;

    // Flush the input chunk into the state table.
    virtual Status flush(RuntimeState* state, vectorized::Chunk* chunk) = 0;

    // Flush the input chunk into the state table: StreamChunk contains the ops columns.
    virtual Status flush(RuntimeState* state, vectorized::StreamChunk* chunk) = 0;

    // Commit the flushed state data to be used in the later transaction.
    virtual Status commit(RuntimeState* state) = 0;
};

} // namespace starrocks::stream
