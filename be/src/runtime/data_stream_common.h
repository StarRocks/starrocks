// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <map>

#include "column/vectorized_fwd.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h" // for PlanNodeId

namespace starrocks {

// To manage pass through chunks between sink/sources in the same process.
using ChunkPtrVector = std::vector<vectorized::ChunkPtr>;
using PassThroughChunkKey = std::pair<TUniqueId, PlanNodeId>;

class PassThroughChunkBuffer {
public:
    PassThroughChunkBuffer() {}
    void append_chunk(const PassThroughChunkKey& key, const vectorized::ChunkPtr& chunk);
    void pull_cunks(const PassThroughChunkKey& key, ChunkPtrVector* chunks);

private:
    std::mutex _lock;
    std::map<PassThroughChunkKey, ChunkPtrVector> _buffer;
};
using PassThroughChunkBufferPtr = std::shared_ptr<PassThroughChunkBuffer>;

}; // namespace starrocks