// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <map>

#include "column/vectorized_fwd.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h" // for PlanNodeId

namespace starrocks {

// To manage pass through chunks between sink/sources in the same process.
using ChunkPtrVector = std::vector<vectorized::ChunkPtr>;
class PassThroughChannel;

class PassThroughChunkBuffer {
public:
    using Key = std::pair<TUniqueId, PlanNodeId>;
    PassThroughChunkBuffer() = default;
    ~PassThroughChunkBuffer();
    PassThroughChannel* get_or_create_channel(const Key& key);

private:
    std::mutex _lock;
    std::map<Key, PassThroughChannel*> _key_to_channel;
};
using PassThroughChunkBufferPtr = std::shared_ptr<PassThroughChunkBuffer>;

class PassThroughContext {
public:
    PassThroughContext(const PassThroughChunkBufferPtr& chunk_buffer, TUniqueId fragment_instance_id,
                       PlanNodeId node_id)
            : _chunk_buffer(chunk_buffer) {
        _channel = _chunk_buffer->get_or_create_channel(PassThroughChunkBuffer::Key(fragment_instance_id, node_id));
    }

    void append_chunk(const vectorized::Chunk* chunk);
    void pull_chunks(ChunkPtrVector* chunks);

private:
    PassThroughChunkBufferPtr _chunk_buffer;
    PassThroughChannel* _channel;
};

} // namespace starrocks