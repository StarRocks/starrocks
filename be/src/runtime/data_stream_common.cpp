// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/data_stream_common.h"

#include "column/chunk.h"

namespace starrocks {

class PassThroughChannel {
public:
    void append_chunk(const vectorized::Chunk* chunk) {
        vectorized::ChunkPtr clone = chunk->clone_shared();
        {
            std::unique_lock l(_lock);
            _buffer.emplace_back(std::move(clone));
        }
    }

    void pull_chunks(ChunkPtrVector* chunks) {
        std::unique_lock l(_lock);
        chunks->swap(_buffer);
    }

private:
    std::mutex _lock; // lock-step to push/pull chunks
    ChunkPtrVector _buffer;
};

PassThroughChannel* PassThroughChunkBuffer::get_or_create_channel(const Key& key) {
    std::unique_lock l(_lock);
    auto it = _key_to_channel.find(key);
    if (it == _key_to_channel.end()) {
        auto* channel = new PassThroughChannel();
        _key_to_channel.emplace(std::make_pair(key, channel));
        it = _key_to_channel.find(key);
    }
    return it->second;
}

PassThroughChunkBuffer::~PassThroughChunkBuffer() {
    std::unique_lock l(_lock);
    for (auto& it : _key_to_channel) {
        delete it.second;
    }
}

void PassThroughContext::append_chunk(const vectorized::Chunk* chunk) {
    _channel->append_chunk(chunk);
}
void PassThroughContext::pull_chunks(ChunkPtrVector* chunks) {
    _channel->pull_chunks(chunks);
}

} // namespace starrocks