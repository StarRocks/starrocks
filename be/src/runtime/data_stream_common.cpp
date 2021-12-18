// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/data_stream_common.h"

#include "column/chunk.h"
#include "common/logging.h"

namespace starrocks {

class PassThroughChannel {
public:
    void append_chunk(const vectorized::Chunk* chunk, size_t chunk_size) {
        auto clone = chunk->clone_unique();
        {
            std::unique_lock l(_mutex);
            _buffer.emplace_back(std::move(clone));
            _bytes.push_back(chunk_size);
        }
    }

    void pull_chunks(ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes) {
        {
            std::unique_lock l(_mutex);
            chunks->swap(_buffer);
            bytes->swap(_bytes);
        }
    }

private:
    std::mutex _mutex; // lock-step to push/pull chunks
    ChunkUniquePtrVector _buffer;
    std::vector<size_t> _bytes;
};

PassThroughChunkBuffer::PassThroughChunkBuffer(const TUniqueId& query_id) : _mutex(), _query_id(query_id) {
    VLOG_FILE << "PassThroughChunkBuffer::create. query_id = " << _query_id;
}

PassThroughChunkBuffer::~PassThroughChunkBuffer() {
    VLOG_FILE << "PassThroughChunkBuffer::destroy. query_id = " << _query_id;
    for (auto& it : _key_to_channel) {
        delete it.second;
    }
    _key_to_channel.clear();
}

PassThroughChannel* PassThroughChunkBuffer::get_or_create_channel(const Key& key) {
    std::unique_lock l(_mutex);
    auto it = _key_to_channel.find(key);
    if (it == _key_to_channel.end()) {
        auto* channel = new PassThroughChannel();
        _key_to_channel.emplace(std::make_pair(key, channel));
        return channel;
    } else {
        return it->second;
    }
}

void PassThroughContext::init() {
    _channel = _chunk_buffer->get_or_create_channel(PassThroughChunkBuffer::Key(_fragment_instance_id, _node_id));
}

void PassThroughContext::append_chunk(const vectorized::Chunk* chunk, size_t chunk_size) {
    _channel->append_chunk(chunk, chunk_size);
}
void PassThroughContext::pull_chunks(ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes) {
    _channel->pull_chunks(chunks, bytes);
}

} // namespace starrocks