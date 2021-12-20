// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/data_stream_common.h"

#include "column/chunk.h"
#include "common/logging.h"

namespace starrocks {

// channel per [sender_id]
class PassThroughSenderChannel {
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

// channel per [fragment_instance_id, dest_node_id]
class PassThroughChannel {
public:
    PassThroughSenderChannel* get_or_create_sender_channel(int sender_id) {
        std::unique_lock l(_mutex);
        auto it = _sender_id_to_channel.find(sender_id);
        if (it == _sender_id_to_channel.end()) {
            auto* channel = new PassThroughSenderChannel();
            _sender_id_to_channel.emplace(std::make_pair(sender_id, channel));
            return channel;
        } else {
            return it->second;
        }
    }
    ~PassThroughChannel() {
        for (auto& it : _sender_id_to_channel) {
            delete it.second;
        }
        _sender_id_to_channel.clear();
    }

private:
    std::mutex _mutex;
    std::unordered_map<int, PassThroughSenderChannel*> _sender_id_to_channel;
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

void PassThroughContext::append_chunk(int sender_id, const vectorized::Chunk* chunk, size_t chunk_size) {
    PassThroughSenderChannel* sender_channel = _channel->get_or_create_sender_channel(sender_id);
    sender_channel->append_chunk(chunk, chunk_size);
}
void PassThroughContext::pull_chunks(int sender_id, ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes) {
    PassThroughSenderChannel* sender_channel = _channel->get_or_create_sender_channel(sender_id);
    sender_channel->pull_chunks(chunks, bytes);
}

} // namespace starrocks