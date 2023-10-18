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

#include "runtime/local_pass_through_buffer.h"

#include "column/chunk.h"
#include "common/logging.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

// channel per [sender_id]
class PassThroughSenderChannel {
public:
    PassThroughSenderChannel(std::atomic_int64_t& total_bytes) : _total_bytes(total_bytes) {}

    ~PassThroughSenderChannel() {
        if (_physical_bytes > 0) {
            CurrentThread::current().mem_consume(_physical_bytes);
        }
    }

    void append_chunk(const Chunk* chunk, size_t chunk_size, int32_t driver_sequence) {
        // Release allocated bytes in current MemTracker, since it would not be released at current MemTracker
        int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
        auto clone = chunk->clone_unique();
        int64_t physical_bytes = CurrentThread::current().get_consumed_bytes() - before_bytes;
        DCHECK_GE(physical_bytes, 0);
        CurrentThread::current().mem_release(physical_bytes);

        std::unique_lock lock(_mutex);
        _buffer.emplace_back(std::make_pair(std::move(clone), driver_sequence));
        _bytes.push_back(chunk_size);
        _physical_bytes += physical_bytes;
        _total_bytes += physical_bytes;
    }
    void pull_chunks(ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes) {
        std::unique_lock lock(_mutex);
        chunks->swap(_buffer);
        bytes->swap(_bytes);

        // Consume physical bytes in current MemTracker, since later it would be released
        CurrentThread::current().mem_consume(_physical_bytes);
        _total_bytes -= _physical_bytes;
        _physical_bytes = 0;
    }

private:
    std::mutex _mutex; // lock-step to push/pull chunks
    ChunkUniquePtrVector _buffer;
    std::vector<size_t> _bytes;
    int64_t _physical_bytes = 0; // Physical consumed bytes for each chunk
    std::atomic_int64_t& _total_bytes;
};

// channel per [fragment_instance_id, dest_node_id]
class PassThroughChannel {
public:
    PassThroughSenderChannel* get_or_create_sender_channel(int sender_id) {
        std::unique_lock lock(_mutex);
        auto it = _sender_id_to_channel.find(sender_id);
        if (it == _sender_id_to_channel.end()) {
            auto* channel = new PassThroughSenderChannel(_total_bytes);
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

    int64_t get_total_bytes() const { return _total_bytes; }

private:
    std::mutex _mutex;
    std::unordered_map<int, PassThroughSenderChannel*> _sender_id_to_channel;
    std::atomic_int64_t _total_bytes = 0;
};

PassThroughChunkBuffer::PassThroughChunkBuffer(const TUniqueId& query_id)
        : _mutex(), _query_id(query_id), _ref_count(1) {}

PassThroughChunkBuffer::~PassThroughChunkBuffer() {
    DCHECK(_ref_count == 0);
    for (auto& it : _key_to_channel) {
        delete it.second;
    }
    _key_to_channel.clear();
}

PassThroughChannel* PassThroughChunkBuffer::get_or_create_channel(const Key& key) {
    std::unique_lock lock(_mutex);
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

void PassThroughContext::append_chunk(int sender_id, const Chunk* chunk, size_t chunk_size, int32_t driver_sequence) {
    PassThroughSenderChannel* sender_channel = _channel->get_or_create_sender_channel(sender_id);
    sender_channel->append_chunk(chunk, chunk_size, driver_sequence);
}
void PassThroughContext::pull_chunks(int sender_id, ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes) {
    PassThroughSenderChannel* sender_channel = _channel->get_or_create_sender_channel(sender_id);
    sender_channel->pull_chunks(chunks, bytes);
}

int64_t PassThroughContext::total_bytes() const {
    return _channel->get_total_bytes();
}

void PassThroughChunkBufferManager::open_fragment_instance(const TUniqueId& query_id) {
    VLOG_FILE << "PassThroughChunkBufferManager::open_fragment_instance, query_id = " << query_id;
    {
        std::unique_lock lock(_mutex);
        auto it = _query_id_to_buffer.find(query_id);
        if (it == _query_id_to_buffer.end()) {
            auto* buffer = new PassThroughChunkBuffer(query_id);
            _query_id_to_buffer.emplace(std::make_pair(query_id, buffer));
        } else {
            it->second->ref();
        }
    }
}

void PassThroughChunkBufferManager::close() {
    std::unique_lock lock(_mutex);
    for (auto it = _query_id_to_buffer.begin(); it != _query_id_to_buffer.end();) {
        delete it->second;
        it = _query_id_to_buffer.erase(it);
    }
}

void PassThroughChunkBufferManager::close_fragment_instance(const TUniqueId& query_id) {
    VLOG_FILE << "PassThroughChunkBufferManager::close_fragment_instance, query_id = " << query_id;
    {
        std::unique_lock lock(_mutex);
        auto it = _query_id_to_buffer.find(query_id);
        if (it != _query_id_to_buffer.end()) {
            int rc = it->second->unref();
            if (rc == 0) {
                delete it->second;
                _query_id_to_buffer.erase(it);
            }
        }
    }
}

PassThroughChunkBuffer* PassThroughChunkBufferManager::get(const TUniqueId& query_id) {
    {
        std::unique_lock lock(_mutex);
        auto it = _query_id_to_buffer.find(query_id);
        if (it == _query_id_to_buffer.end()) {
            return nullptr;
        }
        return it->second;
    }
}

} // namespace starrocks
