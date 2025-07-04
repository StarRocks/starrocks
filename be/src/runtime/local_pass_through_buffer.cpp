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
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/moodycamel/concurrentqueue.h"

namespace starrocks {

// channel per [fragment_instance_id, dest_node_id]
class PassThroughChannel {
public:
	PassThroughChannel(int32_t degree_of_parallelism) : _chunk_queues(degree_of_parallelism) {}

    ~PassThroughChannel() {
        _chunk_queues.clear();
    }

	void append_chunk(const Chunk* chunk, size_t chunk_size, int32_t index) {
        // Release allocated bytes in current MemTracker, since it would not be released at current MemTracker
        int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
        auto clone = chunk->clone_unique();
        int64_t physical_bytes = CurrentThread::current().get_consumed_bytes() - before_bytes;
        DCHECK_GE(physical_bytes, 0);
        CurrentThread::current().mem_release(physical_bytes);
        GlobalEnv::GetInstance()->passthrough_mem_tracker()->consume(physical_bytes);

		ChunkItem chunk_item(physical_bytes, chunk_size, std::move(clone));
		_chunk_queues[index].enqueue(std::move(chunk_item));
		++_num_chunks;
        _total_bytes += physical_bytes;
    }

	std::pair<ChunkUniquePtr, int64_t> pull_chunk(int32_t index) {
		ChunkItem item;
		if (!_chunk_queues[index].try_dequeue(item)) {
			return {nullptr, 0};
		}

		--_num_chunks;
        GlobalEnv::GetInstance()->passthrough_mem_tracker()->release(item.physical_bytes);
        // Consume physical bytes in current MemTracker, since later it would be released
        CurrentThread::current().mem_consume(item.physical_bytes);
        _total_bytes -= item.physical_bytes;
		return {std::move(item.chunk_ptr), item.chunk_size};
    }

	bool has_output(int32_t index) {
		return _chunk_queues[index].size_approx() > 0;
    }

    bool has_output() { return _num_chunks > 0; }

    int64_t get_total_bytes() const { return _total_bytes; }

private:
	struct ChunkItem {
        int64_t physical_bytes = 0;
		int64_t chunk_size = 0;
        ChunkUniquePtr chunk_ptr;

		ChunkItem() = default;

        ChunkItem(int64_t physical_bytes, int64_t chunk_size, ChunkUniquePtr&& chunk_ptr)
                : physical_bytes(physical_bytes),
                  chunk_size(chunk_size),
                  chunk_ptr(std::move(chunk_ptr)) {}
    };

	typedef moodycamel::ConcurrentQueue<ChunkItem> ChunkQueue;

    std::mutex _mutex;
	int32_t _degree_of_parallelism;
	std::vector<ChunkQueue> _chunk_queues;
	std::atomic_int64_t _num_chunks = 0;
    std::atomic_int64_t _total_bytes = 0;
};

PassThroughChunkBuffer::PassThroughChunkBuffer(const TUniqueId& query_id)
        : _mutex(), _query_id(query_id), _ref_count(1) {}

PassThroughChunkBuffer::~PassThroughChunkBuffer() {
    if (UNLIKELY(_ref_count != 0)) {
        LOG(WARNING) << "PassThroughChunkBuffer reference leak detected! query_id=" << print_id(_query_id)
                     << ", _ref_count=" << _ref_count;
    }
    for (auto& it : _key_to_channel) {
        delete it.second;
    }
    _key_to_channel.clear();
}

PassThroughChannel* PassThroughChunkBuffer::create_channel(const Key& key, int32_t degree_of_parallelism) {
    std::unique_lock lock(_mutex);
    auto it = _key_to_channel.find(key);
    if (it != _key_to_channel.end()) {
        return it->second;
    }

    auto* channel = new PassThroughChannel(degree_of_parallelism);
    _key_to_channel.emplace(key, channel);
    return channel;
}

PassThroughChannel* PassThroughChunkBuffer::get_channel(const Key& key) {
    std::unique_lock lock(_mutex);
    auto it = _key_to_channel.find(key);
    if (it != _key_to_channel.end()) {
        return it->second;
    }

	return nullptr;
}

void PassThroughContext::init(int32_t degree_of_parallelism) {
    _channel = _chunk_buffer->create_channel(PassThroughChunkBuffer::Key(_fragment_instance_id, _node_id),
				degree_of_parallelism);
}

void PassThroughContext::append_chunk(const Chunk* chunk, size_t chunk_size, int32_t index) {
    _channel->append_chunk(chunk, chunk_size, index);
}

std::pair<ChunkUniquePtr, int64_t> PassThroughContext::pull_chunk(int32_t index) {
    return _channel->pull_chunk(index);
}

bool PassThroughContext::has_output(int32_t index) {
    return _channel->has_output(index);
}

int64_t PassThroughContext::total_bytes() const {
    return _channel->get_total_bytes();
}

bool PassThroughContext::has_output() const {
    return _channel->has_output();
}

void PassThroughChunkBufferManager::open_fragment_instance(const TUniqueId& query_id) {
    VLOG_FILE << "PassThroughChunkBufferManager::open_fragment_instance, query_id = " << query_id;
    {
        std::unique_lock lock(_mutex);
        auto it = _query_id_to_buffer.find(query_id);
        if (it == _query_id_to_buffer.end()) {
            auto* buffer = new PassThroughChunkBuffer(query_id);
            _query_id_to_buffer.emplace(query_id, buffer);
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
