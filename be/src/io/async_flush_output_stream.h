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

#include <future>
#include <queue>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "fs/fs.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"
#include "util/raw_container.h"

namespace starrocks::io {

// NOT thread-safe
class AsyncFlushOutputStream {
public:
    inline static const int64_t BUFFER_MAX_SIZE = 16L * 1024 * 1024; // 16MB

    class SliceChunk {
    public:
        using Buffer = raw::RawVector<uint8_t>;

        SliceChunk(int64_t max_size) : max_size_(max_size) {}

        // return the number of bytes appended
        int64_t append(const uint8_t* data, int64_t size);

        bool is_full() { return buffer_.size() == max_size_; }

        bool is_empty() { return buffer_.empty(); }

        Buffer* get_buffer_ptr() { return &buffer_; }

    private:
        Buffer buffer_;
        int64_t max_size_{0};
    };

    using SliceChunkPtr = SliceChunk*;
    using Task = std::function<void()>;

    AsyncFlushOutputStream(std::unique_ptr<WritableFile> file, PriorityThreadPool* io_executor,
                           RuntimeState* runtime_state);

    Status write(const uint8_t* data, int64_t size);

    const std::string& filename() const { return _file->filename(); }

    int64_t tell() const { return _total_size; }

    int64_t releasable_memory() const { return _releasable_bytes.load(); }

    // called exactly once
    Status close();

    // called exactly once
    std::future<Status> io_status() { return _promise.get_future(); };

    void enqueue_tasks_and_maybe_submit_task(std::vector<Task> tasks);

private:
    int64_t _total_size{0};
    std::promise<Status> _promise;
    std::unique_ptr<WritableFile> _file;
    PriorityThreadPool* _io_executor = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::deque<SliceChunkPtr> _slice_chunk_queue;
    std::atomic_int64_t _releasable_bytes{0};

    std::mutex _mutex; // guards following
    bool _has_in_flight_io{false};
    std::queue<std::function<void()>> _task_queue;
    Status _io_status;
};

} // namespace starrocks::io
