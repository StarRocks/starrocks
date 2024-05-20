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

#include <string>
#include <vector>
#include <queue>
#include <future>

#include "common/statusor.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"
#include "util/raw_container.h"
#include "fs/fs.h"

namespace starrocks::io {

// NOT thread-safe
class AsyncFlushOutputStream {
public:
    inline static const int64_t BUFFER_MAX_SIZE = 16L * 1024 * 1024; // 16MB

    class SliceChunk {
    public:
        using Buffer = raw::RawVector<uint8_t>; // for RAII and eliminating initialization overhead

        SliceChunk(int64_t max_size) : max_size_(max_size) {}

        // return the number of bytes appended
        int64_t append(const uint8_t* data, int64_t size) {
            int64_t old_size = static_cast<int64_t>(buffer_.size());
            int64_t to_write_bytes = std::min(max_size_ - old_size, size);
            buffer_.resize(buffer_.size() + to_write_bytes);
            std::memcpy(buffer_.data() + old_size, data, to_write_bytes);
            DCHECK(buffer_.size() <= max_size_);
            return to_write_bytes;
        }

        bool is_full() {
            return buffer_.size() == max_size_;
        }

        bool is_empty() {
            return buffer_.empty();
        }

        Buffer* get_buffer() {
            return &buffer_;
        }

    private:
        Buffer buffer_;
        int64_t max_size_{0};
    };

    using SliceChunkPtr = std::shared_ptr<SliceChunk>;
    using Task = std::function<void()>;

    AsyncFlushOutputStream(std::unique_ptr<WritableFile> file, PriorityThreadPool* io_executor, RuntimeState* runtime_state) : _file(std::move(file)), _io_executor(io_executor), _runtime_state(runtime_state) {}

    Status write(const uint8_t* data, int64_t size) {
        _total_size += size;
        DCHECK(_slice_chunk_queue.empty() || (_slice_chunk_queue.size() == 1 && !_slice_chunk_queue.front()->is_full()))
            << "empty or at most one not full buffer";
        while (size > 0) {
            // append a new buffer if queue is empty or the last buffer is full
            if (_slice_chunk_queue.empty() || _slice_chunk_queue.back()->is_full()) {
                _slice_chunk_queue.push_back(std::make_shared<SliceChunk>(BUFFER_MAX_SIZE));
            }
            SliceChunkPtr& last_chunk = _slice_chunk_queue.back();
            int64_t appended_bytes = last_chunk->append(data, size);
            data += appended_bytes;
            size -= appended_bytes;
        }
        DCHECK(size == 0);

        std::vector<Task> to_enqueue_tasks;
        {
            while (!_slice_chunk_queue.empty() && _slice_chunk_queue.front()->is_full()) {
                auto chunk = _slice_chunk_queue.front();
                _slice_chunk_queue.pop_front();
                _releasable_bytes.fetch_add(chunk->get_buffer()->capacity());

                auto task = [this, chunk = std::move(chunk)]() mutable {
                    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
                    CurrentThread::current().set_query_id(_runtime_state->query_id());
                    CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
                    auto scoped_chunk = std::move(chunk);
                    auto buffer = scoped_chunk->get_buffer();
                    DeferOp op([this, capacity = buffer->capacity()] {
                        _releasable_bytes.fetch_sub(capacity);
                    });
                    auto status = _file->append(Slice(buffer->data(), buffer->size()));
                    {
                        std::scoped_lock lock(_mutex);
                        _io_status.update(status);
                        if (_task_queue.empty()) {
                            _has_in_flight_io = false;
                            return;
                        }
                        auto task = _task_queue.front();
                        _task_queue.pop();
                        CHECK(_io_executor->offer(task)); // TODO: handle
                    }
                };
                to_enqueue_tasks.push_back(task);
            }
        }

        if (!to_enqueue_tasks.empty()) {
            enqueue_tasks_and_maybe_submit_task(std::move(to_enqueue_tasks));
        }
        return Status::OK();
    }

    const std::string& filename() const {
        return _file->filename();
    }

    int64_t tell() const {
        return _total_size;
    }

    int64_t releasable_memory() const {
        return _releasable_bytes.load();
    }

    // called exactly once
    Status close() {
        DCHECK(_slice_chunk_queue.empty() || (_slice_chunk_queue.size() == 1 && !_slice_chunk_queue.front()->is_full()))
                        << "empty or at most one not full buffer";

        std::vector<Task> to_enqueue_tasks;
        if (!_slice_chunk_queue.empty() || !_slice_chunk_queue.front()->is_empty()) {
            auto chunk = _slice_chunk_queue.front();
            _slice_chunk_queue.pop_front();
            _releasable_bytes.fetch_add(chunk->get_buffer()->capacity());
            auto task = [&, chunk = std::move(chunk)]() mutable{
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
                CurrentThread::current().set_query_id(_runtime_state->query_id());
                CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
                auto scoped_chunk = std::move(chunk);
                auto buffer = scoped_chunk->get_buffer();
                DeferOp op([this, capacity = buffer->capacity()] {
                    _releasable_bytes.fetch_sub(capacity);
                });
                auto status = _file->append(Slice(buffer->data(), buffer->size()));
                {
                    std::scoped_lock lock(_mutex);
                    _io_status.update(status);
                    if (_task_queue.empty()) {
                        _has_in_flight_io = false;
                        return;
                    }
                    auto task = _task_queue.front();
                    _task_queue.pop();
                    CHECK(_io_executor->offer(task)); // TODO: handle
                }
            };
            to_enqueue_tasks.push_back(task);
        }

        auto close_task = [&]() {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
            CurrentThread::current().set_query_id(_runtime_state->query_id());
            CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
            auto status = _file->close();
            {
                std::scoped_lock lock(_mutex);
                _io_status.update(status);
                CHECK(_task_queue.empty()); // close task is the last task
                _has_in_flight_io = false;
                _promise.set_value(_io_status); // notify
            }
        };
        to_enqueue_tasks.push_back(close_task);
        enqueue_tasks_and_maybe_submit_task(std::move(to_enqueue_tasks));
        return Status::OK();
    }

    // called exactly once
    std::future<Status> io_status() {
        return _promise.get_future();
    };

    void enqueue_tasks_and_maybe_submit_task(std::vector<Task> tasks) {
        std::scoped_lock lock(_mutex);
        std::for_each(tasks.begin(), tasks.end(), [&](auto& task) {
            _task_queue.push(task);
        });

        if (_has_in_flight_io) {
            return;
        }
        auto task = _task_queue.front();
        _task_queue.pop();
        _has_in_flight_io = true;
        CHECK(_io_executor->offer(task));
    }

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
