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
    inline static const int64_t SLICE_CHUNK_CAPACITY = 16L * 1024 * 1024; // 16MB

    class SliceChunk {
    public:
        using Buffer = raw::RawVector<uint8_t>; // for RAII and eliminating initialization overhead

        SliceChunk(int64_t capacity) : capacity_(capacity) {
            buffer_.reserve(capacity);
        }

        // return the number of bytes appended
        int64_t append(const uint8_t* data, int64_t size) {
            int64_t to_write_bytes = std::min(capacity_ - size_, size);
            std::memcpy(buffer_.data() + size_, data, to_write_bytes);
            size_ += to_write_bytes;
            return to_write_bytes;
        }

        bool is_full() {
            return size_ == capacity_;
        }

        bool is_empty() {
            return size_ == 0;
        }

        uint8_t* data() {
            return buffer_.data();
        };

        int64_t size() {
            return size_;
        }

    private:
        Buffer buffer_;
        int64_t size_{0};
        int64_t capacity_;
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
                _slice_chunk_queue.push(std::make_shared<SliceChunk>(SLICE_CHUNK_CAPACITY));
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
                _slice_chunk_queue.pop();
                auto task = [&, chunk]() {
#ifndef BE_TEST
                    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
                    CurrentThread::current().set_query_id(_runtime_state->query_id());
                    CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
#endif
                    auto status = _file->append(Slice(chunk->data(), chunk->size()));
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

                    _releasable_chunk_counter.fetch_sub(1);
                };
                to_enqueue_tasks.push_back(task);
            }
        }

        enqueue_tasks_and_maybe_submit_task(std::move(to_enqueue_tasks));
        return Status::OK();
    }

    const std::string& filename() const {
        return _file->filename();
    }

    int64_t tell() const {
        return _total_size;
    }

    int64_t releasable_memory() const {
        return _releasable_chunk_counter * SLICE_CHUNK_CAPACITY;
    }

    // called exactly once
    Status close() {
        DCHECK(_slice_chunk_queue.empty() || (_slice_chunk_queue.size() == 1 && !_slice_chunk_queue.front()->is_full()))
                        << "empty or at most one not full buffer";

        std::vector<Task> to_enqueue_tasks;
        if (!_slice_chunk_queue.empty() || !_slice_chunk_queue.front()->is_empty()) {
            auto chunk = _slice_chunk_queue.front();
            _slice_chunk_queue.pop();
            auto task = [&, chunk]() {
#ifndef BE_TEST
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
                CurrentThread::current().set_query_id(_runtime_state->query_id());
                CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
#endif
                auto status = _file->append(Slice(chunk->data(), chunk->size()));
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
                _releasable_chunk_counter.fetch_sub(1);
            };
            to_enqueue_tasks.push_back(task);
        }

        auto close_task = [&]() {
#ifndef BE_TEST
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
            CurrentThread::current().set_query_id(_runtime_state->query_id());
            CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
#endif
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
            _releasable_chunk_counter.fetch_add(1);
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
    std::queue<SliceChunkPtr> _slice_chunk_queue;
    std::atomic_int64_t _releasable_chunk_counter{0};
    std::mutex _mutex; // guards following
    bool _has_in_flight_io{false};
    std::queue<std::function<void()>> _task_queue;
    Status _io_status;
};

} // namespace starrocks::io
