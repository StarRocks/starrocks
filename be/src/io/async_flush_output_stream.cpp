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

#include "async_flush_output_stream.h"

namespace starrocks::io {

int64_t AsyncFlushOutputStream::SliceChunk::append(const uint8_t* data, int64_t size) {
    int64_t old_size = static_cast<int64_t>(buffer_.size());
    int64_t to_write_bytes = std::min(max_size_ - old_size, size);
    buffer_.resize(buffer_.size() + to_write_bytes);
    std::memcpy(buffer_.data() + old_size, data, to_write_bytes);
    DCHECK(buffer_.size() <= max_size_);
    return to_write_bytes;
}

AsyncFlushOutputStream::AsyncFlushOutputStream(std::unique_ptr<WritableFile> file, PriorityThreadPool* io_executor,
                                               RuntimeState* runtime_state)
        : _file(std::move(file)), _io_executor(io_executor), _runtime_state(runtime_state) {}

Status AsyncFlushOutputStream::write(const uint8_t* data, int64_t size) {
    _total_size += size;
    DCHECK(_slice_chunk_queue.empty() || (_slice_chunk_queue.size() == 1 && !_slice_chunk_queue.front()->is_full()))
            << "empty or at most one not full buffer";
    while (size > 0) {
        // append a new buffer if queue is empty or the last buffer is full
        if (_slice_chunk_queue.empty() || _slice_chunk_queue.back()->is_full()) {
            _slice_chunk_queue.push_back(new SliceChunk(BUFFER_MAX_SIZE));
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
            _releasable_bytes.fetch_add(chunk->get_buffer_ptr()->capacity());

            auto task = [this, chunk]() {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
                CurrentThread::current().set_query_id(_runtime_state->query_id());
                CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
                auto buffer = chunk->get_buffer_ptr();
                DeferOp op([this, chunk, capacity = buffer->capacity()] {
                    _releasable_bytes.fetch_sub(capacity);
                    delete chunk;
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
                    bool ok = _io_executor->offer(task);
                    if (!ok) {
                        _io_status.update(Status::ResourceBusy("fail to submit io task"));
                    }
                }
            };
            to_enqueue_tasks.emplace_back(task);
        }
    }

    if (!to_enqueue_tasks.empty()) {
        enqueue_tasks_and_maybe_submit_task(std::move(to_enqueue_tasks));
    }
    return Status::OK();
}

Status AsyncFlushOutputStream::close() {
    DCHECK(_slice_chunk_queue.empty() || (_slice_chunk_queue.size() == 1 && !_slice_chunk_queue.front()->is_full()))
            << "empty or at most one not full buffer";

    std::vector<Task> to_enqueue_tasks;
    if (!_slice_chunk_queue.empty() && !_slice_chunk_queue.front()->is_empty()) {
        auto chunk = _slice_chunk_queue.front();
        _slice_chunk_queue.pop_front();
        _releasable_bytes.fetch_add(chunk->get_buffer_ptr()->capacity());
        auto task = [&, chunk]() {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
            CurrentThread::current().set_query_id(_runtime_state->query_id());
            CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
            auto buffer = chunk->get_buffer_ptr();
            DeferOp op([this, chunk, capacity = buffer->capacity()] {
                _releasable_bytes.fetch_sub(capacity);
                delete chunk;
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
                bool ok = _io_executor->offer(task);
                if (!ok) {
                    _io_status.update(Status::ResourceBusy("fail to submit io task"));
                }
            }
        };
        to_enqueue_tasks.emplace_back(task);
    }

    auto close_task = [&]() {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
        CurrentThread::current().set_query_id(_runtime_state->query_id());
        CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
        auto status = _file->close();
        {
            std::scoped_lock lock(_mutex);
            _io_status.update(status);
            DCHECK(_task_queue.empty()); // close task is the last task
            _has_in_flight_io = false;
            _promise.set_value(_io_status); // notify
        }
    };
    to_enqueue_tasks.emplace_back(close_task);
    enqueue_tasks_and_maybe_submit_task(std::move(to_enqueue_tasks));
    return Status::OK();
}

void AsyncFlushOutputStream::enqueue_tasks_and_maybe_submit_task(std::vector<Task> tasks) {
    if (_io_executor == nullptr) {
        for (auto& task : tasks) {
            task();
        }
        return;
    }

    std::scoped_lock lock(_mutex);
    std::for_each(tasks.begin(), tasks.end(), [&](auto& task) { _task_queue.push(task); });

    if (_has_in_flight_io) {
        return;
    }
    auto task = _task_queue.front();
    _task_queue.pop();
    _has_in_flight_io = true;
    bool ok = _io_executor->offer(task);
    if (!ok) {
        _io_status.update(Status::ResourceBusy("fail to submit io task"));
    }
}
} // namespace starrocks::io
