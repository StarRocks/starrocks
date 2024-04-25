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
#include "fs/fs.h"

namespace starrocks::io {

// NOT thread-safe
class AsyncFlushOutputStream {
public:
    AsyncFlushOutputStream(std::unique_ptr<WritableFile> file, PriorityThreadPool* io_executor, RuntimeState* runtime_state) : _file(std::move(file)), _io_executor(io_executor), _runtime_state(runtime_state) {}

    Status write(const uint8_t* data, size_t size) {
        _total_size += size;

        auto buffer = std::make_shared<std::vector<uint8_t>>(size);
        std::memcpy(buffer->data(), data, size);

        auto task = [&, buffer]() {
#ifndef BE_TEST
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
        CurrentThread::current().set_query_id(_runtime_state->query_id());
        CurrentThread::current().set_fragment_instance_id(_runtime_state->fragment_instance_id());
#endif
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
                CHECK(_io_executor->offer(task));
            }
        };

        enqueue_and_maybe_submit_task(task);
        return Status::OK();
    }

    int64_t tell() const {
        return _total_size;
    }

    // called exactly once
    Status close() {
        auto task = [&]() {
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

        enqueue_and_maybe_submit_task(task);
        return Status::OK();
    }

    // called exactly once
    std::future<Status> io_status() {
        return _promise.get_future();
    };

    void enqueue_and_maybe_submit_task(const std::function<void()>& task) {
        std::scoped_lock lock(_mutex);
        _task_queue.push(task);
        if (_has_in_flight_io) {
            return;
        }
        auto task2 = _task_queue.front();
        _task_queue.pop();
        _has_in_flight_io = true;
        CHECK(_io_executor->offer(task2));
    }

private:
    int64_t _total_size{0};
    std::promise<Status> _promise;
    std::unique_ptr<WritableFile> _file;
    PriorityThreadPool* _io_executor = nullptr;
    RuntimeState* _runtime_state = nullptr;

    std::mutex _mutex; // guards following
    bool _has_in_flight_io{false};
    std::queue<std::function<void()>> _task_queue;
    Status _io_status;
};

} // namespace starrocks::io
