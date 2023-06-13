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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memtable_flush_executor.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/memtable_flush_executor.h"

#include <memory>

#include "exec/workgroup/scan_executor.h"
#include "gen_cpp/data.pb.h"
#include "runtime/current_thread.h"
#include "storage/memtable.h"
#include "util/bthreads/executor.h"

namespace starrocks {

class MemtableFlushTask final : public Runnable {
public:
    MemtableFlushTask(FlushToken* flush_token, std::unique_ptr<MemTable> memtable, bool eos,
                      std::function<void(std::unique_ptr<SegmentPB>, bool)> cb)
            : _flush_token(flush_token), _memtable(std::move(memtable)), _eos(eos), _cb(std::move(cb)) {}

    ~MemtableFlushTask() override = default;

    void run() override {
        _flush_token->_stats.queueing_memtable_num--;
        std::unique_ptr<SegmentPB> segment = nullptr;
        if (_memtable) {
            SCOPED_THREAD_LOCAL_MEM_SETTER(_memtable->mem_tracker(), false);
            segment = std::make_unique<SegmentPB>();

            _flush_token->_stats.cur_flush_count++;
            _flush_token->_flush_memtable(_memtable.get(), segment.get());
            _flush_token->_stats.cur_flush_count--;
            _memtable.reset();

            // memtable flush fail, skip sync segment
            if (!_flush_token->status().ok()) {
                return;
            }

            // segment doesn't has path means no memtable had flushed, so that reset segment
            if (!segment->has_path() && !segment->has_delete_path() && !segment->has_update_path()) {
                segment.reset();
            }
        }

        if (_cb) {
            _cb(std::move(segment), _eos);
        }
    }

private:
    FlushToken* _flush_token;
    std::unique_ptr<MemTable> _memtable;
    bool _eos;
    std::function<void(std::unique_ptr<SegmentPB>, bool)> _cb;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / 1000 / 1000 << ", flush count=" << stat.flush_count << ")"
       << ", flush flush_size_bytes = " << stat.flush_size_bytes;
    return os;
}

Status FlushToken::submit(std::unique_ptr<MemTable> memtable, bool eos,
                          std::function<void(std::unique_ptr<SegmentPB>, bool)> cb) {
    RETURN_IF_ERROR(status());
    if (memtable == nullptr && !eos) {
        return Status::InternalError(fmt::format("memtable=null eos=false"));
    }
    // Does not acount the size of MemtableFlushTask into any memory tracker
    SCOPED_THREAD_LOCAL_MEM_SETTER(nullptr, false);
    auto task = std::make_shared<MemtableFlushTask>(this, std::move(memtable), eos, std::move(cb));
    _stats.queueing_memtable_num++;
    return _flush_token->submit(std::move(task));
}

void FlushToken::shutdown() {
    _flush_token->shutdown();
}

void FlushToken::cancel(const Status& st) {
    if (st.ok()) return;
    std::lock_guard l(_status_lock);
    if (_status.ok()) {
        _status = st;
    }
}

Status FlushToken::wait() {
    _flush_token->wait();
    std::lock_guard l(_status_lock);
    return _status;
}

void FlushToken::_flush_memtable(MemTable* memtable, SegmentPB* segment) {
    // If previous flush has failed, return directly
    if (!status().ok()) return;

    MonotonicStopWatch timer;
    timer.start();
    set_status(memtable->flush(segment));
    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_count++;
    _stats.flush_size_bytes += memtable->memory_usage();
}

Status FlushQueue::init(bthread::Executor* executor) {
    bthread::ExecutionQueueOptions opts;
    opts.executor = executor;
    int r = bthread::execution_queue_start(&_queue_id, &opts, _execute, this);
    RETURN_IF(r != 0, Status::ResourceBusy("start execution queue failed"));
    return {};
}

Status FlushQueue::submit(std::unique_ptr<MemTable> memtable, bool eos, SegmentCallback cb) {
    RETURN_IF_ERROR(status());
    RETURN_IF(memtable == nullptr && !eos, Status::InternalError(fmt::format("memtable=null eos=false")));

    SCOPED_THREAD_LOCAL_MEM_SETTER(nullptr, false);
    FlushTask task(std::move(memtable));
    task.callback = std::move(cb);
    int r = bthread::execution_queue_execute(_queue_id, std::move(task));
    RETURN_IF(r != 0, Status::ResourceBusy("execute_queue_execute failed"));
    _stats.queueing_memtable_num++;
    return {};
}

Status FlushQueue::wait() {
    int r = bthread::execution_queue_join(_queue_id);
    RETURN_IF(r != 0, Status::ResourceBusy("join execution queue error"));
    return {};
}

void FlushQueue::cancel(const Status& st) {
    if (st.ok()) return;
    {
        std::lock_guard<SpinLock> guard(_status_lock);
        if (_status.ok()) {
            _status = st;
        }
    }

    FlushTask task(nullptr);
    task.abort = true;
    int r = bthread::execution_queue_execute(_queue_id, task);
    LOG_IF(WARNING, r != 0) << "Cancel execution queue error: " << r;
}

void FlushQueue::close() {
    int r = bthread::execution_queue_stop(_queue_id);
    LOG_IF(WARNING, r != 0) << "Fail to stop execution queue: " << r;
    r = bthread::execution_queue_join(_queue_id);
    LOG_IF(WARNING, r != 0) << "Fail to join execution queue: " << r;
}

Status FlushQueue::_flush_memtable(MemTable* memtable, SegmentPB* segment) {
    if (!status().ok()) return status();

    MonotonicStopWatch timer;
    timer.start();
    _stats.cur_flush_count++;
    set_status(memtable->flush(segment));
    _stats.cur_flush_count--;
    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_count++;
    _stats.flush_size_bytes += memtable->memory_usage();
    return status();
}

int FlushQueue::_execute(void* meta, bthread::TaskIterator<FlushTask>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }
    auto* queue = static_cast<FlushQueue*>(meta);
    for (; iter; ++iter) {
        if (iter->abort) {
            continue;
        }
        queue->_stats.queueing_memtable_num--;
        std::unique_ptr<SegmentPB> segment = nullptr;
        if (iter->memtable) {
            SCOPED_THREAD_LOCAL_MEM_SETTER(iter->memtable->mem_tracker(), false);
            segment = std::make_unique<SegmentPB>();
            Status st = queue->_flush_memtable(iter->memtable.get(), segment.get());
            iter->memtable.reset();
            if (!st.ok()) {
                continue;
            }
            if (!segment->has_path() && !segment->has_delete_path() && !segment->has_update_path()) {
                segment.reset();
            }
        }
        if (iter->callback) {
            iter->callback(std::move(segment), iter->eos);
        }
    }
    return 0;
}

Status MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = std::max(data_dir_num * min_threads, min_threads);
    RETURN_IF_ERROR(ThreadPoolBuilder("memtable_flush") // mem table flush
                            .set_min_threads(min_threads)
                            .set_max_threads(max_threads)
                            .build(&_flush_pool));
    _executor = std::make_unique<bthreads::ThreadPoolExecutor>(_flush_pool.get(), kDontTakeOwnership);
    return {};
}

Status MemTableFlushExecutor::update_max_threads(int max_threads) {
    if (_flush_pool != nullptr) {
        return _flush_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

std::unique_ptr<FlushToken> MemTableFlushExecutor::create_flush_token(ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<FlushToken>(_flush_pool->new_token(execution_mode));
}

} // namespace starrocks