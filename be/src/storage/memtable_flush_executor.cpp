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

#include "gen_cpp/data.pb.h"
#include "runtime/current_thread.h"

namespace starrocks {

class MemtableFlushTask final : public Runnable {
public:
    MemtableFlushTask(FlushToken* flush_token, std::unique_ptr<MemTable> memtable, bool eos,
                      std::function<void(std::unique_ptr<SegmentPB>, bool, int64_t)> cb)
            : _flush_token(flush_token),
              _memtable(std::move(memtable)),
              _eos(eos),
              _cb(std::move(cb)),
              _create_time_ns(MonotonicNanos()) {}

    ~MemtableFlushTask() override = default;

    void run() override {
        _flush_token->_stats.queueing_memtable_num--;
        _flush_token->_stats.pending_time_ns += MonotonicNanos() - _create_time_ns;
        std::unique_ptr<SegmentPB> segment = nullptr;
        int64_t flush_data_size = 0;
        if (_memtable) {
            SCOPED_THREAD_LOCAL_MEM_SETTER(_memtable->mem_tracker(), false);
            segment = std::make_unique<SegmentPB>();

            _flush_token->_stats.cur_flush_count++;
            _flush_token->_flush_memtable(_memtable.get(), segment.get(), _eos, &flush_data_size);
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
            _cb(std::move(segment), _eos, flush_data_size);
        }
    }

private:
    FlushToken* _flush_token;
    std::unique_ptr<MemTable> _memtable;
    bool _eos;
    std::function<void(std::unique_ptr<SegmentPB>, bool, int64_t)> _cb;
    int64_t _create_time_ns;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.memtable_stats.flush_time_ns / 1000 / 1000 << ", flush count=" << stat.flush_count
       << "), flush flush_size_bytes=" << stat.memtable_stats.flush_memory_size;
    return os;
}

Status FlushToken::submit(std::unique_ptr<MemTable> memtable, bool eos,
                          std::function<void(std::unique_ptr<SegmentPB>, bool, int64_t)> cb) {
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
    if (st.ok()) {
        return;
    }

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

void FlushToken::_flush_memtable(MemTable* memtable, SegmentPB* segment, bool eos, int64_t* flush_data_size) {
    // If previous flush has failed, return directly
    if (!status().ok()) {
        return;
    }

    set_status(memtable->flush(segment, eos, flush_data_size));
    _stats.flush_count++;
    _stats.memtable_stats += memtable->get_stat();
}

Status MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = std::max(data_dir_num * min_threads, min_threads);
    return ThreadPoolBuilder("memtable_flush") // mem table flush
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool);
}

// Used in shared-data mode
Status MemTableFlushExecutor::init_for_lake_table(const std::vector<DataDir*>& data_dirs) {
    int max_threads = calc_max_threads_for_lake_table(data_dirs);
    return ThreadPoolBuilder("lake_memtable_flush") // mem table flush
            .set_min_threads(0)
            .set_max_threads(max_threads)
            .build(&_flush_pool);
}

// Calculate max thread number for lake table
// If lake_flush_thread_num_per_store > 0, return lake_flush_thread_num_per_store * data_dirs.size()
// If lake_flush_thread_num_per_store == 0, return 2 * cpu_cores * data_dirs.size()
// If lake_flush_thread_num_per_store < 0, return |lake_flush_thread_num_per_store| * cpu_cores * data_dirs.size()
// data_dirs.size() is limited in [1, 8]
int MemTableFlushExecutor::calc_max_threads_for_lake_table(const std::vector<DataDir*>& data_dirs) {
    int threads = config::lake_flush_thread_num_per_store;
    if (threads == 0) {
        threads = -2;
    }
    if (threads <= 0) {
        threads = -threads;
        threads *= CpuInfo::num_cores();
    }
    int data_dir_num = static_cast<int>(data_dirs.size());
    data_dir_num = std::max(1, data_dir_num);
    data_dir_num = std::min(8, data_dir_num);
    return data_dir_num * threads;
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
