// This file is made available under Elastic License 2.0.
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

#include <functional>
#include <memory>

#include "runtime/current_thread.h"
#include "storage/memtable.h"
#include "storage/vectorized/memtable.h"
#include "util/defer_op.h"
#include "util/scoped_cleanup.h"

namespace starrocks {

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / 1000 / 1000 << ", flush count=" << stat.flush_count << ")"
       << ", flush flush_size_bytes = " << stat.flush_size_bytes;
    return os;
}

// The type of parameter is safe to be a reference. Because the function object
// returned by std::bind() will increase the reference count of Memtable. i.e.,
// after the submit() method returns, even if the caller immediately releases the
// passed shared_ptr object, the Memtable object will not be destructed because
// its reference count is not 0.
OLAPStatus FlushToken::submit(const std::shared_ptr<MemTable>& memtable) {
    RETURN_NOT_OK(_flush_status.load());
    _flush_token->submit_func([this, memtable] { _flush_memtable(memtable); });
    return OLAP_SUCCESS;
}

Status FlushToken::submit(const std::shared_ptr<vectorized::MemTable>& memtable) {
    if (_flush_status.load() != OLAP_SUCCESS) {
        std::stringstream ss;
        ss << "tablet_id = " << memtable->tablet_id() << " flush_status error ";
        return Status::InternalError(ss.str());
    }
    _flush_token->submit_func([this, memtable] {
        _stats.cur_flush_count++;
        MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(memtable->mem_tracker());
        DeferOp op([&] {
            const_cast<std::shared_ptr<vectorized::MemTable>&>(memtable).reset();
            tls_thread_status.set_mem_tracker(prev_tracker);
            _stats.cur_flush_count--;
        });
        _flush_vectorized_memtable(memtable);
    });
    return Status::OK();
}

void FlushToken::cancel() {
    _flush_token->shutdown();
}

OLAPStatus FlushToken::wait() {
    _flush_token->wait();
    return _flush_status.load();
}

void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable) {
    SCOPED_CLEANUP({ memtable.reset(); });

    // If previous flush has failed, return directly
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    MonotonicStopWatch timer;
    timer.start();
    _flush_status.store(memtable->flush());
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_count++;
    _stats.flush_size_bytes += memtable->memory_usage();
}

void FlushToken::_flush_vectorized_memtable(std::shared_ptr<vectorized::MemTable> memtable) {
    SCOPED_CLEANUP({ memtable.reset(); });

    // If previous flush has failed, return directly
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    MonotonicStopWatch timer;
    timer.start();
    _flush_status.store(memtable->flush());
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_count++;
    _stats.flush_size_bytes += memtable->memory_usage();
}

Status MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int32_t data_dir_num = data_dirs.size();
    size_t min_threads = std::max(1, config::flush_thread_num_per_store);
    size_t max_threads = data_dir_num * min_threads;
    return ThreadPoolBuilder("MemTableFlushThreadPool")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool);
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
OLAPStatus MemTableFlushExecutor::create_flush_token(std::unique_ptr<FlushToken>* flush_token,
                                                     ThreadPool::ExecutionMode execution_mode) {
    *flush_token = std::make_unique<FlushToken>(_flush_pool->new_token(execution_mode));
    return OLAP_SUCCESS;
}

} // namespace starrocks
