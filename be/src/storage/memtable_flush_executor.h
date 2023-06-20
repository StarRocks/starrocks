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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memtable_flush_executor.h

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

#pragma once

#include <bthread/execution_queue.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/memtable.h"
#include "storage/olap_define.h"
#include "util/bthreads/executor.h"
#include "util/spinlock.h"
#include "util/threadpool.h"

namespace starrocks {

class DataDir;
class ExecEnv;
class SegmentPB;
class MemTable;

// the statistic of a certain flush handler.
// use atomic because it may be updated by multi threads
struct FlushStatistic {
    int64_t flush_time_ns = 0;
    int64_t flush_count = 0;
    int64_t flush_size_bytes = 0;
    int64_t cur_flush_count = 0;
    std::atomic<int64_t> queueing_memtable_num = 0;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat);

// A thin wrapper of ThreadPoolToken to submit task.
// For a tablet, there may be multiple memtables, which will be flushed to disk
// one by one in the order of generation.
// If a memtable flush fails, then:
// 1. Immediately disallow submission of any subsequent memtable
// 2. For the memtables that have already been submitted, there is no need to flush,
//    because the entire job will definitely fail;
class FlushToken {
public:
    explicit FlushToken(std::unique_ptr<workgroup::TaskToken> flush_pool_token)
            : _flush_token(std::move(flush_pool_token)), _status() {}

    Status submit(std::unique_ptr<MemTable> mem_table, bool eos = false,
                  std::function<void(std::unique_ptr<SegmentPB>, bool)> cb = nullptr);

    // error has happpens, so we cancel this token
    // And remove all tasks in the queue.
    void shutdown();

    void cancel(const Status& st);

    // wait all tasks in token to be completed.
    Status wait();

    // get flush operations' statistics
    const FlushStatistic& get_stats() const { return _stats; }

    Status status() const {
        std::lock_guard l(_status_lock);
        return _status;
    }

    void set_status(const Status& status) {
        if (status.ok()) return;
        std::lock_guard l(_status_lock);
        if (_status.ok()) _status = status;
    }

private:
    friend class MemtableFlushTask;

    void _flush_memtable(MemTable* memtable, SegmentPB* segment);

    std::unique_ptr<workgroup::TaskToken> _flush_token;

    mutable SpinLock _status_lock;
    // Records the current flush status of the tablet.
    // Note: Once its value is set to Failed, it cannot return to OK.
    Status _status;

    FlushStatistic _stats;
};

// MemTableFlushExecutor is responsible for flushing memtables to disk.
// It encapsulate a ThreadPool to handle all tasks.
// Usage Example:
//      ...
//      std::shared_ptr<FlushHandler> flush_handler;
//      memTableFlushExecutor.create_flush_token(path_hash, &flush_handler);
//      ...
//      flush_token->submit(memtable)
//      ...
class MemTableFlushExecutor {
public:
    MemTableFlushExecutor() = default;
    ~MemTableFlushExecutor() = default;

    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    Status init(const std::vector<DataDir*>& data_dirs);

    // dynamic update max threads num
    Status update_max_threads(int max_threads);

    // NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
    std::unique_ptr<FlushToken> create_flush_token(
            ThreadPool::ExecutionMode execution_mode = ThreadPool::ExecutionMode::SERIAL);

    ThreadPool* get_thread_pool() { return _flush_pool.get(); }
    bthread::Executor* get_executor() { return _executor.get(); }

private:
    std::unique_ptr<ThreadPool> _flush_pool;
    std::unique_ptr<bthreads::ThreadPoolExecutor> _executor;
};

} // namespace starrocks
