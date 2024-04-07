// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "storage/olap_define.h"
#include "util/spinlock.h"
#include "util/threadpool.h"

namespace brpc {
class Controller;
}

namespace google::protobuf {
class Closure;
}

namespace starrocks {

class DataDir;
class ExecEnv;
class PTabletWriterAddSegmentRequest;
class PTabletWriterAddSegmentResult;
class ThreadPoolToken;

namespace vectorized {
class DeltaWriter;
}

class SegmentFlushToken {
public:
    SegmentFlushToken(std::unique_ptr<ThreadPoolToken> flush_pool_token);

    Status submit(vectorized::DeltaWriter* writer, brpc::Controller* cntl,
                  const PTabletWriterAddSegmentRequest* request, PTabletWriterAddSegmentResult* response,
                  google::protobuf::Closure* done);

    Status status() const {
        std::lock_guard l(_status_lock);
        return _status;
    }

    void set_status(const Status& status) {
        if (status.ok()) return;
        std::lock_guard l(_status_lock);
        if (_status.ok()) _status = status;
    }

    void cancel(const Status& st);

    void shutdown();

    Status wait();

private:
    std::unique_ptr<ThreadPoolToken> _flush_token;

    mutable SpinLock _status_lock;
    // Records the current flush status of the tablet.
    Status _status;
};

class SegmentFlushExecutor {
public:
    SegmentFlushExecutor() = default;
    ~SegmentFlushExecutor() = default;

    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    Status init(const std::vector<DataDir*>& data_dirs);

    Status update_max_threads(int max_threads);

    std::unique_ptr<SegmentFlushToken> create_flush_token(
            ThreadPool::ExecutionMode execution_mode = ThreadPool::ExecutionMode::CONCURRENT);

    ThreadPool* get_thread_pool() { return _flush_pool.get(); }

private:
    std::unique_ptr<ThreadPool> _flush_pool;
};

} // namespace starrocks
