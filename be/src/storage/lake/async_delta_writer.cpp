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

#include "storage/lake/async_delta_writer.h"

#include <bthread/execution_queue.h>
#include <fmt/format.h>

#include <memory>
#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "runtime/current_thread.h"
#include "storage/lake/delta_writer.h"
#include "storage/storage_engine.h"
#include "testutil/sync_point.h"
#include "util/stack_trace_mutex.h"

namespace starrocks::lake {

class AsyncDeltaWriterImpl {
    using Chunk = starrocks::Chunk;

public:
    using Callback = AsyncDeltaWriter::Callback;

    // Undocumented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;

    AsyncDeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                         const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker)
            : _writer(DeltaWriter::create(tablet_manager, tablet_id, txn_id, partition_id, slots, mem_tracker)),
              _queue_id{kInvalidQueueId},
              _mtx(),
              _status(),
              _opened(false),
              _closed(false) {}

    AsyncDeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                         const std::vector<SlotDescriptor*>* slots, const std::string& merge_condition,
                         MemTracker* mem_tracker)
            : _writer(DeltaWriter::create(tablet_manager, tablet_id, txn_id, partition_id, slots, merge_condition,
                                          mem_tracker)),
              _queue_id{kInvalidQueueId},
              _mtx(),
              _status(),
              _opened(false),
              _closed(false) {}

    ~AsyncDeltaWriterImpl();

    DISALLOW_COPY_AND_MOVE(AsyncDeltaWriterImpl);

    [[nodiscard]] Status open();

    void write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size, Callback cb);

    void finish(Callback cb);

    void close();

    [[nodiscard]] int64_t tablet_id() const { return _writer->tablet_id(); }

    [[nodiscard]] int64_t partition_id() const { return _writer->partition_id(); }

    [[nodiscard]] int64_t txn_id() const { return _writer->txn_id(); }

private:
    struct Task {
        Callback cb;
        // If chunk == nullptr, this is a finish task
        const Chunk* chunk = nullptr;
        const uint32_t* indexes = nullptr;
        uint32_t indexes_size = 0;
        bool finish_after_write = false;
    };

    static int execute(void* meta, bthread::TaskIterator<AsyncDeltaWriterImpl::Task>& iter);

    Status do_open();
    bool closed();

    DeltaWriter::Ptr _writer;
    bthread::ExecutionQueueId<Task> _queue_id;
    StackTraceMutex<bthread::Mutex> _mtx;
    // _status、_opened and _closed are protected by _mtx
    Status _status;
    bool _opened;
    bool _closed;
};

AsyncDeltaWriterImpl::~AsyncDeltaWriterImpl() {
    close();
}

inline bool AsyncDeltaWriterImpl::closed() {
    std::lock_guard l(_mtx);
    return _closed;
}

inline int AsyncDeltaWriterImpl::execute(void* meta, bthread::TaskIterator<AsyncDeltaWriterImpl::Task>& iter) {
    TEST_SYNC_POINT("AsyncDeltaWriterImpl::execute:1");
    auto async_writer = static_cast<AsyncDeltaWriterImpl*>(meta);
    auto delta_writer = async_writer->_writer.get();
    if (iter.is_queue_stopped()) {
        delta_writer->close();
        return 0;
    }
    auto st = Status{};
    for (; iter; ++iter) {
        // It's safe to run without checking `closed()` but doing so can make the task quit earlier on cancel/error.
        if (async_writer->closed()) {
            iter->cb(Status::InternalError("AsyncDeltaWriter has been closed"));
            continue;
        }
        if (st.ok() && iter->chunk != nullptr && iter->indexes_size > 0) {
            st = delta_writer->write(*iter->chunk, iter->indexes, iter->indexes_size);
            LOG_IF(ERROR, !st.ok()) << "Fail to write. tablet_id: " << delta_writer->tablet_id()
                                    << " txn_id: " << delta_writer->txn_id() << ": " << st;
        }
        if (st.ok() && iter->finish_after_write) {
            st = delta_writer->finish();
            LOG_IF(ERROR, !st.ok()) << "Fail to finish write. tablet_id: " << delta_writer->tablet_id()
                                    << " txn_id: " << delta_writer->txn_id() << ": " << st;
        }
        iter->cb(st);
    }
    return 0;
}

inline Status AsyncDeltaWriterImpl::open() {
    std::lock_guard l(_mtx);
    if (_closed) {
        return Status::InternalError("AsyncDeltaWriter has been closed");
    }
    if (_opened) {
        return _status;
    }
    _status = do_open();
    _opened = true;
    return _status;
}

inline Status AsyncDeltaWriterImpl::do_open() {
    if (UNLIKELY(StorageEngine::instance() == nullptr)) {
        return Status::InternalError("StorageEngine::instance() is NULL");
    }
    bthread::ExecutionQueueOptions opts;
    opts.executor = StorageEngine::instance()->async_delta_writer_executor();
    if (UNLIKELY(opts.executor == nullptr)) {
        return Status::InternalError("AsyncDeltaWriterExecutor init failed");
    }
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, execute, this); r != 0) {
        _queue_id.value = kInvalidQueueId;
        return Status::InternalError(fmt::format("fail to create bthread execution queue: {}", r));
    }
    return _writer->open();
}

inline void AsyncDeltaWriterImpl::write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size,
                                        Callback cb) {
    Task task;
    task.chunk = chunk;
    task.indexes = indexes;
    task.indexes_size = indexes_size;
    task.cb = std::move(cb); // Do NOT touch |cb| since here
    task.finish_after_write = false;
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        task.cb(Status::InternalError("AsyncDeltaWriterImpl not open()ed or has been close()ed"));
    }
}

inline void AsyncDeltaWriterImpl::finish(Callback cb) {
    Task task;
    task.chunk = nullptr;
    task.indexes = nullptr;
    task.indexes_size = 0;
    task.finish_after_write = true;
    task.cb = std::move(cb); // Do NOT touch |cb| since here
    // NOTE: the submited tasks will be executed in the thread pool `StorageEngine::instance()->async_delta_writer_executor()`,
    // which is a thread pool of pthraed NOT bthread, so don't worry the bthread worker threads or RPC threads will be blocked
    // by the submitted tasks.
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task.cb(Status::InternalError("AsyncDeltaWriterImpl not open()ed or has been close()ed"));
    }
}

inline void AsyncDeltaWriterImpl::close() {
    std::unique_lock l(_mtx);
    TEST_SYNC_POINT("AsyncDeltaWriterImpl::close:1");
    _closed = true;
    if (_queue_id.value != kInvalidQueueId) {
        auto old_id = _queue_id;
        _queue_id.value = kInvalidQueueId;

        // Must unlock mutex first before joining the executino queue, otherwise deadlock may occur:
        //           Current Thread                  Execution Queue Thread
        //
        //   AsyncDeltaWriterImpl::close()     |
        //   \__  _mtx.lock (acquired)         |
        //                                     |  AsyncDeltaWriter::execute()
        //                                     |  \__ AsyncDeltaWriter::closed()
        //                                     |      \__ _mtx.lock (blocked)
        //                                     |
        //   execution_queue_join() (blocked)  |
        //
        l.unlock();

        // After the execution_queue been `stop()`ed all incoming `write()` and `finish()` requests
        // will fail immediately.
        int r = bthread::execution_queue_stop(old_id);
        PLOG_IF(WARNING, r != 0) << "Fail to stop execution queue";

        // Wait for all running tasks completed.
        r = bthread::execution_queue_join(old_id);
        PLOG_IF(WARNING, r != 0) << "Fail to join execution queue";
    }
}

AsyncDeltaWriter::~AsyncDeltaWriter() {
    delete _impl;
}

Status AsyncDeltaWriter::open() {
    return _impl->open();
}

void AsyncDeltaWriter::write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size, Callback cb) {
    _impl->write(chunk, indexes, indexes_size, std::move(cb));
}

void AsyncDeltaWriter::finish(Callback cb) {
    _impl->finish(std::move(cb));
}

void AsyncDeltaWriter::close() {
    _impl->close();
}

int64_t AsyncDeltaWriter::tablet_id() const {
    return _impl->tablet_id();
}

int64_t AsyncDeltaWriter::partition_id() const {
    return _impl->partition_id();
}

int64_t AsyncDeltaWriter::txn_id() const {
    return _impl->txn_id();
}

std::unique_ptr<AsyncDeltaWriter> AsyncDeltaWriter::create(TabletManager* tablet_manager, int64_t tablet_id,
                                                           int64_t txn_id, int64_t partition_id,
                                                           const std::vector<SlotDescriptor*>* slots,
                                                           MemTracker* mem_tracker) {
    auto impl = new AsyncDeltaWriterImpl(tablet_manager, tablet_id, txn_id, partition_id, slots, mem_tracker);
    return std::make_unique<AsyncDeltaWriter>(impl);
}

std::unique_ptr<AsyncDeltaWriter> AsyncDeltaWriter::create(TabletManager* tablet_manager, int64_t tablet_id,
                                                           int64_t txn_id, int64_t partition_id,
                                                           const std::vector<SlotDescriptor*>* slots,
                                                           const std::string& merge_condition,
                                                           MemTracker* mem_tracker) {
    auto impl = new AsyncDeltaWriterImpl(tablet_manager, tablet_id, txn_id, partition_id, slots, merge_condition,
                                         mem_tracker);
    return std::make_unique<AsyncDeltaWriter>(impl);
}

} // namespace starrocks::lake
