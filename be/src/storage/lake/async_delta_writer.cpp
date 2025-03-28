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
#include <vector>

#include "common/compiler_util.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/load_spill_block_manager.h"
#include "storage/storage_engine.h"
#include "testutil/sync_point.h"
#include "util/stack_trace_mutex.h"

namespace starrocks::lake {

class AsyncDeltaWriterImpl {
    friend class MergeBlockTask;
    using Chunk = starrocks::Chunk;

public:
    using Callback = AsyncDeltaWriter::Callback;
    using FinishCallback = AsyncDeltaWriter::FinishCallback;

    // Undocumented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;

    AsyncDeltaWriterImpl(std::unique_ptr<DeltaWriter> writer) : _writer(std::move(writer)) {
        CHECK(_writer != nullptr) << "delta writer is null";
    }

    ~AsyncDeltaWriterImpl();

    DISALLOW_COPY_AND_MOVE(AsyncDeltaWriterImpl);

    Status open();

    void write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size, Callback cb);

    void flush(Callback cb);

    void finish(DeltaWriterFinishMode mode, FinishCallback cb);

    void close();

    [[nodiscard]] int64_t queueing_memtable_num() const { return _writer->queueing_memtable_num(); }

    [[nodiscard]] int64_t tablet_id() const { return _writer->tablet_id(); }

    [[nodiscard]] int64_t partition_id() const { return _writer->partition_id(); }

    [[nodiscard]] int64_t txn_id() const { return _writer->txn_id(); }

    [[nodiscard]] bool is_immutable() const { return _writer->is_immutable(); }

    Status check_immutable() { return _writer->check_immutable(); }

    [[nodiscard]] int64_t last_write_ts() const { return _writer->last_write_ts(); }

    DeltaWriter* delta_writer() { return _writer.get(); }

private:
    enum TaskType {
        kWriteTask = 0,
        kFlushTask = 1,
        kFinishTask = 2,
    };

    struct Task {
        explicit Task(TaskType t) : type(t), create_time_ns(MonotonicNanos()) {}
        virtual ~Task() = default;

        TaskType type;
        int64_t create_time_ns;
    };

    struct WriteTask : public Task {
        WriteTask() : Task(kWriteTask) {}
        ~WriteTask() override = default;

        Callback cb;
        const Chunk* chunk = nullptr;
        const uint32_t* indexes = nullptr;
        uint32_t indexes_size = 0;
    };

    struct FlushTask : public Task {
        FlushTask() : Task(kFlushTask) {}
        ~FlushTask() override = default;

        Callback cb;
    };

    struct FinishTask : public Task {
        FinishTask() : Task(kFinishTask) {}
        ~FinishTask() override = default;

        FinishCallback cb;
        DeltaWriterFinishMode finish_mode = DeltaWriterFinishMode::kWriteTxnLog;
    };

    using TaskPtr = std::shared_ptr<Task>;

    static int execute(void* meta, bthread::TaskIterator<AsyncDeltaWriterImpl::TaskPtr>& iter);

    Status do_open();
    bool closed();

    std::unique_ptr<DeltaWriter> _writer{};
    bthread::ExecutionQueueId<TaskPtr> _queue_id{kInvalidQueueId};
    std::unique_ptr<ThreadPoolToken> _block_merge_token{};
    StackTraceMutex<bthread::Mutex> _mtx{};
    // _status、_opened and _closed are protected by _mtx
    Status _status{};
    bool _opened{false};
    bool _closed{false};
};

AsyncDeltaWriterImpl::~AsyncDeltaWriterImpl() {
    close();
}

inline bool AsyncDeltaWriterImpl::closed() {
    std::lock_guard l(_mtx);
    return _closed;
}

class MergeBlockTask : public Runnable {
public:
    MergeBlockTask(std::shared_ptr<AsyncDeltaWriterImpl::FinishTask> finish_task, AsyncDeltaWriterImpl* async_writer)
            : _finish_task(std::move(finish_task)), _async_writer(async_writer) {}

    void run() override {
        auto delta_writer = _async_writer->_writer.get();
        if (_async_writer->closed()) {
            _finish_task->cb(Status::InternalError("AsyncDeltaWriter has been closed"));
            return;
        }
        auto res = delta_writer->finish_with_txnlog(_finish_task->finish_mode);
        LOG_IF(ERROR, !res.ok()) << "Fail to finish write. tablet_id: " << delta_writer->tablet_id()
                                 << " txn_id: " << delta_writer->txn_id() << ": " << res.status();
        _finish_task->cb(std::move(res));
    }

private:
    std::shared_ptr<AsyncDeltaWriterImpl::FinishTask> _finish_task;
    AsyncDeltaWriterImpl* _async_writer;
};

inline int AsyncDeltaWriterImpl::execute(void* meta, bthread::TaskIterator<AsyncDeltaWriterImpl::TaskPtr>& iter) {
    TEST_SYNC_POINT("AsyncDeltaWriterImpl::execute:1");
    auto async_writer = static_cast<AsyncDeltaWriterImpl*>(meta);
    auto delta_writer = async_writer->_writer.get();
    if (iter.is_queue_stopped()) {
        delta_writer->close();
        return 0;
    }
    int num_tasks = 0;
    auto st = Status{};
    int64_t pending_time_ns = 0;
    for (; iter; ++iter) {
        // It's safe to run without checking `closed()` but doing so can make the task quit earlier on cancel/error.
        if (async_writer->closed()) {
            st.update(Status::InternalError("AsyncDeltaWriter has been closed"));
        }
        auto task_ptr = *iter;
        num_tasks += 1;
        pending_time_ns += MonotonicNanos() - task_ptr->create_time_ns;
        switch (task_ptr->type) {
        case kWriteTask: {
            auto write_task = std::static_pointer_cast<WriteTask>(task_ptr);
            if (st.ok()) {
                st.update(delta_writer->write(*(write_task->chunk), write_task->indexes, write_task->indexes_size));
                LOG_IF(ERROR, !st.ok()) << "Fail to write. tablet_id: " << delta_writer->tablet_id()
                                        << " txn_id: " << delta_writer->txn_id() << ": " << st;
            }
            write_task->cb(st);
            break;
        }
        case kFlushTask: {
            auto flush_task = std::static_pointer_cast<FlushTask>(task_ptr);
            if (st.ok()) {
                st.update(delta_writer->manual_flush());
            }
            flush_task->cb(st);
            break;
        }
        case kFinishTask: {
            auto finish_task = std::static_pointer_cast<FinishTask>(task_ptr);
            if (!st.ok()) {
                finish_task->cb(st);
            } else if (delta_writer->has_spill_block()) {
                // If there are spill blocks, we merge them using another thread pool
                auto merge_task = std::make_shared<MergeBlockTask>(finish_task, async_writer);
                auto res = async_writer->_block_merge_token->submit(merge_task);
                if (!res.ok()) {
                    st.update(res);
                    LOG_IF(ERROR, !st.ok()) << "Fail to submit merge task: " << st;
                    finish_task->cb(st);
                }
            } else {
                auto res = delta_writer->finish_with_txnlog(finish_task->finish_mode);
                st.update(res.status());
                LOG_IF(ERROR, !st.ok()) << "Fail to finish write. tablet_id: " << delta_writer->tablet_id()
                                        << " txn_id: " << delta_writer->txn_id() << ": " << st;
                finish_task->cb(std::move(res));
            }
            break;
        }
        }
    }
    async_writer->_writer->update_task_stat(num_tasks, pending_time_ns);
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
    if (_block_merge_token == nullptr) {
        _block_merge_token = StorageEngine::instance()->load_spill_block_merge_executor()->create_token();
    }
    return _writer->open();
}

inline void AsyncDeltaWriterImpl::write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size,
                                        Callback cb) {
    auto task = std::make_shared<WriteTask>();
    task->chunk = chunk;
    task->indexes = indexes;
    task->indexes_size = indexes_size;
    task->cb = std::move(cb); // Do NOT touch |cb| since here
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        task->cb(Status::InternalError("AsyncDeltaWriterImpl not opened or has been closed"));
    }
}

inline void AsyncDeltaWriterImpl::flush(Callback cb) {
    auto task = std::make_shared<FlushTask>();
    task->cb = std::move(cb); // Do NOT touch |cb| since here
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task->cb(Status::InternalError("AsyncDeltaWriterImpl not opened or has been closed"));
    }
}

inline void AsyncDeltaWriterImpl::finish(DeltaWriterFinishMode mode, FinishCallback cb) {
    auto task = std::make_shared<FinishTask>();
    task->cb = std::move(cb); // Do NOT touch |cb| since here
    task->finish_mode = mode;
    // NOTE: the submited tasks will be executed in the thread pool `StorageEngine::instance()->async_delta_writer_executor()`,
    // which is a thread pool of pthraed NOT bthread, so don't worry the bthread worker threads or RPC threads will be blocked
    // by the submitted tasks.
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task->cb(Status::InternalError("AsyncDeltaWriterImpl not opened or has been closed"));
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

        TEST_SYNC_POINT("AsyncDeltaWriterImpl::close:2");

        // Wait for block merge finished.
        if (_block_merge_token != nullptr) {
            _block_merge_token->shutdown();
            _block_merge_token.reset();
        }

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

void AsyncDeltaWriter::flush(Callback cb) {
    _impl->flush(std::move(cb));
}

void AsyncDeltaWriter::finish(DeltaWriterFinishMode mode, FinishCallback cb) {
    TEST_SYNC_POINT_CALLBACK("AsyncDeltaWriter:enter_finish", this);
    _impl->finish(mode, std::move(cb));
}

void AsyncDeltaWriter::close() {
    _impl->close();
}

int64_t AsyncDeltaWriter::queueing_memtable_num() const {
    return _impl->queueing_memtable_num();
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

bool AsyncDeltaWriter::is_immutable() const {
    return _impl->is_immutable();
}

Status AsyncDeltaWriter::check_immutable() {
    return _impl->check_immutable();
}

int64_t AsyncDeltaWriter::last_write_ts() const {
    return _impl->last_write_ts();
}

DeltaWriter* AsyncDeltaWriter::delta_writer() {
    return _impl->delta_writer();
}

StatusOr<AsyncDeltaWriterBuilder::AsyncDeltaWriterPtr> AsyncDeltaWriterBuilder::build() {
    ASSIGN_OR_RETURN(auto writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_mgr)
                                          .set_txn_id(_txn_id)
                                          .set_tablet_id(_tablet_id)
                                          .set_table_id(_table_id)
                                          .set_partition_id(_partition_id)
                                          .set_slot_descriptors(_slots)
                                          .set_merge_condition(_merge_condition)
                                          .set_mem_tracker(_mem_tracker)
                                          .set_immutable_tablet_size(_immutable_tablet_size)
                                          .set_miss_auto_increment_column(_miss_auto_increment_column)
                                          .set_schema_id(_schema_id)
                                          .set_partial_update_mode(_partial_update_mode)
                                          .set_column_to_expr_value(_column_to_expr_value)
                                          .set_load_id(_load_id)
                                          .set_profile(_profile)
                                          .build());
    auto impl = new AsyncDeltaWriterImpl(std::move(writer));
    return std::make_unique<AsyncDeltaWriter>(impl);
}

} // namespace starrocks::lake
