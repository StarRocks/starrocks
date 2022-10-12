// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/async_delta_writer.h"

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP
#include <fmt/format.h>

#include <memory>
#include <string>
#include <vector>

#include "runtime/current_thread.h"
#include "storage/lake/delta_writer.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

class AsyncDeltaWriterImpl {
    using Chunk = starrocks::vectorized::Chunk;

public:
    using Callback = AsyncDeltaWriter::Callback;

    // Undocemented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;

    AsyncDeltaWriterImpl(int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                         const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker)
            : _writer(DeltaWriter::create(tablet_id, txn_id, partition_id, slots, mem_tracker)),
              _queue_id{kInvalidQueueId},
              _open_mtx(),
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

    DeltaWriter::Ptr _writer;
    bthread::ExecutionQueueId<Task> _queue_id;
    bthread::Mutex _open_mtx;
    Status _status;
    std::atomic<bool> _opened;
    std::atomic<bool> _closed;
};

AsyncDeltaWriterImpl::~AsyncDeltaWriterImpl() {
    close();
}

inline int AsyncDeltaWriterImpl::execute(void* meta, bthread::TaskIterator<AsyncDeltaWriterImpl::Task>& iter) {
    auto async_writer = static_cast<AsyncDeltaWriterImpl*>(meta);
    auto delta_writer = async_writer->_writer.get();
    if (iter.is_queue_stopped()) {
        delta_writer->close();
        return 0;
    }
    auto st = Status{};
    for (; iter; ++iter) {
        // It's safe to run without checking `_closed` but doing so can make the task quit earlier on cancel/error.
        if (async_writer->_closed.load(std::memory_order_acquire)) {
            iter->cb(Status::InternalError("AsyncDeltaWriter has been closed"));
            continue;
        }
        if (st.ok() && iter->chunk != nullptr && iter->indexes_size > 0) {
            st = delta_writer->write(*iter->chunk, iter->indexes, iter->indexes_size);
            LOG_IF(ERROR, !st.ok()) << "Fail to write. tablet_id=" << delta_writer->tablet_id()
                                    << " txn_id=" << delta_writer->txn_id() << ": " << st;
        }
        if (st.ok() && iter->finish_after_write) {
            st = delta_writer->finish();
            LOG_IF(ERROR, !st.ok()) << "Fail to finish write. tablet_id=" << delta_writer->tablet_id()
                                    << " txn_id=" << delta_writer->txn_id() << ": " << st;
        }
        iter->cb(st);
    }
    return 0;
}

inline Status AsyncDeltaWriterImpl::open() {
    if (_opened.load(std::memory_order_acquire)) {
        return _status;
    }
    std::lock_guard l(_open_mtx);
    if (_opened.load(std::memory_order_acquire)) {
        return _status;
    }
    _status = do_open();
    _opened.store(true, std::memory_order_release);
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
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task.cb(Status::InternalError("AsyncDeltaWriterImpl not open()ed or has been close()ed"));
    }
}

inline void AsyncDeltaWriterImpl::close() {
    bool expect = _closed.load(std::memory_order_acquire);
    if (expect || !_opened.load(std::memory_order_acquire)) return;
    if (_closed.compare_exchange_strong(expect, true, std::memory_order_acq_rel)) {
        // After the execution_queue been `stop()`ed all incoming `write()` and `finish()` requests
        // will fail immediately.
        int r = bthread::execution_queue_stop(_queue_id);
        PLOG_IF(WARNING, r != 0) << "Fail to stop execution queue";

        // Wait for all running tasks completed.
        r = bthread::execution_queue_join(_queue_id);
        PLOG_IF(WARNING, r != 0) << "Fail to join execution queue";

        // Destroy TabletWriter. Since the execution_queue has been stopped and all
        // running tasks have finished, no further execution will call `_writer` anymore, it's
        // safe to destroy it.
        _writer.reset();
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

std::unique_ptr<AsyncDeltaWriter> AsyncDeltaWriter::create(int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                                                           const std::vector<SlotDescriptor*>* slots,
                                                           MemTracker* mem_tracker) {
    auto impl = new AsyncDeltaWriterImpl(tablet_id, txn_id, partition_id, slots, mem_tracker);
    return std::make_unique<AsyncDeltaWriter>(impl);
}

} // namespace starrocks::lake
