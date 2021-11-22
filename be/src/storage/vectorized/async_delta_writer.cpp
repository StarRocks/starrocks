// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/async_delta_writer.h"

#include "runtime/current_thread.h"
#include "storage/vectorized/async_delta_writer_executor.h"
#include "util/uid_util.h"

namespace starrocks::vectorized {

AsyncDeltaWriter::~AsyncDeltaWriter() {
    (void)bthread::execution_queue_stop(_queue_id);
    (void)bthread::execution_queue_join(_queue_id);
}

int AsyncDeltaWriter::_execute(void* meta, bthread::TaskIterator<AsyncDeltaWriter::Task>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }
    auto writer = static_cast<DeltaWriter*>(meta);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(writer->mem_tracker());

    for (; iter; ++iter) {
        Status st;
        if (iter->chunk != nullptr && iter->indexes_size > 0) {
            st = writer->write(*iter->chunk, iter->indexes, 0, iter->indexes_size);
        }
        if (st.ok() && iter->commit_after_write) {
            st = writer->commit();
            if (st.ok()) {
                CommittedRowsetInfo info{.tablet = writer->tablet(),
                                         .rowset = writer->committed_rowset(),
                                         .rowset_writer = writer->committed_rowset_writer()};
                iter->write_cb->run(st, &info);
            } else {
                iter->write_cb->run(st, nullptr);
            }
        } else {
            iter->write_cb->run(st, nullptr);
        }
        // Do NOT touch |iter->commit_cb| since here, it may have been deleted.
    }
    return 0;
}

StatusOr<std::unique_ptr<AsyncDeltaWriter>> AsyncDeltaWriter::open(DeltaWriterOptions* req, MemTracker* mem_tracker) {
    auto res = DeltaWriter::open(req, mem_tracker);
    if (!res.ok()) {
        return res.status();
    }
    auto w = std::make_unique<AsyncDeltaWriter>(private_type(0), std::move(res).value());
    RETURN_IF_ERROR(w->_init());
    return std::move(w);
}

Status AsyncDeltaWriter::_init() {
    bthread::ExecutionQueueOptions opts;
    opts.executor = AsyncDeltaWriterExecutor::Instance();

    int r = bthread::execution_queue_start(&_queue_id, &opts, _execute, _writer.get());
    if (r) {
        return Status::InternalError("fail to create bthread execution queue");
    }
    return Status::OK();
}

void AsyncDeltaWriter::write(const AsyncDeltaWriterRequest& req, AsyncDeltaWriterCallback* cb) {
    DCHECK(cb != nullptr);
    Task task;
    task.chunk = req.chunk;
    task.indexes = req.indexes;
    task.indexes_size = req.indexes_size;
    task.write_cb = cb;
    task.commit_after_write = req.commit_after_write;
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task.write_cb->run(Status::InternalError("fail to call execution_queue_execute"), nullptr);
    }
}

void AsyncDeltaWriter::commit(AsyncDeltaWriterCallback* cb) {
    DCHECK(cb != nullptr);
    Task task;
    task.chunk = nullptr;
    task.indexes = nullptr;
    task.indexes_size = 0;
    task.write_cb = cb;
    task.commit_after_write = true;
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task.write_cb->run(Status::InternalError("fail to call execution_queue_execute"), nullptr);
    }
}

void AsyncDeltaWriter::abort() {
    bthread::execution_queue_stop(_queue_id);
    _writer->abort();
}

} // namespace starrocks::vectorized