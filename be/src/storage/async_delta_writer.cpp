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

#include "storage/async_delta_writer.h"

#include <fmt/format.h>

#include "runtime/current_thread.h"
#include "storage/segment_flush_executor.h"
#include "storage/storage_engine.h"

namespace starrocks {

AsyncDeltaWriter::~AsyncDeltaWriter() {
    _close();
    _writer.reset();
}

int AsyncDeltaWriter::_execute(void* meta, bthread::TaskIterator<AsyncDeltaWriter::Task>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }
    auto writer = static_cast<DeltaWriter*>(meta);
    bool flush_after_write = false;
    for (; iter; ++iter) {
        Status st;
        if (iter->abort) {
            writer->abort(iter->abort_with_log);
            continue;
        }
        if (iter->chunk != nullptr && iter->indexes_size > 0) {
            st = writer->write(*iter->chunk, iter->indexes, 0, iter->indexes_size);
        }

        if (iter->flush_after_write) {
            flush_after_write = true;
            continue;
        }
        FailedRowsetInfo failed_info{.tablet_id = writer->tablet()->tablet_id(),
                                     .replicate_token = writer->replicate_token()};
        if (st.ok() && iter->commit_after_write) {
            if (st = writer->close(); !st.ok()) {
                LOG(WARNING) << "Fail to write or commit. txn_id: " << writer->txn_id()
                             << " tablet_id: " << writer->tablet()->tablet_id() << ": " << st;
                iter->write_cb->run(st, nullptr, &failed_info);
                continue;
            }
            if (st = writer->commit(); !st.ok()) {
                LOG(WARNING) << "Fail to write or commit. txn_id: " << writer->txn_id()
                             << " tablet_id: " << writer->tablet()->tablet_id() << ": " << st;
                iter->write_cb->run(st, nullptr, &failed_info);
                continue;
            }
            CommittedRowsetInfo info{.tablet = writer->tablet(),
                                     .rowset = writer->committed_rowset(),
                                     .rowset_writer = writer->committed_rowset_writer(),
                                     .replicate_token = writer->replicate_token()};
            iter->write_cb->run(st, &info, nullptr);
        } else if (st.ok()) {
            iter->write_cb->run(st, nullptr, nullptr);
        } else {
            iter->write_cb->run(st, nullptr, &failed_info);
        }
        // Do NOT touch |iter->commit_cb| since here, it may have been deleted.
        LOG_IF(ERROR, !st.ok()) << "Fail to write or commit. txn_id: " << writer->txn_id()
                                << " tablet_id: " << writer->tablet()->tablet_id() << ": " << st;
    }
    if (flush_after_write) {
        auto st = writer->flush_memtable_async(false);
        LOG_IF(WARNING, !st.ok()) << "Fail to flush. txn_id: " << writer->txn_id()
                                  << " tablet_id: " << writer->tablet()->tablet_id() << ": " << st;
    }
    return 0;
}

StatusOr<std::unique_ptr<AsyncDeltaWriter>> AsyncDeltaWriter::open(const DeltaWriterOptions& opt,
                                                                   MemTracker* mem_tracker) {
    auto res = DeltaWriter::open(opt, mem_tracker);
    if (!res.ok()) {
        return res.status();
    }
    auto w = std::make_unique<AsyncDeltaWriter>(private_type(0), std::move(res).value());
    RETURN_IF_ERROR(w->_init());
    return std::move(w);
}

Status AsyncDeltaWriter::_init() {
    if (UNLIKELY(StorageEngine::instance() == nullptr)) {
        return Status::InternalError("StorageEngine::instance() is NULL");
    }
    bthread::ExecutionQueueOptions opts;
    opts.executor = StorageEngine::instance()->async_delta_writer_executor();
    if (UNLIKELY(opts.executor == nullptr)) {
        return Status::InternalError("AsyncDeltaWriterExecutor init failed");
    }
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, _execute, _writer.get()); r != 0) {
        return Status::InternalError(fmt::format("fail to create bthread execution queue: {}", r));
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
        FailedRowsetInfo failed_info{.tablet_id = _writer->tablet()->tablet_id(), .replicate_token = nullptr};
        task.write_cb->run(Status::InternalError("fail to call execution_queue_execute"), nullptr, &failed_info);
    }
}

void AsyncDeltaWriter::flush() {
    Task task;
    task.chunk = nullptr;
    task.indexes = nullptr;
    task.indexes_size = 0;
    task.flush_after_write = true;
    if (int r = bthread::execution_queue_execute(_queue_id, task); r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute tablet_id: " << _writer->tablet()->tablet_id()
                     << " ret: " << r;
    }
}

void AsyncDeltaWriter::write_segment(const AsyncDeltaWriterSegmentRequest& req) {
    auto st = _writer->segment_flush_token()->submit(_writer.get(), req.cntl, req.request, req.response, req.done);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to submit write segment, err=" << st;
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
        FailedRowsetInfo failed_info{.tablet_id = _writer->tablet()->tablet_id(), .replicate_token = nullptr};
        task.write_cb->run(Status::InternalError("fail to call execution_queue_execute"), nullptr, &failed_info);
    }
}

void AsyncDeltaWriter::cancel(const Status& st) {
    _writer->cancel(st);
}

void AsyncDeltaWriter::abort(bool with_log) {
    Task task;
    task.abort = true;
    task.abort_with_log = with_log;

    bthread::TaskOptions options;
    int r = bthread::execution_queue_execute(_queue_id, task, &options);
    LOG_IF(WARNING, r != 0) << "Fail to execution_queue_execute: " << r;

    // Wait until all background tasks finished
    // https://github.com/StarRocks/starrocks/issues/8906
    _close();
}

void AsyncDeltaWriter::_close() {
    bool value = _closed.load(std::memory_order_acquire);
    if (value) {
        return;
    }
    if (_closed.compare_exchange_strong(value, true, std::memory_order_acq_rel) && _queue_id.value != kInvalidQueueId) {
        int r = bthread::execution_queue_stop(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to stop execution queue: " << r;
        r = bthread::execution_queue_join(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to join execution queue: " << r;
    }
}

} // namespace starrocks
