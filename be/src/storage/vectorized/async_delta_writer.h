// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <bthread/execution_queue.h>
#include <google/protobuf/service.h>

#include "storage/vectorized/delta_writer.h"

namespace starrocks::vectorized {

class AsyncDeltaWriterRequest;
class CommittedRowsetInfo;
class AsyncDeltaWriterCallback;

class AsyncDeltaWriter {
    struct private_type;

public:
    static StatusOr<std::unique_ptr<AsyncDeltaWriter>> open(DeltaWriterOptions* req, MemTracker* mem_tracker);

    AsyncDeltaWriter(private_type, std::unique_ptr<DeltaWriter> writer) : _writer(std::move(writer)) {}

    ~AsyncDeltaWriter();

    // Disable copy c'tor and copy assignment
    AsyncDeltaWriter(const AsyncDeltaWriter&) = delete;
    void operator=(const AsyncDeltaWriter&) = delete;
    // Disable move c'tor and move assignment because no usage for now.
    AsyncDeltaWriter(AsyncDeltaWriter&&) = delete;
    void operator=(AsyncDeltaWriter&&) = delete;

    // REQUIRE:
    //  - |cb| cannot be NULL
    //  - if |req.chunk| is not NULL, the chunk it points to must keep alive until |cb->run()| been called
    //  - if |req.chunk| is not NULL, |req.indexes| must not be NULL and, it must be kept alive until
    //    |cb->run()| been called.
    //
    // NOTE: cb->run() will be called in the current thread if this delta writer has been `abort()`ed.
    //
    // [thread-safe]
    void write(const AsyncDeltaWriterRequest& req, AsyncDeltaWriterCallback* cb);

    // This method is equivalent to calling `write` with a AsyncDeltaWriterRequest that contains a NULL chunk
    // and commit_after_write is true.
    // [thread-safe]
    void commit(AsyncDeltaWriterCallback* cb);

    // [thread-safe]
    void abort();

    int64_t partition_id() const { return _writer->partition_id(); }

private:
    struct private_type {
        explicit private_type(int) {}
    };

    struct Task {
        // If chunk == nullptr, this is a commit task
        vectorized::Chunk* chunk = nullptr;
        const uint32_t* indexes = nullptr;
        AsyncDeltaWriterCallback* write_cb;
        uint32_t indexes_size = 0;
        bool commit_after_write = false;
    };

    Status _init();

    static int _execute(void* meta, bthread::TaskIterator<AsyncDeltaWriter::Task>& iter);

    std::unique_ptr<DeltaWriter> _writer;
    bthread::ExecutionQueueId<Task> _queue_id;
};

class CommittedRowsetInfo {
public:
    const Tablet* tablet;
    const Rowset* rowset;
    const RowsetWriter* rowset_writer;
};

class AsyncDeltaWriterRequest {
public:
    // nullptr means no record to write
    vectorized::Chunk* chunk = nullptr;
    const uint32_t* indexes = nullptr;
    uint32_t indexes_size = 0;
    bool commit_after_write = false;
};

class AsyncDeltaWriterCallback {
public:
    virtual ~AsyncDeltaWriterCallback() = default;

    // st != Status::OK means either the writes or the commit failed.
    // st == Status::OK && info != nullptr means commit succeeded.
    // st == Status::OK && info == nullptr means the writes succeeded with no commit.
    virtual void run(const Status& st, const CommittedRowsetInfo* info) = 0;
};

} // namespace starrocks::vectorized