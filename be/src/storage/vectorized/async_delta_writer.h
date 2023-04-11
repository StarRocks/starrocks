// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/execution_queue.h>
DIAGNOSTIC_POP

#include <google/protobuf/service.h>

#include "storage/vectorized/delta_writer.h"

namespace starrocks::vectorized {

class AsyncDeltaWriterRequest;
class CommittedRowsetInfo;
class AsyncDeltaWriterCallback;

// AsyncDeltaWriter is a wrapper on DeltaWriter to support non-blocking async write.
// All submitted tasks will be executed in the FIFO order.
class AsyncDeltaWriter {
    struct private_type;

public:
    // Undocumented rule of bthread that -1 is an invalid queue id
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;

    // Create a new transaction in TxnManager and return a AsyncDeltaWriter for write.
    static StatusOr<std::unique_ptr<AsyncDeltaWriter>> open(const DeltaWriterOptions& opt, MemTracker* mem_tracker);

    AsyncDeltaWriter(private_type, std::unique_ptr<DeltaWriter> writer)
            : _writer(std::move(writer)), _queue_id{kInvalidQueueId}, _closed(false) {}

    ~AsyncDeltaWriter();

    AsyncDeltaWriter(const AsyncDeltaWriter&) = delete; // DISALLOW COPY
    void operator=(const AsyncDeltaWriter&) = delete;   // DISALLOW ASSIGN

    // REQUIRE:
    //  - |cb| cannot be NULL
    //  - if |req.chunk| is not NULL, the chunk it points to must be kept alive until |cb->run()| been called
    //  - if |req.chunk| is not NULL, |req.indexes| must not be NULL and kept alive until |cb->run()| been
    //    called.
    //
    // [thread-safe and wait-free]
    void write(const AsyncDeltaWriterRequest& req, AsyncDeltaWriterCallback* cb);

    // [thread-safe and wait-free]
    void commit(AsyncDeltaWriterCallback* cb);

    // [thread-safe and wait-free]
    void abort(bool with_log = true);

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
        bool abort = false;
        bool abort_with_log = false;
    };

    static int _execute(void* meta, bthread::TaskIterator<AsyncDeltaWriter::Task>& iter);

    Status _init();
    void _close();

    std::unique_ptr<DeltaWriter> _writer;
    bthread::ExecutionQueueId<Task> _queue_id;
    std::atomic<bool> _closed;
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
