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

#pragma once

#include <bthread/execution_queue.h>
#include <google/protobuf/service.h>

#include <atomic>

#include "common/compiler_util.h"
#include "storage/delta_writer.h"

namespace brpc {
class Controller;
}

namespace google::protobuf {
class Closure;
}

namespace starrocks {
class SegmentFlushExecutor;
class SegmentFlushToken;

class AsyncDeltaWriterRequest;
class CommittedRowsetInfo;
class FailedRowsetInfo;
class AsyncDeltaWriterCallback;
class AsyncDeltaWriterSegmentRequest;

// AsyncDeltaWriter is a wrapper on DeltaWriter to support non-blocking async write.
// All submitted tasks will be executed in the FIFO order.
// TODO: this class is too similar to lake::AsyncDeltaWriter, remove this AsyncDeltaWriter and
// keep lake::AsyncDeltaWriter.
class AsyncDeltaWriter {
    struct private_type;

public:
    // Undocemented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
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
    void write_segment(const AsyncDeltaWriterSegmentRequest& req);

    // This method will flush all the records in memtable to disk.
    //
    // [thread-safe and wait-free]
    void flush();

    // [thread-safe and wait-free]
    void commit(AsyncDeltaWriterCallback* cb);

    // [thread-safe and wait-free]
    void abort(bool with_log = true);

    void cancel(const Status& st);

    int64_t partition_id() const { return _writer->partition_id(); }

    ReplicaState replica_state() const { return _writer->replica_state(); }

    State get_state() const { return _writer->get_state(); }

    const std::vector<PNetworkAddress>& replicas() const { return _writer->replicas(); }

    const FlushStatistic& get_flush_stats() const { return _writer->get_flush_stats(); }

    bool is_immutable() const { return _writer->is_immutable(); }

    int64_t last_write_ts() const { return _writer->last_write_ts(); }

    int64_t write_buffer_size() const { return _writer->write_buffer_size(); }

    // Just for testing
    DeltaWriter* writer() { return _writer.get(); }

private:
    struct private_type {
        explicit private_type(int) {}
    };

    struct Task {
        // If chunk == nullptr, this is a commit task
        Chunk* chunk = nullptr;
        const uint32_t* indexes = nullptr;
        AsyncDeltaWriterCallback* write_cb = nullptr;
        uint32_t indexes_size = 0;
        bool commit_after_write = false;
        bool abort = false;
        bool abort_with_log = false;
        bool flush_after_write = false;
        int64_t create_time_us;
    };

    static int _execute(void* meta, bthread::TaskIterator<AsyncDeltaWriter::Task>& iter);

    Status _init();
    void _close();

    std::shared_ptr<DeltaWriter> _writer;
    bthread::ExecutionQueueId<Task> _queue_id;
    std::atomic<bool> _closed;
};

class CommittedRowsetInfo {
public:
    const Tablet* tablet;
    const Rowset* rowset;
    const RowsetWriter* rowset_writer;
    const ReplicateToken* replicate_token;
};

class FailedRowsetInfo {
public:
    const int64_t tablet_id;
    const ReplicateToken* replicate_token;
};

class AsyncDeltaWriterRequest {
public:
    // nullptr means no record to write
    Chunk* chunk = nullptr;
    const uint32_t* indexes = nullptr;
    uint32_t indexes_size = 0;
    bool commit_after_write = false;
};

class AsyncDeltaWriterSegmentRequest {
public:
    brpc::Controller* cntl;
    const PTabletWriterAddSegmentRequest* request;
    PTabletWriterAddSegmentResult* response;
    google::protobuf::Closure* done;
    int64_t receive_time_us;
};

struct WriterStat {
    int64_t tablet_id;
    bool commit;
    int64_t create_time_us;
    int64_t receive_time_us;
    int64_t write_time_us;
    int64_t close_time_us;
    int64_t commit_time_us;
    int64_t finish_time_us;
};

class AsyncDeltaWriterCallback {
public:
    virtual ~AsyncDeltaWriterCallback() = default;

    // st != Status::OK means either the writes or the commit failed.
    // st == Status::OK && info != nullptr means commit succeeded.
    // st == Status::OK && info == nullptr means the writes succeeded with no commit.
    virtual void run(const Status& st, const CommittedRowsetInfo* info, const FailedRowsetInfo* failed_info,
                     WriterStat* writer_stat = nullptr) = 0;
};

} // namespace starrocks
