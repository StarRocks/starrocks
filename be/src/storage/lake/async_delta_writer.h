// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "gutil/macros.h"

namespace starrocks {
class MemTracker;
class SlotDescriptor;
} // namespace starrocks

namespace starrocks::vectorized {
class Chunk;
}

namespace starrocks::lake {

class AsyncDeltaWriterImpl;
class CommittedRowsetInfo;

// AsyncDeltaWriter is a wrapper on DeltaWriter to support non-blocking async write.
// All submitted tasks will be executed in the FIFO order.
class AsyncDeltaWriter {
    using Chunk = starrocks::vectorized::Chunk;

public:
    using Ptr = std::unique_ptr<AsyncDeltaWriter>;
    using Callback = std::function<void(Status st, int64_t tablet_id)>;

    // |slots| and |mem_tracker| must outlive the AsyncDeltaWriter
    static Ptr create(int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker);

    AsyncDeltaWriter(AsyncDeltaWriterImpl* impl) : _impl(impl) {}

    ~AsyncDeltaWriter();

    DISALLOW_COPY_AND_MOVE(AsyncDeltaWriter);

    Status open();

    // REQUIRE:
    //  - this AsyncDeltaWriter not `close()`ed
    //  - |chunk| and |indexes| must be kept alive until |cb| been invoked
    //
    // [thread-safe]
    //
    // TODO: Change signature to `Future<Status> write(Chunk*, uint32_t*, uint32_t)`
    void write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size, Callback cb);

    // REQUIRE:
    //  - this AsyncDeltaWriter not `close()`ed
    //
    // [thread-safe]
    //
    // TODO: Change signature to `Future<Status> finish()`
    void finish(Callback cb);

    // [NOT thread-safe]
    void close();

    int64_t tablet_id() const;

    int64_t partition_id() const;

    int64_t txn_id() const;

private:
    AsyncDeltaWriterImpl* _impl;
};

} // namespace starrocks::lake
