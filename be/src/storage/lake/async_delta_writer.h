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

namespace starrocks {
class Chunk;
}

namespace starrocks::lake {

class AsyncDeltaWriterImpl;
class TabletManager;

// AsyncDeltaWriter is a wrapper on DeltaWriter to support non-blocking async write.
// All submitted tasks will be executed in the FIFO order.
class AsyncDeltaWriter {
    using Chunk = starrocks::Chunk;

public:
    using Ptr = std::unique_ptr<AsyncDeltaWriter>;
    using Callback = std::function<void(Status st)>;

    // |tablet_manager|、|slots| and |mem_tracker| must outlive the AsyncDeltaWriter
    static Ptr create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker);

    // |tablet_manager|、|slots| and |mem_tracker| must outlive the AsyncDeltaWriter
    static Ptr create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, const std::string& merge_condition,
                      MemTracker* mem_tracker);

    explicit AsyncDeltaWriter(AsyncDeltaWriterImpl* impl) : _impl(impl) {}

    ~AsyncDeltaWriter();

    DISALLOW_COPY_AND_MOVE(AsyncDeltaWriter);

    // This method can be called concurrently and multiple times and only the
    // first call will take real actions, all subsequent calls will return the
    // same Status as the first call.
    //
    // [thread-safe]
    [[nodiscard]] Status open();

    // REQUIRE:
    //  - |chunk| and |indexes| must be kept alive until |cb| been invoked
    //
    // [thread-safe]
    //
    // TODO: Change signature to `Future<Status> write(Chunk*, uint32_t*, uint32_t)`
    void write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size, Callback cb);

    // If the AsyncDeltaWriter has been `close()`ed, |cb| will be invoked immediately
    // in the caller's thread with an error status.
    //
    // [thread-safe]
    //
    // TODO: Change signature to `Future<Status> finish()`
    void finish(Callback cb);

    // This method will wait for all running tasks completed.
    //
    // This method can be called concurrently and multiple times and only the
    // first call will take real actions.
    //
    // If AsyncDeltaWriter `close()`ed without `finish()` all the records written
    // will be deleted.
    //
    // [thread-safe]
    void close();

    [[nodiscard]] int64_t tablet_id() const;

    [[nodiscard]] int64_t partition_id() const;

    [[nodiscard]] int64_t txn_id() const;

private:
    AsyncDeltaWriterImpl* _impl;
};

} // namespace starrocks::lake
