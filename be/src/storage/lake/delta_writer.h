// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
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

class DeltaWriterImpl;
class TabletWriter;

class DeltaWriter {
    using Chunk = starrocks::vectorized::Chunk;

public:
    using Ptr = std::unique_ptr<DeltaWriter>;

    // for load
    static Ptr create(int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker);

    // for schema change
    static Ptr create(int64_t tablet_id, int64_t max_buffer_size, MemTracker* mem_tracker);

    explicit DeltaWriter(DeltaWriterImpl* impl) : _impl(impl) {}

    ~DeltaWriter();

    DISALLOW_COPY_AND_MOVE(DeltaWriter);

    // NOTE: It's ok to invoke this method in a bthread, there is no I/O operation in this method.
    [[nodiscard]] Status open();

    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status finish();

    // Manual flush, mainly used in UT
    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status flush();

    // Manual flush, mainly used in UT
    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status flush_async();

    // NOTE: Do NOT invoke this method in a bthread unless you are sure that `write()` has never been called.
    void close();

    [[nodiscard]] int64_t partition_id() const;

    [[nodiscard]] int64_t tablet_id() const;

    [[nodiscard]] int64_t txn_id() const;

    [[nodiscard]] MemTracker* mem_tracker();

    [[nodiscard]] TabletWriter* tablet_writer();

private:
    DeltaWriterImpl* _impl;
};

} // namespace starrocks::lake
