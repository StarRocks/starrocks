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

#include <memory>
#include <vector>

#include "common/statusor.h"
#include "gutil/macros.h"

namespace starrocks {
class MemTracker;
class SlotDescriptor;
class Chunk;
class TabletSchema;
class ThreadPool;
struct FileInfo;
} // namespace starrocks

namespace starrocks::lake {

class DeltaWriterImpl;
class TabletManager;
class TabletWriter;

class DeltaWriter {
    using Chunk = starrocks::Chunk;

public:
    using Ptr = std::unique_ptr<DeltaWriter>;

    enum FinishMode {
        kWriteTxnLog,
        kDontWriteTxnLog,
    };

    // for load
    // Does NOT take the ownership of |tablet_manager|、|slots| and |mem_tracker|
    static Ptr create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker);

    // for condition update
    // Does NOT take the ownership of |tablet_manager|、|slots| and |mem_tracker|
    static Ptr create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, const std::string& merge_condition,
                      MemTracker* mem_tracker);

    // for auto increment
    // Does NOT take the ownership of |tablet_manager|、|slots| and |mem_tracker|
    static Ptr create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                      const std::vector<SlotDescriptor*>* slots, const std::string& merge_condition,
                      bool miss_auto_increment_column, int64_t table_id, MemTracker* mem_tracker);

    // for schema change
    // Does NOT take the ownership of |tablet_manager| and |mem_tracker|
    static Ptr create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t max_buffer_size,
                      MemTracker* mem_tracker);

    // Return the thread pool used for performing write IO.
    static ThreadPool* io_threads();

    explicit DeltaWriter(DeltaWriterImpl* impl) : _impl(impl) {}

    ~DeltaWriter();

    DISALLOW_COPY_AND_MOVE(DeltaWriter);

    // NOTE: It's ok to invoke this method in a bthread, there is no I/O operation in this method.
    [[nodiscard]] Status open();

    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status finish(FinishMode mode = kWriteTxnLog);

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

    const int64_t queueing_memtable_num() const;

    // Return the list of file infos created by this DeltaWriter.
    // NOTE: Do NOT invoke this function after `close()`, otherwise may get unexpected result.
    std::vector<FileInfo> files() const;

    // The sum of all segment file sizes, in bytes.
    // NOTE: Do NOT invoke this function after `close()`, otherwise may get unexpected result.
    int64_t data_size() const;

    // The total number of rows have been written.
    // NOTE: Do NOT invoke this function after `close()`, otherwise may get unexpected result.
    int64_t num_rows() const;

    void TEST_set_partial_update(std::shared_ptr<const TabletSchema> tschema,
                                 const std::vector<int32_t>& referenced_column_ids);

    void TEST_set_miss_auto_increment_column();

private:
    DeltaWriterImpl* _impl;
};

} // namespace starrocks::lake
