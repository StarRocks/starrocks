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
    friend class DeltaWriterBuilder;

public:
    enum FinishMode {
        kWriteTxnLog,
        kDontWriteTxnLog,
    };

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

    bool is_immutable() const;

    Status check_immutable();

    int64_t last_write_ts() const;

private:
    DeltaWriterImpl* _impl;
};

class DeltaWriterBuilder {
public:
    using DeltaWriterPtr = std::unique_ptr<DeltaWriter>;

    DeltaWriterBuilder() = default;
    ~DeltaWriterBuilder() = default;

    DISALLOW_COPY_AND_MOVE(DeltaWriterBuilder);

    DeltaWriterBuilder& set_tablet_manager(TabletManager* tablet_mgr) {
        _tablet_mgr = tablet_mgr;
        return *this;
    }

    DeltaWriterBuilder& set_txn_id(int64_t txn_id) {
        _txn_id = txn_id;
        return *this;
    }

    DeltaWriterBuilder& set_table_id(int64_t table_id) {
        _table_id = table_id;
        return *this;
    }

    DeltaWriterBuilder& set_partition_id(int64_t partition_id) {
        _partition_id = partition_id;
        return *this;
    }

    DeltaWriterBuilder& set_tablet_id(int64_t tablet_id) {
        _tablet_id = tablet_id;
        return *this;
    }

    DeltaWriterBuilder& set_slot_descriptors(const std::vector<SlotDescriptor*>* slots) {
        _slots = slots;
        return *this;
    }

    DeltaWriterBuilder& set_merge_condition(std::string merge_condition) {
        _merge_condition = std::move(merge_condition);
        return *this;
    }

    DeltaWriterBuilder& set_immutable_tablet_size(int64_t immutable_tablet_size) {
        _immutable_tablet_size = immutable_tablet_size;
        return *this;
    }

    DeltaWriterBuilder& set_mem_tracker(MemTracker* mem_tracker) {
        _mem_tracker = mem_tracker;
        return *this;
    }

    DeltaWriterBuilder& set_miss_auto_increment_column(bool miss_auto_increment_column) {
        _miss_auto_increment_column = miss_auto_increment_column;
        return *this;
    }

    DeltaWriterBuilder& set_max_buffer_size(int64_t max_buffer_size) {
        _max_buffer_size = max_buffer_size;
        return *this;
    }

    DeltaWriterBuilder& set_index_id(int64_t index_id) {
        _index_id = index_id;
        return *this;
    }

    StatusOr<DeltaWriterPtr> build();

private:
    TabletManager* _tablet_mgr{nullptr};
    int64_t _txn_id{0};
    int64_t _table_id{0};
    int64_t _partition_id{0};
    int64_t _index_id{0};
    int64_t _tablet_id{0};
    const std::vector<SlotDescriptor*>* _slots{nullptr};
    std::string _merge_condition{};
    int64_t _immutable_tablet_size{0};
    MemTracker* _mem_tracker{nullptr};
    int64_t _max_buffer_size{0};
    bool _miss_auto_increment_column{false};
};

} // namespace starrocks::lake
