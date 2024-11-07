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
#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "storage/lake/delta_writer_finish_mode.h"

namespace starrocks {
class MemTracker;
class SlotDescriptor;
} // namespace starrocks

namespace starrocks {
class Chunk;
class TxnLogPB;
} // namespace starrocks

namespace starrocks::lake {

class AsyncDeltaWriterImpl;
class DeltaWriter;
class TabletManager;

// AsyncDeltaWriter is a wrapper on DeltaWriter to support non-blocking async write.
// All submitted tasks will be executed in the FIFO order.
class AsyncDeltaWriter {
    friend class AsyncDeltaWriterBuilder;

public:
    using TxnLogPtr = std::shared_ptr<const TxnLogPB>;
    using Ptr = std::unique_ptr<AsyncDeltaWriter>;
    using Callback = std::function<void(Status st)>;
    using FinishCallback = std::function<void(StatusOr<TxnLogPtr> res)>;

    explicit AsyncDeltaWriter(AsyncDeltaWriterImpl* impl) : _impl(impl) {}

    ~AsyncDeltaWriter();

    DISALLOW_COPY_AND_MOVE(AsyncDeltaWriter);

    // This method can be called concurrently and multiple times and only the
    // first call will take real actions, all subsequent calls will return the
    // same Status as the first call.
    //
    // [thread-safe]
    Status open();

    // REQUIRE:
    //  - |chunk| and |indexes| must be kept alive until |cb| been invoked
    //
    // [thread-safe]
    //
    // TODO: Change signature to `Future<Status> write(Chunk*, uint32_t*, uint32_t)`
    void write(const Chunk* chunk, const uint32_t* indexes, uint32_t indexes_size, Callback cb);

    // This method will flush all the records in memtable to disk.
    //
    // [thread-safe]
    void flush(Callback cb);

    // If the AsyncDeltaWriter has been `close()`ed, |cb| will be invoked immediately
    // in the caller's thread with an error status.
    //
    // [thread-safe]
    //
    // TODO: Change signature to `Future<Status> finish()`
    void finish(FinishCallback cb) { finish(DeltaWriterFinishMode::kWriteTxnLog, cb); }

    void finish(DeltaWriterFinishMode mode, FinishCallback cb);

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

    [[nodiscard]] int64_t queueing_memtable_num() const;

    [[nodiscard]] int64_t tablet_id() const;

    [[nodiscard]] int64_t partition_id() const;

    [[nodiscard]] int64_t txn_id() const;

    [[nodiscard]] bool is_immutable() const;

    Status check_immutable();

    [[nodiscard]] int64_t last_write_ts() const;

private:
    AsyncDeltaWriterImpl* _impl;
};

class AsyncDeltaWriterBuilder {
public:
    using AsyncDeltaWriterPtr = std::unique_ptr<AsyncDeltaWriter>;

    AsyncDeltaWriterBuilder() = default;

    ~AsyncDeltaWriterBuilder() = default;

    DISALLOW_COPY_AND_MOVE(AsyncDeltaWriterBuilder);

    AsyncDeltaWriterBuilder& set_tablet_manager(TabletManager* tablet_mgr) {
        _tablet_mgr = tablet_mgr;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_tablet_id(int64_t tablet_id) {
        _tablet_id = tablet_id;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_txn_id(int64_t txn_id) {
        _txn_id = txn_id;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_table_id(int64_t table_id) {
        _table_id = table_id;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_partition_id(int64_t partition_id) {
        _partition_id = partition_id;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_slot_descriptors(const std::vector<SlotDescriptor*>* slots) {
        _slots = slots;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_immutable_tablet_size(int64_t immutable_tablet_size) {
        _immutable_tablet_size = immutable_tablet_size;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_mem_tracker(MemTracker* mem_tracker) {
        _mem_tracker = mem_tracker;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_miss_auto_increment_column(bool miss_auto_increment_column) {
        _miss_auto_increment_column = miss_auto_increment_column;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_merge_condition(std::string merge_condition) {
        _merge_condition = std::move(merge_condition);
        return *this;
    }

    AsyncDeltaWriterBuilder& set_schema_id(int64_t schema_id) {
        _schema_id = schema_id;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_partial_update_mode(const PartialUpdateMode& partial_update_mode) {
        _partial_update_mode = partial_update_mode;
        return *this;
    }

    AsyncDeltaWriterBuilder& set_column_to_expr_value(const std::map<std::string, std::string>* column_to_expr_value) {
        _column_to_expr_value = column_to_expr_value;
        return *this;
    }

    StatusOr<AsyncDeltaWriterPtr> build();

private:
    TabletManager* _tablet_mgr{nullptr};
    int64_t _txn_id{0};
    int64_t _table_id{0};
    int64_t _partition_id{0};
    int64_t _schema_id{0};
    int64_t _tablet_id{0};
    const std::vector<SlotDescriptor*>* _slots{nullptr};
    int64_t _immutable_tablet_size{0};
    MemTracker* _mem_tracker{nullptr};
    std::string _merge_condition{};
    bool _miss_auto_increment_column{false};
    PartialUpdateMode _partial_update_mode{PartialUpdateMode::ROW_MODE};
    const std::map<std::string, std::string>* _column_to_expr_value{nullptr};
};

} // namespace starrocks::lake
