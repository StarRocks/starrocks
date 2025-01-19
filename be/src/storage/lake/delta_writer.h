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

#include <atomic>
#include <memory>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "storage/lake/delta_writer_finish_mode.h"
#include "storage/memtable_flush_executor.h"
#include "util/runtime_profile.h"

namespace starrocks {
class MemTracker;
class SlotDescriptor;
class Chunk;
class TabletSchema;
class ThreadPool;
struct FileInfo;
class TxnLogPB;
} // namespace starrocks

namespace starrocks::lake {

class DeltaWriterImpl;
class TabletManager;
class TabletWriter;

// Statistics for DeltaWriter
struct DeltaWriterStat {
    std::atomic_int32_t task_count = 0;
    std::atomic_int64_t pending_time_ns = 0;

    // ====== statistics for write()

    // The number of write()
    std::atomic_int32_t write_count = 0;
    // The number of rows to write
    std::atomic_int32_t row_count = 0;
    // Accumulated time for write()
    std::atomic_int64_t write_time_ns = 0;
    // The number that memtable is full
    std::atomic_int32_t memtable_full_count = 0;
    // The number that reach memory limit, and each will
    // trigger memtable flush, and wait for it to finish
    std::atomic_int32_t memory_exceed_count = 0;
    // Accumulated time to wait for flush because of reaching memory limit
    std::atomic_int64_t write_wait_flush_time_ns = 0;

    // ====== statistics for finish_with_txnlog()

    std::atomic_int64_t finish_time_ns = 0;
    // Time to wait for memtable flush
    std::atomic_int64_t finish_wait_flush_time_ns = 0;
    // Time to prepare txn log in
    std::atomic_int64_t finish_prepare_txn_log_time_ns = 0;
    // Time to put txn log
    std::atomic_int64_t finish_put_txn_log_time_ns = 0;
    // Time to preload pk
    std::atomic_int64_t finish_pk_preload_time_ns = 0;

    // ====== statistics for close()

    // Time for close()
    std::atomic_int64_t close_time_ns = 0;
};

class DeltaWriter {
    friend class DeltaWriterBuilder;

public:
    using TxnLogPtr = std::shared_ptr<const TxnLogPB>;

    // Return the thread pool used for performing write IO.
    static ThreadPool* io_threads();

    explicit DeltaWriter(DeltaWriterImpl* impl) : _impl(impl) {}

    ~DeltaWriter();

    DISALLOW_COPY_AND_MOVE(DeltaWriter);

    // NOTE: It's ok to invoke this method in a bthread, there is no I/O operation in this method.
    Status open();

    // NOTE: Do NOT invoke this method in a bthread.
    Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    // NOTE: Do NOT invoke this method in a bthread.
    StatusOr<TxnLogPtr> finish_with_txnlog(DeltaWriterFinishMode mode = kWriteTxnLog);

    // NOTE: Do NOT invoke this method in a bthread.
    Status finish();

    // Manual flush used by stale memtable flush
    // different from `flush()`, this method will reduce memory usage in `mem_tracker`
    Status manual_flush();

    // Manual flush, mainly used in UT
    // NOTE: Do NOT invoke this method in a bthread.
    Status flush();

    // Manual flush, mainly used in UT
    // NOTE: Do NOT invoke this method in a bthread.
    Status flush_async();

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

    void update_task_stat(int32_t num_tasks, int64_t pending_time_ns);

    const DeltaWriterStat& get_writer_stat() const;

    const FlushStatistic* get_flush_stats() const;

    bool has_spill_block() const;

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

    DeltaWriterBuilder& set_schema_id(int64_t schema_id) {
        _schema_id = schema_id;
        return *this;
    }

    DeltaWriterBuilder& set_partial_update_mode(const PartialUpdateMode& partial_update_mode) {
        _partial_update_mode = partial_update_mode;
        return *this;
    }

    DeltaWriterBuilder& set_column_to_expr_value(const std::map<std::string, std::string>* column_to_expr_value) {
        _column_to_expr_value = column_to_expr_value;
        return *this;
    }

    DeltaWriterBuilder& set_load_id(const PUniqueId& load_id) {
        _load_id = load_id;
        return *this;
    }

    DeltaWriterBuilder& set_profile(RuntimeProfile* profile) {
        _profile = profile;
        return *this;
    }

    StatusOr<DeltaWriterPtr> build();

private:
    TabletManager* _tablet_mgr{nullptr};
    int64_t _txn_id{0};
    int64_t _table_id{0};
    int64_t _partition_id{0};
    int64_t _schema_id{0};
    int64_t _tablet_id{0};
    const std::vector<SlotDescriptor*>* _slots{nullptr};
    std::string _merge_condition{};
    int64_t _immutable_tablet_size{0};
    MemTracker* _mem_tracker{nullptr};
    int64_t _max_buffer_size{0};
    bool _miss_auto_increment_column{false};
    PartialUpdateMode _partial_update_mode{PartialUpdateMode::ROW_MODE};
    const std::map<std::string, std::string>* _column_to_expr_value{nullptr};
    PUniqueId _load_id;
    RuntimeProfile* _profile{nullptr};
};

} // namespace starrocks::lake
