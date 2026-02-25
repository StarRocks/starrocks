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

#include <butil/containers/linked_list.h>

#include <atomic>
#include <memory>
#include <string>

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks {
struct OlapReaderStatistics;
struct OlapWriterStatistics;
} // namespace starrocks

namespace starrocks::lake {

class CompactionTaskCallback;
class Progress {
public:
    int value() const { return _value.load(std::memory_order_acquire); }

    void update(int value) { _value.store(value, std::memory_order_release); }

private:
    std::atomic<int> _value{0};
};

struct CompactionTaskStats {
    int64_t io_ns_read_remote = 0;
    int64_t io_ns_read_local_disk = 0;
    int64_t io_bytes_read_remote = 0;
    int64_t io_bytes_read_local_disk = 0;
    int64_t segment_init_ns = 0;
    int64_t column_iterator_init_ns = 0;
    int64_t io_count_local_disk = 0;
    int64_t io_count_remote = 0;
    int64_t in_queue_time_sec = 0;
    int64_t read_segment_count = 0;
    int64_t write_segment_count = 0;
    int64_t write_segment_bytes = 0;
    int64_t io_ns_write_remote = 0;
    int64_t pk_sst_merge_ns = 0;
    int64_t input_file_size = 0;

    void collect(const OlapReaderStatistics& reader_stats);
    void collect(const OlapWriterStatistics& writer_stats);
    CompactionTaskStats operator+(const CompactionTaskStats& that) const;
    CompactionTaskStats operator-(const CompactionTaskStats& that) const;
    std::string to_json_stats();
};

// Context of a single tablet compaction task.
struct CompactionTaskContext : public butil::LinkNode<CompactionTaskContext> {
    // Constructor for normal compaction
    explicit CompactionTaskContext(int64_t txn_id_, int64_t tablet_id_, int64_t version_, bool force_base_compaction_,
                                   bool skip_write_txnlog_, std::shared_ptr<CompactionTaskCallback> cb_,
                                   int64_t table_id_ = 0, int64_t partition_id_ = 0)
            : txn_id(txn_id_),
              tablet_id(tablet_id_),
              version(version_),
              force_base_compaction(force_base_compaction_),
              skip_write_txnlog(skip_write_txnlog_),
              callback(std::move(cb_)),
              table_id(table_id_),
              partition_id(partition_id_) {}

    // Factory method for parallel compaction subtasks (with subtask_id)
    static std::unique_ptr<CompactionTaskContext> create_for_subtask(int64_t txn_id_, int64_t tablet_id_,
                                                                     int64_t version_, bool force_base_compaction_,
                                                                     bool skip_write_txnlog_,
                                                                     std::shared_ptr<CompactionTaskCallback> cb_,
                                                                     int32_t subtask_id_) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id_, tablet_id_, version_, force_base_compaction_,
                                                           skip_write_txnlog_, std::move(cb_));
        ctx->subtask_id = subtask_id_;
        return ctx;
    }

#ifndef NDEBUG
    ~CompactionTaskContext() {
        CHECK(next() == this && previous() == this) << "Must remove CompactionTaskContext from list before destructor";
    }
#endif

    const int64_t txn_id;
    const int64_t tablet_id;
    const int64_t version;
    const bool force_base_compaction;
    const bool skip_write_txnlog;
    std::atomic<int64_t> start_time{0};
    std::atomic<int64_t> finish_time{0};
    std::atomic<bool> skipped{false};
    std::atomic<int> runs{0};
    Status status;
    Progress progress;
    int64_t enqueue_time_sec{0}; // time point when put into queue
    std::shared_ptr<CompactionTaskCallback> callback;
    std::unique_ptr<CompactionTaskStats> stats = std::make_unique<CompactionTaskStats>();
    std::shared_ptr<TxnLogPB> txn_log;
    int64_t table_id;
    int64_t partition_id;
    int32_t subtask_id = -1; // -1 means not a parallel compaction subtask
    // Number of subtasks in this compaction (1 for normal compaction, >1 for parallel compaction)
    int32_t subtask_count = 1;
    // Flag to indicate this is a merged context from parallel compaction.
    // When true, cleanup_tablet should be called in remove_states after RPC response is sent.
    bool is_parallel_merged = false;
};

} // namespace starrocks::lake
