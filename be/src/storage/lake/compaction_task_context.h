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

namespace starrocks {
struct OlapReaderStatistics;
}

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
    int64_t io_ns_remote = 0;
    int64_t io_ns_local_disk = 0;
    int64_t io_bytes_read_remote = 0;
    int64_t io_bytes_read_local_disk = 0;
    int64_t segment_init_ns = 0;
    int64_t column_iterator_init_ns = 0;
    int64_t io_count_local_disk = 0;
    int64_t io_count_remote = 0;
    int64_t in_queue_time_sec = 0;
    int64_t pk_sst_merge_ns = 0;

    void collect(const OlapReaderStatistics& reader_stats);
    CompactionTaskStats operator+(const CompactionTaskStats& that) const;
    CompactionTaskStats operator-(const CompactionTaskStats& that) const;
    std::string to_json_stats();
};

// Context of a single tablet compaction task.
struct CompactionTaskContext : public butil::LinkNode<CompactionTaskContext> {
    explicit CompactionTaskContext(int64_t txn_id_, int64_t tablet_id_, int64_t version_, bool force_base_compaction_,
                                   std::shared_ptr<CompactionTaskCallback> cb_)
            : txn_id(txn_id_),
              tablet_id(tablet_id_),
              version(version_),
              force_base_compaction(force_base_compaction_),
              callback(std::move(cb_)) {}

#ifndef NDEBUG
    ~CompactionTaskContext() {
        CHECK(next() == this && previous() == this) << "Must remove CompactionTaskContext from list before destructor";
    }
#endif

    const int64_t txn_id;
    const int64_t tablet_id;
    const int64_t version;
    const bool force_base_compaction;
    std::atomic<int64_t> start_time{0};
    std::atomic<int64_t> finish_time{0};
    std::atomic<bool> skipped{false};
    std::atomic<int> runs{0};
    Status status;
    Progress progress;
    int64_t enqueue_time_sec; // time point when put into queue
    std::shared_ptr<CompactionTaskCallback> callback;
    std::unique_ptr<CompactionTaskStats> stats = std::make_unique<CompactionTaskStats>();
};

} // namespace starrocks::lake
