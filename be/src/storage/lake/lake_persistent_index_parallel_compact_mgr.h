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
#include <mutex>
#include <vector>

#include "base/uid_util.h"
#include "common/status.h"
#include "common/thread/threadpool.h"
#include "gutil/ref_counted.h"
#include "storage/lake/sst_seek_range.h"
#include "storage/lake/tablet_metadata.h"
#include "util/trace.h"

namespace starrocks::lake {

class TabletManager;
class PersistentIndexSstable;

class AsyncCompactCB {
public:
    AsyncCompactCB(std::unique_ptr<ThreadPoolToken> token,
                   std::function<Status(const std::vector<PersistentIndexSstablePB>&)> callback);
    virtual ~AsyncCompactCB() = default;

    void add_result(const std::vector<PersistentIndexSstablePB>& ssts);

    void update_status(const Status& status);

    // -1 means wait forever.
    // return true when all tasks are done before timeout.
    // return false when timeout happens.
    // It should be called in main thread to wait for compaction done.
    StatusOr<bool> wait_for(int timeout_ms = -1);

    ThreadPoolToken* thread_pool_token() { return _thread_pool_token.get(); }

    Trace* trace();

    int64_t create_us() const { return _create_us; }

private:
    std::unique_ptr<ThreadPoolToken> _thread_pool_token;
    std::function<Status(const std::vector<PersistentIndexSstablePB>&)> _callback;
    Status _status;
    std::mutex _mutex;
    std::vector<PersistentIndexSstablePB> _output_sstables;
    scoped_refptr<Trace> _trace_guard;
    int64_t _create_us;
};

using AsyncCompactCBPtr = std::unique_ptr<AsyncCompactCB>;

class LakePersistentIndexParallelCompactTask : public Runnable {
public:
    LakePersistentIndexParallelCompactTask(const std::vector<std::vector<PersistentIndexSstablePB>>& input_sstables,
                                           TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                           bool merge_base_level, const UniqueId& fileset_id,
                                           const SstSeekRange& seek_range)
            : _input_sstables(input_sstables),
              _tablet_mgr(tablet_mgr),
              _metadata(metadata),
              _merge_base_level(merge_base_level),
              _output_fileset_id(fileset_id),
              _seek_range(seek_range) {}

    void set_cb(AsyncCompactCB* cb) { _cb = cb; }

    void run() override;

    void cancel() override;

    const std::vector<PersistentIndexSstablePB>& output_sstables() const { return _output_sstables; }

private:
    Status do_run();

    size_t input_sstable_file_cnt() const;

private:
    // Input sstables to be compacted
    // Each fileset is a vector of sstable metadata.
    // input_sstables contains partial sstable files in each input filesets.
    std::vector<std::vector<PersistentIndexSstablePB>> _input_sstables;

    // Context info needed for compaction
    TabletManager* _tablet_mgr = nullptr;
    TabletMetadataPtr _metadata;
    bool _merge_base_level = false;
    UniqueId _output_fileset_id;
    SstSeekRange _seek_range;
    AsyncCompactCB* _cb = nullptr;
    // output sstable pb
    std::vector<PersistentIndexSstablePB> _output_sstables;
};

// Manages parallel compaction of persistent index sstables.
class LakePersistentIndexParallelCompactMgr {
public:
    LakePersistentIndexParallelCompactMgr(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}
    ~LakePersistentIndexParallelCompactMgr();

    Status init();

    void shutdown();

    Status update_max_threads(int max_threads);

    int32_t calc_max_threads() const;

    StatusOr<AsyncCompactCBPtr> async_compact(
            const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, const TabletMetadataPtr& metadata,
            bool merge_base_level, const std::function<Status(const std::vector<PersistentIndexSstablePB>&)>& callback);

    Status compact(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                   const TabletMetadataPtr& metadata, bool merge_base_level,
                   std::vector<PersistentIndexSstablePB>* output_sstables);

    ThreadPool* thread_pool() { return _thread_pool.get(); }

    // For UT to test generate_compaction_tasks.
    void TEST_generate_compaction_tasks(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                                        const TabletMetadataPtr& metadata, bool merge_base_level,
                                        std::vector<std::shared_ptr<LakePersistentIndexParallelCompactTask>>* tasks);

    // For UT to test sample_keys_from_sstable.
    Status TEST_sample_keys_from_sstable(const PersistentIndexSstablePB& sstable_pb, const TabletMetadataPtr& metadata,
                                         std::vector<std::string>* sample_keys);

    // For UT
    void TEST_set_tablet_mgr(TabletManager* tablet_mgr) { _tablet_mgr = tablet_mgr; }

private:
    // generate compaction tasks using candidate filesets.
    // The final task number will be decided by config pk_index_parallel_compaction_task_split_threshold_bytes
    void generate_compaction_tasks(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                                   const TabletMetadataPtr& metadata, bool merge_base_level,
                                   std::vector<std::shared_ptr<LakePersistentIndexParallelCompactTask>>* tasks);

    Status sample_keys_from_sstable(const PersistentIndexSstablePB& sstable_pb, const TabletMetadataPtr& metadata,
                                    std::vector<std::string>* sample_keys);

private:
    // Check if two key ranges overlap
    // Returns true if [start1, end1) overlaps with [start2, end2)
    static bool key_ranges_overlap(const std::string& start1, const std::string& end1, const std::string& start2,
                                   const std::string& end2);

    std::unique_ptr<ThreadPool> _thread_pool;
    TabletManager* _tablet_mgr = nullptr;
};

} // namespace starrocks::lake
