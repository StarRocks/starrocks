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

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace starrocks::lake {

class TabletManager;
class PersistentIndexSstable;

// SeekRange represents a basic key range unit for compaction tasks split.
struct SeekRange {
    // scan range is [seek_key, stop_key). stop_key is exclusive.
    std::string seek_key;
    std::string stop_key; // could be empty meaning infinity
    bool has_overlap(const PersistentIndexSstableRangePB& range) const;
    bool full_contains(const PersistentIndexSstableRangePB& range) const;
};

class LakePersistentIndexParallelCompactTask {
public:
    LakePersistentIndexParallelCompactTask(const std::vector<std::vector<PersistentIndexSstablePB>>& input_sstables,
                                           TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                           bool merge_base_level, const UniqueId& fileset_id,
                                           const SeekRange& seek_range)
            : _input_sstables(input_sstables),
              _tablet_mgr(tablet_mgr),
              _metadata(metadata),
              _merge_base_level(merge_base_level),
              _output_fileset_id(fileset_id),
              _seek_range(seek_range) {}

    Status run();

    const std::vector<PersistentIndexSstablePB>& output_sstables() { return _output_sstables; }

private:
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
    SeekRange _seek_range;

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

    Status compact(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                   const TabletMetadataPtr& metadata, bool merge_base_level,
                   std::vector<PersistentIndexSstablePB>* output_sstables);

    // generate compaction tasks using candidate filesets.
    // The final task number will be decided by config pk_index_parallel_compaction_task_split_threshold_bytes
    void generate_compaction_tasks(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                                   const TabletMetadataPtr& metadata, bool merge_base_level,
                                   std::vector<std::unique_ptr<LakePersistentIndexParallelCompactTask>>* tasks);

private:
    // Check if two key ranges overlap
    // Returns true if [start1, end1) overlaps with [start2, end2)
    static bool key_ranges_overlap(const std::string& start1, const std::string& end1, const std::string& start2,
                                   const std::string& end2);

    std::unique_ptr<ThreadPool> _thread_pool;
    TabletManager* _tablet_mgr = nullptr;
};

} // namespace starrocks::lake
