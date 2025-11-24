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

namespace starrocks::lake {

class TabletManager;
class PersistentIndexSstable;

class LakePersistentIndexParallelCompactTask {
public:
    LakePersistentIndexParallelCompactTask(const std::vector<std::vector<PersistentIndexSstablePB>>& input_sstables,
                                           const std::string& start_key, const std::string& end_key,
                                           TabletManager* tablet_mgr, int64_t tablet_id, bool merge_base_level)
            : _input_sstables(input_sstables),
              _start_key(start_key),
              _end_key(end_key),
              _tablet_mgr(tablet_mgr),
              _tablet_id(tablet_id),
              _merge_base_level(merge_base_level) {}

    Status run();

    PersistentIndexSstablePB* get_output_sstable() { return &_output_sstable; }

private:
    // Input sstables to be compacted
    // Each fileset is a vector of sstable metadata.
    // input_sstables contains partial sstable files in each input filesets.
    std::vector<std::vector<PersistentIndexSstablePB>> _input_sstables;
    std::string _start_key; // Minimum start_key in this fileset
    std::string _end_key;   // Maximum end_key in this fileset

    // Context info needed for compaction
    TabletManager* _tablet_mgr = nullptr;
    int64_t _tablet_id = 0;
    bool _merge_base_level = false;

    // output sstable pb
    PersistentIndexSstablePB _output_sstable;
};

// Manages parallel compaction of persistent index sstables.
class LakePersistentIndexParallelCompactMgr {
public:
    LakePersistentIndexParallelCompactMgr(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}
    ~LakePersistentIndexParallelCompactMgr();

    Status init();

    Status compact(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates, int64_t tablet_id,
                   bool merge_base_level, std::vector<PersistentIndexSstablePB>* output_sstables);

private:
    // Check if two key ranges overlap
    // Returns true if [start1, end1) overlaps with [start2, end2)
    static bool key_ranges_overlap(const std::string& start1, const std::string& end1, const std::string& start2,
                                   const std::string& end2);

    // generate compaction tasks using candidate filesets.
    // The final task number will be decided by config pk_parallel_compaction_task_split_threshold_bytes
    static void generate_compaction_tasks(const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                                          int64_t tablet_id, bool merge_base_level,
                                          std::vector<LakePersistentIndexParallelCompactTask>* tasks);

    std::unique_ptr<ThreadPool> _thread_pool;
    TabletManager* _tablet_mgr = nullptr;
};

} // namespace starrocks::lake
