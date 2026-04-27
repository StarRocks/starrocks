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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

// CompactionResultManager persists CompactionResultPB files locally on the BE,
// caches them between autonomous compaction completion and FE PUBLISH_AUTONOMOUS.
//
// File layout: {root_dir}/{tablet_id}_{base_version}_{result_id}.pb
// where {root_dir} is one of the BE store paths joined with kSubDir.
//
// Two indices are maintained in memory:
//  - tablet_results: tablet_id -> [(base_version, result_id, file_path)]
//  - pending_inputs: tablet_id -> set<rowset_id>  (union of all not-yet-published
//                    results' input_rowsets, used by pick_rowsets to exclude)
//
// Both indices are rebuilt from disk by scan_on_startup() to survive BE restarts.
class CompactionResultManager {
public:
    static constexpr const char* kSubDir = "lake/compaction_results";

    struct ResultRef {
        int64_t base_version;
        int64_t result_id;
        std::string file_path;
        // input_rowsets cached so pending_inputs maintenance avoids re-parsing files
        std::vector<uint32_t> input_rowsets;
    };

    explicit CompactionResultManager(std::vector<std::string> root_dirs);
    ~CompactionResultManager() = default;

    // Scan all root_dirs, parse every result file, rebuild in-memory indices.
    // Corrupt files are logged and skipped (file is renamed with .corrupt suffix).
    Status scan_on_startup();

    // Persist a result and update indices. Reject if total local bytes exceeds
    // config::lake_autonomous_compaction_local_result_dir_max_bytes.
    Status append_result(const CompactionResultPB& result);

    // Load all results for a tablet whose base_version <= upper_bound_version.
    // Returned results are sorted by base_version asc, result_id asc.
    StatusOr<std::vector<CompactionResultPB>> load_results(int64_t tablet_id, int64_t upper_bound_version);

    // Delete result files identified by (tablet_id, result_id) pairs and update indices.
    // Errors on individual deletes are logged but do not fail the whole call.
    Status delete_results(int64_t tablet_id, const std::vector<int64_t>& result_ids);

    // Return the rowset_ids that are currently held by not-yet-published results
    // for `tablet_id`. Used by compaction_policy to exclude these rowsets from new picks.
    std::unordered_set<uint32_t> pending_inputs(int64_t tablet_id) const;

    // Total bytes currently used across all results (refreshed lazily on append/delete).
    int64_t total_bytes() const { return _total_bytes.load(std::memory_order_relaxed); }

    // Number of result files across all tablets.
    size_t result_count() const;

    // Per-tablet accessors mainly for tests / debug.
    std::vector<ResultRef> list_results_for_tablet(int64_t tablet_id) const;

    // Allocate the next result_id for a tablet (monotonic, used by writers).
    int64_t next_result_id(int64_t tablet_id);

private:
    static std::string make_file_name(int64_t tablet_id, int64_t base_version, int64_t result_id);
    static bool parse_file_name(const std::string& name, int64_t* tablet_id, int64_t* base_version,
                                int64_t* result_id);

    Status load_one_file(const std::string& path);
    std::string pick_root_dir() const;

    const std::vector<std::string> _root_dirs;
    mutable std::mutex _mu;
    std::unordered_map<int64_t /*tablet_id*/, std::vector<ResultRef>> _tablet_results;
    std::unordered_map<int64_t /*tablet_id*/, std::unordered_multiset<uint32_t>> _pending_inputs;
    std::unordered_map<int64_t /*tablet_id*/, int64_t> _next_result_id;
    std::atomic<int64_t> _total_bytes{0};
};

// Helper: build a CompactionResultPB from a finished TxnLogPB and persist it via |mgr|.
// |base_version| should equal op_compaction.compact_version recorded by the task.
// Returns an error if the TxnLog has no op_compaction.
Status persist_compaction_result_from_txn_log(CompactionResultManager* mgr, int64_t tablet_id, int64_t base_version,
                                              const TxnLogPB& txn_log);

// Helper: merge multiple finished CompactionResultPB into a single TxnLogPB
// containing one OpParallelCompaction. Each result becomes one subtask, with
// re-assigned subtask_id 0..N-1 so that downstream apply paths stay consistent.
// Always returns a TxnLogPB with op_parallel_compaction set, even if N == 1.
// Caller is expected to put_txn_log() the returned log.
std::shared_ptr<TxnLogPB> merge_results_to_txn_log(const std::vector<CompactionResultPB>& results, int64_t tablet_id,
                                                   int64_t txn_id);

} // namespace starrocks::lake
