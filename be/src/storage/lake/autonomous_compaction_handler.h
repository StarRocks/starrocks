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

#include <unordered_set>

#include "common/status.h"
#include "gen_cpp/lake_service.pb.h"

namespace starrocks::lake {

class TabletManager;
class CompactionResultManager;
class LakeCompactionManager;

// Statistics for autonomous compaction processing
struct AutonomousCompactionStats {
    int64_t tablets_processed = 0;
    int64_t tablets_with_compaction = 0;
    int64_t tablets_succeeded = 0;
    int64_t tablets_scheduled = 0; // Tablets added to compaction queue
    int64_t result_files_cleaned = 0;
};

// Handles autonomous compaction requests
//
// Autonomous compaction workflow:
// 1. Trigger async compaction for tablets that need it (add to scheduling queue)
// 2. Collect completed compaction results from BE local storage
// 3. Build TxnLogs and return to FE via response
//
// Key difference from original compaction:
// - Original: FE sends request -> BE synchronously executes compaction -> returns result
// - Autonomous: FE sends request -> BE triggers async compaction + collects completed results -> returns
//
// Commit and Publish are still handled by FE (commitCompaction + PublishVersionDaemon)
class AutonomousCompactionHandler {
public:
    explicit AutonomousCompactionHandler(TabletManager* tablet_mgr);
    ~AutonomousCompactionHandler();

    // Process autonomous compaction request:
    // 1. Trigger async compaction for tablets that need it
    // 2. Collect completed compaction results and build TxnLogs
    // 3. Return TxnLogs to FE for commit
    // Note: FE is responsible for commit (commitCompaction) and PublishVersionDaemon handles publish
    Status process_request(const CompactRequest* request, CompactResponse* response);

    // Get statistics from the last process_request call
    const AutonomousCompactionStats& last_stats() const { return _last_stats; }

private:
    // Trigger compaction scheduling for tablets that need it
    void trigger_compaction_scheduling(const std::vector<int64_t>& tablet_ids);

    // Check which tablets need compaction (have high score or pending results)
    std::unordered_set<int64_t> identify_tablets_needing_compaction(const std::vector<int64_t>& tablet_ids);

    // Load and filter compaction results for a tablet
    StatusOr<std::vector<CompactionResultPB>> load_tablet_results(int64_t tablet_id, int64_t max_version);

    // Build TxnLog from compaction results
    StatusOr<TxnLogPB> build_txn_log(int64_t tablet_id, const std::vector<CompactionResultPB>& results);

    // Clean up result files for a tablet
    void cleanup_tablet_results(int64_t tablet_id, const std::vector<std::string>& file_paths);

    TabletManager* _tablet_mgr;
    std::unique_ptr<CompactionResultManager> _result_mgr;
    AutonomousCompactionStats _last_stats;
};

} // namespace starrocks::lake
