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
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

class TabletManager;

// Information about a recovered compaction result
struct RecoveredResultInfo {
    std::string file_path;
    CompactionResultPB result;
    bool is_valid = false;
    std::string invalid_reason;
};

// Recovery statistics
struct RecoveryStats {
    int64_t total_files_scanned = 0;
    int64_t valid_results = 0;
    int64_t invalid_results_cleaned = 0;
    int64_t parse_errors = 0;
    int64_t validation_errors = 0;
    std::unordered_set<int64_t> recovered_tablet_ids;
};

// Manages local persistence of autonomous compaction results.
// Results are stored in: {storage_root}/lake/compaction_results/tablet_{id}_result_{timestamp}_{random}.pb
//
// Key features for exception recovery (Section 6.1.1 of design doc):
// - recover_on_startup(): Scans local result directory after BE restart
// - Validates effectiveness: base_version <= visible_version and input rowsets still exist
// - Invalid results are cleaned up, valid ones are retained
class CompactionResultManager {
public:
    CompactionResultManager() = default;
    ~CompactionResultManager() = default;

    // ============== Startup Recovery (Mechanism 1) ==============
    
    // Recover compaction results after BE restart
    // This implements Mechanism 1: Local Result Recovery
    // - Scans the local result directory
    // - Loads all CompactionResultPB files
    // - Validates effectiveness (base_version <= visible_version, input rowsets exist)
    // - Valid results are retained, invalid ones are cleaned up
    // Returns recovery statistics
    StatusOr<RecoveryStats> recover_on_startup(TabletManager* tablet_mgr);

    // ============== Basic Operations ==============

    // Save a compaction result to local disk
    // Returns the file path where the result was saved
    StatusOr<std::string> save_result(const CompactionResultPB& result);

    // Load all compaction results for a specific tablet
    // Filters out results with base_version > max_version if max_version >= 0
    StatusOr<std::vector<CompactionResultPB>> load_results(int64_t tablet_id, int64_t max_version = -1);

    // Delete result file by path
    Status delete_result(const std::string& file_path);

    // Delete all result files for a tablet
    Status delete_tablet_results(int64_t tablet_id);

    // List all result file paths for a tablet
    StatusOr<std::vector<std::string>> list_result_files(int64_t tablet_id);

    // ============== Validation ==============

    // Validate a single compaction result
    // Checks:
    // 1. base_version <= visible_version
    // 2. input rowsets still exist in the tablet
    // Returns true if valid, false otherwise with reason
    StatusOr<bool> validate_result(TabletManager* tablet_mgr, const CompactionResultPB& result, 
                                    std::string* invalid_reason = nullptr);

    // Load and validate results for a tablet, filtering out invalid ones
    // Also cleans up invalid result files if cleanup_invalid is true
    StatusOr<std::vector<CompactionResultPB>> load_and_validate_results(
            TabletManager* tablet_mgr, int64_t tablet_id, int64_t max_version = -1,
            bool cleanup_invalid = true);

    // ============== Query Methods ==============

    // Check if there are any pending results for a tablet
    bool has_pending_results(int64_t tablet_id);

    // Get the set of tablet IDs that have pending results (after recovery)
    std::unordered_set<int64_t> get_tablets_with_pending_results();

    // Get the compaction results directory for a specific storage root
    static std::string get_results_dir(const std::string& storage_root);

private:
    // Generate a unique filename for a compaction result
    static std::string generate_result_filename(int64_t tablet_id);

    // Get all storage roots configured in the system
    static std::vector<std::string> get_storage_roots();

    // Scan all result files in all storage roots
    StatusOr<std::vector<std::pair<std::string, int64_t>>> scan_all_result_files();

    // Extract tablet ID from result filename
    static StatusOr<int64_t> extract_tablet_id_from_filename(const std::string& filename);

    // Cache of tablet IDs with pending results (populated during recovery)
    std::unordered_set<int64_t> _tablets_with_pending_results;
    mutable std::mutex _pending_results_mutex;
};

} // namespace starrocks::lake


