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

#include "storage/lake/autonomous_compaction_handler.h"

#include <unordered_map>
#include <unordered_set>

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_result_manager.h"
#include "storage/lake/lake_compaction_manager.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

AutonomousCompactionHandler::AutonomousCompactionHandler(TabletManager* tablet_mgr)
        : _tablet_mgr(tablet_mgr), _result_mgr(std::make_unique<CompactionResultManager>()) {}

AutonomousCompactionHandler::~AutonomousCompactionHandler() = default;

Status AutonomousCompactionHandler::process_request(const CompactRequest* request, CompactResponse* response) {
    if (!request->autonomous_compaction()) {
        return Status::InvalidArgument("Not an autonomous compaction request");
    }

    // Reset statistics
    _last_stats = AutonomousCompactionStats{};
    _last_stats.tablets_processed = request->tablet_ids_size();

    LOG(INFO) << "Processing autonomous compaction request for " << request->tablet_ids_size()
              << " tablets, txn_id=" << request->txn_id() << ", version=" << request->version()
              << ", visible_version=" << request->visible_version();

    int64_t visible_version = request->has_visible_version() ? request->visible_version() : request->version();
    std::vector<int64_t> tablet_ids(request->tablet_ids().begin(), request->tablet_ids().end());

    // Step 1: Trigger async compaction for tablets that need it
    trigger_compaction_scheduling(tablet_ids);

    // Step 2: Collect completed compaction results
    std::unordered_map<int64_t, std::vector<CompactionResultPB>> tablet_results;
    std::unordered_map<int64_t, std::vector<std::string>> tablet_result_files;

    for (int64_t tablet_id : tablet_ids) {
        auto results_or = load_tablet_results(tablet_id, visible_version);
        if (!results_or.ok()) {
            LOG(WARNING) << "Failed to load results for tablet " << tablet_id << ": " << results_or.status();
            continue;
        }

        auto results = results_or.value();
        if (!results.empty()) {
            tablet_results[tablet_id] = std::move(results);

            auto files_or = _result_mgr->list_result_files(tablet_id);
            if (files_or.ok()) {
                tablet_result_files[tablet_id] = std::move(files_or.value());
            }
        }
    }

    _last_stats.tablets_with_compaction = tablet_results.size();
    LOG(INFO) << "Loaded compaction results for " << tablet_results.size() << " tablets out of "
              << request->tablet_ids_size() << " total tablets";

    // Step 3: Build TxnLogs and add to response
    int success_count = 0;
    for (const auto& [tablet_id, results] : tablet_results) {
        auto txn_log_or = build_txn_log(tablet_id, results);
        if (!txn_log_or.ok()) {
            LOG(WARNING) << "Failed to build TxnLog for tablet " << tablet_id << ": " << txn_log_or.status();
            response->add_failed_tablets(tablet_id);
            continue;
        }

        auto* txn_log = response->add_txn_logs();
        *txn_log = std::move(txn_log_or.value());
        success_count++;

        // Clean up result files
        if (tablet_result_files.count(tablet_id) > 0) {
            cleanup_tablet_results(tablet_id, tablet_result_files[tablet_id]);
            _last_stats.result_files_cleaned += tablet_result_files[tablet_id].size();
        }
    }

    _last_stats.tablets_succeeded = success_count;

    LOG(INFO) << "Autonomous compaction completed: scheduled=" << _last_stats.tablets_scheduled
              << ", collected=" << tablet_results.size() << ", succeeded=" << success_count
              << ", result_files_cleaned=" << _last_stats.result_files_cleaned;

    Status::OK().to_protobuf(response->mutable_status());
    return Status::OK();
}

void AutonomousCompactionHandler::trigger_compaction_scheduling(const std::vector<int64_t>& tablet_ids) {
    if (!config::enable_lake_autonomous_compaction) {
        VLOG(2) << "Autonomous compaction disabled, skipping scheduling trigger";
        return;
    }

    auto* compaction_mgr = LakeCompactionManager::instance();
    if (compaction_mgr == nullptr) {
        LOG(WARNING) << "LakeCompactionManager not initialized, cannot trigger scheduling";
        return;
    }

    auto tablets_needing_compaction = identify_tablets_needing_compaction(tablet_ids);

    int scheduled_count = 0;
    for (int64_t tablet_id : tablets_needing_compaction) {
        compaction_mgr->update_tablet_async(tablet_id);
        scheduled_count++;
    }

    _last_stats.tablets_scheduled = scheduled_count;
    LOG(INFO) << "Triggered compaction scheduling for " << scheduled_count << " tablets out of " << tablet_ids.size();
}

std::unordered_set<int64_t> AutonomousCompactionHandler::identify_tablets_needing_compaction(
        const std::vector<int64_t>& tablet_ids) {
    std::unordered_set<int64_t> result;

    for (int64_t tablet_id : tablet_ids) {
        bool needs_compaction = false;

        // Check 1: Has pending results from previous compaction
        if (_result_mgr->has_pending_results(tablet_id)) {
            needs_compaction = true;
            VLOG(2) << "Tablet " << tablet_id << " has pending compaction results";
        }

        // Check 2: High compaction score
        if (!needs_compaction) {
            auto metadata_iter_or = _tablet_mgr->list_tablet_metadata(tablet_id);
            if (metadata_iter_or.ok()) {
                TabletMetadataPtr metadata = nullptr;
                int64_t max_version = 0;
                auto& metadata_iter = metadata_iter_or.value();
                while (metadata_iter.has_next()) {
                    auto meta_or = metadata_iter.next();
                    if (meta_or.ok() && meta_or.value()->version() > max_version) {
                        max_version = meta_or.value()->version();
                        metadata = meta_or.value();
                    }
                }
                if (metadata) {
                    double score = compaction_score(_tablet_mgr, metadata);
                    if (score >= config::lake_compaction_score_threshold) {
                        needs_compaction = true;
                        VLOG(2) << "Tablet " << tablet_id << " has high compaction score: " << score;
                    }
                }
            }
        }

        if (needs_compaction) {
            result.insert(tablet_id);
        }
    }

    return result;
}

StatusOr<std::vector<CompactionResultPB>> AutonomousCompactionHandler::load_tablet_results(int64_t tablet_id,
                                                                                           int64_t max_version) {
    ASSIGN_OR_RETURN(auto results, _result_mgr->load_and_validate_results(_tablet_mgr, tablet_id, max_version, true));

    if (results.empty()) {
        VLOG(2) << "No valid compaction results found for tablet " << tablet_id;
    } else {
        LOG(INFO) << "Loaded " << results.size() << " valid compaction results for tablet " << tablet_id
                  << ", max_version=" << max_version;
    }

    return results;
}

StatusOr<TxnLogPB> AutonomousCompactionHandler::build_txn_log(int64_t tablet_id,
                                                              const std::vector<CompactionResultPB>& results) {
    if (results.empty()) {
        return Status::InvalidArgument("No results to build TxnLog");
    }

    TxnLogPB txn_log;
    txn_log.set_tablet_id(tablet_id);

    auto* op_compaction = txn_log.mutable_op_compaction();

    // Collect all input rowsets (deduplicate)
    std::unordered_set<uint32_t> input_rowset_set;
    for (const auto& result : results) {
        for (uint32_t rid : result.input_rowset_ids()) {
            input_rowset_set.insert(rid);
        }
    }

    for (uint32_t rid : input_rowset_set) {
        op_compaction->add_input_rowsets(rid);
    }

    // Set output rowset (OpCompaction only supports single output)
    // Use the first result's output_rowset
    for (const auto& result : results) {
        if (result.has_output_rowset()) {
            *op_compaction->mutable_output_rowset() = result.output_rowset();
            break; // Only one output rowset is supported
        }
    }

    // Handle SSTable metadata for primary key tables
    for (const auto& result : results) {
        for (const auto& input_sst : result.input_sstables()) {
            op_compaction->add_input_sstables()->CopyFrom(input_sst);
        }
        // output_sstable is singular in OpCompaction
        if (result.output_sstables_size() > 0) {
            *op_compaction->mutable_output_sstable() = result.output_sstables(0);
        }
    }

    // Set base version for conflict checking
    if (results[0].has_base_version()) {
        op_compaction->set_compact_version(results[0].base_version());
    }

    LOG(INFO) << "Built TxnLog for tablet " << tablet_id << ": " << input_rowset_set.size() << " input rowsets, "
              << results.size() << " output rowsets";

    return txn_log;
}

void AutonomousCompactionHandler::cleanup_tablet_results(int64_t tablet_id,
                                                         const std::vector<std::string>& file_paths) {
    int deleted_count = 0;
    for (const auto& file_path : file_paths) {
        auto st = _result_mgr->delete_result(file_path);
        if (st.ok()) {
            deleted_count++;
        }
    }

    LOG(INFO) << "Cleaned up " << deleted_count << " result files for tablet " << tablet_id;
}

} // namespace starrocks::lake
