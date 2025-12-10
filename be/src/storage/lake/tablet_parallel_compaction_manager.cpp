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

#include "storage/lake/tablet_parallel_compaction_manager.h"

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/memtable_flush_executor.h"
#include "storage/storage_engine.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace starrocks::lake {

TabletParallelCompactionManager::TabletParallelCompactionManager(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}

TabletParallelCompactionManager::~TabletParallelCompactionManager() = default;

StatusOr<int> TabletParallelCompactionManager::create_parallel_tasks(
        int64_t tablet_id, int64_t txn_id, int64_t version, const TabletParallelConfig& config,
        std::shared_ptr<CompactionTaskCallback> callback, bool force_base_compaction, ThreadPool* thread_pool,
        int64_t table_id, int64_t partition_id) {
    // Validate configuration
    int32_t max_parallel = config.has_max_parallel_per_tablet() ? config.max_parallel_per_tablet()
                                                                : config::lake_compaction_max_parallel_per_tablet;
    int64_t max_bytes = config.has_max_bytes_per_subtask() ? config.max_bytes_per_subtask()
                                                           : config::lake_compaction_max_bytes_per_subtask;

    if (max_parallel <= 0) {
        max_parallel = 1;
    }
    if (max_bytes <= 0) {
        max_bytes = config::lake_compaction_max_bytes_per_subtask;
    }

    // Create tablet state
    std::string state_key = make_state_key(tablet_id, txn_id);
    std::unique_ptr<TabletParallelState> state;
    {
        std::lock_guard<std::mutex> lock(_states_mutex);
        auto it = _tablet_states.find(state_key);
        if (it != _tablet_states.end()) {
            return Status::AlreadyExist(
                    strings::Substitute("Parallel compaction already exists for tablet $0 txn $1", tablet_id, txn_id));
        }
        state = std::make_unique<TabletParallelState>();
        state->tablet_id = tablet_id;
        state->txn_id = txn_id;
        state->version = version;
        state->table_id = table_id;
        state->partition_id = partition_id;
        state->max_parallel = max_parallel;
        state->max_bytes_per_subtask = max_bytes;
        state->callback = callback;
        _tablet_states[state_key] = std::move(state);
    }

    // Get the state pointer for use
    TabletParallelState* state_ptr = nullptr;
    {
        std::lock_guard<std::mutex> lock(_states_mutex);
        state_ptr = _tablet_states[state_key].get();
    }

    int subtasks_created = 0;
    std::unordered_set<uint32_t> all_selected_rowsets;

    // Create subtasks
    while (subtasks_created < max_parallel) {
        // Pick non-conflicting rowsets
        auto rowsets_or =
                pick_non_conflicting_rowsets(tablet_id, version, max_bytes, all_selected_rowsets, force_base_compaction);
        if (!rowsets_or.ok()) {
            if (subtasks_created == 0) {
                // Clean up state if no subtasks were created
                cleanup_tablet(tablet_id, txn_id);
                return rowsets_or.status();
            }
            // Otherwise, we have at least one subtask, so just break
            break;
        }

        auto rowsets = std::move(rowsets_or.value());
        if (rowsets.size() < 2) {
            // Not enough rowsets to compact
            break;
        }

        // Collect rowset IDs and calculate input bytes
        std::vector<uint32_t> rowset_ids;
        int64_t input_bytes = 0;
        for (const auto& rowset : rowsets) {
            rowset_ids.push_back(rowset->id());
            input_bytes += rowset->data_size();
            all_selected_rowsets.insert(rowset->id());
        }

        // Mark rowsets as compacting
        {
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            mark_rowsets_compacting(state_ptr, rowset_ids);

            // Create subtask info
            int32_t subtask_id = state_ptr->next_subtask_id++;
            SubtaskInfo info;
            info.subtask_id = subtask_id;
            info.input_rowset_ids = rowset_ids;
            info.input_bytes = input_bytes;
            info.start_time = ::time(nullptr);
            state_ptr->running_subtasks[subtask_id] = std::move(info);
            state_ptr->total_subtasks_created++;
        }

        int32_t subtask_id = subtasks_created;
        _running_subtasks++;

        // Submit task to thread pool
        auto submit_st =
                thread_pool->submit_func([this, tablet_id, txn_id, subtask_id, rowsets = std::move(rowsets), version,
                                          force_base_compaction]() mutable {
                    execute_subtask(tablet_id, txn_id, subtask_id, std::move(rowsets), version, force_base_compaction);
                });

        if (!submit_st.ok()) {
            // Failed to submit, revert state changes
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            unmark_rowsets_compacting(state_ptr, rowset_ids);
            state_ptr->running_subtasks.erase(subtask_id);
            state_ptr->total_subtasks_created--;
            _running_subtasks--;

            if (subtasks_created == 0) {
                cleanup_tablet(tablet_id, txn_id);
                return submit_st;
            }
            break;
        }

        subtasks_created++;

        LOG(INFO) << "Created parallel compaction subtask " << subtask_id << " for tablet " << tablet_id
                  << ", txn_id=" << txn_id << ", rowsets=" << rowset_ids.size() << ", input_bytes=" << input_bytes;
    }

    if (subtasks_created == 0) {
        cleanup_tablet(tablet_id, txn_id);
        return Status::NotFound(strings::Substitute("No rowsets to compact for tablet $0", tablet_id));
    }

    LOG(INFO) << "Created " << subtasks_created << " parallel compaction subtasks for tablet " << tablet_id
              << ", txn_id=" << txn_id << ", max_parallel=" << max_parallel;

    return subtasks_created;
}

TabletParallelState* TabletParallelCompactionManager::get_tablet_state(int64_t tablet_id, int64_t txn_id) {
    std::string state_key = make_state_key(tablet_id, txn_id);
    std::lock_guard<std::mutex> lock(_states_mutex);
    auto it = _tablet_states.find(state_key);
    if (it == _tablet_states.end()) {
        return nullptr;
    }
    return it->second.get();
}

void TabletParallelCompactionManager::on_subtask_complete(int64_t tablet_id, int64_t txn_id, int32_t subtask_id,
                                                          std::unique_ptr<CompactionTaskContext> context) {
    std::string state_key = make_state_key(tablet_id, txn_id);

    TabletParallelState* state_ptr = nullptr;
    {
        std::lock_guard<std::mutex> lock(_states_mutex);
        auto it = _tablet_states.find(state_key);
        if (it == _tablet_states.end()) {
            LOG(WARNING) << "Tablet state not found for subtask completion, tablet=" << tablet_id
                         << ", txn_id=" << txn_id << ", subtask_id=" << subtask_id;
            return;
        }
        state_ptr = it->second.get();
    }

    std::shared_ptr<CompactionTaskCallback> callback;
    bool all_complete = false;

    {
        std::lock_guard<std::mutex> lock(state_ptr->mutex);

        auto it = state_ptr->running_subtasks.find(subtask_id);
        if (it == state_ptr->running_subtasks.end()) {
            LOG(WARNING) << "Subtask not found, tablet=" << tablet_id << ", txn_id=" << txn_id
                         << ", subtask_id=" << subtask_id;
            return;
        }

        // Unmark rowsets
        unmark_rowsets_compacting(state_ptr, it->second.input_rowset_ids);

        // Move to completed
        state_ptr->completed_subtasks.push_back(std::move(context));
        state_ptr->running_subtasks.erase(it);

        _running_subtasks--;
        _completed_subtasks++;

        all_complete = state_ptr->is_complete();
        callback = state_ptr->callback;

        LOG(INFO) << "Parallel compaction subtask completed, tablet=" << tablet_id << ", txn_id=" << txn_id
                  << ", subtask_id=" << subtask_id << ", remaining=" << state_ptr->running_subtasks.size()
                  << ", completed=" << state_ptr->completed_subtasks.size();
    }

    // If all subtasks are complete, notify the callback
    if (all_complete && callback) {
        // Build merged context
        // Note: skip_write_txnlog must be true so that merged txn_log is added to response
        auto merged_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, state_ptr->version,
                                                                      false /* force_base_compaction */,
                                                                      true /* skip_write_txnlog */, callback);

        // Merge TxnLogs
        auto merged_txn_log_or = get_merged_txn_log(tablet_id, txn_id);
        if (merged_txn_log_or.ok()) {
            merged_context->txn_log = std::make_unique<TxnLogPB>(std::move(merged_txn_log_or.value()));
        } else {
            merged_context->status = merged_txn_log_or.status();
        }

        // Set status based on subtask statuses
        {
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            for (const auto& subtask_ctx : state_ptr->completed_subtasks) {
                if (!subtask_ctx->status.ok()) {
                    merged_context->status.update(subtask_ctx->status);
                }
            }
        }

        callback->finish_task(std::move(merged_context));

        // Cleanup
        cleanup_tablet(tablet_id, txn_id);
    }
}

bool TabletParallelCompactionManager::is_tablet_complete(int64_t tablet_id, int64_t txn_id) {
    auto* state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        return true;
    }
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->is_complete();
}

void TabletParallelCompactionManager::cleanup_tablet(int64_t tablet_id, int64_t txn_id) {
    std::string state_key = make_state_key(tablet_id, txn_id);
    std::lock_guard<std::mutex> lock(_states_mutex);
    _tablet_states.erase(state_key);
    LOG(INFO) << "Cleaned up parallel compaction state for tablet " << tablet_id << ", txn_id=" << txn_id;
}

StatusOr<TxnLogPB> TabletParallelCompactionManager::get_merged_txn_log(int64_t tablet_id, int64_t txn_id) {
    auto* state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        return Status::NotFound(
                strings::Substitute("Tablet state not found for tablet $0 txn $1", tablet_id, txn_id));
    }

    TxnLogPB merged_log;
    merged_log.set_tablet_id(tablet_id);
    merged_log.set_txn_id(txn_id);

    auto* op_compaction = merged_log.mutable_op_compaction();

    std::lock_guard<std::mutex> lock(state->mutex);

    // Collect all input rowsets (deduplicate)
    std::unordered_set<uint32_t> input_rowset_set;
    for (const auto& ctx : state->completed_subtasks) {
        if (ctx->txn_log != nullptr && ctx->txn_log->has_op_compaction()) {
            for (uint32_t rid : ctx->txn_log->op_compaction().input_rowsets()) {
                input_rowset_set.insert(rid);
            }
        }
    }

    // Add input rowsets to merged log
    for (uint32_t rid : input_rowset_set) {
        op_compaction->add_input_rowsets(rid);
    }

    // Collect all output rowsets
    for (const auto& ctx : state->completed_subtasks) {
        if (ctx->txn_log != nullptr && ctx->txn_log->has_op_compaction()) {
            const auto& sub_compaction = ctx->txn_log->op_compaction();
            if (sub_compaction.has_output_rowset()) {
                op_compaction->mutable_output_rowset()->CopyFrom(sub_compaction.output_rowset());
            }

            // Copy SSTable metadata for primary key tables
            for (const auto& input_sst : sub_compaction.input_sstables()) {
                op_compaction->add_input_sstables()->CopyFrom(input_sst);
            }
            if (sub_compaction.has_output_sstable()) {
                op_compaction->mutable_output_sstable()->CopyFrom(sub_compaction.output_sstable());
            }
        }
    }

    // Set compact_version (all subtasks should use the same base version)
    if (!state->completed_subtasks.empty() && state->completed_subtasks[0]->txn_log != nullptr &&
        state->completed_subtasks[0]->txn_log->has_op_compaction()) {
        const auto& first_compaction = state->completed_subtasks[0]->txn_log->op_compaction();
        if (first_compaction.has_compact_version()) {
            op_compaction->set_compact_version(first_compaction.compact_version());
        }
    }

    LOG(INFO) << "Merged TxnLog for tablet " << tablet_id << ", txn_id=" << txn_id
              << ", input_rowsets=" << input_rowset_set.size()
              << ", subtasks=" << state->completed_subtasks.size();

    return merged_log;
}

StatusOr<std::vector<RowsetPtr>> TabletParallelCompactionManager::pick_non_conflicting_rowsets(
        int64_t tablet_id, int64_t version, int64_t max_bytes, const std::unordered_set<uint32_t>& exclude_rowsets,
        bool force_base_compaction) {
    // Get tablet metadata
    ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id, version));
    auto metadata = tablet.metadata();

    if (!metadata) {
        return Status::NotFound(strings::Substitute("Tablet $0 metadata not found", tablet_id));
    }

    // Create compaction policy
    ASSIGN_OR_RETURN(auto policy, CompactionPolicy::create(_tablet_mgr, metadata, force_base_compaction));

    // Use the existing pick_rowsets_with_limit method
    return policy->pick_rowsets_with_limit(max_bytes, exclude_rowsets);
}

void TabletParallelCompactionManager::mark_rowsets_compacting(TabletParallelState* state,
                                                              const std::vector<uint32_t>& rowset_ids) {
    // Assumes state->mutex is already held
    for (uint32_t rid : rowset_ids) {
        state->compacting_rowsets.insert(rid);
    }
}

void TabletParallelCompactionManager::unmark_rowsets_compacting(TabletParallelState* state,
                                                                const std::vector<uint32_t>& rowset_ids) {
    // Assumes state->mutex is already held
    for (uint32_t rid : rowset_ids) {
        state->compacting_rowsets.erase(rid);
    }
}

void TabletParallelCompactionManager::execute_subtask(int64_t tablet_id, int64_t txn_id, int32_t subtask_id,
                                                      std::vector<RowsetPtr> input_rowsets, int64_t version,
                                                      bool force_base_compaction) {
    LOG(INFO) << "Executing parallel compaction subtask " << subtask_id << " for tablet " << tablet_id
              << ", txn_id=" << txn_id << ", rowsets=" << input_rowsets.size();

    // Get tablet state and callback
    auto* state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        LOG(WARNING) << "Tablet state not found during subtask execution, tablet=" << tablet_id
                     << ", txn_id=" << txn_id << ", subtask_id=" << subtask_id;
        return;
    }

    // Create compaction context for this subtask
    // Note: skip_write_txnlog must be true for parallel compaction, so that txn_log is saved to context
    // and can be merged later in on_subtask_complete
    auto context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, force_base_compaction,
                                                           true /* skip_write_txnlog */, state->callback,
                                                           state->table_id, state->partition_id);

    auto start_time = ::time(nullptr);
    context->start_time.store(start_time, std::memory_order_relaxed);

    // Create compaction task using pre-selected rowsets
    auto compaction_task_or = _tablet_mgr->compact(context.get(), std::move(input_rowsets));
    if (!compaction_task_or.ok()) {
        LOG(WARNING) << "Failed to create compaction task for tablet " << tablet_id << " subtask " << subtask_id << ": "
                     << compaction_task_or.status();
        context->status = compaction_task_or.status();
        on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
        return;
    }

    auto compaction_task = compaction_task_or.value();

    // Execute compaction
    auto cancel_func = [state]() {
        // Check if tablet state still exists
        return Status::OK();
    };

    ThreadPool* flush_pool = nullptr;
    if (config::lake_enable_compaction_async_write) {
        flush_pool = StorageEngine::instance()->lake_memtable_flush_executor()->get_thread_pool();
    }

    auto exec_st = compaction_task->execute(cancel_func, flush_pool);

    auto finish_time = std::max<int64_t>(::time(nullptr), start_time);
    auto cost = finish_time - start_time;

    if (!exec_st.ok()) {
        LOG(WARNING) << "Compaction subtask " << subtask_id << " failed for tablet " << tablet_id << ": " << exec_st
                     << ", cost=" << cost << "s";
        context->status = exec_st;
    } else {
        LOG(INFO) << "Compaction subtask " << subtask_id << " completed for tablet " << tablet_id
                  << ", cost=" << cost << "s";
    }

    context->finish_time.store(finish_time, std::memory_order_release);

    // Notify completion
    on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
}

} // namespace starrocks::lake
