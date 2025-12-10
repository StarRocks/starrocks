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

#include "storage/lake/lake_compaction_manager.h"

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_result_manager.h"
#include "storage/lake/tablet_manager.h"
#include "util/defer_op.h"

namespace starrocks::lake {

// Singleton instance
static LakeCompactionManager* g_lake_compaction_manager = nullptr;
static std::once_flag g_init_once;
static std::mutex g_mgr_mutex;

Status LakeCompactionManager::create_instance(TabletManager* tablet_mgr) {
    std::lock_guard<std::mutex> lock(g_mgr_mutex);
    if (g_lake_compaction_manager != nullptr) {
        return Status::OK();
    }

    if (tablet_mgr == nullptr) {
        return Status::InvalidArgument("tablet_mgr cannot be null");
    }

    g_lake_compaction_manager = new LakeCompactionManager(tablet_mgr);
    return g_lake_compaction_manager->init();
}

LakeCompactionManager* LakeCompactionManager::instance() {
    if (g_lake_compaction_manager == nullptr) {
        LOG(FATAL) << "LakeCompactionManager not initialized. "
                   << "Call create_instance() first during system startup.";
    }
    return g_lake_compaction_manager;
}

LakeCompactionManager::LakeCompactionManager(TabletManager* tablet_mgr)
        : _tablet_mgr(tablet_mgr), _result_mgr(std::make_unique<CompactionResultManager>()) {}

LakeCompactionManager::~LakeCompactionManager() {
    stop();
}

Status LakeCompactionManager::init() {
    if (_stopped.load()) {
        return Status::InternalError("LakeCompactionManager already stopped");
    }

    if (_tablet_mgr == nullptr) {
        return Status::InvalidArgument("_tablet_mgr cannot be null");
    }

    // Validate configuration parameters (P8 fix)
    if (config::lake_compaction_max_concurrent_tasks <= 0) {
        return Status::InvalidArgument(
            fmt::format("Invalid lake_compaction_max_concurrent_tasks: {}",
                       config::lake_compaction_max_concurrent_tasks));
    }

    if (config::lake_compaction_max_tasks_per_tablet <= 0) {
        return Status::InvalidArgument(
            fmt::format("Invalid lake_compaction_max_tasks_per_tablet: {}",
                       config::lake_compaction_max_tasks_per_tablet));
    }

    if (config::lake_compaction_max_bytes_per_task <= 0) {
        return Status::InvalidArgument(
            fmt::format("Invalid lake_compaction_max_bytes_per_task: {}",
                       config::lake_compaction_max_bytes_per_task));
    }

    if (config::lake_compaction_score_threshold < 0) {
        return Status::InvalidArgument(
            fmt::format("Invalid lake_compaction_score_threshold: {}",
                       config::lake_compaction_score_threshold));
    }

    // Create thread pool for executing compaction tasks
    RETURN_IF_ERROR(ThreadPoolBuilder("lake_auto_compact")
                            .set_min_threads(0)
                            .set_max_threads(config::lake_compaction_max_concurrent_tasks)
                            .set_max_queue_size(1000)
                            .build(&_thread_pool));

    // Start background scheduler thread (P2 fix - correct method name)
    _schedule_thread = std::make_unique<std::thread>(
        [this]() { this->_schedule_thread_func(); }
    );

    LOG(INFO) << "LakeCompactionManager initialized with max_concurrent_tasks="
              << config::lake_compaction_max_concurrent_tasks;

    return Status::OK();
}

void LakeCompactionManager::stop() {
    if (_stopped.exchange(true)) {
        return;
    }

    LOG(INFO) << "Stopping LakeCompactionManager";

    // Wake up scheduler thread
    _queue_cv.notify_all();

    // Wait for scheduler thread to exit
    if (_schedule_thread && _schedule_thread->joinable()) {
        _schedule_thread->join();
    }

    // Shutdown thread pool
    if (_thread_pool) {
        _thread_pool->shutdown();
    }

    LOG(INFO) << "LakeCompactionManager stopped";
}

void LakeCompactionManager::update_tablet_async(int64_t tablet_id) {
    if (_stopped.load()) {
        return;
    }

    // Calculate compaction score
    auto score_or = _calculate_score(tablet_id);
    if (!score_or.ok()) {
        VLOG(2) << "Failed to calculate score for tablet " << tablet_id << ": " << score_or.status();
        return;
    }

    double score = score_or.value();
    
    // Only add to queue if score exceeds threshold
    if (score < config::lake_compaction_score_threshold) {
        VLOG(2) << "Tablet " << tablet_id << " score " << score << " below threshold "
                << config::lake_compaction_score_threshold;
        return;
    }

    _update_tablet_state(tablet_id, score);
}

StatusOr<double> LakeCompactionManager::_calculate_score(int64_t tablet_id) {
    // Get tablet metadata
    ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    auto metadata = tablet.metadata();
    
    if (!metadata) {
        return Status::NotFound(strings::Substitute("Tablet $0 metadata not found", tablet_id));
    }

    // Calculate compaction score
    double score = compaction_score(_tablet_mgr, metadata);
    
    return score;
}

void LakeCompactionManager::_update_tablet_state(int64_t tablet_id, double score) {
    // P6 fix: Separate lock operations to avoid potential deadlock
    // Step 1: Update state
    TabletCompactionInfo info;
    bool should_add_to_queue = false;
    {
        std::lock_guard<std::mutex> state_lock(_state_mutex);

        auto& state = _tablet_states[tablet_id];
        state.tablet_id = tablet_id;
        state.last_score = score;
        state.last_update_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count();

        if (!state.in_queue) {
            info.tablet_id = tablet_id;
            info.score = score;
            info.last_update_time_ms = state.last_update_time_ms;
            should_add_to_queue = true;
        }
    }

    // Step 2: Add to queue if needed (outside state lock)
    if (should_add_to_queue) {
        std::lock_guard<std::mutex> queue_lock(_queue_mutex);
        _tablet_queue.push(info);

        // Update in_queue flag
        {
            std::lock_guard<std::mutex> state_lock(_state_mutex);
            auto it = _tablet_states.find(tablet_id);
            if (it != _tablet_states.end()) {
                it->second.in_queue = true;
            }
        }

        VLOG(2) << "Added tablet " << tablet_id << " to compaction queue with score " << info.score;

        // Wake up scheduler thread
        _queue_cv.notify_one();
    }
}

int64_t LakeCompactionManager::queue_size() const {
    std::lock_guard<std::mutex> lock(_queue_mutex);
    return _tablet_queue.size();
}

bool LakeCompactionManager::_can_schedule_tablet(const TabletCompactionState& state) {
    // Check global concurrent task limit
    if (_running_tasks.load() >= config::lake_compaction_max_concurrent_tasks) {
        return false;
    }

    // Check per-tablet task limit
    if (state.running_tasks >= config::lake_compaction_max_tasks_per_tablet) {
        return false;
    }

    return true;
}

void LakeCompactionManager::_schedule_thread_func() {
    LOG(INFO) << "LakeCompactionManager scheduler thread started";

    while (!_stopped.load()) {
        std::unique_lock<std::mutex> lock(_queue_mutex);

        // Wait for tablets in queue or stop signal
        _queue_cv.wait_for(lock, std::chrono::seconds(1), [this]() { return !_tablet_queue.empty() || _stopped.load(); });

        if (_stopped.load()) {
            break;
        }

        if (_tablet_queue.empty()) {
            continue;
        }

        // Get highest priority tablet
        auto info = _tablet_queue.top();
        _tablet_queue.pop();
        lock.unlock();

        // Mark as not in queue
        {
            std::lock_guard<std::mutex> state_lock(_state_mutex);
            auto it = _tablet_states.find(info.tablet_id);
            if (it != _tablet_states.end()) {
                it->second.in_queue = false;
            }
        }

        // Try to schedule compaction for this tablet
        auto st = _schedule_tablet(info.tablet_id);
        if (!st.ok()) {
            VLOG(2) << "Failed to schedule tablet " << info.tablet_id << ": " << st;
            
            // Re-add to queue if it's a temporary failure
            if (st.is_resource_busy()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                update_tablet_async(info.tablet_id);
            }
        }
    }

    LOG(INFO) << "LakeCompactionManager scheduler thread exited";
}

Status LakeCompactionManager::_schedule_tablet(int64_t tablet_id) {
    // Get tablet state
    std::unique_lock<std::mutex> state_lock(_state_mutex);
    auto it = _tablet_states.find(tablet_id);
    if (it == _tablet_states.end()) {
        return Status::NotFound(strings::Substitute("Tablet $0 state not found", tablet_id));
    }

    auto& state = it->second;

    // Check if we can schedule more tasks for this tablet
    if (!_can_schedule_tablet(state)) {
        return Status::ResourceBusy(strings::Substitute("Tablet $0 already at max concurrent tasks", tablet_id));
    }

    // Get tablet and metadata
    ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    auto metadata = tablet.metadata();

    if (!metadata) {
        return Status::NotFound(strings::Substitute("Tablet $0 metadata not found", tablet_id));
    }

    // Create compaction policy
    ASSIGN_OR_RETURN(auto policy, CompactionPolicy::create(_tablet_mgr, metadata, false));

    // Pick rowsets with limit and exclusion
    ASSIGN_OR_RETURN(auto input_rowsets,
                     policy->pick_rowsets_with_limit(config::lake_compaction_max_bytes_per_task, state.compacting_rowsets));

    if (input_rowsets.empty()) {
        VLOG(2) << "No rowsets to compact for tablet " << tablet_id;
        return Status::OK();
    }

    // Collect rowset IDs
    std::vector<uint32_t> input_rowset_ids;
    for (const auto& rowset : input_rowsets) {
        input_rowset_ids.push_back(rowset->id());
    }

    // Mark rowsets as being compacted
    _mark_rowsets_compacting(tablet_id, input_rowset_ids);

    // Update running tasks count
    state.running_tasks++;
    _running_tasks++;

    state_lock.unlock();

    // Create compaction context
    // Set skip_write_txnlog=true so the txn_log is saved in context->txn_log
    // instead of being written to storage. This allows us to extract output_rowset
    // for CompactionResultPB.
    auto context = std::make_unique<CompactionTaskContext>(0 /* txn_id */, tablet_id, metadata->version(),
                                                           false /* force_base_compaction */,
                                                           true /* skip_write_txnlog */, nullptr /* callback */);

    // Submit to thread pool
    auto submit_st = _thread_pool->submit_func([this, ctx = std::move(context)]() mutable { _execute_compaction(std::move(ctx)); });

    if (!submit_st.ok()) {
        // Failed to submit, revert state changes
        std::lock_guard<std::mutex> lock(_state_mutex);
        auto it = _tablet_states.find(tablet_id);
        if (it != _tablet_states.end()) {
            _unmark_rowsets_compacting(tablet_id, input_rowset_ids);
            it->second.running_tasks--;
            _running_tasks--;
        }
        return submit_st;
    }

    LOG(INFO) << "Scheduled autonomous compaction for tablet " << tablet_id << ", rowsets: " << input_rowset_ids.size();

    return Status::OK();
}

void LakeCompactionManager::_mark_rowsets_compacting(int64_t tablet_id, const std::vector<uint32_t>& rowset_ids) {
    // Assumes _state_mutex is already held
    auto& state = _tablet_states[tablet_id];
    for (uint32_t rid : rowset_ids) {
        state.compacting_rowsets.insert(rid);
    }
}

void LakeCompactionManager::_unmark_rowsets_compacting(int64_t tablet_id, const std::vector<uint32_t>& rowset_ids) {
    std::lock_guard<std::mutex> lock(_state_mutex);
    auto it = _tablet_states.find(tablet_id);
    if (it != _tablet_states.end()) {
        for (uint32_t rid : rowset_ids) {
            it->second.compacting_rowsets.erase(rid);
        }
    }
}

void LakeCompactionManager::_execute_compaction(std::unique_ptr<CompactionTaskContext> context) {
    int64_t tablet_id = context->tablet_id;
    DeferOp defer([this, tablet_id]() {
        // Decrement running tasks count
        _running_tasks--;

        std::lock_guard<std::mutex> lock(_state_mutex);
        auto it = _tablet_states.find(tablet_id);
        if (it != _tablet_states.end()) {
            it->second.running_tasks--;
        }
    });

    VLOG(1) << "Executing autonomous compaction for tablet " << tablet_id;

    // Get tablet
    auto tablet_or = _tablet_mgr->get_tablet(tablet_id);
    if (!tablet_or.ok()) {
        LOG(WARNING) << "Failed to get tablet " << tablet_id << ": " << tablet_or.status();
        _failed_tasks++;
        return;
    }

    auto tablet = tablet_or.value();
    auto metadata = tablet.metadata();

    if (!metadata) {
        LOG(WARNING) << "Tablet " << tablet_id << " metadata not found";
        _failed_tasks++;
        return;
    }

    // Create compaction policy and pick rowsets
    auto policy_or = CompactionPolicy::create(_tablet_mgr, metadata, false);
    if (!policy_or.ok()) {
        LOG(WARNING) << "Failed to create compaction policy for tablet " << tablet_id << ": " << policy_or.status();
        _failed_tasks++;
        return;
    }

    std::unordered_set<uint32_t> exclude_rowsets;
    {
        std::lock_guard<std::mutex> lock(_state_mutex);
        auto it = _tablet_states.find(tablet_id);
        if (it != _tablet_states.end()) {
            exclude_rowsets = it->second.compacting_rowsets;
        }
    }

    auto rowsets_or = policy_or.value()->pick_rowsets_with_limit(config::lake_compaction_max_bytes_per_task, exclude_rowsets);
    if (!rowsets_or.ok() || rowsets_or.value().empty()) {
        VLOG(2) << "No rowsets to compact for tablet " << tablet_id;
        _completed_tasks++;
        return;
    }

    auto input_rowsets = rowsets_or.value();
    std::vector<uint32_t> input_rowset_ids;
    for (const auto& rowset : input_rowsets) {
        input_rowset_ids.push_back(rowset->id());
    }

    // Perform compaction using tablet_mgr->compact()
    // Note: context->version is already set during context creation in _schedule_tablet
    auto compaction_task_or = _tablet_mgr->compact(context.get());
    
    if (!compaction_task_or.ok()) {
        LOG(WARNING) << "Failed to create compaction task for tablet " << tablet_id << ": " << compaction_task_or.status();
        _unmark_rowsets_compacting(tablet_id, input_rowset_ids);
        _failed_tasks++;
        return;
    }

    auto compaction_task = compaction_task_or.value();
    
    // Execute compaction
    auto cancel_func = []() { return Status::OK(); };
    auto exec_st = compaction_task->execute(cancel_func, nullptr);

    _unmark_rowsets_compacting(tablet_id, input_rowset_ids);

    if (!exec_st.ok()) {
        LOG(WARNING) << "Compaction failed for tablet " << tablet_id << ": " << exec_st;
        _failed_tasks++;
        return;
    }

    // Save compaction result locally
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(metadata->version());
    result.set_finish_time_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count());
    
    for (uint32_t rid : input_rowset_ids) {
        result.add_input_rowset_ids(rid);
    }

    // Extract output rowset and SSTable info from txn_log
    // This is critical: without output_rowset, the publish will only delete input rowsets
    // without adding the compacted output, causing data loss.
    if (context->txn_log != nullptr && context->txn_log->has_op_compaction()) {
        const auto& op_compaction = context->txn_log->op_compaction();
        if (op_compaction.has_output_rowset()) {
            *result.mutable_output_rowset() = op_compaction.output_rowset();
        }
        // Copy SSTable metadata for primary key tables
        for (const auto& input_sst : op_compaction.input_sstables()) {
            result.add_input_sstables()->CopyFrom(input_sst);
        }
        for (const auto& output_sst : op_compaction.output_sstables()) {
            result.add_output_sstables()->CopyFrom(output_sst);
        }
    }

    auto save_st = _result_mgr->save_result(result);
    if (!save_st.ok()) {
        LOG(WARNING) << "Failed to save compaction result for tablet " << tablet_id << ": " << save_st.status();
        _failed_tasks++;
        return;
    }

    _completed_tasks++;

    LOG(INFO) << "Autonomous compaction completed for tablet " << tablet_id 
              << ", result saved to " << save_st.value();

    // Check if we should trigger next round
    int64_t total_bytes = 0;
    for (const auto& rowset : input_rowsets) {
        total_bytes += rowset->data_size();
    }

    // If processed close to limit, trigger next task
    if (total_bytes >= config::lake_compaction_max_bytes_per_task * 0.8) {
        update_tablet_async(tablet_id);
    }
}

} // namespace starrocks::lake


