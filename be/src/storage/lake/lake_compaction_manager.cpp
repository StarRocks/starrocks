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

#include <bvar/bvar.h>

#include <algorithm>

#include "common/config.h"
#include "common/logging.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_result_manager.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

// Phase 5 metrics. These are best-effort counters that allow operators to detect
// queue blow-ups, dispatch starvation, or local-result accumulation. Values are
// approximate (queue size sampled lazily; running_tasks updated on each dispatch
// and finish).
static bvar::Adder<int64_t> g_autonomous_completed_tasks("lake_autonomous_compaction_completed_tasks");
static bvar::Adder<int64_t> g_autonomous_dispatched_tasks("lake_autonomous_compaction_dispatched_tasks");
static bvar::Adder<int64_t> g_autonomous_skipped_tasks("lake_autonomous_compaction_skipped_tasks");

LakeCompactionManager* LakeCompactionManager::instance() {
    static LakeCompactionManager s_instance;
    return &s_instance;
}

void LakeCompactionManager::start(TabletManager* tablet_mgr, CompactionResultManager* result_mgr) {
    std::lock_guard<std::mutex> guard(_mu);
    if (_started) return;
    _tablet_mgr = tablet_mgr;
    _result_mgr = result_mgr;
    _stopping.store(false, std::memory_order_relaxed);
    _started = true;
    _dispatch_thread = std::thread([this] { dispatch_loop(); });
    LOG(INFO) << "LakeCompactionManager started";
}

void LakeCompactionManager::stop() {
    {
        std::lock_guard<std::mutex> guard(_mu);
        if (!_started) return;
        _stopping.store(true, std::memory_order_relaxed);
        _cv.notify_all();
    }
    if (_dispatch_thread.joinable()) _dispatch_thread.join();
    std::lock_guard<std::mutex> guard(_mu);
    _started = false;
    LOG(INFO) << "LakeCompactionManager stopped";
}

void LakeCompactionManager::update_tablet_async(int64_t tablet_id) {
    if (!config::enable_lake_autonomous_compaction) return;
    std::lock_guard<std::mutex> guard(_mu);
    if (!_started) return;
    if (_enqueued.count(tablet_id) > 0) return;
    double score = compute_score_locked(tablet_id);
    if (score < config::lake_autonomous_compaction_score_threshold) {
        return;
    }
    TabletEntry e;
    e.tablet_id = tablet_id;
    e.score = score;
    e.enqueue_time_ms = static_cast<int64_t>(time(nullptr)) * 1000;
    _queue.push(e);
    _enqueued.insert(tablet_id);
    _cv.notify_one();
}

double LakeCompactionManager::compute_score_locked(int64_t tablet_id) {
    // TODO(Phase 4 follow-up): plug in a real "latest cached metadata" path.
    // For now we return a positive constant above the threshold so that
    // any update_tablet_async call enqueues the tablet. The dispatch loop
    // still respects the per-tablet and global concurrency caps; mis-priority
    // only affects fairness, not correctness.
    (void)tablet_id;
    return std::max(1.0 + config::lake_autonomous_compaction_score_threshold, 1.0);
}

void LakeCompactionManager::notify_task_finished(int64_t tablet_id,
                                                 const std::vector<uint32_t>& consumed_input_rowsets) {
    std::lock_guard<std::mutex> guard(_mu);
    // Only decrement when there is a matching reservation. notify without a
    // prior dispatch (test fixtures, racy retry paths) must not let counters
    // go negative — that would break the global-cap check in dispatch_loop.
    auto it = _running_per_tablet.find(tablet_id);
    bool had_reservation = (it != _running_per_tablet.end());
    if (had_reservation) {
        if (--it->second <= 0) _running_per_tablet.erase(it);
    }
    auto rit = _running_inputs.find(tablet_id);
    if (rit != _running_inputs.end()) {
        for (uint32_t rid : consumed_input_rowsets) {
            auto pit = rit->second.find(rid);
            if (pit != rit->second.end()) rit->second.erase(pit);
        }
        if (rit->second.empty()) _running_inputs.erase(rit);
    }
    if (had_reservation) {
        int64_t prev = _running_total.fetch_sub(1, std::memory_order_relaxed);
        DCHECK_GT(prev, 0) << "running_total underflow";
    }
    _cv.notify_one();
}

size_t LakeCompactionManager::queue_size() const {
    std::lock_guard<std::mutex> guard(_mu);
    return _queue.size();
}

int64_t LakeCompactionManager::running_tasks_for_tablet(int64_t tablet_id) const {
    std::lock_guard<std::mutex> guard(_mu);
    auto it = _running_per_tablet.find(tablet_id);
    return it == _running_per_tablet.end() ? 0 : it->second;
}

std::unordered_set<uint32_t> LakeCompactionManager::running_inputs(int64_t tablet_id) const {
    std::lock_guard<std::mutex> guard(_mu);
    auto it = _running_inputs.find(tablet_id);
    if (it == _running_inputs.end()) return {};
    return std::unordered_set<uint32_t>(it->second.begin(), it->second.end());
}

void LakeCompactionManager::dispatch_loop() {
    std::unique_lock<std::mutex> lock(_mu);
    while (!_stopping.load(std::memory_order_relaxed)) {
        _cv.wait_for(lock, std::chrono::seconds(1), [this] {
            if (_stopping.load(std::memory_order_relaxed)) return true;
            if (_queue.empty()) return false;
            // Wake when there's queue work AND we're under global cap.
            return _running_total.load(std::memory_order_relaxed) <
                   config::lake_autonomous_compaction_max_concurrent_tasks;
        });
        if (_stopping.load(std::memory_order_relaxed)) break;
        while (!_queue.empty() && _running_total.load(std::memory_order_relaxed) <
                                          config::lake_autonomous_compaction_max_concurrent_tasks) {
            if (!try_dispatch_one_locked()) break;
        }
    }
}

bool LakeCompactionManager::try_dispatch_one_locked() {
    if (_queue.empty()) return false;
    TabletEntry top = _queue.top();
    int64_t tablet_id = top.tablet_id;
    auto perTabletIt = _running_per_tablet.find(tablet_id);
    int64_t per_tablet_running = (perTabletIt == _running_per_tablet.end()) ? 0 : perTabletIt->second;
    if (per_tablet_running >= config::lake_autonomous_compaction_max_tasks_per_tablet) {
        // This tablet is saturated; pop and re-enqueue later. To avoid head-of-line
        // blocking we drop it from the queue here (and from _enqueued), so a future
        // notify_task_finished + update_tablet_async can re-enqueue with fresh score.
        _queue.pop();
        _enqueued.erase(tablet_id);
        g_autonomous_skipped_tasks << 1;
        return true; // we did make progress on the queue
    }
    _queue.pop();
    _enqueued.erase(tablet_id);

    // Reserve slots before releasing the lock so a concurrent dispatch sees us.
    _running_per_tablet[tablet_id] = per_tablet_running + 1;
    _running_total.fetch_add(1, std::memory_order_relaxed);

    // TODO(Phase 4 follow-up): actually submit the compaction task into the
    // existing CompactionScheduler's task pool. That requires:
    //   1. Build a CompactionTaskContext with write_to_local_result=true and
    //      local_result_base_version=metadata.version().
    //   2. pick_rowsets_with_limit(running_inputs ∪ pending_inputs, max_bytes).
    //   3. Submit via CompactionScheduler::compaction_thread_pool().submit_func().
    //   4. On completion call notify_task_finished(tablet_id, input_rowsets).
    //
    // The integration point is the same task path used by lake_service.cpp::compact;
    // we just bypass the RPC layer and synthesize a CompactionTaskContext directly.
    // Skeleton kept here so Phase 4.3 UTs can exercise queue + counters; live wiring
    // tracked separately to keep this PR reviewable in isolation.
    LOG(INFO) << "LakeCompactionManager dispatched tablet=" << tablet_id << " score=" << top.score
              << " (skeleton — task submission not yet wired)";
    g_autonomous_dispatched_tasks << 1;

    // Eagerly release the slot so the queue keeps moving in this skeleton mode.
    // Live integration removes this and instead releases on real task completion.
    _running_per_tablet[tablet_id] = per_tablet_running;
    if (_running_per_tablet[tablet_id] == 0) _running_per_tablet.erase(tablet_id);
    _running_total.fetch_sub(1, std::memory_order_relaxed);
    g_autonomous_completed_tasks << 1;
    return true;
}

} // namespace starrocks::lake
