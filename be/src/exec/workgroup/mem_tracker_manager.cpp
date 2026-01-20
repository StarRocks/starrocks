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

#include "mem_tracker_manager.h"

#include <memory>

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "work_group.h"

namespace starrocks::workgroup {

MemTrackerPtr MemTrackerManager::register_workgroup(const WorkGroupPtr& wg) {
    if (WorkGroup::DEFAULT_MEM_POOL == wg->mem_pool()) {
        return GlobalEnv::GetInstance()->query_pool_mem_tracker_shared();
    }

    std::unique_lock write_lock(_mutex);

    const double mem_limit_fraction = wg->mem_limit();
    const int64_t memory_limit_bytes =
            static_cast<int64_t>(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit() * mem_limit_fraction);

    // Frontend (FE) validation ensures that active resource groups (RGs) sharing
    // the same mem_pool also have the same mem_limit.
    if (_shared_mem_trackers.contains(wg->mem_pool())) {
        // We must handle an edge case:
        // 1. All RGs using a specific mem_pool are marked for deletion.
        // 2. The shared tracker for that pool remains cached here.
        // 3. A new RG is created with the same mem_pool name but a different mem_limit.
        // Therefore, we must verify the cached tracker's limit matches the current RG's limit.
        // Otherwise, a new mem_pool with thew new mem_limit will be constructed, and expiring children are orphaned.
        if (MemTrackerInfo& tracker_info = _shared_mem_trackers.at(wg->mem_pool());
            tracker_info.tracker->limit() == memory_limit_bytes) {
            tracker_info.child_count++;
            return tracker_info.tracker;
        }
    }

    auto shared_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL, memory_limit_bytes,
                                         wg->mem_pool(), GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _shared_mem_trackers[wg->mem_pool()].tracker = shared_mem_tracker;
    _shared_mem_trackers[wg->mem_pool()].child_count++; // also handles orphaned children

    _add_metrics_unlocked(wg->mem_pool(), write_lock);
    return shared_mem_tracker;
}

void MemTrackerManager::deregister_workgroup(const std::string& mem_pool) {
    if (WorkGroup::DEFAULT_MEM_POOL == mem_pool) {
        return;
    }

    std::unique_lock write_lock(_mutex);

    if (_shared_mem_trackers.contains(mem_pool)) {
        MemTrackerInfo& tracker_info = _shared_mem_trackers.at(mem_pool);
        if (tracker_info.child_count == 1) {
            // The shared tracker will be erased if its last child is being deregistered
            _shared_mem_trackers.erase(mem_pool);
        } else {
            DCHECK(tracker_info.child_count > 1);
            tracker_info.child_count--;
        }
    }
}

void MemTrackerManager::_add_metrics_unlocked(const std::string& mem_pool, UniqueLockType& lock) {
    std::call_once(_register_metrics_hook_once_flag, [this] {
        StarRocksMetrics::instance()->metrics()->register_hook("mem_pool_metrics_hook", [this] { _update_metrics(); });
    });

    if (_shared_mem_trackers_metrics.contains(mem_pool)) {
        return;
    }

    // We have to free the write_lock during metrics registration to prevent a deadlock with the metrics collector.
    // The collector will first claim MetricsRegistry::mutex and then MemTrackerManager::mutex due to _update_metrics()
    // callback hook. At this point in the code, we are holding the MemTrackerManager::mutex and will try to claim the
    // MetricsRegistry::mutex after calling register_metric. Without unlocking, we would run into a deadlock.
    lock.unlock();

    auto* registry = StarRocksMetrics::instance()->metrics();

    auto mem_limit = std::make_unique<IntGauge>(MetricUnit::BYTES);
    const bool mem_limit_registered = registry->register_metric("mem_pool_mem_limit_bytes",
                                                                MetricLabels().add("name", mem_pool), mem_limit.get());

    auto mem_usage_bytes = std::make_unique<IntGauge>(MetricUnit::BYTES);
    const bool mem_usage_bytes_registered = registry->register_metric(
            "mem_pool_mem_usage_bytes", MetricLabels().add("name", mem_pool), mem_usage_bytes.get());

    auto mem_usage_ratio = std::make_unique<DoubleGauge>(MetricUnit::PERCENT);
    const bool mem_usage_ratio_registered = registry->register_metric(
            "mem_pool_mem_usage_ratio", MetricLabels().add("name", mem_pool), mem_usage_ratio.get());

    auto workgroup_count = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
    const bool workgroup_count_registered = registry->register_metric(
            "mem_pool_workgroup_count", MetricLabels().add("name", mem_pool), workgroup_count.get());

    lock.lock();

    // During the unlocked period above, another thread might have already inserted a metrics entry. Use it if exists.
    const std::shared_ptr<MemTrackerMetrics>& metrics =
            _shared_mem_trackers_metrics.contains(mem_pool)
                    ? _shared_mem_trackers_metrics.at(mem_pool)
                    : _shared_mem_trackers_metrics.emplace(mem_pool, std::make_shared<MemTrackerMetrics>())
                              .first->second;

    // During the unlocked period above, other threads might have tried to register the same metrics and succeeded in
    // some of them. register_metric will return false if you try to register a metric with the same name and labels
    // that is already registered. Therefore, we only std::move the individual metrics which this thread registered.
    if (mem_limit_registered) {
        metrics->mem_limit = std::move(mem_limit);
    }
    if (mem_usage_bytes_registered) {
        metrics->mem_usage_bytes = std::move(mem_usage_bytes);
    }
    if (mem_usage_ratio_registered) {
        metrics->mem_usage_ratio = std::move(mem_usage_ratio);
    }
    if (workgroup_count_registered) {
        metrics->workgroup_count = std::move(workgroup_count);
    }

    // Note: It is also theoretically possible that the mem_tracker was deleted during the unlocked period.
    // We are ok with leaking a metric in this case and do not try to unregister.
}

void MemTrackerManager::_update_metrics() {
    std::unique_lock write_lock(_mutex);
    _update_metrics_unlocked(write_lock);
}

void MemTrackerManager::_update_metrics_unlocked(UniqueLockType&) {
    const auto divide = [](const int64_t a, const int64_t b) { return b <= 0 ? 0.0 : static_cast<double>(a) / b; };

    for (auto& [mem_pool, metrics] : _shared_mem_trackers_metrics) {
        if (const auto it = _shared_mem_trackers.find(mem_pool); it != _shared_mem_trackers.end()) {
            const auto& [mem_tracker, child_count] = it->second;
            metrics->mem_limit->set_value(mem_tracker->limit());
            metrics->mem_usage_bytes->set_value(mem_tracker->consumption());
            metrics->mem_usage_ratio->set_value(divide(mem_tracker->consumption(), mem_tracker->limit()));
            metrics->workgroup_count->set_value(child_count);
        } else {
            // Metrics entries for deleted shared_mem_trackers are never deleted, but simply set to 0.
            metrics->mem_limit->set_value(0);
            metrics->mem_usage_bytes->set_value(0);
            metrics->mem_usage_ratio->set_value(0);
            metrics->workgroup_count->set_value(0);
        }
    }
}

std::vector<std::string> MemTrackerManager::list_mem_trackers() const {
    std::shared_lock read_lock(_mutex);
    std::vector<std::string> mem_trackers{};
    mem_trackers.reserve(_shared_mem_trackers.size());
    for (const auto& mem_tracker : _shared_mem_trackers) {
        mem_trackers.push_back(mem_tracker.first);
    }
    return mem_trackers;
}
} // namespace starrocks::workgroup