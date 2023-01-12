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

#include "exec/pipeline/stream_epoch_manager.h"

#include <fmt/format.h>

#include "exec/pipeline/pipeline_driver_executor.h"
#include "gen_cpp/MVMaintenance_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

ScanRangeInfo ScanRangeInfo::from_start_epoch_start(const TMVStartEpochTask& start_epoch) {
    ScanRangeInfo res;
    for (auto& [instance, instance_scan] : start_epoch.per_node_scan_ranges) {
        auto& instance_scan_info = res.instance_scan_range_map[instance];
        for (auto& [plan_node, node_scan] : instance_scan) {
            auto& node_scan_info = instance_scan_info[plan_node];
            for (const TScanRange& scan_range : node_scan) {
                DCHECK(scan_range.__isset.binlog_scan_range);
                auto& binlog = scan_range.binlog_scan_range;
                auto& scan_info = node_scan_info[binlog.tablet_id];
                scan_info.tablet_id = binlog.tablet_id;
                scan_info.tablet_version = binlog.offset.version;
                scan_info.lsn = binlog.offset.lsn;
            }
        }
    }
    return res;
}

// Start the new epoch from input epoch info
Status StreamEpochManager::start_epoch(ExecEnv* exec_env, const QueryContext* query_ctx,
                                       const std::vector<FragmentContext*>& fragment_ctxs, const EpochInfo& epoch_info,
                                       const ScanRangeInfo& scan_info) {
    {
        auto& fragment_id_to_node_id_scan_ranges = scan_info.instance_scan_range_map;
        std::unique_lock<std::shared_mutex> l(_epoch_lock);
        if (!_fragment_id_to_node_id_scan_ranges.empty()) {
            if (_fragment_id_to_node_id_scan_ranges.size() != fragment_id_to_node_id_scan_ranges.size()) {
                return Status::InternalError(fmt::format("Update epoch failed: {} is not equal to origin's size {}",
                                                         fragment_id_to_node_id_scan_ranges.size(),
                                                         _fragment_id_to_node_id_scan_ranges.size()));
            }
            for (auto& [fragment_instance_id, node_id_to_scan_ranges] : fragment_id_to_node_id_scan_ranges) {
                auto iter = _fragment_id_to_node_id_scan_ranges.find(fragment_instance_id);
                if (iter == _fragment_id_to_node_id_scan_ranges.end()) {
                    return Status::InternalError(
                            fmt::format("Update epoch failed: fragment_instance_id {} not exists before",
                                        print_id(fragment_instance_id)));
                }
                auto existed_node_id_to_scan_ranges = iter->second;
                for (auto& [scan_node_id, scan_ranges] : node_id_to_scan_ranges) {
                    auto scan_node_id_iter = existed_node_id_to_scan_ranges.find(scan_node_id);
                    if (scan_node_id_iter == existed_node_id_to_scan_ranges.end()) {
                        return Status::InternalError(
                                fmt::format("Update epoch failed: scan_node_id {} not exists before", scan_node_id));
                    }
                    // replace
                    scan_node_id_iter->second = scan_ranges;
                }
            }
        } else {
            _fragment_id_to_node_id_scan_ranges = fragment_id_to_node_id_scan_ranges;
        }

        _epoch_info = epoch_info;
        _finished_fragment_ctxs.clear();
    }

    // Reset epoch state for each fragment ctx
    for (auto* fragment_ctx : fragment_ctxs) {
        RETURN_IF_ERROR(fragment_ctx->reset_epoch());
    }

    // Activate parked drivers must be at the last!
    RETURN_IF_ERROR(activate_parked_driver(exec_env, query_ctx->query_id(), _num_drivers, _enable_resource_group));

    return Status::OK();
}

Status StreamEpochManager::prepare(const MVMaintenanceTaskInfo& maintenance_task_info,
                                   const std::vector<FragmentContext*>& fragment_ctxs) {
    std::unique_lock<std::shared_mutex> l(_epoch_lock);

    _maintenance_task_info = maintenance_task_info;

    // TODO(lism):
    // - Prepare enable_resource_gorup in FE.
    // - Ensure all fragment ctx's enable_resource_group are the same.
    for (auto* fragment_ctx : fragment_ctxs) {
        _enable_resource_group &= fragment_ctx->enable_resource_group();
        _num_drivers += fragment_ctx->num_drivers();
    }
    return Status::OK();
}

const MVMaintenanceTaskInfo& StreamEpochManager::maintenance_task_info() const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);
    return _maintenance_task_info;
}

Status StreamEpochManager::set_finished(ExecEnv* exec_env, const QueryContext* query_ctx) {
    _is_finished.store(true, std::memory_order_release);
    // Activate parked drivers must be at the last!
    return activate_parked_driver(exec_env, query_ctx->query_id(), _num_drivers, _enable_resource_group);
}

Status StreamEpochManager::update_binlog_offset(const TUniqueId& fragment_instance_id, int64_t scan_node_id,
                                                int64_t tablet_id, BinlogOffset binlog_offset) {
    std::unique_lock<std::shared_mutex> l(_epoch_lock);
    auto node_id_to_scan_ranges_iter = _fragment_id_to_node_id_scan_ranges.find(fragment_instance_id);
    if (node_id_to_scan_ranges_iter == _fragment_id_to_node_id_scan_ranges.end()) {
        return Status::InternalError(fmt::format("Update binlog offset failed: fragment_instance_id {} is not found.",
                                                 print_id(fragment_instance_id)));
    }
    auto& node_id_to_scan_ranges = node_id_to_scan_ranges_iter->second;
    auto iter = node_id_to_scan_ranges.find(scan_node_id);
    if (iter == node_id_to_scan_ranges.end()) {
        return Status::InternalError(
                fmt::format("Update binlog offset failed: scan_node_id {} is not found.", scan_node_id));
    }
    auto& scan_range_mapping = iter->second;
    auto scan_range_iter = scan_range_mapping.find(tablet_id);
    if (scan_range_iter == scan_range_mapping.end()) {
        return Status::InternalError(fmt::format("Update binlog offset failed: tablet_id {} is not found.", tablet_id));
    }
    // update binlog to report the final status
    scan_range_iter->second = binlog_offset;
    return Status::OK();
}

const BinlogOffset* StreamEpochManager::get_binlog_offset(const TUniqueId& fragment_instance_id, int64_t scan_node_id,
                                                          int64_t tablet_id) const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);

    auto node_id_to_scan_ranges_iter = _fragment_id_to_node_id_scan_ranges.find(fragment_instance_id);
    if (node_id_to_scan_ranges_iter == _fragment_id_to_node_id_scan_ranges.end()) {
        return nullptr;
    }
    auto& node_id_to_scan_ranges = node_id_to_scan_ranges_iter->second;
    auto iter = node_id_to_scan_ranges.find(scan_node_id);
    if (iter == node_id_to_scan_ranges.end()) {
        return nullptr;
    }
    return _get_epoch_unlock(iter->second, tablet_id);
}

const EpochInfo& StreamEpochManager::epoch_info() const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);
    return _epoch_info;
}

const std::unordered_map<TUniqueId, NodeId2ScanRanges>& StreamEpochManager::fragment_id_to_node_id_scan_ranges() const {
    std::shared_lock<std::shared_mutex> l(_epoch_lock);
    return _fragment_id_to_node_id_scan_ranges;
}

void StreamEpochManager::count_down_fragment_ctx(RuntimeState* state, FragmentContext* fragment_ctx, size_t val) {
    _finished_fragment_ctxs.emplace_back(fragment_ctx);
    bool all_fragment_finished = _finished_fragment_ctxs.size() == _fragment_id_to_node_id_scan_ranges.size();
    if (!all_fragment_finished) {
        return;
    }

    // do epoch report stats
    auto* query_ctx = state->query_ctx();
    state->exec_env()->driver_executor()->report_epoch(state->exec_env(), query_ctx, _finished_fragment_ctxs);
}

const BinlogOffset* StreamEpochManager::_get_epoch_unlock(const TabletId2BinlogOffset& tablet_id_scan_ranges_mapping,
                                                          int64_t tablet_id) const {
    auto iter = tablet_id_scan_ranges_mapping.find(tablet_id);
    if (iter != tablet_id_scan_ranges_mapping.end()) {
        return &(iter->second);
    } else {
        return nullptr;
    }
}

Status StreamEpochManager::activate_parked_driver(ExecEnv* exec_env, const TUniqueId& query_id,
                                                  int64_t expected_num_drivers, bool enable_resource_group) {
    int64_t num_drivers = 0;
    if (enable_resource_group) {
        num_drivers = exec_env->wg_driver_executor()->activate_parked_driver(
                [query_id](const pipeline::PipelineDriver* driver) {
                    return driver->query_ctx()->query_id() == query_id;
                });
    } else {
        num_drivers =
                exec_env->driver_executor()->activate_parked_driver([query_id](const pipeline::PipelineDriver* driver) {
                    return driver->query_ctx()->query_id() == query_id;
                });
    }
    if (num_drivers != expected_num_drivers) {
        return Status::InternalError(
                fmt::format("Update epoch failed: num activated drivers {} not equal to num drivers {}", num_drivers,
                            expected_num_drivers));
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
