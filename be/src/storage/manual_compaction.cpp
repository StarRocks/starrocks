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

#include "storage/manual_compaction.h"

#include <exception>
#include <string>
#include <vector>

#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/config_compaction_fwd.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "storage/base_compaction.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/cumulative_compaction.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/storage_metrics.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"

namespace starrocks {

Status run_manual_compaction(uint64_t tablet_id, const std::string& compaction_type,
                             const std::string& rowset_ids_string) {
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    RETURN_IF(tablet == nullptr, Status::InvalidArgument(fmt::format("Not Found tablet:{}", tablet_id)));

    auto* mem_tracker = GlobalEnv::GetInstance()->compaction_mem_tracker();
    if (tablet->updates() != nullptr) {
        StorageMetrics::instance()->update_compaction_request_total.increment(1);
        StorageMetrics::instance()->running_update_compaction_task_num.increment(1);
        DeferOp op([&] { StorageMetrics::instance()->running_update_compaction_task_num.increment(-1); });
        Status res;
        if (rowset_ids_string.empty()) {
            res = tablet->updates()->compaction(mem_tracker);
        } else {
            std::vector<std::string> id_str_list = strings::Split(rowset_ids_string, ",", strings::SkipEmpty());
            std::vector<uint32_t> rowset_ids;
            for (const auto& id_str : id_str_list) {
                try {
                    auto rowset_id = std::stoull(id_str);
                    if (rowset_id > UINT32_MAX) {
                        throw std::exception();
                    }
                    rowset_ids.push_back((uint32_t)rowset_id);
                } catch (const std::exception& e) {
                    std::string msg = fmt::format("invalid argument. rowset_ids:{}", rowset_ids_string);
                    LOG(WARNING) << msg;
                    return Status::InvalidArgument(msg);
                }
            }
            if (rowset_ids.empty()) {
                return Status::InvalidArgument(fmt::format("empty argument. rowset_ids:{}", rowset_ids_string));
            }
            res = tablet->updates()->compaction(mem_tracker, rowset_ids);
        }
        if (!res.ok()) {
            StorageMetrics::instance()->update_compaction_request_failed.increment(1);
            LOG(WARNING) << "failed to perform update compaction. res=" << res.message()
                         << ", tablet=" << tablet->full_name();
            return res;
        }
        return Status::OK();
    }

    if (compaction_type != to_string(CompactionType::BASE_COMPACTION) &&
        compaction_type != to_string(CompactionType::CUMULATIVE_COMPACTION)) {
        return Status::NotSupported(fmt::format("unsupport compaction type:{}", compaction_type));
    }

    if (compaction_type == to_string(CompactionType::CUMULATIVE_COMPACTION)) {
        if (config::enable_size_tiered_compaction_strategy) {
            if (tablet->need_compaction()) {
                auto compaction_task = tablet->create_compaction_task();
                if (compaction_task != nullptr) {
                    compaction_task->set_task_id(
                            StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
                    compaction_task->set_is_manual_compaction(true);
                    compaction_task->start();
                    if (compaction_task->compaction_task_state() != COMPACTION_SUCCESS) {
                        return Status::InternalError(fmt::format("Failed to base compaction tablet={} err={}",
                                                                 tablet->full_name(),
                                                                 tablet->last_cumu_compaction_failure_status()));
                    }
                }
            }
        } else {
            CumulativeCompaction cumulative_compaction(mem_tracker, tablet);

            Status res = cumulative_compaction.compact();
            if (!res.ok()) {
                if (!res.is_mem_limit_exceeded()) {
                    tablet->set_last_cumu_compaction_failure_time(UnixMillis());
                }
                if (!res.is_not_found()) {
                    StorageMetrics::instance()->cumulative_compaction_request_failed.increment(1);
                    LOG(WARNING) << "Fail to vectorized compact tablet=" << tablet->full_name()
                                 << ", err=" << res.to_string();
                }
                return res;
            }
        }
        tablet->set_last_cumu_compaction_failure_time(0);
    } else if (compaction_type == to_string(CompactionType::BASE_COMPACTION)) {
        if (config::enable_size_tiered_compaction_strategy) {
            if (tablet->force_base_compaction()) {
                auto compaction_task = tablet->create_compaction_task();
                if (compaction_task != nullptr) {
                    compaction_task->set_task_id(
                            StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
                    compaction_task->set_is_manual_compaction(true);
                    compaction_task->start();
                    if (compaction_task->compaction_task_state() != COMPACTION_SUCCESS) {
                        return Status::InternalError(fmt::format("Failed to base compaction tablet={} task_id={}",
                                                                 tablet->full_name(), compaction_task->task_id()));
                    }
                }
            } else {
                return Status::InternalError(
                        fmt::format("Failed to base compaction tablet={} no need to do", tablet->full_name()));
            }
        } else {
            BaseCompaction base_compaction(mem_tracker, tablet);

            Status res = base_compaction.compact();
            if (!res.ok()) {
                tablet->set_last_base_compaction_failure_time(UnixMillis());
                if (!res.is_not_found()) {
                    StorageMetrics::instance()->base_compaction_request_failed.increment(1);
                    LOG(WARNING) << "failed to init vectorized base compaction. res=" << res.to_string()
                                 << ", tablet=" << tablet->full_name();
                }
                return res;
            }
        }

        tablet->set_last_base_compaction_failure_time(0);
    } else {
        __builtin_unreachable();
    }
    return Status::OK();
}

} // namespace starrocks
