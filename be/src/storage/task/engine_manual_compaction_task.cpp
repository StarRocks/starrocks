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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_checksum_task.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/task/engine_manual_compaction_task.h"

#include <memory>

#include "runtime/exec_env.h"
#include "storage/base_compaction.h"
#include "storage/chunk_helper.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/cumulative_compaction.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_updates.h"
#include "util/defer_op.h"

namespace starrocks {

EngineManualCompactionTask::EngineManualCompactionTask(MemTracker* mem_tracker, TTabletId tablet_id,
                                                       bool base_compaction)
        : _tablet_id(tablet_id), _base_compaction(base_compaction) {
    _mem_tracker = mem_tracker;
}

Status EngineManualCompactionTask::execute() {
    auto st = _manual_compaction();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to manual compaction tablet_id=" << _tablet_id << " error=" << st;
    }
    return st;
} // execute

Status EngineManualCompactionTask::_manual_compaction() {
    LOG(INFO) << "begin to manual compaction. "
              << "tablet_id=" << _tablet_id << ", base_compaction=" << _base_compaction;

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    RETURN_IF(tablet == nullptr, Status::InvalidArgument(fmt::format("Not Found tablet:{}", _tablet_id)));

    if (tablet->updates() != nullptr) {
        RETURN_IF_ERROR(tablet->updates()->compaction(_mem_tracker));
        return Status::OK();
    }

    if (_base_compaction) {
        StarRocksMetrics::instance()->base_compaction_request_total.increment(1);
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
            BaseCompaction base_compaction(_mem_tracker, tablet);

            Status res = base_compaction.compact();
            if (!res.ok()) {
                tablet->set_last_base_compaction_failure_time(UnixMillis());
                if (!res.is_not_found()) {
                    StarRocksMetrics::instance()->base_compaction_request_failed.increment(1);
                    LOG(WARNING) << "failed to init base compaction. res=" << res.to_string()
                                 << ", tablet=" << tablet->full_name();
                }
                return res;
            }
        }
        tablet->set_last_base_compaction_failure_time(0);
    } else {
        StarRocksMetrics::instance()->cumulative_compaction_request_total.increment(1);
        if (config::enable_size_tiered_compaction_strategy) {
            if (tablet->need_compaction()) {
                auto compaction_task = tablet->create_compaction_task();
                if (compaction_task != nullptr) {
                    compaction_task->set_task_id(
                            StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
                    compaction_task->set_is_manual_compaction(true);
                    compaction_task->start();
                    if (compaction_task->compaction_task_state() != COMPACTION_SUCCESS) {
                        return Status::InternalError(fmt::format("Failed to cumulative compaction tablet={} err={}",
                                                                 tablet->full_name(),
                                                                 tablet->last_cumu_compaction_failure_status()));
                    }
                }
            }
        } else {
            CumulativeCompaction cumulative_compaction(_mem_tracker, tablet);

            Status res = cumulative_compaction.compact();
            if (!res.ok()) {
                if (!res.is_mem_limit_exceeded()) {
                    tablet->set_last_cumu_compaction_failure_time(UnixMillis());
                }
                if (!res.is_not_found()) {
                    StarRocksMetrics::instance()->cumulative_compaction_request_failed.increment(1);
                    LOG(WARNING) << "Fail to cumulative compact tablet=" << tablet->full_name()
                                 << ", err=" << res.to_string();
                }
                return res;
            }
        }
        tablet->set_last_cumu_compaction_failure_time(0);
    }
    return Status::OK();
}

} // namespace starrocks
