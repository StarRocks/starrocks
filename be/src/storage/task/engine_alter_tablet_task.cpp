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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_alter_tablet_task.cpp

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

#include "storage/task/engine_alter_tablet_task.h"

#include "runtime/current_thread.h"
#include "storage/lake/schema_change.h"
#include "storage/schema_change.h"
#include "util/defer_op.h"

namespace starrocks {

using std::to_string;

EngineAlterTabletTask::EngineAlterTabletTask(MemTracker* mem_tracker, const TAlterTabletReqV2& request)
        : _alter_tablet_req(request) {
    size_t mem_limit = static_cast<size_t>(config::memory_limitation_per_thread_for_schema_change) * 1024 * 1024 * 1024;
    _mem_tracker =
            std::make_unique<MemTracker>(MemTracker::SCHEMA_CHANGE_TASK, mem_limit, "schema change task", mem_tracker);
}

Status EngineAlterTabletTask::execute() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    StarRocksMetrics::instance()->create_rollup_requests_total.increment(1);

    Status res;
    std::string alter_msg_header = strings::Substitute("[Alter Job:$0, tablet:$1]: ", _alter_tablet_req.job_id,
                                                       _alter_tablet_req.base_tablet_id);
    if (_alter_tablet_req.tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        lake::SchemaChangeHandler handler(ExecEnv::GetInstance()->lake_tablet_manager());
        res = handler.process_alter_tablet(_alter_tablet_req);
    } else {
        SchemaChangeHandler handler;
        handler.set_alter_msg_header(alter_msg_header);
        res = handler.process_alter_tablet_v2(_alter_tablet_req);
    }
    if (!res.ok()) {
        LOG(WARNING) << alter_msg_header << "failed to do alter task. status=" << res.to_string()
                     << " base_tablet_id=" << _alter_tablet_req.base_tablet_id
                     << ", base_schema_hash=" << _alter_tablet_req.base_schema_hash
                     << ", new_tablet_id=" << _alter_tablet_req.new_tablet_id
                     << ", new_schema_hash=" << _alter_tablet_req.new_schema_hash;
        StarRocksMetrics::instance()->create_rollup_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << alter_msg_header << "success to do alter task. base_tablet_id=" << _alter_tablet_req.base_tablet_id;
    return Status::OK();
} // execute

} // namespace starrocks
