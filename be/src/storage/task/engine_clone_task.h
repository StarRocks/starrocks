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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_clone_task.h

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

#pragma once

#include "agent/task_worker_pool.h"
#include "agent/utils.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/MasterService_types.h"
#include "storage/olap_define.h"
#include "storage/task/engine_task.h"

namespace starrocks {

void run_clone_task(std::shared_ptr<CloneAgentTaskRequest> agent_task_req);

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineCloneTask : public EngineTask {
public:
    EngineCloneTask(MemTracker* mem_tracker, const TCloneReq& _clone_req, int64_t _signature,
                    vector<string>* error_msgs, vector<TTabletInfo>* tablet_infos, AgentStatus* _res_status);

    ~EngineCloneTask() override = default;

    Status execute() override;

private:
    Status _do_clone_primary_tablet(Tablet* tablet);

    Status _do_clone(Tablet* tablet);

    Status _finish_clone(Tablet* tablet, const std::string& clone_dir, int64_t committed_version,
                         bool is_incremental_clone);

    Status _clone_incremental_data(Tablet* tablet, const TabletMeta& cloned_tablet_meta, int64_t committed_version);

    Status _clone_full_data(Tablet* tablet, TabletMeta* cloned_tablet_meta,
                            std::vector<RowsetMetaSharedPtr>& rs_to_clone);

    Status _clone_copy(DataDir& data_dir, const string& local_data_path, vector<string>* error_msgs,
                       const vector<Version>* missing_versions,
                       const std::vector<int64_t>* missing_version_ranges = nullptr);

    void _set_tablet_info(Status status, bool is_new_tablet);

    // Download tablet files from
    Status _download_files(DataDir* data_dir, const std::string& remote_url_prefix, const std::string& local_path);

    Status _make_snapshot(const std::string& ip, int port, TTableId tablet_id, TSchemaHash schema_hash, int timeout_s,
                          const std::vector<Version>* missed_versions,
                          const std::vector<int64_t>* missing_version_ranges, std::string* snapshot_path,
                          int32_t* snapshot_version);

    Status _release_snapshot(const std::string& ip, int port, const std::string& snapshot_path);

    Status _finish_clone_primary(Tablet* tablet, const std::string& clone_dir);

private:
    std::unique_ptr<MemTracker> _mem_tracker;
    const TCloneReq& _clone_req;
    vector<string>* _error_msgs;
    vector<TTabletInfo>* _tablet_infos;
    AgentStatus* _res_status;
    int64_t _signature;
}; // EngineTask

} // namespace starrocks
