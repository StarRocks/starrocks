// This file is made available under Elastic License 2.0.
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

#ifndef STARROCKS_BE_SRC_OLAP_TASK_ENGINE_CLONE_TASK_H
#define STARROCKS_BE_SRC_OLAP_TASK_ENGINE_CLONE_TASK_H

#include "agent/utils.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/MasterService_types.h"
#include "storage/olap_define.h"
#include "storage/task/engine_task.h"

namespace starrocks {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineCloneTask : public EngineTask {
public:
    EngineCloneTask(MemTracker* mem_tracker, const TCloneReq& _clone_req, const TMasterInfo& _master_info,
                    int64_t _signature, vector<string>* error_msgs, vector<TTabletInfo>* tablet_infos,
                    AgentStatus* _res_status);

    ~EngineCloneTask() override = default;

    OLAPStatus execute() override;

private:
    Status _do_clone(Tablet* tablet);

    Status _finish_clone(Tablet* tablet, const std::string& clone_dir, int64_t committed_version,
                         bool is_incremental_clone);

    Status _clone_incremental_data(Tablet* tablet, const TabletMeta& cloned_tablet_meta, int64_t committed_version);

    Status _clone_full_data(Tablet* tablet, TabletMeta* cloned_tablet_meta);

    Status _clone_copy(DataDir& data_dir, const string& local_data_path, vector<string>* error_msgs,
                       const vector<Version>* missing_versions);

    void _set_tablet_info(Status status, bool is_new_tablet);

    // Download tablet files from
    Status _download_files(DataDir* data_dir, const std::string& remote_url_prefix, const std::string& local_path);

    Status _make_snapshot(const std::string& ip, int port, TTableId tablet_id, TSchemaHash schema_hash, int timeout_s,
                          const std::vector<Version>* missed_versions, std::string* snapshot_path,
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
    const TMasterInfo& _master_info;
}; // EngineTask

} // namespace starrocks
#endif //STARROCKS_BE_SRC_OLAP_TASK_ENGINE_CLONE_TASK_H
