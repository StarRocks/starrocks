// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_alter_tablet_task.h

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

#ifndef STARROCKS_BE_SRC_OLAP_TASK_ENGINE_ALTER_TABLET_TASK_H
#define STARROCKS_BE_SRC_OLAP_TASK_ENGINE_ALTER_TABLET_TASK_H

#include "gen_cpp/AgentService_types.h"
#include "storage/olap_define.h"
#include "storage/task/engine_task.h"

namespace starrocks {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineAlterTabletTask : public EngineTask {
public:
    OLAPStatus execute() override;

public:
    EngineAlterTabletTask(MemTracker* mem_tracker, const TAlterTabletReqV2& alter_tablet_request, int64_t signature,
                          const TTaskType::type task_type, vector<string>* error_msgs, const string& process_name);
    ~EngineAlterTabletTask() override = default;

private:
    std::unique_ptr<MemTracker> _mem_tracker;

    const TAlterTabletReqV2& _alter_tablet_req;
    int64_t _signature;
    const TTaskType::type _task_type;
    vector<string>* _error_msgs;
    const string& _process_name;

}; // EngineTask

} // namespace starrocks
#endif //STARROCKS_BE_SRC_OLAP_TASK_ENGINE_ALTER_TABLET_TASK_H
