// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_task.h

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

#ifndef STARROCKS_BE_SRC_OLAP_TASK_ENGINE_TASK_H
#define STARROCKS_BE_SRC_OLAP_TASK_ENGINE_TASK_H

#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineTask {
public:
    // use olap_status not agent_status, because the task is very close to engine
    virtual OLAPStatus prepare() { return OLAP_SUCCESS; }
    virtual OLAPStatus execute() { return OLAP_SUCCESS; }
    virtual OLAPStatus finish() { return OLAP_SUCCESS; }
    virtual OLAPStatus cancel() { return OLAP_SUCCESS; }
    virtual void get_related_tablets(std::vector<TabletInfo>* tablet_infos) {}
    virtual ~EngineTask() = default;
};

} // end namespace starrocks
#endif //STARROCKS_BE_SRC_OLAP_TASK_ENGINE_TASK_H
