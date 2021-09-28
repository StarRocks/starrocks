// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_publish_version_task.h

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

#ifndef STARROCKS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H
#define STARROCKS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H

#include "gen_cpp/AgentService_types.h"
#include "storage/olap_define.h"
#include "storage/task/engine_task.h"

namespace starrocks {

class EnginePublishVersionTask : public EngineTask {
public:
    EnginePublishVersionTask(TPublishVersionRequest& publish_version_req, vector<TTabletId>* error_tablet_ids);
    ~EnginePublishVersionTask() override = default;

    OLAPStatus finish() override;

private:
    const TPublishVersionRequest& _publish_version_req;
    vector<TTabletId>* _error_tablet_ids;
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H
