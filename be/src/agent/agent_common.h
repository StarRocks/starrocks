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

#pragma once

#include "gen_cpp/AgentService_types.h"

namespace starrocks {

enum class TaskWorkerType {
    CREATE_TABLE,
    DROP_TABLE,
    PUSH,
    REALTIME_PUSH,
    PUBLISH_VERSION,
    CLEAR_ALTER_TASK, // Deprecated
    CLEAR_TRANSACTION_TASK,
    DELETE,
    ALTER_TABLE,
    QUERY_SPLIT_KEY, // Deprecated
    CLONE,
    STORAGE_MEDIUM_MIGRATE,
    CHECK_CONSISTENCY,
    REPORT_TASK,
    REPORT_DISK_STATE,
    REPORT_OLAP_TABLE,
    REPORT_WORKGROUP,
    UPLOAD,
    DOWNLOAD,
    MAKE_SNAPSHOT,
    RELEASE_SNAPSHOT,
    MOVE,
    RECOVER_TABLET,
    UPDATE_TABLET_META_INFO
};

template <class TReq>
struct AgentTaskRequestWithReqBody {
    AgentTaskRequestWithReqBody(const TAgentTaskRequest& task, const TReq& t_req, time_t ts) {
        isset = task.__isset;
        protocol_version = task.protocol_version;
        task_type = task.task_type;
        signature = task.signature;
        priority = task.priority;
        recv_time = ts;
        isset.recv_time = true;
        task_req = t_req;
    }

    _TAgentTaskRequest__isset isset;
    TAgentServiceVersion::type protocol_version;
    TTaskType::type task_type;
    int64_t signature;
    TPriority::type priority;
    int64_t recv_time;
    TReq task_req;
};

struct AgentTaskRequestWithoutReqBody {
    explicit AgentTaskRequestWithoutReqBody(const TAgentTaskRequest& task, time_t ts) {
        isset = task.__isset;
        protocol_version = task.protocol_version;
        task_type = task.task_type;
        signature = task.signature;
        priority = task.priority;
        recv_time = ts;
        isset.recv_time = true;
    }

    _TAgentTaskRequest__isset isset;
    TAgentServiceVersion::type protocol_version;
    TTaskType::type task_type;
    int64_t signature;
    TPriority::type priority;
    int64_t recv_time;
};

using CreateTabletAgentTaskRequest = AgentTaskRequestWithReqBody<TCreateTabletReq>;
using DropTabletAgentTaskRequest = AgentTaskRequestWithReqBody<TDropTabletReq>;
using PushReqAgentTaskRequest = AgentTaskRequestWithReqBody<TPushReq>;
using PublishVersionAgentTaskRequest = AgentTaskRequestWithReqBody<TPublishVersionRequest>;
using ClearTransactionAgentTaskRequest = AgentTaskRequestWithReqBody<TClearTransactionTaskRequest>;
using AlterTabletAgentTaskRequest = AgentTaskRequestWithReqBody<TAlterTabletReqV2>;
using CloneAgentTaskRequest = AgentTaskRequestWithReqBody<TCloneReq>;
using StorageMediumMigrateTaskRequest = AgentTaskRequestWithReqBody<TStorageMediumMigrateReq>;
using CheckConsistencyTaskRequest = AgentTaskRequestWithReqBody<TCheckConsistencyReq>;
using UploadAgentTaskRequest = AgentTaskRequestWithReqBody<TUploadReq>;
using DownloadAgentTaskRequest = AgentTaskRequestWithReqBody<TDownloadReq>;
using SnapshotAgentTaskRequest = AgentTaskRequestWithReqBody<TSnapshotRequest>;
using ReleaseSnapshotAgentTaskRequest = AgentTaskRequestWithReqBody<TReleaseSnapshotRequest>;
using MoveDirAgentTaskRequest = AgentTaskRequestWithReqBody<TMoveDirReq>;
using UpdateTabletMetaInfoAgentTaskRequest = AgentTaskRequestWithReqBody<TUpdateTabletMetaInfoReq>;
using DropAutoIncrementMapAgentTaskRequest = AgentTaskRequestWithReqBody<TDropAutoIncrementMapReq>;

} // namespace starrocks
