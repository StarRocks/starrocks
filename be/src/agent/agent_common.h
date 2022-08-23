// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    RECOVER_TABLET, // Deprecated
    UPDATE_TABLET_META_INFO
};

template <TaskWorkerType type>
struct TaskWorkerTypeTraits {
    bool has_req = false;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CREATE_TABLE> {
    using TReq = TCreateTabletReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::DROP_TABLE> {
    using TReq = TDropTabletReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::PUSH> {
    using TReq = TPushReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::REALTIME_PUSH> {
    using TReq = TPushReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::PUBLISH_VERSION> {
    using TReq = TPublishVersionRequest;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CLEAR_TRANSACTION_TASK> {
    using TReq = TClearTransactionTaskRequest;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::DELETE> {
    using TReq = TPushReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::ALTER_TABLE> {
    using TReq = TAlterTabletReqV2;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CLONE> {
    using TReq = TCloneReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::STORAGE_MEDIUM_MIGRATE> {
    using TReq = TStorageMediumMigrateReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CHECK_CONSISTENCY> {
    using TReq = TCheckConsistencyReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::UPLOAD> {
    using TReq = TUploadReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::DOWNLOAD> {
    using TReq = TDownloadReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::MAKE_SNAPSHOT> {
    using TReq = TSnapshotRequest;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::RELEASE_SNAPSHOT> {
    using TReq = TReleaseSnapshotRequest;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::MOVE> {
    using TReq = TMoveDirReq;
    bool has_req = true;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::UPDATE_TABLET_META_INFO> {
    using TReq = TUpdateTabletMetaInfoReq;
    bool has_req = true;
};

template <TaskWorkerType TaskType>
struct AgentTaskRequest {
    TAgentServiceVersion::type protocol_version = TAgentServiceVersion::V1;
    TTaskType::type task_type = TTaskType::NUM_TASK_TYPE;
    int64_t signature = 0;
    TPriority::type priority = TPriority::NORMAL;
    int64_t recv_time = 0;
    typename TaskWorkerTypeTraits<TaskType>::TReq task_req;

    _TAgentTaskRequest__isset isset;
};

#define AgentTaskRequestNoTReq(TaskType)                                        \
    template <>                                                                 \
    struct AgentTaskRequest<TaskType> {                                         \
        TAgentServiceVersion::type protocol_version = TAgentServiceVersion::V1; \
        TTaskType::type task_type = TTaskType::NUM_TASK_TYPE;                   \
        int64_t signature = 0;                                                  \
        TPriority::type priority = TPriority::NORMAL;                           \
        int64_t recv_time = 0;                                                  \
        _TAgentTaskRequest__isset isset;                                        \
    }

AgentTaskRequestNoTReq(TaskWorkerType::REPORT_OLAP_TABLE);
AgentTaskRequestNoTReq(TaskWorkerType::REPORT_DISK_STATE);
AgentTaskRequestNoTReq(TaskWorkerType::REPORT_TASK);
AgentTaskRequestNoTReq(TaskWorkerType::REPORT_WORKGROUP);

} // namespace starrocks