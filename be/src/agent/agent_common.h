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

#define APPLY_FOR_TASK_WORKER_WITH_BODY_VARIANTS(M)                                                       \
    M(CREATE_TABLE, create_tablet_req, _create_tablet_worker_thread_callback)                             \
    M(DROP_TABLE, drop_tablet_req, _drop_tablet_worker_thread_callback)                                   \
    M(PUSH, push_req, _push_worker_thread_callback)                                                       \
    M(PUBLISH_VERSION, publish_version_req, _publish_version_worker_thread_callback)                      \
    M(CLEAR_TRANSACTION_TASK, clear_transaction_task_req, _clear_transaction_task_worker_thread_callback) \
    M(DELETE, push_req, _delete_worker_thread_callback)                                                   \
    M(ALTER_TABLE, alter_tablet_req_v2, _alter_tablet_worker_thread_callback)                             \
    M(CLONE, clone_req, _clone_worker_thread_callback)                                                    \
    M(STORAGE_MEDIUM_MIGRATE, storage_medium_migrate_req, )                                               \
    M(CHECK_CONSISTENCY, check_consistency_req, _storage_medium_migrate_worker_thread_callback)           \
    M(UPLOAD, upload_req, _upload_worker_thread_callback)                                                 \
    M(DOWNLOAD, download_req, _download_worker_thread_callback)                                           \
    M(MAKE_SNAPSHOT, snapshot_req, _make_snapshot_thread_callback)                                        \
    M(RELEASE_SNAPSHOT, release_snapshot_req, _release_snapshot_thread_callback)                          \
    M(MOVE, move_dir_req, _move_dir_thread_callback)                                                      \
    M(UPDATE_TABLET_META_INFO, update_tablet_meta_info_req, _update_tablet_meta_worker_thread_callback)

#define APPLY_FOR_TASK_WORKER_WITHOUT_BODY_VARIANTS(M)                                   \
    M(TaskWorkerType::REPORT_TASK, NOP, _report_task_worker_thread_callback)             \
    M(TaskWorkerType::REPORT_DISK_STATE, NOP, _report_disk_state_worker_thread_callback) \
    M(TaskWorkerType::REPORT_OLAP_TABLE, NOP, _report_task_worker_thread_callback)       \
    M(TaskWorkerType::REPORT_WORKGROUP, NOP, _report_workgroup_thread_callback)

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