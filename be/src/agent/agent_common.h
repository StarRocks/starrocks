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

#define APPLY_FOR_TASK_WORKER_WITH_BODY_VARIANTS(M)                                                     \
    M(CreateTable, CREATE_TABLE, create_tablet_req, create_tablet)                                      \
    M(DropTable, DROP_TABLE, drop_tablet_req, drop_tablet)                                              \
    M(Push, PUSH, push_req, push)                                                                       \
    M(PublishVersion, PUBLISH_VERSION, publish_version_req, publish_version)                            \
    M(ClearTransactionTask, CLEAR_TRANSACTION_TASK, clear_transaction_task_req, clear_transaction_task) \
    M(Delete, DELETE, push_req, delete_tablet)                                                          \
    M(AlterTable, ALTER_TABLE, alter_tablet_req_v2, alter_tablet)                                       \
    M(Clone, CLONE, clone_req, clone)                                                                   \
    M(StorageMediumMigrate, STORAGE_MEDIUM_MIGRATE, storage_medium_migrate_req, storage_medium_migrate) \
    M(CheckConsistency, CHECK_CONSISTENCY, check_consistency_req, check_consistency)                    \
    M(Upload, UPLOAD, upload_req, upload)                                                               \
    M(Download, DOWNLOAD, download_req, download)                                                       \
    M(MakeSnapshot, MAKE_SNAPSHOT, snapshot_req, make_snapshot)                                         \
    M(ReleaseSnapshot, RELEASE_SNAPSHOT, release_snapshot_req, release_snapshot)                        \
    M(Move, MOVE, move_dir_req, move_dir)                                                               \
    M(UpdateTabletMetaInfo, UPDATE_TABLET_META_INFO, update_tablet_meta_info_req, update_tablet_meta)

#define APPLY_FOR_TASK_WORKER_WITHOUT_BODY_VARIANTS(M)            \
    M(ReportTask, REPORT_TASK, NOP, report_task)                  \
    M(ReportDiskState, REPORT_DISK_STATE, NOP, report_disk_state) \
    M(ReportOlapTable, REPORT_OLAP_TABLE, NOP, report_tablet)     \
    M(ReportWorkgroup, REPORT_WORKGROUP, NOP, report_workgroup)

template <TaskWorkerType type>
struct TaskWorkerTypeTraits {};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CREATE_TABLE> {
    using TReq = TCreateTabletReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::DROP_TABLE> {
    using TReq = TDropTabletReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::PUSH> {
    using TReq = TPushReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::REALTIME_PUSH> {
    using TReq = TPushReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::PUBLISH_VERSION> {
    using TReq = TPublishVersionRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CLEAR_TRANSACTION_TASK> {
    using TReq = TClearTransactionTaskRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::DELETE> {
    using TReq = TPushReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::ALTER_TABLE> {
    using TReq = TAlterTabletReqV2;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CLONE> {
    using TReq = TCloneReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::STORAGE_MEDIUM_MIGRATE> {
    using TReq = TStorageMediumMigrateReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::CHECK_CONSISTENCY> {
    using TReq = TCheckConsistencyReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::UPLOAD> {
    using TReq = TUploadReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::DOWNLOAD> {
    using TReq = TDownloadReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::MAKE_SNAPSHOT> {
    using TReq = TSnapshotRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::RELEASE_SNAPSHOT> {
    using TReq = TReleaseSnapshotRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::MOVE> {
    using TReq = TMoveDirReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerType::UPDATE_TABLET_META_INFO> {
    using TReq = TUpdateTabletMetaInfoReq;
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