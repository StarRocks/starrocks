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

#define APPLY_FOR_TASK_WORKER_WITH_BODY_VARIANTS(M)                                                           \
    M(CreateTable, CREATE_TABLE, TCreateTabletReq, create_tablet_req, create_tablet)                          \
    M(DropTable, DROP_TABLE, TDropTabletReq, drop_tablet_req, drop_tablet)                                    \
    M(Push, PUSH, TPushReq, push_req, push)                                                                   \
    M(PublishVersion, PUBLISH_VERSION, TPublishVersionRequest, publish_version_req, publish_version)          \
    M(ClearTransactionTask, CLEAR_TRANSACTION_TASK, TClearTransactionTaskRequest, clear_transaction_task_req, \
      clear_transaction_task)                                                                                 \
    M(Delete, DELETE, TPushReq, push_req, delete_tablet)                                                      \
    M(AlterTable, ALTER_TABLE, TAlterTabletReqV2, alter_tablet_req_v2, alter_tablet)                          \
    M(Clone, CLONE, TCloneReq, clone_req, clone)                                                              \
    M(StorageMediumMigrate, STORAGE_MEDIUM_MIGRATE, TStorageMediumMigrateReq, storage_medium_migrate_req,     \
      storage_medium_migrate)                                                                                 \
    M(CheckConsistency, CHECK_CONSISTENCY, TCheckConsistencyReq, check_consistency_req, check_consistency)    \
    M(Upload, UPLOAD, TUploadReq, upload_req, upload)                                                         \
    M(Download, DOWNLOAD, TDownloadReq, download_req, download)                                               \
    M(MakeSnapshot, MAKE_SNAPSHOT, TSnapshotRequest, snapshot_req, make_snapshot)                             \
    M(ReleaseSnapshot, RELEASE_SNAPSHOT, TReleaseSnapshotRequest, release_snapshot_req, release_snapshot)     \
    M(Move, MOVE, TMoveDirReq, move_dir_req, move_dir)                                                        \
    M(UpdateTabletMetaInfo, UPDATE_TABLET_META_INFO, TUpdateTabletMetaInfoReq, update_tablet_meta_info_req,   \
      update_tablet_meta)

#define APPLY_FOR_TASK_WORKER_WITHOUT_BODY_VARIANTS(M)                 \
    M(ReportTask, REPORT_TASK, NOP, NOP, report_task)                  \
    M(ReportDiskState, REPORT_DISK_STATE, NOP, NOP, report_disk_state) \
    M(ReportOlapTable, REPORT_OLAP_TABLE, NOP, NOP, report_tablet)     \
    M(ReportWorkgroup, REPORT_WORKGROUP, NOP, NOP, report_workgroup)

template <TaskWorkerType TaskType>
struct AgentTaskRequest {};

#define M(WORKER_POOL_NAME, WORKER_TYPE, T_ITEM_TYPE, T_ITEM_NAME, CALLBACK)    \
    template <>                                                                 \
    struct AgentTaskRequest<TaskWorkerType::WORKER_TYPE> {                      \
        TAgentServiceVersion::type protocol_version = TAgentServiceVersion::V1; \
        TTaskType::type task_type = TTaskType::NUM_TASK_TYPE;                   \
        int64_t signature = 0;                                                  \
        TPriority::type priority = TPriority::NORMAL;                           \
        int64_t recv_time = 0;                                                  \
        T_ITEM_TYPE task_req;                                                   \
        _TAgentTaskRequest__isset isset;                                        \
    };

APPLY_FOR_TASK_WORKER_WITH_BODY_VARIANTS(M)
#undef M

#define M(WORKER_POOL_NAME, WORKER_TYPE, T_ITEM_TYPE, T_ITEM_NAME, CALLBACK)    \
    template <>                                                                 \
    struct AgentTaskRequest<TaskWorkerType::WORKER_TYPE> {                      \
        TAgentServiceVersion::type protocol_version = TAgentServiceVersion::V1; \
        TTaskType::type task_type = TTaskType::NUM_TASK_TYPE;                   \
        int64_t signature = 0;                                                  \
        TPriority::type priority = TPriority::NORMAL;                           \
        int64_t recv_time = 0;                                                  \
        _TAgentTaskRequest__isset isset;                                        \
    };

APPLY_FOR_TASK_WORKER_WITHOUT_BODY_VARIANTS(M)
#undef M

} // namespace starrocks