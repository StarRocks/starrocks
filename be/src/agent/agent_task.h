// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "agent/task_worker_pool.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"

namespace starrocks {

void run_drop_tablet_task(std::shared_ptr<DropTabletAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_create_tablet_task(std::shared_ptr<CreateTabletAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_alter_tablet_task(std::shared_ptr<AlterTabletAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_clear_transaction_task(std::shared_ptr<ClearTransactionAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_clone_task(std::shared_ptr<CloneAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_storage_medium_migrate_task(std::shared_ptr<StorageMediumMigrateTaskRequest> agent_task_req,
                                     ExecEnv* exec_env);
void run_check_consistency_task(std::shared_ptr<CheckConsistencyTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_upload_task(std::shared_ptr<UploadAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_download_task(std::shared_ptr<DownloadAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_make_snapshot_task(std::shared_ptr<SnapshotAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_release_snapshot_task(std::shared_ptr<ReleaseSnapshotAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_move_dir_task(std::shared_ptr<MoveDirAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
void run_update_meta_info_task(std::shared_ptr<UpdateTabletMetaInfoAgentTaskRequest> agent_task_req, ExecEnv* exec_env);
} // namespace starrocks