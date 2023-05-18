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

#include "agent/task_worker_pool.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"

namespace starrocks {

void run_drop_tablet_task(const std::shared_ptr<DropTabletAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_create_tablet_task(const std::shared_ptr<CreateTabletAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_alter_tablet_task(const std::shared_ptr<AlterTabletAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_clear_transaction_task(const std::shared_ptr<ClearTransactionAgentTaskRequest>& agent_task_req,
                                ExecEnv* exec_env);
void run_clone_task(const std::shared_ptr<CloneAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_storage_medium_migrate_task(const std::shared_ptr<StorageMediumMigrateTaskRequest>& agent_task_req,
                                     ExecEnv* exec_env);
void run_check_consistency_task(const std::shared_ptr<CheckConsistencyTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_compaction_task(const std::shared_ptr<CompactionTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_upload_task(const std::shared_ptr<UploadAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_download_task(const std::shared_ptr<DownloadAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_make_snapshot_task(const std::shared_ptr<SnapshotAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_release_snapshot_task(const std::shared_ptr<ReleaseSnapshotAgentTaskRequest>& agent_task_req,
                               ExecEnv* exec_env);
void run_move_dir_task(const std::shared_ptr<MoveDirAgentTaskRequest>& agent_task_req, ExecEnv* exec_env);
void run_update_meta_info_task(const std::shared_ptr<UpdateTabletMetaInfoAgentTaskRequest>& agent_task_req,
                               ExecEnv* exec_env);
void run_drop_auto_increment_map_task(const std::shared_ptr<DropAutoIncrementMapAgentTaskRequest>& agent_task_req,
                                      ExecEnv* exec_env);
} // namespace starrocks
