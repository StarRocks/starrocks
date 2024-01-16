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

#include "agent/agent_task.h"

#include "agent/agent_common.h"
#include "agent/finish_task.h"
#include "agent/task_signatures_manager.h"
#include "boost/lexical_cast.hpp"
#include "common/status.h"
#include "io/io_profiler.h"
#include "runtime/current_thread.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/schema_change.h"
#include "storage/lake/tablet_manager.h"
#include "storage/replication_txn_manager.h"
#include "storage/snapshot_manager.h"
#include "storage/tablet_manager.h"
#include "storage/task/engine_alter_tablet_task.h"
#include "storage/task/engine_checksum_task.h"
#include "storage/task/engine_clone_task.h"
#include "storage/task/engine_manual_compaction_task.h"
#include "storage/task/engine_storage_migration_task.h"
#include "storage/txn_manager.h"
#include "storage/update_manager.h"

namespace starrocks {

extern std::atomic<int64_t> g_report_version;

static AgentStatus get_tablet_info(TTabletId tablet_id, TSchemaHash schema_hash, int64_t signature,
                                   TTabletInfo* tablet_info) {
    AgentStatus status = STARROCKS_SUCCESS;

    tablet_info->__set_tablet_id(tablet_id);
    tablet_info->__set_schema_hash(schema_hash);
    Status st = StorageEngine::instance()->tablet_manager()->report_tablet_info(tablet_info);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to get tablet info, status=" << st.to_string() << " signature=" << signature;
        status = STARROCKS_ERROR;
    }
    return status;
}

static void alter_tablet(const TAlterTabletReqV2& agent_task_req, int64_t signature,
                         TFinishTaskRequest* finish_task_request) {
    TStatus task_status;
    std::vector<std::string> error_msgs;

    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    TTabletId new_tablet_id;
    TSchemaHash new_schema_hash = 0;
    new_tablet_id = agent_task_req.new_tablet_id;
    new_schema_hash = agent_task_req.new_schema_hash;
    EngineAlterTabletTask engine_task(ExecEnv::GetInstance()->schema_change_mem_tracker(), agent_task_req);
    Status sc_status = StorageEngine::instance()->execute_task(&engine_task);
    AgentStatus status;
    if (!sc_status.ok()) {
        status = STARROCKS_ERROR;
    } else {
        status = STARROCKS_SUCCESS;
    }

    std::string alter_msg_head =
            strings::Substitute("[Alter Job:$0, tablet:$1]: ", agent_task_req.job_id, agent_task_req.base_tablet_id);
    if (status == STARROCKS_SUCCESS) {
        g_report_version.fetch_add(1, std::memory_order_relaxed);
        LOG(INFO) << alter_msg_head << "alter finished. signature: " << signature;
    }

    // Return result to fe
    finish_task_request->__set_backend(BackendOptions::get_localBackend());
    finish_task_request->__set_report_version(g_report_version.load(std::memory_order_relaxed));
    finish_task_request->__set_task_type(TTaskType::ALTER);
    finish_task_request->__set_signature(signature);

    std::vector<TTabletInfo> finish_tablet_infos;
    if (status == STARROCKS_SUCCESS) {
        TTabletInfo& tablet_info = finish_tablet_infos.emplace_back();
        if (agent_task_req.tablet_type != TTabletType::TABLET_TYPE_LAKE) {
            status = get_tablet_info(new_tablet_id, new_schema_hash, signature, &tablet_info);
        } else {
            tablet_info.__set_tablet_id(new_tablet_id);
            // Following are unused for LakeTablet but they are defined as required thrift fields, have to init them.
            tablet_info.__set_schema_hash(0);
            tablet_info.__set_version(0);
            tablet_info.__set_row_count(0);
            tablet_info.__set_data_size(0);
            tablet_info.__set_version_count(1);
        }

        LOG_IF(WARNING, status != STARROCKS_SUCCESS)
                << alter_msg_head << "alter success, but get new tablet info failed."
                << "tablet_id: " << new_tablet_id << ", schema_hash: " << new_schema_hash
                << ", signature: " << signature;
    }

    if (status == STARROCKS_SUCCESS) {
        swap(finish_tablet_infos, finish_task_request->finish_tablet_infos);
        finish_task_request->__isset.finish_tablet_infos = true;
        LOG(INFO) << alter_msg_head << "alter success. signature: " << signature;
        error_msgs.push_back("alter success");
        task_status.__set_status_code(TStatusCode::OK);
    } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
        LOG(WARNING) << alter_msg_head << "alter table request task type invalid. "
                     << "signature:" << signature;
        error_msgs.emplace_back("alter table request new tablet id or schema count invalid.");
        task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
    } else {
        LOG(WARNING) << alter_msg_head << "alter failed. signature: " << signature;
        error_msgs.emplace_back("alter failed");
        error_msgs.emplace_back("status: " + print_agent_status(status));
        error_msgs.emplace_back(sc_status.get_error_msg());
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
    }

    task_status.__set_error_msgs(error_msgs);
    finish_task_request->__set_task_status(task_status);
}

static void unify_finish_agent_task(TStatusCode::type status_code, const std::vector<std::string>& error_msgs,
                                    const TTaskType::type task_type, int64_t signature, bool report_version = false) {
    TStatus task_status;
    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(task_type);
    finish_task_request.__set_signature(signature);
    if (report_version) {
        finish_task_request.__set_report_version(g_report_version.load(std::memory_order_relaxed));
    }
    finish_task_request.__set_task_status(task_status);

    finish_task(finish_task_request);
    size_t task_queue_size = remove_task_info(task_type, signature);
    LOG(INFO) << "Remove task success. type=" << task_type << ", signature=" << signature
              << ", task_count_in_queue=" << task_queue_size;
}

void run_drop_tablet_task(const std::shared_ptr<DropTabletAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    StarRocksMetrics::instance()->clone_requests_total.increment(1);

    const TDropTabletReq& drop_tablet_req = agent_task_req->task_req;

    bool force_drop = drop_tablet_req.__isset.force && drop_tablet_req.force;
    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    auto dropped_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(drop_tablet_req.tablet_id);
    if (dropped_tablet != nullptr) {
        if (!config::enable_drop_tablet_if_unfinished_txn) {
            int64_t partition_id;
            std::set<int64_t> transaction_ids;
            {
                std::lock_guard push_lock(dropped_tablet->get_push_lock());
                StorageEngine::instance()->txn_manager()->get_tablet_related_txns(
                        drop_tablet_req.tablet_id, drop_tablet_req.schema_hash, dropped_tablet->tablet_uid(),
                        &partition_id, &transaction_ids);
            }
            if (!transaction_ids.empty()) {
                std::stringstream ss;
                ss << "Failed to drop tablet when there is unfinished txns."
                   << "tablet_id: " << drop_tablet_req.tablet_id << ", txn_ids: ";
                for (auto itr = transaction_ids.begin(); itr != transaction_ids.end(); itr++) {
                    ss << *itr;
                    if (std::next(itr) != transaction_ids.end()) {
                        ss << ",";
                    }
                }
                LOG(WARNING) << ss.str();
                error_msgs.emplace_back(ss.str());
                status_code = TStatusCode::RUNTIME_ERROR;
                unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
                return;
            }
        }

        TabletDropFlag flag = force_drop ? kDeleteFiles : kMoveFilesToTrash;
        auto st = StorageEngine::instance()->tablet_manager()->drop_tablet(drop_tablet_req.tablet_id, flag);
        if (!st.ok()) {
            LOG(WARNING) << "drop table failed! signature: " << agent_task_req->signature;
            error_msgs.emplace_back("drop table failed!");
            error_msgs.emplace_back("drop tablet " + st.get_error_msg());
            status_code = TStatusCode::RUNTIME_ERROR;
        }
        // if tablet is dropped by fe, then the related txn should also be removed
        StorageEngine::instance()->txn_manager()->force_rollback_tablet_related_txns(
                dropped_tablet->data_dir()->get_meta(), drop_tablet_req.tablet_id, drop_tablet_req.schema_hash,
                dropped_tablet->tablet_uid());
    }

    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
}

void run_create_tablet_task(const std::shared_ptr<CreateTabletAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const auto& create_tablet_req = agent_task_req->task_req;
    TFinishTaskRequest finish_task_request;
    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    auto tablet_type = create_tablet_req.tablet_type;
    Status create_status;
    if (tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        create_status = exec_env->lake_tablet_manager()->create_tablet(create_tablet_req);
    } else {
        create_status = StorageEngine::instance()->create_tablet(create_tablet_req);
    }
    if (!create_status.ok()) {
        LOG(WARNING) << "create table failed. status: " << create_status.to_string()
                     << ", signature: " << agent_task_req->signature;
        status_code = TStatusCode::RUNTIME_ERROR;
        if (tablet_type == TTabletType::TABLET_TYPE_LAKE) {
            error_msgs.emplace_back(create_status.to_string(false));
        } else {
            error_msgs.emplace_back("create tablet " + create_status.get_error_msg());
        }
    } else if (create_tablet_req.tablet_type != TTabletType::TABLET_TYPE_LAKE) {
        g_report_version.fetch_add(1, std::memory_order_relaxed);
        // get path hash of the created tablet
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(create_tablet_req.tablet_id);
        DCHECK(tablet != nullptr);
        TTabletInfo& tablet_info = finish_task_request.finish_tablet_infos.emplace_back();
        finish_task_request.__isset.finish_tablet_infos = true;
        tablet_info.tablet_id = tablet->tablet_id();
        tablet_info.schema_hash = tablet->schema_hash();
        tablet_info.version = create_tablet_req.version;
        tablet_info.row_count = 0;
        tablet_info.data_size = 0;
        tablet_info.__set_path_hash(tablet->data_dir()->path_hash());
    }

    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature, true);
}

void run_alter_tablet_task(const std::shared_ptr<AlterTabletAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    int64_t signatrue = agent_task_req->signature;
    std::string alter_msg_head = strings::Substitute("[Alter Job:$0, tablet:$1]: ", agent_task_req->task_req.job_id,
                                                     agent_task_req->task_req.base_tablet_id);
    LOG(INFO) << alter_msg_head << "get alter table task, signature: " << agent_task_req->signature;
    bool is_task_timeout = false;
    if (agent_task_req->isset.recv_time) {
        int64_t time_elapsed = time(nullptr) - agent_task_req->recv_time;
        if (time_elapsed > config::report_task_interval_seconds * 20) {
            LOG(INFO) << "task elapsed " << time_elapsed << " seconds since it is inserted to queue, it is timeout";
            is_task_timeout = true;
        }
    }
    if (!is_task_timeout) {
        TFinishTaskRequest finish_task_request;
        TTaskType::type task_type = agent_task_req->task_type;
        if (task_type == TTaskType::ALTER) {
            alter_tablet(agent_task_req->task_req, signatrue, &finish_task_request);
        }
        finish_task(finish_task_request);
    }
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

void run_clear_transaction_task(const std::shared_ptr<ClearTransactionAgentTaskRequest>& agent_task_req,
                                ExecEnv* exec_env) {
    const TClearTransactionTaskRequest& clear_transaction_task_req = agent_task_req->task_req;
    LOG(INFO) << "get clear transaction task task, signature:" << agent_task_req->signature
              << ", txn_id: " << clear_transaction_task_req.transaction_id
              << ", partition id size: " << clear_transaction_task_req.partition_id.size();

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    if (clear_transaction_task_req.transaction_id > 0) {
        // transaction_id should be greater than zero.
        // If it is not greater than zero, no need to execute
        // the following clear_transaction_task() function.
        if (clear_transaction_task_req.__isset.txn_type &&
            clear_transaction_task_req.txn_type == TTxnType::TXN_REPLICATION) {
            StorageEngine::instance()->replication_txn_manager()->clear_txn(clear_transaction_task_req.transaction_id);
        } else {
            if (!clear_transaction_task_req.partition_id.empty()) {
                StorageEngine::instance()->clear_transaction_task(clear_transaction_task_req.transaction_id,
                                                                  clear_transaction_task_req.partition_id);
            } else {
                StorageEngine::instance()->clear_transaction_task(clear_transaction_task_req.transaction_id);
            }
        }
        LOG(INFO) << "finish to clear transaction task. signature:" << agent_task_req->signature
                  << ", txn_id: " << clear_transaction_task_req.transaction_id;
    } else {
        LOG(WARNING) << "invalid txn_id: " << clear_transaction_task_req.transaction_id
                     << ", signature: " << agent_task_req->signature;
        error_msgs.emplace_back("invalid txn_id: " + std::to_string(clear_transaction_task_req.transaction_id));
    }

    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
}

void run_clone_task(const std::shared_ptr<CloneAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TCloneReq& clone_req = agent_task_req->task_req;
    AgentStatus status = STARROCKS_SUCCESS;

    auto scope = IOProfiler::scope(IOProfiler::TAG_CLONE, clone_req.tablet_id);

    // Return result to fe
    TStatus task_status;
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    std::vector<TTabletInfo> tablet_infos;
    if (clone_req.__isset.is_local && clone_req.is_local) {
        DataDir* dest_store = StorageEngine::instance()->get_store(clone_req.dest_path_hash);
        if (dest_store == nullptr) {
            LOG(WARNING) << "fail to get dest store. path_hash:" << clone_req.dest_path_hash;
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            EngineStorageMigrationTask engine_task(clone_req.tablet_id, clone_req.schema_hash, dest_store);
            Status res = StorageEngine::instance()->execute_task(&engine_task);
            if (!res.ok()) {
                status_code = TStatusCode::RUNTIME_ERROR;
                LOG(WARNING) << "local tablet migration failed. status: " << res
                             << ", signature: " << agent_task_req->signature;
                error_msgs.emplace_back("local tablet migration failed. error message: " + res.get_error_msg());
            } else {
                LOG(INFO) << "local tablet migration succeeded. status: " << res
                          << ", signature: " << agent_task_req->signature;

                TTabletInfo tablet_info;
                AgentStatus status = TaskWorkerPoolBase::get_tablet_info(clone_req.tablet_id, clone_req.schema_hash,
                                                                         agent_task_req->signature, &tablet_info);
                if (status != STARROCKS_SUCCESS) {
                    LOG(WARNING) << "local tablet migration succeeded, but get tablet info failed"
                                 << ". status: " << status << ", signature: " << agent_task_req->signature;
                } else {
                    tablet_infos.push_back(tablet_info);
                }
                finish_task_request.__set_finish_tablet_infos(tablet_infos);
            }
        }
    } else {
        EngineCloneTask engine_task(ExecEnv::GetInstance()->clone_mem_tracker(), clone_req, agent_task_req->signature,
                                    &error_msgs, &tablet_infos, &status);
        Status res = StorageEngine::instance()->execute_task(&engine_task);
        if (!res.ok()) {
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "clone failed. status:" << res << ", signature:" << agent_task_req->signature;
            error_msgs.emplace_back("clone failed.");
        } else {
            if (status != STARROCKS_SUCCESS && status != STARROCKS_CREATE_TABLE_EXIST) {
                StarRocksMetrics::instance()->clone_requests_failed.increment(1);
                status_code = TStatusCode::RUNTIME_ERROR;
                LOG(WARNING) << "clone failed. signature: " << agent_task_req->signature;
                error_msgs.emplace_back("clone failed.");
            } else {
                LOG(INFO) << "clone success, set tablet infos. status:" << status
                          << ", signature:" << agent_task_req->signature;
                finish_task_request.__set_finish_tablet_infos(tablet_infos);
            }
        }
    }

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);
    finish_task_request.__set_task_status(task_status);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

void run_storage_medium_migrate_task(const std::shared_ptr<StorageMediumMigrateTaskRequest>& agent_task_req,
                                     ExecEnv* exec_env) {
    const TStorageMediumMigrateReq& storage_medium_migrate_req = agent_task_req->task_req;

    auto scope = IOProfiler::scope(IOProfiler::TAG_CLONE, storage_medium_migrate_req.tablet_id);

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);

    do {
        TTabletId tablet_id = storage_medium_migrate_req.tablet_id;
        TSchemaHash schema_hash = storage_medium_migrate_req.schema_hash;
        TStorageMedium::type storage_medium = storage_medium_migrate_req.storage_medium;

        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << "can't find tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
            status_code = TStatusCode::RUNTIME_ERROR;
            break;
        }

        TStorageMedium::type src_storage_medium = tablet->data_dir()->storage_medium();
        if (src_storage_medium == storage_medium) {
            // status code is ok
            LOG(INFO) << "tablet is already on specified storage medium. "
                      << "storage_medium=" << storage_medium;
            break;
        }

        uint32_t count = StorageEngine::instance()->available_storage_medium_type_count();
        if (count <= 1) {
            LOG(INFO) << "available storage medium type count is less than 1, "
                      << "no need to migrate. count=" << count;
            status_code = TStatusCode::RUNTIME_ERROR;
            break;
        }

        // get a random store of specified storage medium
        auto stores = StorageEngine::instance()->get_stores_for_create_tablet(storage_medium);
        if (stores.empty()) {
            LOG(WARNING) << "fail to get path for migration. storage_medium=" << storage_medium;
            status_code = TStatusCode::RUNTIME_ERROR;
            break;
        }

        EngineStorageMigrationTask engine_task(tablet_id, schema_hash, stores[0]);
        Status res = StorageEngine::instance()->execute_task(&engine_task);
        if (!res.ok()) {
            LOG(WARNING) << "storage media migrate failed. status: " << res
                         << ", signature: " << agent_task_req->signature;
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            // status code is ok
            LOG(INFO) << "storage media migrate success. "
                      << "signature:" << agent_task_req->signature;

            std::vector<TTabletInfo> tablet_infos;
            TTabletInfo tablet_info;
            AgentStatus status = get_tablet_info(tablet_id, schema_hash, agent_task_req->signature, &tablet_info);
            if (status != STARROCKS_SUCCESS) {
                LOG(WARNING) << "storage migrate success, but get tablet info failed"
                             << ". status:" << status << ", signature:" << agent_task_req->signature;
            } else {
                tablet_infos.push_back(tablet_info);
            }
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        }
    } while (false);

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);
    finish_task_request.__set_task_status(task_status);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

void run_check_consistency_task(const std::shared_ptr<CheckConsistencyTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TCheckConsistencyReq& check_consistency_req = agent_task_req->task_req;
    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;
    uint32_t checksum = 0;

    MemTracker* mem_tracker = ExecEnv::GetInstance()->consistency_mem_tracker();
    Status check_limit_st = mem_tracker->check_mem_limit("Start consistency check.");
    if (!check_limit_st.ok()) {
        LOG(WARNING) << "check consistency failed: " << check_limit_st.message();
        status_code = TStatusCode::MEM_LIMIT_EXCEEDED;
    } else {
        EngineChecksumTask engine_task(mem_tracker, check_consistency_req.tablet_id, check_consistency_req.schema_hash,
                                       check_consistency_req.version, &checksum);
        Status res = StorageEngine::instance()->execute_task(&engine_task);
        if (!res.ok()) {
            LOG(WARNING) << "check consistency failed. status: " << res << ", signature: " << agent_task_req->signature;
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            LOG(INFO) << "check consistency success. status:" << res << ", signature:" << agent_task_req->signature
                      << ", checksum:" << checksum;
        }
    }

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);
    finish_task_request.__set_task_status(task_status);
    finish_task_request.__set_tablet_checksum(static_cast<int64_t>(checksum));
    finish_task_request.__set_request_version(check_consistency_req.version);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

void run_compaction_task(const std::shared_ptr<CompactionTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TCompactionReq& compaction_req = agent_task_req->task_req;
    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;

    for (auto tablet_id : compaction_req.tablet_ids) {
        EngineManualCompactionTask engine_task(ExecEnv::GetInstance()->compaction_mem_tracker(), tablet_id,
                                               compaction_req.is_base_compaction);
        StorageEngine::instance()->execute_task(&engine_task);
    }

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);
    finish_task_request.__set_task_status(task_status);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

void run_upload_task(const std::shared_ptr<UploadAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TUploadReq& upload_request = agent_task_req->task_req;

    LOG(INFO) << "Got upload task signature=" << agent_task_req->signature << " job id=" << upload_request.job_id;

    std::map<int64_t, std::vector<std::string>> tablet_files;
    SnapshotLoader loader(exec_env, upload_request.job_id, agent_task_req->signature);
    Status status = loader.upload(upload_request.src_dest_map, upload_request, &tablet_files);

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    if (!status.ok()) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "Fail to upload job id=" << upload_request.job_id << " msg=" << status.get_error_msg();
        error_msgs.push_back(status.get_error_msg());
    }

    TStatus task_status;
    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);
    finish_task_request.__set_task_status(task_status);
    finish_task_request.__set_tablet_files(tablet_files);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);

    LOG(INFO) << "Uploaded task signature=" << agent_task_req->signature << " job id=" << upload_request.job_id;
}

void run_download_task(const std::shared_ptr<DownloadAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TDownloadReq& download_request = agent_task_req->task_req;
    LOG(INFO) << "Got download task signature=" << agent_task_req->signature << " job id=" << download_request.job_id;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;

    // TODO: download
    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(exec_env, download_request.job_id, agent_task_req->signature);
    Status status = loader.download(download_request.src_dest_map, download_request, &downloaded_tablet_ids);

    if (!status.ok()) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "Fail to download job id=" << download_request.job_id << " msg=" << status.get_error_msg();
        error_msgs.push_back(status.get_error_msg());
    }

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);
    finish_task_request.__set_task_status(task_status);
    finish_task_request.__set_downloaded_tablet_ids(downloaded_tablet_ids);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);

    LOG(INFO) << "Downloaded task signature=" << agent_task_req->signature << " job id=" << download_request.job_id;
}

void run_make_snapshot_task(const std::shared_ptr<SnapshotAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TSnapshotRequest& snapshot_request = agent_task_req->task_req;
    LOG(INFO) << "Got snapshot task signature=" << agent_task_req->signature;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;

    std::string snapshot_path;
    std::vector<std::string> snapshot_files;
    Status st = SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path);
    if (!st.ok()) {
        status_code = st.code();
        LOG(WARNING) << "Fail to make_snapshot, tablet_id=" << snapshot_request.tablet_id
                     << " schema_hash=" << snapshot_request.schema_hash << " version=" << snapshot_request.version
                     << " status=" << st.to_string();
        error_msgs.push_back("make_snapshot failed. status: " + st.to_string());
    } else {
        LOG(INFO) << "Created snapshot tablet_id=" << snapshot_request.tablet_id
                  << " schema_hash=" << snapshot_request.schema_hash << " version=" << snapshot_request.version
                  << " snapshot_path=" << snapshot_path;
        if (snapshot_request.__isset.list_files) {
            // list and save all snapshot files
            // snapshot_path like: data/snapshot/20180417205230.1.86400
            // we need to add subdir: tablet_id/schema_hash/
            std::stringstream ss;
            ss << snapshot_path << "/" << snapshot_request.tablet_id << "/" << snapshot_request.schema_hash << "/";
            st = FileSystem::Default()->get_children(ss.str(), &snapshot_files);
            if (!st.ok()) {
                status_code = TStatusCode::RUNTIME_ERROR;
                LOG(WARNING) << "Fail to make snapshot tablet_id" << snapshot_request.tablet_id
                             << " schema_hash=" << snapshot_request.schema_hash
                             << " version=" << snapshot_request.version << ", list file failed, " << st.get_error_msg();
                error_msgs.push_back("make_snapshot failed. list file failed: " + st.get_error_msg());
            }
        }
    }

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);
    finish_task_request.__set_snapshot_path(snapshot_path);
    finish_task_request.__set_snapshot_files(snapshot_files);
    finish_task_request.__set_task_status(task_status);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

void run_release_snapshot_task(const std::shared_ptr<ReleaseSnapshotAgentTaskRequest>& agent_task_req,
                               ExecEnv* exec_env) {
    const TReleaseSnapshotRequest& release_snapshot_request = agent_task_req->task_req;
    LOG(INFO) << "Got release snapshot task signature=" << agent_task_req->signature;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    const std::string& snapshot_path = release_snapshot_request.snapshot_path;
    Status release_snapshot_status = SnapshotManager::instance()->release_snapshot(snapshot_path);
    if (!release_snapshot_status.ok()) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "Fail to release snapshot snapshot_path=" << snapshot_path
                     << " status=" << release_snapshot_status;
        error_msgs.push_back("release_snapshot failed. status: " +
                             boost::lexical_cast<std::string>(release_snapshot_status));
    } else {
        LOG(INFO) << "Released snapshot path=" << snapshot_path << " status=" << release_snapshot_status;
    }

    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
}

AgentStatus move_dir(TTabletId tablet_id, TSchemaHash schema_hash, const std::string& src, int64_t job_id,
                     bool overwrite, std::vector<std::string>* error_msgs, ExecEnv* exec_env) {
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(INFO) << "Fail to get tablet_id=" << tablet_id << " schema hash=" << schema_hash;
        error_msgs->push_back("failed to get tablet");
        return STARROCKS_TASK_REQUEST_ERROR;
    }

    std::string dest_tablet_dir = tablet->schema_hash_path();
    SnapshotLoader loader(exec_env, job_id, tablet_id);
    Status status;
    if (tablet->updates() == nullptr) {
        status = loader.move(src, tablet, overwrite);
    } else {
        status = loader.primary_key_move(src, tablet, overwrite);
    }

    if (!status.ok()) {
        LOG(WARNING) << "Fail to move job id=" << job_id << ", " << status.get_error_msg();
        error_msgs->push_back(status.get_error_msg());
        return STARROCKS_INTERNAL_ERROR;
    }

    return STARROCKS_SUCCESS;
}

void run_move_dir_task(const std::shared_ptr<MoveDirAgentTaskRequest>& agent_task_req, ExecEnv* exec_env) {
    const TMoveDirReq& move_dir_req = agent_task_req->task_req;
    LOG(INFO) << "Got move dir task signature=" << agent_task_req->signature << " job id=" << move_dir_req.job_id;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    // TODO: move dir
    AgentStatus status = move_dir(move_dir_req.tablet_id, move_dir_req.schema_hash, move_dir_req.src,
                                  move_dir_req.job_id, true /* TODO */, &error_msgs, exec_env);

    if (status != STARROCKS_SUCCESS) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "Fail to move dir=" << move_dir_req.src << " tablet id=" << move_dir_req.tablet_id
                     << " signature=" << agent_task_req->signature << " job id=" << move_dir_req.job_id;
        error_msgs.emplace_back("Fail to move dir=" + move_dir_req.src +
                                " tablet id=" + std::to_string(move_dir_req.tablet_id));
    } else {
        LOG(INFO) << "Moved dir=" << move_dir_req.src << " tablet_id=" << move_dir_req.tablet_id
                  << " signature=" << agent_task_req->signature << " job id=" << move_dir_req.job_id;
    }

    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
}

void run_update_meta_info_task(const std::shared_ptr<UpdateTabletMetaInfoAgentTaskRequest>& agent_task_req,
                               ExecEnv* exec_env) {
    const TUpdateTabletMetaInfoReq& update_tablet_meta_req = agent_task_req->task_req;

    LOG(INFO) << "get update tablet meta task, signature:" << agent_task_req->signature;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    // alter meta SHARED_DATA
    if (update_tablet_meta_req.__isset.tablet_type &&
        update_tablet_meta_req.tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        lake::SchemaChangeHandler handler(ExecEnv::GetInstance()->lake_tablet_manager());
        auto res = handler.process_update_tablet_meta(update_tablet_meta_req);
        if (!res.ok()) {
            // TODO explict the error message and errorCode
            error_msgs.emplace_back(res.get_error_msg());
            status_code = TStatusCode::RUNTIME_ERROR;
        }
        unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
        LOG(INFO) << "finish update tablet meta task. signature:" << agent_task_req->signature;
        return;
    }

    // SHARED_NOTHING, tablet_type = TTabletType::TABLET_TYPE_DISK
    for (const auto& tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_meta_info.tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << "could not find tablet when update partition id"
                         << " tablet_id=" << tablet_meta_info.tablet_id
                         << " schema_hash=" << tablet_meta_info.schema_hash;
            continue;
        }
        std::unique_lock wrlock(tablet->get_header_lock());
        // update tablet meta
        if (!tablet_meta_info.__isset.meta_type) {
            tablet->set_partition_id(tablet_meta_info.partition_id);
        } else {
            switch (tablet_meta_info.meta_type) {
            case TTabletMetaType::PARTITIONID:
                tablet->set_partition_id(tablet_meta_info.partition_id);
                break;
            case TTabletMetaType::INMEMORY:
                // This property is no longer supported.
                break;
            case TTabletMetaType::WRITE_QUORUM:
                break;
            case TTabletMetaType::REPLICATED_STORAGE:
                break;
            case TTabletMetaType::DISABLE_BINLOG:
                break;
            case TTabletMetaType::BINLOG_CONFIG:
                LOG(INFO) << "update tablet:" << tablet->tablet_id() << "binlog config";
                {
                    auto curr_binlog_config = tablet->tablet_meta()->get_binlog_config();
                    if (curr_binlog_config != nullptr) {
                        LOG(INFO) << "current binlog config:" << curr_binlog_config->to_string();
                    }
                }

                BinlogConfig binlog_config;
                binlog_config.update(tablet_meta_info.binlog_config);
                tablet->update_binlog_config(binlog_config);
                break;
            case TTabletMetaType::ENABLE_PERSISTENT_INDEX:
                LOG(INFO) << "update tablet:" << tablet->tablet_id()
                          << " enable_persistent_index:" << tablet_meta_info.enable_persistent_index;
                tablet->set_enable_persistent_index(tablet_meta_info.enable_persistent_index);
                // If tablet is doing apply rowset right now, remove primary index from index cache may be failed
                // because the primary index is available in cache
                // But it will be remove from index cache after apply is finished
                auto manager = StorageEngine::instance()->update_manager();
                manager->index_cache().try_remove_by_key(tablet->tablet_id());
                break;
            }
        }
        tablet->save_meta();
    }

    LOG(INFO) << "finish update tablet meta task. signature:" << agent_task_req->signature;

    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
}

AgentStatus drop_auto_increment_map(TTableId table_id) {
    // always success
    StorageEngine::instance()->remove_increment_map_by_table_id(table_id);
    return STARROCKS_SUCCESS;
}

void run_drop_auto_increment_map_task(const std::shared_ptr<DropAutoIncrementMapAgentTaskRequest>& agent_task_req,
                                      ExecEnv* exec_env) {
    const TDropAutoIncrementMapReq& drop_auto_increment_map_req = agent_task_req->task_req;
    LOG(INFO) << "drop auto increment map task tableid=" << drop_auto_increment_map_req.table_id;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    drop_auto_increment_map(drop_auto_increment_map_req.table_id);
    LOG(INFO) << "drop auto increment map task success, tableid=" << drop_auto_increment_map_req.table_id;
    unify_finish_agent_task(status_code, error_msgs, agent_task_req->task_type, agent_task_req->signature);
}

void run_remote_snapshot_task(const std::shared_ptr<RemoteSnapshotAgentTaskRequest>& agent_task_req,
                              ExecEnv* exec_env) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(ExecEnv::GetInstance()->replication_mem_tracker());
    DeferOp op([prev_tracker] { tls_thread_status.set_mem_tracker(prev_tracker); });

    const TRemoteSnapshotRequest& remote_snapshot_req = agent_task_req->task_req;

    // Return result to fe
    TStatus task_status;
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    std::string src_snapshot_path;
    bool incremental_snapshot;

    Status res;
    if (remote_snapshot_req.tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        res = exec_env->lake_replication_txn_manager()->remote_snapshot(remote_snapshot_req, &src_snapshot_path,
                                                                        &incremental_snapshot);
    } else {
        res = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
                remote_snapshot_req, &src_snapshot_path, &incremental_snapshot);
    }

    if (!res.ok()) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "remote snapshot failed. status: " << res << ", signature:" << agent_task_req->signature;
        error_msgs.emplace_back("replicate snapshot failed, " + res.to_string());
    } else {
        finish_task_request.__set_snapshot_path(src_snapshot_path);
        finish_task_request.__set_incremental_snapshot(incremental_snapshot);
    }

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);
    finish_task_request.__set_task_status(task_status);

#ifndef BE_TEST
    finish_task(finish_task_request);
#endif
    auto task_queue_size = remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    LOG(INFO) << "Remove task success. type=" << agent_task_req->task_type
              << ", signature=" << agent_task_req->signature << ", task_count_in_queue=" << task_queue_size;
}

void run_replicate_snapshot_task(const std::shared_ptr<ReplicateSnapshotAgentTaskRequest>& agent_task_req,
                                 ExecEnv* exec_env) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(ExecEnv::GetInstance()->replication_mem_tracker());
    DeferOp op([prev_tracker] { tls_thread_status.set_mem_tracker(prev_tracker); });

    const TReplicateSnapshotRequest& replicate_snapshot_req = agent_task_req->task_req;

    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;

    Status res;
    if (replicate_snapshot_req.tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        res = exec_env->lake_replication_txn_manager()->replicate_snapshot(replicate_snapshot_req);
    } else {
        res = StorageEngine::instance()->replication_txn_manager()->replicate_snapshot(replicate_snapshot_req);
    }

    if (!res.ok()) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "replicate snapshot failed. status: " << res << ", signature:" << agent_task_req->signature;
        error_msgs.emplace_back("replicate snapshot failed, " + res.to_string());
    }

    // Return result to fe
    TStatus task_status;
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);
    finish_task_request.__set_task_status(task_status);

#ifndef BE_TEST
    finish_task(finish_task_request);
#endif
    auto task_queue_size = remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    LOG(INFO) << "Remove task success. type=" << agent_task_req->task_type
              << ", signature=" << agent_task_req->signature << ", task_count_in_queue=" << task_queue_size;
}

} // namespace starrocks
