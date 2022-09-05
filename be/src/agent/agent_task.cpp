// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "agent/agent_task.h"

#include "agent/agent_common.h"
#include "agent/finish_task.h"
#include "agent/task_singatures_manager.h"
#include "common/status.h"
#include "runtime/current_thread.h"
#include "service/backend_options.h"
#include "storage/lake/tablet_manager.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"

namespace starrocks {

static std::atomic<int64_t> g_report_version(time(nullptr) * 10000);

void run_drop_tablet_task(std::shared_ptr<DropTabletAgentTaskRequest> agent_task_req) {
    const TDropTabletReq& drop_tablet_req = agent_task_req->task_req;

    bool force_drop = drop_tablet_req.__isset.force && drop_tablet_req.force;
    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;

    auto dropped_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(drop_tablet_req.tablet_id);
    if (dropped_tablet != nullptr) {
        TabletDropFlag flag = force_drop ? kDeleteFiles : kMoveFilesToTrash;
        auto st = StorageEngine::instance()->tablet_manager()->drop_tablet(drop_tablet_req.tablet_id, flag);
        if (!st.ok()) {
            LOG(WARNING) << "drop table failed! signature: " << agent_task_req->signature;
            error_msgs.emplace_back("drop table failed!");
            status_code = TStatusCode::RUNTIME_ERROR;
        }
        // if tablet is dropped by fe, then the related txn should also be removed
        StorageEngine::instance()->txn_manager()->force_rollback_tablet_related_txns(
                dropped_tablet->data_dir()->get_meta(), drop_tablet_req.tablet_id, drop_tablet_req.schema_hash,
                dropped_tablet->tablet_uid());
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

void run_create_tablet_task(std::shared_ptr<CreateTabletAgentTaskRequest> agent_task_req, ExecEnv* exec_env) {
    const auto& create_tablet_req = agent_task_req->task_req;
    TFinishTaskRequest finish_task_request;
    TStatusCode::type status_code = TStatusCode::OK;
    std::vector<std::string> error_msgs;
    TStatus task_status;

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

    task_status.__set_status_code(status_code);
    task_status.__set_error_msgs(error_msgs);

    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_report_version(g_report_version.load(std::memory_order_relaxed));
    finish_task_request.__set_task_type(agent_task_req->task_type);
    finish_task_request.__set_signature(agent_task_req->signature);
    finish_task_request.__set_task_status(task_status);

    finish_task(finish_task_request);
    remove_task_info(agent_task_req->task_type, agent_task_req->signature);
}

} // namespace starrocks