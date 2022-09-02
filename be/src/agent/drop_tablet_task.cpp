// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "agent/drop_tablet_task.h"

#include "agent/agent_common.h"
#include "agent/finish_task.h"
#include "agent/task_singatures_manager.h"
#include "common/status.h"
#include "runtime/current_thread.h"
#include "service/backend_options.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"

namespace starrocks {

void run_drop_tablet_task(std::shared_ptr<TAgentTaskRequest> agent_task_req) {
    const TDropTabletReq& drop_tablet_req = agent_task_req->drop_tablet_req;

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

} // namespace starrocks