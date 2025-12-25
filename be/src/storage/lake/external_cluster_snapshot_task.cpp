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

#include "storage/lake/external_cluster_snapshot_task.h"

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "agent/finish_task.h"
#include "agent/task_signatures_manager.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/lake/external_cluster_snapshot_task_helper.h"
#include "storage/lake/external_cluster_snapshot_task_rpc.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"

namespace starrocks::lake {

void run_external_cluster_snapshot_task(const TExternalClusterSnapshotRequest& request, int64_t signature,
                                        ExecEnv* exec_env) {
    VLOG(3) << "run_external_cluster_snapshot_task, " << request.db_id << ", " << request.table_id << ", "
            << request.partition_id << ", " << request.physical_partition_id
            << ", is_filebundling: " << request.is_filebundling;

    auto* tablet_mgr = exec_env->lake_tablet_manager();
    const int64_t pre_version = request.pre_version;
    const int64_t new_version = request.new_version;
    const int64_t table_id = request.table_id;
    const int64_t physical_partition_id = request.physical_partition_id;

    FileSet globally_bound_segments;

    TFinishTaskRequest finish_task_request;
    TStatus task_status;
    task_status.__set_status_code(TStatusCode::OK);
    finish_task_request.__set_task_status(task_status);
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(TTaskType::EXTERNAL_CLUSTER_SNAPSHOT);
    finish_task_request.__set_signature(signature);

    TClusterSnapshotPartitionSpec spec;
    spec.__set_db_id(request.db_id);
    spec.__set_table_id(request.table_id);
    spec.__set_partition_id(request.partition_id);
    spec.__set_physical_partition_id(request.physical_partition_id);
    finish_task_request.__set_cluster_snapshot_partition_spec(spec);

    ClusterSnapshotRpcCtx cluster_snapshot_rpc_ctx;
    cluster_snapshot_rpc_ctx.latch = std::make_unique<BThreadCountDownLatch>(request.compute_node_tablets.size());
    cluster_snapshot_rpc_ctx.finish_task_req = &finish_task_request;

    bool is_filebundling = request.is_filebundling;
    bool meta_added = false;
    phmap::flat_hash_set<int64_t> pre_schema_ids;
    phmap::flat_hash_set<int64_t> new_schema_ids;

    for (const auto& compute_node_tablets : request.compute_node_tablets) {
        const auto& compute_node = compute_node_tablets.compute_node;
        const auto& tablet_ids = compute_node_tablets.tablets;
        VLOG(3) << "run cluster snapshot rpc, cn: " << compute_node << ", tablet_ids: " << tablet_ids.size();
        if (cluster_snapshot_rpc_ctx.has_failure()) {
            cluster_snapshot_rpc_ctx.handle_failure("", tablet_ids);
            cluster_snapshot_rpc_ctx.count_down();
            continue;
        }

        UploadSnapshotFilesRequestPB node_req;
        node_req.set_job_id(request.job_id);
        node_req.set_db_id(request.db_id);
        node_req.set_table_id(table_id);
        node_req.set_partition_id(request.partition_id);
        node_req.set_physical_partition_id(physical_partition_id);
        node_req.set_dest_tablet_id(request.dest_tablet_id);

        // Process each tablet in current node
        auto process_tablet_status = Status::OK();
        for (int64_t tablet_id : tablet_ids) {
            process_tablet_status = process_tablet_for_snapshot(tablet_mgr, tablet_id, pre_version, new_version,
                                                                is_filebundling, meta_added, pre_schema_ids,
                                                                new_schema_ids, globally_bound_segments, node_req);
            if (!process_tablet_status.ok()) {
                break;
            }
            meta_added = true;
        }

        // Skip if tablet processing failed or no snapshots to send
        if (!process_tablet_status.ok() || node_req.tablet_snapshots_size() == 0) {
            if (!process_tablet_status.ok()) {
                cluster_snapshot_rpc_ctx.handle_failure(process_tablet_status.to_string(), tablet_ids);
            }
            cluster_snapshot_rpc_ctx.count_down();
            continue;
        }

        // Send RPC to backend
        send_snapshot_rpc_to_backend(compute_node, tablet_ids, node_req, cluster_snapshot_rpc_ctx);
    }

    // Wait for all RPCs to complete
    cluster_snapshot_rpc_ctx.wait();
    LOG(INFO) << "finish cluster snapshot task: " << signature
              << ", status: " << cluster_snapshot_rpc_ctx.final_status.to_string();

    // TODO(zhangqiang)
    // write delete txn log
    finish_task(finish_task_request);
    remove_task_info(finish_task_request.task_type, finish_task_request.signature);
}

} // namespace starrocks::lake