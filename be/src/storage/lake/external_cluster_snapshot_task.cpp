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
#include "base/concurrency/countdown_latch.h"
#include "base/phmap/phmap.h"
#include "base/utility/defer_op.h"
#include "common/brpc/brpc_stub_cache.h"
#include "common/statusor.h"
#include "common/system/backend_options.h"
#include "fmt/format.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "gen_cpp/lake_types.pb.h"
#include "glog/logging.h"
#include "runtime/exec_env.h"
#include "storage/lake/external_cluster_snapshot_task_helper.h"
#include "storage/lake/external_cluster_snapshot_task_rpc.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/snapshot_file_syncer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/protobuf_file.h"

namespace starrocks::lake {

Status write_snapshot_log(int64_t job_id, int64_t db_id, int64_t table_id, int64_t partition_id,
                          int64_t physical_partition_id, int64_t tablet_id, const FileSet& unused_data_files,
                          const FileSet& unused_meta_files, const FileSet& unused_schema_files, ExecEnv* exec_env) {
    ExternalClusterSnapshotLogPB log_pb;
    // Populate basic fields
    log_pb.set_job_id(job_id);
    log_pb.set_db_id(db_id);
    log_pb.set_table_id(table_id);
    log_pb.set_partition_id(partition_id);
    log_pb.set_physical_partition_id(physical_partition_id);

    // Add files to delete
    for (const auto& file : unused_data_files) {
        log_pb.add_delete_data_files(file);
    }
    for (const auto& file : unused_meta_files) {
        log_pb.add_delete_meta_files(file);
    }
    for (const auto& file : unused_schema_files) {
        log_pb.add_delete_schema_files(file);
    }

    // Get log location and save
    auto location_provider = exec_env->lake_location_provider();
    auto log_location = location_provider->snapshot_log_location(tablet_id, job_id, physical_partition_id);

    ProtobufFile file(log_location);
    auto log_status = file.save(log_pb);

    // Log result
    if (!log_status.ok()) {
        LOG(WARNING) << "failed to persist external snapshot delete log, path=" << log_location
                     << ", status: " << log_status.to_string();
    } else {
        VLOG(3) << "external snapshot delete log saved, path=" << log_location
                << ", data_files: " << log_pb.delete_data_files_size()
                << ", meta_files: " << log_pb.delete_meta_files_size()
                << ", schema_files: " << log_pb.delete_schema_files_size();
    }

    return log_status;
}

void run_external_cluster_snapshot_task(const TExternalClusterSnapshotRequest& request, int64_t signature,
                                        ExecEnv* exec_env) {
    VLOG(3) << "run_external_cluster_snapshot_task, " << request.db_id << ", " << request.table_id << ", "
            << request.partition_id << ", " << request.physical_partition_id
            << ", is_filebundling: " << request.is_filebundling << ", is_drop_partition: " << request.is_drop_partition
            << ", dest_tablet_id: " << request.dest_tablet_id;

    // Handle drop partition case
    if (request.is_drop_partition) {
        return run_delete_partition_task(request, signature, exec_env);
    }

    // Handle delete files case
    if (request.new_version == -1) {
        return run_delete_files_task(request, signature, exec_env);
    }

    // Initialize core variables
    auto* tablet_mgr = exec_env->lake_tablet_manager();
    const int64_t pre_version = request.pre_version;
    const int64_t new_version = request.new_version;
    const int64_t table_id = request.table_id;
    const int64_t physical_partition_id = request.physical_partition_id;

    phmap::flat_hash_set<std::string> globally_bound_segments;
    FileSet pre_bundle_data_files;
    FileSet unused_data_files;
    FileSet unused_meta_files;
    phmap::flat_hash_set<int64_t> pre_schema_ids;
    phmap::flat_hash_set<int64_t> new_schema_ids;

    // Prepare finish task request
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

    // Initialize RPC context
    ClusterSnapshotRpcCtx cluster_snapshot_rpc_ctx;
    cluster_snapshot_rpc_ctx.latch = std::make_unique<BThreadCountDownLatch>(request.compute_node_tablets.size());
    cluster_snapshot_rpc_ctx.finish_task_req = &finish_task_request;

    // Process each backend node
    bool meta_added = false;
    for (const auto& compute_node_tablets : request.compute_node_tablets) {
        const auto& compute_node = compute_node_tablets.compute_node;
        const auto& tablet_ids = compute_node_tablets.tablets;
        VLOG(3) << "run cluster snapshot rpc, cn: " << compute_node << ", tablet_ids: " << tablet_ids.size();

        // Skip if failure already occurred
        if (cluster_snapshot_rpc_ctx.has_failure()) {
            cluster_snapshot_rpc_ctx.handle_failure("", tablet_ids);
            cluster_snapshot_rpc_ctx.count_down();
            continue;
        }

        // Prepare node request
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
            process_tablet_status = process_tablet_for_snapshot(
                    tablet_mgr, tablet_id, pre_version, new_version, request.is_filebundling, meta_added,
                    pre_bundle_data_files, unused_data_files, unused_meta_files, pre_schema_ids, new_schema_ids,
                    globally_bound_segments, node_req);
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

    // Write snapshot log if no failures
    Status log_status = Status::OK();
    if (pre_version >= 0 && cluster_snapshot_rpc_ctx.final_status.ok()) {
        FileSet unused_schema_files;
        prepare_unused_files_for_log(pre_version, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                     pre_schema_ids, new_schema_ids, unused_schema_files);

        // Get first tablet ID for log
        int64_t first_tablet_id = 0;
        if (!request.compute_node_tablets.empty() && !request.compute_node_tablets[0].tablets.empty()) {
            first_tablet_id = request.compute_node_tablets[0].tablets[0];
        }

        log_status = write_snapshot_log(request.job_id, request.db_id, request.table_id, request.partition_id,
                                        physical_partition_id, first_tablet_id, unused_data_files, unused_meta_files,
                                        unused_schema_files, exec_env);
    }

    // Update finish task status if log write failed
    if (!log_status.ok() && finish_task_request.task_status.status_code == TStatusCode::OK) {
        TStatus failed_status;
        failed_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        failed_status.__set_error_msgs(std::vector<std::string>{log_status.to_string()});
        finish_task_request.__set_task_status(failed_status);
    }

    // Finalize task
    finish_task(finish_task_request);
    remove_task_info(finish_task_request.task_type, finish_task_request.signature);
}

void run_delete_partition_task(const TExternalClusterSnapshotRequest& request, int64_t signature, ExecEnv* exec_env) {
    VLOG(3) << "run_delete_partition_task, " << request.db_id << ", " << request.table_id << ", "
            << request.partition_id << ", " << request.physical_partition_id;

    TFinishTaskRequest finish_task_request;
    TStatus task_status;
    task_status.__set_status_code(TStatusCode::OK);
    finish_task_request.__set_task_status(task_status);
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(TTaskType::EXTERNAL_CLUSTER_SNAPSHOT);
    finish_task_request.__set_signature(signature);

    auto snapshot_file_syncer = lake::SnapshotFileSyncer(exec_env);
    auto st = snapshot_file_syncer.delete_partition(request.dest_tablet_id, request.db_id, request.table_id,
                                                    request.partition_id, request.physical_partition_id);
    if (!st.ok()) {
        LOG(ERROR) << "delete partition failed, status: " << st.to_string();
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        task_status.__set_error_msgs(std::vector<std::string>{st.to_string()});
        finish_task_request.__set_task_status(task_status);
    }

    finish_task(finish_task_request);
    remove_task_info(finish_task_request.task_type, finish_task_request.signature);
}

void run_delete_files_task(const TExternalClusterSnapshotRequest& request, int64_t signature, ExecEnv* exec_env) {
    VLOG(3) << "run_delete_files_task, " << request.db_id << ", " << request.table_id << ", " << request.partition_id
            << ", " << request.physical_partition_id;

    TFinishTaskRequest finish_task_request;
    TStatus task_status;
    task_status.__set_status_code(TStatusCode::OK);
    finish_task_request.__set_task_status(task_status);
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(TTaskType::EXTERNAL_CLUSTER_SNAPSHOT);
    finish_task_request.__set_signature(signature);

    auto location_provider = exec_env->lake_location_provider();
    auto tablet_id = 0L;
    if (request.compute_node_tablets.size() == 0 || request.compute_node_tablets[0].tablets.size() == 0) {
        LOG(WARNING) << "no compute node tablets or tablets found, job_id=" << request.job_id
                     << ", physical_partition_id=" << request.physical_partition_id;
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        task_status.__set_error_msgs(std::vector<std::string>{"no compute node tablets or tablets found"});
        finish_task_request.__set_task_status(task_status);
        finish_task(finish_task_request);
        remove_task_info(finish_task_request.task_type, finish_task_request.signature);
        return;
    }
    tablet_id = request.compute_node_tablets[0].tablets[0];
    auto log_path = location_provider->snapshot_log_location(tablet_id, request.job_id, request.physical_partition_id);

    auto fs = FileSystemFactory::CreateSharedFromString(log_path);
    if (!fs.ok()) {
        LOG(WARNING) << "create file system failed, path=" << log_path << ", status=" << fs.status().to_string();
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        task_status.__set_error_msgs(std::vector<std::string>{fs.status().to_string()});
        finish_task_request.__set_task_status(task_status);
    } else {
        ExternalClusterSnapshotLogPB log_pb;
        ProtobufFile file(log_path, fs.value());
        auto st = file.load(&log_pb, false);
        if (!st.ok()) {
            LOG(WARNING) << "load external snapshot delete log failed, path=" << log_path
                         << ", status=" << st.to_string();
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
            task_status.__set_error_msgs(std::vector<std::string>{st.to_string()});
            finish_task_request.__set_task_status(task_status);
        } else {
            auto snapshot_file_syncer = lake::SnapshotFileSyncer(exec_env);
            st = snapshot_file_syncer.delete_files(request.dest_tablet_id, log_pb);
            if (!st.ok()) {
                LOG(WARNING) << "delete files according to snapshot log failed, path=" << log_path
                             << ", status=" << st.to_string();
                task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
                task_status.__set_error_msgs(std::vector<std::string>{st.to_string()});
                finish_task_request.__set_task_status(task_status);
            } else {
                (void)(*fs)->delete_file(log_path);
            }
        }
    }

    finish_task(finish_task_request);
    remove_task_info(finish_task_request.task_type, finish_task_request.signature);
}

} // namespace starrocks::lake
