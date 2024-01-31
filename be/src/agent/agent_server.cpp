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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/agent_server.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "agent/agent_server.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <filesystem>
#include <string>
#include <vector>

#include "agent/agent_task.h"
#include "agent/master_info.h"
#include "agent/task_signatures_manager.h"
#include "agent/task_worker_pool.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "storage/snapshot_manager.h"
#include "testutil/sync_point.h"
#include "util/phmap/phmap.h"
#include "util/threadpool.h"

namespace starrocks {

namespace {
constexpr size_t DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE = 2048;
constexpr size_t MIN_CLONE_TASK_THREADS_IN_POOL = 2;
} // namespace

using TTaskTypeHash = std::hash<std::underlying_type<TTaskType::type>::type>;

const uint32_t REPORT_TASK_WORKER_COUNT = 1;
const uint32_t REPORT_DISK_STATE_WORKER_COUNT = 1;
const uint32_t REPORT_OLAP_TABLE_WORKER_COUNT = 1;
const uint32_t REPORT_WORKGROUP_WORKER_COUNT = 1;
const uint32_t REPORT_RESOURCE_USAGE_WORKER_COUNT = 1;
const uint32_t REPORT_DATACACHE_METRICS_WORKER_COUNT = 1;

class AgentServer::Impl {
public:
    explicit Impl(ExecEnv* exec_env, bool is_compute_node) : _exec_env(exec_env), _is_compute_node(is_compute_node) {}

    ~Impl();

    void init_or_die();

    void stop();

    void submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks);

    void make_snapshot(TAgentResult& agent_result, const TSnapshotRequest& snapshot_request);

    void release_snapshot(TAgentResult& agent_result, const std::string& snapshot_path);

    void publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request);

    void update_max_thread_by_type(int type, int new_val);

    ThreadPool* get_thread_pool(int type) const;

    DISALLOW_COPY_AND_MOVE(Impl);

private:
    ExecEnv* _exec_env;

    std::unique_ptr<ThreadPool> _thread_pool_publish_version;
    std::unique_ptr<ThreadPool> _thread_pool_clone;
    std::unique_ptr<ThreadPool> _thread_pool_drop;
    std::unique_ptr<ThreadPool> _thread_pool_create_tablet;
    std::unique_ptr<ThreadPool> _thread_pool_alter_tablet;
    std::unique_ptr<ThreadPool> _thread_pool_clear_transaction;
    std::unique_ptr<ThreadPool> _thread_pool_storage_medium_migrate;
    std::unique_ptr<ThreadPool> _thread_pool_check_consistency;
    std::unique_ptr<ThreadPool> _thread_pool_compaction;
    std::unique_ptr<ThreadPool> _thread_pool_update_schema;

    std::unique_ptr<ThreadPool> _thread_pool_upload;
    std::unique_ptr<ThreadPool> _thread_pool_download;
    std::unique_ptr<ThreadPool> _thread_pool_make_snapshot;
    std::unique_ptr<ThreadPool> _thread_pool_release_snapshot;
    std::unique_ptr<ThreadPool> _thread_pool_move_dir;
    std::unique_ptr<ThreadPool> _thread_pool_update_tablet_meta_info;
    std::unique_ptr<ThreadPool> _thread_pool_drop_auto_increment_map;
    std::unique_ptr<ThreadPool> _thread_pool_replication;

    std::unique_ptr<PushTaskWorkerPool> _push_workers;
    std::unique_ptr<PublishVersionTaskWorkerPool> _publish_version_workers;
    std::unique_ptr<DeleteTaskWorkerPool> _delete_workers;

    // These 3 worker-pool do not accept tasks from FE.
    // It is self triggered periodically and reports to Fe master
    std::unique_ptr<ReportTaskWorkerPool> _report_task_workers;
    std::unique_ptr<ReportDiskStateTaskWorkerPool> _report_disk_state_workers;
    std::unique_ptr<ReportOlapTableTaskWorkerPool> _report_tablet_workers;
    std::unique_ptr<ReportWorkgroupTaskWorkerPool> _report_workgroup_workers;
    std::unique_ptr<ReportResourceUsageTaskWorkerPool> _report_resource_usage_workers;
    std::unique_ptr<ReportDataCacheMetricsTaskWorkerPool> _report_datacache_metrics_workers;

    // Compute node only need _report_resource_usage_workers.
    const bool _is_compute_node;
};

void AgentServer::Impl::init_or_die() {
    if (!_is_compute_node) {
        for (auto& path : _exec_env->store_paths()) {
            try {
                std::string dpp_download_path_str = path.path + DPP_PREFIX;
                std::filesystem::path dpp_download_path(dpp_download_path_str);
                if (std::filesystem::exists(dpp_download_path)) {
                    std::filesystem::remove_all(dpp_download_path);
                }
            } catch (...) {
                LOG(WARNING) << "std exception when remove dpp download path. path=" << path.path;
            }
        }

#define BUILD_DYNAMIC_TASK_THREAD_POOL(name, min_threads, max_threads, queue_size, pool) \
    do {                                                                                 \
        auto st = ThreadPoolBuilder(name)                                                \
                          .set_min_threads(min_threads)                                  \
                          .set_max_threads(max_threads)                                  \
                          .set_max_queue_size(queue_size)                                \
                          .build(&(pool));                                               \
        CHECK(st.ok()) << st;                                                            \
    } while (false)

// The ideal queue size of threadpool should be larger than the maximum number of tablet of a partition.
// But it seems that there's no limit for the number of tablets of a partition.
// Since a large queue size brings a little overhead, a big one is chosen here.
#ifdef BE_TEST
        BUILD_DYNAMIC_TASK_THREAD_POOL("publish_version", 1, 3, DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE,
                                       _thread_pool_publish_version);
#else
        int max_publish_version_worker_count = config::transaction_publish_version_worker_count;
        if (max_publish_version_worker_count <= 0) {
            max_publish_version_worker_count = CpuInfo::num_cores();
        }
        max_publish_version_worker_count =
                std::max(max_publish_version_worker_count, MIN_TRANSACTION_PUBLISH_WORKER_COUNT);
        BUILD_DYNAMIC_TASK_THREAD_POOL("publish_version", MIN_TRANSACTION_PUBLISH_WORKER_COUNT,
                                       max_publish_version_worker_count, DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE,
                                       _thread_pool_publish_version);
        REGISTER_GAUGE_STARROCKS_METRIC(publish_version_queue_count,
                                        [this]() { return _thread_pool_publish_version->num_queued_tasks(); });
#endif

        BUILD_DYNAMIC_TASK_THREAD_POOL("drop", 1, config::drop_tablet_worker_count, std::numeric_limits<int>::max(),
                                       _thread_pool_drop);

        BUILD_DYNAMIC_TASK_THREAD_POOL("create_tablet", 1, config::create_tablet_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_create_tablet);

        BUILD_DYNAMIC_TASK_THREAD_POOL("alter_tablet", 0, config::alter_tablet_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_alter_tablet);

        BUILD_DYNAMIC_TASK_THREAD_POOL("clear_transaction", 0, config::clear_transaction_task_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_clear_transaction);

        BUILD_DYNAMIC_TASK_THREAD_POOL("storage_medium_migrate", 0, config::storage_medium_migrate_count,
                                       std::numeric_limits<int>::max(), _thread_pool_storage_medium_migrate);

        BUILD_DYNAMIC_TASK_THREAD_POOL("check_consistency", 0, config::check_consistency_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_check_consistency);

        BUILD_DYNAMIC_TASK_THREAD_POOL("manual_compaction", 0, 1, std::numeric_limits<int>::max(),
                                       _thread_pool_compaction);

        BUILD_DYNAMIC_TASK_THREAD_POOL("update_schema", 0, config::update_schema_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_update_schema);

        BUILD_DYNAMIC_TASK_THREAD_POOL("upload", 0, config::upload_worker_count, std::numeric_limits<int>::max(),
                                       _thread_pool_upload);

        BUILD_DYNAMIC_TASK_THREAD_POOL("download", 0, config::download_worker_count, std::numeric_limits<int>::max(),
                                       _thread_pool_download);

        BUILD_DYNAMIC_TASK_THREAD_POOL("make_snapshot", 0, config::make_snapshot_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_make_snapshot);

        BUILD_DYNAMIC_TASK_THREAD_POOL("release_snapshot", 0, config::release_snapshot_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_release_snapshot);

        BUILD_DYNAMIC_TASK_THREAD_POOL("move_dir", 0, 1, std::numeric_limits<int>::max(), _thread_pool_move_dir);

        BUILD_DYNAMIC_TASK_THREAD_POOL("update_tablet_meta_info", 0, 1, std::numeric_limits<int>::max(),
                                       _thread_pool_update_tablet_meta_info);

        BUILD_DYNAMIC_TASK_THREAD_POOL("drop_auto_increment_map_dir", 0, 1, std::numeric_limits<int>::max(),
                                       _thread_pool_drop_auto_increment_map);

        // Currently FE can have at most num_of_storage_path * schedule_slot_num_per_path(default 2) clone tasks
        // scheduled simultaneously, but previously we have only 3 clone worker threads by default,
        // so this is to keep the dop of clone task handling in sync with FE.
        //
        // TODO(shangyiming): using dynamic thread pool to handle task directly instead of using TaskThreadPool
        // Currently, the task submission and processing logic is deeply coupled with TaskThreadPool, change that will
        // need to modify many interfaces. So for now we still use TaskThreadPool to submit clone tasks, but with
        // only a single worker thread, then we use dynamic thread pool to handle the task concurrently in clone task
        // callback, so that we can match the dop of FE clone task scheduling.
        BUILD_DYNAMIC_TASK_THREAD_POOL("clone", 0,
                                       std::max(_exec_env->store_paths().size() * config::parallel_clone_task_per_path,
                                                MIN_CLONE_TASK_THREADS_IN_POOL),
                                       DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE, _thread_pool_clone);

        BUILD_DYNAMIC_TASK_THREAD_POOL(
                "replication", 0, config::replication_threads > 0 ? config::replication_threads : CpuInfo::num_cores(),
                std::numeric_limits<int>::max(), _thread_pool_replication);

        // It is the same code to create workers of each type, so we use a macro
        // to make code to be more readable.
#ifndef BE_TEST
#define CREATE_AND_START_POOL(pool_name, CLASS_NAME, worker_num) \
    pool_name.reset(new CLASS_NAME(_exec_env, worker_num));      \
    pool_name->start();
#else
#define CREATE_AND_START_POOL(pool_name, CLASS_NAME, worker_num)
#endif // BE_TEST

        CREATE_AND_START_POOL(_publish_version_workers, PublishVersionTaskWorkerPool, CpuInfo::num_cores())
        // Both PUSH and REALTIME_PUSH type use _push_workers
        CREATE_AND_START_POOL(_push_workers, PushTaskWorkerPool,
                              config::push_worker_count_high_priority + config::push_worker_count_normal_priority)
        CREATE_AND_START_POOL(_delete_workers, DeleteTaskWorkerPool,
                              config::delete_worker_count_normal_priority + config::delete_worker_count_high_priority)
        CREATE_AND_START_POOL(_report_task_workers, ReportTaskWorkerPool, REPORT_TASK_WORKER_COUNT)
        CREATE_AND_START_POOL(_report_disk_state_workers, ReportDiskStateTaskWorkerPool, REPORT_DISK_STATE_WORKER_COUNT)
        CREATE_AND_START_POOL(_report_tablet_workers, ReportOlapTableTaskWorkerPool, REPORT_OLAP_TABLE_WORKER_COUNT)
        CREATE_AND_START_POOL(_report_workgroup_workers, ReportWorkgroupTaskWorkerPool, REPORT_WORKGROUP_WORKER_COUNT)
    }
    CREATE_AND_START_POOL(_report_resource_usage_workers, ReportResourceUsageTaskWorkerPool,
                          REPORT_RESOURCE_USAGE_WORKER_COUNT)
    CREATE_AND_START_POOL(_report_datacache_metrics_workers, ReportDataCacheMetricsTaskWorkerPool,
                          REPORT_DATACACHE_METRICS_WORKER_COUNT)
#undef CREATE_AND_START_POOL
}

void AgentServer::Impl::stop() {
    if (!_is_compute_node) {
        _thread_pool_publish_version->shutdown();
        _thread_pool_drop->shutdown();
        _thread_pool_create_tablet->shutdown();
        _thread_pool_alter_tablet->shutdown();
        _thread_pool_clear_transaction->shutdown();
        _thread_pool_storage_medium_migrate->shutdown();
        _thread_pool_check_consistency->shutdown();
        _thread_pool_compaction->shutdown();
        _thread_pool_update_schema->shutdown();
        _thread_pool_upload->shutdown();
        _thread_pool_download->shutdown();
        _thread_pool_make_snapshot->shutdown();
        _thread_pool_release_snapshot->shutdown();
        _thread_pool_move_dir->shutdown();
        _thread_pool_update_tablet_meta_info->shutdown();
        _thread_pool_drop_auto_increment_map->shutdown();

#ifndef BE_TEST
        _thread_pool_clone->shutdown();
        _thread_pool_replication->shutdown();
#define STOP_POOL(type, pool_name) pool_name->stop();
#else
#define STOP_POOL(type, pool_name)
#endif // BE_TEST
        STOP_POOL(PUBLISH_VERSION, _publish_version_workers);
        // Both PUSH and REALTIME_PUSH type use _push_workers
        STOP_POOL(PUSH, _push_workers);
        STOP_POOL(DELETE, _delete_workers);
        STOP_POOL(REPORT_TASK, _report_task_workers);
        STOP_POOL(REPORT_DISK_STATE, _report_disk_state_workers);
        STOP_POOL(REPORT_OLAP_TABLE, _report_tablet_workers);
        STOP_POOL(REPORT_WORKGROUP, _report_workgroup_workers);
    }
    STOP_POOL(REPORT_WORKGROUP, _report_resource_usage_workers);
    STOP_POOL(REPORT_DATACACHE_METRICS, _report_datacache_metrics_workers);
#undef STOP_POOL
}

AgentServer::Impl::~Impl() = default;

// TODO(lingbin): each task in the batch may have it own status or FE must check and
// resend request when something is wrong(BE may need some logic to guarantee idempotence.
void AgentServer::Impl::submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks) {
    Status ret_st;
    auto master_address = get_master_address();
    if (master_address.hostname.empty() || master_address.port == 0) {
        ret_st = Status::Cancelled("Have not get FE Master heartbeat yet");
        ret_st.to_thrift(&agent_result.status);
        return;
    }

    phmap::flat_hash_map<TTaskType::type, std::vector<const TAgentTaskRequest*>, TTaskTypeHash> task_divider;
    phmap::flat_hash_map<TPushType::type, std::vector<const TAgentTaskRequest*>, TTaskTypeHash> push_divider;

    for (const auto& task : tasks) {
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        TTaskType::type task_type = task.task_type;
        int64_t signature = task.signature;

#define HANDLE_TYPE(t_task_type, req_member)                                                        \
    case t_task_type:                                                                               \
        if (task.__isset.req_member) {                                                              \
            task_divider[t_task_type].push_back(&task);                                             \
        } else {                                                                                    \
            ret_st = Status::InvalidArgument(                                                       \
                    strings::Substitute("task(signature=$0) has wrong request member", signature)); \
        }                                                                                           \
        break;

        // TODO(lingbin): It still too long, divided these task types into several categories
        switch (task_type) {
            HANDLE_TYPE(TTaskType::CREATE, create_tablet_req);
            HANDLE_TYPE(TTaskType::DROP, drop_tablet_req);
            HANDLE_TYPE(TTaskType::PUBLISH_VERSION, publish_version_req);
            HANDLE_TYPE(TTaskType::CLEAR_TRANSACTION_TASK, clear_transaction_task_req);
            HANDLE_TYPE(TTaskType::CLONE, clone_req);
            HANDLE_TYPE(TTaskType::STORAGE_MEDIUM_MIGRATE, storage_medium_migrate_req);
            HANDLE_TYPE(TTaskType::CHECK_CONSISTENCY, check_consistency_req);
            HANDLE_TYPE(TTaskType::COMPACTION, compaction_req);
            HANDLE_TYPE(TTaskType::UPLOAD, upload_req);
            HANDLE_TYPE(TTaskType::UPDATE_SCHEMA, update_schema_req);
            HANDLE_TYPE(TTaskType::DOWNLOAD, download_req);
            HANDLE_TYPE(TTaskType::MAKE_SNAPSHOT, snapshot_req);
            HANDLE_TYPE(TTaskType::RELEASE_SNAPSHOT, release_snapshot_req);
            HANDLE_TYPE(TTaskType::MOVE, move_dir_req);
            HANDLE_TYPE(TTaskType::UPDATE_TABLET_META_INFO, update_tablet_meta_info_req);
            HANDLE_TYPE(TTaskType::DROP_AUTO_INCREMENT_MAP, drop_auto_increment_map_req);
            HANDLE_TYPE(TTaskType::REMOTE_SNAPSHOT, remote_snapshot_req);
            HANDLE_TYPE(TTaskType::REPLICATE_SNAPSHOT, replicate_snapshot_req);

        case TTaskType::REALTIME_PUSH:
            if (!task.__isset.push_req) {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0) has wrong request member", signature));
                break;
            }
            if (task.push_req.push_type == TPushType::LOAD_V2 || task.push_req.push_type == TPushType::DELETE ||
                task.push_req.push_type == TPushType::CANCEL_DELETE) {
                push_divider[task.push_req.push_type].push_back(&task);
            } else {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0, type=$1, push_type=$2) has wrong push_type", signature,
                                            task_type, task.push_req.push_type));
            }
            break;
        case TTaskType::ALTER:
            if (task.__isset.alter_tablet_req || task.__isset.alter_tablet_req_v2) {
                task_divider[TTaskType::ALTER].push_back(&task);
            } else {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0) has wrong request member", signature));
            }
            break;
        default:
            ret_st = Status::InvalidArgument(
                    strings::Substitute("task(signature=$0, type=$1) has wrong task type", signature, task_type));
            break;
        }
#undef HANDLE_TYPE

        if (!ret_st.ok()) {
            LOG(WARNING) << "fail to submit task. reason: " << ret_st.message() << ", task: " << task;
            // For now, all tasks in the batch share one status, so if any task
            // was failed to submit, we can only return error to FE(even when some
            // tasks have already been successfully submitted).
            // However, Fe does not check the return status of submit_tasks() currently,
            // and it is not sure that FE will retry when something is wrong, so here we
            // only print an warning log and go on(i.e. do not break current loop),
            // to ensure every task can be submitted once. It is OK for now, because the
            // ret_st can be error only when it encounters an wrong task_type and
            // req-member in TAgentTaskRequest, which is basically impossible.
            // TODO(lingbin): check the logic in FE again later.
        }
    }

#define HANDLE_TASK(t_task_type, all_tasks, do_func, AGENT_REQ, request, env)                                      \
    for (auto* task : all_tasks) {                                                                                 \
        auto pool = get_thread_pool(t_task_type);                                                                  \
        auto signature = task->signature;                                                                          \
        std::pair<bool, size_t> register_pair = register_task_info(task_type, signature);                          \
        if (register_pair.first) {                                                                                 \
            LOG(INFO) << "Submit task success. type=" << t_task_type << ", signature=" << signature                \
                      << ", task_count_in_queue=" << register_pair.second;                                         \
            ret_st = pool->submit_func(                                                                            \
                    std::bind(do_func, std::make_shared<AGENT_REQ>(*task, task->request, time(nullptr)), env));    \
            if (!ret_st.ok()) {                                                                                    \
                LOG(WARNING) << "fail to submit task. reason: " << ret_st.message() << ", task: " << task;         \
            }                                                                                                      \
        } else {                                                                                                   \
            LOG(INFO) << "Submit task failed, already exists type=" << t_task_type << ", signature=" << signature; \
        }                                                                                                          \
    }

    // batch submit tasks
    for (const auto& task_item : task_divider) {
        const auto& task_type = task_item.first;
        auto all_tasks = task_item.second;
        switch (task_type) {
        case TTaskType::CREATE:
            HANDLE_TASK(TTaskType::CREATE, all_tasks, run_create_tablet_task, CreateTabletAgentTaskRequest,
                        create_tablet_req, _exec_env);
            break;
        case TTaskType::DROP:
            HANDLE_TASK(TTaskType::DROP, all_tasks, run_drop_tablet_task, DropTabletAgentTaskRequest, drop_tablet_req,
                        _exec_env);
            break;
        case TTaskType::PUBLISH_VERSION: {
            for (const auto& task : all_tasks) {
                _publish_version_workers->submit_task(*task);
            }
            break;
        }
        case TTaskType::CLEAR_TRANSACTION_TASK:
            HANDLE_TASK(TTaskType::CLEAR_TRANSACTION_TASK, all_tasks, run_clear_transaction_task,
                        ClearTransactionAgentTaskRequest, clear_transaction_task_req, _exec_env);
            break;
        case TTaskType::CLONE:
            HANDLE_TASK(TTaskType::CLONE, all_tasks, run_clone_task, CloneAgentTaskRequest, clone_req, _exec_env);
            break;
        case TTaskType::STORAGE_MEDIUM_MIGRATE:
            HANDLE_TASK(TTaskType::STORAGE_MEDIUM_MIGRATE, all_tasks, run_storage_medium_migrate_task,
                        StorageMediumMigrateTaskRequest, storage_medium_migrate_req, _exec_env);
            break;
        case TTaskType::CHECK_CONSISTENCY:
            HANDLE_TASK(TTaskType::CHECK_CONSISTENCY, all_tasks, run_check_consistency_task,
                        CheckConsistencyTaskRequest, check_consistency_req, _exec_env);
            break;
        case TTaskType::COMPACTION:
            HANDLE_TASK(TTaskType::COMPACTION, all_tasks, run_compaction_task, CompactionTaskRequest, compaction_req,
                        _exec_env);
            break;
        case TTaskType::UPDATE_SCHEMA:
            HANDLE_TASK(TTaskType::UPDATE_SCHEMA, all_tasks, run_update_schema_task, UpdateSchemaTaskRequest,
                        update_schema_req, _exec_env);
            break;
        case TTaskType::UPLOAD:
            HANDLE_TASK(TTaskType::UPLOAD, all_tasks, run_upload_task, UploadAgentTaskRequest, upload_req, _exec_env);
            break;
        case TTaskType::DOWNLOAD:
            HANDLE_TASK(TTaskType::DOWNLOAD, all_tasks, run_download_task, DownloadAgentTaskRequest, download_req,
                        _exec_env);
            break;
        case TTaskType::MAKE_SNAPSHOT:
            HANDLE_TASK(TTaskType::MAKE_SNAPSHOT, all_tasks, run_make_snapshot_task, SnapshotAgentTaskRequest,
                        snapshot_req, _exec_env);
            break;
        case TTaskType::RELEASE_SNAPSHOT:
            HANDLE_TASK(TTaskType::RELEASE_SNAPSHOT, all_tasks, run_release_snapshot_task,
                        ReleaseSnapshotAgentTaskRequest, release_snapshot_req, _exec_env);
            break;
        case TTaskType::MOVE:
            HANDLE_TASK(TTaskType::MOVE, all_tasks, run_move_dir_task, MoveDirAgentTaskRequest, move_dir_req,
                        _exec_env);
            break;
        case TTaskType::UPDATE_TABLET_META_INFO:
            HANDLE_TASK(TTaskType::UPDATE_TABLET_META_INFO, all_tasks, run_update_meta_info_task,
                        UpdateTabletMetaInfoAgentTaskRequest, update_tablet_meta_info_req, _exec_env);
            break;
        case TTaskType::DROP_AUTO_INCREMENT_MAP:
            HANDLE_TASK(TTaskType::DROP_AUTO_INCREMENT_MAP, all_tasks, run_drop_auto_increment_map_task,
                        DropAutoIncrementMapAgentTaskRequest, drop_auto_increment_map_req, _exec_env);
            break;
        case TTaskType::REMOTE_SNAPSHOT:
            HANDLE_TASK(TTaskType::REMOTE_SNAPSHOT, all_tasks, run_remote_snapshot_task, RemoteSnapshotAgentTaskRequest,
                        remote_snapshot_req, _exec_env);
            break;
        case TTaskType::REPLICATE_SNAPSHOT:
            HANDLE_TASK(TTaskType::REPLICATE_SNAPSHOT, all_tasks, run_replicate_snapshot_task,
                        ReplicateSnapshotAgentTaskRequest, replicate_snapshot_req, _exec_env);
            break;
        case TTaskType::REALTIME_PUSH:
        case TTaskType::PUSH: {
            // should not run here
            break;
        }
        case TTaskType::ALTER:
            HANDLE_TASK(TTaskType::ALTER, all_tasks, run_alter_tablet_task, AlterTabletAgentTaskRequest,
                        alter_tablet_req_v2, _exec_env);
            break;
        default:
            ret_st = Status::InvalidArgument(strings::Substitute("tasks(type=$0) has wrong task type", task_type));
            LOG(WARNING) << "fail to batch submit task. reason: " << ret_st.message();
        }
    }

    // batch submit push tasks
    if (!push_divider.empty()) {
        LOG(INFO) << "begin batch submit task: " << tasks[0].task_type;
        for (const auto& push_item : push_divider) {
            const auto& push_type = push_item.first;
            auto all_push_tasks = push_item.second;
            switch (push_type) {
            case TPushType::LOAD_V2:
                _push_workers->submit_tasks(all_push_tasks);
                break;
            case TPushType::DELETE:
            case TPushType::CANCEL_DELETE:
                _delete_workers->submit_tasks(all_push_tasks);
                break;
            default:
                ret_st = Status::InvalidArgument(strings::Substitute("tasks(type=$0, push_type=$1) has wrong task type",
                                                                     TTaskType::PUSH, push_type));
                LOG(WARNING) << "fail to batch submit push task. reason: " << ret_st.message();
            }
        }
    }

    ret_st.to_thrift(&agent_result.status);
}

void AgentServer::Impl::make_snapshot(TAgentResult& t_agent_result, const TSnapshotRequest& snapshot_request) {
    std::string snapshot_path;
    auto st = SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path);
    if (!st.ok()) {
        LOG(WARNING) << "fail to make_snapshot. tablet_id:" << snapshot_request.tablet_id << " msg:" << st.to_string();
    } else {
        LOG(INFO) << "success to make_snapshot. tablet_id:" << snapshot_request.tablet_id << " path:" << snapshot_path;
        t_agent_result.__set_snapshot_path(snapshot_path);
    }

    st.to_thrift(&t_agent_result.status);
    t_agent_result.__set_snapshot_format(snapshot_request.preferred_snapshot_format);
    t_agent_result.__set_allow_incremental_clone(true);
}

void AgentServer::Impl::release_snapshot(TAgentResult& t_agent_result, const std::string& snapshot_path) {
    Status ret_st = SnapshotManager::instance()->release_snapshot(snapshot_path);
    if (!ret_st.ok()) {
        LOG(WARNING) << "Fail to release_snapshot. snapshot_path:" << snapshot_path;
    } else {
        LOG(INFO) << "success to release_snapshot. snapshot_path:" << snapshot_path;
    }
    ret_st.to_thrift(&t_agent_result.status);
}

void AgentServer::Impl::publish_cluster_state(TAgentResult& t_agent_result, const TAgentPublishRequest& request) {
    Status status = Status::NotSupported("deprecated method(publish_cluster_state) was invoked");
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::Impl::update_max_thread_by_type(int type, int new_val) {
    Status st;
    switch (type) {
    case TTaskType::CLONE:
        st = _thread_pool_clone->update_max_threads(new_val);
        break;
    case TTaskType::REMOTE_SNAPSHOT:
    case TTaskType::REPLICATE_SNAPSHOT:
        st = _thread_pool_replication->update_max_threads(new_val > 0 ? new_val : CpuInfo::num_cores());
        break;
    default:
        break;
    }
    LOG_IF(ERROR, !st.ok()) << st;
}

ThreadPool* AgentServer::Impl::get_thread_pool(int type) const {
    // TODO: more thread pools.
    ThreadPool* ret = nullptr;
    switch (type) {
    case TTaskType::PUBLISH_VERSION:
        ret = _thread_pool_publish_version.get();
        break;
    case TTaskType::CLONE:
        ret = _thread_pool_clone.get();
        break;
    case TTaskType::DROP:
        ret = _thread_pool_drop.get();
        break;
    case TTaskType::CREATE:
        ret = _thread_pool_create_tablet.get();
        break;
    case TTaskType::STORAGE_MEDIUM_MIGRATE:
        ret = _thread_pool_storage_medium_migrate.get();
        break;
    case TTaskType::MAKE_SNAPSHOT:
        ret = _thread_pool_make_snapshot.get();
        break;
    case TTaskType::RELEASE_SNAPSHOT:
        ret = _thread_pool_release_snapshot.get();
        break;
    case TTaskType::CHECK_CONSISTENCY:
        ret = _thread_pool_check_consistency.get();
        break;
    case TTaskType::COMPACTION:
        ret = _thread_pool_compaction.get();
        break;
    case TTaskType::UPDATE_SCHEMA:
        ret = _thread_pool_update_schema.get();
        break;
    case TTaskType::UPLOAD:
        ret = _thread_pool_upload.get();
        break;
    case TTaskType::DOWNLOAD:
        ret = _thread_pool_download.get();
        break;
    case TTaskType::MOVE:
        ret = _thread_pool_move_dir.get();
        break;
    case TTaskType::UPDATE_TABLET_META_INFO:
        ret = _thread_pool_update_tablet_meta_info.get();
        break;
    case TTaskType::ALTER:
        ret = _thread_pool_alter_tablet.get();
        break;
    case TTaskType::CLEAR_TRANSACTION_TASK:
        ret = _thread_pool_clear_transaction.get();
        break;
    case TTaskType::DROP_AUTO_INCREMENT_MAP:
        ret = _thread_pool_drop_auto_increment_map.get();
        break;
    case TTaskType::REMOTE_SNAPSHOT:
    case TTaskType::REPLICATE_SNAPSHOT:
        ret = _thread_pool_replication.get();
        break;
    case TTaskType::PUSH:
    case TTaskType::REALTIME_PUSH:
    case TTaskType::ROLLUP:
    case TTaskType::SCHEMA_CHANGE:
    case TTaskType::CANCEL_DELETE:
    case TTaskType::CLEAR_REMOTE_FILE:
    case TTaskType::CLEAR_ALTER_TASK:
    case TTaskType::RECOVER_TABLET:
    case TTaskType::STREAM_LOAD:
    case TTaskType::INSTALL_PLUGIN:
    case TTaskType::UNINSTALL_PLUGIN:
    case TTaskType::NUM_TASK_TYPE:
        break;
    }
    TEST_SYNC_POINT_CALLBACK("AgentServer::Impl::get_thread_pool:1", &ret);
    return ret;
}

AgentServer::AgentServer(ExecEnv* exec_env, bool is_compute_node)
        : _impl(std::make_unique<AgentServer::Impl>(exec_env, is_compute_node)) {}

AgentServer::~AgentServer() = default;

void AgentServer::submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks) {
    _impl->submit_tasks(agent_result, tasks);
}

void AgentServer::make_snapshot(TAgentResult& agent_result, const TSnapshotRequest& snapshot_request) {
    _impl->make_snapshot(agent_result, snapshot_request);
}

void AgentServer::release_snapshot(TAgentResult& agent_result, const std::string& snapshot_path) {
    _impl->release_snapshot(agent_result, snapshot_path);
}

void AgentServer::publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request) {
    _impl->publish_cluster_state(agent_result, request);
}

void AgentServer::update_max_thread_by_type(int type, int new_val) {
    _impl->update_max_thread_by_type(type, new_val);
}

ThreadPool* AgentServer::get_thread_pool(int type) const {
    return _impl->get_thread_pool(type);
}

void AgentServer::init_or_die() {
    return _impl->init_or_die();
}

void AgentServer::stop() {
    return _impl->stop();
}

} // namespace starrocks
