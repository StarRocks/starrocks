// This file is made available under Elastic License 2.0.
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

#include "agent/master_info.h"
#include "agent/task_worker_pool.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "storage/snapshot_manager.h"
#include "util/phmap/phmap.h"
#include "util/threadpool.h"

namespace starrocks {

namespace {
constexpr size_t DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE = 2048;
constexpr size_t MIN_CLONE_TASK_THREADS_IN_POOL = 2;
} // namespace

const uint32_t REPORT_TASK_WORKER_COUNT = 1;
const uint32_t REPORT_DISK_STATE_WORKER_COUNT = 1;
const uint32_t REPORT_OLAP_TABLE_WORKER_COUNT = 1;
const uint32_t REPORT_WORKGROUP_WORKER_COUNT = 1;

class AgentServer::Impl {
public:
    explicit Impl(ExecEnv* exec_env) : _exec_env(exec_env) {}

    ~Impl();

    void init_or_die();

    void submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks);

    void make_snapshot(TAgentResult& agent_result, const TSnapshotRequest& snapshot_request);

    void release_snapshot(TAgentResult& agent_result, const std::string& snapshot_path);

    void publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request);

    ThreadPool* get_thread_pool(int type) const;

    DISALLOW_COPY_AND_MOVE(Impl);

private:
    ExecEnv* _exec_env;

    std::unique_ptr<ThreadPool> _thread_pool_publish_version;
    std::unique_ptr<ThreadPool> _thread_pool_clone;

    std::unique_ptr<TaskWorkerPool<TaskWorkerType::CREATE_TABLE>> _create_tablet_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::DROP_TABLE>> _drop_tablet_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::PUSH>> _push_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::PUBLISH_VERSION>> _publish_version_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::CLEAR_TRANSACTION_TASK>> _clear_transaction_task_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::DELETE>> _delete_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::ALTER_TABLE>> _alter_tablet_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::CLONE>> _clone_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::STORAGE_MEDIUM_MIGRATE>> _storage_medium_migrate_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::CHECK_CONSISTENCY>> _check_consistency_workers;

    // These 3 worker-pool do not accept tasks from FE.
    // It is self triggered periodically and reports to Fe master
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::REPORT_TASK>> _report_task_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::REPORT_DISK_STATE>> _report_disk_state_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::REPORT_OLAP_TABLE>> _report_tablet_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::REPORT_WORKGROUP>> _report_workgroup_workers;

    std::unique_ptr<TaskWorkerPool<TaskWorkerType::UPLOAD>> _upload_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::DOWNLOAD>> _download_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::MAKE_SNAPSHOT>> _make_snapshot_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::RELEASE_SNAPSHOT>> _release_snapshot_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::MOVE>> _move_dir_workers;
    std::unique_ptr<TaskWorkerPool<TaskWorkerType::UPDATE_TABLET_META_INFO>> _update_tablet_meta_info_workers;
};

void AgentServer::Impl::init_or_die() {
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
    BUILD_DYNAMIC_TASK_THREAD_POOL("publish_version", config::transaction_publish_version_worker_count,
                                   config::transaction_publish_version_worker_count,
                                   DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE, _thread_pool_publish_version);
#ifndef BE_TEST
    // Currently FE can have at most num_of_storage_path * schedule_slot_num_per_path(default 2) clone tasks
    // scheduled simultaneously, but previously we have only 3 clone worker threads by default,
    // so this is to keep the dop of clone task handling in sync with FE.
    //
    // TODO(shangyiming): using dynamic thread pool to handle task directly instead of using TaskThreadPool
    // Currently, the task submission and processing logic is deeply coupled with TaskThreadPool, change that will
    // need to modify many interfaces. So for now we still use TaskThreadPool to submit clone tasks, but with
    // only a single worker thread, then we use dynamic thread pool to handle the task concurrently in clone task
    // callback, so that we can match the dop of FE clone task scheduling.
    BUILD_DYNAMIC_TASK_THREAD_POOL("clone", MIN_CLONE_TASK_THREADS_IN_POOL,
                                   _exec_env->store_paths().size() * config::parallel_clone_task_per_path,
                                   DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE, _thread_pool_clone);
#endif

    // It is the same code to create workers of each type, so we use a macro
    // to make code to be more readable.
#ifndef BE_TEST
#define CREATE_AND_START_POOL(type, pool_name, worker_num)            \
    pool_name.reset(new TaskWorkerPool<type>(_exec_env, worker_num)); \
    pool_name->start();
#else
#define CREATE_AND_START_POOL(type, pool_name, worker_num)
#endif // BE_TEST

    CREATE_AND_START_POOL(TaskWorkerType::CREATE_TABLE, _create_tablet_workers, config::create_tablet_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::DROP_TABLE, _drop_tablet_workers, config::drop_tablet_worker_count);
    // Both PUSH and REALTIME_PUSH type use _push_workers
    CREATE_AND_START_POOL(TaskWorkerType::PUSH, _push_workers,
                          config::push_worker_count_normal_priority + config::push_worker_count_high_priority);
    CREATE_AND_START_POOL(TaskWorkerType::PUBLISH_VERSION, _publish_version_workers, 1);
    CREATE_AND_START_POOL(TaskWorkerType::CLEAR_TRANSACTION_TASK, _clear_transaction_task_workers,
                          config::clear_transaction_task_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::DELETE, _delete_workers,
                          config::delete_worker_count_normal_priority + config::delete_worker_count_high_priority);
    CREATE_AND_START_POOL(TaskWorkerType::ALTER_TABLE, _alter_tablet_workers, config::alter_tablet_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::CLONE, _clone_workers, 1);
    CREATE_AND_START_POOL(TaskWorkerType::STORAGE_MEDIUM_MIGRATE, _storage_medium_migrate_workers,
                          config::storage_medium_migrate_count);
    CREATE_AND_START_POOL(TaskWorkerType::CHECK_CONSISTENCY, _check_consistency_workers,
                          config::check_consistency_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::REPORT_TASK, _report_task_workers, REPORT_TASK_WORKER_COUNT);
    CREATE_AND_START_POOL(TaskWorkerType::REPORT_DISK_STATE, _report_disk_state_workers,
                          REPORT_DISK_STATE_WORKER_COUNT);
    CREATE_AND_START_POOL(TaskWorkerType::REPORT_OLAP_TABLE, _report_tablet_workers, REPORT_OLAP_TABLE_WORKER_COUNT);
    CREATE_AND_START_POOL(TaskWorkerType::REPORT_WORKGROUP, _report_workgroup_workers, REPORT_WORKGROUP_WORKER_COUNT);
    CREATE_AND_START_POOL(TaskWorkerType::UPLOAD, _upload_workers, config::upload_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::DOWNLOAD, _download_workers, config::download_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::MAKE_SNAPSHOT, _make_snapshot_workers, config::make_snapshot_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::RELEASE_SNAPSHOT, _release_snapshot_workers,
                          config::release_snapshot_worker_count);
    CREATE_AND_START_POOL(TaskWorkerType::MOVE, _move_dir_workers, 1);
    CREATE_AND_START_POOL(TaskWorkerType::UPDATE_TABLET_META_INFO, _update_tablet_meta_info_workers, 1);
#undef CREATE_AND_START_POOL
}

AgentServer::Impl::~Impl() {
    _thread_pool_publish_version->shutdown();

#ifndef BE_TEST
    _thread_pool_clone->shutdown();
#define STOP_POOL(pool_name) pool_name->stop();
#else
#define STOP_POOL(pool_name)
#endif // BE_TEST
    STOP_POOL(_create_tablet_workers);
    STOP_POOL(_drop_tablet_workers);
    // Both PUSH and REALTIME_PUSH type use _push_workers
    STOP_POOL(_push_workers);
    STOP_POOL(_publish_version_workers);
    STOP_POOL(_clear_transaction_task_workers);
    STOP_POOL(_delete_workers);
    STOP_POOL(_alter_tablet_workers);
    STOP_POOL(_clone_workers);
    STOP_POOL(_storage_medium_migrate_workers);
    STOP_POOL(_check_consistency_workers);
    STOP_POOL(_report_task_workers);
    STOP_POOL(_report_disk_state_workers);
    STOP_POOL(_report_tablet_workers);
    STOP_POOL(_report_workgroup_workers);
    STOP_POOL(_upload_workers);
    STOP_POOL(_download_workers);
    STOP_POOL(_make_snapshot_workers);
    STOP_POOL(_release_snapshot_workers);
    STOP_POOL(_move_dir_workers);
    STOP_POOL(_update_tablet_meta_info_workers);
#undef STOP_POOL
}

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

    phmap::flat_hash_map<TTaskType::type, std::vector<TAgentTaskRequest>> task_divider;
    phmap::flat_hash_map<TPushType::type, std::vector<TAgentTaskRequest>> push_divider;

    for (const auto& task : tasks) {
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        TTaskType::type task_type = task.task_type;
        int64_t signature = task.signature;

#define HANDLE_TYPE(t_task_type, work_pool, req_member)                                             \
    case t_task_type:                                                                               \
        if (task.__isset.req_member) {                                                              \
            task_divider[t_task_type].push_back(task);                                              \
        } else {                                                                                    \
            ret_st = Status::InvalidArgument(                                                       \
                    strings::Substitute("task(signature=$0) has wrong request member", signature)); \
        }                                                                                           \
        break;

        // TODO(lingbin): It still too long, divided these task types into several categories
        switch (task_type) {
            HANDLE_TYPE(TTaskType::CREATE, _create_tablet_workers, create_tablet_req);
            HANDLE_TYPE(TTaskType::DROP, _drop_tablet_workers, drop_tablet_req);
            HANDLE_TYPE(TTaskType::PUBLISH_VERSION, _publish_version_workers, publish_version_req);
            HANDLE_TYPE(TTaskType::CLEAR_TRANSACTION_TASK, _clear_transaction_task_workers, clear_transaction_task_req);
            HANDLE_TYPE(TTaskType::CLONE, _clone_workers, clone_req);
            HANDLE_TYPE(TTaskType::STORAGE_MEDIUM_MIGRATE, _storage_medium_migrate_workers, storage_medium_migrate_req);
            HANDLE_TYPE(TTaskType::CHECK_CONSISTENCY, _check_consistency_workers, check_consistency_req);
            HANDLE_TYPE(TTaskType::UPLOAD, _upload_workers, upload_req);
            HANDLE_TYPE(TTaskType::DOWNLOAD, _download_workers, download_req);
            HANDLE_TYPE(TTaskType::MAKE_SNAPSHOT, _make_snapshot_workers, snapshot_req);
            HANDLE_TYPE(TTaskType::RELEASE_SNAPSHOT, _release_snapshot_workers, release_snapshot_req);
            HANDLE_TYPE(TTaskType::MOVE, _move_dir_workers, move_dir_req);
            HANDLE_TYPE(TTaskType::UPDATE_TABLET_META_INFO, _update_tablet_meta_info_workers,
                        update_tablet_meta_info_req);

        case TTaskType::REALTIME_PUSH:
            if (!task.__isset.push_req) {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0) has wrong request member", signature));
                break;
            }
            if (task.push_req.push_type == TPushType::LOAD_V2 || task.push_req.push_type == TPushType::DELETE ||
                task.push_req.push_type == TPushType::CANCEL_DELETE) {
                push_divider[task.push_req.push_type].push_back(task);
            } else {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0, type=$1, push_type=$2) has wrong push_type", signature,
                                            task_type, task.push_req.push_type));
            }
            break;
        case TTaskType::ALTER:
            if (task.__isset.alter_tablet_req || task.__isset.alter_tablet_req_v2) {
                task_divider[TTaskType::ALTER].push_back(task);
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
            LOG(WARNING) << "fail to submit task. reason: " << ret_st.get_error_msg() << ", task: " << task;
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

    // batch submit tasks
    for (const auto& task_item : task_divider) {
        const auto& task_type = task_item.first;
        auto all_tasks = task_item.second;
        switch (task_type) {
        case TTaskType::CREATE:
            _create_tablet_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::DROP:
            _drop_tablet_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::PUBLISH_VERSION: {
            for (const auto& task : all_tasks) {
                _publish_version_workers->submit_task(task);
            }
            break;
        }
        case TTaskType::CLEAR_TRANSACTION_TASK:
            _clear_transaction_task_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::CLONE:
            _clone_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::STORAGE_MEDIUM_MIGRATE:
            _storage_medium_migrate_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::CHECK_CONSISTENCY:
            _check_consistency_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::UPLOAD:
            _upload_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::DOWNLOAD:
            _download_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::MAKE_SNAPSHOT:
            _make_snapshot_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::RELEASE_SNAPSHOT:
            _release_snapshot_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::MOVE:
            _move_dir_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::UPDATE_TABLET_META_INFO:
            _update_tablet_meta_info_workers->submit_tasks(all_tasks);
            break;
        case TTaskType::REALTIME_PUSH:
        case TTaskType::PUSH: {
            // should not run here
            break;
        }
        case TTaskType::ALTER:
            _alter_tablet_workers->submit_tasks(all_tasks);
            break;
        default:
            ret_st = Status::InvalidArgument(strings::Substitute("tasks(type=$0) has wrong task type", task_type));
            LOG(WARNING) << "fail to batch submit task. reason: " << ret_st.get_error_msg();
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
                LOG(WARNING) << "fail to batch submit push task. reason: " << ret_st.get_error_msg();
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

ThreadPool* AgentServer::Impl::get_thread_pool(int type) const {
    // TODO: more thread pools.
    switch (type) {
    case TTaskType::PUBLISH_VERSION:
        return _thread_pool_publish_version.get();
    case TTaskType::CLONE:
        return _thread_pool_clone.get();
    case TTaskType::CREATE:
    case TTaskType::DROP:
    case TTaskType::PUSH:
    case TTaskType::STORAGE_MEDIUM_MIGRATE:
    case TTaskType::ROLLUP:
    case TTaskType::SCHEMA_CHANGE:
    case TTaskType::CANCEL_DELETE:
    case TTaskType::MAKE_SNAPSHOT:
    case TTaskType::RELEASE_SNAPSHOT:
    case TTaskType::CHECK_CONSISTENCY:
    case TTaskType::UPLOAD:
    case TTaskType::DOWNLOAD:
    case TTaskType::CLEAR_REMOTE_FILE:
    case TTaskType::MOVE:
    case TTaskType::REALTIME_PUSH:
    case TTaskType::CLEAR_ALTER_TASK:
    case TTaskType::CLEAR_TRANSACTION_TASK:
    case TTaskType::RECOVER_TABLET:
    case TTaskType::STREAM_LOAD:
    case TTaskType::UPDATE_TABLET_META_INFO:
    case TTaskType::ALTER:
    case TTaskType::INSTALL_PLUGIN:
    case TTaskType::UNINSTALL_PLUGIN:
    case TTaskType::NUM_TASK_TYPE:
    default:
        break;
    }
    return nullptr;
}

AgentServer::AgentServer(ExecEnv* exec_env) : _impl(std::make_unique<AgentServer::Impl>(exec_env)) {}

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

ThreadPool* AgentServer::get_thread_pool(int type) const {
    return _impl->get_thread_pool(type);
}

void AgentServer::init_or_die() {
    return _impl->init_or_die();
}

} // namespace starrocks
