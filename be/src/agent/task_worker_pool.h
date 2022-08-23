// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/task_worker_pool.h

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

#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "agent/agent_common.h"
#include "agent/status.h"
#include "agent/utils.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "util/threadpool.h"

namespace starrocks {

class ExecEnv;

class TaskWorkerPoolBase {
public:
    typedef void* (*CALLBACK_FUNCTION)(void*);

    TaskWorkerPoolBase(ExecEnv* env, int worker_num);
    ~TaskWorkerPoolBase();

    // start the task worker callback thread
    void start();

    // stop the task worker callback thread
    void stop();

    static void remove_task_info(TTaskType::type task_type, int64_t signature);

    TaskWorkerPoolBase(const TaskWorkerPoolBase&) = delete;
    const TaskWorkerPoolBase& operator=(const TaskWorkerPoolBase&) = delete;

    static AgentStatus get_tablet_info(TTabletId tablet_id, TSchemaHash schema_hash, int64_t signature,
                                       TTabletInfo* tablet_info);

protected:
    bool _register_task_info(TTaskType::type task_type, int64_t signature);
    void _spawn_callback_worker_thread(CALLBACK_FUNCTION callback_func);

    AgentStatus _move_dir(TTabletId tablet_id, TSchemaHash schema_hash, const std::string& src, int64_t job_id,
                          bool overwrite, std::vector<std::string>* error_msgs);

    TBackend _backend;
    ExecEnv* _env;

    // Protect task queue
    mutable std::mutex _worker_thread_lock;
    std::condition_variable* _worker_thread_condition_variable;

    uint32_t _worker_count = 0;
    TaskWorkerType _task_worker_type;
    CALLBACK_FUNCTION _callback_function = nullptr;

    static std::atomic<int64_t> _s_report_version;

    static std::mutex _s_task_signatures_locks[TTaskType::type::NUM_TASK_TYPE];
    static std::set<int64_t> _s_task_signatures[TTaskType::type::NUM_TASK_TYPE];

    std::atomic<bool> _stopped{false};
    std::vector<std::thread> _worker_threads;
};


template <TaskWorkerType TaskType>
class TaskWorkerPool : public TaskWorkerPoolBase {
    friend class TaskWorkerPoolThreadCallback;

public:
    using AgentTaskRequestPtr = std::shared_ptr<AgentTaskRequest<TaskType>>;

    TaskWorkerPool(ExecEnv* env, int worker_num);

    // Submit task to task pool
    //
    // Input parameters:
    // * task: the task need callback thread to do
    void submit_task(const TAgentTaskRequest& task);
    void submit_tasks(std::vector<TAgentTaskRequest>* task);

    size_t num_queued_tasks() const;

private:
    size_t _push_task(AgentTaskRequestPtr task);
    AgentTaskRequestPtr _pop_task();
    AgentTaskRequestPtr _pop_task(TPriority::type pri);
    static AgentTaskRequestPtr _convert_task(const TAgentTaskRequest& task);

    std::deque<AgentTaskRequestPtr> _tasks;
};

using CreateTableWorkerPool = TaskWorkerPool<TaskWorkerType::CREATE_TABLE>;
using DropTableWorkerPool = TaskWorkerPool<TaskWorkerType::DROP_TABLE>;
using PushWorkerPool = TaskWorkerPool<TaskWorkerType::PUSH>;
using PublishVersionWorkerPool = TaskWorkerPool<TaskWorkerType::PUBLISH_VERSION>;
using ClearTransactionWorkerPool = TaskWorkerPool<TaskWorkerType::CLEAR_TRANSACTION_TASK>;
using DeleteWorkerPool = TaskWorkerPool<TaskWorkerType::DELETE>;
using AlterTableWorkerPool = TaskWorkerPool<TaskWorkerType::ALTER_TABLE>;
using CloneWorkerPool = TaskWorkerPool<TaskWorkerType::CLONE>;
using StorageMediumMigrateWorkerPool = TaskWorkerPool<TaskWorkerType::STORAGE_MEDIUM_MIGRATE>;
using CheckConsistencyWorkerPool = TaskWorkerPool<TaskWorkerType::CHECK_CONSISTENCY>;
using ReportTaskWorkerPool = TaskWorkerPool<TaskWorkerType::REPORT_TASK>;
using ReportDiskStateWorkerPool = TaskWorkerPool<TaskWorkerType::REPORT_DISK_STATE>;
using ReportOlapTableWorkerPool = TaskWorkerPool<TaskWorkerType::REPORT_OLAP_TABLE>;
using ReportWorkgroupWorkerPool = TaskWorkerPool<TaskWorkerType::REPORT_WORKGROUP>;
using UploadWorkerPool = TaskWorkerPool<TaskWorkerType::UPLOAD>;
using DownloadWorkerPool = TaskWorkerPool<TaskWorkerType::DOWNLOAD>;
using MakeSnapshotWorkerPool = TaskWorkerPool<TaskWorkerType::MAKE_SNAPSHOT>;
using ReleaseSnapshotWorkerPool = TaskWorkerPool<TaskWorkerType::RELEASE_SNAPSHOT>;
using MoveWorkerPool = TaskWorkerPool<TaskWorkerType::MOVE>;
using UpdateTabletMetaInfoWorkerPool = TaskWorkerPool<TaskWorkerType::UPDATE_TABLET_META_INFO>;

class TaskWorkerPoolThreadCallback {
public:
    static void* _create_tablet_worker_thread_callback(void* arg_this);
    static void* _drop_tablet_worker_thread_callback(void* arg_this);
    static void* _push_worker_thread_callback(void* arg_this);
    static void* _delete_worker_thread_callback(void* arg_this);
    static void* _publish_version_worker_thread_callback(void* arg_this);
    static void* _clear_transaction_task_worker_thread_callback(void* arg_this);
    static void* _alter_tablet_worker_thread_callback(void* arg_this);
    static void* _clone_worker_thread_callback(void* arg_this);
    static void* _storage_medium_migrate_worker_thread_callback(void* arg_this);
    static void* _check_consistency_worker_thread_callback(void* arg_this);
    static void* _report_task_worker_thread_callback(void* arg_this);
    static void* _report_disk_state_worker_thread_callback(void* arg_this);
    static void* _report_tablet_worker_thread_callback(void* arg_this);
    static void* _report_workgroup_thread_callback(void* arg_this);
    static void* _upload_worker_thread_callback(void* arg_this);
    static void* _download_worker_thread_callback(void* arg_this);
    static void* _make_snapshot_thread_callback(void* arg_this);
    static void* _release_snapshot_thread_callback(void* arg_this);
    static void* _move_dir_thread_callback(void* arg_this);
    static void* _update_tablet_meta_worker_thread_callback(void* arg_this);

    static void _alter_tablet(const TAlterTabletReqV2& alter_tablet_request, int64_t signature,
                              TTaskType::type task_type, TFinishTaskRequest* finish_task_request);
};


} // namespace starrocks
