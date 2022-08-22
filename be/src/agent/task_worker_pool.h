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

#include "agent/status.h"
#include "agent/utils.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "util/threadpool.h"

namespace starrocks {

class ExecEnv;

class TaskWorkerPool {
public:
    enum TaskWorkerType {
        CREATE_TABLE,
        DROP_TABLE,
        PUSH,
        REALTIME_PUSH,
        PUBLISH_VERSION,
        CLEAR_ALTER_TASK, // Deprecated
        CLEAR_TRANSACTION_TASK,
        DELETE,
        ALTER_TABLE,
        QUERY_SPLIT_KEY, // Deprecated
        CLONE,
        STORAGE_MEDIUM_MIGRATE,
        CHECK_CONSISTENCY,
        REPORT_TASK,
        REPORT_DISK_STATE,
        REPORT_OLAP_TABLE,
        REPORT_WORKGROUP,
        UPLOAD,
        DOWNLOAD,
        MAKE_SNAPSHOT,
        RELEASE_SNAPSHOT,
        MOVE,
        RECOVER_TABLET, // Deprecated
        UPDATE_TABLET_META_INFO
    };

    typedef void* (*CALLBACK_FUNCTION)(void*);

    TaskWorkerPool(TaskWorkerType task_worker_type, ExecEnv* env, int worker_num);
    ~TaskWorkerPool();

    // start the task worker callback thread
    void start();

    // stop the task worker callback thread
    void stop();

    // Submit task to task pool
    //
    // Input parameters:
    // * task: the task need callback thread to do
    void submit_task(const TAgentTaskRequest& task);
    void submit_tasks(std::vector<TAgentTaskRequest>* task);

    AgentStatus get_tablet_info(TTabletId tablet_id, TSchemaHash schema_hash, int64_t signature,
                                TTabletInfo* tablet_info);
    void remove_task_info(TTaskType::type task_type, int64_t signature);

    size_t num_queued_tasks() const;

    TaskWorkerPool(const TaskWorkerPool&) = delete;
    const TaskWorkerPool& operator=(const TaskWorkerPool&) = delete;

private:
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

    bool _register_task_info(TTaskType::type task_type, int64_t signature);
    void _spawn_callback_worker_thread(CALLBACK_FUNCTION callback_func);

    using TAgentTaskRequestPtr = std::shared_ptr<TAgentTaskRequest>;

    size_t _push_task(TAgentTaskRequestPtr task);
    TAgentTaskRequestPtr _pop_task();
    TAgentTaskRequestPtr _pop_task(TPriority::type pri);

    void _alter_tablet(TaskWorkerPool* worker_pool_this, const TAgentTaskRequest& alter_tablet_request,
                       int64_t signature, TTaskType::type task_type, TFinishTaskRequest* finish_task_request);

    AgentStatus _move_dir(TTabletId tablet_id, TSchemaHash schema_hash, const std::string& src, int64_t job_id,
                          bool overwrite, std::vector<std::string>* error_msgs);

    TBackend _backend;
    ExecEnv* _env;

    // Protect task queue
    mutable std::mutex _worker_thread_lock;
    std::condition_variable* _worker_thread_condition_variable;
    std::deque<TAgentTaskRequestPtr> _tasks;

    uint32_t _worker_count = 0;
    TaskWorkerType _task_worker_type;
    CALLBACK_FUNCTION _callback_function;

    static std::atomic<int64_t> _s_report_version;

    static std::mutex _s_task_signatures_locks[TTaskType::type::NUM_TASK_TYPE];
    static std::set<int64_t> _s_task_signatures[TTaskType::type::NUM_TASK_TYPE];

    std::atomic<bool> _stopped{false};
    std::vector<std::thread> _worker_threads;
}; // class TaskWorkerPool

template <TaskWorkerPool::TaskWorkerType type>
struct TaskWorkerTypeTraits {};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::CREATE_TABLE> {
    using TReq = TCreateTabletReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::DROP_TABLE> {
    using TReq = TDropTabletReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::PUSH> {
    using TReq = TPushReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::REALTIME_PUSH> {
    using TReq = TPushReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::PUBLISH_VERSION> {
    using TReq = TPublishVersionRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::CLEAR_TRANSACTION_TASK> {
    using TReq = TClearTransactionTaskRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::DELETE> {
    using TReq = TPushReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::ALTER_TABLE> {
    using TReq = TAlterTabletReqV2;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::CLONE> {
    using TReq = TCloneReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::STORAGE_MEDIUM_MIGRATE> {
    using TReq = TStorageMediumMigrateReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::CHECK_CONSISTENCY> {
    using TReq = TCheckConsistencyReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::UPLOAD> {
    using TReq = TUploadReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::DOWNLOAD> {
    using TReq = TDownloadReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::MAKE_SNAPSHOT> {
    using TReq = TSnapshotRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::RELEASE_SNAPSHOT> {
    using TReq = TReleaseSnapshotRequest;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::MOVE> {
    using TReq = TMoveDirReq;
};

template <>
struct TaskWorkerTypeTraits<TaskWorkerPool::UPDATE_TABLET_META_INFO> {
    using TReq = TUpdateTabletMetaInfoReq;
};

} // namespace starrocks
