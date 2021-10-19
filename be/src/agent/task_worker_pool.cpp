// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/task_worker_pool.cpp

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

#include "agent/task_worker_pool.h"

#include <pthread.h>
#include <sys/stat.h>

#include <boost/lexical_cast.hpp>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <ctime>
#include <mutex>
#include <sstream>
#include <string>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/task/engine_alter_tablet_task.h"
#include "storage/task/engine_batch_load_task.h"
#include "storage/task/engine_checksum_task.h"
#include "storage/task/engine_clone_task.h"
#include "storage/task/engine_publish_version_task.h"
#include "storage/task/engine_storage_migration_task.h"
#include "storage/utils.h"
#include "util/file_utils.h"
#include "util/monotime.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"

namespace starrocks {

const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t PUBLISH_VERSION_MAX_RETRY = 3;

std::atomic_ulong TaskWorkerPool::_s_report_version(time(nullptr) * 10000);
std::mutex TaskWorkerPool::_s_task_signatures_lock;
std::map<TTaskType::type, std::set<int64_t>> TaskWorkerPool::_s_task_signatures;
FrontendServiceClientCache TaskWorkerPool::_master_service_client_cache;

TaskWorkerPool::TaskWorkerPool(const TaskWorkerType task_worker_type, ExecEnv* env, const TMasterInfo& master_info,
                               int worker_count)
        : _master_info(master_info),
          _agent_utils(new AgentUtils()),
          _master_client(new MasterServerClient(_master_info, &_master_service_client_cache)),
          _env(env),
          _worker_thread_condition_variable(new std::condition_variable()),
          _worker_count(worker_count),
          _task_worker_type(task_worker_type) {
    _backend.__set_host(BackendOptions::get_localhost());
    _backend.__set_be_port(config::be_port);
    _backend.__set_http_port(config::webserver_port);
}

TaskWorkerPool::~TaskWorkerPool() {
    // We should notify all waiting threads to destroy this condition variable
    // If we don't notify, pthread_cond_destroy will hang in some GLIBC version.
    // See https://bugzilla.redhat.com/show_bug.cgi?id=1647381
    // "In glibc 2.25 we implemented a new version of POSIX condition variables to provide stronger
    // ordering guarantees. The change in the implementation caused the undefined behaviour
    // to change."
    std::lock_guard l(_worker_thread_lock);
    _worker_thread_condition_variable->notify_all();
    delete _worker_thread_condition_variable;
}

void TaskWorkerPool::start() {
    // Init task pool and task workers
    switch (_task_worker_type) {
    case TaskWorkerType::CREATE_TABLE:
        _callback_function = _create_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::DROP_TABLE:
        _callback_function = _drop_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::PUSH:
    case TaskWorkerType::REALTIME_PUSH:
        _callback_function = _push_worker_thread_callback;
        break;
    case TaskWorkerType::PUBLISH_VERSION:
        _callback_function = _publish_version_worker_thread_callback;
        break;
    case TaskWorkerType::CLEAR_TRANSACTION_TASK:
        _callback_function = _clear_transaction_task_worker_thread_callback;
        break;
    case TaskWorkerType::DELETE:
        _worker_count = config::delete_worker_count;
        _callback_function = _push_worker_thread_callback;
        break;
    case TaskWorkerType::ALTER_TABLE:
        _callback_function = _alter_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::CLONE:
        _callback_function = _clone_worker_thread_callback;
        break;
    case TaskWorkerType::STORAGE_MEDIUM_MIGRATE:
        _callback_function = _storage_medium_migrate_worker_thread_callback;
        break;
    case TaskWorkerType::CHECK_CONSISTENCY:
        _callback_function = _check_consistency_worker_thread_callback;
        break;
    case TaskWorkerType::REPORT_TASK:
        _callback_function = _report_task_worker_thread_callback;
        break;
    case TaskWorkerType::REPORT_DISK_STATE:
        _callback_function = _report_disk_state_worker_thread_callback;
        break;
    case TaskWorkerType::REPORT_OLAP_TABLE:
        _callback_function = _report_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::UPLOAD:
        _callback_function = _upload_worker_thread_callback;
        break;
    case TaskWorkerType::DOWNLOAD:
        _callback_function = _download_worker_thread_callback;
        break;
    case TaskWorkerType::MAKE_SNAPSHOT:
        _callback_function = _make_snapshot_thread_callback;
        break;
    case TaskWorkerType::RELEASE_SNAPSHOT:
        _callback_function = _release_snapshot_thread_callback;
        break;
    case TaskWorkerType::MOVE:
        _callback_function = _move_dir_thread_callback;
        break;
    case TaskWorkerType::UPDATE_TABLET_META_INFO:
        _callback_function = _update_tablet_meta_worker_thread_callback;
        break;
    default:
        // pass
        break;
    }

#ifndef BE_TEST
    for (uint32_t i = 0; i < _worker_count; i++) {
        _spawn_callback_worker_thread(_callback_function);
    }
#endif
}

void TaskWorkerPool::submit_task(const TAgentTaskRequest& task) {
    const TTaskType::type task_type = task.task_type;
    int64_t signature = task.signature;

    std::string type_str;
    EnumToString(TTaskType, task_type, type_str);
    LOG(INFO) << "submitting task. type=" << type_str << ", signature=" << signature;

    if (_register_task_info(task_type, signature)) {
        // Set the receiving time of task so that we can determine whether it is timed out later
        (const_cast<TAgentTaskRequest&>(task)).__set_recv_time(time(nullptr));
        size_t task_count_in_queue = 0;
        {
            std::unique_lock l(_worker_thread_lock);
            _tasks.push_back(task);
            task_count_in_queue = _tasks.size();
            _worker_thread_condition_variable->notify_one();
        }
        LOG(INFO) << "success to submit task. type=" << type_str << ", signature=" << signature
                  << ", task_count_in_queue=" << task_count_in_queue;
    } else {
        LOG(INFO) << "fail to register task. type=" << type_str << ", signature=" << signature;
    }
}

bool TaskWorkerPool::_register_task_info(const TTaskType::type task_type, int64_t signature) {
    std::lock_guard task_signatures_lock(_s_task_signatures_lock);
    std::set<int64_t>& signature_set = _s_task_signatures[task_type];
    return signature_set.insert(signature).second;
}

void TaskWorkerPool::_remove_task_info(const TTaskType::type task_type, int64_t signature) {
    std::lock_guard task_signatures_lock(_s_task_signatures_lock);
    _s_task_signatures[task_type].erase(signature);
}

void TaskWorkerPool::_spawn_callback_worker_thread(CALLBACK_FUNCTION callback_func) {
    pthread_t thread;
    sigset_t mask;
    sigset_t omask;
    int err = 0;

    // TODO: why need to catch these signals, should leave a comment
    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_SETMASK, &mask, &omask);

    while (true) {
        err = pthread_create(&thread, nullptr, callback_func, this);
        if (err != 0) {
            LOG(WARNING) << "failed to spawn a thread. error: " << err;
#ifndef BE_TEST
            sleep(config::sleep_one_second);
#endif
        } else {
            pthread_detach(thread);
            break;
        }
    }
}

void TaskWorkerPool::_finish_task(const TFinishTaskRequest& finish_task_request) {
    // Return result to FE
    TMasterResult result;
    uint32_t try_time = 0;

    while (try_time < TASK_FINISH_MAX_RETRY) {
        StarRocksMetrics::instance()->finish_task_requests_total.increment(1);
        AgentStatus client_status = _master_client->finish_task(finish_task_request, &result);

        if (client_status == STARROCKS_SUCCESS) {
            break;
        } else {
            try_time += 1;
            StarRocksMetrics::instance()->finish_task_requests_failed.increment(1);
            LOG(WARNING) << "finish task failed " << try_time << "/" << TASK_FINISH_MAX_RETRY
                         << ". status_code=" << result.status.status_code;
        }
#ifndef BE_TEST
        sleep(config::sleep_one_second);
#endif
    }
}

uint32_t TaskWorkerPool::_get_next_task_index(int32_t thread_count, std::deque<TAgentTaskRequest>& tasks,
                                              TPriority::type priority) {
    int32_t index = -1;
    deque<TAgentTaskRequest>::size_type task_count = tasks.size();
    for (uint32_t i = 0; i < task_count; ++i) {
        TAgentTaskRequest task = tasks[i];
        if (priority == TPriority::HIGH) {
            if (task.__isset.priority && task.priority == TPriority::HIGH) {
                index = i;
                break;
            }
        }
    }

    if (index == -1) {
        if (priority == TPriority::HIGH) {
            return index;
        }

        index = 0;
    }

    return index;
}

void* TaskWorkerPool::_create_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TCreateTabletReq create_tablet_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            create_tablet_req = agent_task_req.create_tablet_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        std::vector<TTabletInfo> finish_tablet_infos;
        Status create_status = worker_pool_this->_env->storage_engine()->create_tablet(create_tablet_req);
        if (!create_status.ok()) {
            LOG(WARNING) << "create table failed. status: " << create_status.to_string()
                         << ", signature: " << agent_task_req.signature;
            // TODO liutao09 distinguish the OLAPStatus
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            ++_s_report_version;
            // get path hash of the created tablet
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    create_tablet_req.tablet_id, create_tablet_req.tablet_schema.schema_hash);
            DCHECK(tablet != nullptr);
            TTabletInfo tablet_info;
            tablet_info.tablet_id = tablet->tablet_id();
            tablet_info.schema_hash = tablet->schema_hash();
            tablet_info.version = create_tablet_req.version;
            tablet_info.version_hash = create_tablet_req.version_hash;
            tablet_info.row_count = 0;
            tablet_info.data_size = 0;
            tablet_info.__set_path_hash(tablet->data_dir()->path_hash());
            finish_tablet_infos.push_back(tablet_info);
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_report_version(_s_report_version);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_drop_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TDropTabletReq drop_tablet_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            drop_tablet_req = agent_task_req.drop_tablet_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;
        TabletSharedPtr dropped_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                drop_tablet_req.tablet_id, drop_tablet_req.schema_hash);
        if (dropped_tablet != nullptr) {
            Status drop_status = StorageEngine::instance()->tablet_manager()->drop_tablet(drop_tablet_req.tablet_id,
                                                                                          drop_tablet_req.schema_hash);
            if (!drop_status.ok()) {
                LOG(WARNING) << "drop table failed! signature: " << agent_task_req.signature;
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
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_alter_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            worker_pool_this->_tasks.pop_front();
        }
        int64_t signatrue = agent_task_req.signature;
        LOG(INFO) << "get alter table task, signature: " << agent_task_req.signature;
        bool is_task_timeout = false;
        if (agent_task_req.__isset.recv_time) {
            int64_t time_elapsed = time(nullptr) - agent_task_req.recv_time;
            if (time_elapsed > config::report_task_interval_seconds * 20) {
                LOG(INFO) << "task elapsed " << time_elapsed << " seconds since it is inserted to queue, it is timeout";
                is_task_timeout = true;
            }
        }
        if (!is_task_timeout) {
            TFinishTaskRequest finish_task_request;
            TTaskType::type task_type = agent_task_req.task_type;
            switch (task_type) {
            case TTaskType::ALTER:
                worker_pool_this->_alter_tablet(worker_pool_this, agent_task_req, signatrue, task_type,
                                                &finish_task_request);
                break;
            default:
                // pass
                break;
            }
            worker_pool_this->_finish_task(finish_task_request);
        }
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void TaskWorkerPool::_alter_tablet(TaskWorkerPool* worker_pool_this, const TAgentTaskRequest& agent_task_req,
                                   int64_t signature, const TTaskType::type task_type,
                                   TFinishTaskRequest* finish_task_request) {
    AgentStatus status = STARROCKS_SUCCESS;
    TStatus task_status;
    std::vector<std::string> error_msgs;

    std::string process_name;
    switch (task_type) {
    case TTaskType::ALTER:
        process_name = "alter";
        break;
    default:
        std::string task_name;
        EnumToString(TTaskType, task_type, task_name);
        LOG(WARNING) << "schema change type invalid. type: " << task_name << ", signature: " << signature;
        status = STARROCKS_TASK_REQUEST_ERROR;
        break;
    }

    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    TTabletId new_tablet_id;
    TSchemaHash new_schema_hash = 0;
    if (status == STARROCKS_SUCCESS) {
        new_tablet_id = agent_task_req.alter_tablet_req_v2.new_tablet_id;
        new_schema_hash = agent_task_req.alter_tablet_req_v2.new_schema_hash;
        EngineAlterTabletTask engine_task(agent_task_req.alter_tablet_req_v2, signature, task_type, &error_msgs,
                                          process_name);
        OLAPStatus sc_status = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        if (sc_status != OLAP_SUCCESS) {
            status = STARROCKS_ERROR;
        } else {
            status = STARROCKS_SUCCESS;
        }
    }

    if (status == STARROCKS_SUCCESS) {
        ++_s_report_version;
        LOG(INFO) << process_name << " finished. signature: " << signature;
    }

    // Return result to fe
    finish_task_request->__set_backend(_backend);
    finish_task_request->__set_report_version(_s_report_version);
    finish_task_request->__set_task_type(task_type);
    finish_task_request->__set_signature(signature);

    std::vector<TTabletInfo> finish_tablet_infos;
    if (status == STARROCKS_SUCCESS) {
        TTabletInfo tablet_info;
        status = _get_tablet_info(new_tablet_id, new_schema_hash, signature, &tablet_info);

        if (status != STARROCKS_SUCCESS) {
            LOG(WARNING) << process_name << " success, but get new tablet info failed."
                         << "tablet_id: " << new_tablet_id << ", schema_hash: " << new_schema_hash
                         << ", signature: " << signature;
        } else {
            finish_tablet_infos.push_back(tablet_info);
        }
    }

    if (status == STARROCKS_SUCCESS) {
        finish_task_request->__set_finish_tablet_infos(finish_tablet_infos);
        LOG(INFO) << process_name << " success. signature: " << signature;
        error_msgs.push_back(process_name + " success");
        task_status.__set_status_code(TStatusCode::OK);
    } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
        LOG(WARNING) << "alter table request task type invalid. "
                     << "signature:" << signature;
        error_msgs.emplace_back("alter table request new tablet id or schema count invalid.");
        task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
    } else {
        LOG(WARNING) << process_name << " failed. signature: " << signature;
        error_msgs.push_back(process_name + " failed");
        error_msgs.push_back("status: " + _agent_utils->print_agent_status(status));
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
    }

    task_status.__set_error_msgs(error_msgs);
    finish_task_request->__set_task_status(task_status);
}

void* TaskWorkerPool::_push_worker_thread_callback(void* arg_this) {
    static uint32_t s_worker_count = 0;

    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TPriority::type priority = TPriority::NORMAL;
    // https://starrocks.atlassian.net/browse/DSDB-991
    if (worker_pool_this->_task_worker_type != DELETE) {
        int32_t push_worker_count_high_priority = config::push_worker_count_high_priority;
        std::lock_guard worker_thread_lock(worker_pool_this->_worker_thread_lock);
        if (s_worker_count < push_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

#ifndef BE_TEST
    while (true) {
#endif
        AgentStatus status = STARROCKS_SUCCESS;
        TAgentTaskRequest agent_task_req;
        TPushReq push_req;
        int32_t index = 0;
        do {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            index = worker_pool_this->_get_next_task_index(
                    config::push_worker_count_normal_priority + config::push_worker_count_high_priority,
                    worker_pool_this->_tasks, priority);

            if (index < 0) {
                // there is no high priority task. notify other thread to handle normal task
                worker_pool_this->_worker_thread_condition_variable->notify_one();
                break;
            }

            agent_task_req = worker_pool_this->_tasks[index];
            push_req = agent_task_req.push_req;
            worker_pool_this->_tasks.erase(worker_pool_this->_tasks.begin() + index);
        } while (false);

#ifndef BE_TEST
        if (index < 0) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }
#endif

        LOG(INFO) << "get push task. signature: " << agent_task_req.signature << " priority: " << priority
                  << " push_type: " << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;

        EngineBatchLoadTask engine_task(push_req, &tablet_infos, agent_task_req.signature, &status);
        worker_pool_this->_env->storage_engine()->execute_task(&engine_task);

#ifndef BE_TEST
        if (status == STARROCKS_PUSH_HAD_LOADED) {
            // remove the task and not return to fe
            worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
            continue;
        }
#endif
        // Return result to fe
        std::vector<string> error_msgs;
        TStatus task_status;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        if (push_req.push_type == TPushType::DELETE) {
            finish_task_request.__set_request_version(push_req.version);
            finish_task_request.__set_request_version_hash(push_req.version_hash);
        }

        if (status == STARROCKS_SUCCESS) {
            VLOG(3) << "push ok. signature: " << agent_task_req.signature << ", push_type: " << push_req.push_type;
            error_msgs.emplace_back("push success");

            ++_s_report_version;

            task_status.__set_status_code(TStatusCode::OK);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
            LOG(WARNING) << "push request push_type invalid. type: " << push_req.push_type
                         << ", signature: " << agent_task_req.signature;
            error_msgs.emplace_back("push request push_type invalid.");
            task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
        } else {
            LOG(WARNING) << "push failed, error_code: " << status << ", signature: " << agent_task_req.signature;
            error_msgs.emplace_back("push failed");
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        }
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_report_version(_s_report_version);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif

    return (void*)nullptr;
}

void* TaskWorkerPool::_publish_version_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TPublishVersionRequest publish_version_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            publish_version_req = agent_task_req.publish_version_req;
            worker_pool_this->_tasks.pop_front();
        }

        StarRocksMetrics::instance()->publish_task_request_total.increment(1);
        LOG(INFO) << "get publish version task, signature:" << agent_task_req.signature;

        Status st;
        std::vector<TTabletId> error_tablet_ids;
        uint32_t retry_time = 0;
        OLAPStatus res = OLAP_SUCCESS;
        while (retry_time < PUBLISH_VERSION_MAX_RETRY) {
            error_tablet_ids.clear();
            EnginePublishVersionTask engine_task(publish_version_req, &error_tablet_ids);
            res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
            if (res == OLAP_SUCCESS) {
                break;
            } else {
                LOG(WARNING) << "publish version error, retry. [transaction_id=" << publish_version_req.transaction_id
                             << ", error_tablets_size=" << error_tablet_ids.size() << "]";
                ++retry_time;
                SleepFor(MonoDelta::FromSeconds(1));
            }
        }

        TFinishTaskRequest finish_task_request;
        if (res != OLAP_SUCCESS) {
            StarRocksMetrics::instance()->publish_task_failed_total.increment(1);
            // if publish failed, return failed, FE will ignore this error and
            // check error tablet ids and FE will also republish this task
            LOG(WARNING) << "publish version failed. signature:" << agent_task_req.signature << ", error_code=" << res;
            st = Status::RuntimeError(strings::Substitute("publish version failed. error=$0", res));
            finish_task_request.__set_error_tablet_ids(error_tablet_ids);
        } else {
            LOG(INFO) << "publish_version success. signature:" << agent_task_req.signature;
        }

        st.to_thrift(&finish_task_request.task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_report_version(_s_report_version);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_clear_transaction_task_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TClearTransactionTaskRequest clear_transaction_task_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            clear_transaction_task_req = agent_task_req.clear_transaction_task_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "get clear transaction task task, signature:" << agent_task_req.signature
                  << ", transaction_id: " << clear_transaction_task_req.transaction_id
                  << ", partition id size: " << clear_transaction_task_req.partition_id.size();

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        if (clear_transaction_task_req.transaction_id > 0) {
            // transaction_id should be greater than zero.
            // If it is not greater than zero, no need to execute
            // the following clear_transaction_task() function.
            if (!clear_transaction_task_req.partition_id.empty()) {
                worker_pool_this->_env->storage_engine()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id, clear_transaction_task_req.partition_id);
            } else {
                worker_pool_this->_env->storage_engine()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id);
            }
            LOG(INFO) << "finish to clear transaction task. signature:" << agent_task_req.signature
                      << ", transaction_id: " << clear_transaction_task_req.transaction_id;
        } else {
            LOG(WARNING) << "invalid transaction id: " << clear_transaction_task_req.transaction_id
                         << ", signature: " << agent_task_req.signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_update_tablet_meta_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
    while (true) {
        TAgentTaskRequest agent_task_req;
        TUpdateTabletMetaInfoReq update_tablet_meta_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            update_tablet_meta_req = agent_task_req.update_tablet_meta_info_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "get update tablet meta task, signature:" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        for (const auto& tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_meta_info.tablet_id, tablet_meta_info.schema_hash);
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
                    tablet->tablet_meta()->mutable_tablet_schema()->set_is_in_memory(tablet_meta_info.is_in_memory);
                    break;
                }
            }
            tablet->save_meta();
        }

        LOG(INFO) << "finish update tablet meta task. signature:" << agent_task_req.signature;

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
    return (void*)nullptr;
}

void* TaskWorkerPool::_clone_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        AgentStatus status = STARROCKS_SUCCESS;
        TAgentTaskRequest agent_task_req;
        TCloneReq clone_req;

        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            clone_req = agent_task_req.clone_req;
            worker_pool_this->_tasks.pop_front();
        }

        StarRocksMetrics::instance()->clone_requests_total.increment(1);
        LOG(INFO) << "get clone task. signature:" << agent_task_req.signature;

        // Return result to fe
        TStatus task_status;
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

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
                OLAPStatus res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
                if (res != OLAP_SUCCESS) {
                    status_code = TStatusCode::RUNTIME_ERROR;
                    LOG(WARNING) << "storage migrate failed. status:" << res
                                 << ", signature:" << agent_task_req.signature;
                    error_msgs.emplace_back("storage migrate failed.");
                } else {
                    LOG(INFO) << "storage migrate success. status:" << res
                              << ", signature:" << agent_task_req.signature;

                    TTabletInfo tablet_info;
                    AgentStatus status = worker_pool_this->_get_tablet_info(clone_req.tablet_id, clone_req.schema_hash,
                                                                            agent_task_req.signature, &tablet_info);
                    if (status != STARROCKS_SUCCESS) {
                        LOG(WARNING) << "storage migrate success, but get tablet info failed"
                                     << ". status:" << status << ", signature:" << agent_task_req.signature;
                    } else {
                        tablet_infos.push_back(tablet_info);
                    }
                    finish_task_request.__set_finish_tablet_infos(tablet_infos);
                }
            }
        } else {
            EngineCloneTask engine_task(ExecEnv::GetInstance()->tablet_meta_mem_tracker(), clone_req,
                                        worker_pool_this->_master_info, agent_task_req.signature, &error_msgs,
                                        &tablet_infos, &status);
            OLAPStatus res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
            if (res != OLAP_SUCCESS) {
                status_code = TStatusCode::RUNTIME_ERROR;
                LOG(WARNING) << "clone failed. status:" << res << ", signature:" << agent_task_req.signature;
                error_msgs.emplace_back("clone failed.");
            } else {
                if (status != STARROCKS_SUCCESS && status != STARROCKS_CREATE_TABLE_EXIST) {
                    StarRocksMetrics::instance()->clone_requests_failed.increment(1);
                    status_code = TStatusCode::RUNTIME_ERROR;
                    LOG(WARNING) << "clone failed. signature: " << agent_task_req.signature;
                    error_msgs.emplace_back("clone failed.");
                } else {
                    LOG(INFO) << "clone success, set tablet infos. status:" << status
                              << ", signature:" << agent_task_req.signature;
                    finish_task_request.__set_finish_tablet_infos(tablet_infos);
                }
            }
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif

    return (void*)nullptr;
}

void* TaskWorkerPool::_storage_medium_migrate_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TStorageMediumMigrateReq storage_medium_migrate_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            storage_medium_migrate_req = agent_task_req.storage_medium_migrate_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        do {
            TTabletId tablet_id = storage_medium_migrate_req.tablet_id;
            TSchemaHash schema_hash = storage_medium_migrate_req.schema_hash;
            TStorageMedium::type storage_medium = storage_medium_migrate_req.storage_medium;

            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
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
            OLAPStatus res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "storage media migrate failed. status: " << res
                             << ", signature: " << agent_task_req.signature;
                status_code = TStatusCode::RUNTIME_ERROR;
            } else {
                // status code is ok
                LOG(INFO) << "storage media migrate success. "
                          << "signature:" << agent_task_req.signature;

                std::vector<TTabletInfo> tablet_infos;
                TTabletInfo tablet_info;
                AgentStatus status = worker_pool_this->_get_tablet_info(tablet_id, schema_hash,
                                                                        agent_task_req.signature, &tablet_info);
                if (status != STARROCKS_SUCCESS) {
                    LOG(WARNING) << "storage migrate success, but get tablet info failed"
                                 << ". status:" << status << ", signature:" << agent_task_req.signature;
                } else {
                    tablet_infos.push_back(tablet_info);
                }
                finish_task_request.__set_finish_tablet_infos(tablet_infos);
            }
        } while (false);

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_check_consistency_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TCheckConsistencyReq check_consistency_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            check_consistency_req = agent_task_req.check_consistency_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        uint32_t checksum = 0;
        EngineChecksumTask engine_task(check_consistency_req.tablet_id, check_consistency_req.schema_hash,
                                       check_consistency_req.version, check_consistency_req.version_hash, &checksum);
        OLAPStatus res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "check consistency failed. status: " << res << ", signature: " << agent_task_req.signature;
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            LOG(INFO) << "check consistency success. status:" << res << ", signature:" << agent_task_req.signature
                      << ", checksum:" << checksum;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_tablet_checksum(static_cast<int64_t>(checksum));
        finish_task_request.__set_request_version(check_consistency_req.version);
        finish_task_request.__set_request_version_hash(check_consistency_req.version_hash);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_report_task_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__set_backend(worker_pool_this->_backend);

#ifndef BE_TEST
    while (true) {
#endif
        {
            std::lock_guard task_signatures_lock(_s_task_signatures_lock);
            request.__set_tasks(_s_task_signatures);
        }

        StarRocksMetrics::instance()->report_task_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = worker_pool_this->_master_client->report(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_task_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report task to " << worker_pool_this->_master_info.network_address.hostname << ":"
                         << worker_pool_this->_master_info.network_address.port << ", err=" << status;
        }

#ifndef BE_TEST
        sleep(config::report_task_interval_seconds);
    }
#endif

    return (void*)nullptr;
}

void* TaskWorkerPool::_report_disk_state_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__set_backend(worker_pool_this->_backend);

#ifndef BE_TEST
    while (true) {
        if (worker_pool_this->_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            LOG(INFO) << "Waiting to receive first heartbeat from frontend";
            sleep(config::sleep_one_second);
            continue;
        }
#endif
        std::vector<DataDirInfo> data_dir_infos;
        worker_pool_this->_env->storage_engine()->get_all_data_dir_info(&data_dir_infos, true /* update */);

        std::map<std::string, TDisk> disks;
        for (auto& root_path_info : data_dir_infos) {
            TDisk disk;
            disk.__set_root_path(root_path_info.path);
            disk.__set_path_hash(root_path_info.path_hash);
            disk.__set_storage_medium(root_path_info.storage_medium);
            disk.__set_disk_total_capacity(static_cast<double>(root_path_info.disk_capacity));
            disk.__set_data_used_capacity(static_cast<double>(root_path_info.data_used_capacity));
            disk.__set_disk_available_capacity(static_cast<double>(root_path_info.available));
            disk.__set_used(root_path_info.is_used);
            disks[root_path_info.path] = disk;

            StarRocksMetrics::instance()->disks_total_capacity.set_metric(root_path_info.path,
                                                                          root_path_info.disk_capacity);
            StarRocksMetrics::instance()->disks_avail_capacity.set_metric(root_path_info.path,
                                                                          root_path_info.available);
            StarRocksMetrics::instance()->disks_data_used_capacity.set_metric(root_path_info.path,
                                                                              root_path_info.data_used_capacity);
            StarRocksMetrics::instance()->disks_state.set_metric(root_path_info.path, root_path_info.is_used ? 1L : 0L);
        }
        request.__set_disks(disks);

        StarRocksMetrics::instance()->report_disk_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = worker_pool_this->_master_client->report(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_disk_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report disk state to " << worker_pool_this->_master_info.network_address.hostname
                         << ":" << worker_pool_this->_master_info.network_address.port << ", err=" << status;
        }

#ifndef BE_TEST
        // wait for notifying until timeout
        StorageEngine::instance()->wait_for_report_notify(config::report_disk_state_interval_seconds, false);
    }
#endif

    return (void*)nullptr;
}

void* TaskWorkerPool::_report_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__set_backend(worker_pool_this->_backend);
    request.__isset.tablets = true;
    AgentStatus status = STARROCKS_SUCCESS;

#ifndef BE_TEST
    while (true) {
        if (worker_pool_this->_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            LOG(INFO) << "Waiting to receive first heartbeat from frontend";
            sleep(config::sleep_one_second);
            continue;
        }
#endif
        request.tablets.clear();

        request.__set_report_version(_s_report_version);
        Status st_report = StorageEngine::instance()->tablet_manager()->report_all_tablets_info(&request.tablets);
        if (!st_report.ok()) {
            LOG(WARNING) << "Fail to report all tablets info, err=" << st_report.to_string();
#ifndef BE_TEST
            // wait for notifying until timeout
            StorageEngine::instance()->wait_for_report_notify(config::report_tablet_interval_seconds, true);
            continue;
#else
        return (void*)0;
#endif
        }
        int64_t max_compaction_score =
                std::max(StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.value(),
                         StarRocksMetrics::instance()->tablet_base_max_compaction_score.value());
        request.__set_tablet_max_compaction_score(max_compaction_score);

        TMasterResult result;
        status = worker_pool_this->_master_client->report(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_all_tablets_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report olap table state to "
                         << worker_pool_this->_master_info.network_address.hostname << ":"
                         << worker_pool_this->_master_info.network_address.port << ", err=" << status;
        }

#ifndef BE_TEST
        // wait for notifying until timeout
        StorageEngine::instance()->wait_for_report_notify(config::report_tablet_interval_seconds, true);
    }
#endif

    return (void*)nullptr;
}

void* TaskWorkerPool::_upload_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TUploadReq upload_request;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            upload_request = agent_task_req.upload_req;
            worker_pool_this->_tasks.pop_front();
        }

        LOG(INFO) << "Got upload task signature=" << agent_task_req.signature << " job id=" << upload_request.job_id;

        std::map<int64_t, std::vector<std::string>> tablet_files;
        SnapshotLoader loader(worker_pool_this->_env, upload_request.job_id, agent_task_req.signature);
        Status status = loader.upload(upload_request.src_dest_map, upload_request.broker_addr,
                                      upload_request.broker_prop, &tablet_files);

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
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_tablet_files(tablet_files);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);

        LOG(INFO) << "Uploaded task signature=" << agent_task_req.signature << " job id=" << upload_request.job_id;
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_download_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TDownloadReq download_request;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            download_request = agent_task_req.download_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "Got download task signature=" << agent_task_req.signature
                  << " job id=" << download_request.job_id;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        // TODO: download
        std::vector<int64_t> downloaded_tablet_ids;
        SnapshotLoader loader(worker_pool_this->_env, download_request.job_id, agent_task_req.signature);
        Status status = loader.download(download_request.src_dest_map, download_request.broker_addr,
                                        download_request.broker_prop, &downloaded_tablet_ids);

        if (!status.ok()) {
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "Fail to download job id=" << download_request.job_id << " msg=" << status.get_error_msg();
            error_msgs.push_back(status.get_error_msg());
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_downloaded_tablet_ids(downloaded_tablet_ids);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);

        LOG(INFO) << "Downloaded task signature=" << agent_task_req.signature << " job id=" << download_request.job_id;
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_make_snapshot_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TSnapshotRequest snapshot_request;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            snapshot_request = agent_task_req.snapshot_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "Got snapshot task signature=" << agent_task_req.signature;

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
                         << " version_hash=" << snapshot_request.version_hash << " status=" << st.to_string();
            error_msgs.push_back("make_snapshot failed. status: " + st.to_string());
        } else {
            LOG(INFO) << "Created snapshot tablet_id=" << snapshot_request.tablet_id
                      << " schema_hash=" << snapshot_request.schema_hash << " version=" << snapshot_request.version
                      << " version_hash=" << snapshot_request.version_hash << " snapshot_path=" << snapshot_path;
            if (snapshot_request.__isset.list_files) {
                // list and save all snapshot files
                // snapshot_path like: data/snapshot/20180417205230.1.86400
                // we need to add subdir: tablet_id/schema_hash/
                std::stringstream ss;
                ss << snapshot_path << "/" << snapshot_request.tablet_id << "/" << snapshot_request.schema_hash << "/";
                st = FileUtils::list_files(Env::Default(), ss.str(), &snapshot_files);
                if (!st.ok()) {
                    status_code = TStatusCode::RUNTIME_ERROR;
                    LOG(WARNING) << "Fail to make snapshot tablet_id" << snapshot_request.tablet_id
                                 << " schema_hash=" << snapshot_request.schema_hash
                                 << " version=" << snapshot_request.version
                                 << " version_hash=" << snapshot_request.version_hash << ", list file failed, "
                                 << st.get_error_msg();
                    error_msgs.push_back("make_snapshot failed. list file failed: " + st.get_error_msg());
                }
            }
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_snapshot_path(snapshot_path);
        finish_task_request.__set_snapshot_files(snapshot_files);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

void* TaskWorkerPool::_release_snapshot_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TReleaseSnapshotRequest release_snapshot_request;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            release_snapshot_request = agent_task_req.release_snapshot_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "Got release snapshot task signature=" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        std::string& snapshot_path = release_snapshot_request.snapshot_path;
        OLAPStatus release_snapshot_status = SnapshotManager::instance()->release_snapshot(snapshot_path);
        if (release_snapshot_status != OLAP_SUCCESS) {
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "Fail to release snapshot snapshot_path=" << snapshot_path
                         << " status=" << release_snapshot_status;
            error_msgs.push_back("release_snapshot failed. status: " +
                                 boost::lexical_cast<std::string>(release_snapshot_status));
        } else {
            LOG(INFO) << "Released snapshot path=" << snapshot_path << " status=" << release_snapshot_status;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);
#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

AgentStatus TaskWorkerPool::_get_tablet_info(const TTabletId tablet_id, const TSchemaHash schema_hash,
                                             int64_t signature, TTabletInfo* tablet_info) {
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

void* TaskWorkerPool::_move_dir_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TMoveDirReq move_dir_req;
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_variable->wait(l);
            }

            agent_task_req = worker_pool_this->_tasks.front();
            move_dir_req = agent_task_req.move_dir_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "Got move dir task signature=" << agent_task_req.signature << " job id=" << move_dir_req.job_id;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        // TODO: move dir
        AgentStatus status =
                worker_pool_this->_move_dir(move_dir_req.tablet_id, move_dir_req.schema_hash, move_dir_req.src,
                                            move_dir_req.job_id, true /* TODO */, &error_msgs);

        if (status != STARROCKS_SUCCESS) {
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "Fail to move dir=" << move_dir_req.src << " tablet id=" << move_dir_req.tablet_id
                         << " signature=" << agent_task_req.signature << " job id=" << move_dir_req.job_id;
        } else {
            LOG(INFO) << "Moved dir=" << move_dir_req.src << " tablet_id=" << move_dir_req.tablet_id
                      << " signature=" << agent_task_req.signature << " job id=" << move_dir_req.job_id;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature);

#ifndef BE_TEST
    }
#endif
    return (void*)nullptr;
}

AgentStatus TaskWorkerPool::_move_dir(const TTabletId tablet_id, const TSchemaHash schema_hash, const std::string& src,
                                      int64_t job_id, bool overwrite, std::vector<std::string>* error_msgs) {
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        LOG(INFO) << "Fail to get tablet_id=" << tablet_id << " schema hash=" << schema_hash;
        error_msgs->push_back("failed to get tablet");
        return STARROCKS_TASK_REQUEST_ERROR;
    }

    std::string dest_tablet_dir = tablet->tablet_path();
    SnapshotLoader loader(_env, job_id, tablet_id);
    Status status = loader.move(src, tablet, overwrite);

    if (!status.ok()) {
        LOG(WARNING) << "Fail to move job id=" << job_id << ", " << status.get_error_msg();
        error_msgs->push_back(status.get_error_msg());
        return STARROCKS_INTERNAL_ERROR;
    }

    return STARROCKS_SUCCESS;
}

} // namespace starrocks
