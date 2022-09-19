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

#include <atomic>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <sstream>
#include <string>

#include "agent/agent_server.h"
#include "agent/finish_task.h"
#include "agent/master_info.h"
#include "agent/publish_version.h"
#include "agent/report_task.h"
#include "agent/task_singatures_manager.h"
#include "common/status.h"
#include "exec/workgroup/work_group.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "storage/data_dir.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/task/engine_alter_tablet_task.h"
#include "storage/task/engine_batch_load_task.h"
#include "storage/task/engine_checksum_task.h"
#include "storage/task/engine_clone_task.h"
#include "storage/task/engine_storage_migration_task.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"

namespace starrocks {

const size_t PUBLISH_VERSION_BATCH_SIZE = 10;

std::atomic<int64_t> TaskWorkerPoolBase::_s_report_version(time(nullptr) * 10000);

using std::swap;

template <class AgentTaskRequest>
TaskWorkerPool<AgentTaskRequest>::TaskWorkerPool(ExecEnv* env, int worker_count)
        : _env(env), _worker_thread_condition_variable(new std::condition_variable()), _worker_count(worker_count) {
    _backend.__set_host(BackendOptions::get_localhost());
    _backend.__set_be_port(config::be_port);
    _backend.__set_http_port(config::webserver_port);
}

template <class AgentTaskRequest>
TaskWorkerPool<AgentTaskRequest>::~TaskWorkerPool() {
    stop();
    delete _worker_thread_condition_variable;
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::start() {
    for (uint32_t i = 0; i < _worker_count; i++) {
        _spawn_callback_worker_thread(_callback_function);
    }
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::stop() {
    if (_stopped) {
        return;
    }
    _stopped = true;
    _worker_thread_condition_variable->notify_all();
    for (uint32_t i = 0; i < _worker_count; ++i) {
        if (_worker_threads[i].joinable()) {
            _worker_threads[i].join();
        }
    }
}

template <class AgentTaskRequest>
size_t TaskWorkerPool<AgentTaskRequest>::_push_task(AgentTaskRequestPtr task) {
    std::unique_lock l(_worker_thread_lock);
    _tasks.emplace_back(std::move(task));
    _worker_thread_condition_variable->notify_one();
    return _tasks.size();
}

template <class AgentTaskRequest>
typename TaskWorkerPool<AgentTaskRequest>::AgentTaskRequestPtr TaskWorkerPool<AgentTaskRequest>::_pop_task() {
    std::unique_lock l(_worker_thread_lock);
    _worker_thread_condition_variable->wait(l, [&]() { return !_tasks.empty() || _stopped; });
    if (!_stopped) {
        auto ret = std::move(_tasks.front());
        _tasks.pop_front();
        return ret;
    }
    return nullptr;
}

template <class AgentTaskRequest>
typename TaskWorkerPool<AgentTaskRequest>::AgentTaskRequestPtr TaskWorkerPool<AgentTaskRequest>::_pop_task(
        TPriority::type pri) {
    std::unique_lock l(_worker_thread_lock);
    _worker_thread_condition_variable->wait(l, [&]() { return !_tasks.empty() || _stopped; });
    if (_stopped) {
        return nullptr;
    }
    for (int64_t i = static_cast<int64_t>(_tasks.size()) - 1; i >= 0; --i) {
        auto& task = _tasks[i];
        if (task->isset.priority && task->priority == pri) {
            auto ret = std::move(task);
            _tasks.erase(_tasks.begin() + i);
            return ret;
        }
    }
    return nullptr;
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::submit_task(const TAgentTaskRequest& task) {
    const TTaskType::type task_type = task.task_type;
    int64_t signature = task.signature;

    std::string type_str;
    EnumToString(TTaskType, task_type, type_str);

    if (register_task_info(task_type, signature)) {
        // Set the receiving time of task so that we can determine whether it is timed out later
        auto new_task = _convert_task(task, time(nullptr));
        size_t task_count = _push_task(std::move(new_task));
        LOG(INFO) << "Submit task success. type=" << type_str << ", signature=" << signature
                  << ", task_count_in_queue=" << task_count;
    } else {
        LOG(INFO) << "Submit task failed, already exists type=" << type_str << ", signature=" << signature;
    }
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::submit_tasks(const std::vector<const TAgentTaskRequest*>& tasks) {
    DCHECK(!tasks.empty());
    std::string type_str;
    size_t task_count = tasks.size();
    const TTaskType::type task_type = tasks[0]->task_type;
    EnumToString(TTaskType, task_type, type_str);
    const auto recv_time = time(nullptr);
    auto failed_task = batch_register_task_info(tasks);

    size_t non_zeros = SIMD::count_nonzero(failed_task);

    std::stringstream fail_ss;
    std::stringstream succ_ss;
    size_t fail_counter = non_zeros;
    size_t succ_counter = task_count - non_zeros;
    for (int i = 0; i < failed_task.size(); ++i) {
        if (failed_task[i] == 1) {
            fail_ss << tasks[i]->signature;
            fail_counter--;
            if (fail_counter > 0) {
                fail_ss << ",";
            }
        } else {
            succ_ss << tasks[i]->signature;
            succ_counter--;
            if (succ_counter > 0) {
                succ_ss << ",";
            }
        }
    }
    if (fail_ss.str().length() > 0) {
        LOG(INFO) << "fail to register task. type=" << type_str << ", signatures=[" << fail_ss.str() << "]";
    }

    size_t queue_size = 0;
    {
        std::unique_lock l(_worker_thread_lock);
        if (UNLIKELY(task_type == TTaskType::REALTIME_PUSH &&
                     tasks[0]->push_req.push_type == TPushType::CANCEL_DELETE)) {
            for (size_t i = 0; i < task_count; i++) {
                if (failed_task[i] == 0) {
                    auto new_task = _convert_task(*tasks[i], recv_time);
                    _tasks.emplace_front(std::move(new_task));
                }
            }
        } else {
            for (size_t i = 0; i < task_count; i++) {
                if (failed_task[i] == 0) {
                    auto new_task = _convert_task(*tasks[i], recv_time);
                    _tasks.emplace_back(std::move(new_task));
                }
            }
        }
        queue_size = _tasks.size();
        _worker_thread_condition_variable->notify_all();
    }

    if (succ_ss.str().length() > 0) {
        LOG(INFO) << "success to submit task. type=" << type_str << ", signature=[" << succ_ss.str()
                  << "], task_count_in_queue=" << queue_size;
    }
}

template <class AgentTaskRequest>
size_t TaskWorkerPool<AgentTaskRequest>::num_queued_tasks() const {
    std::lock_guard l(_worker_thread_lock);
    return _tasks.size();
}

template <class AgentTaskRequest>
void TaskWorkerPool<AgentTaskRequest>::_spawn_callback_worker_thread(CALLBACK_FUNCTION callback_func) {
    std::thread worker_thread(callback_func, this);
    _worker_threads.emplace_back(std::move(worker_thread));
    Thread::set_thread_name(_worker_threads.back(), "task_worker");
}

void* CreateTabletTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (CreateTabletTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const auto& create_tablet_req = agent_task_req->task_req;
        TFinishTaskRequest finish_task_request;
        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        auto tablet_type = create_tablet_req.tablet_type;
        Status create_status;
        if (tablet_type == TTabletType::TABLET_TYPE_LAKE) {
            create_status = worker_pool_this->_env->lake_tablet_manager()->create_tablet(create_tablet_req);
        } else {
            create_status = StorageEngine::instance()->create_tablet(create_tablet_req);
        }
        if (!create_status.ok()) {
            LOG(WARNING) << "create table failed. status: " << create_status.to_string()
                         << ", signature: " << agent_task_req->signature;
            status_code = TStatusCode::RUNTIME_ERROR;
        } else if (create_tablet_req.tablet_type != TTabletType::TABLET_TYPE_LAKE) {
            _s_report_version.fetch_add(1, std::memory_order_relaxed);
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
        finish_task_request.__set_report_version(_s_report_version.load(std::memory_order_relaxed));
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);
        finish_task_request.__set_task_status(task_status);

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }
    return nullptr;
}

void* AlterTableTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (AlterTableTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        int64_t signatrue = agent_task_req->signature;
        LOG(INFO) << "get alter table task, signature: " << agent_task_req->signature;
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
                _alter_tablet(agent_task_req->task_req, signatrue, task_type, &finish_task_request);
            }
            finish_task(finish_task_request);
        }
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }
    return nullptr;
}

void AlterTableTaskWorkerPool::_alter_tablet(const TAlterTabletReqV2& agent_task_req, int64_t signature,
                                             TTaskType::type task_type, TFinishTaskRequest* finish_task_request) {
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
        new_tablet_id = agent_task_req.new_tablet_id;
        new_schema_hash = agent_task_req.new_schema_hash;
        EngineAlterTabletTask engine_task(ExecEnv::GetInstance()->schema_change_mem_tracker(), agent_task_req,
                                          signature, task_type, &error_msgs, process_name);
        Status sc_status = StorageEngine::instance()->execute_task(&engine_task);
        if (!sc_status.ok()) {
            status = STARROCKS_ERROR;
        } else {
            status = STARROCKS_SUCCESS;
        }
    }

    if (status == STARROCKS_SUCCESS) {
        _s_report_version.fetch_add(1, std::memory_order_relaxed);
        LOG(INFO) << process_name << " finished. signature: " << signature;
    }

    // Return result to fe
    finish_task_request->__set_backend(BackendOptions::get_localBackend());
    finish_task_request->__set_report_version(_s_report_version.load(std::memory_order_relaxed));
    finish_task_request->__set_task_type(task_type);
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
                << process_name << " success, but get new tablet info failed."
                << "tablet_id: " << new_tablet_id << ", schema_hash: " << new_schema_hash
                << ", signature: " << signature;
    }

    if (status == STARROCKS_SUCCESS) {
        swap(finish_tablet_infos, finish_task_request->finish_tablet_infos);
        finish_task_request->__isset.finish_tablet_infos = true;
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
        error_msgs.push_back("status: " + print_agent_status(status));
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
    }

    task_status.__set_error_msgs(error_msgs);
    finish_task_request->__set_task_status(task_status);
}

void* PushTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    static uint32_t s_worker_count = 0;

    auto* worker_pool_this = (PushTaskWorkerPool*)arg_this;

    TPriority::type priority = TPriority::NORMAL;

    int32_t push_worker_count_high_priority = config::push_worker_count_high_priority;
    {
        std::lock_guard worker_thread_lock(worker_pool_this->_worker_thread_lock);
        if (s_worker_count < push_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

    while (true) {
        AgentStatus status = STARROCKS_SUCCESS;
        AgentTaskRequestPtr agent_task_req;
        do {
            agent_task_req = worker_pool_this->_pop_task(priority);
            if (agent_task_req == nullptr) {
                // there is no high priority task. notify other thread to handle normal task
                worker_pool_this->_worker_thread_condition_variable->notify_one();
                break;
            }
        } while (false);

        if (worker_pool_this->_stopped) {
            break;
        }
        if (agent_task_req == nullptr) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }
        auto& push_req = agent_task_req->task_req;

        LOG(INFO) << "get push task. signature: " << agent_task_req->signature << " priority: " << priority
                  << " push_type: " << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;

        EngineBatchLoadTask engine_task(push_req, &tablet_infos, agent_task_req->signature, &status,
                                        ExecEnv::GetInstance()->load_mem_tracker());
        StorageEngine::instance()->execute_task(&engine_task);

        if (status == STARROCKS_PUSH_HAD_LOADED) {
            // remove the task and not return to fe
            remove_task_info(agent_task_req->task_type, agent_task_req->signature);
            continue;
        }
        // Return result to fe
        std::vector<string> error_msgs;
        TStatus task_status;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);

        if (status == STARROCKS_SUCCESS) {
            VLOG(3) << "push ok. signature: " << agent_task_req->signature << ", push_type: " << push_req.push_type;
            error_msgs.emplace_back("push success");

            _s_report_version.fetch_add(1, std::memory_order_relaxed);

            task_status.__set_status_code(TStatusCode::OK);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
            LOG(WARNING) << "push request push_type invalid. type: " << push_req.push_type
                         << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("push request push_type invalid.");
            task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
        } else {
            LOG(WARNING) << "push failed, error_code: " << status << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("push failed");
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        }
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_report_version(_s_report_version.load(std::memory_order_relaxed));

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }

    return nullptr;
}

void* DeleteTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    static uint32_t s_worker_count = 0;

    auto* worker_pool_this = (DeleteTaskWorkerPool*)arg_this;

    TPriority::type priority = TPriority::NORMAL;

    int32_t delete_worker_count_high_priority = config::delete_worker_count_high_priority;
    {
        std::lock_guard worker_thread_lock(worker_pool_this->_worker_thread_lock);
        if (s_worker_count < delete_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

    while (true) {
        AgentStatus status = STARROCKS_SUCCESS;
        AgentTaskRequestPtr agent_task_req;
        do {
            agent_task_req = worker_pool_this->_pop_task(priority);
            if (agent_task_req == nullptr) {
                // there is no high priority task. notify other thread to handle normal task
                worker_pool_this->_worker_thread_condition_variable->notify_one();
                break;
            }
            const auto& push_req = agent_task_req->task_req;

            int num_of_remove_task = 0;
            if (push_req.push_type == TPushType::CANCEL_DELETE) {
                LOG(INFO) << "get delete push task. remove delete task txn_id: " << push_req.transaction_id
                          << " priority: " << priority << " push_type: " << push_req.push_type;

                std::lock_guard l(worker_pool_this->_worker_thread_lock);
                auto& tasks = worker_pool_this->_tasks;
                for (auto it = tasks.begin(); it != tasks.end();) {
                    AgentTaskRequestPtr& task_req = *it;
                    if (task_req->task_type == TTaskType::REALTIME_PUSH) {
                        const TPushReq& push_task_in_queue = task_req->task_req;
                        if (push_task_in_queue.push_type == TPushType::DELETE &&
                            push_task_in_queue.transaction_id == push_req.transaction_id) {
                            it = worker_pool_this->_tasks.erase(it);
                            ++num_of_remove_task;
                        } else {
                            ++it;
                        }
                    }
                }
                LOG(INFO) << "finish remove delete task txn_id: " << push_req.transaction_id
                          << " num_of_remove_task: " << num_of_remove_task << " priority: " << priority
                          << " push_type: " << push_req.push_type;
            }
        } while (false);

        if (worker_pool_this->_stopped) {
            break;
        }
        if (agent_task_req == nullptr) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }
        auto& push_req = agent_task_req->task_req;

        LOG(INFO) << "get delete push task. signature: " << agent_task_req->signature << " priority: " << priority
                  << " push_type: " << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;

        EngineBatchLoadTask engine_task(push_req, &tablet_infos, agent_task_req->signature, &status,
                                        ExecEnv::GetInstance()->load_mem_tracker());
        StorageEngine::instance()->execute_task(&engine_task);

        if (status == STARROCKS_PUSH_HAD_LOADED) {
            // remove the task and not return to fe
            remove_task_info(agent_task_req->task_type, agent_task_req->signature);
            continue;
        }
        // Return result to fe
        std::vector<string> error_msgs;
        TStatus task_status;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);
        if (push_req.push_type == TPushType::DELETE) {
            finish_task_request.__set_request_version(push_req.version);
        }

        if (status == STARROCKS_SUCCESS) {
            VLOG(3) << "delete push ok. signature: " << agent_task_req->signature
                    << ", push_type: " << push_req.push_type;
            error_msgs.emplace_back("push success");

            _s_report_version.fetch_add(1, std::memory_order_relaxed);

            task_status.__set_status_code(TStatusCode::OK);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else if (status == STARROCKS_TASK_REQUEST_ERROR) {
            LOG(WARNING) << "delete push request push_type invalid. type: " << push_req.push_type
                         << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("push request push_type invalid.");
            task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
        } else {
            LOG(WARNING) << "delete push failed, error_code: " << status
                         << ", signature: " << agent_task_req->signature;
            error_msgs.emplace_back("delete push failed");
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        }
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_report_version(_s_report_version.load(std::memory_order_relaxed));

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }

    return nullptr;
}

void* PublishVersionTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (PublishVersionTaskWorkerPool*)arg_this;
    auto* agent_server = worker_pool_this->_env->agent_server();
    auto token =
            agent_server->get_thread_pool(TTaskType::PUBLISH_VERSION)->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    struct VersionCmp {
        bool operator()(const AgentTaskRequestPtr& lhs, const AgentTaskRequestPtr& rhs) const {
            if (lhs->task_req.__isset.commit_timestamp && rhs->task_req.__isset.commit_timestamp) {
                if (lhs->task_req.commit_timestamp > rhs->task_req.commit_timestamp) {
                    return true;
                }
            }
            return false;
        }
    };
    std::priority_queue<AgentTaskRequestPtr, std::vector<AgentTaskRequestPtr>, VersionCmp> priority_tasks;
    std::unordered_set<DataDir*> affected_dirs;
    std::vector<TFinishTaskRequest> finish_task_requests;
    int64_t batch_publish_latency = 0;

    while (true) {
        {
            std::unique_lock l(worker_pool_this->_worker_thread_lock);
            worker_pool_this->_worker_thread_condition_variable->wait(l, [&]() {
                return !priority_tasks.empty() || !worker_pool_this->_tasks.empty() || worker_pool_this->_stopped;
            });
            if (worker_pool_this->_stopped) {
                break;
            }

            while (!worker_pool_this->_tasks.empty()) {
                // collect some publish version tasks as a group.
                priority_tasks.emplace(std::move(worker_pool_this->_tasks.front()));
                worker_pool_this->_tasks.pop_front();
            }
        }

        const auto& publish_version_task = *priority_tasks.top();
        LOG(INFO) << "get publish version task txn_id: " << publish_version_task.task_req.transaction_id
                  << " priority queue size: " << priority_tasks.size();
        StarRocksMetrics::instance()->publish_task_request_total.increment(1);
        auto& finish_task_request = finish_task_requests.emplace_back();
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_report_version(_s_report_version.load(std::memory_order_relaxed));
        int64_t start_ts = MonotonicMillis();
        run_publish_version_task(token.get(), publish_version_task, finish_task_request, affected_dirs);
        batch_publish_latency += MonotonicMillis() - start_ts;
        priority_tasks.pop();

        if (priority_tasks.empty() || finish_task_requests.size() > PUBLISH_VERSION_BATCH_SIZE ||
            batch_publish_latency > config::max_batch_publish_latency_ms) {
            int64_t t0 = MonotonicMillis();
            StorageEngine::instance()->txn_manager()->flush_dirs(affected_dirs);
            int64_t t1 = MonotonicMillis();
            // notify FE when all tasks of group have been finished.
            for (auto& finish_task_request : finish_task_requests) {
                finish_task(finish_task_request);
                remove_task_info(finish_task_request.task_type, finish_task_request.signature);
            }
            int64_t t2 = MonotonicMillis();
            LOG(INFO) << "batch flush " << finish_task_requests.size()
                      << " txn publish task(s). #dir:" << affected_dirs.size() << " flush:" << t1 - t0
                      << "ms finish_task_rpc:" << t2 - t1 << "ms";
            finish_task_requests.clear();
            affected_dirs.clear();
            batch_publish_latency = 0;
        }
    }
    return nullptr;
}

void* ClearTransactionTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ClearTransactionTaskWorkerPool*)arg_this;
    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TClearTransactionTaskRequest& clear_transaction_task_req = agent_task_req->task_req;
        LOG(INFO) << "get clear transaction task task, signature:" << agent_task_req->signature
                  << ", txn_id: " << clear_transaction_task_req.transaction_id
                  << ", partition id size: " << clear_transaction_task_req.partition_id.size();

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        if (clear_transaction_task_req.transaction_id > 0) {
            // transaction_id should be greater than zero.
            // If it is not greater than zero, no need to execute
            // the following clear_transaction_task() function.
            if (!clear_transaction_task_req.partition_id.empty()) {
                StorageEngine::instance()->clear_transaction_task(clear_transaction_task_req.transaction_id,
                                                                  clear_transaction_task_req.partition_id);
            } else {
                StorageEngine::instance()->clear_transaction_task(clear_transaction_task_req.transaction_id);
            }
            LOG(INFO) << "finish to clear transaction task. signature:" << agent_task_req->signature
                      << ", txn_id: " << clear_transaction_task_req.transaction_id;
        } else {
            LOG(WARNING) << "invalid txn_id: " << clear_transaction_task_req.transaction_id
                         << ", signature: " << agent_task_req->signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }
    return nullptr;
}

void* UpdateTabletMetaInfoTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (UpdateTabletMetaInfoTaskWorkerPool*)arg_this;
    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TUpdateTabletMetaInfoReq& update_tablet_meta_req = agent_task_req->task_req;

        LOG(INFO) << "get update tablet meta task, signature:" << agent_task_req->signature;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        for (const auto& tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
            TabletSharedPtr tablet =
                    StorageEngine::instance()->tablet_manager()->get_tablet(tablet_meta_info.tablet_id);
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

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(BackendOptions::get_localBackend());
        finish_task_request.__set_task_type(agent_task_req->task_type);
        finish_task_request.__set_signature(agent_task_req->signature);

        finish_task(finish_task_request);
        remove_task_info(agent_task_req->task_type, agent_task_req->signature);
    }
    return nullptr;
}

void* CloneTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (CloneTaskWorkerPool*)arg_this;
    auto* agent_server = worker_pool_this->_env->agent_server();

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        StarRocksMetrics::instance()->clone_requests_total.increment(1);
        auto clone_thread_pool = agent_server->get_thread_pool(TTaskType::CLONE);
        LOG(INFO) << "get clone task. signature:" << agent_task_req->signature
                  << ", queued tasks in worker pool: " << worker_pool_this->num_queued_tasks()
                  << ", queued tasks in dynamic thread pool: " << clone_thread_pool->num_queued_tasks()
                  << ", running tasks in dynamic thread pool: " << clone_thread_pool->num_threads();
        clone_thread_pool->submit_func(std::bind(run_clone_task, agent_task_req));
    }

    return nullptr;
}

void* StorageMediumMigrateTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (StorageMediumMigrateTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TStorageMediumMigrateReq& storage_medium_migrate_req = agent_task_req->task_req;
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
    return nullptr;
}

void* CheckConsistencyTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (CheckConsistencyTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
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
            EngineChecksumTask engine_task(mem_tracker, check_consistency_req.tablet_id,
                                           check_consistency_req.schema_hash, check_consistency_req.version, &checksum);
            Status res = StorageEngine::instance()->execute_task(&engine_task);
            if (!res.ok()) {
                LOG(WARNING) << "check consistency failed. status: " << res
                             << ", signature: " << agent_task_req->signature;
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
    return nullptr;
}

void* ReportTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportTaskWorkerPool*)arg_this;

    TReportRequest request;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(1);
            continue;
        }
        auto tasks = count_all_tasks();
        request.__set_tasks(tasks);
        request.__set_backend(BackendOptions::get_localBackend());

        StarRocksMetrics::instance()->report_task_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_task_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report task to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }

        sleep(config::report_task_interval_seconds);
    }

    return nullptr;
}

void* ReportDiskStateTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportDiskStateTaskWorkerPool*)arg_this;

    TReportRequest request;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }
        std::vector<DataDirInfo> data_dir_infos;
        StorageEngine::instance()->get_all_data_dir_info(&data_dir_infos, true /* update */);

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
        request.__set_backend(BackendOptions::get_localBackend());

        StarRocksMetrics::instance()->report_disk_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_disk_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report disk state to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }

        // wait for notifying until timeout
        StorageEngine::instance()->wait_for_report_notify(config::report_disk_state_interval_seconds, false);
    }

    return nullptr;
}

void* ReportOlapTableTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportOlapTableTaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__isset.tablets = true;
    AgentStatus status = STARROCKS_SUCCESS;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }
        request.tablets.clear();

        request.__set_report_version(_s_report_version.load(std::memory_order_relaxed));
        Status st_report = StorageEngine::instance()->tablet_manager()->report_all_tablets_info(&request.tablets);
        if (!st_report.ok()) {
            LOG(WARNING) << "Fail to report all tablets info, err=" << st_report.to_string();
            // wait for notifying until timeout
            StorageEngine::instance()->wait_for_report_notify(config::report_tablet_interval_seconds, true);
            continue;
        }
        int64_t max_compaction_score =
                std::max(StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.value(),
                         StarRocksMetrics::instance()->tablet_base_max_compaction_score.value());
        request.__set_tablet_max_compaction_score(max_compaction_score);
        request.__set_backend(BackendOptions::get_localBackend());

        TMasterResult result;
        status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_all_tablets_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report olap table state to " << master_address.hostname << ":"
                         << master_address.port << ", err=" << status;
        }

        // wait for notifying until timeout
        StorageEngine::instance()->wait_for_report_notify(config::report_tablet_interval_seconds, true);
    }

    return nullptr;
}

void* ReportWorkgroupTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReportWorkgroupTaskWorkerPool*)arg_this;

    TReportRequest request;
    AgentStatus status = STARROCKS_SUCCESS;

    while ((!worker_pool_this->_stopped)) {
        auto master_address = get_master_address();
        if (master_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            sleep(config::sleep_one_second);
            continue;
        }

        StarRocksMetrics::instance()->report_workgroup_requests_total.increment(1);
        request.__set_report_version(_s_report_version.load(std::memory_order_relaxed));
        auto workgroups = workgroup::WorkGroupManager::instance()->list_workgroups();
        request.__set_active_workgroups(std::move(workgroups));
        request.__set_backend(BackendOptions::get_localBackend());
        TMasterResult result;
        status = report_task(request, &result);

        if (status != STARROCKS_SUCCESS) {
            StarRocksMetrics::instance()->report_workgroup_requests_failed.increment(1);
            LOG(WARNING) << "Fail to report workgroup to " << master_address.hostname << ":" << master_address.port
                         << ", err=" << status;
        }
        if (result.__isset.workgroup_ops) {
            workgroup::WorkGroupManager::instance()->apply(result.workgroup_ops);
        }
        sleep(config::report_workgroup_interval_seconds);
    }

    return nullptr;
}

void* UploadTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (UploadTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TUploadReq& upload_request = agent_task_req->task_req;

        LOG(INFO) << "Got upload task signature=" << agent_task_req->signature << " job id=" << upload_request.job_id;

        std::map<int64_t, std::vector<std::string>> tablet_files;
        SnapshotLoader loader(worker_pool_this->_env, upload_request.job_id, agent_task_req->signature);
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
    return nullptr;
}

void* DownloadTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (DownloadTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TDownloadReq& download_request = agent_task_req->task_req;
        LOG(INFO) << "Got download task signature=" << agent_task_req->signature
                  << " job id=" << download_request.job_id;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

        // TODO: download
        std::vector<int64_t> downloaded_tablet_ids;
        SnapshotLoader loader(worker_pool_this->_env, download_request.job_id, agent_task_req->signature);
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
    return nullptr;
}

void* MakeSnapshotTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (MakeSnapshotTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
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
                                 << " version=" << snapshot_request.version << ", list file failed, "
                                 << st.get_error_msg();
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
    return nullptr;
}

void* ReleaseSnapshotTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (ReleaseSnapshotTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TReleaseSnapshotRequest& release_snapshot_request = agent_task_req->task_req;
        LOG(INFO) << "Got release snapshot task signature=" << agent_task_req->signature;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<std::string> error_msgs;
        TStatus task_status;

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
    return nullptr;
}

AgentStatus TaskWorkerPoolBase::get_tablet_info(TTabletId tablet_id, TSchemaHash schema_hash, int64_t signature,
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

void* MoveTaskWorkerPool::_worker_thread_callback(void* arg_this) {
    auto* worker_pool_this = (MoveTaskWorkerPool*)arg_this;

    while (true) {
        AgentTaskRequestPtr agent_task_req = worker_pool_this->_pop_task();
        if (agent_task_req == nullptr) {
            break;
        }
        const TMoveDirReq& move_dir_req = agent_task_req->task_req;
        LOG(INFO) << "Got move dir task signature=" << agent_task_req->signature << " job id=" << move_dir_req.job_id;

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
                         << " signature=" << agent_task_req->signature << " job id=" << move_dir_req.job_id;
        } else {
            LOG(INFO) << "Moved dir=" << move_dir_req.src << " tablet_id=" << move_dir_req.tablet_id
                      << " signature=" << agent_task_req->signature << " job id=" << move_dir_req.job_id;
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
    return nullptr;
}

AgentStatus MoveTaskWorkerPool::_move_dir(TTabletId tablet_id, TSchemaHash schema_hash, const std::string& src,
                                          int64_t job_id, bool overwrite, std::vector<std::string>* error_msgs) {
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(INFO) << "Fail to get tablet_id=" << tablet_id << " schema hash=" << schema_hash;
        error_msgs->push_back("failed to get tablet");
        return STARROCKS_TASK_REQUEST_ERROR;
    }

    std::string dest_tablet_dir = tablet->schema_hash_path();
    SnapshotLoader loader(_env, job_id, tablet_id);
    Status status = loader.move(src, tablet, overwrite);

    if (!status.ok()) {
        LOG(WARNING) << "Fail to move job id=" << job_id << ", " << status.get_error_msg();
        error_msgs->push_back(status.get_error_msg());
        return STARROCKS_INTERNAL_ERROR;
    }

    return STARROCKS_SUCCESS;
}

template class TaskWorkerPool<CreateTabletAgentTaskRequest>;
template class TaskWorkerPool<PushReqAgentTaskRequest>;
template class TaskWorkerPool<PublishVersionAgentTaskRequest>;
template class TaskWorkerPool<ClearTransactionAgentTaskRequest>;
template class TaskWorkerPool<AlterTabletAgentTaskRequest>;
template class TaskWorkerPool<CloneAgentTaskRequest>;
template class TaskWorkerPool<StorageMediumMigrateTaskRequest>;
template class TaskWorkerPool<CheckConsistencyTaskRequest>;
template class TaskWorkerPool<AgentTaskRequestWithoutReqBody>;
template class TaskWorkerPool<UploadAgentTaskRequest>;
template class TaskWorkerPool<DownloadAgentTaskRequest>;
template class TaskWorkerPool<SnapshotAgentTaskRequest>;
template class TaskWorkerPool<ReleaseSnapshotAgentTaskRequest>;
template class TaskWorkerPool<MoveDirAgentTaskRequest>;
template class TaskWorkerPool<UpdateTabletMetaInfoAgentTaskRequest>;

} // namespace starrocks
