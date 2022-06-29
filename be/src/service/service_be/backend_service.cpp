// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/backend_service.cpp

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

#include "backend_service.h"

#include <arrow/record_batch.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/processor/TMultiplexedProcessor.h>

#include <memory>

#include "agent/agent_server.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/TStarrocksExternalService.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/blocking_queue.hpp"
#include "util/thrift_server.h"

namespace starrocks {

using apache::thrift::TProcessor;
using apache::thrift::concurrency::ThreadFactory;

BackendService::BackendService(ExecEnv* exec_env)
        : BackendServiceBase(exec_env), _agent_server(exec_env->agent_server()) {}

BackendService::~BackendService() = default;

std::unique_ptr<ThriftServer> BackendService::create(ExecEnv* exec_env, int port) {
    std::shared_ptr<BackendService> handler(new BackendService(exec_env));
    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    std::shared_ptr<ThreadFactory> thread_factory(new ThreadFactory());

    std::shared_ptr<TProcessor> be_processor(new BackendServiceProcessor(handler));

    LOG(INFO) << "StarRocksInternalService listening on " << port;
    return std::make_unique<ThriftServer>("backend", be_processor, port, exec_env->metrics(),
                                          config::be_service_threads);
}

void BackendService::get_tablet_stat(TTabletStatResult& result) {
    StorageEngine::instance()->tablet_manager()->get_tablet_stat(&result);
}

void BackendService::submit_tasks(TAgentResult& return_value, const std::vector<TAgentTaskRequest>& tasks) {
    _agent_server->submit_tasks(return_value, tasks);
}

void BackendService::make_snapshot(TAgentResult& return_value, const TSnapshotRequest& snapshot_request) {
    _agent_server->make_snapshot(return_value, snapshot_request);
}

void BackendService::release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) {
    _agent_server->release_snapshot(return_value, snapshot_path);
}

void BackendService::publish_cluster_state(TAgentResult& result, const TAgentPublishRequest& request) {
    _agent_server->publish_cluster_state(result, request);
}

} // namespace starrocks
