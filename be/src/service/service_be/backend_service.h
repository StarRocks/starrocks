// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/backend_service.h

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

#include <thrift/protocol/TDebugProtocol.h>

#include <ctime>
#include <map>
#include <memory>

#include "agent/agent_server.h"
#include "common/status.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/TStarrocksExternalService.h"
#include "service/backend_base.h"

namespace starrocks {

class ExecEnv;
class ThriftServer;
class TAgentResult;
class TAgentTaskRequest;
class TAgentPublishRequest;
class TMiniLoadEtlTaskRequest;
class TMiniLoadEtlStatusResult;
class TMiniLoadEtlStatusRequest;
class TDeleteEtlFilesRequest;
class TPlanExecRequest;
class TPlanExecParams;
class TExecPlanFragmentParams;
class TExecPlanFragmentResult;
class TInsertResult;
class TReportExecStatusArgs;
class TReportExecStatusParams;
class TReportExecStatusResult;
class TCancelPlanFragmentArgs;
class TCancelPlanFragmentResult;
class TTransmitDataArgs;
class TTransmitDataResult;
class TNetworkAddress;
class TClientRequest;
class TExecRequest;
class TSessionState;
class TQueryOptions;
class TExportTaskRequest;
class TExportStatusResult;

// This class just forward rpc requests to actual handlers, used
// to bind multiple services on single port.
class BackendService : public BackendServiceBase {
public:
    explicit BackendService(ExecEnv* exec_env);

    ~BackendService() override = default;

    // NOTE: now we do not support multiple backend in one process
    static Status create_service(ExecEnv* exec_env, int port, ThriftServer** server);

    // Agent service
    void submit_tasks(TAgentResult& return_value, const std::vector<TAgentTaskRequest>& tasks) override {
        _agent_server->submit_tasks(return_value, tasks);
    }

    void make_snapshot(TAgentResult& return_value, const TSnapshotRequest& snapshot_request) override {
        _agent_server->make_snapshot(return_value, snapshot_request);
    }

    void release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) override {
        _agent_server->release_snapshot(return_value, snapshot_path);
    }

    void publish_cluster_state(TAgentResult& result, const TAgentPublishRequest& request) override {
        _agent_server->publish_cluster_state(result, request);
    }

    void get_tablet_stat(TTabletStatResult& result) override;

private:
    std::unique_ptr<AgentServer> _agent_server;
};

} // namespace starrocks
