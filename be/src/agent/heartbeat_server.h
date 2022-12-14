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
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/heartbeat_server.h

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

#include <memory>
#include <mutex>

#include "agent/status.h"
#include "common/statusor.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/Status_types.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"
#include "thrift/transport/TTransportUtils.h"

namespace starrocks {

class StorageEngine;
class Status;
class ThriftServer;

class HeartbeatServer : public HeartbeatServiceIf {
public:
    HeartbeatServer();
    ~HeartbeatServer() override = default;

    virtual void init_cluster_id_or_die();

    // Master send heartbeat to this server
    //
    // Input parameters:
    // * master_info: The struct of master info, contains host ip and port
    //
    // Output parameters:
    // * heartbeat_result: The result of heartbeat set
    void heartbeat(THeartbeatResult& heartbeat_result, const TMasterInfo& master_info) override;

    DISALLOW_COPY_AND_MOVE(HeartbeatServer);

private:
    enum CmpResult {
        kUnchanged,
        kNeedUpdate,
        kNeedUpdateAndReport,
    };

    StatusOr<CmpResult> compare_master_info(const TMasterInfo& master_info);

    StorageEngine* _olap_engine;
}; // class HeartBeatServer

StatusOr<std::unique_ptr<ThriftServer>> create_heartbeat_server(ExecEnv* exec_env, uint32_t heartbeat_server_port,
                                                                uint32_t worker_thread_num);
} // namespace starrocks
