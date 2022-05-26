// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/brpc_service.cpp

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

#include "service/brpc_service.h"

#include <cstring>

#include "common/config.h"
#include "common/logging.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

namespace starrocks {

BRpcService::BRpcService(ExecEnv* exec_env) : _exec_env(exec_env), _server(new brpc::Server()) {
    // Set config.
    brpc::FLAGS_max_body_size = config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = config::brpc_socket_max_unwritten_bytes;
}

BRpcService::~BRpcService() = default;

Status BRpcService::start(int port, google::protobuf::Service* service, google::protobuf::Service* doris_service) {
    // Add services.
    _server->AddService(service, brpc::SERVER_OWNS_SERVICE);
    _server->AddService(doris_service, brpc::SERVER_OWNS_SERVICE);
    // Start services.
    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }

    if (_server->Start(port, &options) != 0) {
        PLOG(WARNING) << "Fail to start brpc server on port " << port;
        return Status::InternalError("Start brpc service failed");
    }
    return Status::OK();
}

void BRpcService::join() {
    _server->Stop(0);
    _server->Join();
}

} // namespace starrocks
