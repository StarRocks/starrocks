// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
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

#include "common/status.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/TStarrocksExternalService.h"
#include "service/backend_base.h"

namespace starrocks {

// This class just forward rpc requests to actual handlers, used
// to bind multiple services on single port.
class ComputeService : public BackendServiceBase {
public:
    explicit ComputeService(ExecEnv* exec_env);

    // NOTE: now we do not support multiple backend in one process
    static Status create_service(ExecEnv* exec_env, int port, ThriftServer** server);
};

} // namespace starrocks
