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
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/utils.cpp

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

#include "agent/utils.h"

#include <sstream>

#include "agent/master_info.h"
#include "common/status.h"
#include "fmt/format.h"

using std::map;
using std::string;
using std::stringstream;
using std::vector;
using apache::thrift::TException;
using apache::thrift::transport::TTransportException;

namespace starrocks {

MasterServerClient::MasterServerClient(FrontendServiceClientCache* client_cache) : _client_cache(client_cache) {}

AgentStatus MasterServerClient::finish_task(const TFinishTaskRequest& request, TMasterResult* result) {
    Status client_status;
    TNetworkAddress network_address = get_master_address();
    FrontendServiceConnection client(_client_cache, network_address, config::thrift_rpc_timeout_ms, &client_status);

    if (!client_status.ok()) {
        LOG(WARNING) << "Fail to get master client from cache. "
                     << "host=" << network_address.hostname << ", port=" << network_address.port
                     << ", code=" << client_status.code();
        return STARROCKS_ERROR;
    }

    try {
        try {
            client->finishTask(*result, request);
        } catch (TTransportException& e) {
            client_status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!client_status.ok()) {
                LOG(WARNING) << "Fail to get master client from cache. "
                             << "host=" << network_address.hostname << ", port=" << network_address.port
                             << ", code=" << client_status.code();
                return STARROCKS_ERROR;
            }
            client->finishTask(*result, request);
        }
    } catch (TException& e) {
        WARN_IF_ERROR(client.reopen(config::thrift_rpc_timeout_ms),
                      fmt::format("reopen thrift client error, host={}, port={}", network_address.hostname,
                                  network_address.port));
        LOG(WARNING) << "Fail to finish_task. "
                     << "host=" << network_address.hostname << ", port=" << network_address.port
                     << ", error=" << e.what();
        return STARROCKS_ERROR;
    }

    return STARROCKS_SUCCESS;
}

AgentStatus MasterServerClient::report(const TReportRequest& request, TMasterResult* result) {
    Status client_status;
    TNetworkAddress network_address = get_master_address();
    FrontendServiceConnection client(_client_cache, network_address, config::thrift_rpc_timeout_ms, &client_status);

    if (!client_status.ok()) {
        LOG(WARNING) << "Fail to get master client from cache. "
                     << "host=" << network_address.hostname << " port=" << network_address.port
                     << " code=" << client_status.code();
        return STARROCKS_ERROR;
    }

    try {
        try {
            client->report(*result, request);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                client_status = client.reopen(config::thrift_rpc_timeout_ms);
                if (!client_status.ok()) {
                    LOG(WARNING) << "Fail to get master client from cache. "
                                 << "host=" << network_address.hostname << ", port=" << network_address.port
                                 << ", code=" << client_status.code();
                    return STARROCKS_ERROR;
                }

                client->report(*result, request);
            } else {
                // TIMED_OUT exception. do not retry
                // actually we don't care what FE returns.
                LOG(WARNING) << "Fail to report to master: " << e.what();
                return STARROCKS_ERROR;
            }
        }
    } catch (TException& e) {
        WARN_IF_ERROR(client.reopen(config::thrift_rpc_timeout_ms),
                      fmt::format("reopen thrift client error, host={}, port={}", network_address.hostname,
                                  network_address.port));
        LOG(WARNING) << "Fail to report to master. "
                     << "host=" << network_address.hostname << ", port=" << network_address.port
                     << ", code=" << client_status.code();
        return STARROCKS_ERROR;
    }

    return STARROCKS_SUCCESS;
}

} // namespace starrocks
