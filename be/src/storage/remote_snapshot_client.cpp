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

#include "storage/remote_snapshot_client.h"

#include "common/config_storage_fwd.h"
#include "common/util/thrift_client_cache.h"
#include "gen_cpp/BackendService.h"
#include "platform/thrift_rpc_helper.h"

namespace starrocks {

class ThriftRemoteSnapshotClient final : public RemoteSnapshotClient {
public:
    Status make_snapshot(const std::string& host, int32_t port, const TSnapshotRequest& request,
                         TAgentResult* result) override {
        return ThriftRpcHelper::rpc<BackendServiceClient>(
                host, port,
                [&request, result](BackendServiceConnection& client) { client->make_snapshot(*result, request); },
                config::make_snapshot_rpc_timeout_ms);
    }

    Status release_snapshot(const std::string& host, int32_t port, const std::string& snapshot_path,
                            TAgentResult* result) override {
        return ThriftRpcHelper::rpc<BackendServiceClient>(host, port,
                                                          [&snapshot_path, result](BackendServiceConnection& client) {
                                                              client->release_snapshot(*result, snapshot_path);
                                                          });
    }
};

RemoteSnapshotClient* default_remote_snapshot_client() {
    static ThriftRemoteSnapshotClient client;
    return &client;
}

} // namespace starrocks
