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

#include "testutil/local_snapshot_client.h"

#include "storage/remote_snapshot_client.h"
#include "storage/snapshot_manager.h"

namespace starrocks {

class LocalSnapshotClientForTest final : public RemoteSnapshotClient {
public:
    Status make_snapshot(const std::string& host, int32_t port, const TSnapshotRequest& request,
                         TAgentResult* result) override {
        std::string snapshot_path;
        auto status = SnapshotManager::instance()->make_snapshot(request, &snapshot_path);
        if (status.ok()) {
            result->__set_snapshot_path(snapshot_path);
        }
        status.to_thrift(&result->status);
        result->__set_snapshot_format(request.preferred_snapshot_format);
        result->__set_allow_incremental_clone(true);
        return Status::OK();
    }

    Status release_snapshot(const std::string& host, int32_t port, const std::string& snapshot_path,
                            TAgentResult* result) override {
        auto status = SnapshotManager::instance()->release_snapshot(snapshot_path);
        status.to_thrift(&result->status);
        return Status::OK();
    }
};

RemoteSnapshotClient* local_snapshot_client_for_test() {
    static LocalSnapshotClientForTest client;
    return &client;
}

TBackend local_snapshot_backend_for_test() {
    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(1);
    backend.__set_http_port(1);
    return backend;
}

} // namespace starrocks
