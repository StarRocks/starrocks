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

#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"

namespace starrocks {

class RemoteSnapshotClient {
public:
    virtual ~RemoteSnapshotClient() = default;

    virtual Status make_snapshot(const std::string& host, int32_t port, const TSnapshotRequest& request,
                                 TAgentResult* result) = 0;

    virtual Status release_snapshot(const std::string& host, int32_t port, const std::string& snapshot_path,
                                    TAgentResult* result) = 0;
};

RemoteSnapshotClient* default_remote_snapshot_client();

} // namespace starrocks
