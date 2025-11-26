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

#include "common/statusor.h"
#include "gen_cpp/lake_service.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

struct TabletSnapshotInfo {
    int64_t db_id;
    int64_t table_id;
    int64_t partition_id;
    int64_t physical_partition_id;
    int64_t virtual_tablet_id;
    TabletDataSnapshotPB tablet_snapshot;
};

class SnapshotFileSyncer {
public:
    SnapshotFileSyncer(ExecEnv* env) : _env(env) {}
    ~SnapshotFileSyncer() = default;

    Status upload(const TabletSnapshotInfo snapshot_info, UploadSnapshotFilesResponsePB* response);
    /*
    Status download(const TClusterSnapshotRequest& request);
    Status move(const TClusterSnapshotRequest& request);
    Status delete(const TClusterSnapshotRequest& request);
    Status list(const TClusterSnapshotRequest& request);
    Status get(const TClusterSnapshotRequest& request);
    Status set(const TClusterSnapshotRequest& request);
    */

private:
    ExecEnv* _env;
};

} // namespace starrocks::lake