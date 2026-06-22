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
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

struct TabletSnapshotInfo {
    int64_t db_id;
    int64_t table_id;
    int64_t partition_id;
    int64_t physical_partition_id;
    int64_t dest_tablet_id;
    const TabletDataSnapshotPB* tablet_snapshot;
};

class SnapshotFileSyncer {
public:
    SnapshotFileSyncer() = default;
    ~SnapshotFileSyncer() = default;

    Status upload(const TabletSnapshotInfo& snapshot_info, UploadSnapshotFilesResponsePB* response);
    Status delete_partition(int64_t tablet_id, int64_t db_id, int64_t table_id, int64_t partition_id,
                            int64_t physical_partition_id);
    Status delete_files(int64_t tablet_id, const ExternalClusterSnapshotLogPB& log_pb);
};

} // namespace starrocks::lake
