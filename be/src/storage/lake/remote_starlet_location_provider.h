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

#ifdef USE_STAROS
#pragma once

#include <set>
#include <string>

#include "gutil/macros.h"
#include "storage/lake/filenames.h"
#include "storage/lake/starlet_location_provider.h"

namespace starrocks::lake {

// Used to access locations on remote cluster storage
class RemoteStarletLocationProvider : public StarletLocationProvider {
public:
    RemoteStarletLocationProvider() = default;
    ~RemoteStarletLocationProvider() override = default;

    DISALLOW_COPY_AND_MOVE(RemoteStarletLocationProvider);

    // build metadata root location using current rule
    // returned value has following format: `staros://{tablet_id}/db{db_id}/{table_id}/{partition_id}/meta`
    std::string metadata_root_location(int64_t tablet_id, int64_t db_id, int64_t table_id, int64_t partition_id) {
        std::string partition_path = fmt::format("db{}/{}/{}", db_id, table_id, partition_id);
        std::string custom_root_location = join_path(root_location(tablet_id), partition_path);
        return join_path(custom_root_location, kMetadataDirectoryName);
    }

    // build metadata root location using current rule
    // returned value has following format: `staros://{tablet_id}/db{db_id}/{table_id}/{partition_id}/data`
    std::string segment_root_location(int64_t tablet_id, int64_t db_id, int64_t table_id, int64_t partition_id) {
        std::string partition_path = fmt::format("db{}/{}/{}", db_id, table_id, partition_id);
        std::string custom_root_location = join_path(root_location(tablet_id), partition_path);
        return join_path(custom_root_location, kSegmentDirectoryName);
    }
};

} // namespace starrocks::lake
#endif // USE_STAROS