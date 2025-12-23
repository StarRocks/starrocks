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

#ifdef USE_STAROS

#include <algorithm>
#include <set>
#include <string>

#include "gutil/macros.h"
#include "storage/lake/filenames.h"
#include "storage/lake/starlet_location_provider.h"

namespace starrocks::lake {

// Configuration for partitioned prefix feature
struct PartitionedPrefixConfig {
    bool enable_partitioned_prefix = false;
    int32_t num_partitioned_prefix = 0;
};

// Used to access locations on remote cluster storage
class RemoteStarletLocationProvider : public StarletLocationProvider {
public:
    RemoteStarletLocationProvider() = default;
    ~RemoteStarletLocationProvider() override = default;

    DISALLOW_COPY_AND_MOVE(RemoteStarletLocationProvider);

    // build metadata root location using current rule
    // returned value has following format: `staros://{tablet_id}/db{db_id}/{table_id}/{partition_id}/meta`
    // When partitioned prefix is enabled, format becomes:
    // `staros://{tablet_id}/{prefix}/db{db_id}/{table_id}/{partition_id}/meta`
    std::string metadata_root_location(int64_t tablet_id, int64_t db_id, int64_t table_id, int64_t partition_id,
                                       const PartitionedPrefixConfig& prefix_config = {}) {
        std::string partition_path = fmt::format("db{}/{}/{}", db_id, table_id, partition_id);
        std::string custom_root_location = join_path(root_location(tablet_id), partition_path);

        if (prefix_config.enable_partitioned_prefix && prefix_config.num_partitioned_prefix > 0) {
            custom_root_location = add_partitioned_prefix(root_location(tablet_id), partition_path, partition_id,
                                                          prefix_config.num_partitioned_prefix);
        }

        return join_path(custom_root_location, kMetadataDirectoryName);
    }

    // build segment root location using current rule
    // returned value has following format: `staros://{tablet_id}/db{db_id}/{table_id}/{partition_id}/data`
    // When partitioned prefix is enabled, format becomes:
    // `staros://{tablet_id}/{prefix}/db{db_id}/{table_id}/{partition_id}/data`
    std::string segment_root_location(int64_t tablet_id, int64_t db_id, int64_t table_id, int64_t partition_id,
                                      const PartitionedPrefixConfig& prefix_config = {}) {
        std::string partition_path = fmt::format("db{}/{}/{}", db_id, table_id, partition_id);
        std::string custom_root_location = join_path(root_location(tablet_id), partition_path);

        if (prefix_config.enable_partitioned_prefix && prefix_config.num_partitioned_prefix > 0) {
            custom_root_location = add_partitioned_prefix(root_location(tablet_id), partition_path, partition_id,
                                                          prefix_config.num_partitioned_prefix);
        }

        return join_path(custom_root_location, kSegmentDirectoryName);
    }

    // Calculate partitioned prefix string from partition_id
    // This matches the algorithm used in StarClient.allocateFilePath:
    // 1. Calculate: Long.hashCode(partition_id) % num_partitioned_prefix
    // 2. Convert result to lowercase hexadecimal string
    // 3. Reverse the hexadecimal string
    // Example: partitionId=10086, numPrefix=1024 -> 10086%1024=870 -> hex="366" -> reverse="663"
    static std::string calculate_partitioned_prefix(int64_t partition_id, int32_t num_partitioned_prefix) {
        // Use Long.hashCode() equivalent: (int)(partition_id ^ (partition_id >>> 32))
        int32_t hash_code = static_cast<int32_t>(partition_id ^ (static_cast<uint64_t>(partition_id) >> 32));
        int32_t prefix_num = std::abs(hash_code % num_partitioned_prefix);
        // Convert to lowercase hexadecimal string (without "0x" prefix)
        std::string prefix = fmt::format("{:x}", prefix_num);
        std::reverse(prefix.begin(), prefix.end());
        return prefix;
    }

private:
    // Add partitioned prefix to the path
    // Input: root_location = "staros://{tablet_id}", partition_path = "db{db_id}/{table_id}/{partition_id}"
    // Output: "staros://{tablet_id}/{prefix}/db{db_id}/{table_id}/{partition_id}"
    std::string add_partitioned_prefix(const std::string& root_loc, const std::string& partition_path,
                                       int64_t partition_id, int32_t num_partitioned_prefix) {
        std::string prefix = calculate_partitioned_prefix(partition_id, num_partitioned_prefix);
        return join_path(join_path(root_loc, prefix), partition_path);
    }
};

} // namespace starrocks::lake
#endif // USE_STAROS