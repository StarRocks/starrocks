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

#include "storage/storage_engine.h"

namespace starrocks {

class ReplicationUtils {
public:
    static Status make_remote_snapshot(const std::string& host, int32_t be_port, TTabletId tablet_id,
                                       TSchemaHash schema_hash, TVersion version, int32_t timeout_s,
                                       const std::vector<Version>* missed_versions,
                                       const std::vector<int64_t>* missing_version_ranges,
                                       std::string* remote_snapshot_path);

    static Status release_remote_snapshot(const std::string& host, int32_t be_port,
                                          const std::string& remote_snapshot_path);

    static Status download_remote_snapshot(const std::string& host, int32_t http_port, const std::string& remote_token,
                                           const std::string& remote_snapshot_path, TTabletId remote_tablet_id,
                                           TSchemaHash remote_schema_hash, DataDir* data_dir,
                                           const std::string& local_path_prefix,
                                           const std::function<std::string(const std::string&)>& name_converter =
                                                   std::function<std::string(const std::string&)>());

    static StatusOr<std::string> download_remote_snapshot_file(const std::string& host, int32_t http_port,
                                                               const std::string& remote_token,
                                                               const std::string& remote_snapshot_path,
                                                               TTabletId remote_tablet_id,
                                                               TSchemaHash remote_schema_hash,
                                                               const std::string& file_name, uint64_t timeout_sec);
};

} // namespace starrocks
