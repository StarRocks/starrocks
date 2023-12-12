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

#include "storage/file_stream_converter.h"
#include "storage/storage_engine.h"

namespace starrocks {

class TabletSchemaPB;
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
                                           TSchemaHash remote_schema_hash,
                                           const std::function<StatusOr<std::unique_ptr<FileStreamConverter>>(
                                                   const std::string& file_name, uint64_t file_size)>& file_converter,
                                           DataDir* data_dir = nullptr);

    static StatusOr<std::string> download_remote_snapshot_file(const std::string& host, int32_t http_port,
                                                               const std::string& remote_token,
                                                               const std::string& remote_snapshot_path,
                                                               TTabletId remote_tablet_id,
                                                               TSchemaHash remote_schema_hash,
                                                               const std::string& file_name, uint64_t timeout_sec);

    static void calc_column_unique_id_map(const TabletSchemaPB& source_schema, const TabletSchemaPB& target_schema,
                                          std::unordered_map<uint32_t, uint32_t>* column_unique_id_map);

    static void calc_column_unique_id_map(const TabletSchema& source_schema,
                                          const TabletSchema& target_schema,
                                          std::unordered_map<uint32_t, uint32_t>* column_unique_id_map);

    template<typename T>
    static void convert_column_unique_ids(T* columns,
                                          const std::unordered_map<uint32_t, uint32_t>& column_unique_id_map) {
        uint32_t column_unique_id_max_value = -1;
        for (auto& column : *columns) {
            auto iter = column_unique_id_map.find(column.unique_id());
            if (iter == column_unique_id_map.end()) {
                column.set_unique_id(column_unique_id_max_value--);
            } else {
                column.set_unique_id(iter->second);
            }
        }
    }
};

} // namespace starrocks
