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

#include <unordered_map>

#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class TExportSink;
class TUploadReq;
class TDownloadReq;

struct FSOptions {
private:
    FSOptions(const TBrokerScanRangeParams* scan_range_params, const TExportSink* export_sink,
              const THdfsProperties* hdfs_properties, int hdfs_write_buffer_size_kb, const TUploadReq* upload,
              const TDownloadReq* download, const TCloudConfiguration* cloud_configuration,
              std::unordered_map<std::string, std::string> fs_options = {});

public:
    FSOptions();
    FSOptions(const TBrokerScanRangeParams* scan_range_params);
    FSOptions(const TExportSink* export_sink);
    FSOptions(const THdfsProperties* hdfs_properties, int hdfs_write_buffer_size_kb = 0);
    FSOptions(const TUploadReq* upload);
    FSOptions(const TDownloadReq* download);
    FSOptions(const TCloudConfiguration* cloud_configuration);
    FSOptions(const std::unordered_map<std::string, std::string>& fs_options);

    const TBrokerScanRangeParams* scan_range_params;
    const TExportSink* export_sink;
    const THdfsProperties* hdfs_properties;
    const TUploadReq* upload;
    const TDownloadReq* download;
    const TCloudConfiguration* cloud_configuration;
    int hdfs_write_buffer_size_kb;
    const std::unordered_map<std::string, std::string> _fs_options;

    static constexpr const char* FS_S3_ENDPOINT = "fs.s3a.endpoint";
    static constexpr const char* FS_S3_ENDPOINT_REGION = "fs.s3a.endpoint.region";
    static constexpr const char* FS_S3_ACCESS_KEY = "fs.s3a.access.key";
    static constexpr const char* FS_S3_SECRET_KEY = "fs.s3a.secret.key";
    static constexpr const char* FS_S3_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    static constexpr const char* FS_S3_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
    static constexpr const char* FS_S3_READ_AHEAD_RANGE = "fs.s3a.readahead.range";
    static constexpr const char* FS_S3_RETRY_LIMIT = "fs.s3a.retry.limit";
    static constexpr const char* FS_S3_RETRY_INTERVAL = "fs.s3a.retry.interval";
};

} // namespace starrocks
