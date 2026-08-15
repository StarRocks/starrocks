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

#include "fs/fs_options.h"

#include <utility>

namespace starrocks {

FSOptions::FSOptions(const TBrokerScanRangeParams* scan_range_params, const TExportSink* export_sink,
                     const THdfsProperties* hdfs_properties, int hdfs_write_buffer_size_kb, const TUploadReq* upload,
                     const TDownloadReq* download, const TCloudConfiguration* cloud_configuration,
                     std::unordered_map<std::string, std::string> fs_options)
        : scan_range_params(scan_range_params),
          export_sink(export_sink),
          hdfs_properties(hdfs_properties),
          upload(upload),
          download(download),
          cloud_configuration(cloud_configuration),
          hdfs_write_buffer_size_kb(hdfs_write_buffer_size_kb),
          _fs_options(std::move(fs_options)) {}

FSOptions::FSOptions() : FSOptions(nullptr, nullptr, nullptr, 0, nullptr, nullptr, nullptr) {}

FSOptions::FSOptions(const TBrokerScanRangeParams* scan_range_params)
        : FSOptions(scan_range_params, nullptr, nullptr, 0, nullptr, nullptr, nullptr) {}

FSOptions::FSOptions(const TExportSink* export_sink)
        : FSOptions(nullptr, export_sink, nullptr, 0, nullptr, nullptr, nullptr) {}

FSOptions::FSOptions(const THdfsProperties* hdfs_properties, int hdfs_write_buffer_size_kb)
        : FSOptions(nullptr, nullptr, hdfs_properties, hdfs_write_buffer_size_kb, nullptr, nullptr, nullptr) {}

FSOptions::FSOptions(const TUploadReq* upload) : FSOptions(nullptr, nullptr, nullptr, 0, upload, nullptr, nullptr) {}

FSOptions::FSOptions(const TDownloadReq* download)
        : FSOptions(nullptr, nullptr, nullptr, 0, nullptr, download, nullptr) {}

FSOptions::FSOptions(const TCloudConfiguration* cloud_configuration)
        : FSOptions(nullptr, nullptr, nullptr, 0, nullptr, nullptr, cloud_configuration) {}

FSOptions::FSOptions(const std::unordered_map<std::string, std::string>& fs_options)
        : FSOptions(nullptr, nullptr, nullptr, 0, nullptr, nullptr, nullptr, fs_options) {}

} // namespace starrocks
