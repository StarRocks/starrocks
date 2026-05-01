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

#include "fs/fs_options_helper.h"

#include "exec/data_sinks/file_result_writer.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/DataSinks_types.h"

namespace starrocks {

const THdfsProperties* FSOptionsHelper::hdfs_properties(const FSOptions& options) {
    if (options.scan_range_params != nullptr && options.scan_range_params->__isset.hdfs_properties) {
        return &options.scan_range_params->hdfs_properties;
    } else if (options.export_sink != nullptr && options.export_sink->__isset.hdfs_properties) {
        return &options.export_sink->hdfs_properties;
    } else if (options.result_file_options != nullptr) {
        return &options.result_file_options->hdfs_properties;
    } else if (options.upload != nullptr && options.upload->__isset.hdfs_properties) {
        return &options.upload->hdfs_properties;
    } else if (options.download != nullptr && options.download->__isset.hdfs_properties) {
        return &options.download->hdfs_properties;
    }
    return nullptr;
}

const TCloudConfiguration* FSOptionsHelper::cloud_configuration(const FSOptions& options) {
    if (options.cloud_configuration != nullptr) {
        return options.cloud_configuration;
    }

    const THdfsProperties* t_hdfs_properties = FSOptionsHelper::hdfs_properties(options);
    if (t_hdfs_properties != nullptr) {
        return &(t_hdfs_properties->cloud_configuration);
    }

    return nullptr;
}

bool FSOptionsHelper::azure_use_native_sdk(const FSOptions& options) {
#ifndef __APPLE__
    const auto* t_cloud_configuration = FSOptionsHelper::cloud_configuration(options);
    if (t_cloud_configuration == nullptr) {
        return false;
    }
    return t_cloud_configuration->__isset.azure_use_native_sdk && t_cloud_configuration->azure_use_native_sdk;
#else
    return false;
#endif
}

} // namespace starrocks
