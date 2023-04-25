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

#include "fs/hdfs/hdfs_fs_cache.h"

#include <memory>

#include "gutil/strings/substitute.h"
#include "util/hdfs_util.h"

namespace starrocks {

// Try to get azure cloud properties from FSOptions, if azure credentials not existed, return nullptr.
// TODO(SmithCruise): Should remove when using azure cpp sdk
static const std::vector<TCloudProperty>* get_azure_cloud_properties(const FSOptions& options) {
    const TCloudConfiguration* cloud_configuration = nullptr;
    if (options.cloud_configuration != nullptr) {
        // This branch is used by data lake
        cloud_configuration = options.cloud_configuration;
    } else if (options.hdfs_properties() != nullptr && options.hdfs_properties()->__isset.cloud_configuration) {
        // This branch is used by broker load
        cloud_configuration = &options.hdfs_properties()->cloud_configuration;
    }
    if (cloud_configuration != nullptr && cloud_configuration->cloud_type == TCloudType::AZURE) {
        return &cloud_configuration->cloud_properties;
    }
    return nullptr;
}

static Status create_hdfs_fs_handle(const std::string& namenode, std::shared_ptr<HdfsFsClient> hdfs_client,
                                    const FSOptions& options) {
    auto hdfs_builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());
    const THdfsProperties* properties = options.hdfs_properties();
    if (properties != nullptr) {
        if (properties->__isset.hdfs_username) {
            hdfsBuilderSetUserName(hdfs_builder, properties->hdfs_username.data());
        }
        if (properties->__isset.disable_cache && properties->disable_cache) {
            hdfsBuilderSetForceNewInstance(hdfs_builder);
        }
    }

    // Insert azure cloud credential into Hadoop configuration
    // TODO(SmithCruise): Should remove when using azure cpp sdk
    const std::vector<TCloudProperty>* azure_cloud_properties = get_azure_cloud_properties(options);
    if (azure_cloud_properties != nullptr) {
        for (const auto& cloud_property : *azure_cloud_properties) {
            hdfsBuilderConfSetStr(hdfs_builder, cloud_property.key.data(), cloud_property.value.data());
        }
    }
    hdfs_client->hdfs_fs = hdfsBuilderConnect(hdfs_builder);
    if (hdfs_client->hdfs_fs == nullptr) {
        return Status::InternalError(strings::Substitute("fail to connect hdfs namenode, namenode=$0, err=$1", namenode,
                                                         get_hdfs_err_msg()));
    }
    return Status::OK();
}

Status HdfsFsCache::get_connection(const std::string& namenode, std::shared_ptr<HdfsFsClient>& hdfs_client,
                                   const FSOptions& options) {
    std::lock_guard<std::mutex> l(_lock);
    std::string cache_key = namenode;
    const THdfsProperties* properties = options.hdfs_properties();
    if (properties != nullptr && properties->__isset.hdfs_username) {
        cache_key += properties->hdfs_username;
    }

    // Insert azure cloud credential into cache key
    const std::vector<TCloudProperty>* azure_cloud_properties = get_azure_cloud_properties(options);
    if (azure_cloud_properties != nullptr) {
        for (const auto& cloud_property : *azure_cloud_properties) {
            cache_key += cloud_property.key;
            cache_key += cloud_property.value;
        }
    }

    for (size_t idx = 0; idx < _cur_client_idx; idx++) {
        if (_cache_key[idx] == cache_key) {
            hdfs_client = _cache_clients[idx];
            // Found cache client, return directly
            return Status::OK();
        }
    }

    // Not found cached client, create a new one
    hdfs_client = std::make_shared<HdfsFsClient>();
    hdfs_client->namenode = namenode;
    RETURN_IF_ERROR(create_hdfs_fs_handle(namenode, hdfs_client, options));
    if (UNLIKELY(_cur_client_idx >= _max_cache_clients)) {
        uint32_t idx = _rand.Uniform(_max_cache_clients);
        _cache_key[idx] = cache_key;
        _cache_clients[idx] = hdfs_client;
    } else {
        _cache_key[_cur_client_idx] = cache_key;
        _cache_clients[_cur_client_idx] = hdfs_client;
        _cur_client_idx++;
    }
    return Status::OK();
}

} // namespace starrocks
