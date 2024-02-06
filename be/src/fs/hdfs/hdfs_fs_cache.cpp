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

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "udf/java/java_udf.h"
#include "util/hdfs_util.h"

namespace starrocks {

// Try to get cloud properties from FSOptions, if cloud configuration not existed, return nullptr.
// TODO(SmithCruise): Should remove when using cpp sdk
static const std::vector<TCloudProperty>* get_cloud_properties(const FSOptions& options) {
    const TCloudConfiguration* cloud_configuration = nullptr;
    if (options.cloud_configuration != nullptr) {
        // This branch is used by data lake
        cloud_configuration = options.cloud_configuration;
    } else if (options.hdfs_properties() != nullptr && options.hdfs_properties()->__isset.cloud_configuration) {
        // This branch is used by broker load
        cloud_configuration = &options.hdfs_properties()->cloud_configuration;
    }
    if (cloud_configuration != nullptr) {
        return &cloud_configuration->cloud_properties;
    }
    return nullptr;
}

static Status create_hdfs_fs_handle(const std::string& namenode, std::shared_ptr<HdfsFsClient> hdfs_client,
                                    const FSOptions& options) {
    RETURN_IF_ERROR(detect_java_runtime());
    auto hdfs_builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());
    const THdfsProperties* properties = options.hdfs_properties();
    if (properties != nullptr) {
        if (properties->__isset.hdfs_username) {
            hdfsBuilderSetUserName(hdfs_builder, properties->hdfs_username.data());
        }
    }
    hdfsBuilderSetForceNewInstance(hdfs_builder);

    // Insert cloud properties(key-value paired) into Hadoop configuration
    // TODO(SmithCruise): Should remove when using cpp sdk
    const std::vector<TCloudProperty>* cloud_properties = get_cloud_properties(options);
    if (cloud_properties != nullptr) {
        for (const auto& cloud_property : *cloud_properties) {
            hdfsBuilderConfSetStr(hdfs_builder, cloud_property.key.data(), cloud_property.value.data());
        }
    }

    // Set for hdfs client hedged read
    std::string hedged_read_threadpool_size = std::to_string(config::hdfs_client_hedged_read_threadpool_size);
    std::string hedged_read_threshold_millis = std::to_string(config::hdfs_client_hedged_read_threshold_millis);
    if (config::hdfs_client_enable_hedged_read) {
        hdfsBuilderConfSetStr(hdfs_builder, "dfs.client.hedged.read.threadpool.size",
                              hedged_read_threadpool_size.data());
        hdfsBuilderConfSetStr(hdfs_builder, "dfs.client.hedged.read.threshold.millis",
                              hedged_read_threshold_millis.data());
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
    std::string cache_key = namenode;
    const THdfsProperties* properties = options.hdfs_properties();
    if (properties != nullptr && properties->__isset.hdfs_username) {
        cache_key += properties->hdfs_username;
    }

    // Insert cloud properties into cache key
    const std::vector<TCloudProperty>* cloud_properties = get_cloud_properties(options);
    if (cloud_properties != nullptr) {
        for (const auto& cloud_property : *cloud_properties) {
            cache_key += cloud_property.key;
            cache_key += cloud_property.value;
        }
    }

    std::lock_guard<std::mutex> l(_lock);

    auto it = _cache_clients.find(cache_key);
    if (it != _cache_clients.end()) {
        hdfs_client = it->second;
        // Found a cache client, return directly
        return Status::OK();
    }

    const uint32_t max_cache_clients = config::hdfs_client_max_cache_size;
    // Not found a cached client, create a new one
    hdfs_client = std::make_shared<HdfsFsClient>();
    hdfs_client->namenode = namenode;
    RETURN_IF_ERROR(create_hdfs_fs_handle(namenode, hdfs_client, options));
    if (UNLIKELY(_cache_keys.size() >= max_cache_clients)) {
        uint32_t idx = _rand.Uniform(max_cache_clients);
        _cache_clients.erase(_cache_keys[idx]);
        _cache_clients[cache_key] = hdfs_client;
        _cache_keys[idx].swap(cache_key);
    } else {
        _cache_clients[cache_key] = hdfs_client;
        _cache_keys.push_back(std::move(cache_key));
    }
    return Status::OK();
}

} // namespace starrocks
