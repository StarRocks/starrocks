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

#include "runtime/hdfs/hdfs_fs_cache.h"

#include <memory>

#include "gutil/strings/substitute.h"
#include "util/hdfs_util.h"

namespace starrocks {

static Status create_hdfs_fs_handle(const std::string& namenode, HdfsFsHandle* handle, const FSOptions& options) {
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
    handle->hdfs_fs = hdfsBuilderConnect(hdfs_builder);
    if (handle->hdfs_fs == nullptr) {
        return Status::InternalError(strings::Substitute("fail to connect hdfs namenode, namenode=$0, err=$1", namenode,
                                                         get_hdfs_err_msg()));
    }
    return Status::OK();
}

Status HdfsFsCache::get_connection(const std::string& namenode, HdfsFsHandle* handle, const FSOptions& options) {
    {
        std::lock_guard<std::mutex> l(_lock);
        std::string id = namenode;
        const THdfsProperties* properties = options.hdfs_properties();
        if (properties != nullptr && properties->__isset.hdfs_username) {
            id += properties->hdfs_username;
        }
        auto it = _cache.find(id);
        if (it != _cache.end()) {
            *handle = it->second;
        } else {
            handle->namenode = namenode;
            RETURN_IF_ERROR(create_hdfs_fs_handle(namenode, handle, options));
            _cache[id] = *handle;
        }
    }
    return Status::OK();
}

} // namespace starrocks
