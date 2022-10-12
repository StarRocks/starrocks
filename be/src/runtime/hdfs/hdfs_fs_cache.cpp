// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/hdfs/hdfs_fs_cache.h"

#include <memory>

#include "gutil/strings/substitute.h"
#include "util/hdfs_util.h"

namespace starrocks {

static Status create_hdfs_fs_handle(const std::string& namenode, HdfsFsHandle* handle, const FSOptions& options) {
    handle->type = HdfsFsHandle::Type::HDFS;
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
