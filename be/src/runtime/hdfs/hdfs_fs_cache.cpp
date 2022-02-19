// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "runtime/hdfs/hdfs_fs_cache.h"

#include <memory>
#include <utility>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "object_store/s3_client.h"
#include "util/hdfs_util.h"

namespace starrocks {

static Status create_hdfs_fs_handle(const std::string& namenode, HdfsFsHandle* handle) {
    const char* nn = namenode.c_str();
    if (is_hdfs_path(nn)) {
        handle->type = HdfsFsHandle::Type::HDFS;
        auto hdfs_builder = hdfsNewBuilder();
        hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());
        handle->hdfs_fs = hdfsBuilderConnect(hdfs_builder);
        if (handle->hdfs_fs == nullptr) {
            return Status::InternalError(strings::Substitute("fail to connect hdfs namenode, namenode=$0, err=$1",
                                                             namenode, get_hdfs_err_msg()));
        }

    } else if (is_s3a_path(nn) || is_oss_path(nn)) {
        handle->type = HdfsFsHandle::Type::S3;
        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTP;
        config.endpointOverride = config::aws_s3_endpoint;
        config.maxConnections = config::aws_s3_max_connection;

        S3Credential cred;
        cred.access_key_id = config::aws_access_key_id;
        cred.secret_access_key = config::aws_secret_access_key;

        S3Client* s3_client = new S3Client(config, &cred, false);
        handle->s3_client = s3_client;
    } else {
        return Status::InternalError(strings::Substitute("failed to make client, namenode=$0", namenode));
    }
    return Status::OK();
}

Status HdfsFsCache::get_connection(const std::string& namenode, HdfsFsHandle* handle) {
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _cache.find(namenode);
        if (it != _cache.end()) {
            *handle = it->second;
        } else {
            handle->namenode = namenode;
            RETURN_IF_ERROR(create_hdfs_fs_handle(namenode, handle));
            _cache[namenode] = *handle;
        }
    }
    return Status::OK();
}

} // namespace starrocks
