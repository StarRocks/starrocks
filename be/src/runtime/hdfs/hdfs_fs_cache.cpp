// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/hdfs/hdfs_fs_cache.h"

#include <memory>
#include <utility>

#include "gutil/strings/substitute.h"
#include "util/hdfs_util.h"

namespace starrocks {

static Status parse_namenode(const std::string& path, std::string* namenode) {
    const std::string local_fs("file:/");
    size_t n = path.find("://");

    if (n == std::string::npos) {
        if (path.compare(0, local_fs.length(), local_fs) == 0) {
            // Hadoop Path routines strip out consecutive /'s, so recognize 'file:/blah'.
            *namenode = "file:///";
        } else {
            // Path is not qualified, so use the default FS.
            *namenode = "default";
        }
    } else if (n == 0) {
        return Status::InvalidArgument(strings::Substitute("Path missing scheme: $0", path));
    } else {
        // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
        // "scheme://authority/".
        n = path.find('/', n + 3);
        if (n == std::string::npos) {
            return Status::InvalidArgument(strings::Substitute("Path missing '/' after authority: $0", path));
        } else {
            // Include the trailing '/' for local filesystem case, i.e. "file:///".
            *namenode = path.substr(0, n + 1);
        }
    }
    return Status::OK();
}

Status HdfsFsCache::get_connection(const std::string& path, hdfsFS* fs, FileOpenLimitPtr* res, HdfsFsMap* local_cache) {
    std::string namenode;
    RETURN_IF_ERROR(parse_namenode(path, &namenode));

    if (local_cache != nullptr) {
        auto it = local_cache->find(namenode);
        if (it != local_cache->end()) {
            *fs = it->second.first;
            *res = it->second.second.get();
            return Status::OK();
        }
    }

    {
        std::lock_guard<std::mutex> l(_lock);

        auto it = _cache.find(namenode);
        if (it != _cache.end()) {
            *fs = it->second.first;
            *res = it->second.second.get();
        } else {
            auto hdfs_builder = hdfsNewBuilder();
            hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());
            auto semaphore = std::make_unique<FileOpenLimit>(0);
            *fs = hdfsBuilderConnect(hdfs_builder);
            *res = semaphore.get();
            if (*fs == nullptr) {
                return Status::InternalError(strings::Substitute("fail to connect hdfs namenode, name=$0, err=$1",
                                                                 namenode, get_hdfs_err_msg()));
            }
            _cache[namenode] = std::make_pair(*fs, std::move(semaphore));
        }
    }

    if (local_cache != nullptr) {
        local_cache->emplace(namenode, std::make_pair(*fs, std::make_unique<FileOpenLimit>(0)));
    }

    return Status::OK();
}

} // namespace starrocks
