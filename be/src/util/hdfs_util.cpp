// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "util/hdfs_util.h"

#include <common/status.h>
#include <hdfs/hdfs.h>

#include <string>
using std::string;

#include "util/error_util.h"

namespace starrocks {

const char* FILESYS_PREFIX_HDFS = "hdfs://";
const char* FILESYS_PREFIX_S3 = "s3a://";
const char* FILESYS_PREFIX_OSS = "oss://";

std::string get_hdfs_err_msg() {
    std::string error_msg = get_str_err_msg();
    std::stringstream ss;
    ss << "error=" << error_msg;
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        ss << ", root_cause=" << root_cause;
    }
    return ss.str();
}

Status get_name_node_from_path(const std::string& path, std::string* namenode) {
    const string local_fs("file:/");
    size_t n = path.find("://");

    if (n == string::npos) {
        if (path.compare(0, local_fs.length(), local_fs) == 0) {
            // Hadoop Path routines strip out consecutive /'s, so recognize 'file:/blah'.
            *namenode = "file:///";
        } else {
            // Path is not qualified, so use the default FS.
            *namenode = "default";
        }
    } else if (n == 0) {
        return Status::InternalError("Path missing schema");
    } else {
        // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
        // "scheme://authority/".
        n = path.find('/', n + 3);
        if (n == string::npos) {
            return Status::InternalError("Path missing '/' after authority");
        }
        // Include the trailing '/' for local filesystem case, i.e. "file:///".
        *namenode = path.substr(0, n + 1);
    }
    return Status::OK();
}

bool is_specific_path(const char* path, const char* specific_prefix) {
    size_t prefix_len = strlen(specific_prefix);
    return strncmp(path, specific_prefix, prefix_len) == 0;
}

bool is_hdfs_path(const char* path) {
    return is_specific_path(path, FILESYS_PREFIX_HDFS);
}

bool is_s3a_path(const char* path) {
    return is_specific_path(path, FILESYS_PREFIX_S3);
}

bool is_oss_path(const char* path) {
    return is_specific_path(path, FILESYS_PREFIX_OSS);
}

bool is_object_storage_path(const char* path) {
    return (is_oss_path(path) || is_s3a_path(path));
}

} // namespace starrocks
