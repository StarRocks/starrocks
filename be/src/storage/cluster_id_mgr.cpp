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

#include "cluster_id_mgr.h"

#include <sys/file.h>
#include <sys/stat.h>

#include <fstream>
#include <utility>

#include "common/version.h"
#include "gutil/strings/substitute.h"
#include "util/defer_op.h"
#include "util/errno.h"

namespace starrocks {

ClusterIdMgr::ClusterIdMgr(std::string path) : _path(std::move(path)) {}

Status ClusterIdMgr::init() {
    std::string cluster_id_path = _cluster_id_path();
    if (access(cluster_id_path.c_str(), F_OK) != 0) {
        int fd = open(cluster_id_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fd < 0 || close(fd) < 0) {
            RETURN_IF_ERROR_WITH_WARN(Status::IOError(strings::Substitute("failed to create cluster id file $0, err=$1",
                                                                          cluster_id_path, errno_to_string(errno))),
                                      "create file failed");
        }
    }

    // obtain lock of all cluster id paths
    FILE* fp = nullptr;
    fp = fopen(cluster_id_path.c_str(), "r+b");
    if (fp == nullptr) {
        RETURN_IF_ERROR_WITH_WARN(
                Status::IOError(strings::Substitute("failed to open cluster id file $0", cluster_id_path)),
                "open file failed");
    }

    DeferOp close_fp([&]() {
        fclose(fp);
        fp = nullptr;
    });

    int lock_res = flock(fp->_fileno, LOCK_EX | LOCK_NB);
    if (lock_res < 0) {
        RETURN_IF_ERROR_WITH_WARN(
                Status::IOError(strings::Substitute("failed to flock cluster id file $0", cluster_id_path)),
                "flock file failed");
    }

    // obtain cluster id of all root paths
    auto st = _read_cluster_id(cluster_id_path, &_cluster_id);
    return st;
}

Status ClusterIdMgr::_read_cluster_id(const std::string& path, int32_t* cluster_id) {
    RETURN_IF_ERROR(_add_version_info_to_cluster_id(path));

    std::fstream fs(path.c_str(), std::fstream::in);
    if (!fs.is_open()) {
        RETURN_IF_ERROR_WITH_WARN(Status::IOError(strings::Substitute("failed to open cluster id file $0", path)),
                                  "open file failed");
    }

    std::string cluster_id_str;
    fs >> cluster_id_str;
    fs.close();
    int32_t tmp_cluster_id = -1;
    if (!cluster_id_str.empty()) {
        size_t pos = cluster_id_str.find('-');
        if (pos != std::string::npos) {
            tmp_cluster_id = std::stoi(cluster_id_str.substr(0, pos));
        } else {
            tmp_cluster_id = std::stoi(cluster_id_str);
        }
    }

    if (tmp_cluster_id == -1 && (fs.rdstate() & std::fstream::eofbit) != 0) {
        *cluster_id = -1;
    } else if (tmp_cluster_id >= 0 && (fs.rdstate() & std::fstream::eofbit) != 0) {
        *cluster_id = tmp_cluster_id;
    } else {
        RETURN_IF_ERROR_WITH_WARN(Status::Corruption(strings::Substitute(
                                          "cluster id file $0 is corrupt. [id=$1 eofbit=$2 failbit=$3 badbit=$4]", path,
                                          tmp_cluster_id, fs.rdstate() & std::fstream::eofbit,
                                          fs.rdstate() & std::fstream::failbit, fs.rdstate() & std::fstream::badbit)),
                                  "file content is error");
    }
    return Status::OK();
}

Status ClusterIdMgr::set_cluster_id(int32_t cluster_id) {
    if (_cluster_id != -1) {
        if (_cluster_id == cluster_id) {
            return Status::OK();
        }
        LOG(ERROR) << "going to set cluster id to already assigned store, cluster_id=" << _cluster_id
                   << ", new_cluster_id=" << cluster_id;
        return Status::InternalError("going to set cluster id to already assigned store");
    }
    std::string cluster_id_path = _cluster_id_path();
    std::fstream fs(cluster_id_path.c_str(), std::fstream::out);
    if (!fs.is_open()) {
        LOG(WARNING) << "fail to open cluster id path. path=" << cluster_id_path;
        return Status::InternalError("IO Error");
    }
    fs << cluster_id;
    fs << "-" << std::string(STARROCKS_VERSION);
    fs.close();
    return Status::OK();
}

// This function is to add version info into file named cluster_id
// This feacture is used to restrict the degrading from StarRocks-1.17.2 to lower version
// Because the StarRocks with lower version cannot read the file written by RocksDB-6.22.1
// This feature takes into effect after StarRocks-1.17.2
// Without this feature, staring BE will be failed
Status ClusterIdMgr::_add_version_info_to_cluster_id(const std::string& path) {
    std::fstream in_fs(path.c_str(), std::fstream::in);
    if (!in_fs.is_open()) {
        RETURN_IF_ERROR_WITH_WARN(Status::IOError(strings::Substitute("failed to open cluster id file $0", path)),
                                  "open file failed");
    }
    std::string cluster_id_str;
    in_fs >> cluster_id_str;
    in_fs.close();

    if (cluster_id_str.empty() || cluster_id_str.find('-') != std::string::npos) {
        return Status::OK();
    }

    std::fstream out_fs(path.c_str(), std::fstream::out);
    if (!out_fs.is_open()) {
        RETURN_IF_ERROR_WITH_WARN(Status::IOError(strings::Substitute("failed to open cluster id file $0", path)),
                                  "open file failed");
    }
    out_fs << cluster_id_str;
    out_fs << "-" << std::string(STARROCKS_VERSION);
    out_fs.close();
    return Status::OK();
}

} // namespace starrocks
