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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/options.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/options.h"

#include <algorithm>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "fs/fs.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "util/path_util.h"

namespace starrocks {

using std::string;
using std::vector;

// capacity config is deprecated
static std::string CAPACITY_UC = "CAPACITY";
static std::string MEDIUM_UC = "MEDIUM";
static std::string SSD_UC = "SSD";
static std::string HDD_UC = "HDD";

// TODO: should be a general util method
static std::string to_upper(const std::string& str) {
    std::string out = str;
    std::transform(out.begin(), out.end(), out.begin(), ::toupper);
    return out;
}

// format: /data,medium:ssd
// deprecated format: /data,capacity:50 or /data,50
Status parse_root_path(const string& root_path, StorePath* path) {
    std::vector<string> tmp_vec = strings::Split(root_path, ",", strings::SkipWhitespace());

    // parse root path name
    StripWhiteSpace(&tmp_vec[0]);
    tmp_vec[0].erase(tmp_vec[0].find_last_not_of('/') + 1);
    if (tmp_vec[0].empty() || tmp_vec[0][0] != '/') {
        LOG(WARNING) << "invalid store path. path=" << tmp_vec[0];
        return Status::InvalidArgument("Invalid store path");
    }

    string canonicalized_path;
    Status status = FileSystem::Default()->canonicalize(tmp_vec[0], &canonicalized_path);
    if (!status.ok()) {
        LOG(WARNING) << "path can not be canonicalized. may be not exist. path=" << tmp_vec[0];
        return status;
    }
    path->path = tmp_vec[0];

    // parse root path storage medium
    string medium_str = HDD_UC;

    string extension = path_util::file_extension(canonicalized_path);
    if (!extension.empty()) {
        medium_str = to_upper(extension.substr(1));
    }

    for (int i = 1; i < tmp_vec.size(); i++) {
        // <property>:<value> or <value>
        string property;
        string value;
        std::pair<string, string> pair = strings::Split(tmp_vec[i], strings::delimiter::Limit(":", 1));
        if (pair.second.empty()) {
            LOG(WARNING) << "invalid property of store path, " << tmp_vec[i];
            return Status::InvalidArgument(strings::Substitute("invalid property of store path, $0", tmp_vec[i]));
        } else {
            // format_2
            property = to_upper(pair.first);
            value = pair.second;
        }

        StripWhiteSpace(&property);
        StripWhiteSpace(&value);
        if (property == CAPACITY_UC) {
            // deprecated, do nothing
            // keep this logic to prevent users who use the old config from failing to start after upgrading
        } else if (property == MEDIUM_UC) {
            // property 'medium' has a higher priority than the extension of
            // path, so it can override medium_str
            medium_str = to_upper(value);
        } else {
            LOG(WARNING) << "invalid property of store path, " << tmp_vec[i];
            return Status::InvalidArgument("Invalid property of store path");
        }
    }

    path->storage_medium = TStorageMedium::HDD;
    if (!medium_str.empty()) {
        if (medium_str == SSD_UC) {
            path->storage_medium = TStorageMedium::SSD;
        } else if (medium_str == HDD_UC) {
            path->storage_medium = TStorageMedium::HDD;
        } else {
            LOG(WARNING) << "invalid storage medium. medium=" << medium_str;
            return Status::InternalError("Invalid storage medium");
        }
    }

    return Status::OK();
}

Status parse_conf_store_paths(const string& config_path, std::vector<StorePath>* paths) {
    std::vector<string> path_vec = strings::Split(config_path, ";", strings::SkipWhitespace());
    for (auto& item : path_vec) {
        StorePath path;
        auto res = parse_root_path(item, &path);
        if (res.ok()) {
            paths->emplace_back(std::move(path));
        } else {
            LOG(WARNING) << "failed to parse store path " << item << ", res=" << res;
        }
    }
    if (paths->empty() || (path_vec.size() != paths->size() && !config::ignore_broken_disk)) {
        LOG(WARNING) << "fail to parse storage_root_path config. value=[" << config_path << "]";
        return Status::InvalidArgument("Fail to parse storage_root_path");
    }
    return Status::OK();
}

} // end namespace starrocks
