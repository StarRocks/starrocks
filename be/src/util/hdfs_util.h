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

#pragma once

#include <string>

#include "common/status.h"

namespace starrocks {

std::string get_hdfs_err_msg();

Status get_namenode_from_path(const std::string& path, std::string* namenode);
std::string get_bucket_from_namenode(const std::string& namenode);
std::string get_endpoint_from_oss_bucket(const std::string& default_bucket, std::string* bucket);

// Returns true if the path refers to a location on an HDFS filesystem.
bool is_hdfs_path(const char* path);

// Returns true if the path refers to a location on object storage filesystem.
bool is_object_storage_path(const char* path);
bool is_s3a_path(const char* path);
bool is_oss_path(const char* path);

} // namespace starrocks
