// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "common/status.h"

namespace starrocks {

std::string get_hdfs_err_msg();

Status get_name_node_from_path(const std::string& path, std::string* namenode);

// Returns true if the path refers to a location on an HDFS filesystem.
bool is_hdfs_path(const char* path);

// Returns true if the path refers to a location on object storage filesystem.
bool is_object_storage_path(const char* path);

} // namespace starrocks
