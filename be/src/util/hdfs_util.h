// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "common/status.h"

namespace starrocks {

std::string get_hdfs_err_msg();

Status get_namenode_from_path(const std::string& path, std::string* namenode);

} // namespace starrocks
