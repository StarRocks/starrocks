// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "common/status.h"

namespace starrocks {

class DownloadUtil {
public:
    static Status download(const std::string& url, const std::string& tmp_file, const std::string& target_file,
                           const std::string& expected_checksum);
};
} // namespace starrocks