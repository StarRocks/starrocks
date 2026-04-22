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

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace starrocks::fs {

inline bool starts_with(std::string_view s, std::string_view prefix) {
    return (s.size() >= prefix.size()) && (std::memcmp(s.data(), prefix.data(), prefix.size()) == 0);
}

inline bool is_in_list(std::string_view uri, const std::vector<std::string>& list) {
    for (const auto& item : list) {
        if (starts_with(uri, item)) {
            return true;
        }
    }
    return false;
}

bool is_fallback_to_hadoop_fs(std::string_view uri);

bool is_s3_uri(std::string_view uri);

inline bool is_azblob_uri(std::string_view uri) {
    return starts_with(uri, "wasb://") || starts_with(uri, "wasbs://");
}

inline bool is_azure_uri(std::string_view uri) {
    return starts_with(uri, "wasb://") || starts_with(uri, "wasbs://") || starts_with(uri, "adl://") ||
           starts_with(uri, "abfs://") || starts_with(uri, "abfss://");
}

inline bool is_gcs_uri(std::string_view uri) {
    return starts_with(uri, "gs://");
}

inline bool is_hdfs_uri(std::string_view uri) {
    return starts_with(uri, "hdfs://");
}

inline bool is_posix_uri(std::string_view uri) {
    return (std::memchr(uri.data(), ':', uri.size()) == nullptr) || starts_with(uri, "posix://");
}

} // namespace starrocks::fs
