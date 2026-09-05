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

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"

namespace starrocks::formats {

struct FileStatistics {
    int64_t record_count = 0;
    int64_t file_size = 0;
    std::optional<std::vector<int64_t>> split_offsets;
    std::optional<std::map<int32_t, int64_t>> column_sizes;
    std::optional<std::map<int32_t, int64_t>> value_counts;
    std::optional<std::map<int32_t, int64_t>> null_value_counts;
    std::optional<std::map<int32_t, std::string>> lower_bounds;
    std::optional<std::map<int32_t, std::string>> upper_bounds;
};

struct FileCommitResult {
    Status io_status;
    std::string format;
    FileStatistics file_statistics;
    std::string location;
    std::function<void()> rollback_action;
};

} // namespace starrocks::formats
