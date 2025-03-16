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

#include "common/status.h"

namespace starrocks {

enum class DummyCacheStatus { NORMAL, UPDATING, ABNORMAL, LOADING };

struct DummyCacheMetrics {
    struct DirSpace {
        std::string path;
        size_t quota_bytes;
    };
    DummyCacheStatus status;
    int64_t mem_quota_bytes;
    size_t mem_used_bytes;
    size_t disk_quota_bytes;
    size_t disk_used_bytes;
    std::vector<DirSpace> disk_dir_spaces;
    size_t meta_used_bytes = 0;
};

} // namespace starrocks
