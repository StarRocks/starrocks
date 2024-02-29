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

#ifdef USE_STAROS

#include "storage/lake/staros_cache_stats_collector.h"

#include "fs/fs.h"

namespace starrocks::lake {

StatusOr<int64_t> calculate_cache_size(std::vector<std::string> paths) {
    if (paths.empty()) {
        return 0;
    }

    // REQUIRE: All files in |paths| have the same file system scheme.
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(paths[0]));

    int64_t total_size = 0;
    for (auto path : paths) {
        auto size_st = fs->calculate_cache_size(path);
        if (size_st.ok()) {
            total_size += size_st.value();
        }
    }
    return total_size;
}
} // namespace starrocks::lake
#endif