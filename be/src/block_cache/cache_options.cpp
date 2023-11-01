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

#include "block_cache/cache_options.h"

#include <filesystem>

#include "common/logging.h"
#include "util/parse_util.h"

namespace starrocks {

int64_t parse_mem_size(const std::string& mem_size_str, int64_t mem_limit) {
    return ParseUtil::parse_mem_spec(mem_size_str, mem_limit);
}

int64_t parse_disk_size(const std::string& disk_path, const std::string& disk_size_str, int64_t disk_limit) {
    if (disk_limit == -1) {
        std::filesystem::path dpath(disk_path);
        // The datacache directory may be created automatically later.
        if (!std::filesystem::exists(dpath)) {
            if (!dpath.has_parent_path()) {
                LOG(ERROR) << "invalid disk path for datacache, disk_path: " << disk_path;
                return -1;
            }
            dpath = dpath.parent_path();
        }

        std::error_code ec;
        auto space_info = std::filesystem::space(dpath, ec);
        if (ec) {
            LOG(ERROR) << "fail to get disk space info, path: " << dpath << ", error: " << ec.message();
            return -1;
        }
        disk_limit = space_info.capacity;
    }
    return ParseUtil::parse_mem_spec(disk_size_str, disk_limit);
}

} // namespace starrocks
