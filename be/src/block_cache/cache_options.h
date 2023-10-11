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
#include <vector>

namespace starrocks {

struct DirSpace {
    std::string path;
    size_t size;
};

struct CacheOptions {
    // basic
    size_t mem_space_size;
    std::vector<DirSpace> disk_spaces;
    std::string meta_path;

    // advanced
    size_t block_size;
    bool enable_checksum;
    bool enable_direct_io;
    std::string engine;
    size_t max_concurrent_inserts;
    size_t max_flying_memory_mb;
};

int64_t parse_mem_size(const std::string& mem_size_str, int64_t mem_limit = -1);

int64_t parse_disk_size(const std::string& disk_path, const std::string& disk_size_str, int64_t disk_limit = -1);

} // namespace starrocks
