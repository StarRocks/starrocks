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

#include "block_cache/kv_cache.h"
#include "gen_cpp/DataCache_types.h"

namespace starrocks {

class DataCacheUtils {
public:
    static void set_metrics_from_thrift(TDataCacheMetrics& t_metrics, const DataCacheMetrics& metrics);

    static Status parse_conf_datacache_mem_size(const std::string& conf_mem_size_str, int64_t mem_limit,
                                                size_t* mem_size);

    static int64_t parse_conf_datacache_disk_size(const std::string& disk_path, const std::string& disk_size_str,
                                                  int64_t disk_limit);

    static Status parse_conf_datacache_disk_paths(const std::string& config_path, std::vector<std::string>* paths,
                                                  bool ignore_broken_disk);

    static Status parse_conf_datacache_disk_spaces(const std::string& config_disk_path,
                                                   const std::string& config_disk_size, bool ignore_broken_disk,
                                                   std::vector<DirSpace>* disk_spaces);

    static void clean_residual_datacache(const std::string& disk_path);
};

} // namespace starrocks