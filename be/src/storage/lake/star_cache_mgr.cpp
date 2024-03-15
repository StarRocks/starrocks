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

#include "storage/lake/star_cache_mgr.h"

#include <fslib/cache_stats_collector.h>
#include <starlet.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "fs/fs_util.h"
#include "service/staros_worker.h"

namespace starrocks::lake {

using CacheStatCollector = staros::starlet::fslib::CacheStatCollector;

StatusOr<int64_t> calculate_cache_size(const std::string& path, int64_t file_size) {
    ASSIGN_OR_RETURN(auto pair, starrocks::fs::parse_starlet_uri(path));
    auto shard_info = g_worker->retrieve_shard_info(pair.second);
    if (!shard_info.ok()) {
        LOG(WARNING) << "Retrive shard info failed for file path: " << path
                     << ", error msg: " << shard_info.status().ToString();
        return to_status(shard_info.status());
    }

    auto collector = CacheStatCollector::instance();
    absl::StatusOr<int64_t> size_st = collector->collect_cache_size(shard_info.value(), pair.first, file_size);
    if (size_st.ok()) {
        return size_st.value();
    } else {
        return to_status(size_st.status());
    }
}
} // namespace starrocks::lake
#endif