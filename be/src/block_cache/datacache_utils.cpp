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

#include "block_cache/datacache_utils.h"

namespace starrocks {
void DataCacheUtils::set_metrics_from_thrift(TDataCacheMetrics& t_metrics, const DataCacheMetrics& metrics) {
    switch (metrics.status) {
    case starcache::CacheStatus::NORMAL:
        t_metrics.__set_status(TDataCacheStatus::NORMAL);
        break;
    case starcache::CacheStatus::UPDATING:
        t_metrics.__set_status(TDataCacheStatus::UPDATING);
        break;
    case starcache::CacheStatus::LOADING:
        t_metrics.__set_status(TDataCacheStatus::LOADING);
        break;
    default:
        t_metrics.__set_status(TDataCacheStatus::ABNORMAL);
    }

    t_metrics.__set_disk_quota_bytes(metrics.disk_quota_bytes);
    t_metrics.__set_disk_used_bytes(metrics.disk_used_bytes);
    t_metrics.__set_mem_quota_bytes(metrics.mem_quota_bytes);
    t_metrics.__set_mem_used_bytes(metrics.mem_used_bytes);
}

} // namespace starrocks