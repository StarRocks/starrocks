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
#include "gen_cpp/DataCache_types.h"

#ifdef WITH_STARCACHE
#include "starcache/star_cache.h"
#endif

namespace starrocks {

enum class DataCacheStatus { NORMAL, UPDATING, ABNORMAL, LOADING };

struct DataCacheStatusUtils {
    static std::string to_string(DataCacheStatus status) {
        switch (status) {
        case DataCacheStatus::NORMAL:
            return "NORMAL";
        case DataCacheStatus::UPDATING:
            return "UPDATING";
        case DataCacheStatus::ABNORMAL:
            return "ABNORMAL";
        case DataCacheStatus::LOADING:
            return "LOADING";
        default:
            return "UNKNOWN";
        }
    }

    static TDataCacheStatus::type to_thrift(DataCacheStatus status) {
        switch (status) {
        case DataCacheStatus::NORMAL:
            return TDataCacheStatus::NORMAL;
        case DataCacheStatus::UPDATING:
            return TDataCacheStatus::UPDATING;
        case DataCacheStatus::LOADING:
            return TDataCacheStatus::LOADING;
        default:
            return TDataCacheStatus::ABNORMAL;
        }
    }
};

struct DataCacheMetrics {
    DataCacheStatus status;

    size_t mem_quota_bytes;
    size_t mem_used_bytes;
    size_t disk_quota_bytes;
    size_t disk_used_bytes;
    size_t meta_used_bytes = 0;
};

#ifdef WITH_STARCACHE
using StarCacheMetrics = starcache::CacheMetrics;
using StarCacheStatus = starcache::CacheStatus;
#endif

} // namespace starrocks
