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
//
namespace cpp starrocks
namespace java com.starrocks.thrift

struct TDataCacheOptions {
    // just placeholder, not needed now
    // 1: optional bool enable_scan_datacache;
    2: optional bool enable_populate_datacache;

    // not public to user now
    100: optional i32 priority;
}

enum TDataCacheStatus {
    DISABLED,
    NORMAL,
    UPDATING,
    ABNORMAL,
    LOADING
}

struct TDataCacheMetrics {
    1: optional TDataCacheStatus status
    2: optional i64 mem_quota_bytes
    3: optional i64 mem_used_bytes
    4: optional i64 disk_quota_bytes
    5: optional i64 disk_used_bytes
}

struct TLoadDataCacheMetrics {
    1: optional i64 read_bytes;
    2: optional i64 read_time_ns;
    3: optional i64 write_bytes;
    4: optional i64 write_time_ns;
    // the number of metrics merged
    5: optional i64 count;
    6: optional TDataCacheMetrics metrics;
}