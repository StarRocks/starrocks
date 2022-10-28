// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

namespace cpp starrocks
namespace java com.starrocks.thrift

struct TResourceUsage {
    1: optional i32 num_running_queries
    2: optional i64 mem_limit_bytes
    3: optional i64 mem_used_bytes
}
