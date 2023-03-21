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

struct TResourceUsage {
    1: optional i32 num_running_queries
    2: optional i64 mem_limit_bytes
    3: optional i64 mem_used_bytes
    4: optional i32 cpu_used_permille;
}
