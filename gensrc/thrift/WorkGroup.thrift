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

enum TWorkGroupType {
  // Not suppported.
  WG_REALTIME,
  WG_NORMAL,
  WG_DEFAULT,
  WG_SHORT_QUERY,
  WG_MV
}

struct TWorkGroup {
  1: optional string name
  2: optional i64 id
  3: optional i32 cpu_core_limit
  4: optional double mem_limit
  5: optional i32 concurrency_limit
  6: optional TWorkGroupType workgroup_type
  7: optional i64 version
  9: optional string state
  10: optional i64 num_drivers
  11: optional i64 big_query_mem_limit
  12: optional i64 big_query_scan_rows_limit
  13: optional i64 big_query_cpu_second_limit
}

enum TWorkGroupOpType {
  WORKGROUP_OP_CREATE,
  WORKGROUP_OP_ALTER,
  WORKGROUP_OP_DELETE
}

struct TWorkGroupOp {
  1: TWorkGroup workgroup
  2: TWorkGroupOpType op_type
}
