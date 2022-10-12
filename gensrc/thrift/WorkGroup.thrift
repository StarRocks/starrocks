// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

namespace cpp starrocks
namespace java com.starrocks.thrift

enum TWorkGroupType {
  // Not suppported.
  WG_REALTIME,
  WG_NORMAL,
  WG_DEFAULT,
  WG_SHORT_QUERY
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
