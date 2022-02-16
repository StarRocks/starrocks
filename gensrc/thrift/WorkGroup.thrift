// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

namespace cpp starrocks
namespace java com.starrocks.thrift

enum TWorkGroupType {
  WG_REALTIME,
  WG_NORMAL,
  WG_DEFAULT
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
