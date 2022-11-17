// This file is made available under Elastic License 2.0

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Status.thrift"
include "Types.thrift"
include "AgentService.thrift"
include "InternalService.thrift"

struct TMVMaintenanceStartTask {
    1: required i64 job_id
    2: required i64 task_id
    3: required string db_name
    4: required string mv_name
    5: optional InternalService.TExecPlanFragmentParams plan_params
}

struct TMVMaintenanceStopTask {
    1: required i64 job_id
    2: required i64 task_id
}

struct TBinlogScanRange {
  1: optional string db_name
  2: optional string table_name
  3: optional i64 partition_id
  4: required Types.TTabletId tablet_id
  5: required i64 start_lsn;
}

struct TMVEpoch {
    1: optional Types.TTransactionId txn_id
    2: optional i64 epoch_id
    3: optional Types.TTimestamp start_ts
    4: optional Types.TTimestamp commit_ts
    5: optional TBinlogScanRange binlog_scan
    6: optional i64 max_exec_millis
    7: optional i64 max_scan_rows
}

struct TMVStartEpochTask {
    1: optional TMVEpoch epoch
}

struct TMVCommitEpochTask {
    1: optional TMVEpoch epoch
    2: optional list<AgentService.TPartitionVersionInfo> partition_version_infos
}

enum MVTaskType {
    START_MAINTENANCE,
    STOP_MAINTENANCE,
    START_EPOCH,
    COMMIT_EPOCH,
}

// All tasks of MV maintenance
// Why not put them into the AgentTask interface? 
// Cause it's under rapid development and changed quickly, it should not disturb the stable interfaces
struct TMVMaintenanceTasks {
    1: required AgentService.TAgentServiceVersion protocol_version
    2: required MVTaskType task_type
    3: required i64 signature

    11: optional TMVMaintenanceStartTask start_maintenance
    12: optional TMVMaintenanceStopTask stop_maintenance
    13: optional TMVStartEpochTask start_epoch
    14: optional TMVCommitEpochTask commit_epoch
}
