// This file is made available under Elastic License 2.0

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Status.thrift"
include "Types.thrift"
include "AgentService.thrift"
include "InternalService.thrift"

struct TMVMaintenanceStartTask {
    1: optional string db_name
    2: optional string mv_name
    3: optional InternalService.TExecPlanFragmentParams plan_params
}

struct TMVMaintenanceStopTask {
}

struct TBinlogOffset {
    1: optional Types.TVersion version
    2: optional i64 lsn
}

struct TBinlogScanRange {
  1: optional string db_name
  2: optional Types.TTableId table_id
  3: optional Types.TPartitionId partition_id
  4: optional Types.TTabletId tablet_id
  5: optional TBinlogOffset lsn
}

struct TMVEpoch {
    1: optional Types.TTransactionId txn_id
    2: optional i64 epoch_id
    3: optional Types.TTimestamp start_ts
    4: optional Types.TTimestamp commit_ts
    5: optional list<TBinlogScanRange> binlog_scan

    11: optional i64 max_exec_millis
    12: optional i64 max_scan_rows
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
    1: optional AgentService.TAgentServiceVersion protocol_version
    2: optional MVTaskType task_type
    3: optional i64 signature
    4: optional i64 job_id
    5: optional i64 task_id

    11: optional TMVMaintenanceStartTask start_maintenance
    12: optional TMVMaintenanceStopTask stop_maintenance
    13: optional TMVStartEpochTask start_epoch
    14: optional TMVCommitEpochTask commit_epoch
}
