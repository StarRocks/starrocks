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

struct TMVReportEpochTask {
    1: optional TMVEpoch epoch
    // Each tablet's binlog consumption state
    2: optional map<i64, TBinlogOffset> binlog_consume_state
    // Transaction state
    3: optional list<Types.TTabletCommitInfo> txn_commit_info
    4: optional list<Types.TTabletFailInfo> txn_fail_info
}

struct TMVReportEpochResponse {
}

enum MVTaskType {
    // For BE
    START_MAINTENANCE,
    STOP_MAINTENANCE,
    START_EPOCH,
    COMMIT_EPOCH,
    
    // For FE
    REPORT_EPOCH,
}

// All tasks of MV maintenance
// Why not put them into the AgentTask interface? 
// Cause it's under rapid development and changed quickly, it should not disturb the stable interfaces
struct TMVMaintenanceTasks {
    // Common parameters for task
    1: optional AgentService.TAgentServiceVersion protocol_version
    2: optional MVTaskType task_type
    3: optional i64 signature
    4: optional i64 job_id
    5: optional i64 task_id

    // Tasks for BE
    11: optional TMVMaintenanceStartTask start_maintenance
    12: optional TMVMaintenanceStopTask stop_maintenance
    13: optional TMVStartEpochTask start_epoch
    14: optional TMVCommitEpochTask commit_epoch
    
    // Tasks for FE
    21: optional TMVReportEpochTask report_epoch
}
