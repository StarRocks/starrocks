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

include "AgentService.thrift"
include "PlanNodes.thrift"
include "Status.thrift"
include "Types.thrift"
include "InternalService.thrift"

enum MVTaskType {
    // For BE
    START_MAINTENANCE,
    STOP_MAINTENANCE,
    START_EPOCH,
    COMMIT_EPOCH,
    
    // For FE
    REPORT_EPOCH,
}

struct TMVMaintenanceStartTask {
    1: optional list<InternalService.TExecPlanFragmentParams> fragments;
}

struct TMVMaintenanceStopTask {
}

struct TMVEpoch {
    1: optional Types.TTransactionId txn_id
    2: optional i64 epoch_id
    3: optional Types.TTimestamp start_ts
    4: optional Types.TTimestamp commit_ts
}

struct TMVStartEpochTask {
    1: optional TMVEpoch epoch
    
    // Start position of this epoch
    // map<FragmentInstnaceId, map<PlanNodeId, list<ScanRanges>>>
    2: optional map<Types.TUniqueId, map<Types.TPlanNodeId, list<PlanNodes.TScanRange>>> per_node_scan_ranges
    
    // Max execution threshold of this epoch
    3: optional i64 max_exec_millis
    4: optional i64 max_scan_rows
}

struct TMVCommitEpochTask {
    1: optional TMVEpoch epoch
    
    2: optional list<AgentService.TPartitionVersionInfo> partition_version_infos
    3: optional Types.TTransactionId transaction_id
    4: optional i64 commit_timestamp
}

// TOD
struct TMVAbortEpochTask {
    1: optional TMVEpoch epoch
}

struct TMVReportEpochTask {
    1: optional TMVEpoch epoch
    
    // Each tablet's binlog consumption state
    2: optional map<Types.TUniqueId, map<Types.TPlanNodeId, list<PlanNodes.TScanRange>>> binlog_consume_state 
    // Transaction state
    3: optional list<Types.TTabletCommitInfo> txn_commit_info
    4: optional list<Types.TTabletFailInfo> txn_fail_info
}

struct TMVReportEpochResponse {
}

// Union of all tasks of MV maintenance, distinguished by task_type
// Why not put them into the AgentTask interface? 
// Cause it's under rapid development and changed quickly, it should not disturb the stable interfaces
struct TMVMaintenanceTasks {
    // Common parameters for task
    1: optional AgentService.TAgentServiceVersion protocol_version
    2: optional MVTaskType task_type 
    3: optional i64 signature
    4: optional string db_name
    5: optional string mv_name
    6: optional i64 db_id
    7: optional i64 mv_id
    8: optional i64 job_id
    9: optional i64 task_id
    10: optional Types.TUniqueId query_id;

    // Tasks for BE
    11: optional TMVMaintenanceStartTask start_maintenance
    12: optional TMVMaintenanceStopTask stop_maintenance
    13: optional TMVStartEpochTask start_epoch
    14: optional TMVCommitEpochTask commit_epoch
    15: optional TMVAbortEpochTask abort_epoch
    
    // Tasks for FE
    31: optional TMVReportEpochTask report_epoch
}
