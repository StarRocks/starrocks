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

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TBinlogOffset;
import com.starrocks.thrift.TBinlogScanRange;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceStartTask;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TMVReportEpochTask;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * TODO(murphy) implement the Coordinator to compute task correctly
 * <p>
 * Runnable maintenance task on each executor
 * 1. After maintenance job started, generated tasks are deployed on executors
 * 2. The execution of task is coordinated by EpochCoordinator on FE
 */
public class MVMaintenanceTask {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceTask.class);

    // Job information of the job
    private MVMaintenanceJob job;
    private String dbName;

    // Task specific information
    private long taskId;
    private TNetworkAddress beRpcAddr;
    private List<TExecPlanFragmentParams> fragmentInstances = new ArrayList<>();
    private Map<TUniqueId, Map<Integer, List<TScanRange>>> binlogConsumeState = new HashMap<>();

    public static MVMaintenanceTask build(MVMaintenanceJob job, long taskId, TNetworkAddress beRpcAddr,
                                          List<TExecPlanFragmentParams> fragmentInstances) {
        MVMaintenanceTask task = new MVMaintenanceTask();
        task.dbName = GlobalStateMgr.getCurrentState().getDb(job.getView().getDbId()).getFullName();
        task.job = job;
        task.beRpcAddr = beRpcAddr;
        task.taskId = taskId;
        task.fragmentInstances = fragmentInstances;
        return task;
    }

    public TMVMaintenanceTasks toThrift() {
        TMVMaintenanceTasks request = new TMVMaintenanceTasks();
        request.setQuery_id(job.getQueryId());
        request.setTask_type(MVTaskType.START_MAINTENANCE);
        TMVMaintenanceStartTask task = new TMVMaintenanceStartTask();
        request.setJob_id(job.getJobId());
        request.setTask_id(taskId);
        request.setStart_maintenance(task);
        request.setDb_name(dbName);
        request.setMv_name(job.getView().getName());
        task.setFragments(fragmentInstances);

        return request;
    }

    public MVMaintenanceJob getJob() {
        return job;
    }

    public void setJob(MVMaintenanceJob job) {
        this.job = job;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public TNetworkAddress getBeRpcAddr() {
        return beRpcAddr;
    }

    public void addFragmentInstance(TExecPlanFragmentParams instance) {
        fragmentInstances.add(instance);
    }

    public List<TExecPlanFragmentParams> getFragmentInstances() {
        return fragmentInstances;
    }

    public void setFragmentInstances(List<TExecPlanFragmentParams> fragmentInstances) {
        this.fragmentInstances = fragmentInstances;
    }

    public synchronized Map<TUniqueId, Map<Integer, List<TScanRange>>> getBinlogConsumeState() {
        // TODO(lism): how to initialize binlog consume state at first?
        if (binlogConsumeState.isEmpty()) {
            for (TExecPlanFragmentParams params : fragmentInstances) {
                Map<Integer, List<TScanRange>> nodeScanRangesMapping = new HashMap<>();
                for (Map.Entry<Integer, List<TScanRangeParams>> entry : params.params.getPer_node_scan_ranges().entrySet()) {
                    List<TScanRange> scanRanges = new ArrayList<>();
                    for (TScanRangeParams scanRangeParams : entry.getValue()) {
                        if (scanRangeParams.scan_range.isSetBinlog_scan_range()) {
                            scanRanges.add(scanRangeParams.scan_range);
                        }
                    }
                    nodeScanRangesMapping.putIfAbsent(entry.getKey(), scanRanges);
                }
                binlogConsumeState.put(params.params.getFragment_instance_id(), nodeScanRangesMapping);
            }
        }
        return binlogConsumeState;
    }

    public synchronized void updateEpochState(TMVReportEpochTask reportTask) {
        // Validate state
        reportTask.getBinlog_consume_state().forEach((k, v) -> {
            Map<Integer, List<TScanRange>> nodeScanRanges = binlogConsumeState.get(k);
            Preconditions.checkState(nodeScanRanges != null, "fragment instance not exists: " + k);
            v.forEach((node, rangeList) -> {
                List<TScanRange> ranges = nodeScanRanges.get(node);
                Preconditions.checkState(ranges != null, "plan node not exists: " + node);
                ranges.forEach((range) -> {
                    Preconditions.checkState(range.isSetBinlog_scan_range(), "must be binlog scan");
                    TBinlogScanRange binlogScan = range.getBinlog_scan_range();
                    TBinlogOffset offset = binlogScan.getOffset();
                    long tabletId = offset.getTablet_id();
                    long version = offset.getVersion();
                    long lsn = offset.getLsn();
                    Optional<TScanRange> existedState =
                            ranges.stream()
                                    .filter(x -> x.getBinlog_scan_range().getTablet_id() == tabletId)
                                    .findFirst();
                    Preconditions.checkState(existedState.isPresent(), "no existed state");

                    // Check version is progressive increase
                    TBinlogOffset existedOffset = existedState.get().getBinlog_scan_range().getOffset();
                    long existedVersion = existedOffset.getVersion();
                    long existedLsn = existedOffset.getLsn();
                    Preconditions.checkState(version >= existedVersion || lsn >= existedLsn,
                            "offset must be increased");
                });
            });
        });

        this.binlogConsumeState = reportTask.getBinlog_consume_state();
    }

    @Override
    public String toString() {
        return "MVMaintenanceTask{" +
                "job=" + job +
                ", dbName='" + dbName + '\'' +
                ", host=" + beRpcAddr +
                ", taskId=" + taskId +
                '}';
    }
}
