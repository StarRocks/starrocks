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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/ExportPendingTask.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.task;

import com.starrocks.catalog.Database;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.load.ExportFailMsg;
import com.starrocks.load.ExportJob;
import com.starrocks.proto.LockTabletMetadataRequest;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TAgentResult;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TypesConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ExportPendingTask extends PriorityLeaderTask {
    private static final Logger LOG = LogManager.getLogger(ExportPendingTask.class);

    protected final ExportJob job;
    protected Database db;

    public ExportPendingTask(ExportJob job) {
        super();
        this.job = job;
        this.signature = job.getId();
    }

    @Override
    protected void exec() {
        if (job.getState() != ExportJob.JobState.PENDING) {
            return;
        }

        long dbId = job.getDbId();
        db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, "database does not exist");
            return;
        }

        if (job.isReplayed()) {
            // If the job is created from replay thread, all plan info will be lost.
            // so the job has to be cancelled.
            String failMsg = "FE restarted or Leader changed during exporting. Job must be cancalled.";
            job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return;
        }

        if (!job.exportOlapTable()) {
            // make snapshots
            Status snapshotStatus = makeSnapshots();
            if (!snapshotStatus.ok()) {
                job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, snapshotStatus.getErrorMsg());
                return;
            }
        }

        if (job.updateState(ExportJob.JobState.EXPORTING)) {
            LOG.info("submit pending export job success. job: {}", job);
            return;
        }
    }

    private Status makeSnapshots() {
        List<TScanRangeLocations> tabletLocations = job.getTabletLocations();
        if (tabletLocations == null) {
            return Status.OK;
        }

        for (TScanRangeLocations tablet : tabletLocations) {
            TScanRange scanRange = tablet.getScan_range();
            if (!scanRange.isSetInternal_scan_range()) {
                continue;
            }

            TInternalScanRange internalScanRange = scanRange.getInternal_scan_range();
            List<TScanRangeLocation> locations = tablet.getLocations();
            for (TScanRangeLocation location : locations) {
                TNetworkAddress address = location.getServer();
                String host = address.getHostname();
                int port = address.getPort();
                Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(host, port);
                if (backend == null) {
                    return Status.CANCELLED;
                }
                long backendId = backend.getId();
                if (!GlobalStateMgr.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                    return Status.CANCELLED;
                }
                this.job.setBeStartTime(backendId, backend.getLastStartTime());
                Status status;
                if (job.exportLakeTable()) {
                    status = lockTabletMetadata(internalScanRange, backend);
                } else {
                    status = makeSnapshot(internalScanRange, address);
                }
                if (!status.ok()) {
                    return status;
                }
            }
        }
        return Status.OK;
    }

    private Status lockTabletMetadata(TInternalScanRange internalScanRange, Backend backend) {
        try {
            LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
            LockTabletMetadataRequest request = new LockTabletMetadataRequest();
            request.tabletId = internalScanRange.getTablet_id();
            request.version = Long.parseLong(internalScanRange.getVersion());
            request.expireTime = (job.getCreateTimeMs() / 1000) + job.getTimeoutSecond();
            lakeService.lockTabletMetadata(request);
        } catch (Throwable e) {
            return new Status(TStatusCode.CANCELLED, e.getMessage());
        }
        return Status.OK;
    }

    private Status makeSnapshot(TInternalScanRange internalScanRange, TNetworkAddress address) {
        String host = address.getHostname();
        int port = address.getPort();

        TSnapshotRequest snapshotRequest = new TSnapshotRequest();
        snapshotRequest.setTablet_id(internalScanRange.getTablet_id());
        snapshotRequest.setSchema_hash(Integer.parseInt(internalScanRange.getSchema_hash()));
        snapshotRequest.setVersion(Long.parseLong(internalScanRange.getVersion()));
        snapshotRequest.setTimeout(job.getTimeoutSecond());
        snapshotRequest.setPreferred_snapshot_format(TypesConstants.TPREFER_SNAPSHOT_REQ_VERSION);

        AgentClient client = new AgentClient(host, port);
        TAgentResult result = client.makeSnapshot(snapshotRequest);
        if (result == null || result.getStatus().getStatus_code() != TStatusCode.OK) {
            String err = "snapshot for tablet " + internalScanRange.getTablet_id() + " failed on backend "
                    + address.toString() + ". reason: "
                    + (result == null ? "unknown" : result.getStatus().error_msgs);
            LOG.warn("{}, export job: {}", err, job.getId());
            return new Status(TStatusCode.CANCELLED, err);
        }
        job.addSnapshotPath(new Pair<TNetworkAddress, String>(address, result.getSnapshot_path()));
        return Status.OK;
    }
}
