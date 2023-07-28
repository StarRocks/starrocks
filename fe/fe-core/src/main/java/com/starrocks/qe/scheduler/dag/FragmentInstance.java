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

package com.starrocks.qe.scheduler.dag;

import com.google.common.collect.Maps;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;

// fragment instance exec param, it is used to assemble
// the per-instance TPlanFragmentExecParas, as a member of
// FragmentExecParams
public class FragmentInstance {
    static final int ABSENT_PIPELINE_DOP = -1;
    static final int ABSENT_DRIVER_SEQUENCE = -1;
    private static final int ABSENT_BACKEND_NUM = -1;

    TUniqueId instanceId;
    final Long workerId;

    Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();
    Map<Integer, Map<Integer, List<TScanRangeParams>>> nodeToPerDriverSeqScanRanges = Maps.newHashMap();

    Map<Integer, Integer> bucketSeqToDriverSeq = Maps.newHashMap();

    int backendNum = ABSENT_BACKEND_NUM;

    ExecutionFragment execFragment;

    int pipelineDop = ABSENT_PIPELINE_DOP;

    public void addBucketSeqAndDriverSeq(int bucketSeq, int driverSeq) {
        this.bucketSeqToDriverSeq.putIfAbsent(bucketSeq, driverSeq);
    }

    public void addBucketSeq(int bucketSeq) {
        this.bucketSeqToDriverSeq.putIfAbsent(bucketSeq, ABSENT_DRIVER_SEQUENCE);
    }

    public FragmentInstance(TUniqueId id, Long workerId, ExecutionFragment execFragment) {
        this.instanceId = id;
        this.workerId = workerId;
        this.execFragment = execFragment;
    }

    public PlanFragment fragment() {
        return execFragment.fragment;
    }

    public boolean isSetPipelineDop() {
        return pipelineDop != ABSENT_PIPELINE_DOP;
    }

    public int getPipelineDop() {
        return pipelineDop;
    }

    public Map<Integer, Integer> getBucketSeqToDriverSeq() {
        return bucketSeqToDriverSeq;
    }

    public Map<Integer, List<TScanRangeParams>> getPerNodeScanRanges() {
        return perNodeScanRanges;
    }

    public Map<Integer, Map<Integer, List<TScanRangeParams>>> getNodeToPerDriverSeqScanRanges() {
        return nodeToPerDriverSeqScanRanges;
    }

    public int getBackendNum() {
        return backendNum;
    }

    public void setBackendNum(int backendNum) {
        this.backendNum = backendNum;
    }

    public TUniqueId getInstanceId() {
        return instanceId;
    }

    public Long getWorkerId() {
        return workerId;
    }

    public int getTableSinkDop() {
        PlanFragment fragment = fragment();
        if (!fragment.forceSetTableSinkDop()) {
            return getPipelineDop(); // instance dop.
        }

        DataSink dataSink = fragment.getSink();
        int fragmentDop = fragment.getPipelineDop();
        if (!(dataSink instanceof IcebergTableSink)) {
            return fragmentDop;
        } else {
            int sessionVarSinkDop = ConnectContext.get().getSessionVariable().getPipelineSinkDop();
            if (sessionVarSinkDop > 0) {
                return Math.min(fragmentDop, sessionVarSinkDop);
            } else {
                return Math.min(fragmentDop, IcebergTableSink.ICEBERG_SINK_MAX_DOP);
            }
        }
    }
}
