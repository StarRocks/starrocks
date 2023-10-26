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
import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.HiveTableSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TableFunctionTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.ExplainBuilder;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The {@code FragmentInstance} represents a parallel instance of a {@link PlanFragment}.
 * It may be executed once or multiple times, each of which resulting in the spawning of an {@link FragmentInstanceExecState}.
 */
public class FragmentInstance {
    private static final int ABSENT_PIPELINE_DOP = -1;
    public static final int ABSENT_DRIVER_SEQUENCE = -1;

    /**
     * The index in the job.
     * <p></p>
     * The instance ordinals of {@code indexInJob} in the fragment and {@link #indexInFragment} should be the same.
     * For a shuffle join, its shuffle partitions and corresponding one-map-one GRF components should have the same ordinals.
     * - Fragment instances' ordinals {@link #indexInFragment} determine shuffle partitions' ordinals in DataStreamSink.
     * - {@code indexInJob} of fragment instances which contain shuffle join determine the ordinals of GRF components in the GRF.
     * Therefore, here assign monotonic unique indexInJob to Fragment instances to keep consistent order with Fragment
     * instances in ExecutionFragment.instances.
     */
    private int indexInJob = -1;
    /**
     * The index in {@link ExecutionFragment#getInstances()}, which is set when adding this instance to {@link ExecutionFragment}.
     */
    private int indexInFragment = -1;
    private TUniqueId instanceId = null;

    private final ExecutionFragment execFragment;

    private int pipelineDop = ABSENT_PIPELINE_DOP;

    private final ComputeNode worker;

    private final Map<Integer, Integer> bucketSeqToDriverSeq = Maps.newHashMap();
    private final Map<Integer, List<TScanRangeParams>> node2ScanRanges = Maps.newHashMap();
    private final Map<Integer, Map<Integer, List<TScanRangeParams>>> node2DriverSeqToScanRanges = Maps.newHashMap();

    private FragmentInstanceExecState execution = null;

    public FragmentInstance(ComputeNode worker, ExecutionFragment execFragment) {
        this.worker = worker;
        this.execFragment = execFragment;
    }

    @Override
    public String toString() {
        return "FragmentInstance{" + "fragmentId=" + getFragmentId() + ", instanceId=" + DebugUtil.printId(instanceId) +
                ", indexInJob=" + indexInJob + ", indexInFragment=" + indexInFragment + ", workerId=" + getWorkerId() +
                ", execution=" + execution + '}';
    }

    public String getReadableId() {
        return indexInJob + "-" + getFragmentId() + "#" + indexInFragment;
    }

    public void buildExplainString(ExplainBuilder builder) {
        builder.addValue("INSTANCE(" + getReadableId() + ")", () -> {
            if (execution != null) {
                List<TPlanFragmentDestination> destinations = execution.getDestinations();
                if (!destinations.isEmpty()) {
                    ExecutionDAG executionDAG = execFragment.getExecutionDAG();
                    String destInstanceIds = destinations.stream().map(destination -> {
                        TUniqueId destInstanceId = destination.getFragment_instance_id();
                        return executionDAG.getInstanceByInstanceId(destInstanceId).getReadableId();
                    }).collect(Collectors.joining(","));

                    builder.addValue("DESTINATIONS", destInstanceIds);
                }
            }

            builder.addValue("BE", worker.getId());

            if (isSetPipelineDop()) {
                builder.addValue("DOP", pipelineDop);
            }

            if (!node2ScanRanges.isEmpty()) {
                explainScanRangesWithoutDriverSeq(builder);
            }
            if (!node2DriverSeqToScanRanges.isEmpty()) {
                explainScanRangesWithDriverSeq(builder);
            }
        });
    }

    private void explainScanRangesWithoutDriverSeq(ExplainBuilder builder) {
        builder.addValue("SCAN RANGES", () -> {
            if (!bucketSeqToDriverSeq.isEmpty()) {
                builder.addValue("BUCKET SEQUENCES", bucketSeqToDriverSeq.keySet());
            }

            node2ScanRanges.forEach((scanId, scanRanges) -> {
                ScanNode scanNode = execFragment.getScanNode(new PlanNodeId(scanId));
                builder.addValue(scanId + ":" + scanNode.getPlanNodeName(),
                        () -> explainScanRanges(builder, scanRanges, 0));
            });
        });
    }

    private void explainScanRangesWithDriverSeq(ExplainBuilder builder) {
        builder.addValue("SCAN RANGES (per driver sequence)", () -> {
            if (!bucketSeqToDriverSeq.isEmpty()) {
                builder.addValue("BUCKET SEQUENCE TO DRIVER SEQUENCE", bucketSeqToDriverSeq.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue())
                        .collect(Collectors.toList()));
            }

            node2DriverSeqToScanRanges.forEach((scanId, driverSeqToScanRanges) -> {
                ScanNode scanNode = execFragment.getScanNode(new PlanNodeId(scanId));
                builder.addValue(scanId + ":" + scanNode.getPlanNodeName(), () -> {
                    int offset = 0;
                    for (Map.Entry<Integer, List<TScanRangeParams>> entry : driverSeqToScanRanges.entrySet()) {
                        Integer driverSeq = entry.getKey();
                        List<TScanRangeParams> scanRanges = entry.getValue();
                        int localOffset = offset;
                        builder.addValue("DriverSequence#" + driverSeq,
                                () -> explainScanRanges(builder, scanRanges, localOffset));

                        offset += scanRanges.size();
                    }
                });
            });
        });
    }

    private void explainScanRanges(ExplainBuilder builder, List<TScanRangeParams> scanRanges, int offset) {
        for (int i = 0; i < scanRanges.size(); i++) {
            builder.addOrderedItem(offset + i + 1, explainScanRange(scanRanges.get(i).getScan_range()));
        }
    }

    private String explainScanRange(TScanRange scanRange) {
        StringBuilder builder = new StringBuilder();
        if (scanRange.isSetInternal_scan_range()) {
            TInternalScanRange olapScanRange = scanRange.getInternal_scan_range();
            builder.append("partitionID=").append(olapScanRange.getPartition_id())
                    .append(",tabletID=").append(olapScanRange.getTablet_id());
        } else {
            builder.append("<PLACEHOLDER>");
        }
        return builder.toString();
    }

    public ExecutionFragment getExecFragment() {
        return execFragment;
    }

    public Long getWorkerId() {
        return worker.getId();
    }

    public ComputeNode getWorker() {
        return worker;
    }

    public boolean isSetPipelineDop() {
        return pipelineDop != ABSENT_PIPELINE_DOP;
    }

    public int getPipelineDop() {
        if (isSetPipelineDop()) {
            return pipelineDop;
        } else {
            return execFragment.getPlanFragment().getPipelineDop();
        }
    }

    public void setPipelineDop(int pipelineDop) {
        if (execFragment.getPlanFragment().isUseRuntimeAdaptiveDop()) {
            pipelineDop = Utils.computeMinGEPower2(pipelineDop);
        }

        this.pipelineDop = pipelineDop;
    }

    public FragmentInstanceExecState getExecution() {
        return execution;
    }

    public void setExecution(FragmentInstanceExecState execution) {
        this.execution = execution;
    }

    public PlanFragmentId getFragmentId() {
        return execFragment.getPlanFragment().getFragmentId();
    }

    public int getIndexInJob() {
        return indexInJob;
    }

    public void setIndexInJob(int indexInJob) {
        this.indexInJob = indexInJob;
    }

    public int getIndexInFragment() {
        return indexInFragment;
    }

    public void setIndexInFragment(int indexInFragment) {
        this.indexInFragment = indexInFragment;
    }

    public TUniqueId getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(TUniqueId instanceId) {
        this.instanceId = instanceId;
    }

    public Map<Integer, List<TScanRangeParams>> getNode2ScanRanges() {
        return node2ScanRanges;
    }

    public Map<Integer, Map<Integer, List<TScanRangeParams>>> getNode2DriverSeqToScanRanges() {
        return node2DriverSeqToScanRanges;
    }

    public void addBucketSeqAndDriverSeq(int bucketSeq, int driverSeq) {
        bucketSeqToDriverSeq.putIfAbsent(bucketSeq, driverSeq);
    }

    public void addBucketSeq(int bucketSeq) {
        bucketSeqToDriverSeq.putIfAbsent(bucketSeq, ABSENT_DRIVER_SEQUENCE);
    }

    public Collection<Integer> getBucketSeqs() {
        return bucketSeqToDriverSeq.keySet();
    }

    public Integer getDriverSeqOfBucketSeq(Integer bucketSeq) {
        return bucketSeqToDriverSeq.get(bucketSeq);
    }

    public Map<Integer, Integer> getBucketSeqToDriverSeq() {
        return bucketSeqToDriverSeq;
    }

    public void addScanRanges(Integer scanId, List<TScanRangeParams> scanRanges) {
        node2ScanRanges.computeIfAbsent(scanId, k -> new ArrayList<>()).addAll(scanRanges);
    }

    public void addScanRanges(Integer scanId, Integer driverSeq, List<TScanRangeParams> scanRanges) {
        node2DriverSeqToScanRanges.computeIfAbsent(scanId, k -> new HashMap<>())
                .computeIfAbsent(driverSeq, k -> new ArrayList<>()).addAll(scanRanges);
    }

    public void paddingScanRanges() {
        node2DriverSeqToScanRanges.forEach((scanId, driverSeqToScanRanges) -> {
            for (int driverSeq = 0; driverSeq < pipelineDop; driverSeq++) {
                driverSeqToScanRanges.computeIfAbsent(driverSeq, k -> new ArrayList<>());
            }
        });
    }

    public int getTableSinkDop() {
        PlanFragment fragment = execFragment.getPlanFragment();
        if (!fragment.forceSetTableSinkDop()) {
            return getPipelineDop();
        }

        DataSink dataSink = fragment.getSink();
        int dop = fragment.getPipelineDop();
        if (!(dataSink instanceof IcebergTableSink || dataSink instanceof HiveTableSink
                || dataSink instanceof TableFunctionTableSink)) {
            return dop;
        } else {
            int sessionVarSinkDop = ConnectContext.get().getSessionVariable().getPipelineSinkDop();
            if (sessionVarSinkDop > 0) {
                return Math.min(dop, sessionVarSinkDop);
            } else {
                return Math.min(dop, IcebergTableSink.ICEBERG_SINK_MAX_DOP);
            }
        }
    }

}
