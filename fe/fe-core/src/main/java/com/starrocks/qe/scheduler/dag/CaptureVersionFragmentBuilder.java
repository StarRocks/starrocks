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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TupleId;
import com.starrocks.planner.BlackHoleTableSink;
import com.starrocks.planner.CaptureVersionNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CaptureVersionFragmentBuilder {
    private final List<ExecutionFragment> fragments;

    public CaptureVersionFragmentBuilder(List<ExecutionFragment> fragments) {
        this.fragments = fragments;
    }

    public ExecutionFragment build(ExecutionDAG dag) {
        Map<ComputeNode, List<TScanRangeParams>> workerId2ScanRanges = Maps.newHashMap();
        int id = 0;

        for (ExecutionFragment fragment : fragments) {
            final PlanFragmentId fragmentId = fragment.getFragmentId();
            for (FragmentInstance instance : fragment.getInstances()) {
                final Set<Integer> localNativeScanNodeIds = instance.getExecFragment().getScanNodes().stream()
                        .filter(ScanNode::isLocalNativeTable).map(node -> node.getId().asInt()).collect(
                                Collectors.toSet());
                final ComputeNode worker = instance.getWorker();
                final Map<Integer, List<TScanRangeParams>> node2ScanRanges = instance.getNode2ScanRanges();
                final Map<Integer, Map<Integer, List<TScanRangeParams>>> node2DriverSeqToScanRanges =
                        instance.getNode2DriverSeqToScanRanges();

                // collect olap per driver scan ranges
                final Stream<TScanRangeParams> tabletStream = node2DriverSeqToScanRanges.entrySet().stream()
                        .filter(e -> localNativeScanNodeIds.contains(e.getKey()))
                        .map(k -> k.getValue().values()).flatMap(Collection::stream).flatMap(Collection::stream);

                // collect olap normal scan ranges
                final Stream<TScanRangeParams> nodeStream =
                        node2ScanRanges.entrySet().stream().filter(e -> localNativeScanNodeIds.contains(e.getKey()))
                                .map(Map.Entry::getValue).flatMap(Collection::stream);

                final List<TScanRangeParams> instanceScanRanges =
                        Stream.concat(tabletStream, nodeStream).filter(r -> r.scan_range.isSetInternal_scan_range())
                                .collect(Collectors.toList());

                if (instanceScanRanges.isEmpty()) {
                    continue;
                }

                final List<TScanRangeParams> workerIdScanRanges =
                        workerId2ScanRanges.computeIfAbsent(worker, k -> Lists.newArrayList());
                workerIdScanRanges.addAll(instanceScanRanges);
            }
            id = Math.max(id, fragmentId.asInt());
        }

        if (!workerId2ScanRanges.isEmpty()) {
            final PlanFragmentId captureVersionFragmentId = new PlanFragmentId(id + 1);
            final int fid = captureVersionFragmentId.asInt();
            // Just get a tuple id.
            final ArrayList<TupleId> tupleIds = fragments.get(0).getPlanFragment().getPlanRoot().getTupleIds();
            PlanNode dummyNode = new CaptureVersionNode(PlanNodeId.DUMMY_PLAN_NODE_ID, tupleIds);
            final PlanFragment captureVersionFragment =
                    new PlanFragment(captureVersionFragmentId, dummyNode, DataPartition.RANDOM);
            captureVersionFragment.setSink(new BlackHoleTableSink());
            final ExecutionFragment fragment = new ExecutionFragment(dag, captureVersionFragment, fragments.size());

            workerId2ScanRanges.forEach((worker, scanRanges) -> {
                final FragmentInstance instance = new FragmentInstance(worker, fragment);
                instance.addScanRanges(PlanNodeId.DUMMY_PLAN_NODE_ID.asInt(), scanRanges);
                fragment.addInstance(instance);
            });

            return fragment;
        }

        return null;
    }
}
