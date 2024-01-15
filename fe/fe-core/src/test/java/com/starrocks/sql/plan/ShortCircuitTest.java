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

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ShortCircuitTest extends PlanTestBase {

    @Test
    public void testShortcircuit() throws Exception {
        connectContext.getSessionVariable().setEnableShortCircuit(true);
        connectContext.getSessionVariable().setPreferComputeNode(true);

        // project support short circuit
        String sql = "select pk1 || v3 from tprimary1 where pk1=20";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("Short Circuit Scan: true"));

        // boolean filter
        sql = "select pk1 || v3 from tprimary_bool where pk1=20 and pk2=false";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("Short Circuit Scan: true"));

        // boolean filter
        sql = "select pk1 || v3 from tprimary_bool where pk1=33 and pk2=true";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("Short Circuit Scan: true"));

        //  support short circuit
        sql = "select * from tprimary1 where pk1 in (20)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("Short Circuit Scan: true"));

        // not support short circuit
        sql = "select * from tprimary1 ";
        planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("Short Circuit Scan: true"));
    }

    @Test
    public void testShortCircuitExec() throws Exception {
        // support short circuit read
        String sql = "select * from tprimary where pk=20";
        connectContext.setExecutionId(new TUniqueId(0x33, 0x0));
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        TScanRange scanRange = new TScanRange();
        TInternalScanRange internalScanRange = new TInternalScanRange();
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation();
        internalScanRange.setTablet_id(11L);
        internalScanRange.setVersion("version_1");
        internalScanRange.setHosts(ImmutableList.of(new TNetworkAddress("127.0.0.1", 8060)));
        scanRangeLocation.setBackend_id(1L);
        scanRange.setInternal_scan_range(internalScanRange);
        scanRangeLocations.setScan_range(scanRange);

        DescriptorTable desc = new DescriptorTable();
        TupleDescriptor tupleDescriptor = desc.createTupleDescriptor();
        tupleDescriptor.setTable(getTable("tprimary"));

        OlapScanNode scanNode = OlapScanNode.createOlapScanNodeByLocation(execPlan.getNextNodeId(), tupleDescriptor,
                "OlapScanNodeForShortCircuit", ImmutableList.of(scanRangeLocations));
        List<Long> selectPartitionIds = ImmutableList.of(1L);
        scanNode.setSelectedPartitionIds(selectPartitionIds);

        DefaultCoordinator coord = new DefaultCoordinator.Factory().createQueryScheduler(connectContext,
                execPlan.getFragments(), ImmutableList.of(scanNode), execPlan.getDescTbl().toThrift());
        coord.startScheduling();

        ExecutionFragment execFragment = coord.getExecutionDAG().getRootFragment();
        Assert.assertEquals(true, execFragment.getPlanFragment().isShortCircuit());
    }
}
