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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExportJobTest {

    @Test
    public void testExportUpdateInfo() {
        ExportJob.ExportUpdateInfo updateInfo = new ExportJob.ExportUpdateInfo();

        List<Pair<TNetworkAddress, String>> tList = Lists.newArrayList(
                Pair.create(new TNetworkAddress("host1", 1000), "path1"));
        List<Pair<ExportJob.NetworkAddress, String>> sList = Lists.newArrayList(
                Pair.create(new ExportJob.NetworkAddress("host1", 1000), "path1")
        );

        Assert.assertEquals(sList, updateInfo.serialize(tList));
        Assert.assertEquals(tList, updateInfo.deserialize(sList));
    }

    @Test
    public void testLakeGenTaskFragments(@Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked TabletInvertedIndex invertedIndex,
                                         @Mocked Table table,
                                         @Mocked Partition partition,
                                         @Mocked MaterializedIndex index,
                                         @Mocked Tablet tablet,
                                         @Mocked OlapScanNode scanNode,
                                         @Mocked PlanFragment fragment,
                                         @Mocked BrokerDesc brokerDesc) {
        // tabletId  backendId  dataSize
        //     1        0           1
        //     2        0           2
        //     3        0           3
        //     4        0           4
        //     5        0           5
        TabletMeta tabletMeta = new TabletMeta(0L, 1L, 2L, 3L, 4, TStorageMedium.HDD, true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                result = invertedIndex;
                invertedIndex.getTabletMeta(anyLong);
                result = tabletMeta;
                tablet.getDataSize(true);
                returns(1L, 2L, 3L, 4L, 5L);
                brokerDesc.hasBroker();
                result = true;
            }
        };

        List<TScanRangeLocations> locationsList = Lists.newArrayList();
        for (int i = 1; i < 6; ++i) {
            TInternalScanRange internalScanRange = new TInternalScanRange();
            internalScanRange.setTablet_id(i);
            TScanRange scanRange = new TScanRange();
            scanRange.setInternal_scan_range(internalScanRange);

            TScanRangeLocation scanRangeLocation = new TScanRangeLocation();
            scanRangeLocation.setBackend_id(0);

            TScanRangeLocations locations = new TScanRangeLocations();
            locations.setScan_range(scanRange);
            locations.setLocations(Lists.newArrayList(scanRangeLocation));
            locationsList.add(locations);
        }



        ExportJob job = new ExportJob(0, UUIDUtil.genUUID());
        Deencapsulation.setField(job, "tabletLocations", locationsList);
        Deencapsulation.setField(job, "exportTable", table);
        Deencapsulation.setField(job, "exportTupleDesc", new TupleDescriptor(new TupleId(0)));
        Deencapsulation.setField(job, "brokerDesc", brokerDesc);

        // 1 task: (1,2,3,4,5)
        List<PlanFragment> fragments = Lists.newArrayList();
        List<ScanNode> scanNodes = Lists.newArrayList();
        Config.export_max_bytes_per_be_per_task = 100L;
        Deencapsulation.invoke(job, "genTaskFragments", fragments, scanNodes);
        Assert.assertEquals(1, fragments.size());
        Assert.assertEquals(1, scanNodes.size());

        // 2 tasks: (1,2,3), (4,5)
        fragments.clear();
        scanNodes.clear();
        Config.export_max_bytes_per_be_per_task = 5L;
        Deencapsulation.invoke(job, "genTaskFragments", fragments, scanNodes);
        Assert.assertEquals(5, fragments.size());
        Assert.assertEquals(5, scanNodes.size());

        // 5 tasks: (1), (2), (3), (4), (5)
        fragments.clear();
        scanNodes.clear();
        Config.export_max_bytes_per_be_per_task = 1L;
        Deencapsulation.invoke(job, "genTaskFragments", fragments, scanNodes);
        Assert.assertEquals(5, fragments.size());
        Assert.assertEquals(5, scanNodes.size());
    }

    @Test
    public void testOlapGenTaskFragments(@Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked TabletInvertedIndex invertedIndex,
                                         @Mocked Table table,
                                         @Mocked Partition partition,
                                         @Mocked MaterializedIndex index,
                                         @Mocked Tablet tablet,
                                         @Mocked OlapScanNode scanNode,
                                         @Mocked PlanFragment fragment,
                                         @Mocked BrokerDesc brokerDesc) {
        // tabletId  backendId  dataSize
        //     1        0           1
        //     2        0           2
        //     3        0           3
        //     4        0           4
        //     5        0           5
        TabletMeta tabletMeta = new TabletMeta(0L, 1L, 2L, 3L, 4, TStorageMedium.HDD, false);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                result = invertedIndex;
                invertedIndex.getTabletMeta(anyLong);
                result = tabletMeta;
                invertedIndex.getReplica(anyLong, anyLong);
                returns(
                        new Replica(1L, 0L, 1L, 0, 1L, 1L, ReplicaState.NORMAL, -1L, -1L),
                        new Replica(2L, 0L, 1L, 0, 2L, 2L, ReplicaState.NORMAL, -1L, -1L),
                        new Replica(3L, 0L, 1L, 0, 3L, 3L, ReplicaState.NORMAL, -1L, -1L),
                        new Replica(4L, 0L, 1L, 0, 4L, 4L, ReplicaState.NORMAL, -1L, -1L),
                        new Replica(5L, 0L, 1L, 0, 5L, 5L, ReplicaState.NORMAL, -1L, -1L));
                brokerDesc.hasBroker();
                result = true;
            }
        };

        List<TScanRangeLocations> locationsList = Lists.newArrayList();
        for (int i = 1; i < 6; ++i) {
            TInternalScanRange internalScanRange = new TInternalScanRange();
            internalScanRange.setTablet_id(i);
            TScanRange scanRange = new TScanRange();
            scanRange.setInternal_scan_range(internalScanRange);

            TScanRangeLocation scanRangeLocation = new TScanRangeLocation();
            scanRangeLocation.setBackend_id(0);

            TScanRangeLocations locations = new TScanRangeLocations();
            locations.setScan_range(scanRange);
            locations.setLocations(Lists.newArrayList(scanRangeLocation));
            locationsList.add(locations);
        }



        ExportJob job = new ExportJob(0, UUIDUtil.genUUID());
        Deencapsulation.setField(job, "tabletLocations", locationsList);
        Deencapsulation.setField(job, "exportTable", table);
        Deencapsulation.setField(job, "exportTupleDesc", new TupleDescriptor(new TupleId(0)));
        Deencapsulation.setField(job, "brokerDesc", brokerDesc);

        // 1 task: (1,2,3,4,5)
        List<PlanFragment> fragments = Lists.newArrayList();
        List<ScanNode> scanNodes = Lists.newArrayList();
        Config.export_max_bytes_per_be_per_task = 100L;
        Deencapsulation.invoke(job, "genTaskFragments", fragments, scanNodes);
        Assert.assertEquals(1, fragments.size());
        Assert.assertEquals(1, scanNodes.size());

        // 2 tasks: (1,2,3), (4,5)
        fragments.clear();
        scanNodes.clear();
        Config.export_max_bytes_per_be_per_task = 5L;
        Deencapsulation.invoke(job, "genTaskFragments", fragments, scanNodes);
        Assert.assertEquals(5, fragments.size());
        Assert.assertEquals(5, scanNodes.size());

        // 5 tasks: (1), (2), (3), (4), (5)
        fragments.clear();
        scanNodes.clear();
        Config.export_max_bytes_per_be_per_task = 1L;
        Deencapsulation.invoke(job, "genTaskFragments", fragments, scanNodes);
        Assert.assertEquals(5, fragments.size());
        Assert.assertEquals(5, scanNodes.size());
    }
}
