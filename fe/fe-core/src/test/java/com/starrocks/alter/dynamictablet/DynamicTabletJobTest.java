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

package com.starrocks.alter.dynamictablet;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import javax.validation.constraints.NotNull;

public class DynamicTabletJobTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");

        new MockUp<Utils>() {
            @Mock
            public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                    long newVersion, List<String> distributionColumns, Map<Long, Double> compactionScores,
                    ComputeResource computeResource, Map<Long, Long> tabletRowNums,
                    boolean useAggregatePublish) {
                return;
            }
        };

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public <T> Future<T> submit(Callable<T> task) throws Exception {
                return CompletableFuture.completedFuture(task.call());
            }
        };
    }

    @Test
    public void testRunDynamicTabletJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        DynamicTabletJob dynamicTabletJob = createDynamicTabletJob();
        Assertions.assertNotNull(dynamicTabletJob);

        dynamicTabletJob.run();
        Assertions.assertEquals(DynamicTabletJob.JobState.RUNNING, dynamicTabletJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.DYNAMIC_TABLET, table.getState());

        dynamicTabletJob.run();
        Assertions.assertEquals(DynamicTabletJob.JobState.FINISHED, dynamicTabletJob.getJobState());
        Assertions.assertNull(table.getPhysicalPartition(physicalPartition.getId()));
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        PhysicalPartition newPhysicalPartition = table.getAllPhysicalPartitions().iterator().next();
        Assertions.assertTrue(newPhysicalPartition != physicalPartition);

        long newVersion = newPhysicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        MaterializedIndex newMaterializedIndex = newPhysicalPartition.getBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());
    }

    @Test
    public void testReplayDynamicTabletJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        DynamicTabletJob dynamicTabletJob = createDynamicTabletJob();
        Assertions.assertNotNull(dynamicTabletJob);

        Assertions.assertEquals(DynamicTabletJob.JobState.PENDING, dynamicTabletJob.getJobState());
        dynamicTabletJob.replay();

        dynamicTabletJob.setJobState(DynamicTabletJob.JobState.PREPARING);
        dynamicTabletJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.DYNAMIC_TABLET, table.getState());

        dynamicTabletJob.setJobState(DynamicTabletJob.JobState.RUNNING);
        dynamicTabletJob.replay();

        dynamicTabletJob.setJobState(DynamicTabletJob.JobState.CLEANING);
        dynamicTabletJob.replay();

        dynamicTabletJob.setJobState(DynamicTabletJob.JobState.FINISHED);
        dynamicTabletJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        PhysicalPartition newPhysicalPartition = table.getAllPhysicalPartitions().iterator().next();
        Assertions.assertTrue(newPhysicalPartition != physicalPartition);

        long newVersion = newPhysicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        MaterializedIndex newMaterializedIndex = newPhysicalPartition.getBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());
    }

    @Test
    public void testAbortDynamicTabletJob() throws Exception {
        DynamicTabletJob dynamicTabletJob = createDynamicTabletJob();
        Assertions.assertNotNull(dynamicTabletJob);

        dynamicTabletJob.abort("test abort");
        Assertions.assertEquals(DynamicTabletJob.JobState.ABORTING, dynamicTabletJob.getJobState());

        dynamicTabletJob.run();
        Assertions.assertEquals(DynamicTabletJob.JobState.ABORTED, dynamicTabletJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testReplayAbortDynamicTabletJob() throws Exception {
        DynamicTabletJob dynamicTabletJob = createDynamicTabletJob();
        Assertions.assertNotNull(dynamicTabletJob);

        Assertions.assertEquals(DynamicTabletJob.JobState.PENDING, dynamicTabletJob.getJobState());
        dynamicTabletJob.replay();

        dynamicTabletJob.setJobState(DynamicTabletJob.JobState.ABORTING);
        dynamicTabletJob.replay();

        dynamicTabletJob.setJobState(DynamicTabletJob.JobState.ABORTED);
        dynamicTabletJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    private DynamicTabletJob createDynamicTabletJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();

        long tabletId = materializedIndex.getTablets().get(0).getId();
        TabletList tabletList = new TabletList(List.of(tabletId));

        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_DYNAMIC_TABLET_SPLIT_SIZE, "-2");
        SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
        clause.setDynamicTabletSplitSize(-2);

        DynamicTabletJobFactory factory = new SplitTabletJobFactory(db, table, clause);
        return factory.createDynamicTabletJob();
    }
}
