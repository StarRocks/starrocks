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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class SplitTabletJobTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "order by(key1)\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public <T> Future<T> submit(Callable<T> task) throws Exception {
                return CompletableFuture.completedFuture(task.call());
            }
        };

        new MockUp<MockLakeService>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.status.statusCode = TStatusCode.OK.getValue();
                if (request.reshardingTabletInfos == null) {
                    return CompletableFuture.completedFuture(response);
                }

                response.tabletRanges = new HashMap<>();
                for (ReshardingTabletInfoPB reshardingTabletInfoPB : request.reshardingTabletInfos) {
                    if (reshardingTabletInfoPB.splittingTabletInfo == null) {
                        continue;
                    }
                    for (Long tabletId : reshardingTabletInfoPB.splittingTabletInfo.newTabletIds) {
                        response.tabletRanges.put(tabletId, new TabletRangePB());
                    }
                }

                return CompletableFuture.completedFuture(response);
            }

            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.status.statusCode = TStatusCode.OK.getValue();
                response.tabletRanges = new HashMap<>();

                for (PublishVersionRequest publishRequest : request.publishReqs) {
                    if (publishRequest.reshardingTabletInfos == null) {
                        continue;
                    }

                    for (ReshardingTabletInfoPB reshardingTabletInfoPB : publishRequest.reshardingTabletInfos) {
                        if (reshardingTabletInfoPB.splittingTabletInfo == null) {
                            continue;
                        }
                        for (Long tabletId : reshardingTabletInfoPB.splittingTabletInfo.newTabletIds) {
                            response.tabletRanges.put(tabletId, new TabletRangePB());
                        }
                    }
                }

                return CompletableFuture.completedFuture(response);
            }
        };
    }

    @Test
    public void testRunTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        MaterializedIndex newMaterializedIndex = physicalPartition.getBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());
    }

    @Test
    public void testReplayTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.PREPARING);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.setJobState(TabletReshardJob.JobState.RUNNING);
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.CLEANING);
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.FINISHED);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        MaterializedIndex newMaterializedIndex = physicalPartition.getBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());
    }

    @Test
    public void testAbortTabletReshardJob() throws Exception {
        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        tabletReshardJob.abort("test abort");
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTING, tabletReshardJob.getJobState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testReplayAbortTabletReshardJob() throws Exception {
        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.ABORTING);
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.ABORTED);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    private TabletReshardJob createTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();

        long tabletId = materializedIndex.getTablets().get(0).getId();
        TabletList tabletList = new TabletList(List.of(tabletId));

        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2");
        SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
        clause.setTabletReshardTargetSize(-2);

        TabletReshardJobFactory factory = new SplitTabletJobFactory(db, table, clause);
        TabletReshardJob tabletReshardJob = factory.createTabletReshardJob();

        Assertions.assertTrue(tabletReshardJob.getParallelTablets() > 0);
        Assertions.assertNotNull(tabletReshardJob.toString());
        Assertions.assertNotNull(tabletReshardJob.getInfo());

        return tabletReshardJob;
    }
}
