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
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
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
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
    }

    @Test
    public void testRunTabletReshardJob() throws Exception {
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
                    if (reshardingTabletInfoPB.splittingTabletInfo != null) {
                        // For splitting tablets, get the old tablet's range and split it into
                        // contiguous sub-ranges
                        Long oldTabletId = reshardingTabletInfoPB.splittingTabletInfo.oldTabletId;
                        List<Long> newTabletIds = reshardingTabletInfoPB.splittingTabletInfo.newTabletIds;
                        response.tabletRanges.putAll(createSplitTabletRanges(oldTabletId, newTabletIds));
                    }
                    if (reshardingTabletInfoPB.identicalTabletInfo != null) {
                        // For identical tablets, the new tablet has the same range as the old tablet
                        Long oldTabletId = reshardingTabletInfoPB.identicalTabletInfo.oldTabletId;
                        Long newTabletId = reshardingTabletInfoPB.identicalTabletInfo.newTabletId;
                        response.tabletRanges.put(newTabletId, createTabletRangePBFromOldTablet(oldTabletId));
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
                        if (reshardingTabletInfoPB.splittingTabletInfo != null) {
                            // For splitting tablets, get the old tablet's range and split it into
                            // contiguous sub-ranges
                            Long oldTabletId = reshardingTabletInfoPB.splittingTabletInfo.oldTabletId;
                            List<Long> newTabletIds = reshardingTabletInfoPB.splittingTabletInfo.newTabletIds;
                            response.tabletRanges.putAll(createSplitTabletRanges(oldTabletId, newTabletIds));
                        }
                        if (reshardingTabletInfoPB.identicalTabletInfo != null) {
                            // For identical tablets, the new tablet has the same range as the old tablet
                            Long oldTabletId = reshardingTabletInfoPB.identicalTabletInfo.oldTabletId;
                            Long newTabletId = reshardingTabletInfoPB.identicalTabletInfo.newTabletId;
                            response.tabletRanges.put(newTabletId, createTabletRangePBFromOldTablet(oldTabletId));
                        }
                    }
                }

                return CompletableFuture.completedFuture(response);
            }
        };

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        List<Long> oldTabletIds = new ArrayList<>();
        for (Tablet tablet : materializedIndex.getTablets()) {
            oldTabletIds.add(tablet.getId());
        }
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Long tabletId : oldTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId));
        }
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

        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());
        for (Long tabletId : oldTabletIds) {
            Assertions.assertNull(invertedIndex.getTabletMeta(tabletId));
        }
        for (Tablet tablet : newMaterializedIndex.getTablets()) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tablet.getId()));
        }
    }

    @Test
    public void testFallbackToIdenticalTablet() throws Exception {
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
                    // Only return range for the first tablet (fallback to identical tablet).
                    // The range should be the same as the old tablet's range.
                    Long oldTabletId = reshardingTabletInfoPB.splittingTabletInfo.oldTabletId;
                    Long firstTabletId = reshardingTabletInfoPB.splittingTabletInfo.newTabletIds.get(0);
                    response.tabletRanges.put(firstTabletId, createTabletRangePBFromOldTablet(oldTabletId));
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
                        // Only return range for the first tablet (fallback to identical tablet).
                        // The range should be the same as the old tablet's range.
                        Long oldTabletId = reshardingTabletInfoPB.splittingTabletInfo.oldTabletId;
                        Long firstTabletId = reshardingTabletInfoPB.splittingTabletInfo.newTabletIds.get(0);
                        response.tabletRanges.put(firstTabletId, createTabletRangePBFromOldTablet(oldTabletId));
                    }
                }

                return CompletableFuture.completedFuture(response);
            }
        };

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
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

        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() == materializedIndex.getTablets().size());
    }

    @Test
    public void testReplayTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
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

        // After CLEANING replay, the new tablets have been added to the partition
        // but without proper ranges (they have default Range.all() with null bounds).
        // We need to set proper contiguous ranges for the new tablets to satisfy
        // the strict validation in shareAdjacentTabletRangeBounds().
        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        List<Tablet> newTablets = newMaterializedIndex.getTablets();
        List<Long> newTabletIds = new ArrayList<>();
        for (Tablet tablet : newTablets) {
            newTabletIds.add(tablet.getId());
        }
        Map<Long, TabletRangePB> tabletRanges = createContiguousTabletRanges(newTabletIds);
        for (Tablet tablet : newTablets) {
            TabletRangePB rangePB = tabletRanges.get(tablet.getId());
            if (rangePB != null) {
                tablet.setRange(TabletRange.fromProto(rangePB));
            }
        }
        // Now call shareAdjacentTabletRangeBounds to optimize memory
        newMaterializedIndex.shareAdjacentTabletRangeBounds();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.FINISHED);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

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

    private static Tuple createTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    /**
     * Creates a TabletRangePB with proper bounds for contiguous ranges.
     * The ranges will be: [lowerValue, upperValue)
     */
    private static TabletRangePB createTabletRangePB(int lowerValue, int upperValue) {
        Range<Tuple> range = Range.of(createTuple(lowerValue), createTuple(upperValue), true, false);
        return new TabletRange(range).toProto();
    }

    /**
     * Creates a TabletRangePB from an existing tablet's range by looking up the
     * tablet in the table.
     * This is used when fallback to identical tablet - the new tablet should have
     * the same range as the old tablet.
     */
    private TabletRangePB createTabletRangePBFromOldTablet(long oldTabletId) {
        // Find the tablet with the given ID from the table
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            for (MaterializedIndex index : partition
                    .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    if (tablet.getId() == oldTabletId) {
                        TabletRange tabletRange = tablet.getRange();
                        return tabletRange == null ? new TabletRange().toProto() : tabletRange.toProto();
                    }
                }
            }
        }
        // Tablet not found, return unbounded range as fallback
        return new TabletRange().toProto();
    }

    /**
     * Creates contiguous TabletRangePB objects for a list of tablet IDs.
     * Returns a map from tabletId to TabletRangePB with proper contiguous ranges.
     */
    private static Map<Long, TabletRangePB> createContiguousTabletRanges(List<Long> tabletIds) {
        Map<Long, TabletRangePB> result = new HashMap<>();
        int baseValue = 0;
        int step = 100;
        for (int i = 0; i < tabletIds.size(); i++) {
            int lowerValue = baseValue + i * step;
            int upperValue = baseValue + (i + 1) * step;
            result.put(tabletIds.get(i), createTabletRangePB(lowerValue, upperValue));
        }
        return result;
    }

    /**
     * Creates split tablet ranges by dividing the old tablet's range into
     * contiguous sub-ranges.
     * This simulates what BE does when splitting a tablet.
     */
    private Map<Long, TabletRangePB> createSplitTabletRanges(long oldTabletId, List<Long> newTabletIds) {
        Map<Long, TabletRangePB> result = new HashMap<>();
        TabletRangePB oldRange = createTabletRangePBFromOldTablet(oldTabletId);

        // If old tablet has unbounded range (Range.all()), create arbitrary contiguous
        // ranges
        if (oldRange.lowerBound == null && oldRange.upperBound == null) {
            int step = 100;
            for (int i = 0; i < newTabletIds.size(); i++) {
                int lowerValue = i * step;
                int upperValue = (i + 1) * step;
                result.put(newTabletIds.get(i), createTabletRangePB(lowerValue, upperValue));
            }
        } else {
            // For bounded ranges, split evenly (simplified for test)
            // This is a simplified version - in reality, BE would split based on data
            // distribution
            int lowerValue = 0;
            int upperValue = 0;
            if (oldRange.lowerBound != null && oldRange.lowerBound.values != null
                    && !oldRange.lowerBound.values.isEmpty()) {
                lowerValue = Integer.parseInt(oldRange.lowerBound.values.get(0).value);
            }
            if (oldRange.upperBound != null && oldRange.upperBound.values != null
                    && !oldRange.upperBound.values.isEmpty()) {
                upperValue = Integer.parseInt(oldRange.upperBound.values.get(0).value);
            }
            int rangeSize = upperValue - lowerValue;
            int step = rangeSize / newTabletIds.size();
            for (int i = 0; i < newTabletIds.size(); i++) {
                int subLower = lowerValue + i * step;
                int subUpper = (i == newTabletIds.size() - 1) ? upperValue : lowerValue + (i + 1) * step;
                result.put(newTabletIds.get(i), createTabletRangePB(subLower, subUpper));
            }
        }
        return result;
    }

    private TabletReshardJob createTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();

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
