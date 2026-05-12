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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.Utils;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

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
        installLakeServiceMock(this::addDataDrivenRanges);

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
        installLakeServiceMock(this::addFallbackToIdenticalRanges);

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

    @Test
    public void testRunRunningUsesBackgroundComputeResource() throws Exception {
        SplitTabletJob splitJob = (SplitTabletJob) createTabletReshardJob();
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        ComputeResource expectedResource = WarehouseComputeResource.of(10086L);
        AtomicReference<ComputeResource> actualResource = new AtomicReference<>();

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResource(long tableId) {
                Assertions.assertEquals(table.getId(), tableId);
                return expectedResource;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public void publishVersion(List<Tablet> tablets, TxnInfoPB txnInfo,
                                       long baseVersion, long newVersion, Map<Long, Double> compactionScores,
                                       Map<Long, TabletRange> tabletRanges, ComputeResource computeResource,
                                       Map<Long, Long> tabletRowNums, boolean useAggregatePublish,
                                       List<VectorIndexBuildInfoPB> vectorIndexBuildInfos) {
                actualResource.set(computeResource);
            }
        };

        try {
            splitJob.run();
            Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, splitJob.getJobState());
            Assertions.assertSame(expectedResource, actualResource.get());
        } finally {
            splitJob.replayAbortedJob();
            physicalPartition.setNextVersion(physicalPartition.getVisibleVersion() + 1);
        }
    }

    // -------------------------------------------------------------------------
    // external boundaries end-to-end driver.
    //
    // Drives SplitTabletJobFactory.forExternalBoundaries(...) through PENDING
    // -> RUNNING -> FINISHED with a mocked BE that echoes the FE-supplied
    // ranges back verbatim (the BE contract: "external boundaries honored
    // verbatim"). Asserts the SplittingTablet carries newTabletRanges all the
    // way through toProto(), and that the resulting new tablets land with the
    // FE-supplied ranges.
    // -------------------------------------------------------------------------
    @Test
    public void testRunExternalBoundariesTabletReshardJob() throws Exception {
        installLakeServiceMock(this::addEchoedRanges);

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        Tablet oldTablet = materializedIndex.getTablets().get(0);
        long oldTabletId = oldTablet.getId();
        long oldVersion = physicalPartition.getVisibleVersion();
        int oldTabletCount = materializedIndex.getTablets().size();

        // Tests in this class share a single static table, and earlier @Test
        // methods can leave the latest index's tablets with bounded ranges.
        // Reset the chosen old tablet to Range.all() so the K=3 external boundaries ranges
        // below — built for a Range.all() parent — are deterministically
        // valid regardless of test order.
        oldTablet.setRange(new TabletRange());

        // K=3 external boundaries ranges that satisfy the BE-side structural validator on a
        // Range.all() parent: first.lower absent, last.upper absent, interior
        // boundaries set with closed-lower / open-upper, adjacent ranges meet
        // exactly. This mirrors the contract enforced in
        // TabletRangeHelper::validate_new_tablet_ranges (BE).
        List<TabletRange> newTabletRanges = List.of(
                tabletRangeUpperOnly(100),
                tabletRange(100, 200),
                tabletRangeLowerOnly(200));

        TabletReshardJob tabletReshardJob = SplitTabletJobFactory.forExternalBoundaries(
                db, table, oldTabletId, newTabletRanges);
        Assertions.assertNotNull(tabletReshardJob);

        // The SplittingTablet inside the job must carry the FE-supplied
        // ranges, and toProto() must serialize them into the wire shape that
        // BE's split_tablet dispatches on.
        SplittingTablet splittingTablet = findSplittingTablet(tabletReshardJob, oldTabletId);
        Assertions.assertEquals(newTabletRanges.size(), splittingTablet.getNewTabletIds().size());
        Assertions.assertEquals(newTabletRanges.size(), splittingTablet.getNewTabletRanges().size());

        ReshardingTabletInfoPB pb = splittingTablet.toProto();
        Assertions.assertNotNull(pb.splittingTabletInfo.newTabletRanges,
                "newTabletRanges must be present on the wire so BE dispatches to external boundaries");
        Assertions.assertEquals(newTabletRanges.size(), pb.splittingTabletInfo.newTabletRanges.size());

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(oldVersion + 1, physicalPartition.getVisibleVersion());

        // Net effect: the index gained K-1 tablets; the chosen old tablet is
        // gone; the K new tablets carry the FE-supplied ranges (echoed by the
        // mocked BE).
        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertEquals(oldTabletCount + (newTabletRanges.size() - 1),
                newMaterializedIndex.getTablets().size());
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(oldTabletId));

        // TabletRangePB (generated jprotobuf class) has no equals override;
        // compare the underlying Range<Tuple> which does. The values must
        // match the FE-supplied external boundaries ranges position-for-position.
        List<Long> ids = splittingTablet.getNewTabletIds();
        for (int idx = 0; idx < ids.size(); idx++) {
            Tablet newTablet = newMaterializedIndex.getTablet(ids.get(idx));
            Assertions.assertNotNull(newTablet);
            Assertions.assertNotNull(newTablet.getRange());
            Assertions.assertEquals(newTabletRanges.get(idx).getRange(), newTablet.getRange().getRange(),
                    "new tablet range must match the FE-supplied external boundaries range exactly (idx=" + idx + ")");
        }
    }

    // forExternalBoundaries rejects non-external boundaries-shaped input.
    @Test
    public void testForExternalBoundariesRejectsTooFewRanges() {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        long oldTabletId = physicalPartition.getLatestBaseIndex().getTablets().get(0).getId();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(db, table, oldTabletId,
                        List.of(tabletRange(0, 100))));
    }

    @Test
    public void testForExternalBoundariesRejectsUnknownTablet() {
        Assertions.assertThrows(StarRocksException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(db, table, /*oldTabletId=*/Long.MAX_VALUE,
                        List.of(tabletRange(0, 100), tabletRange(100, 200))));
    }

    // external boundaries must respect the tablet_reshard_max_split_count cap that the
    // data-driven path enforces via TabletReshardUtils.calcSplitCount.
    @Test
    public void testForExternalBoundariesRejectsTooManyRanges() {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        long oldTabletId = physicalPartition.getLatestBaseIndex().getTablets().get(0).getId();

        int oversize = Config.tablet_reshard_max_split_count + 1;
        List<TabletRange> tooMany = new ArrayList<>(oversize);
        for (int i = 0; i < oversize; i++) {
            tooMany.add(tabletRange(i * 100, (i + 1) * 100));
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(db, table, oldTabletId, tooMany));
    }

    // Installs a MockUp on MockLakeService that intercepts both publishVersion and
    // aggregatePublishVersion, populating the response's tabletRanges by applying
    // {@code rangeBuilder} to every ReshardingTabletInfoPB in the request(s). The
    // status is always OK; null reshardingTabletInfos are short-circuited.
    private void installLakeServiceMock(BiConsumer<ReshardingTabletInfoPB, Map<Long, TabletRangePB>> rangeBuilder) {
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
                for (ReshardingTabletInfoPB info : request.reshardingTabletInfos) {
                    rangeBuilder.accept(info, response.tabletRanges);
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
                    for (ReshardingTabletInfoPB info : publishRequest.reshardingTabletInfos) {
                        rangeBuilder.accept(info, response.tabletRanges);
                    }
                }
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    // Mock helper: data-driven BE response. Splitting tablets get evenly-distributed
    // sub-ranges of the old tablet; identical tablets retain the old tablet's range.
    private void addDataDrivenRanges(ReshardingTabletInfoPB info, Map<Long, TabletRangePB> out) {
        if (info.splittingTabletInfo != null) {
            Long oldTabletId = info.splittingTabletInfo.oldTabletId;
            List<Long> newTabletIds = info.splittingTabletInfo.newTabletIds;
            out.putAll(createSplitTabletRanges(oldTabletId, newTabletIds));
        }
        if (info.identicalTabletInfo != null) {
            Long oldTabletId = info.identicalTabletInfo.oldTabletId;
            Long newTabletId = info.identicalTabletInfo.newTabletId;
            out.put(newTabletId, createTabletRangePBFromOldTablet(oldTabletId));
        }
    }

    // Mock helper: BE-side fallback to identical tablet. Only the first new tablet
    // is published, with the old tablet's range (no actual split happened).
    private void addFallbackToIdenticalRanges(ReshardingTabletInfoPB info, Map<Long, TabletRangePB> out) {
        if (info.splittingTabletInfo == null) {
            return;
        }
        Long oldTabletId = info.splittingTabletInfo.oldTabletId;
        Long firstTabletId = info.splittingTabletInfo.newTabletIds.get(0);
        out.put(firstTabletId, createTabletRangePBFromOldTablet(oldTabletId));
    }

    // Mock helper: BE echo of the FE-supplied external boundaries ranges, plus identical-tablet
    // siblings retaining their original ranges. This matches the BE's success
    // contract — newTabletRanges honored verbatim into K new tablets.
    private void addEchoedRanges(ReshardingTabletInfoPB info, Map<Long, TabletRangePB> out) {
        if (info.splittingTabletInfo != null) {
            List<Long> newTabletIds = info.splittingTabletInfo.newTabletIds;
            List<TabletRangePB> ranges = info.splittingTabletInfo.newTabletRanges;
            if (ranges != null && ranges.size() == newTabletIds.size()) {
                for (int i = 0; i < newTabletIds.size(); i++) {
                    out.put(newTabletIds.get(i), ranges.get(i));
                }
            } else {
                // Data-driven path fallback (no external boundaries ranges on the wire).
                out.putAll(createSplitTabletRanges(info.splittingTabletInfo.oldTabletId, newTabletIds));
            }
        }
        if (info.identicalTabletInfo != null) {
            Long oldId = info.identicalTabletInfo.oldTabletId;
            Long newId = info.identicalTabletInfo.newTabletId;
            out.put(newId, createTabletRangePBFromOldTablet(oldId));
        }
    }

    private static SplittingTablet findSplittingTablet(TabletReshardJob job, long oldTabletId) {
        SplitTabletJob splitJob = (SplitTabletJob) job;
        for (ReshardingPhysicalPartition rpp : splitJob.getReshardingPhysicalPartitions().values()) {
            for (ReshardingMaterializedIndex rmi : rpp.getReshardingIndexes().values()) {
                for (ReshardingTablet rt : rmi.getReshardingTablets()) {
                    SplittingTablet st = rt.getSplittingTablet();
                    if (st != null && st.getOldTabletId() == oldTabletId) {
                        return st;
                    }
                }
            }
        }
        throw new AssertionError("SplittingTablet for old tablet " + oldTabletId + " not found");
    }

    private static TabletRange tabletRange(int lowerValue, int upperValue) {
        return new TabletRange(Range.of(createTuple(lowerValue), createTuple(upperValue), true, false));
    }

    // First range of a external boundaries list that splits a Range.all() parent: (-inf, upperValue).
    private static TabletRange tabletRangeUpperOnly(int upperValue) {
        return new TabletRange(Range.lt(createTuple(upperValue)));
    }

    // Last range of a external boundaries list that splits a Range.all() parent: [lowerValue, +inf).
    private static TabletRange tabletRangeLowerOnly(int lowerValue) {
        return new TabletRange(Range.ge(createTuple(lowerValue)));
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
