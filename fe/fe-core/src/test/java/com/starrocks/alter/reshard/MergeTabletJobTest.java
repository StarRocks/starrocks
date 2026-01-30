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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class MergeTabletJobTest {
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

        starRocksAssert.withDatabase("merge_test").useDatabase("merge_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("merge_test");

        String sql = "create table merge_test_table (key1 int, key2 varchar(10))\n" +
                "order by(key1)\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "merge_test_table");

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
                    if (reshardingTabletInfoPB.splittingTabletInfo != null) {
                        Map<Long, TabletRange> ranges = createSplitTabletRanges(
                                reshardingTabletInfoPB.splittingTabletInfo.oldTabletId,
                                reshardingTabletInfoPB.splittingTabletInfo.newTabletIds);
                        for (Map.Entry<Long, TabletRange> entry : ranges.entrySet()) {
                            response.tabletRanges.put(entry.getKey(), entry.getValue().toProto());
                        }
                    } else if (reshardingTabletInfoPB.mergingTabletInfo != null) {
                        response.tabletRanges.put(reshardingTabletInfoPB.mergingTabletInfo.newTabletId,
                                createMergedTabletRange(reshardingTabletInfoPB.mergingTabletInfo.oldTabletIds)
                                        .toProto());
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
                            Map<Long, TabletRange> ranges = createSplitTabletRanges(
                                    reshardingTabletInfoPB.splittingTabletInfo.oldTabletId,
                                    reshardingTabletInfoPB.splittingTabletInfo.newTabletIds);
                            for (Map.Entry<Long, TabletRange> entry : ranges.entrySet()) {
                                response.tabletRanges.put(entry.getKey(), entry.getValue().toProto());
                            }
                        } else if (reshardingTabletInfoPB.mergingTabletInfo != null) {
                            response.tabletRanges.put(reshardingTabletInfoPB.mergingTabletInfo.newTabletId,
                                    createMergedTabletRange(reshardingTabletInfoPB.mergingTabletInfo.oldTabletIds)
                                            .toProto());
                        }
                    }
                }

                return CompletableFuture.completedFuture(response);
            }
        };
    }

    @Test
    public void testRunMergeTabletReshardJob() throws Exception {
        TabletReshardJob splitJob = createSplitTabletReshardJob();
        splitJob.run();
        splitJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, splitJob.getJobState());

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex beforeMergeIndex = physicalPartition.getLatestBaseIndex();
        List<Long> oldTabletIds = new ArrayList<>();
        for (Tablet tablet : beforeMergeIndex.getTablets()) {
            oldTabletIds.add(tablet.getId());
        }
        Assertions.assertTrue(oldTabletIds.size() >= 2);

        long oldVersion = physicalPartition.getVisibleVersion();

        TabletReshardJob mergeJob = createMergeTabletReshardJob();
        Assertions.assertNotNull(mergeJob);

        mergeJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, mergeJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        mergeJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, mergeJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertEquals(oldVersion + 1, newVersion);

        MaterializedIndex afterMergeIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertNotEquals(beforeMergeIndex, afterMergeIndex);
        Assertions.assertTrue(afterMergeIndex.getTablets().size() < beforeMergeIndex.getTablets().size());

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Long tabletId : oldTabletIds) {
            Assertions.assertNull(invertedIndex.getTabletMeta(tabletId));
        }
        for (Tablet tablet : afterMergeIndex.getTablets()) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tablet.getId()));
        }
    }

    @Test
    public void testGettersAndParallelTablets() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        Assertions.assertEquals(db.getId(), mergeJob.getDbId());
        Assertions.assertEquals(table.getId(), mergeJob.getTableId());
        Assertions.assertNotNull(mergeJob.getReshardingPhysicalPartitions());
        Assertions.assertEquals(0, mergeJob.getTransactionId());
        Assertions.assertTrue(mergeJob.getParallelTablets() > 0);
    }

    @Test
    public void testRunPreparingNotReady() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        mergeJob.setJobState(TabletReshardJob.JobState.PREPARING);
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        ReshardingPhysicalPartition reshardingPhysicalPartition =
                mergeJob.getReshardingPhysicalPartitions().values().iterator().next();
        reshardingPhysicalPartition.setCommitVersion(physicalPartition.getVisibleVersion() + 2);

        mergeJob.runPreparingJob();
        Assertions.assertEquals(TabletReshardJob.JobState.PREPARING, mergeJob.getJobState());
    }

    @Test
    public void testRunPreparingMissingPartition() throws Exception {
        Map<Long, ReshardingPhysicalPartition> reshardingPartitions = new HashMap<>();
        reshardingPartitions.put(-1L, new ReshardingPhysicalPartition(-1L, new HashMap<>()));
        MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), reshardingPartitions);
        mergeJob.setJobState(TabletReshardJob.JobState.PREPARING);

        mergeJob.runPreparingJob();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, mergeJob.getJobState());
    }

    @Test
    public void testRunPendingMissingPartition() throws Exception {
        Map<Long, ReshardingPhysicalPartition> reshardingPartitions = new HashMap<>();
        reshardingPartitions.put(-1L, new ReshardingPhysicalPartition(-1L, new HashMap<>()));
        MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), reshardingPartitions);
        OlapTable.OlapTableState original = table.getState();
        try {
            mergeJob.runPendingJob();
            Assertions.assertEquals(TabletReshardJob.JobState.PREPARING, mergeJob.getJobState());
        } finally {
            table.setState(original);
        }
    }

    @Test
    public void testRunRunningMissingPartition() throws Exception {
        Map<Long, ReshardingPhysicalPartition> reshardingPartitions = new HashMap<>();
        reshardingPartitions.put(-1L, new ReshardingPhysicalPartition(-1L, new HashMap<>()));
        MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), reshardingPartitions);
        mergeJob.setJobState(TabletReshardJob.JobState.RUNNING);

        mergeJob.runRunningJob();
        Assertions.assertEquals(TabletReshardJob.JobState.CLEANING, mergeJob.getJobState());
    }

    @Test
    public void testRunRunningInProgress() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        mergeJob.setJobState(TabletReshardJob.JobState.RUNNING);
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        ReshardingPhysicalPartition reshardingPhysicalPartition =
                mergeJob.getReshardingPhysicalPartitions().values().iterator().next();
        reshardingPhysicalPartition.setCommitVersion(physicalPartition.getVisibleVersion() + 1);
        reshardingPhysicalPartition.setPublishFuture(new CompletableFuture<>());

        mergeJob.runRunningJob();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, mergeJob.getJobState());
    }

    @Test
    public void testRunRunningUnknownState() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        mergeJob.setJobState(TabletReshardJob.JobState.RUNNING);
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        ReshardingPhysicalPartition reshardingPhysicalPartition =
                mergeJob.getReshardingPhysicalPartitions().values().iterator().next();
        reshardingPhysicalPartition.setCommitVersion(physicalPartition.getVisibleVersion() + 1);

        new MockUp<ReshardingPhysicalPartition>() {
            @Mock
            public ReshardingPhysicalPartition.PublishResult getPublishResult() {
                return new ReshardingPhysicalPartition.PublishResult(null, null);
            }
        };

        Assertions.assertThrows(TabletReshardException.class, mergeJob::runRunningJob);
    }

    @Test
    public void testRunCleaningNotFinished() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        mergeJob.setJobState(TabletReshardJob.JobState.CLEANING);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIds) {
                return false;
            }
        };

        mergeJob.runCleaningJob();
        Assertions.assertEquals(TabletReshardJob.JobState.CLEANING, mergeJob.getJobState());
    }

    @Test
    public void testRunCleaningIgnoreAnalysisException() throws Exception {
        Map<Long, ReshardingPhysicalPartition> reshardingPartitions = new HashMap<>();
        reshardingPartitions.put(-1L, new ReshardingPhysicalPartition(-1L, new HashMap<>()));
        MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), reshardingPartitions);
        mergeJob.setJobState(TabletReshardJob.JobState.CLEANING);
        OlapTable.OlapTableState original = table.getState();
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIds)
                    throws AnalysisException {
                throw new AnalysisException("mock");
            }
        };

        try {
            mergeJob.runCleaningJob();
            Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, mergeJob.getJobState());
        } finally {
            table.setState(original);
        }
    }

    @Test
    public void testRunAbortingJobCatch() throws Exception {
        MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), createMergeTabletReshardJob().getReshardingPhysicalPartitions()) {
            @Override
            protected void unregisterReshardingTablets() {
                throw new RuntimeException("mock");
            }
        };
        mergeJob.setJobState(TabletReshardJob.JobState.ABORTING);
        mergeJob.runAbortingJob();
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTED, mergeJob.getJobState());
    }

    @Test
    public void testReplayAbortingAndAborted() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        mergeJob.replayAbortingJob();
        mergeJob.setJobState(TabletReshardJob.JobState.ABORTED);
        mergeJob.replayAbortedJob();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testRegisterReshardingTabletsOnRestart() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();

        mergeJob.setJobState(TabletReshardJob.JobState.PENDING);
        mergeJob.registerReshardingTabletsOnRestart();
        ReshardingTablet sample = mergeJob.getReshardingPhysicalPartitions().values().iterator().next()
                .getReshardingIndexes().values().iterator().next().getReshardingTablets().get(0);
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                .unregisterReshardingTablet(sample.getOldTabletIds().get(0));
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                .getReshardingTablet(sample.getOldTabletIds().get(0), 1));

        mergeJob.setJobState(TabletReshardJob.JobState.RUNNING);
        mergeJob.registerReshardingTabletsOnRestart();
        Assertions.assertNotNull(GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                .getReshardingTablet(sample.getOldTabletIds().get(0), 1));

        mergeJob.unregisterReshardingTablets();
    }

    @Test
    public void testToStringAndGetInfo() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        mergeJob.errorMessage = "err";
        mergeJob.transactionId = 7;
        mergeJob.endTransactionId = 9;
        mergeJob.setJobState(TabletReshardJob.JobState.FINISHED);
        String desc = mergeJob.toString();
        Assertions.assertTrue(desc.contains("error_message"));
        Assertions.assertNotNull(mergeJob.getInfo());

        MergeTabletJob runningJob = createMergeTabletReshardJob();
        String runningDesc = runningJob.toString();
        Assertions.assertTrue(runningDesc.contains("state_started_time"));

        MergeTabletJob invalidJob = new MergeTabletJob(1, -1, -1, new HashMap<>());
        Assertions.assertNotNull(invalidJob.getInfo());

        MergeTabletJob invalidTableJob = new MergeTabletJob(2, db.getId(), -1, new HashMap<>());
        Assertions.assertNotNull(invalidTableJob.getInfo());
    }

    @Test
    public void testSetTableStateMismatch() throws Exception {
        OlapTable.OlapTableState original = table.getState();
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                    db.getId(), table.getId(), new HashMap<>());
            Assertions.assertThrows(TabletReshardException.class, mergeJob::runPendingJob);
        } finally {
            table.setState(original);
        }
    }

    @Test
    public void testPublishVersionThrows() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        new MockUp<Utils>() {
            @Mock
            public void publishVersion(List<Tablet> tablets, TxnInfoPB txnInfo,
                                       long baseVersion, long newVersion, Map<Long, Double> compactionScores,
                                       Map<Long, TabletRange> tabletRanges,
                                       ComputeResource computeResource,
                                       Map<Long, Long> tabletRowNums,
                                       boolean useAggregatePublish) throws Exception {
                throw new RuntimeException("mock");
            }
        };

        Assertions.assertThrows(TabletReshardException.class,
                () -> Deencapsulation.invoke(mergeJob, "publishVersion", List.of(), 2L, false));
    }

    @Test
    public void testPrivateHelpersWithMissingPartitions() throws Exception {
        Map<Long, ReshardingPhysicalPartition> missingPartition = new HashMap<>();
        missingPartition.put(-1L, new ReshardingPhysicalPartition(-1L, new HashMap<>()));
        MergeTabletJob addIndexJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), missingPartition);
        Deencapsulation.invoke(addIndexJob, "addNewMaterializedIndexes");

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex oldIndex = physicalPartition.getLatestBaseIndex();
        ReshardingMaterializedIndex badIndex = new ReshardingMaterializedIndex(-1L,
                new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                        oldIndex.getMetaId(), IndexState.NORMAL, oldIndex.getShardGroupId()),
                new ArrayList<>());
        Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
        reshardingIndexes.put(badIndex.getMaterializedIndexId(), badIndex);
        Map<Long, ReshardingPhysicalPartition> reshardingPartitions = new HashMap<>();
        reshardingPartitions.put(-1L, new ReshardingPhysicalPartition(-1L, new HashMap<>()));
        reshardingPartitions.put(physicalPartition.getId(),
                new ReshardingPhysicalPartition(physicalPartition.getId(), reshardingIndexes));
        MergeTabletJob removeIndexJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), reshardingPartitions);
        Deencapsulation.invoke(removeIndexJob, "removeOldMaterializedIndexes");
    }

    @Test
    public void testGetLockedTableNotFound() throws Exception {
        MergeTabletJob mergeJob = new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), -1, new HashMap<>());
        Assertions.assertThrows(TabletReshardException.class, mergeJob::runPendingJob);
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTING, mergeJob.getJobState());
        Assertions.assertEquals("Table not found", mergeJob.getErrorMessage());
    }

    @Test
    public void testReplayMergeTabletReshardJob() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        Assertions.assertNotNull(mergeJob);

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, mergeJob.getJobState());
        mergeJob.replay();

        mergeJob.setJobState(TabletReshardJob.JobState.PREPARING);
        mergeJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        mergeJob.setJobState(TabletReshardJob.JobState.RUNNING);
        mergeJob.replay();

        mergeJob.setJobState(TabletReshardJob.JobState.CLEANING);
        mergeJob.replay();

        mergeJob.setJobState(TabletReshardJob.JobState.FINISHED);
        mergeJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertEquals(oldVersion + 1, newVersion);

        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertNotEquals(materializedIndex, newMaterializedIndex);
    }

    @Test
    public void testAbortMergeTabletReshardJob() throws Exception {
        MergeTabletJob mergeJob = createMergeTabletReshardJob();
        Assertions.assertNotNull(mergeJob);

        mergeJob.abort("test abort");
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTING, mergeJob.getJobState());

        mergeJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTED, mergeJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    private TabletReshardJob createSplitTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();

        long tabletId = materializedIndex.getTablets().get(0).getId();
        TabletList tabletList = new TabletList(List.of(tabletId));

        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2");
        SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
        clause.setTabletReshardTargetSize(-2);

        TabletReshardJobFactory factory = new SplitTabletJobFactory(db, table, clause);
        return factory.createTabletReshardJob();
    }

    private static Tuple createTuple(int value) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    private static TabletRange createTabletRange(int lowerValue, int upperValue) {
        Range<Tuple> range = Range.of(createTuple(lowerValue), createTuple(upperValue), true, false);
        return new TabletRange(range);
    }

    private static Integer extractBoundValue(TabletRange range, boolean lower) {
        if (range == null) {
            return null;
        }
        Tuple tuple = lower ? range.getRange().getLowerBound() : range.getRange().getUpperBound();
        if (tuple == null || tuple.getValues() == null || tuple.getValues().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(tuple.getValues().get(0).getStringValue());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static TabletRange getTabletRangeFromOldTablet(long oldTabletId) {
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            for (MaterializedIndex index : partition
                    .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    if (tablet.getId() == oldTabletId) {
                        TabletRange tabletRange = tablet.getRange();
                        return (tabletRange == null || tabletRange.getRange().isAll()) ? null : tabletRange;
                    }
                }
            }
        }
        return null;
    }

    private static Map<Long, TabletRange> createContiguousTabletRanges(List<Long> tabletIds, int baseValue, int step) {
        Map<Long, TabletRange> result = new HashMap<>();
        for (int i = 0; i < tabletIds.size(); i++) {
            int lowerValue = baseValue + i * step;
            int upperValue = baseValue + (i + 1) * step;
            result.put(tabletIds.get(i), createTabletRange(lowerValue, upperValue));
        }
        return result;
    }

    private static Map<Long, TabletRange> createSplitTabletRanges(long oldTabletId, List<Long> newTabletIds) {
        TabletRange oldRange = getTabletRangeFromOldTablet(oldTabletId);
        if (oldRange == null) {
            return createContiguousTabletRanges(newTabletIds, 0, 100);
        }

        Integer lowerValue = extractBoundValue(oldRange, true);
        Integer upperValue = extractBoundValue(oldRange, false);
        if (lowerValue == null || upperValue == null || upperValue <= lowerValue) {
            return createContiguousTabletRanges(newTabletIds, 0, 100);
        }

        int rangeSize = upperValue - lowerValue;
        int step = Math.max(1, rangeSize / newTabletIds.size());
        Map<Long, TabletRange> result = new HashMap<>();
        for (int i = 0; i < newTabletIds.size(); i++) {
            int subLower = lowerValue + i * step;
            int subUpper = (i == newTabletIds.size() - 1) ? upperValue : lowerValue + (i + 1) * step;
            result.put(newTabletIds.get(i), createTabletRange(subLower, subUpper));
        }
        return result;
    }

    private static TabletRange createMergedTabletRange(List<Long> oldTabletIds) {
        if (oldTabletIds == null || oldTabletIds.isEmpty()) {
            return createTabletRange(0, 100);
        }
        TabletRange firstRange = getTabletRangeFromOldTablet(oldTabletIds.get(0));
        TabletRange lastRange = getTabletRangeFromOldTablet(oldTabletIds.get(oldTabletIds.size() - 1));
        if (firstRange == null || lastRange == null) {
            return createTabletRange(0, oldTabletIds.size() * 100);
        }

        Tuple lowerBound = firstRange.getRange().getLowerBound();
        Tuple upperBound = lastRange.getRange().getUpperBound();
        if (lowerBound == null || upperBound == null) {
            return createTabletRange(0, oldTabletIds.size() * 100);
        }

        return new TabletRange(Range.of(lowerBound, upperBound,
                firstRange.getRange().isLowerBoundIncluded(),
                lastRange.getRange().isUpperBoundIncluded()));
    }

    private MergeTabletJob createMergeTabletReshardJob() throws Exception {
        ensureSplitTablets();

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex oldIndex = physicalPartition.getLatestBaseIndex();
        List<Tablet> oldTablets = oldIndex.getTablets();
        Preconditions.checkState(oldTablets.size() >= 2, "Not enough tablets for merge");

        List<Long> mergeOldTabletIds = new ArrayList<>();
        mergeOldTabletIds.add(oldTablets.get(0).getId());
        mergeOldTabletIds.add(oldTablets.get(1).getId());
        MergingTablet mergingTablet = new MergingTablet(mergeOldTabletIds, GlobalStateMgr.getCurrentState().getNextId());

        List<ReshardingTablet> reshardingTablets = new ArrayList<>();
        reshardingTablets.add(mergingTablet);
        for (int i = 2; i < oldTablets.size(); i++) {
            long oldTabletId = oldTablets.get(i).getId();
            reshardingTablets.add(new IdenticalTablet(oldTabletId, GlobalStateMgr.getCurrentState().getNextId()));
        }

        MaterializedIndex newIndex = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                oldIndex.getMetaId(), IndexState.NORMAL, oldIndex.getShardGroupId());
        for (ReshardingTablet reshardingTablet : reshardingTablets) {
            TabletRange initialRange = getInitialTabletRange(oldIndex, reshardingTablet);
            for (long newTabletId : reshardingTablet.getNewTabletIds()) {
                newIndex.addTablet(new LakeTablet(newTabletId, initialRange), null, false);
            }
        }

        Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
        reshardingIndexes.put(oldIndex.getId(),
                new ReshardingMaterializedIndex(oldIndex.getId(), newIndex, reshardingTablets));
        Map<Long, ReshardingPhysicalPartition> reshardingPartitions = new HashMap<>();
        reshardingPartitions.put(physicalPartition.getId(),
                new ReshardingPhysicalPartition(physicalPartition.getId(), reshardingIndexes));

        createNewShards(physicalPartition, newIndex, reshardingTablets);

        return new MergeTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), reshardingPartitions);
    }

    private void ensureSplitTablets() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        if (materializedIndex.getTablets().size() >= 2) {
            return;
        }

        TabletReshardJob splitJob = createSplitTabletReshardJob();
        splitJob.run();
        splitJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, splitJob.getJobState());
    }

    private void createNewShards(PhysicalPartition physicalPartition,
            MaterializedIndex newIndex,
            List<ReshardingTablet> reshardingTablets) throws Exception {
        Map<Long, List<Long>> oldToNewTabletIds = new HashMap<>();
        for (ReshardingTablet reshardingTablet : reshardingTablets) {
            oldToNewTabletIds.put(reshardingTablet.getFirstOldTabletId(), reshardingTablet.getNewTabletIds());
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartition.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(newIndex.getId()));

        GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                oldToNewTabletIds,
                table.getPartitionFilePathInfo(physicalPartition.getId()),
                table.getPartitionFileCacheInfo(physicalPartition.getId()),
                newIndex.getShardGroupId(),
                properties, WarehouseManager.DEFAULT_RESOURCE);
    }

    private TabletRange getInitialTabletRange(MaterializedIndex oldIndex, ReshardingTablet reshardingTablet) {
        MergingTablet mergingTablet = reshardingTablet.getMergingTablet();
        if (mergingTablet == null) {
            Tablet oldTablet = oldIndex.getTablet(reshardingTablet.getFirstOldTabletId());
            Preconditions.checkNotNull(oldTablet, "Not found tablet " + reshardingTablet.getFirstOldTabletId());
            return oldTablet.getRange();
        }

        List<Long> oldTabletIds = mergingTablet.getOldTabletIds();
        Tablet firstTablet = oldIndex.getTablet(oldTabletIds.get(0));
        Tablet lastTablet = oldIndex.getTablet(oldTabletIds.get(oldTabletIds.size() - 1));
        Preconditions.checkNotNull(firstTablet, "Not found tablet " + oldTabletIds.get(0));
        Preconditions.checkNotNull(lastTablet, "Not found tablet " + oldTabletIds.get(oldTabletIds.size() - 1));

        Range<Tuple> first = firstTablet.getRange().getRange();
        Range<Tuple> last = lastTablet.getRange().getRange();
        return new TabletRange(
                Range.of(first.getLowerBound(), last.getUpperBound(),
                        first.isLowerBoundIncluded(), last.isUpperBoundIncluded()));
    }

}
