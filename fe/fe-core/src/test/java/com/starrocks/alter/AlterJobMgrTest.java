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

package com.starrocks.alter;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AlterJobMgrTest {

    @Test
    public void testGetRunningAlterJobCount() {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler();
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        // warehouse 0
        SchemaChangeJobV2 job1 = new SchemaChangeJobV2(1L, 20001, 30001, "test", 3600000);
        job1.setComputeResource(WarehouseComputeResource.of(0));
        job1.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeHandler.addAlterJobV2(job1);
        SchemaChangeJobV2 job2 = new SchemaChangeJobV2(2L, 20001, 30001, "test", 3600000);
        job2.setComputeResource(WarehouseComputeResource.of(0));
        job2.setJobState(AlterJobV2.JobState.CANCELLED);
        schemaChangeHandler.addAlterJobV2(job2);
        SchemaChangeJobV2 job3 = new SchemaChangeJobV2(3L, 20001, 30001, "test", 3600000);
        job3.setComputeResource(WarehouseComputeResource.of(0));
        job3.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(job3);

        // warehouse 1
        SchemaChangeJobV2 job4 = new SchemaChangeJobV2(4L, 20001, 30001, "test", 3600000);
        job4.setComputeResource(WarehouseComputeResource.of(1));
        job4.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeHandler.addAlterJobV2(job4);
        SchemaChangeJobV2 job5 = new SchemaChangeJobV2(5L, 20001, 30001, "test", 3600000);
        job5.setComputeResource(WarehouseComputeResource.of(1));
        job5.setJobState(AlterJobV2.JobState.CANCELLED);
        schemaChangeHandler.addAlterJobV2(job5);
        SchemaChangeJobV2 job6 = new SchemaChangeJobV2(6L, 20001, 30001, "test", 3600000);
        job6.setComputeResource(WarehouseComputeResource.of(1));
        job6.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(job6);
        RollupJobV2 job7 = buildRollupJob(7);
        job7.setComputeResource(WarehouseComputeResource.of(1));
        job7.setJobState(AlterJobV2.JobState.FINISHED);
        materializedViewHandler.addAlterJobV2(job7);
        RollupJobV2 job8 = buildRollupJob(8);
        job8.setComputeResource(WarehouseComputeResource.of(1));
        job8.setJobState(AlterJobV2.JobState.CANCELLED);
        materializedViewHandler.addAlterJobV2(job8);
        RollupJobV2 job9 = buildRollupJob(9);
        job9.setComputeResource(WarehouseComputeResource.of(1));
        job9.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(job9);

        // warehouse 2
        RollupJobV2 job10 = buildRollupJob(10);
        job10.setComputeResource(WarehouseComputeResource.of(2));
        job10.setJobState(AlterJobV2.JobState.FINISHED);
        materializedViewHandler.addAlterJobV2(job10);
        RollupJobV2 job11 = buildRollupJob(11);
        job11.setComputeResource(WarehouseComputeResource.of(2));
        job11.setJobState(AlterJobV2.JobState.CANCELLED);
        materializedViewHandler.addAlterJobV2(job11);
        RollupJobV2 job12 = buildRollupJob(12);
        job12.setComputeResource(WarehouseComputeResource.of(2));
        job12.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(job12);

        Map<Long, Long> result = alterJobMgr.getRunningAlterJobCount();
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals((Long) 1L, result.get(0L));
        Assertions.assertEquals((Long) 2L, result.get(1L));
        Assertions.assertEquals((Long) 1L, result.get(2L));
    }

    @Test
    public void testStopBestEffortStopsAllHandlers() {
        // AlterJobMgr is an aggregator; stopBestEffort() must reach every wired handler so each one's
        // worker is asked to stop. Observe via isStopRequested(), which the stop request sets and which
        // needs no started worker.
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler();
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        alterJobMgr.stopBestEffort();

        Assertions.assertTrue(schemaChangeHandler.isStopRequested(), "schemaChangeHandler must be stopped");
        Assertions.assertTrue(materializedViewHandler.isStopRequested(), "materializedViewHandler must be stopped");
        Assertions.assertTrue(systemHandler.isStopRequested(), "clusterHandler must be stopped");
    }

    @Test
    public void testStopBestEffortContinuesWhenOneHandlerThrows() {
        // A misbehaving handler must not abort the demotion stop; the remaining handlers still need to
        // be stopped. stopBestEffort() evaluates interruptOnStop(), so a throwing interruptOnStop bubbles
        // out of that handler's stopBestEffort - AlterJobMgr's per-handler try/catch is the safety net.
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler();

        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler() {
            @Override
            protected boolean interruptOnStop() {
                throw new RuntimeException("simulated handler stop failure");
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        alterJobMgr.stopBestEffort();

        Assertions.assertTrue(materializedViewHandler.isStopRequested(), "materializedViewHandler must still be stopped");
        Assertions.assertTrue(systemHandler.isStopRequested(), "clusterHandler must still be stopped");
    }

    @Test
    public void testStopBestEffortContinuesWhenMaterializedViewHandlerThrows() {
        // Mirror of the previous test, but the middle handler is the one that misbehaves. Covers the
        // materializedViewHandler-specific try/catch arm so a failure there does not skip the
        // SystemHandler stop.
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        SystemHandler systemHandler = new SystemHandler();

        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler() {
            @Override
            protected boolean interruptOnStop() {
                throw new RuntimeException("simulated materializedView stop failure");
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        alterJobMgr.stopBestEffort();

        Assertions.assertTrue(schemaChangeHandler.isStopRequested(), "schemaChangeHandler must still be stopped");
        Assertions.assertTrue(systemHandler.isStopRequested(), "clusterHandler must still be stopped");
    }

    @Test
    public void testStopBestEffortToleratesClusterHandlerThrowing() {
        // The last handler in the fan-out is SystemHandler (a.k.a. clusterHandler). Its try/catch arm
        // has no successor to verify, so we just assert the call completes without propagating the
        // exception - the safety net must absorb it.
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler() {
            @Override
            protected boolean interruptOnStop() {
                throw new RuntimeException("simulated cluster stop failure");
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        // Should not throw - the SystemHandler-arm catch swallows.
        Assertions.assertDoesNotThrow(() -> alterJobMgr.stopBestEffort());
    }

    @Test
    public void testLoadPrunesDoneRollupJobFromImage() throws Exception {
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        AlterJobMgr alterJobMgr =
                new AlterJobMgr(new SchemaChangeHandler(), materializedViewHandler, new SystemHandler());
        RollupJobV2 doneJob = buildRollupJobWithTabletData(21);
        doneJob.setJobState(AlterJobV2.JobState.FINISHED);
        doneJob.setFinishedTimeMs(System.currentTimeMillis());
        materializedViewHandler.addAlterJobV2(doneJob);
        RollupJobV2 runningJob = buildRollupJobWithTabletData(22);
        runningJob.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(runningJob);
        // SHOW ALTER of the loaded job must match that of a job that went through the same serialization.
        AlterJobV2 doneCopy =
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(doneJob, AlterJobV2.class), AlterJobV2.class);
        List<List<Comparable>> expectedRow = new ArrayList<>();
        doneCopy.getInfo(expectedRow);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr =
                new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restoredMgr.load(reader);
        reader.close();

        Map<Long, AlterJobV2> restoredJobs = restoredMgr.getMaterializedViewHandler().getAlterJobsV2();
        RollupJobV2 restoredDone = (RollupJobV2) restoredJobs.get(doneJob.getJobId());
        Assertions.assertTrue(restoredDone.getPartitionIdToRollupIndex().isEmpty());
        Assertions.assertTrue(baseTabletIds(restoredDone).isEmpty());
        List<List<Comparable>> restoredRow = new ArrayList<>();
        restoredDone.getInfo(restoredRow);
        Assertions.assertEquals(expectedRow, restoredRow);
        RollupJobV2 restoredRunning = (RollupJobV2) restoredJobs.get(runningJob.getJobId());
        Assertions.assertFalse(restoredRunning.getPartitionIdToRollupIndex().isEmpty());
        Assertions.assertFalse(baseTabletIds(restoredRunning).isEmpty());
    }

    @Test
    public void testLoadPrunesDoneJobsOfEveryFamilyFromImage() throws Exception {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, new SystemHandler());
        // A done job of every family, with the fields that its pruneMeta() drops.
        Map<AlterJobV2, String[]> doneJobs = new LinkedHashMap<>();

        SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2(101, 20001, 30001, "test", 3600000);
        fillSchemaChangeMaps(schemaChangeJob);
        doneJobs.put(schemaChangeJob, new String[] {"physicalPartitionIndexTabletMap", "physicalPartitionIndexMap",
                "indexMetaIdToSchema", "indexMetaIdToShortKey"});

        LakeTableSchemaChangeJob lakeSchemaChangeJob = new LakeTableSchemaChangeJob(102, 20001, 30001, "test", 3600000);
        fillSchemaChangeMaps(lakeSchemaChangeJob);
        doneJobs.put(lakeSchemaChangeJob, new String[] {"physicalPartitionIndexTabletMap", "physicalPartitionIndexMap",
                "indexMetaIdToSchema", "indexMetaIdToShortKey"});

        LakeTableAlterMetaJob alterMetaJob = new LakeTableAlterMetaJob(103, 20001, 30001, "test", 3600000,
                TTabletMetaType.ENABLE_PERSISTENT_INDEX, true, "LOCAL");
        fillPartitionIndexMap(alterMetaJob);
        doneJobs.put(alterMetaJob, new String[] {"physicalPartitionIndexMap"});

        LakeTableAsyncFastSchemaChangeJob asyncFastSchemaChangeJob =
                new LakeTableAsyncFastSchemaChangeJob(104, 20001, 30001, "test", 3600000);
        fillPartitionIndexMap(asyncFastSchemaChangeJob);
        asyncFastSchemaChangeJob.setIndexTabletSchema(2L, "test", SchemaInfo.newBuilder()
                .setId(1L)
                .setVersion(1)
                .setSchemaHash(10)
                .setKeysType(KeysType.DUP_KEYS)
                .setShortKeyColumnCount((short) 1)
                .setStorageType(TStorageType.COLUMN)
                .addColumn(new Column("c1", IntegerType.INT))
                .build());
        doneJobs.put(asyncFastSchemaChangeJob, new String[] {"physicalPartitionIndexMap"});

        LakeTableAddIndexJob addIndexJob =
                new LakeTableAddIndexJob(105, 20001, 30001, "test", 3600000, new ArrayList<>(), new ArrayList<>());
        addIndexJob.partitionToTablets.put(1L, new ArrayList<>(List.of(2L, 3L)));
        addIndexJob.tabletToIndexMetaId.put(2L, 4L);
        doneJobs.put(addIndexJob, new String[] {"partitionToTablets", "tabletToIndexMetaId"});

        LakeRangeRewriteSchemaChangeJob rangeRewriteJob =
                new LakeRangeRewriteSchemaChangeJob(106, 20001, 30001, "test", 3600000);
        fillPartitionStates(rangeRewriteJob);
        doneJobs.put(rangeRewriteJob, new String[] {"partitionStates"});

        LakeMvSortKeyRewriteJob mvSortKeyRewriteJob = new LakeMvSortKeyRewriteJob(107, 20001, 30001, "test", 3600000);
        fillPartitionStates(mvSortKeyRewriteJob);
        doneJobs.put(mvSortKeyRewriteJob, new String[] {"partitionStates"});

        OptimizeJobV2 optimizeJob = new OptimizeJobV2(108, 20001, 30001, "test", 3600000);
        fillPartitionLists(optimizeJob);
        doneJobs.put(optimizeJob, new String[] {"tmpPartitionIds", "sourcePartitionNames", "tmpPartitionNames"});

        OnlineOptimizeJobV2 onlineOptimizeJob = new OnlineOptimizeJobV2(109, 20001, 30001, "test", 3600000);
        fillPartitionLists(onlineOptimizeJob);
        doneJobs.put(onlineOptimizeJob,
                new String[] {"tmpPartitionIds", "sourcePartitionNames", "tmpPartitionNames"});

        MergePartitionJob mergePartitionJob = new MergePartitionJob(110, 20001, 30001, "test", 3600000);
        Multimap<Long, Long> tempPartitionIds = Deencapsulation.getField(mergePartitionJob,
                "tempPartitionIdToSourcePartitionIds");
        tempPartitionIds.put(1L, 2L);
        tempPartitionIds.put(1L, 3L);
        Multimap<String, String> tempPartitionNames = Deencapsulation.getField(mergePartitionJob,
                "tempPartitionNameToSourcePartitionNames");
        tempPartitionNames.put("p_tmp", "p1");
        doneJobs.put(mergePartitionJob,
                new String[] {"tempPartitionIdToSourcePartitionIds", "tempPartitionNameToSourcePartitionNames"});

        // The rollup families live in the materialized view handler.
        Map<AlterJobV2, String[]> doneRollupJobs = new LinkedHashMap<>();
        RollupJobV2 rollupJob = buildRollupJobWithTabletData(201);
        doneRollupJobs.put(rollupJob,
                new String[] {"physicalPartitionIdToRollupIndex", "physicalPartitionIdToBaseRollupTabletIdMap"});

        LakeRollupJob lakeRollupJob = new LakeRollupJob(202, 20001, 30001, "test", 3600000, 0, 0, "test", "test", 1,
                null, null, 1, 1, null, (short) 0, null, null, false);
        lakeRollupJob.addMVIndex(40001L, new MaterializedIndex(40002L, MaterializedIndex.IndexState.SHADOW));
        lakeRollupJob.addTabletIdMap(40001L, 40003L, 40004L);
        doneRollupJobs.put(lakeRollupJob,
                new String[] {"physicalPartitionIdToRollupIndex", "physicalPartitionIdToBaseRollupTabletIdMap"});

        LakeRangeRollupJob rangeRollupJob = new LakeRangeRollupJob(203, 20001, 30001, "test", 3600000);
        fillPartitionStates(rangeRollupJob);
        doneRollupJobs.put(rangeRollupJob, new String[] {"partitionStates"});

        // SHOW ALTER of a loaded job must match that of the same job after a plain serialization round trip.
        Map<Long, List<List<Comparable>>> expectedRows = new HashMap<>();
        List<AlterJobV2> allDoneJobs = new ArrayList<>(doneJobs.keySet());
        allDoneJobs.addAll(doneRollupJobs.keySet());
        for (int i = 0; i < allDoneJobs.size(); i++) {
            AlterJobV2 job = allDoneJobs.get(i);
            job.setJobState(i % 2 == 0 ? AlterJobV2.JobState.FINISHED : AlterJobV2.JobState.CANCELLED);
            job.setFinishedTimeMs(System.currentTimeMillis());
            AlterJobV2 copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, AlterJobV2.class), AlterJobV2.class);
            // The baseline has to hold the data that the load is expected to drop.
            String[] heavyFields = doneJobs.containsKey(job) ? doneJobs.get(job) : doneRollupJobs.get(job);
            for (String field : heavyFields) {
                Assertions.assertFalse(isEmpty(Deencapsulation.getField(copy, field)),
                        job.getClass().getSimpleName() + "." + field);
            }
            List<List<Comparable>> rows = new ArrayList<>();
            copy.getInfo(rows);
            Assertions.assertFalse(rows.isEmpty(), job.getClass().getSimpleName());
            expectedRows.put(job.getJobId(), rows);
        }
        doneJobs.keySet().forEach(schemaChangeHandler::addAlterJobV2);
        doneRollupJobs.keySet().forEach(materializedViewHandler::addAlterJobV2);

        // A pending job is replayed on load, which registers its tablets, so it keeps its maps.
        SchemaChangeJobV2 pendingJob = new SchemaChangeJobV2(111, 20001, 30001, "test", 3600000);
        fillSchemaChangeMaps(pendingJob);
        pendingJob.setJobState(AlterJobV2.JobState.PENDING);
        schemaChangeHandler.addAlterJobV2(pendingJob);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr =
                new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
        AtomicInteger replayedPendingJobs = new AtomicInteger();
        new MockUp<SchemaChangeJobV2>() {
            @Mock
            public void replay(AlterJobV2 replayedJob) {
                Assertions.assertEquals(pendingJob.getJobId(), replayedJob.getJobId());
                replayedPendingJobs.incrementAndGet();
            }
        };
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restoredMgr.load(reader);
        reader.close();

        Assertions.assertEquals(1, replayedPendingJobs.get());
        Map<Long, AlterJobV2> restoredSchemaChangeJobs = restoredMgr.getSchemaChangeHandler().getAlterJobsV2();
        Map<Long, AlterJobV2> restoredRollupJobs = restoredMgr.getMaterializedViewHandler().getAlterJobsV2();
        Assertions.assertEquals(doneJobs.size() + 1, restoredSchemaChangeJobs.size());
        Assertions.assertEquals(doneRollupJobs.size(), restoredRollupJobs.size());
        for (Map.Entry<AlterJobV2, String[]> entry : doneJobs.entrySet()) {
            assertRestoredPruned(restoredSchemaChangeJobs, entry.getKey(), entry.getValue(), expectedRows);
        }
        for (Map.Entry<AlterJobV2, String[]> entry : doneRollupJobs.entrySet()) {
            assertRestoredPruned(restoredRollupJobs, entry.getKey(), entry.getValue(), expectedRows);
        }
        SchemaChangeJobV2 restoredPending = (SchemaChangeJobV2) restoredSchemaChangeJobs.get(pendingJob.getJobId());
        Assertions.assertFalse(restoredPending.physicalPartitionIndexTabletMap.isEmpty());
        Assertions.assertFalse(restoredPending.physicalPartitionIndexMap.isEmpty());
        Assertions.assertFalse(restoredPending.indexMetaIdToSchema.isEmpty());
        Assertions.assertFalse(restoredPending.indexMetaIdToShortKey.isEmpty());
    }

    private static void assertRestoredPruned(Map<Long, AlterJobV2> restoredJobs, AlterJobV2 original,
                                             String[] heavyFields, Map<Long, List<List<Comparable>>> expectedRows) {
        String name = original.getClass().getSimpleName();
        AlterJobV2 restored = restoredJobs.get(original.getJobId());
        Assertions.assertNotNull(restored, name);
        Assertions.assertEquals(original.getJobState(), restored.getJobState(), name);
        for (String field : heavyFields) {
            Assertions.assertTrue(isEmpty(Deencapsulation.getField(restored, field)), name + "." + field);
        }
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        Assertions.assertEquals(expectedRows.get(original.getJobId()), restoredRows, name);
    }

    private static boolean isEmpty(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).isEmpty();
        }
        if (value instanceof Collection) {
            return ((Collection<?>) value).isEmpty();
        }
        if (value instanceof Table) {
            return ((Table<?, ?, ?>) value).isEmpty();
        }
        if (value instanceof Multimap) {
            return ((Multimap<?, ?>) value).isEmpty();
        }
        throw new IllegalArgumentException(value.getClass().getName());
    }

    // The maps shared by the schema change jobs, with the index maps SHOW ALTER lists.
    private static void fillSchemaChangeMaps(AlterJobV2 job) {
        if (job instanceof SchemaChangeJobV2) {
            ((SchemaChangeJobV2) job).indexMetaIdToSchemaVersionAndHash.put(2L, new SchemaVersionAndHash(1, 1));
        }
        Table<Long, Long, Map<Long, Long>> tabletMap = Deencapsulation.getField(job, "physicalPartitionIndexTabletMap");
        tabletMap.put(1L, 2L, new HashMap<>(Map.of(3L, 4L)));
        fillPartitionIndexMap(job);
        Map<Long, List<Column>> schemas = Deencapsulation.getField(job, "indexMetaIdToSchema");
        schemas.put(2L, new ArrayList<>());
        Map<Long, Short> shortKeys = Deencapsulation.getField(job, "indexMetaIdToShortKey");
        shortKeys.put(2L, (short) 1);
        Map<Long, Long> indexMetaIdMap = Deencapsulation.getField(job, "indexMetaIdMap");
        indexMetaIdMap.put(2L, 1L);
        Map<Long, String> indexNames = Deencapsulation.getField(job, "indexMetaIdToName");
        indexNames.put(2L, "__starrocks_shadow_idx");
    }

    private static void fillPartitionIndexMap(AlterJobV2 job) {
        Table<Long, Long, MaterializedIndex> indexMap = Deencapsulation.getField(job, "physicalPartitionIndexMap");
        indexMap.put(1L, 2L, new MaterializedIndex(5L, MaterializedIndex.IndexState.SHADOW));
    }

    private static void fillPartitionStates(LakeOnlineRewriteJobBase job) {
        job.shadowIndexName = "__starrocks_shadow_idx";
        job.partitionStates.put(1L, new LakeOnlineRewriteJobBase.PartitionRewriteState());
    }

    private static void fillPartitionLists(AlterJobV2 job) {
        List<Long> tmpPartitionIds = Deencapsulation.getField(job, "tmpPartitionIds");
        tmpPartitionIds.add(1L);
        List<String> sourcePartitionNames = Deencapsulation.getField(job, "sourcePartitionNames");
        sourcePartitionNames.add("p1");
        List<String> tmpPartitionNames = Deencapsulation.getField(job, "tmpPartitionNames");
        tmpPartitionNames.add("p_tmp");
    }

    private RollupJobV2 buildRollupJobWithTabletData(long id) {
        RollupJobV2 job = buildRollupJob(id);
        job.addMVIndex(40001L, new MaterializedIndex(40002L, MaterializedIndex.IndexState.SHADOW));
        job.addTabletIdMap(40001L, 40003L, 40004L);
        return job;
    }

    private static Map<Long, Map<Long, Long>> baseTabletIds(RollupJobV2 job) {
        return Deencapsulation.getField(job, "physicalPartitionIdToBaseRollupTabletIdMap");
    }

    private RollupJobV2 buildRollupJob(long id) {
        return new RollupJobV2(id, 20001, 30001, "test", 3600000,
                0, 0, "test", "test", 1,
                null, null, 1, 1, null, (short) 0,
                null, null, false);
    }
}
