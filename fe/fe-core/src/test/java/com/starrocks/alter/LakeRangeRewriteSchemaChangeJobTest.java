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

import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.alter.reshard.presplit.Sampler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.lake.Utils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LakeRangeRewriteSchemaChangeJobTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_range_rewrite_test";
    private static Database db;
    private OlapTable table;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        Config.enable_range_distribution = true;
    }

    @BeforeEach
    public void before() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(DB_NAME);
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        // A range-distribution table: data is routed by the range sort key (k1, k2).
        String sql = "create table t_range (k1 int, k2 int, v1 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_range");
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    /**
     * Builds a job whose new sort key reorders the base key to (k2, k1) and wires a stub sampler.
     */
    private LakeRangeRewriteSchemaChangeJob newJob(Sampler sampler) {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowIndexMetaId = GlobalStateMgr.getCurrentState().getNextId();
        LakeRangeRewriteSchemaChangeJob job = new LakeRangeRewriteSchemaChangeJob(
                jobId, db.getId(), table.getId(), table.getName(), 3600_000L);
        // New schema = base schema (column set unchanged for a sort-key reorder).
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        job.setNewSchema(new ArrayList<>(baseSchema));
        job.setNewKeysType(KeysType.DUP_KEYS);
        // Reorder the sort key to (k2, k1): indexes into the schema.
        job.setNewSortKeyIdxes(List.of(1, 0));
        job.setNewSortKeyColumns(List.of(baseSchema.get(1), baseSchema.get(0)));
        job.setShadowIndex(shadowIndexMetaId, table.getBaseIndexMetaId(),
                SchemaChangeHandler.SHADOW_NAME_PREFIX + table.getName(), (short) 2);
        job.setSampler(sampler);
        return job;
    }

    /** A stub sampler returning canned sort-key tuples; used to drive boundary planning deterministically. */
    private static Sampler stubSampler(List<Tuple> tuples) {
        return request -> tuples.isEmpty()
                ? SampleSet.EMPTY
                : new SampleSet(tuples, new Estimates(1024L, tuples.size()));
    }

    private static Tuple keyTuple(int k2, int k1) {
        return new Tuple(List.of(
                Variant.of(IntegerType.INT, Integer.toString(k2)),
                Variant.of(IntegerType.INT, Integer.toString(k1))));
    }

    @Test
    public void testRunPendingTransitionsToWaitingTxnAndBuildsShadow() throws Exception {
        // A diverse sample so the boundary planner produces >= 1 cut → K >= 2.
        List<Tuple> sample = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sample.add(keyTuple(i, i));
        }
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(sample));

        job.runPendingJob();

        // 1. State machine advanced PENDING -> WAITING_TXN.
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        // 2. The table is now SCHEMA_CHANGE and carries the shadow MaterializedIndexMeta.
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        MaterializedIndexMeta shadowMeta = table.getIndexMetaByMetaId(job.getShadowIndexMetaId());
        Assertions.assertNotNull(shadowMeta);
        Assertions.assertEquals(List.of(1, 0), shadowMeta.getSortKeyIdxes());

        // 3. A SHADOW MaterializedIndex with >= 1 tablet was built and journaled for the partition,
        //    and K = boundaries + 1.
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        MaterializedIndex shadowIndex = job.getShadowIndex(physicalPartitionId);
        Assertions.assertNotNull(shadowIndex);
        Assertions.assertEquals(MaterializedIndex.IndexState.SHADOW, shadowIndex.getState());
        Assertions.assertTrue(shadowIndex.getTablets().size() >= 1);
        Assertions.assertTrue(job.getTabletCount(physicalPartitionId) >= 2,
                "diverse sample should yield K >= 2");
        Assertions.assertEquals(job.getTabletCount(physicalPartitionId),
                job.getBoundaries(physicalPartitionId).size() + 1);

        // 4. The shadow index is NOT yet exposed in the partition's catalog index map (exposure is
        //    deferred to WAITING_TXN once the watershed is allocated).
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        Assertions.assertNull(physicalPartition.getIndex(job.getShadowIndexMetaId()),
                "shadow index must not be in the partition catalog index map during PENDING");
    }

    @Test
    public void testNoDistinctionSampleFallsBackToSingleFullRangeTablet() throws Exception {
        // All-equal keys: the boundary planner collapses to NO_SPLIT → K = 1.
        List<Tuple> sample = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sample.add(keyTuple(7, 7));
        }
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(sample));

        job.runPendingJob();

        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        Assertions.assertEquals(1, job.getTabletCount(physicalPartitionId),
                "no-distinction sample must fall back to a single full-range tablet");
        Assertions.assertTrue(job.getBoundaries(physicalPartitionId).isEmpty());
        MaterializedIndex shadowIndex = job.getShadowIndex(physicalPartitionId);
        Assertions.assertEquals(1, shadowIndex.getTablets().size());
        Assertions.assertTrue(shadowIndex.getTablets().get(0).getRange().getRange().isAll());
    }

    @Test
    public void testEmptySampleFallsBackToSingleFullRangeTablet() throws Exception {
        // Empty sample (sampling found no rows): K = 1 fallback.
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(List.of()));

        job.runPendingJob();

        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        Assertions.assertEquals(1, job.getTabletCount(physicalPartitionId));
        Assertions.assertEquals(1, job.getShadowIndex(physicalPartitionId).getTablets().size());
    }

    @Test
    public void testSamplingFailureFallsBackToSingleFullRangeTablet() throws Exception {
        // A sampler that throws must not fail the alter; it degrades to K = 1.
        Sampler throwing = request -> {
            throw new com.starrocks.common.StarRocksException("boom");
        };
        LakeRangeRewriteSchemaChangeJob job = newJob(throwing);

        job.runPendingJob();

        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        Assertions.assertEquals(1, job.getTabletCount(physicalPartitionId));
    }

    @Test
    public void testGsonRoundTripPreservesSubtypeAndFields() throws Exception {
        // Drive PENDING so the job carries a full journaled state, then serialize/deserialize
        // through the AlterJobV2 runtime-type adapter exactly as the edit log does.
        List<Tuple> sample = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sample.add(keyTuple(i, i));
        }
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(sample));
        job.runPendingJob();

        // copyForPersist() is what EditLog.logAlterJob serializes; it must be a deep snapshot.
        AlterJobV2 persistCopy = job.copyForPersist();
        Assertions.assertNotSame(job, persistCopy);

        String text = GsonUtils.GSON.toJson(persistCopy);
        // The runtime-type adapter must emit the discriminator so replay can pick the subtype.
        Assertions.assertTrue(text.contains("\"clazz\""), "serialized text must carry the clazz discriminator");
        Assertions.assertTrue(text.contains("LakeRangeRewriteSchemaChangeJob"),
                "serialized text must carry the LakeRangeRewriteSchemaChangeJob label");

        AlterJobV2 restored = GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        Assertions.assertInstanceOf(LakeRangeRewriteSchemaChangeJob.class, restored);
        LakeRangeRewriteSchemaChangeJob restoredJob = (LakeRangeRewriteSchemaChangeJob) restored;

        // Key fields survive the round trip.
        Assertions.assertEquals(job.getJobId(), restoredJob.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, restoredJob.getJobState());
        Assertions.assertEquals(job.getShadowIndexMetaId(), restoredJob.getShadowIndexMetaId());
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        Assertions.assertEquals(job.getTabletCount(physicalPartitionId),
                restoredJob.getTabletCount(physicalPartitionId));
        MaterializedIndex restoredShadow = restoredJob.getShadowIndex(physicalPartitionId);
        Assertions.assertNotNull(restoredShadow);
        Assertions.assertEquals(MaterializedIndex.IndexState.SHADOW, restoredShadow.getState());
        Assertions.assertEquals(job.getShadowIndex(physicalPartitionId).getTablets().size(),
                restoredShadow.getTablets().size());
    }

    @Test
    public void testReplayWaitingTxnReconstructsShadowMetaAndIsIdempotent() throws Exception {
        List<Tuple> sample = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sample.add(keyTuple(i, i));
        }
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(sample));
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        // Serialize the journaled job, then simulate FE recovery from a pre-change image: reset the
        // table back to NORMAL and strip the shadow meta + inverted-index entries the live run added.
        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        TabletInvertedIndex invertedIndex =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        table.deleteIndexInfo(SchemaChangeHandler.SHADOW_NAME_PREFIX + table.getName());
        for (long tabletId : shadowTabletIds) {
            invertedIndex.deleteTablet(tabletId);
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
        Assertions.assertNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()),
                "precondition: shadow meta removed before replay");

        // Replay the deserialized journaled image onto an in-memory job loaded from the same image
        // (its jobId/dbId/tableId match, as they would after FE recovery). The TABLE is what was
        // reset above; replay must rebuild the table's in-memory state from the journaled fields.
        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        // The shadow meta, the table state, and the inverted-index entries are reconstructed.
        MaterializedIndexMeta shadowMeta = table.getIndexMetaByMetaId(job.getShadowIndexMetaId());
        Assertions.assertNotNull(shadowMeta, "replay must re-register the shadow MaterializedIndexMeta");
        Assertions.assertEquals(List.of(1, 0), shadowMeta.getSortKeyIdxes());
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        for (long tabletId : shadowTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId),
                    "replay must re-add the shadow tablets to the inverted index");
        }

        // A second replay is a no-op: same meta, no double-added tablets.
        inMemory.replay(replayed);
        Assertions.assertNotNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()));
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        for (long tabletId : shadowTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId));
        }
    }

    @Test
    public void testReplayPendingDoesNotInstallShadowMeta() throws Exception {
        // A PENDING job has run no stages yet: partitionStates is empty and nothing is installed on the
        // table (on the leader the shadow meta is registered only in the WAITING_TXN applier, atomically
        // with that journal entry). Replaying the initial PENDING journal entry must therefore install
        // NOTHING -- otherwise the table would carry a shadow index schema with no per-partition index,
        // and loads would fail (OlapTableSink.createSchema() sees the shadow while createPartition() has
        // no matching partition index) until the job advances.
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        Assertions.assertNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()),
                "precondition: no shadow meta before runPendingJob");
        OlapTable.OlapTableState stateBefore = table.getState();

        String text = GsonUtils.GSON.toJson(job.copyForPersist());
        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        Assertions.assertNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()),
                "replay of a PENDING entry must not register the shadow MaterializedIndexMeta");
        Assertions.assertEquals(stateBefore, table.getState(),
                "replay of a PENDING entry must not change the table state");
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, inMemory.getJobState());
    }

    @Test
    public void testRunPendingResolvesBaseIndexByMetaIdAfterReshard() throws Exception {
        // Simulate a tablet split/merge reshard: the base index keeps its meta id, but its latest
        // MaterializedIndex carries a NEW physical index id and the old physical version is gone.
        // runPendingJob must resolve the base index by META id (getLatestIndex), not by physical index id
        // (getIndex) -- otherwise it cancels with "base index missing" for resharded range tables.
        long baseMetaId = table.getBaseIndexMetaId();
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            MaterializedIndex oldBase = pp.getIndex(baseMetaId);
            Assertions.assertNotNull(oldBase, "fresh table: physical index id == meta id");
            long newPhysId = GlobalStateMgr.getCurrentState().getNextId();
            Assertions.assertNotEquals(baseMetaId, newPhysId);
            MaterializedIndex newBase = new MaterializedIndex(newPhysId, baseMetaId,
                    MaterializedIndex.IndexState.NORMAL, oldBase.getShardGroupId());
            for (Tablet t : oldBase.getTablets()) {
                newBase.addTablet(t, new TabletMeta(db.getId(), table.getId(), pp.getId(), newPhysId,
                        com.starrocks.thrift.TStorageMedium.HDD, true), false);
            }
            pp.addMaterializedIndex(newBase, true);
            pp.deleteMaterializedIndexByIndexId(oldBase.getId());
            // After the reshard the physical-id lookup misses; only the meta-id lookup finds the base.
            Assertions.assertNull(pp.getIndex(baseMetaId), "getIndex(metaId) must miss a resharded index");
            Assertions.assertEquals(newBase, pp.getLatestIndex(baseMetaId));
        }

        // With the fix, runPendingJob resolves the resharded base index and advances to WAITING_TXN;
        // before the fix it threw AlterCancelException("base index missing in partition ...").
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
    }

    /** A diverse sample so the boundary planner produces >= 1 cut → K >= 2. */
    private static List<Tuple> diverseSample() {
        List<Tuple> sample = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sample.add(keyTuple(i, i));
        }
        return sample;
    }

    @Test
    public void testEnterWaitingTxnAllocatesWatershedAndExposesShadow() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        // Precondition: PENDING did not expose the shadow to the partition catalog index map.
        Assertions.assertNull(physicalPartition.getLatestIndex(job.getShadowIndexMetaId()));

        // First WAITING_TXN run: allocate the watershed and expose the shadow. The drain is not
        // checked yet (no W captured), so stay in WAITING_TXN.
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        // The watershed was allocated.
        Assertions.assertTrue(job.getTransactionId().isPresent());
        long watershedTxnId = job.getTransactionId().get();
        Assertions.assertTrue(watershedTxnId >= 0);

        // The shadow MaterializedIndex is now exposed in the partition catalog index map with
        // visibleTxnId == watershedTxnId, so post-watershed loads double-write it.
        MaterializedIndex exposed = physicalPartition.getLatestIndex(job.getShadowIndexMetaId());
        Assertions.assertNotNull(exposed, "WAITING_TXN must expose the shadow index to the partition catalog map");
        Assertions.assertEquals(MaterializedIndex.IndexState.SHADOW, exposed.getState());
        // visibleTxnId == watershedTxnId: transactions at/after the watershed see the shadow (and so
        // double-write it), transactions before it ignore it.
        Assertions.assertTrue(exposed.visibleForTransaction(watershedTxnId));
        Assertions.assertFalse(exposed.visibleForTransaction(watershedTxnId - 1));

        // No watershed version captured yet (the drain has not been confirmed finished).
        Assertions.assertNull(job.getWatershedVersion(physicalPartitionId));
    }

    @Test
    public void testWaitingTxnStaysWhenPreviousLoadsUnfinished() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();

        // First WAITING_TXN run allocates the watershed and exposes the shadow.
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();

        // Prior loads are not finished: the drain check returns false, so stay in WAITING_TXN and do
        // NOT capture W.
        new MockUp<LakeRangeRewriteSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return false;
            }
        };

        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        Assertions.assertNull(job.getWatershedVersion(physicalPartitionId),
                "W must not be captured before the drain is confirmed finished");
    }

    @Test
    public void testWaitingTxnCapturesWatershedVersionAndTransitionsToRunning() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();

        // First WAITING_TXN run allocates the watershed and exposes the shadow.
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        long visibleVersion = table.getPhysicalPartition(physicalPartitionId).getVisibleVersion();

        // Prior loads are finished: the drain check returns true, so capture W = visible version and
        // transition to RUNNING.
        new MockUp<LakeRangeRewriteSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return true;
            }
        };

        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assertions.assertEquals(visibleVersion, job.getWatershedVersion(physicalPartitionId),
                "W must equal the partition visible version captured after the drain");
    }

    @Test
    public void testWaitingTxnAnalysisExceptionCancelsJob() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        new MockUp<LakeRangeRewriteSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                throw new AnalysisException("drain check failed");
            }
        };

        Assertions.assertThrows(AlterCancelException.class, job::runWaitingTxnJob);
    }

    @Test
    public void testReplayWaitingTxnReExposesShadowAfterWatershed() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        // Allocate the watershed and expose the shadow on the live job.
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        long watershedTxnId = job.getTransactionId().get();

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        // Snapshot the journaled (post-watershed) image.
        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        // Simulate FE recovery from a pre-exposure image: strip the shadow meta, the exposed index,
        // the inverted-index entries, and reset the table to NORMAL.
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        physicalPartition.deleteMaterializedIndexByMetaId(job.getShadowIndexMetaId());
        table.deleteIndexInfo(SchemaChangeHandler.SHADOW_NAME_PREFIX + table.getName());
        for (long tabletId : shadowTabletIds) {
            invertedIndex.deleteTablet(tabletId);
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
        Assertions.assertNull(physicalPartition.getLatestIndex(job.getShadowIndexMetaId()),
                "precondition: shadow index exposure removed before replay");

        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        // Replay re-exposes the shadow index to the partition catalog map with the journaled watershed.
        MaterializedIndex reExposed = physicalPartition.getLatestIndex(job.getShadowIndexMetaId());
        Assertions.assertNotNull(reExposed, "replay must re-expose the shadow index after the watershed");
        Assertions.assertEquals(MaterializedIndex.IndexState.SHADOW, reExposed.getState());
        Assertions.assertTrue(reExposed.visibleForTransaction(watershedTxnId));
        Assertions.assertFalse(reExposed.visibleForTransaction(watershedTxnId - 1));

        // A second replay is a no-op: the exposure is idempotent (no double-add crash).
        inMemory.replay(replayed);
        Assertions.assertNotNull(physicalPartition.getLatestIndex(job.getShadowIndexMetaId()));
    }

    /** Drive a job through PENDING + both WAITING_TXN runs into RUNNING with W captured. */
    private LakeRangeRewriteSchemaChangeJob jobInRunning() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        // First WAITING_TXN run allocates the watershed and exposes the shadow.
        job.runWaitingTxnJob();
        // Second run: prior loads finished -> capture W and transition to RUNNING.
        new MockUp<LakeRangeRewriteSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return true;
            }
        };
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        return job;
    }

    /** A TransactionState reporting the given status, so the resume classifier can be driven. */
    private static void mockTransactionStatus(TransactionStatus status) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                if (status == null) {
                    return null;
                }
                TransactionState state = new TransactionState();
                state.setTransactionStatus(status);
                return state;
            }
        };
    }

    /**
     * A VISIBLE TransactionState for a shadow-rewrite txn, so the tightened resume classifier
     * can be driven. The tighter gate only counts a VISIBLE txn as DONE when it is genuinely this job's
     * shadow-rewrite carrier: {@code isShadowRewrite()} with a matching watershed txn id and alter version.
     */
    private static void mockShadowRewriteTransaction(long watershedTxnId, long alterVersion) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                TransactionState state = new TransactionState(dbId, new ArrayList<>(), transactionId, "shadow",
                        null, TransactionState.LoadJobSourceType.SHADOW_REWRITE,
                        new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                        0L, 60_000L);
                state.setTransactionStatus(TransactionStatus.VISIBLE);
                InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment();
                attachment.setShadowRewriteWatershedTxnId(watershedTxnId);
                attachment.setShadowRewriteAlterVersion(alterVersion);
                state.setTxnCommitAttachment(attachment);
                return state;
            }
        };
    }

    @Test
    public void testRunningBuildsShadowRewriteInsertAndJournalsTxnIdBeforeExecute() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        long watershed = job.getWatershedVersion(physicalPartitionId);
        long watershedTxnId = job.getTransactionId().get();

        // The seam captures the built InsertStmt + the scan-version override WITHOUT executing. It also
        // asserts the rewrite txn id was journaled BEFORE the INSERT runs (an aborted id can't be reused).
        AtomicReference<InsertStmt> captured = new AtomicReference<>();
        AtomicReference<Long> overrideAtExecute = new AtomicReference<>();
        AtomicReference<Long> journaledTxnIdAtExecute = new AtomicReference<>();
        job.setRewriteExecutor((context, insertStmt) -> {
            captured.set(insertStmt);
            overrideAtExecute.set(context.getScanVersionOverride().get(physicalPartitionId));
            journaledTxnIdAtExecute.set(job.getRewriteTxnId(physicalPartitionId));
        });

        job.runRunningJob();

        InsertStmt stmt = captured.get();
        Assertions.assertNotNull(stmt, "the rewrite INSERT must be built and handed to the executor");
        Assertions.assertTrue(stmt.isShadowRewrite(), "INSERT must be marked shadow-rewrite");
        Assertions.assertEquals(job.getShadowIndexMetaId(), stmt.getTargetWriteIndexId().longValue(),
                "INSERT must write only the shadow index");
        // The watershed carrier reaches the InsertStmt so the publish converts op_write -> op_schema_change@W.
        Assertions.assertEquals(watershedTxnId, stmt.getShadowRewriteWatershedTxnId());
        Assertions.assertEquals(watershed, stmt.getShadowRewriteAlterVersion());
        // The read-pin is the scan override, NOT isVersionOverwrite.
        Assertions.assertFalse(stmt.isVersionOverwrite(), "rewrite must not pin the commit version");
        Assertions.assertEquals(watershed, overrideAtExecute.get().longValue(),
                "the SELECT must read the watershed snapshot W");

        // The rewrite txn id was journaled before the INSERT executed.
        Assertions.assertNotNull(journaledTxnIdAtExecute.get(),
                "rewrite txn id must be journaled BEFORE executing the INSERT");
    }

    @Test
    public void testRunningResumePublishedSkipsAndTransitionsToFinishedRewriting() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();

        // Seed a journaled rewrite txn id and report it VISIBLE AND a matching shadow-rewrite carrier
        // (this job's watershed txn id + this partition's captured W): the partition is already rewritten.
        job.setRewriteTxnIdForTest(physicalPartitionId, 123456L);
        mockShadowRewriteTransaction(job.getTransactionId().get(), job.getWatershedVersion(physicalPartitionId));

        AtomicReference<Boolean> executed = new AtomicReference<>(false);
        job.setRewriteExecutor((context, insertStmt) -> executed.set(true));

        job.runRunningJob();

        Assertions.assertFalse(executed.get(), "a published partition must be skipped (no re-execute)");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState(),
                "all partitions published -> transition to FINISHED_REWRITING");
    }

    @Test
    public void testRunningResumeVisibleForeignTxnReRunsNotSkips() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();

        // The journaled id belongs to a VISIBLE txn that is NOT this job's shadow-rewrite carrier (a
        // foreign load txn that slipped into the peek->begin window and was journaled by mistake before a
        // leader crash). Under the tightened gate it must NOT skip (DONE) -> classify NEEDS_RUN and re-run.
        job.setRewriteTxnIdForTest(physicalPartitionId, 123456L);
        mockTransactionStatus(TransactionStatus.VISIBLE); // plain VISIBLE, no shadow-rewrite attachment

        AtomicReference<Boolean> executed = new AtomicReference<>(false);
        job.setRewriteExecutor((context, insertStmt) -> executed.set(true));

        job.runRunningJob();

        Assertions.assertTrue(executed.get(),
                "a VISIBLE foreign (non-shadow-rewrite) txn must re-run the rewrite INSERT, not skip");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState(),
                "a mis-journaled foreign VISIBLE txn must not flip to FINISHED_REWRITING");
    }

    @Test
    public void testRunningResumeVisibleShadowRewriteWithMismatchedWatershedReRuns() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();

        // A VISIBLE shadow-rewrite txn whose watershed carrier does NOT match this job (wrong watershed
        // txn id): it is not this job's rewrite -> classify NEEDS_RUN and re-run, never skip.
        job.setRewriteTxnIdForTest(physicalPartitionId, 123456L);
        mockShadowRewriteTransaction(job.getTransactionId().get() + 1, job.getWatershedVersion(physicalPartitionId));

        AtomicReference<Boolean> executed = new AtomicReference<>(false);
        job.setRewriteExecutor((context, insertStmt) -> executed.set(true));

        job.runRunningJob();

        Assertions.assertTrue(executed.get(),
                "a VISIBLE shadow-rewrite txn with a mismatched watershed must re-run, not skip");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
    }

    @Test
    public void testRunningResumeAbortedReRunsWithoutDroppingShadow() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        // Snapshot the shadow tablet ids so we can prove they are NOT dropped on a resume re-run.
        List<Long> shadowTabletIdsBefore = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        // A prior attempt aborted: classify as NEEDS_RUN and re-run the SAME watershed-pinned INSERT.
        job.setRewriteTxnIdForTest(physicalPartitionId, 999L);
        mockTransactionStatus(TransactionStatus.ABORTED);

        AtomicReference<Boolean> executed = new AtomicReference<>(false);
        job.setRewriteExecutor((context, insertStmt) -> executed.set(true));

        job.runRunningJob();

        Assertions.assertTrue(executed.get(), "an aborted partition must re-run the rewrite INSERT");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        // The shadow tablets are untouched: post-watershed double-writes are already published into them.
        List<Long> shadowTabletIdsAfter = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());
        Assertions.assertEquals(shadowTabletIdsBefore, shadowTabletIdsAfter,
                "resume must never drop/rebuild the shadow tablets");
    }

    @Test
    public void testRunningResumeCommittedWaitsWithoutExecuting() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();

        // Committed-not-yet-visible: stay in RUNNING and do NOT re-execute (wait for publish).
        job.setRewriteTxnIdForTest(physicalPartitionId, 777L);
        mockTransactionStatus(TransactionStatus.COMMITTED);

        AtomicReference<Boolean> executed = new AtomicReference<>(false);
        job.setRewriteExecutor((context, insertStmt) -> executed.set(true));

        job.runRunningJob();

        Assertions.assertFalse(executed.get(), "a committed-not-visible partition must wait, not re-execute");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState(),
                "committed-not-visible -> stay in RUNNING");
    }

    /**
     * Drive a job all the way into FINISHED_REWRITING: PENDING -> WAITING_TXN -> RUNNING, then report
     * the per-partition rewrite txn published (a matching shadow-rewrite carrier) so RUNNING flips to
     * FINISHED_REWRITING.
     */
    private LakeRangeRewriteSchemaChangeJob jobInFinishedRewriting() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        job.setRewriteTxnIdForTest(physicalPartitionId, 123456L);
        mockShadowRewriteTransaction(job.getTransactionId().get(), job.getWatershedVersion(physicalPartitionId));
        job.setRewriteExecutor((context, insertStmt) -> { });
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        return job;
    }

    @Test
    public void testReplayFirstFinishedRewritingEntryWithoutReservedCommitVersionDoesNotNpe() throws Exception {
        // The RUNNING -> FINISHED_REWRITING transition (runRunningJob) journals a FINISHED_REWRITING entry
        // BEFORE reserveCommitVersionIfNeeded runs, so that entry's partitionStates carry no commit
        // version. Replaying it (leader failover / follower catch-up) reaches the FINISHED_REWRITING replay
        // branch -> updateNextVersion, which must skip the unreserved partition instead of unboxing a null
        // commit version (which previously NPE'd and aborted journal replay / leader recovery).
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        // Precondition: this is the first FINISHED_REWRITING entry's state -- no commit version reserved.
        Assertions.assertTrue(job.commitVersionMapView().isEmpty(),
                "precondition: the first FINISHED_REWRITING entry has no reserved commit version");

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        long nextVersionBefore = physicalPartition.getNextVersion();

        // Serialize that null-commit-version FR image and replay it onto a fresh in-memory job, exactly as
        // a follower would on journal catch-up.
        String text = GsonUtils.GSON.toJson(job.copyForPersist());
        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        Assertions.assertDoesNotThrow(() -> inMemory.replay(replayed),
                "replaying the unreserved FINISHED_REWRITING entry must not NPE");

        // Nothing was reserved, so replay must not bump nextVersion.
        Assertions.assertEquals(nextVersionBefore, physicalPartition.getNextVersion(),
                "replay of the unreserved FINISHED_REWRITING entry must not bump nextVersion");
    }

    @Test
    public void testFinishedRewritingFlipsBaseToShadowAndResetsTable() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        long originBaseIndexMetaId = table.getBaseIndexMetaId();
        long shadowIndexMetaId = job.getShadowIndexMetaId();
        Assertions.assertNotEquals(originBaseIndexMetaId, shadowIndexMetaId);
        // The shadow index is exposed (WAITING_TXN) and carries the new sort key (k2, k1).
        MaterializedIndex shadowIndex = physicalPartition.getLatestIndex(shadowIndexMetaId);
        Assertions.assertNotNull(shadowIndex);
        List<Long> shadowTabletIds = shadowIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toList());
        long visibleBeforeFlip = physicalPartition.getVisibleVersion();

        // Stub the publish (no backend): the catalog flip is the unit under test, not the BE RPC.
        job.setPublishExecutor(j -> true);

        job.runFinishedRewritingJob();

        // 1. State machine reached FINISHED and the table is back to NORMAL.
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        // 2. The base index meta id now points at the shadow meta, which carries the new sortKeyIdxes (1,0).
        Assertions.assertEquals(shadowIndexMetaId, table.getBaseIndexMetaId());
        MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        Assertions.assertEquals(List.of(1, 0), baseMeta.getSortKeyIdxes(),
                "the new base index meta must carry the reordered sort key");

        // 3. The origin base index meta is gone; the partition's base index is now the K shadow tablets,
        //    promoted to NORMAL, tiling the new key space.
        Assertions.assertNull(table.getIndexMetaByMetaId(originBaseIndexMetaId),
                "the origin base index meta must be dropped after the flip");
        MaterializedIndex newBaseIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertEquals(shadowIndexMetaId, newBaseIndex.getMetaId());
        Assertions.assertEquals(MaterializedIndex.IndexState.NORMAL, newBaseIndex.getState());
        Assertions.assertEquals(shadowTabletIds,
                newBaseIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()),
                "the flipped base index must hold exactly the K shadow tablets");

        // 4. Range distribution columns are derived from the base index sort key, so they now follow the
        //    reorder: (k2, k1).
        List<String> distCols = com.starrocks.sql.common.MetaUtils.getRangeDistributionColumnNames(table);
        Assertions.assertEquals(List.of("k2", "k1"), distCols,
                "range distribution columns follow the flipped base sort key");

        // 5. The partition visible version advanced by exactly one (commitVersion = visibleVersion + 1).
        Assertions.assertEquals(visibleBeforeFlip + 1, physicalPartition.getVisibleVersion());

        // 6. The GC pin set in RUNNING was released.
        Assertions.assertEquals(0, physicalPartition.getMinRetainVersion());

        // 7. The shadow tablets are registered in the inverted index under the new base index id.
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long tabletId : shadowTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId));
        }
    }

    @Test
    public void testFinishedRewritingStaysWhenPublishNotDone() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();

        // The async publish has not completed yet (mirrors AlterJobV2.publishVersion returning false on
        // the first submit): the job must stay in FINISHED_REWRITING and re-check next tick, NOT flip.
        job.setPublishExecutor(j -> false);

        job.runFinishedRewritingJob();

        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState(),
                "an incomplete publish must leave the job in FINISHED_REWRITING");
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState(),
                "the table must not flip to NORMAL until the publish completes");
    }

    @Test
    public void testFinishedRewritingInactivatesDependentMaterializedViews() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();

        // Capture the columns handed to the MV-inactivation path. For a sort-key reorder to (k2, k1)
        // the affected set is the new sort-key columns (k1, k2).
        AtomicReference<Set<String>> inactivatedColumns = new AtomicReference<>();
        new MockUp<AlterMVJobExecutor>() {
            @Mock
            public void inactiveRelatedMaterializedViewsRecursive(OlapTable olapTable, Set<String> modifiedColumns) {
                inactivatedColumns.set(modifiedColumns);
            }
        };

        // A dependent async MV must exist for collectAffectedColumnsForRelatedMVs to compute a non-empty
        // set; register one against the base table id.
        table.addRelatedMaterializedView(new MvId(db.getId(), GlobalStateMgr.getCurrentState().getNextId()));

        job.setPublishExecutor(j -> true);
        job.runFinishedRewritingJob();

        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertNotNull(inactivatedColumns.get(),
                "the flip must explicitly invoke dependent-MV inactivation");
        Assertions.assertTrue(inactivatedColumns.get().contains("k1")
                        && inactivatedColumns.get().contains("k2"),
                "the new sort-key columns must be reported as affected for MV inactivation");
    }

    @Test
    public void testBuildTabletRangesTilesNegInfToPosInf() {
        // Empty boundaries -> one full-range tablet.
        Assertions.assertEquals(1, LakeRangeRewriteSchemaChangeJob.buildTabletRanges(List.of()).size());
        Assertions.assertTrue(LakeRangeRewriteSchemaChangeJob.buildTabletRanges(List.of())
                .get(0).getRange().isAll());

        // Two boundaries -> three tablets: (-inf, c1), [c1, c2), [c2, +inf).
        List<Tuple> boundaries = List.of(keyTuple(10, 10), keyTuple(20, 20));
        var ranges = LakeRangeRewriteSchemaChangeJob.buildTabletRanges(boundaries);
        Assertions.assertEquals(3, ranges.size());
        Assertions.assertTrue(ranges.get(0).getRange().isMinimum());
        Assertions.assertFalse(ranges.get(0).getRange().isMaximum());
        Assertions.assertTrue(ranges.get(2).getRange().isMaximum());
        Assertions.assertFalse(ranges.get(2).getRange().isMinimum());
    }

    // ---- cancelImpl tests ---------------------------------------------------------------

    /**
     * Helper: assert the shadow index is fully cleaned up after a cancel — no catalog exposure, no
     * inverted-index entries, table state NORMAL, GC pin released.
     */
    private void assertShadowCleanedUp(LakeRangeRewriteSchemaChangeJob job, long physicalPartitionId,
                                       List<Long> shadowTabletIds) {
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        Assertions.assertNull(physicalPartition.getLatestIndex(job.getShadowIndexMetaId()),
                "shadow index must be absent from the partition catalog map after cancel");
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState(),
                "table state must be reset to NORMAL after cancel");
        Assertions.assertEquals(0, physicalPartition.getMinRetainVersion(),
                "GC pin must be released after cancel");
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long tabletId : shadowTabletIds) {
            Assertions.assertNull(invertedIndex.getTabletMeta(tabletId),
                    "shadow tablet must be removed from the inverted index after cancel: " + tabletId);
        }
    }

    @Test
    public void testCancelInWaitingTxnDropsShadowAndResetsTable() throws Exception {
        // Drive to WAITING_TXN (shadow exposed to catalog).
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        job.runWaitingTxnJob(); // allocates watershed + exposes shadow
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());
        // Shadow is exposed.
        Assertions.assertNotNull(table.getPhysicalPartition(physicalPartitionId)
                .getLatestIndex(job.getShadowIndexMetaId()));

        boolean cancelled = job.cancelImpl("test cancel in WAITING_TXN");

        Assertions.assertTrue(cancelled, "cancelImpl must return true when not already cancelled/finished");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertShadowCleanedUp(job, physicalPartitionId, shadowTabletIds);
    }

    @Test
    public void testCancelInRunningAbortsRewriteTxnAndDropsShadow() throws Exception {
        // Drive to RUNNING (watershed captured, shadow exposed).
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();

        // Seed a journaled rewrite txn id (PREPARE state = in-flight); cancel must attempt to abort it.
        long rewriteTxnId = 888L;
        job.setRewriteTxnIdForTest(physicalPartitionId, rewriteTxnId);

        AtomicReference<Long> abortedTxnId = new AtomicReference<>();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                TransactionState state = new TransactionState();
                state.setTransactionStatus(TransactionStatus.PREPARE);
                return state;
            }

            @Mock
            public void abortTransaction(long dbId, long transactionId, String reason)
                    throws com.starrocks.common.StarRocksException {
                abortedTxnId.set(transactionId);
            }
        };

        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        boolean cancelled = job.cancelImpl("test cancel in RUNNING");

        Assertions.assertTrue(cancelled);
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        // The in-flight rewrite txn was aborted.
        Assertions.assertEquals(rewriteTxnId, abortedTxnId.get(),
                "cancelImpl must abort the in-flight rewrite txn");
        assertShadowCleanedUp(job, physicalPartitionId, shadowTabletIds);
    }

    @Test
    public void testCancelInRunningSkipsAbortForTerminalRewriteTxn() throws Exception {
        // A rewrite txn that is already VISIBLE must NOT be re-aborted: abort on a VISIBLE txn would throw.
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        job.setRewriteTxnIdForTest(physicalPartitionId, 999L);

        AtomicReference<Boolean> abortCalled = new AtomicReference<>(false);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                TransactionState state = new TransactionState();
                state.setTransactionStatus(TransactionStatus.VISIBLE);
                return state;
            }

            @Mock
            public void abortTransaction(long dbId, long transactionId, String reason)
                    throws com.starrocks.common.StarRocksException {
                abortCalled.set(true);
            }
        };

        job.cancelImpl("test cancel with VISIBLE rewrite txn");

        Assertions.assertFalse(abortCalled.get(),
                "cancelImpl must not abort a txn that is already in a terminal status");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testCancelInWaitingTxnBeforeShadowExposedIsClean() throws Exception {
        // runPendingJob() transitions to WAITING_TXN but does not expose the shadow to the partition
        // catalog map (exposure happens on the first runWaitingTxnJob run). So at this point the shadow
        // meta and tablets are in the inverted index, but the shadow is NOT in the partition catalog map.
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());
        // At this point the shadow IS in the inverted index but NOT in the partition catalog map.
        Assertions.assertNull(table.getPhysicalPartition(physicalPartitionId)
                .getLatestIndex(job.getShadowIndexMetaId()),
                "precondition: shadow not yet in partition catalog map");

        boolean cancelled = job.cancelImpl("cancel after PENDING");

        Assertions.assertTrue(cancelled);
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertShadowCleanedUp(job, physicalPartitionId, shadowTabletIds);
    }

    @Test
    public void testNormalCancelInFinishedRewritingIsRefused() throws Exception {
        // Drive into FINISHED_REWRITING and reserve the commit version (bumps nextVersion past it while
        // visibleVersion is still commitVersion-1). A normal (non-force) cancel must be refused so the
        // shadow is NOT dropped and no version hole is created; the flip should complete/retry instead.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        // First runFinishedRewritingJob run reserves the commit version and bumps nextVersion, but the
        // stubbed publish reports "not done", so the job stays in FINISHED_REWRITING.
        job.setPublishExecutor(j -> false);
        job.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        long nextVersionAfterReserve = physicalPartition.getNextVersion();
        long visibleVersionAfterReserve = physicalPartition.getVisibleVersion();

        boolean cancelled = job.cancelImpl("normal cancel in FINISHED_REWRITING");

        Assertions.assertFalse(cancelled,
                "a normal cancel from FINISHED_REWRITING while the table exists must be refused");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState(),
                "the job must stay in FINISHED_REWRITING after a refused normal cancel");
        // The shadow must NOT have been dropped and the version chain must be intact.
        Assertions.assertNotNull(physicalPartition.getLatestIndex(job.getShadowIndexMetaId()),
                "the shadow index must not be dropped by a refused normal cancel");
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState(),
                "the table must remain in SCHEMA_CHANGE after a refused normal cancel");
        Assertions.assertEquals(nextVersionAfterReserve, physicalPartition.getNextVersion());
        Assertions.assertEquals(visibleVersionAfterReserve, physicalPartition.getVisibleVersion());
    }

    @Test
    public void testForceCancelInFinishedRewritingAdvancesVisibleVersionThenDropsShadow() throws Exception {
        // Drive into FINISHED_REWRITING and reserve the commit version (the hole scenario: nextVersion is
        // commitVersion+1 while visibleVersion is commitVersion-1). A force cancel must no-op publish the
        // visible/origin indices to commitVersion, advance FE visibleVersion to commitVersion (closing the
        // hole), then drop the shadow and reach CANCELLED.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        job.setPublishExecutor(j -> false);
        job.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        long visibleBeforeForceCancel = physicalPartition.getVisibleVersion();
        long commitVersion = visibleBeforeForceCancel + 1;
        Assertions.assertEquals(commitVersion + 1, physicalPartition.getNextVersion(),
                "precondition: nextVersion was bumped past commitVersion, leaving a hole");

        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        // Stub the no-op publish seam (no backend): capture that the force-cancel requested the version
        // advance via Utils.noOpPublishForForceSkip and report success.
        AtomicReference<Long> publishedCommitVersion = new AtomicReference<>();
        new MockUp<com.starrocks.lake.Utils>() {
            @Mock
            public boolean noOpPublishForForceSkip(long jobId, String reason, long watershedTxnId, long watershedGtid,
                                                   java.util.Map<Long, Long> commitVersionMap,
                                                   java.util.Map<Long, List<Tablet>> tabletsByPartition,
                                                   com.starrocks.warehouse.cngroup.ComputeResource cr,
                                                   boolean useAggregatePublish) {
                publishedCommitVersion.set(commitVersionMap.get(physicalPartitionId));
                return true;
            }
        };

        boolean cancelled = job.cancelImpl("force cancel in FINISHED_REWRITING", true);

        Assertions.assertTrue(cancelled, "a force cancel from FINISHED_REWRITING must proceed");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        // The no-op publish was requested for the reserved commit version.
        Assertions.assertEquals(commitVersion, publishedCommitVersion.get().longValue(),
                "force cancel must no-op publish the reserved commit version");
        // The FE visible version advanced to commitVersion: no hole (nextVersion == visibleVersion + 1).
        Assertions.assertEquals(commitVersion, physicalPartition.getVisibleVersion(),
                "force cancel must advance visibleVersion to commitVersion (closing the hole)");
        Assertions.assertEquals(physicalPartition.getVisibleVersion() + 1, physicalPartition.getNextVersion(),
                "after force cancel the version chain must be dense (no hole)");
        // The shadow was then dropped.
        assertShadowCleanedUp(job, physicalPartitionId, shadowTabletIds);
    }

    @Test
    public void testForceCancelReplayAdvancesVisibleVersion() throws Exception {
        // Regression: forceSkippedAtCommitted was not copied in replay()'s field-copy block,
        // so a follower replaying a force-cancelled job (recovered from a pre-cancel image) saw
        // forceSkippedAtCommitted=false, skipped advanceVisibleVersionForForceSkip, and left
        // visibleVersion=commitVersion-1 — a version hole against BE metadata already at commitVersion.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        // Reserve the commit version without actually flipping: setPublishExecutor returns false so
        // runFinishedRewritingJob bumps nextVersion (reserves the slot) but stays in FINISHED_REWRITING.
        job.setPublishExecutor(j -> false);
        job.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        long commitVersion = physicalPartition.getVisibleVersion() + 1;

        // Stub the no-op publish so the force-cancel succeeds without a backend.
        new MockUp<com.starrocks.lake.Utils>() {
            @Mock
            public boolean noOpPublishForForceSkip(long jobId, String reason, long watershedTxnId, long watershedGtid,
                                                   java.util.Map<Long, Long> commitVersionMap,
                                                   java.util.Map<Long, List<Tablet>> tabletsByPartition,
                                                   com.starrocks.warehouse.cngroup.ComputeResource cr,
                                                   boolean useAggregatePublish) {
                return true;
            }
        };

        // Run the live force-cancel; the persisted copy must carry the marker.
        Assertions.assertTrue(job.cancelImpl("force-cancel-replay-test", /*force=*/ true));
        AlterJobV2 persistCopy = job.copyForPersist();
        Assertions.assertTrue(persistCopy.isForceSkippedAtCommitted(),
                "copyForPersist must carry forceSkippedAtCommitted=true after a force cancel");

        // Simulate FE recovery from a pre-cancel image: build an in-memory job WITHOUT the marker
        // and reset the partition's visibleVersion back to commitVersion-1 (not yet bumped).
        LakeRangeRewriteSchemaChangeJob staleInMemory = new LakeRangeRewriteSchemaChangeJob(job);
        staleInMemory.forceSkippedAtCommitted = false;
        staleInMemory.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);
        physicalPartition.setVisibleVersion(commitVersion - 1, 0);

        // Replay: the field-copy block must propagate forceSkippedAtCommitted so the CANCELLED
        // branch applies advanceVisibleVersionForForceSkip and bumps visibleVersion to commitVersion.
        staleInMemory.replay(persistCopy);

        Assertions.assertTrue(staleInMemory.isForceSkippedAtCommitted(),
                "replay must copy forceSkippedAtCommitted from the persisted entry");
        Assertions.assertEquals(commitVersion, physicalPartition.getVisibleVersion(),
                "replay must advance visibleVersion to commitVersion when forceSkippedAtCommitted=true");
    }

    @Test
    public void testFlipPublishIssuesShadowRewritePublishPerPartition() throws Exception {
        // The flip publishes shadow tablets through the generic Utils.publishVersion /
        // createSubRequestForAggregatePublish, under a TXN_SHADOW_REWRITE TxnInfoPB whose txnId is that
        // partition's journaled rewriteTxnId (not the watershed txn id) and whose shadowRewriteAlterVersion
        // is the watershed W. The shadow tablets travel in the request's ordinary tabletIds.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        long watershedVersion = job.getWatershedVersion(physicalPartitionId);
        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        // Intercept Utils calls so we can verify the shape without requiring a live BE.
        List<Long> publishedShadowTabletIds = new ArrayList<>();
        AtomicReference<Long> capturedRewriteTxnId = new AtomicReference<>();
        AtomicReference<Long> capturedAlterVersion = new AtomicReference<>();
        new MockUp<Utils>() {
            // Non-bundling path: both shadow and origin tablets go through the generic publishVersion.
            // The shadow publish is the one carrying a TXN_SHADOW_REWRITE TxnInfoPB; the origin no-op
            // publish carries TXN_EMPTY and is not under test here.
            @Mock
            public void publishVersion(List<Tablet> tablets, com.starrocks.proto.TxnInfoPB txnInfo,
                    long baseVersion, long newVersion, ComputeResource cr,
                    boolean useAggregatePublish) {
                if (txnInfo.txnType == com.starrocks.proto.TxnTypePB.TXN_SHADOW_REWRITE) {
                    tablets.forEach(t -> publishedShadowTabletIds.add(t.getId()));
                    capturedRewriteTxnId.set(txnInfo.txnId);
                    capturedAlterVersion.set(txnInfo.shadowRewriteAlterVersion);
                }
            }

            // Bundling path: both shadow and origin tablets go through the generic
            // createSubRequestForAggregatePublish. The shadow sub-request carries a TXN_SHADOW_REWRITE txn.
            @Mock
            public void createSubRequestForAggregatePublish(List<Tablet> tablets,
                    List<com.starrocks.proto.TxnInfoPB> txnInfos, long baseVersion, long newVersion,
                    java.util.Map<com.starrocks.system.ComputeNode, List<Long>> nodeToTablets,
                    ComputeResource cr,
                    com.starrocks.proto.AggregatePublishVersionRequest req) {
                com.starrocks.proto.TxnInfoPB txnInfo = txnInfos.get(0);
                if (txnInfo.txnType == com.starrocks.proto.TxnTypePB.TXN_SHADOW_REWRITE) {
                    tablets.forEach(t -> publishedShadowTabletIds.add(t.getId()));
                    capturedRewriteTxnId.set(txnInfo.txnId);
                    capturedAlterVersion.set(txnInfo.shadowRewriteAlterVersion);
                }
            }

            @Mock
            public void sendAggregatePublishVersionRequest(
                    com.starrocks.proto.AggregatePublishVersionRequest req, long baseVersion,
                    ComputeResource cr, java.util.Map<Long, Double> compactionScores,
                    java.util.Map<Long, Long> tabletRowNum,
                    List<com.starrocks.proto.VectorIndexBuildInfoPB> vectorInfos) {
                // no-op: aggregate publish RPC is not under test here
            }
        };

        // Drive the flip through the REAL lakePublishVersion (reserve commit version, then publish).
        job.setPublishExecutor(LakeOnlineRewriteJobBase::lakePublishVersion);
        job.runFinishedRewritingJob();

        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        // The flip must have published the shadow tablets under a TXN_SHADOW_REWRITE txn.
        Assertions.assertFalse(publishedShadowTabletIds.isEmpty(),
                "the flip must publish shadow tablets (carried in tabletIds under a TXN_SHADOW_REWRITE txn)");

        // Every shadow tablet must be published.
        Assertions.assertTrue(publishedShadowTabletIds.containsAll(shadowTabletIds),
                "the flip must publish every shadow tablet");

        // The txnId must be the journaled per-partition rewrite txn id (seeded as 123456L).
        Assertions.assertEquals(123456L, capturedRewriteTxnId.get().longValue(),
                "SHADOW_REWRITE txnId must be the journaled per-partition rewrite txn id");

        // The alterVersion must be the captured watershed version W.
        Assertions.assertEquals(watershedVersion, capturedAlterVersion.get().longValue(),
                "SHADOW_REWRITE alterVersion must be the captured watershed version W");
    }

    @Test
    public void testCancelFinishedJobIsNoOp() throws Exception {
        // Drive all the way to FINISHED, then attempt to cancel.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        job.setPublishExecutor(j -> true);
        job.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        boolean cancelled = job.cancelImpl("attempt to cancel a finished job");

        Assertions.assertFalse(cancelled, "cancelImpl must return false (no-op) when job is FINISHED");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(),
                "job state must remain FINISHED after no-op cancel");
    }

    @Test
    public void testCancelAlreadyCancelledJobIsNoOp() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        job.cancelImpl("first cancel");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());

        boolean secondCancel = job.cancelImpl("second cancel");

        Assertions.assertFalse(secondCancel, "cancelImpl must return false (no-op) when already CANCELLED");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testReplayCancelledRemovesShadow() throws Exception {
        // Drive to WAITING_TXN (shadow exposed), then cancel, then replay the CANCELLED log entry.
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(diverseSample()));
        job.runPendingJob();
        job.runWaitingTxnJob();
        job.cancelImpl("replay test cancel");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Long> shadowTabletIds = new ArrayList<>(
                job.getShadowIndex(physicalPartitionId).getTablets()
                        .stream().map(Tablet::getId).collect(Collectors.toList()));

        // Serialize the CANCELLED state, then manually re-add some shadow state (simulating a follower
        // that had not yet applied the cancel) and replay onto a fresh in-memory job.
        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        // Simulate the follower seeing a pre-cancel state: re-expose the shadow index + add tablets back.
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        // Restore shadow exposure (replaying from a prior WAITING_TXN snapshot).
        MaterializedIndex shadowIdx = job.getShadowIndex(physicalPartitionId);
        physicalPartition.createRollupIndex(shadowIdx);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        physicalPartition.setMinRetainVersion(5L);
        for (long tabletId : shadowTabletIds) {
            invertedIndex.addTablet(tabletId,
                    new TabletMeta(db.getId(), table.getId(),
                            physicalPartitionId, job.getShadowIndexMetaId(),
                            com.starrocks.thrift.TStorageMedium.HDD, true));
        }

        // Replay the CANCELLED log entry: must undo all shadow state.
        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        assertShadowCleanedUp(inMemory, physicalPartitionId, shadowTabletIds);

        // A second replay is a no-op: removing already-absent shadow state must not crash.
        inMemory.replay(replayed);
        assertShadowCleanedUp(inMemory, physicalPartitionId, shadowTabletIds);
    }

    // ---- cross-state replay idempotency ------------------------------------------------
    //
    // Replay must, for every journaled state, reconstruct the leader's catalog on the follower and be
    // safe to apply twice (a follower can re-see the same log entry across a restart). WAITING_TXN and
    // CANCELLED are covered above (testReplayWaitingTxn*, testReplayCancelledRemovesShadow); PENDING is
    // covered above too (testReplayPendingDoesNotInstallShadowMeta — a PENDING entry has no durable shadow
    // state, so replay installs nothing); the tests below cover the remaining journaled states RUNNING,
    // FINISHED_REWRITING and FINISHED.

    @Test
    public void testReplayRunningReconstructsWaitingTxnCatalogStateAndIsIdempotent() throws Exception {
        // RUNNING has the same durable catalog state as WAITING_TXN (shadow exposed with
        // visibleTxnId=watershed); the in-flight rewrite txns are resumed by the live scheduler, never by
        // replay. So replaying a RUNNING image reconstructs the WAITING_TXN catalog state idempotently and
        // never re-runs the rewrite INSERT.
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        long watershedTxnId = job.getTransactionId().get();

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Long> shadowTabletIds = job.getShadowIndex(physicalPartitionId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        // Simulate FE recovery from a pre-exposure image: strip the shadow meta, the exposed index, the
        // inverted-index entries, and reset the table to NORMAL.
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        physicalPartition.deleteMaterializedIndexByMetaId(job.getShadowIndexMetaId());
        table.deleteIndexInfo(SchemaChangeHandler.SHADOW_NAME_PREFIX + table.getName());
        for (long tabletId : shadowTabletIds) {
            invertedIndex.deleteTablet(tabletId);
        }
        table.setState(OlapTable.OlapTableState.NORMAL);

        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        // The rewrite executor must never run during replay; fail loudly if it is invoked.
        inMemory.setRewriteExecutor((context, insertStmt) ->
                Assertions.fail("replay must not re-run the rewrite INSERT"));
        inMemory.replay(replayed);

        // The shadow catalog state is reconstructed: meta registered, tablets in the inverted index,
        // table in SCHEMA_CHANGE, shadow index re-exposed with visibleTxnId=watershed.
        Assertions.assertNotNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()),
                "replay must re-register the shadow meta");
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        MaterializedIndex reExposed = physicalPartition.getLatestIndex(job.getShadowIndexMetaId());
        Assertions.assertNotNull(reExposed, "RUNNING replay must re-expose the shadow index");
        Assertions.assertEquals(MaterializedIndex.IndexState.SHADOW, reExposed.getState());
        Assertions.assertTrue(reExposed.visibleForTransaction(watershedTxnId));
        Assertions.assertFalse(reExposed.visibleForTransaction(watershedTxnId - 1));
        for (long tabletId : shadowTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId),
                    "RUNNING replay must re-add the shadow tablets to the inverted index");
        }

        // A second replay is a no-op: no double-add of the exposed index/tablets/meta.
        inMemory.replay(replayed);
        Assertions.assertNotNull(physicalPartition.getLatestIndex(job.getShadowIndexMetaId()));
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
    }

    /**
     * Drive a job into FINISHED_REWRITING with the per-partition commit version reserved (nextVersion
     * bumped, visibleVersion still commitVersion-1) but the publish not yet completed, so the catalog is
     * still in the pre-flip state. This is the catalog state a follower holds when it replays a
     * FINISHED_REWRITING (and, after we relabel the journaled state, FINISHED) log entry.
     */
    private LakeRangeRewriteSchemaChangeJob jobInFinishedRewritingWithCommitReserved() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewriting();
        job.setPublishExecutor(j -> false);
        job.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        return job;
    }

    @Test
    public void testReplayFinishedRewritingBumpsNextVersionAndIsIdempotent() throws Exception {
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewritingWithCommitReserved();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        long commitVersion = physicalPartition.getNextVersion() - 1;
        long visibleVersion = physicalPartition.getVisibleVersion();

        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        // Simulate a follower recovering from a pre-reserve image: roll nextVersion back to commitVersion
        // (the leader bumped it past commitVersion when it reserved). Replay must re-apply that bump.
        physicalPartition.setNextVersion(commitVersion);

        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        Assertions.assertEquals(commitVersion + 1, physicalPartition.getNextVersion(),
                "replay must bump nextVersion past the reserved commit version");
        Assertions.assertEquals(visibleVersion, physicalPartition.getVisibleVersion(),
                "FINISHED_REWRITING replay must not advance visibleVersion (the flip has not happened)");

        // A second replay is a no-op: no double version bump.
        inMemory.replay(replayed);
        Assertions.assertEquals(commitVersion + 1, physicalPartition.getNextVersion(),
                "a second replay must not bump nextVersion again");
    }

    @Test
    public void testReplayFinishedRewritingReleasesWatershedGcPin() throws Exception {
        // On a journal-replay failover, RUNNING replay re-pins minRetainVersion=W (reapplyWatershedGcPin);
        // then the FINISHED_REWRITING entry replays. The live reserve released that pin when it reserved
        // the commit version, so replay of the post-reserve FINISHED_REWRITING entry must release it too,
        // or vacuum can never reclaim versions below W on the recovered leader.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewritingWithCommitReserved();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        // Simulate the journal-replay-failover state: the RUNNING entry's replay left the pin set
        // (minRetainVersion is transient, re-derived by reapplyWatershedGcPin, never serialized).
        physicalPartition.setMinRetainVersion(5L);
        Assertions.assertEquals(5L, physicalPartition.getMinRetainVersion());

        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        Assertions.assertEquals(0, physicalPartition.getMinRetainVersion(),
                "FINISHED_REWRITING replay must release the watershed GC pin once the commit version is reserved");

        // Idempotent: a second replay keeps it released.
        physicalPartition.setMinRetainVersion(5L);
        inMemory.replay(replayed);
        Assertions.assertEquals(0, physicalPartition.getMinRetainVersion());
    }

    @Test
    public void testReplayFinishedFlipsBaseToShadowAndIsIdempotent() throws Exception {
        // Drive into FINISHED_REWRITING with the commit version reserved (catalog still pre-flip), then
        // relabel the journaled state to FINISHED so replay applies the flip the leader would have.
        LakeRangeRewriteSchemaChangeJob job = jobInFinishedRewritingWithCommitReserved();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);

        long originBaseIndexMetaId = table.getBaseIndexMetaId();
        long shadowIndexMetaId = job.getShadowIndexMetaId();
        long visibleBeforeFlip = physicalPartition.getVisibleVersion();
        List<Long> shadowTabletIds = physicalPartition.getLatestIndex(shadowIndexMetaId).getTablets()
                .stream().map(Tablet::getId).collect(Collectors.toList());

        // Journal a FINISHED image (finishedTimeMs must be set so visualiseShadowIndex stamps the version).
        job.setJobState(AlterJobV2.JobState.FINISHED);
        job.setFinishedTimeMs(System.currentTimeMillis());
        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        // The flip was applied: base meta repointed at the shadow (new sort key), origin meta dropped,
        // table back to NORMAL, visibleVersion advanced by one.
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(shadowIndexMetaId, table.getBaseIndexMetaId());
        Assertions.assertNull(table.getIndexMetaByMetaId(originBaseIndexMetaId),
                "the origin base index meta must be dropped after the replayed flip");
        Assertions.assertEquals(List.of(1, 0),
                table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSortKeyIdxes());
        Assertions.assertEquals(visibleBeforeFlip + 1, physicalPartition.getVisibleVersion());
        MaterializedIndex newBaseIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertEquals(shadowIndexMetaId, newBaseIndex.getMetaId());
        Assertions.assertEquals(MaterializedIndex.IndexState.NORMAL, newBaseIndex.getState());

        // A second replay is a no-op: the origin meta is gone, so the guard skips the flip (no double-flip,
        // no double version bump).
        inMemory.replay(replayed);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(shadowIndexMetaId, table.getBaseIndexMetaId());
        Assertions.assertEquals(visibleBeforeFlip + 1, physicalPartition.getVisibleVersion(),
                "a second replay must not advance the visible version again");
        for (long tabletId : shadowTabletIds) {
            Assertions.assertNotNull(invertedIndexTabletMeta(tabletId),
                    "the flipped base index tablets must remain registered");
        }
    }

    private static TabletMeta invertedIndexTabletMeta(long tabletId) {
        return GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
    }

    // ---- getInfo (SHOW ALTER TABLE COLUMN) ---------------------------------------------

    @Test
    public void testGetInfoEmitsRowForRunningJob() throws Exception {
        // A RUNNING job must be visible in SHOW ALTER TABLE COLUMN: getInfo emits one display row
        // carrying the jobId, table name, the running state, and the watershed txn id.
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();

        List<List<Comparable>> infos = new ArrayList<>();
        job.getInfo(infos);

        Assertions.assertEquals(1, infos.size(), "getInfo must emit exactly one row for the single shadow index");
        List<Comparable> row = infos.get(0);
        // Mirrors LakeTableSchemaChangeJob.getInfo: 14 columns matching SchemaChangeProcDir.TITLE_NAMES.
        Assertions.assertEquals(14, row.size(), "row must carry the full SHOW ALTER TABLE COLUMN column set");
        Assertions.assertEquals(job.getJobId(), row.get(0), "first column must be the jobId");
        Assertions.assertEquals(table.getName(), row.get(1), "second column must be the table name");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING.name(), row.get(9),
                "the state column must report RUNNING");
        Assertions.assertEquals(job.getTransactionId().get(), row.get(8),
                "the watershed txn id column must carry the allocated watershed");
    }

    // ---- FIX: durable GC retention pin restored on RUNNING replay ----------------------

    @Test
    public void testReplayRunningRestoresWatershedGcPin() throws Exception {
        // The watershed GC pin (minRetainVersion) is NOT persisted, so a follower that takes over after
        // RUNNING was journaled must re-apply it on replay, or vacuum could GC the exact snapshot W the
        // rewrite SELECT reads.
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        long watershed = job.getWatershedVersion(physicalPartitionId);
        Assertions.assertTrue(watershed > 0, "precondition: a watershed version was captured");

        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        // Simulate a follower recovering from an image where minRetainVersion reset to 0 (not persisted).
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        physicalPartition.setMinRetainVersion(0);
        Assertions.assertEquals(0, physicalPartition.getMinRetainVersion(),
                "precondition: the GC pin reset to 0 on the follower image");

        LakeRangeRewriteSchemaChangeJob replayed =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRewriteSchemaChangeJob inMemory =
                (LakeRangeRewriteSchemaChangeJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        inMemory.replay(replayed);

        Assertions.assertEquals(watershed, physicalPartition.getMinRetainVersion(),
                "RUNNING replay must restore the watershed GC pin to W");

        // A second replay is a no-op (the pin is already W).
        inMemory.replay(replayed);
        Assertions.assertEquals(watershed, physicalPartition.getMinRetainVersion());
    }

    // ---- FIX: 1:1 logical-to-physical partition guard in RUNNING -----------------------

    @Test
    public void testRunningRequiresOneToOneLogicalPhysicalPartition() throws Exception {
        // Range-distribution OLAP tables are 1:1 logical:physical, so the watershed-pinned rewrite (which
        // scans the logical parent but pins only one physical partition) is correct. Assert the guard sees
        // the 1:1 mapping and the rewrite proceeds for the default table.
        LakeRangeRewriteSchemaChangeJob job = jobInRunning();
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        Partition logicalParent = table.getPartition(physicalPartition.getParentId());
        Assertions.assertEquals(1, logicalParent.getSubPartitions().size(),
                "precondition: a range table's logical partition has exactly one physical partition");

        // The rewrite builds the INSERT (the 1:1 guard does not throw) and stays in RUNNING after kicking
        // off one partition's rewrite.
        AtomicReference<InsertStmt> captured = new AtomicReference<>();
        job.setRewriteExecutor((context, insertStmt) -> captured.set(insertStmt));
        job.runRunningJob();

        Assertions.assertNotNull(captured.get(),
                "the 1:1 guard must pass and the rewrite INSERT must be built");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
    }

    // ---- empty-partition characterization (NOTE in the brief) --------------------------

    @Test
    public void testEmptyPartitionRewriteFlowsThroughStateMachineToFinished() throws Exception {
        // Characterize the CURRENT behavior for an EMPTY partition (empty sample -> K=1 single full-range
        // shadow tablet). The job flows PENDING -> WAITING_TXN -> RUNNING -> FINISHED_REWRITING -> FINISHED
        // and the flip swaps the base index for the single-tablet shadow. The deeper empty-DATA handling
        // (an empty-result INSERT producing zero PartitionCommitInfos, so the publish has no
        // op_schema_change@W to apply) is BE-owned and gated on cluster e2e; here we only document that the
        // FE state machine completes without error and produces the expected single-tablet shadow.
        LakeRangeRewriteSchemaChangeJob job = newJob(stubSampler(List.of()));
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        // Empty partition -> a single full-range shadow tablet.
        Assertions.assertEquals(1, job.getTabletCount(physicalPartitionId));

        // Allocate watershed + expose shadow, then drain finished -> capture W -> RUNNING.
        job.runWaitingTxnJob();
        new MockUp<LakeRangeRewriteSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return true;
            }
        };
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        // Report the rewrite txn as a published shadow-rewrite carrier (the empty-result INSERT's commit,
        // as the BE converter would mark it) so RUNNING flips to FINISHED_REWRITING.
        job.setRewriteTxnIdForTest(physicalPartitionId, 424242L);
        mockShadowRewriteTransaction(job.getTransactionId().get(), job.getWatershedVersion(physicalPartitionId));
        job.setRewriteExecutor((context, insertStmt) -> { });
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());

        // Flip completes: table back to NORMAL, base meta repointed at the single-tablet shadow.
        long shadowIndexMetaId = job.getShadowIndexMetaId();
        job.setPublishExecutor(j -> true);
        job.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(shadowIndexMetaId, table.getBaseIndexMetaId());
        MaterializedIndex newBaseIndex = table.getPhysicalPartition(physicalPartitionId).getLatestBaseIndex();
        Assertions.assertEquals(1, newBaseIndex.getTablets().size(),
                "an empty partition flips to a single full-range tablet");
    }
}
