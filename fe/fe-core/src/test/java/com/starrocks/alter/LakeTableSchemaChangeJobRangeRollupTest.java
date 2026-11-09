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

import com.google.common.collect.Table;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.alter.LakeOnlineRewriteJobBase.PendingPartitionPlan;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SampleRequest;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.alter.reshard.presplit.TabletPreSplitCoordinator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Schema changes on a shared-data range-distributed table that carries range rollups: a rollup keeps its own
 * sort key (and the short key derived from it) together with its tablet ranges, and DROP / MODIFY / reorder of
 * a column in a rollup's sort key are rejected.
 */
public class LakeTableSchemaChangeJobRangeRollupTest {
    private static final String DB = "test_range_rollup_sc";
    private static final int K = 3;
    private static final String SORT_KEY_SUFFIX = " on a sort-key column is not supported on tables with range "
            + "distribution, because it would change the column set or type/semantics that the existing range "
            + "tablet boundary values were recorded under. Column: ";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        UtFrameUtils.setUpForPersistTest();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
    }

    @AfterAll
    public static void tearDown() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    @BeforeEach
    public void resetJournal() throws Exception {
        // The replay tests read the journal from the start of their own alter.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    // ------------------------------------------------------------------ fixture

    private static String rangeTableSql(String name, String extraProperties) {
        return "create table " + name + " (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\norder by(k1, k2)\n"
                + "properties('replication_num' = '1'" + extraProperties + ");";
    }

    /** Range table with rollup r_dup (k2, k1, v1) DUPLICATE KEY (k2) ORDER BY (k2, k1): keys (k2), sort key (k2, k1). */
    private static OlapTable rangeTableWithDupRollup(String name, String extraProperties) throws Exception {
        starRocksAssert.withTable(rangeTableSql(name, extraProperties));
        addFinishedRangeRollup(name,
                "alter table " + name + " add rollup r_dup(k2, k1, v1) duplicate key(k2) order by (k2, k1)");
        OlapTable t = table(name);
        assertRollupSortKey(t, "r_dup", List.of("k2", "k1"), uniqueIds(t, "k2", "k1"), 2,
                ranges(t, meta(t, "r_dup").getIndexMetaId()));
        return t;
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB).getTable(name);
    }

    private static OlapTable tableIn(LocalMetastore metastore, String name) {
        return (OlapTable) metastore.getTable(DB, name);
    }

    private static MaterializedIndexMeta meta(OlapTable t, String indexName) {
        Long id = t.getIndexMetaIdByName(indexName);
        assertNotNull(id, "no index " + indexName);
        return t.getIndexMetaByMetaId(id);
    }

    private static List<String> names(List<Column> cols) {
        return cols.stream().map(Column::getName).collect(Collectors.toList());
    }

    private static List<Integer> uniqueIds(OlapTable t, String... columnNames) {
        List<Integer> ids = new ArrayList<>();
        for (String name : columnNames) {
            ids.add(t.getColumn(name).getUniqueId());
        }
        return ids;
    }

    private static PhysicalPartition partition(OlapTable t) {
        return t.getPhysicalPartitions().iterator().next();
    }

    private static List<Tablet> tablets(OlapTable t, long indexMetaId) {
        return partition(t).getLatestIndex(indexMetaId).getTablets();
    }

    private static List<TabletRange> ranges(OlapTable t, long indexMetaId) {
        return tablets(t, indexMetaId).stream().map(Tablet::getRange).collect(Collectors.toList());
    }

    private static int rangeArity(TabletRange tabletRange) {
        Range<Tuple> r = tabletRange.getRange();
        return !r.isMinimum() ? r.getLowerBound().getValues().size() : r.getUpperBound().getValues().size();
    }

    /** Sorted samples over the two sort-key columns: first column in [0, 10), second in [0, 30). */
    private static SampleSet twoColumnSample(SampleRequest request) {
        List<Column> sortKey = request.getSortKey();
        assertEquals(2, sortKey.size());
        List<Tuple> tuples = new ArrayList<>();
        for (int a = 0; a < 10; a++) {
            for (int b = 0; b < 30; b++) {
                tuples.add(new Tuple(List.of(sampleValue(sortKey.get(0), a), sampleValue(sortKey.get(1), b))));
            }
        }
        return new SampleSet(tuples, new Estimates(1024L, tuples.size()));
    }

    private static Variant sampleValue(Column column, int i) {
        // Zero-padded so that the string order matches the numeric order.
        String value = column.getType().isStringType() ? String.format("s%02d", i) : Integer.toString(i);
        return Variant.of(column.getType(), value);
    }

    /**
     * Run ADD ROLLUP for real (MaterializedViewHandler builds the LakeRangeRollupJob with the production schema,
     * sort key and short key), then drive the job's own planPartitionShadow (K-tablet layout cut on the rollup
     * sort key), registerShadowIndexMeta and visualiseShadowIndex: the catalog state a FINISHED range rollup
     * leaves, minus the data rewrite.
     */
    private static void addFinishedRangeRollup(String tableName, String addRollupSql) throws Exception {
        starRocksAssert.alterTable(addRollupSql);
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        List<AlterJobV2> jobs = new ArrayList<>(handler.getAlterJobsV2().values());
        handler.clearJobs();
        assertEquals(1, jobs.size());
        LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, jobs.get(0));

        new MockUp<LakeOnlineRewriteJobBase>() {
            @Mock
            protected int selectRequestedTabletCount(PendingPartitionPlan plan, int activeComputeNodeCount,
                                                     long targetSize) {
                return K;
            }
        };
        job.setSampler(LakeTableSchemaChangeJobRangeRollupTest::twoColumnSample);
        OlapTable t = table(tableName);
        PhysicalPartition pp = partition(t);
        MaterializedIndex baseIndex = pp.getLatestIndex(t.getBaseIndexMetaId());
        PendingPartitionPlan plan = new PendingPartitionPlan(pp.getId(), baseIndex, baseIndex.getShardGroupId(),
                t.getPartition(pp.getParentId()).getName(), baseIndex.getDataSize(), t.getName(), pp);
        job.planPartitionShadow(plan, t, DB);
        assertEquals(K, plan.shadowIndex.getTablets().size(), "rollup must be split into K tablets");
        job.registerShadowIndexMeta(t);
        pp.createRollupIndex(plan.shadowIndex);
        job.stateOf(pp.getId()).commitVersion = pp.getVisibleVersion() + 1;
        job.visualiseShadowIndex(t);
        // The real rollup's rewrite txn consumed this version; keep nextVersion ahead of visibleVersion.
        pp.setNextVersion(pp.getVisibleVersion() + 1);
        assertEquals(OlapTable.OlapTableState.NORMAL, t.getState());
        for (Tablet tablet : plan.shadowIndex.getTablets()) {
            assertEquals(2, rangeArity(tablet.getRange()), "rollup tablet ranges are cut on two columns");
        }
    }

    private static List<AlterJobV2> unfinishedJobs(OlapTable t) {
        return GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler()
                .getUnfinishedAlterJobV2ByTableId(t.getId());
    }

    private static AlterJobV2 takeSchemaChangeJob(OlapTable t) {
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
        List<AlterJobV2> jobs = handler.getUnfinishedAlterJobV2ByTableId(t.getId());
        handler.clearJobs();
        assertEquals(1, jobs.size(), "expected exactly one schema change job");
        return jobs.get(0);
    }

    private static LakeTableSchemaChangeJob takeHeavyJob(OlapTable t) {
        AlterJobV2 job = takeSchemaChangeJob(t);
        return assertInstanceOf(LakeTableSchemaChangeJob.class, job,
                "expected the shadow-index schema change job, got " + job.getClass().getSimpleName());
    }

    private static long shadowMetaIdOf(LakeTableSchemaChangeJob job, long originMetaId) {
        Map<Long, Long> shadowToOrigin = Deencapsulation.getField(job, "indexMetaIdMap");
        return shadowToOrigin.entrySet().stream().filter(e -> e.getValue() == originMetaId)
                .findFirst().orElseThrow(() -> new AssertionError("the job does not shadow index " + originMetaId))
                .getKey();
    }

    /** Shadow tablet id -> origin tablet id, for one shadow index of the (single) partition. */
    private static Map<Long, Long> shadowToOriginTablets(LakeTableSchemaChangeJob job, OlapTable t,
                                                        long shadowMetaId) {
        Table<Long, Long, Map<Long, Long>> map = Deencapsulation.getField(job, "physicalPartitionIndexTabletMap");
        return new HashMap<>(map.get(partition(t).getId(), shadowMetaId));
    }

    /** Make the tablet tasks finish as soon as they are sent and let the publish succeed; capture the creates. */
    private static List<CreateReplicaTask> mockTabletTasks() {
        List<CreateReplicaTask> createTasks = new ArrayList<>();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(task -> task.setFinished(true));
            }

            @Mock
            public void sendCreateReplicaTasks(Invocation inv, AgentBatchTask batchTask) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    createTasks.add((CreateReplicaTask) task);
                }
                inv.proceed(batchTask);
            }
        };
        new MockUp<AlterJobV2>() {
            @Mock
            public boolean publishVersion() {
                return true;
            }
        };
        return createTasks;
    }

    private static void drivePendingToWaitingTxn(LakeTableSchemaChangeJob job) throws Exception {
        long deadline = System.currentTimeMillis() + 60000;
        while (job.getJobState() == AlterJobV2.JobState.PENDING) {
            job.runPendingJob();
            assertTrue(System.currentTimeMillis() < deadline, "job never left PENDING");
            Thread.sleep(10);
        }
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
    }

    private static void driveWaitingTxnToFinished(LakeTableSchemaChangeJob job) throws Exception {
        long deadline = System.currentTimeMillis() + 60000;
        job.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.runFinishedRewritingJob();
            assertTrue(System.currentTimeMillis() < deadline, "job never reached FINISHED");
            Thread.sleep(50);
        }
    }

    /** Drive a heavy job PENDING -> FINISHED; returns the CreateReplicaTasks it sent. */
    private static List<CreateReplicaTask> driveToFinished(LakeTableSchemaChangeJob job) throws Exception {
        List<CreateReplicaTask> createTasks = mockTabletTasks();
        drivePendingToWaitingTxn(job);
        driveWaitingTxnToFinished(job);
        return createTasks;
    }

    private static void runToFinished(AlterJobV2 job) throws Exception {
        long deadline = System.currentTimeMillis() + 60000;
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            assertFalse(job.isDone(), "job ended in " + job.getJobState() + ": " + job.errMsg);
            assertTrue(System.currentTimeMillis() < deadline, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
    }

    // ------------------------------------------------------------------ assertions

    /** The rollup's sort key, short key and tablet ranges as the catalog holds them. */
    private static void assertRollupSortKey(OlapTable t, String rollup, List<String> sortKeyNames,
                                            List<Integer> sortKeyUniqueIds, int shortKeyCount,
                                            List<TabletRange> expectedRanges) {
        MaterializedIndexMeta m = meta(t, rollup);
        assertNotNull(m.getSortKeyIdxes(), rollup + " lost its sort key");
        assertEquals(sortKeyNames, m.getSortKeyIdxes().stream().map(i -> m.getSchema().get(i).getName())
                .collect(Collectors.toList()));
        assertEquals(sortKeyUniqueIds, m.getSortKeyUniqueIds());
        assertEquals(sortKeyNames, names(MetaUtils.getRangeDistributionColumns(t, m.getIndexMetaId())));
        assertEquals(shortKeyCount, m.getShortKeyColumnCount());
        assertEquals(expectedRanges, ranges(t, m.getIndexMetaId()));
    }

    /** The sort key and short key in the tablet schema the job sends to the compute nodes. */
    private static void assertTabletSchemaSortKey(TTabletSchema schema, List<String> sortKeyNames,
                                                  List<Integer> sortKeyUniqueIds, int shortKeyCount) {
        List<Integer> idxes = schema.getSort_key_idxes();
        assertTrue(idxes != null && !idxes.isEmpty(), "tablet schema has no sort key");
        List<TColumn> columns = schema.getColumns();
        assertEquals(sortKeyNames, idxes.stream().map(i -> Column.removeNamePrefix(columns.get(i).getColumn_name()))
                .collect(Collectors.toList()));
        assertEquals(sortKeyUniqueIds, schema.getSort_key_unique_ids());
        assertEquals(shortKeyCount, schema.getShort_key_column_count());
    }

    /** Every shadow tablet of the rollup is created with the rollup's sort key and the range of its origin tablet. */
    private static void assertShadowTabletsKeepSortKey(List<CreateReplicaTask> createTasks, long shadowMetaId,
                                                       Map<Long, Long> shadowToOrigin,
                                                       Map<Long, TabletRange> originRanges,
                                                       List<String> sortKeyNames, List<Integer> sortKeyUniqueIds,
                                                       int shortKeyCount) {
        int count = 0;
        for (CreateReplicaTask task : createTasks) {
            if (task.getIndexId() != shadowMetaId) {
                continue;
            }
            count++;
            TCreateTabletReq req = task.toThrift();
            assertTabletSchemaSortKey(req.getTablet_schema(), sortKeyNames, sortKeyUniqueIds, shortKeyCount);
            Long originTabletId = shadowToOrigin.get(task.getTabletId());
            assertNotNull(originTabletId);
            assertEquals(originRanges.get(originTabletId).toThrift(), req.getRange());
        }
        assertEquals(K, count, "one CreateReplicaTask per rollup shadow tablet");
    }

    private static Map<Long, TabletRange> rangesById(OlapTable t, long indexMetaId) {
        Map<Long, TabletRange> byId = new HashMap<>();
        for (Tablet tablet : tablets(t, indexMetaId)) {
            byId.put(tablet.getId(), tablet.getRange());
        }
        return byId;
    }

    /** What a rejected statement must leave untouched: table state, every index's schema, keys and tablets. */
    private static List<Object> catalogSnapshot(OlapTable t) {
        List<Object> snapshot = new ArrayList<>();
        snapshot.add(t.getState());
        for (Map.Entry<Long, MaterializedIndexMeta> e : new TreeMap<>(t.getIndexMetaIdToMeta()).entrySet()) {
            MaterializedIndexMeta m = e.getValue();
            snapshot.add(t.getIndexNameByMetaId(e.getKey()));
            snapshot.add(m.getSchema().stream()
                    .map(c -> c.getName() + " " + c.getType() + " key=" + c.isKey() + " null=" + c.isAllowNull())
                    .collect(Collectors.toList()));
            snapshot.add(m.getSortKeyIdxes());
            snapshot.add(m.getSortKeyUniqueIds());
            snapshot.add(m.getShortKeyColumnCount());
            snapshot.add(m.getSchemaVersion());
            for (Tablet tablet : tablets(t, e.getKey())) {
                snapshot.add(tablet.getId() + "=" + tablet.getRange());
            }
        }
        return snapshot;
    }

    /** Run a statement that must be rejected; the catalog must be unchanged and no job left. */
    private static String rejectedMessage(OlapTable t, String sql) {
        List<Object> before = catalogSnapshot(t);
        Exception e = assertThrows(Exception.class, () -> starRocksAssert.alterTable(sql), sql);
        assertEquals(before, catalogSnapshot(t), "a rejected statement changed the catalog: " + sql);
        assertTrue(unfinishedJobs(t).isEmpty(), "a rejected statement left a job: " + sql);
        return e.getMessage();
    }

    private static String rangeSortKeyMessage(String operation, String column, String index) {
        return operation + SORT_KEY_SUFFIX + column + " Index: " + index;
    }

    // ------------------------------------------------------------------ sort key kept

    /** A type change of a non-sort-key column takes the shadow-index job, which rebuilds the rollup. */
    @Test
    public void testHeavyJobKeepsRollupSortKey() throws Exception {
        OlapTable t = rangeTableWithDupRollup("t_heavy", "");
        long rollupMetaId = meta(t, "r_dup").getIndexMetaId();
        List<TabletRange> rangesBefore = ranges(t, rollupMetaId);
        Map<Long, TabletRange> originRanges = rangesById(t, rollupMetaId);
        List<Integer> sortKeyUniqueIds = uniqueIds(t, "k2", "k1");

        starRocksAssert.alterTable("alter table t_heavy modify column v1 varchar(32)");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollupMetaId);
        Map<Long, Long> shadowToOrigin = shadowToOriginTablets(job, t, shadowMetaId);
        List<CreateReplicaTask> createTasks = driveToFinished(job);

        assertShadowTabletsKeepSortKey(createTasks, shadowMetaId, shadowToOrigin, originRanges,
                List.of("k2", "k1"), sortKeyUniqueIds, 2);
        assertRollupSortKey(t, "r_dup", List.of("k2", "k1"), sortKeyUniqueIds, 2, rangesBefore);
        assertEquals(List.of("k1", "k2"), names(MetaUtils.getRangeDistributionColumns(t, t.getBaseIndexMetaId())));
    }

    /**
     * Adding a column to the rollup moves its sort-key columns: the sort key is re-expressed in the new schema and
     * the short key stays the sort-key prefix (k2, k1), not the key-column prefix (k2).
     */
    @Test
    public void testHeavyJobRemapsRollupSortKeyPositions() throws Exception {
        OlapTable t = rangeTableWithDupRollup("t_remap", "");
        long rollupMetaId = meta(t, "r_dup").getIndexMetaId();
        List<TabletRange> rangesBefore = ranges(t, rollupMetaId);
        Map<Long, TabletRange> originRanges = rangesById(t, rollupMetaId);
        List<Integer> sortKeyUniqueIds = uniqueIds(t, "k2", "k1");

        // Adding a column to a rollup of a duplicate-key table always rebuilds the rollup.
        starRocksAssert.alterTable("alter table t_remap add column v2 int after k2 to r_dup");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollupMetaId);
        Map<Long, Long> shadowToOrigin = shadowToOriginTablets(job, t, shadowMetaId);
        List<CreateReplicaTask> createTasks = driveToFinished(job);

        assertShadowTabletsKeepSortKey(createTasks, shadowMetaId, shadowToOrigin, originRanges,
                List.of("k2", "k1"), sortKeyUniqueIds, 2);
        MaterializedIndexMeta after = meta(t, "r_dup");
        assertEquals(List.of("k2", "v2", "k1", "v1"), names(after.getSchema()));
        assertEquals(List.of(0, 2), after.getSortKeyIdxes());
        assertRollupSortKey(t, "r_dup", List.of("k2", "k1"), sortKeyUniqueIds, 2, rangesBefore);
    }

    /**
     * With fast schema evolution v1 the change runs as LakeTableAsyncFastSchemaChangeJob, which must hand the
     * rollup its own sort key. The base sort key (k1, k2) sits at the same positions [0, 1] as the rollup's
     * (k2, k1), so the unique ids and column names tell them apart.
     */
    @Test
    public void testFastSchemaEvolutionV1KeepsRollupSortKey() throws Exception {
        OlapTable t = rangeTableWithDupRollup("t_fse_v1",
                ", 'cloud_native_fast_schema_evolution_v2' = 'false'");
        long rollupMetaId = meta(t, "r_dup").getIndexMetaId();
        List<TabletRange> rangesBefore = ranges(t, rollupMetaId);
        List<Integer> sortKeyUniqueIds = uniqueIds(t, "k2", "k1");

        starRocksAssert.alterTable("alter table t_fse_v1 modify column v1 bigint");
        AlterJobV2 job = takeSchemaChangeJob(t);
        LakeTableAsyncFastSchemaChangeJob fastJob = assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job,
                "expected fast schema evolution v1, got " + job.getClass().getSimpleName());
        assertTabletSchemaSortKey(schemaInfoOf(fastJob, rollupMetaId).toTabletSchema(), List.of("k2", "k1"),
                sortKeyUniqueIds, 2);

        runToFinished(fastJob);
        assertRollupSortKey(t, "r_dup", List.of("k2", "k1"), sortKeyUniqueIds, 2, rangesBefore);
    }

    /** Control: on a hash table fast schema evolution v1 hands every index the base sort key, as before. */
    @Test
    public void testFastSchemaEvolutionV1HashRollupGetsBaseSortKey() throws Exception {
        starRocksAssert.withTable("create table t_fse_v1_hash (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\ndistributed by hash(k1) buckets 2\norder by(k1, k2)\n"
                + "rollup (r_h(k2, k1, v1) duplicate key(k2, k1))\n"
                + "properties('replication_num' = '1', 'cloud_native_fast_schema_evolution_v2' = 'false');");
        OlapTable t = table("t_fse_v1_hash");
        long rollupMetaId = meta(t, "r_h").getIndexMetaId();
        assertNull(meta(t, "r_h").getSortKeyIdxes());

        starRocksAssert.alterTable("alter table t_fse_v1_hash modify column v1 bigint");
        AlterJobV2 job = takeSchemaChangeJob(t);
        LakeTableAsyncFastSchemaChangeJob fastJob = assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
        SchemaInfo base = schemaInfoOf(fastJob, t.getBaseIndexMetaId());
        SchemaInfo rollup = schemaInfoOf(fastJob, rollupMetaId);
        assertEquals(List.of(0, 1), base.getSortKeyIndexes());
        assertEquals(uniqueIds(t, "k1", "k2"), base.getSortKeyUniqueIds());
        assertEquals(base.getSortKeyIndexes(), rollup.getSortKeyIndexes());
        assertEquals(base.getSortKeyUniqueIds(), rollup.getSortKeyUniqueIds());
        fastJob.cancel("test");
    }

    /** With fast schema evolution v2 the change is applied to the catalog at once and keeps the rollup sort key. */
    @Test
    public void testFastSchemaEvolutionV2KeepsRollupSortKey() throws Exception {
        OlapTable t = rangeTableWithDupRollup("t_fse_v2", "");
        long rollupMetaId = meta(t, "r_dup").getIndexMetaId();
        List<TabletRange> rangesBefore = ranges(t, rollupMetaId);

        starRocksAssert.alterTable("alter table t_fse_v2 modify column v1 bigint");
        assertTrue(unfinishedJobs(t).isEmpty(), "expected the metadata-only change, got "
                + unfinishedJobs(t).stream().map(j -> j.getClass().getSimpleName()).collect(Collectors.toList()));
        assertRollupSortKey(t, "r_dup", List.of("k2", "k1"), uniqueIds(t, "k2", "k1"), 2, rangesBefore);
    }

    /** Range table whose rollup comes from CREATE TABLE: no sort key of its own, keys (c0, c1), short key 2. */
    private static OlapTable rangeTableWithCreateTableRollup(String name, String extraProperties) throws Exception {
        starRocksAssert.withTable("create table " + name + " (c0 int, c1 varchar(10), c2 int)\n"
                + "duplicate key(c0)\norder by(c0)\nrollup (mv1(c0, c1, c2))\n"
                + "properties('replication_num' = '1'" + extraProperties + ");");
        OlapTable t = table(name);
        assertTrue(t.isRangeDistribution());
        assertEquals(List.of(0), t.getIndexMetaByMetaId(t.getBaseIndexMetaId()).getSortKeyIdxes());
        MaterializedIndexMeta rollup = meta(t, "mv1");
        assertNull(rollup.getSortKeyIdxes());
        assertEquals(2, rollup.getShortKeyColumnCount());
        return t;
    }

    /** Give the rollup the base index's sort-key lists, as fast schema evolution v1 used to. */
    private static MaterializedIndexMeta giveRollupBaseSortKey(OlapTable t, String rollupName) {
        MaterializedIndexMeta base = t.getIndexMetaByMetaId(t.getBaseIndexMetaId());
        MaterializedIndexMeta rollup = meta(t, rollupName);
        rollup.setSortKeyIdxes(new ArrayList<>(base.getSortKeyIdxes()));
        rollup.setSortKeyUniqueIds(new ArrayList<>(base.getSortKeyUniqueIds()));
        return rollup;
    }

    /**
     * Fast schema evolution v1 used to hand such a rollup the base sort key (positions [0] and the unique id of
     * c0). A later schema change that rebuilds the rollup must not take it for the rollup's own sort key: the
     * change is accepted and the rollup comes out with no sort key and its key-column short key, as before.
     */
    @Test
    public void testRollupCarryingBaseSortKeyIsRebuiltAsBefore() throws Exception {
        OlapTable t = rangeTableWithCreateTableRollup("t_base_sk", "");
        MaterializedIndexMeta rollup = giveRollupBaseSortKey(t, "mv1");

        starRocksAssert.alterTable("alter table t_base_sk modify column c2 varchar(20)");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollup.getIndexMetaId());
        List<CreateReplicaTask> createTasks = driveToFinished(job);

        boolean sawRollupTablet = false;
        for (CreateReplicaTask task : createTasks) {
            if (task.getIndexId() == shadowMetaId) {
                sawRollupTablet = true;
                TTabletSchema schema = task.toThrift().getTablet_schema();
                assertTrue(schema.getSort_key_idxes() == null || schema.getSort_key_idxes().isEmpty());
                assertEquals(2, schema.getShort_key_column_count());
            }
        }
        assertTrue(sawRollupTablet);
        MaterializedIndexMeta after = meta(t, "mv1");
        assertNull(after.getSortKeyIdxes());
        assertNull(after.getSortKeyUniqueIds());
        assertEquals(2, after.getShortKeyColumnCount());
        assertEquals(List.of("c0", "c1"), names(MetaUtils.getRangeDistributionColumns(t, after.getIndexMetaId())));
    }

    /** Fast schema evolution v1 gives a range rollup without a sort key of its own no sort key, not the base's. */
    @Test
    public void testFastSchemaEvolutionV1DoesNotHandRangeRollupTheBaseSortKey() throws Exception {
        OlapTable t = rangeTableWithCreateTableRollup("t_fse_v1_no_sk",
                ", 'cloud_native_fast_schema_evolution_v2' = 'false'");
        long rollupMetaId = meta(t, "mv1").getIndexMetaId();

        starRocksAssert.alterTable("alter table t_fse_v1_no_sk modify column c2 bigint");
        AlterJobV2 job = takeSchemaChangeJob(t);
        LakeTableAsyncFastSchemaChangeJob fastJob = assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job,
                "expected fast schema evolution v1, got " + job.getClass().getSimpleName());
        assertEquals(List.of(0), schemaInfoOf(fastJob, t.getBaseIndexMetaId()).getSortKeyIndexes());
        TTabletSchema emitted = schemaInfoOf(fastJob, rollupMetaId).toTabletSchema();
        assertTrue(emitted.getSort_key_idxes() == null || emitted.getSort_key_idxes().isEmpty(),
                "rollup tablet schema got sort key " + emitted.getSort_key_idxes());
        assertTrue(emitted.getSort_key_unique_ids() == null || emitted.getSort_key_unique_ids().isEmpty());

        runToFinished(fastJob);
        MaterializedIndexMeta after = meta(t, "mv1");
        assertNull(after.getSortKeyIdxes());
        assertNull(after.getSortKeyUniqueIds());
        assertEquals(List.of("c0", "c1"), names(MetaUtils.getRangeDistributionColumns(t, rollupMetaId)));
    }

    /**
     * The rollup guards skip a rollup that carries the base sort key: here its position [2] lies outside the
     * two-column rollup, so dropping an unrelated column is accepted as before.
     */
    @Test
    public void testDropBesideRollupCarryingBaseSortKeyIsAccepted() throws Exception {
        starRocksAssert.withTable("create table t_base_sk_drop (c0 int, c1 varchar(10), c2 int, c3 int)\n"
                + "duplicate key(c0)\norder by(c2)\nrollup (mv1(c0, c1))\nproperties('replication_num' = '1');");
        OlapTable t = table("t_base_sk_drop");
        assertTrue(t.isRangeDistribution());
        assertEquals(List.of(2), giveRollupBaseSortKey(t, "mv1").getSortKeyIdxes());

        starRocksAssert.alterTable("alter table t_base_sk_drop drop column c3");
        for (AlterJobV2 job : unfinishedJobs(t)) {
            job.cancel("test");
        }
    }

    /** The reorder guard skips a rollup that carries the base sort key: the reorder runs as before. */
    @Test
    public void testReorderOfRollupCarryingBaseSortKeyIsAccepted() throws Exception {
        OlapTable t = rangeTableWithCreateTableRollup("t_base_sk_reorder", "");
        long rollupMetaId = giveRollupBaseSortKey(t, "mv1").getIndexMetaId();

        starRocksAssert.alterTable("alter table t_base_sk_reorder order by (c1, c0, c2) from mv1");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        Map<Long, List<Column>> schemas = Deencapsulation.getField(job, "indexMetaIdToSchema");
        assertEquals(List.of("c1", "c0", "c2"), names(schemas.get(shadowMetaIdOf(job, rollupMetaId))));
        job.cancel("test");
    }

    /**
     * Range table whose CREATE TABLE rollup mv1 has no sort key: its range key is its key columns (c0, c1, c2), and
     * c1, c2 are not in the base sort key (c0). c3, c4 are value columns of mv1.
     */
    private static OlapTable rangeTableWithKeyColumnRollup(String name) throws Exception {
        starRocksAssert.withTable("create table " + name + " (c0 int, c1 int, c2 varchar(10), c3 int, c4 int)\n"
                + "duplicate key(c0)\norder by(c0)\nrollup (mv1(c0, c1, c2, c3, c4))\n"
                + "properties('replication_num' = '1');");
        OlapTable t = table(name);
        assertTrue(t.isRangeDistribution());
        MaterializedIndexMeta rollup = meta(t, "mv1");
        assertNull(rollup.getSortKeyIdxes());
        assertEquals(List.of("c0", "c1", "c2"), names(rollup.getSchema().stream().filter(Column::isKey)
                .collect(Collectors.toList())));
        assertEquals(List.of("c0", "c1", "c2"),
                names(MetaUtils.getRangeDistributionColumns(t, rollup.getIndexMetaId())));
        return t;
    }

    @Test
    public void testModifyTypeOfRollupKeyColumnIsRejected() throws Exception {
        OlapTable t = rangeTableWithKeyColumnRollup("t_key_modify");
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "c1", "mv1"),
                rejectedMessage(t, "alter table t_key_modify modify column c1 bigint"));
    }

    @Test
    public void testDropOfRollupKeyColumnIsRejected() throws Exception {
        OlapTable t = rangeTableWithKeyColumnRollup("t_key_drop");
        assertEquals(rangeSortKeyMessage("DROP COLUMN", "c1", "mv1"),
                rejectedMessage(t, "alter table t_key_drop drop column c1"));
    }

    @Test
    public void testReorderOfRollupKeyColumnsIsRejected() throws Exception {
        OlapTable t = rangeTableWithKeyColumnRollup("t_key_reorder");
        assertEquals("Reordering the key columns of a rollup is not supported on tables with range distribution, "
                        + "because they are the rollup's sort key. Index: mv1",
                rejectedMessage(t, "alter table t_key_reorder order by (c1, c0, c2, c3, c4) from mv1"));
    }

    /** Moving only value columns keeps the rollup's key-column order, so the reorder still runs. */
    @Test
    public void testReorderOfRollupValueColumnsIsAccepted() throws Exception {
        OlapTable t = rangeTableWithKeyColumnRollup("t_value_reorder");
        long rollupMetaId = meta(t, "mv1").getIndexMetaId();

        starRocksAssert.alterTable("alter table t_value_reorder order by (c0, c1, c2, c4, c3) from mv1");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        Map<Long, List<Column>> schemas = Deencapsulation.getField(job, "indexMetaIdToSchema");
        assertEquals(List.of("c0", "c1", "c2", "c4", "c3"), names(schemas.get(shadowMetaIdOf(job, rollupMetaId))));
        job.cancel("test");
    }

    /** Positions in range and unique ids of the rollup's own columns, but not the ids at those positions. */
    @Test
    public void testMisalignedRollupSortKeyIsNotItsOwn() throws Exception {
        OlapTable t = rangeTableWithKeyColumnRollup("t_misaligned");
        MaterializedIndexMeta rollup = meta(t, "mv1");
        rollup.setSortKeyIdxes(new ArrayList<>(List.of(0, 1, 2)));
        rollup.setSortKeyUniqueIds(new ArrayList<>(uniqueIds(t, "c1", "c0", "c2")));

        starRocksAssert.alterTable("alter table t_misaligned modify column c4 varchar(20)");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollup.getIndexMetaId());
        Map<Long, List<Integer>> indexSortKeys = Deencapsulation.getField(job, "indexSortKeyIdxes");
        assertFalse(indexSortKeys.containsKey(shadowMetaId), "a misaligned sort key was kept as the rollup's own");
        job.cancel("test");
    }

    /**
     * The column is located through a rollup that carries the base sort key [5], outside its five columns. The type
     * change (INT to VARCHAR) takes the shadow-index job, so the outcome does not depend on the fast paths.
     */
    @Test
    public void testModifyThroughRollupCarryingBaseSortKeyIsAccepted() throws Exception {
        starRocksAssert.withTable("create table t_base_sk_from (c0 int, c1 int, c2 varchar(10), c3 int, c4 int, c5 int)\n"
                + "duplicate key(c0)\norder by(c5)\nrollup (mv1(c0, c1, c2, c3, c4))\n"
                + "properties('replication_num' = '1');");
        OlapTable t = table("t_base_sk_from");
        assertTrue(t.isRangeDistribution());
        long rollupMetaId = giveRollupBaseSortKey(t, "mv1").getIndexMetaId();
        assertEquals(List.of(5), meta(t, "mv1").getSortKeyIdxes());

        starRocksAssert.alterTable("alter table t_base_sk_from modify column c4 varchar(20) after c2 from mv1");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        Map<Long, List<Column>> schemas = Deencapsulation.getField(job, "indexMetaIdToSchema");
        assertEquals(List.of("c0", "c1", "c2", "c4", "c3"), schemas.get(shadowMetaIdOf(job, rollupMetaId)).stream()
                .map(c -> Column.removeNamePrefix(c.getName())).collect(Collectors.toList()));
        job.cancel("test");
    }

    private static SchemaInfo schemaInfoOf(LakeTableAsyncFastSchemaChangeJob job, long indexMetaId) {
        List<IndexSchemaInfo> infos = Deencapsulation.getField(job, "schemaInfos");
        return infos.stream().filter(i -> i.getIndexMetaId() == indexMetaId).findFirst()
                .orElseThrow(() -> new AssertionError("no schema for index " + indexMetaId)).getSchemaInfo();
    }

    // ------------------------------------------------------------------ reorder

    @Test
    public void testReorderOfRollupWithOwnSortKeyIsRejected() throws Exception {
        starRocksAssert.withTable("create table t_reorder (s varchar(16), k int, v int)\n"
                + "duplicate key(s, k)\norder by(k, s)\nproperties('replication_num' = '1');");
        addFinishedRangeRollup("t_reorder", "alter table t_reorder add rollup r_sk(s, k, v) order by (s, k)");
        OlapTable t = table("t_reorder");

        String message = rejectedMessage(t, "alter table t_reorder order by (k, s, v) from r_sk");
        assertEquals("Reordering the columns of a rollup that has its own sort key is not supported on tables "
                + "with range distribution. Index: r_sk", message);
    }

    /** Control: reordering a rollup of a hash table still runs the shadow-index job. */
    @Test
    public void testReorderOfHashRollupIsAccepted() throws Exception {
        starRocksAssert.withTable("create table t_reorder_hash (s varchar(16), k int, v int)\n"
                + "duplicate key(s, k)\ndistributed by hash(k) buckets 2\nrollup (r_h(s, k, v))\n"
                + "properties('replication_num' = '1');");
        OlapTable t = table("t_reorder_hash");
        long rollupMetaId = meta(t, "r_h").getIndexMetaId();

        starRocksAssert.alterTable("alter table t_reorder_hash order by (s, v, k) from r_h");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        Map<Long, List<Column>> schemas = Deencapsulation.getField(job, "indexMetaIdToSchema");
        assertEquals(List.of("s", "v", "k"), names(schemas.get(shadowMetaIdOf(job, rollupMetaId))));
        job.cancel("test");
    }

    // ------------------------------------------------------------------ persistence and replay

    /** A heavy job on r_dup, journaled, with the images a follower can start from. */
    private record JournaledJob(OlapTable leaderTable, long jobId, long rollupShadowMetaId,
                                UtFrameUtils.PseudoImage preAlterImage,
                                UtFrameUtils.PseudoImage waitingTxnCatalogImage,
                                UtFrameUtils.PseudoImage waitingTxnJobImage,
                                List<LakeTableSchemaChangeJob> entries) {
    }

    private static JournaledJob runJournaledHeavyJob(String tableName) throws Exception {
        OlapTable t = rangeTableWithDupRollup(tableName, "");
        long rollupMetaId = meta(t, "r_dup").getIndexMetaId();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage preAlterImage = saveMetastore(GlobalStateMgr.getCurrentState().getLocalMetastore());

        starRocksAssert.alterTable("alter table " + tableName + " modify column v1 varchar(32)");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollupMetaId);
        mockTabletTasks();
        drivePendingToWaitingTxn(job);
        UtFrameUtils.PseudoImage catalogImage = saveMetastore(GlobalStateMgr.getCurrentState().getLocalMetastore());
        UtFrameUtils.PseudoImage jobImage = saveJob(job);
        driveWaitingTxnToFinished(job);

        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(job.getJobId());
        assertEquals(List.of(AlterJobV2.JobState.PENDING, AlterJobV2.JobState.WAITING_TXN,
                        AlterJobV2.JobState.FINISHED_REWRITING, AlterJobV2.JobState.FINISHED),
                entries.stream().map(AlterJobV2::getJobState).collect(Collectors.toList()));
        return new JournaledJob(t, job.getJobId(), shadowMetaId, preAlterImage, catalogImage, jobImage, entries);
    }

    private static List<LakeTableSchemaChangeJob> readJournaledEntries(long jobId) throws Exception {
        List<LakeTableSchemaChangeJob> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
            assertEquals(jobId, entry.getJobId());
            entries.add((LakeTableSchemaChangeJob) entry);
        } while (!entry.isDone());
        return entries;
    }

    private static AlterJobV2 gsonCopy(AlterJobV2 job) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, AlterJobV2.class), AlterJobV2.class);
    }

    private static UtFrameUtils.PseudoImage saveMetastore(LocalMetastore metastore) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        metastore.save(image.getImageWriter());
        return image;
    }

    private static LocalMetastore loadMetastore(UtFrameUtils.PseudoImage image) throws Exception {
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        metastore.load(reader);
        reader.close();
        return metastore;
    }

    private static UtFrameUtils.PseudoImage saveJob(AlterJobV2 job) throws Exception {
        AlterJobMgr mgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        mgr.getSchemaChangeHandler().addAlterJobV2(job);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        mgr.save(image.getImageWriter());
        return image;
    }

    private static SchemaChangeHandler loadJobs(UtFrameUtils.PseudoImage image) throws Exception {
        AlterJobMgr mgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        mgr.load(reader);
        reader.close();
        return mgr.getSchemaChangeHandler();
    }

    /** Replay a copy of the journal entry into the handler against the given catalog, as a follower does. */
    private static void replayOn(LocalMetastore metastore, SchemaChangeHandler handler, AlterJobV2 entry) {
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(metastore);
        try {
            handler.replayAlterJobV2(gsonCopy(entry));
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }
    }

    private static void assertSameRollupAsLeader(OlapTable leader, OlapTable follower) {
        assertEquals(OlapTable.OlapTableState.NORMAL, follower.getState());
        MaterializedIndexMeta l = meta(leader, "r_dup");
        MaterializedIndexMeta f = meta(follower, "r_dup");
        assertEquals(l.getIndexMetaId(), f.getIndexMetaId());
        assertEquals(l.getSortKeyIdxes(), f.getSortKeyIdxes());
        assertEquals(l.getSortKeyUniqueIds(), f.getSortKeyUniqueIds());
        assertEquals(l.getShortKeyColumnCount(), f.getShortKeyColumnCount());
        assertEquals(ranges(leader, l.getIndexMetaId()), ranges(follower, f.getIndexMetaId()));
        assertEquals(partition(leader).getVisibleVersion(), partition(follower).getVisibleVersion());
        assertEquals(partition(leader).getNextVersion(), partition(follower).getNextVersion());
    }

    @Test
    public void testJobWithRollupSortKeySurvivesPersistRoundTrip() throws Exception {
        OlapTable t = rangeTableWithDupRollup("t_persist", "");
        long rollupMetaId = meta(t, "r_dup").getIndexMetaId();
        starRocksAssert.alterTable("alter table t_persist modify column v1 varchar(32)");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollupMetaId);

        Map<Long, List<Integer>> idxes = Deencapsulation.getField(job, "indexSortKeyIdxes");
        Map<Long, List<Integer>> uniqueIds = Deencapsulation.getField(job, "indexSortKeyUniqueIds");
        assertEquals(Map.of(shadowMetaId, List.of(0, 1)), idxes);
        assertEquals(Map.of(shadowMetaId, uniqueIds(t, "k2", "k1")), uniqueIds);

        AlterJobV2 copy = gsonCopy(job.copyForPersist());
        assertEquals(idxes, Deencapsulation.getField(copy, "indexSortKeyIdxes"));
        assertEquals(uniqueIds, Deencapsulation.getField(copy, "indexSortKeyUniqueIds"));
        job.cancel("test");
    }

    /** The whole journal replayed onto the pre-alter catalog gives the follower the leader's rollup. */
    @Test
    public void testFollowerReplayOfJournalMatchesLeader() throws Exception {
        JournaledJob journaled = runJournaledHeavyJob("t_replay");
        LocalMetastore follower = loadMetastore(journaled.preAlterImage());
        SchemaChangeHandler handler = new SchemaChangeHandler();
        for (LakeTableSchemaChangeJob entry : journaled.entries()) {
            replayOn(follower, handler, entry);
        }
        assertSameRollupAsLeader(journaled.leaderTable(), tableIn(follower, "t_replay"));
    }

    /** A follower that starts from an image taken at WAITING_TXN and replays the rest keeps the rollup sort key. */
    @Test
    public void testRecoveryFromWaitingTxnImageKeepsRollupSortKey() throws Exception {
        JournaledJob journaled = runJournaledHeavyJob("t_image");
        LocalMetastore follower = loadMetastore(journaled.waitingTxnCatalogImage());
        SchemaChangeHandler handler = loadJobs(journaled.waitingTxnJobImage());
        for (LakeTableSchemaChangeJob entry : journaled.entries().subList(2, journaled.entries().size())) {
            replayOn(follower, handler, entry);
        }
        OlapTable followerTable = tableIn(follower, "t_image");
        assertSameRollupAsLeader(journaled.leaderTable(), followerTable);
        assertRollupSortKey(followerTable, "r_dup", List.of("k2", "k1"), uniqueIds(followerTable, "k2", "k1"), 2,
                ranges(journaled.leaderTable(), meta(journaled.leaderTable(), "r_dup").getIndexMetaId()));
    }

    /**
     * A job journaled by an FE that does not record the rollup sort key: no per-index sort key fields and the
     * key-column short key for the rollup. Its shadow tablets were created without the rollup sort key, so a
     * replay installs none either.
     */
    private static LakeTableSchemaChangeJob asLegacyEntry(LakeTableSchemaChangeJob entry, long rollupShadowMetaId)
            throws Exception {
        Map<Long, List<Column>> schemas = Deencapsulation.getField(entry, "indexMetaIdToSchema");
        short keyBasedShortKey = GlobalStateMgr.calcShortKeyColumnCount(schemas.get(rollupShadowMetaId), null);
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(entry, AlterJobV2.class)).getAsJsonObject();
        json.remove("indexSortKeyIdxes");
        json.remove("indexSortKeyUniqueIds");
        json.getAsJsonObject("indexShortKeyMap").addProperty(Long.toString(rollupShadowMetaId), keyBasedShortKey);
        return (LakeTableSchemaChangeJob) GsonUtils.GSON.fromJson(json, AlterJobV2.class);
    }

    private static void assertLegacyRollup(OlapTable leader, OlapTable follower) {
        assertEquals(OlapTable.OlapTableState.NORMAL, follower.getState());
        MaterializedIndexMeta m = meta(follower, "r_dup");
        assertNull(m.getSortKeyIdxes());
        assertNull(m.getSortKeyUniqueIds());
        assertEquals(1, m.getShortKeyColumnCount());
        assertEquals(ranges(leader, meta(leader, "r_dup").getIndexMetaId()), ranges(follower, m.getIndexMetaId()));
    }

    @Test
    public void testReplayOfLegacyJournalInstallsNoRollupSortKey() throws Exception {
        JournaledJob journaled = runJournaledHeavyJob("t_legacy");
        List<LakeTableSchemaChangeJob> legacy = new ArrayList<>();
        for (LakeTableSchemaChangeJob entry : journaled.entries()) {
            legacy.add(asLegacyEntry(entry, journaled.rollupShadowMetaId()));
        }

        // From the pre-alter image.
        LocalMetastore follower = loadMetastore(journaled.preAlterImage());
        SchemaChangeHandler handler = new SchemaChangeHandler();
        for (LakeTableSchemaChangeJob entry : legacy) {
            replayOn(follower, handler, entry);
        }
        assertLegacyRollup(journaled.leaderTable(), tableIn(follower, "t_legacy"));

        // From a WAITING_TXN image built by replaying the legacy PENDING and WAITING_TXN entries.
        LocalMetastore midJob = loadMetastore(journaled.preAlterImage());
        SchemaChangeHandler midJobHandler = new SchemaChangeHandler();
        replayOn(midJob, midJobHandler, legacy.get(0));
        replayOn(midJob, midJobHandler, legacy.get(1));
        AlterJobV2 midJobState = midJobHandler.getAlterJobsV2().get(journaled.jobId());
        assertEquals(AlterJobV2.JobState.WAITING_TXN, midJobState.getJobState());
        UtFrameUtils.PseudoImage legacyCatalogImage = saveMetastore(midJob);
        UtFrameUtils.PseudoImage legacyJobImage = saveJob(midJobState);

        LocalMetastore recovered = loadMetastore(legacyCatalogImage);
        SchemaChangeHandler recoveredHandler = loadJobs(legacyJobImage);
        for (LakeTableSchemaChangeJob entry : legacy.subList(2, legacy.size())) {
            replayOn(recovered, recoveredHandler, entry);
        }
        assertLegacyRollup(journaled.leaderTable(), tableIn(recovered, "t_legacy"));
    }

    // ------------------------------------------------------------------ guards

    /** Range table with r_v (v1, k1) ORDER BY (v1, k1): v1 leads the rollup sort key but is a base value column. */
    private static OlapTable rangeTableWithValueSortKeyRollup(String name, boolean withSibling) throws Exception {
        starRocksAssert.withTable(rangeTableSql(name, ""));
        addFinishedRangeRollup(name, "alter table " + name + " add rollup r_v(v1, k1) order by (v1, k1)");
        if (withSibling) {
            // r_other has v1 outside its sort key (k2, k1).
            addFinishedRangeRollup(name, "alter table " + name + " add rollup r_other(k2, k1, v1) order by (k2, k1)");
        }
        return table(name);
    }

    /** Range table with r_s (s, k1) ORDER BY (s, k1): the VARCHAR column s leads the rollup sort key only. */
    private static OlapTable rangeTableWithVarcharSortKeyRollup(String name) throws Exception {
        starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, v1 int, s varchar(32) not null)\n"
                + "duplicate key(k1, k2)\norder by(k1, k2)\nproperties('replication_num' = '1');");
        addFinishedRangeRollup(name, "alter table " + name + " add rollup r_s(s, k1) order by (s, k1)");
        return table(name);
    }

    @Test
    public void testDropRollupSortKeyColumnIsRejected() throws Exception {
        OlapTable t = rangeTableWithValueSortKeyRollup("t_g_drop", true);
        assertEquals(rangeSortKeyMessage("DROP COLUMN", "v1", "r_v"),
                rejectedMessage(t, "alter table t_g_drop drop column v1"));
    }

    @Test
    public void testDropRollupSortKeyColumnFromRollupIsRejected() throws Exception {
        OlapTable t = rangeTableWithValueSortKeyRollup("t_g_drop_from", false);
        assertEquals(rangeSortKeyMessage("DROP COLUMN", "v1", "r_v"),
                rejectedMessage(t, "alter table t_g_drop_from drop column v1 from r_v"));
    }

    @Test
    public void testModifyTypeOfRollupSortKeyColumnIsRejected() throws Exception {
        OlapTable t = rangeTableWithValueSortKeyRollup("t_g_bigint", false);
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "v1", "r_v"),
                rejectedMessage(t, "alter table t_g_bigint modify column v1 bigint"));
    }

    @Test
    public void testModifyRollupSortKeyColumnToVarcharIsRejected() throws Exception {
        OlapTable t = rangeTableWithValueSortKeyRollup("t_g_varchar", false);
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "v1", "r_v"),
                rejectedMessage(t, "alter table t_g_varchar modify column v1 varchar(32)"));
    }

    /** The column is located through r_other, whose sort key does not hold it; sibling r_v must still be checked. */
    @Test
    public void testModifyThroughOtherRollupChecksSiblingSortKey() throws Exception {
        OlapTable t = rangeTableWithValueSortKeyRollup("t_g_sibling", true);
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "v1", "r_v"),
                rejectedMessage(t, "alter table t_g_sibling modify column v1 bigint after k1 from r_other"));
    }

    /** Controls: a reposition of the column inside r_v and a keyness flip are rejected by the existing guards. */
    @Test
    public void testRepositionAndKeynessFlipOfRollupSortKeyColumnAreRejected() throws Exception {
        OlapTable t = rangeTableWithValueSortKeyRollup("t_g_controls", false);
        String reposition = rejectedMessage(t, "alter table t_g_controls modify column v1 int key after k1 from r_v");
        assertTrue(reposition.startsWith("MODIFY COLUMN" + SORT_KEY_SUFFIX + "v1"), reposition);
        String keyness = rejectedMessage(t, "alter table t_g_controls modify column v1 int key");
        assertTrue(keyness.startsWith("MODIFY COLUMN that changes keyness is not supported on tables with range "
                + "distribution"), keyness);
    }

    @Test
    public void testShrinkOfRollupSortKeyVarcharIsRejected() throws Exception {
        OlapTable t = rangeTableWithVarcharSortKeyRollup("t_g_shrink");
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "s", "r_s"),
                rejectedMessage(t, "alter table t_g_shrink modify column s varchar(16) not null"));
    }

    @Test
    public void testWidenWithRepositionOfRollupSortKeyVarcharIsRejected() throws Exception {
        OlapTable t = rangeTableWithVarcharSortKeyRollup("t_g_widen_pos");
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "s", "r_s"),
                rejectedMessage(t, "alter table t_g_widen_pos modify column s varchar(64) not null after k2"));
    }

    @Test
    public void testWidenWithNullabilityChangeOfRollupSortKeyVarcharIsRejected() throws Exception {
        OlapTable t = rangeTableWithVarcharSortKeyRollup("t_g_widen_null");
        assertEquals(rangeSortKeyMessage("MODIFY COLUMN", "s", "r_s"),
                rejectedMessage(t, "alter table t_g_widen_null modify column s varchar(64) null"));
    }

    @Test
    public void testPureWidenOfRollupSortKeyVarcharIsAccepted() throws Exception {
        OlapTable t = rangeTableWithVarcharSortKeyRollup("t_g_widen");
        long rollupMetaId = meta(t, "r_s").getIndexMetaId();
        List<TabletRange> rangesBefore = ranges(t, rollupMetaId);
        starRocksAssert.alterTable("alter table t_g_widen modify column s varchar(64) not null");
        assertTrue(unfinishedJobs(t).isEmpty(), "expected the metadata-only change");
        assertRollupSortKey(t, "r_s", List.of("s", "k1"), uniqueIds(t, "s", "k1"), 1, rangesBefore);
    }

    @Test
    public void testModifyOfColumnOutsideEveryRollupSortKeyIsAccepted() throws Exception {
        OlapTable t = rangeTableWithDupRollup("t_g_accept", "");
        starRocksAssert.alterTable("alter table t_g_accept modify column v1 bigint");
        for (AlterJobV2 job : unfinishedJobs(t)) {
            job.cancel("test");
        }
    }

    // ------------------------------------------------------------------ tablet count

    /** The rollup's shadow tablets are sized against the base table's tablet_reshard_target_size when it is set. */
    @Test
    public void testPlanPartitionShadowSizesAgainstTableTargetSize() throws Exception {
        long oneGiB = 1L << 30;
        starRocksAssert.withTable(rangeTableSql("t_target_size", ", 'tablet_reshard_target_size' = '" + oneGiB + "'"));
        starRocksAssert.alterTable(
                "alter table t_target_size add rollup r_ts(k2, k1, v1) duplicate key(k2) order by (k2, k1)");
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        List<AlterJobV2> jobs = new ArrayList<>(handler.getAlterJobsV2().values());
        handler.clearJobs();
        assertEquals(1, jobs.size());
        LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, jobs.get(0));

        List<Long> receivedTargets = new ArrayList<>();
        new MockUp<TabletPreSplitCoordinator>() {
            @Mock
            public int selectTabletCount(Estimates estimates, int activeComputeNodeCount, long targetSize) {
                receivedTargets.add(targetSize);
                return 1;
            }
        };
        OlapTable t = table("t_target_size");
        PhysicalPartition pp = partition(t);
        MaterializedIndex baseIndex = pp.getLatestIndex(t.getBaseIndexMetaId());
        String partitionName = t.getPartition(pp.getParentId()).getName();
        long oldTargetSize = Config.tablet_reshard_target_size;
        try {
            Config.tablet_reshard_target_size = 10 * oneGiB;
            job.planPartitionShadow(new PendingPartitionPlan(pp.getId(), baseIndex, baseIndex.getShardGroupId(),
                    partitionName, 10 * oneGiB, t.getName(), pp), t, DB);
            assertEquals(List.of(oneGiB), receivedTargets);

            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(
                    GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB), t,
                    new HashMap<>(Map.of("tablet_reshard_target_size", "")));
            receivedTargets.clear();
            job.planPartitionShadow(new PendingPartitionPlan(pp.getId(), baseIndex, baseIndex.getShardGroupId(),
                    partitionName, 10 * oneGiB, t.getName(), pp), t, DB);
            assertEquals(List.of(10 * oneGiB), receivedTargets);
        } finally {
            Config.tablet_reshard_target_size = oldTargetSize;
        }
    }

    // ------------------------------------------------------------------ hash table

    /** A hash table rollup has no sort key of its own: the job records none and the rollup keeps none. */
    @Test
    public void testHashTableHeavyJobRecordsNoRollupSortKey() throws Exception {
        starRocksAssert.withTable("create table t_hash (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\ndistributed by hash(k1) buckets 2\norder by(k1, k2)\n"
                + "rollup (r_h(k2, k1, v1) duplicate key(k2, k1))\nproperties('replication_num' = '1');");
        OlapTable t = table("t_hash");
        long rollupMetaId = meta(t, "r_h").getIndexMetaId();

        starRocksAssert.alterTable("alter table t_hash modify column v1 varchar(32)");
        LakeTableSchemaChangeJob job = takeHeavyJob(t);
        long shadowMetaId = shadowMetaIdOf(job, rollupMetaId);
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(job, AlterJobV2.class)).getAsJsonObject();
        for (String field : List.of("indexSortKeyIdxes", "indexSortKeyUniqueIds")) {
            assertTrue(!json.has(field) || json.getAsJsonObject(field).size() == 0, field + " is not empty");
        }
        List<CreateReplicaTask> createTasks = driveToFinished(job);

        assertNull(meta(t, "r_h").getSortKeyIdxes());
        assertNull(meta(t, "r_h").getSortKeyUniqueIds());
        boolean sawRollupTablet = false;
        for (CreateReplicaTask task : createTasks) {
            if (task.getIndexId() == shadowMetaId) {
                sawRollupTablet = true;
                List<Integer> idxes = task.toThrift().getTablet_schema().getSort_key_idxes();
                assertTrue(idxes == null || idxes.isEmpty());
            }
        }
        assertTrue(sawRollupTablet);
    }
}
