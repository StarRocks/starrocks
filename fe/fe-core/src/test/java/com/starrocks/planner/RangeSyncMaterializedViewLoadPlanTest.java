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

package com.starrocks.planner;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeRangeRollupJob;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TOlapTableIndexSchema;
import com.starrocks.thrift.TOlapTableSchemaParam;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real stream-load and INSERT planning; only the UtFrame backend infrastructure is mocked. */
public class RangeSyncMaterializedViewLoadPlanTest {
    private static ConnectContext context;
    private static StarRocksAssert starRocksAssert;
    private static Database db;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        // Shadow tablets below are catalog fixtures, so resolve their StarOS location to the live UtFrame backend.
        new mockit.MockUp<com.starrocks.lake.StarOSAgent>() {
            @mockit.Mock
            public List<Long> getAllNodeIdsByShard(long shardId, long workerGroupId) {
                return List.of(10001L);
            }

            @mockit.Mock
            public Map<Long, List<Long>> getAllNodeIdsByShards(List<Long> shardIds, long workerGroupId) {
                return shardIds.stream().collect(Collectors.toMap(id -> id, id -> List.of(10001L)));
            }
        };
        context = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(context);
        starRocksAssert.withDatabase("range_sync_plan").useDatabase("range_sync_plan");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("range_sync_plan");
    }

    private OlapTable createVisibleRangeSyncMv(String tableName, String mvName) throws Exception {
        starRocksAssert.withTable("create table " + tableName + " (id int, g int, v bigint, n bigint, filter_col int) "
                + "duplicate key(id) partition by range(id) "
                + "(partition p1 values less than('4'), partition p2 values less than(MAXVALUE)) "
                + "order by(id) properties('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTable(tableName);
        CreateSyncMVStmt create = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "create materialized view " + mvName + " as select g, sum(v), count(n) "
                        + "from " + tableName + " where filter_col > 0 group by g", context);
        return installVisibleMv(table, create);
    }

    private OlapTable installVisibleMv(OlapTable table, CreateSyncMVStmt create) throws Exception {
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        handler.processCreateMaterializedView(create, db, table);
        LakeRangeRollupJob job = handler.getAlterJobsV2().values().stream()
                .filter(candidate -> candidate.getTableId() == table.getId())
                .map(candidate -> assertInstanceOf(LakeRangeRollupJob.class, candidate)).findFirst().orElseThrow();
        handler.clearJobs();
        Deencapsulation.invoke(job, "beforeShadowBuild");
        Deencapsulation.invoke(job, "registerShadowIndexMeta", table);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            MaterializedIndex index = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                    job.getShadowIndexMetaId(), MaterializedIndex.IndexState.NORMAL,
                    partition.getLatestIndex(table.getBaseIndexMetaId()).getShardGroupId());
            LakeTablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            tablet.setRange(new TabletRange(Range.all()));
            index.addTablet(tablet, null, false);
            partition.createRollupIndex(index);
        }
        table.rebuildFullSchema();
        table.setState(OlapTable.OlapTableState.NORMAL);
        return table;
    }

    @Test
    public void testDirectRangeSyncMvAggregateWithCommonPlanExtraction() throws Exception {
        OlapTable table = createVisibleRangeSyncMv("read_source", "mv_read");
        assertFalse(table.getColumn("g").isKey());
        assertTrue(table.getIndexMetaByMetaId(table.getIndexMetaIdByName("mv_read")).getSchema().get(0).isKey());

        boolean virtualColumns = Config.enable_virtual_columns;
        double cteReuseRatio = context.getSessionVariable().getCboCTERuseRatio();
        boolean mvRewrite = context.getSessionVariable().isEnableMaterializedViewRewrite();
        boolean syncMvRewrite = context.getSessionVariable().isEnableSyncMaterializedViewRewrite();
        Config.enable_virtual_columns = true;
        context.getSessionVariable().setCboCTERuseRatio(0);
        context.getSessionVariable().setEnableMaterializedViewRewrite(false);
        context.getSessionVariable().setEnableSyncMaterializedViewRewrite(false);
        try {
            ExecPlan plan = UtFrameUtils.getPlanAndFragment(context,
                    "select g, sum(mv_sum_v), sum(mv_count_n) from mv_read [_SYNC_MV_] group by g order by g").second;
            assertTrue(plan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL)
                    .contains("rollup: mv_read"));
            assertFalse(table.getColumn("g").isKey(), "query-only metadata must not change the source schema");
            plan = UtFrameUtils.getPlanAndFragment(context,
                    "select sum(mv_sum_v), sum(mv_count_n) from mv_read [_SYNC_MV_]").second;
            assertTrue(plan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL)
                    .contains("rollup: mv_read"));
            plan = UtFrameUtils.getPlanAndFragment(context,
                    "select _tablet_id_, count(*) from mv_read [_SYNC_MV_] group by _tablet_id_").second;
            assertTrue(plan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL)
                    .contains("rollup: mv_read"));
            plan = UtFrameUtils.getPlanAndFragment(context,
                    "select g, sum(v) from read_source group by g union all "
                            + "select g, sum(mv_sum_v) from mv_read [_SYNC_MV_] group by g").second;
            String explain = plan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL);
            assertTrue(explain.contains("rollup: read_source"), explain);
            assertTrue(explain.contains("rollup: mv_read"), explain);
            context.getSessionVariable().setEnableMaterializedViewRewrite(true);
            context.getSessionVariable().setEnableSyncMaterializedViewRewrite(true);
            plan = UtFrameUtils.getPlanAndFragment(context,
                    "select g, sum(v), count(n) from read_source where filter_col > 0 group by g order by g").second;
            explain = plan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL);
            assertTrue(explain.contains("rollup: mv_read"), explain);
            // The same MV group can occur in both logical partitions, so an index hit still needs a final merge.
            assertTrue(explain.contains("AGGREGATE"), explain);
            assertTrue(explain.contains("sum("), explain);
        } finally {
            Config.enable_virtual_columns = virtualColumns;
            context.getSessionVariable().setCboCTERuseRatio(cteReuseRatio);
            context.getSessionVariable().setEnableMaterializedViewRewrite(mvRewrite);
            context.getSessionVariable().setEnableSyncMaterializedViewRewrite(syncMvRewrite);
        }
    }

    @Test
    public void testDirectSyncMvQueryPreservesMinMaxStats() throws Exception {
        OlapTable table = createVisibleRangeSyncMv("cached_source", "mv_cached");
        IMinMaxStatsMgr manager = IMinMaxStatsMgr.internalInstance();
        AsyncLoadingCache<ColumnIdentifier, Optional<?>> cache = Deencapsulation.getField(manager, "cache");
        ColumnIdentifier identifier = new ColumnIdentifier(table.getId(), table.getColumn("g").getColumnId());
        IMinMaxStatsMgr.ColumnMinMax minMax = new IMinMaxStatsMgr.ColumnMinMax("1", "60");
        Long lastUpdate = StatisticUtils.getTableLastUpdateTimestamp(table);
        assertNotNull(lastUpdate);
        StatsVersion version = new StatsVersion(lastUpdate, lastUpdate);
        var constructor = Class.forName("com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr$CacheValue")
                .getDeclaredConstructor(IMinMaxStatsMgr.ColumnMinMax.class, StatsVersion.class);
        constructor.setAccessible(true);
        CompletableFuture<Optional<?>> cached =
                CompletableFuture.completedFuture(Optional.of(constructor.newInstance(minMax, version)));
        cache.put(identifier, cached);
        boolean rewriteMeta = context.getSessionVariable().isEnableRewriteSimpleAggToMetaScan();
        boolean mvRewrite = context.getSessionVariable().isEnableMaterializedViewRewrite();
        boolean syncMvRewrite = context.getSessionVariable().isEnableSyncMaterializedViewRewrite();
        context.getSessionVariable().setEnableMaterializedViewRewrite(false);
        context.getSessionVariable().setEnableSyncMaterializedViewRewrite(false);
        context.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        try {
            assertEquals(Optional.of(minMax), manager.getStats(identifier, version));
            UtFrameUtils.getPlanAndFragment(context,
                    "select g, sum(mv_sum_v) from mv_cached [_SYNC_MV_] group by g");
            assertSame(cached, cache.getIfPresent(identifier), "direct MV planning must preserve the source cache");
            assertEquals(Optional.of(minMax), manager.getStats(identifier, version));
            ExecPlan basePlan = UtFrameUtils.getPlanAndFragment(context,
                    "select min(g), max(g) from cached_source").second;
            String baseExplain = basePlan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL);
            assertTrue(baseExplain.contains("constant exprs"), baseExplain);
            ExecPlan mvPlan = UtFrameUtils.getPlanAndFragment(context,
                    "select min(g), max(g) from mv_cached [_SYNC_MV_]").second;
            String mvExplain = mvPlan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL);
            assertTrue(mvExplain.contains("rollup: mv_cached"), mvExplain);
            assertFalse(mvExplain.contains("constant exprs"), mvExplain);
            assertSame(cached, cache.getIfPresent(identifier), "MV statistics must not replace the base cache");
            table.rebuildFullSchema();
            assertNull(cache.getIfPresent(identifier), "catalog schema rebuilding must still invalidate statistics");
        } finally {
            context.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(rewriteMeta);
            context.getSessionVariable().setEnableMaterializedViewRewrite(mvRewrite);
            context.getSessionVariable().setEnableSyncMaterializedViewRewrite(syncMvRewrite);
            cache.synchronous().invalidate(identifier);
        }
    }

    @Test
    public void testRecoveredStreamLoadAndTargetOnlyBackfillPlan() throws Exception {
        starRocksAssert.withTable("create table source (k1 int, k2 int, v1 int, d decimal(10,2)) "
                + "partition by range(k1) (partition p0 values less than('10'), partition p1 values less than(MAXVALUE)) "
                + "order by (k1,k2) properties('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTable("source");
        CreateSyncMVStmt create = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "create materialized view mv_plan as select k2 + 1 as derived, count(v1), bitmap_agg(v1), "
                        + "hll_union(hll_hash(v1)), avg_union(avg_state(d)) as avg_value "
                        + "from source where k1 > 0 group by k2 + 1", context);
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        handler.processCreateMaterializedView(create, db, table);
        LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class,
                handler.getAlterJobsV2().values().iterator().next());
        handler.clearJobs();
        List<Column> creationSchema = Deencapsulation.getField(job, "rollupSchema");
        Column creationAvg = creationSchema.stream().filter(column -> column.getAggStateDesc() != null)
                .findFirst().orElseThrow();
        com.starrocks.sql.ast.expression.FunctionCallExpr creationState =
                assertInstanceOf(com.starrocks.sql.ast.expression.FunctionCallExpr.class, creationAvg.getDefineExpr());
        List<com.starrocks.thrift.TTypeDesc> avgArgumentTypes = java.util.Arrays.stream(creationState.getFn().getArgs()).map(com.starrocks.type.TypeSerializer::toThrift)
                .collect(Collectors.toList());
        LakeRangeRollupJob recovered = assertInstanceOf(LakeRangeRollupJob.class,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job.copyForPersist()), AlterJobV2.class));
        Deencapsulation.invoke(recovered, "beforeShadowBuild");
        Deencapsulation.invoke(recovered, "registerShadowIndexMeta", table);
        long streamTxnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(db.getId(),
                List.of(table.getId()), "range_stream",
                new com.starrocks.transaction.TransactionState.TxnCoordinator(
                        com.starrocks.transaction.TransactionState.TxnSourceType.BE, "127.0.0.1"),
                com.starrocks.transaction.TransactionState.LoadJobSourceType.BACKEND_STREAMING, 600);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            MaterializedIndex shadow = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                    job.getShadowIndexMetaId(), MaterializedIndex.IndexState.SHADOW,
                    partition.getLatestIndex(table.getBaseIndexMetaId()).getShardGroupId());
            LakeTablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            tablet.setRange(new TabletRange(Range.all()));
            shadow.addTablet(tablet, null, false);
            shadow.setVisibleTxnId(streamTxnId);
            shadow.setShadowWatershedTxnId(streamTxnId);
            partition.createRollupIndex(shadow);
        }
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(job.getShadowIndexMetaId());
        assertNotNull(meta.getWhereClause());
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(streamTxnId);
        request.setLoadId(new TUniqueId(2, 3));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        StreamLoadInfo info = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        com.starrocks.lake.LakeTable image = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table),
                com.starrocks.lake.LakeTable.class);
        image.onReload();
        TExecPlanFragmentParams stream = new StreamLoadPlanner(context, db, image, info).plan(info.getId());
        TOlapTableSchemaParam streamSchema = stream.getFragment().getOutput_sink().getOlap_table_sink().getSchema();
        assertEquals(2, streamSchema.getIndexesSize());
        assertEquals(2, stream.getFragment().getOutput_sink().getOlap_table_sink().getPartition().getPartitionsSize());
        assertMvRouting(streamSchema, job.getShadowIndexMetaId());
        TOlapTableIndexSchema base = streamSchema.getIndexes().stream()
                .filter(index -> index.getId() == table.getBaseIndexMetaId()).findFirst().orElseThrow();
        List<String> baseRoutingColumns = base.getDistributed_exprs().stream().map(expr -> {
            int id = expr.getNodes().get(0).getSlot_ref().getSlot_id();
            return streamSchema.getSlot_descs().stream().filter(slot -> slot.getId() == id)
                    .findFirst().orElseThrow().getColName();
        }).collect(Collectors.toList());
        assertEquals(List.of("k1", "k2"), baseRoutingColumns);
        Map<Integer, TExpr> contributions = stream.getParams().getPer_node_scan_ranges().get(0).get(0)
                .getScan_range().getBroker_scan_range().getParams().getExpr_of_dest_slot();
        Map<String, Integer> slots = streamSchema.getSlot_descs().stream()
                .collect(Collectors.toMap(slot -> slot.getColName(), slot -> slot.getId()));
        for (Column column : meta.getSchema()) {
            TExpr contribution = contributions.get(slots.get(column.getName()));
            assertNotNull(contribution, "missing stream-load projection for " + column.getName());
            assertEquals(com.starrocks.type.TypeSerializer.toThrift(column.getType()), contribution.getNodes().get(0).getType());
            if (column.getName().contains("count")) {
                assertEquals(com.starrocks.type.IntegerType.BIGINT, column.getDefineExpr().getType());
            }
            if (column.getAggStateDesc() != null) {
                com.starrocks.sql.ast.expression.FunctionCallExpr restoredState =
                        assertInstanceOf(com.starrocks.sql.ast.expression.FunctionCallExpr.class, column.getDefineExpr());
                assertEquals(avgArgumentTypes, java.util.Arrays.stream(restoredState.getFn().getArgs())
                        .map(com.starrocks.type.TypeSerializer::toThrift).collect(Collectors.toList()));
                assertAvgOverload(contribution, avgArgumentTypes);
            }
        }
        InsertStmt insert = (InsertStmt) com.starrocks.sql.parser.SqlParser.parseOneWithStarRocksDialect(
                "insert into source(k1,k2,v1,d) select k1,k2,v1,d from source", context.getSessionVariable());
        insert.setTargetWriteIndexId(job.getShadowIndexMetaId());
        insert.setShadowRewrite(true);
        insert.setShadowRewriteWatershedTxnId(10);
        insert.setShadowRewriteAlterVersion(1);
        context.getSessionVariable().setEnableMaterializedViewRewrite(false);
        context.getSessionVariable().setEnableSyncMaterializedViewRewrite(false);
        context.setScanVersionOverride(Map.of(table.getPhysicalPartitions().iterator().next().getId(), 1L));
        try {
            ExecPlan plan = StatementPlanner.plan(insert, context);
            TOlapTableSchemaParam insertSchema = plan.getFragments().get(0).getSink().toThrift()
                    .getOlap_table_sink().getSchema();
            assertEquals(1, insertSchema.getIndexesSize());
            assertMvRouting(insertSchema, job.getShadowIndexMetaId());
            String explain = plan.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL);
            assertTrue(explain.contains("rollup: source"), explain);
            assertTrue(explain.contains("to_bitmap"), explain);
            assertTrue(explain.contains("avg_state"), explain);
            assertTrue(explain.contains("if("), explain);
            Map<Integer, TExpr> insertContributions = plan.getFragments().get(0).toThrift().getPlan().getNodes().stream()
                    .filter(node -> node.isSetProject_node()).flatMap(node -> node.getProject_node().getSlot_map()
                            .entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (first, second) -> first));
            for (Column column : meta.getSchema()) {
                int position = java.util.stream.IntStream.range(0, table.getFullSchema().size())
                        .filter(i -> table.getFullSchema().get(i).getName().equals(column.getName())).findFirst().orElseThrow();
                com.starrocks.sql.ast.expression.Expr output = plan.getOutputExprs().get(position);
                assertEquals(column.getType(), output.getType());
                int projectionSlot = assertInstanceOf(com.starrocks.sql.ast.expression.SlotRef.class, output).getSlotId().asInt();
                TExpr contribution = insertContributions.get(projectionSlot);
                assertNotNull(contribution, "missing INSERT projection for " + column.getName());
                assertEquals(com.starrocks.type.TypeSerializer.toThrift(column.getType()),
                        contribution.getNodes().get(0).getType());
                if (column.getAggStateDesc() != null) {
                    assertAvgOverload(contribution, avgArgumentTypes);
                }
            }
        } finally {
            context.setScanVersionOverride(null);
            context.getSessionVariable().setEnableMaterializedViewRewrite(true);
            context.getSessionVariable().setEnableSyncMaterializedViewRewrite(true);
        }
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            partition.visualiseShadowIndex(partition.getLatestIndex(job.getShadowIndexMetaId()).getId(), false);
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
        ExecPlan rewritten = UtFrameUtils.getPlanAndFragment(context,
                "select k2 + 1 as derived, count(v1), bitmap_agg(v1), hll_union(hll_hash(v1)), "
                        + "avg_union(avg_state(d)) as avg_value from source where k1 > 0 group by k2 + 1").second;
        assertTrue(rewritten.getExplainString(com.starrocks.sql.ast.StatementBase.ExplainLevel.NORMAL)
                .contains("rollup: mv_plan"));
    }

    private static void assertAvgOverload(TExpr contribution, List<com.starrocks.thrift.TTypeDesc> argumentTypes) {
        com.starrocks.thrift.TExprNode state = contribution.getNodes().stream().filter(node -> node.isSetFn()
                        && node.getFn().getName().getFunction_name().equalsIgnoreCase("avg_state"))
                .findFirst().orElseThrow();
        assertEquals(argumentTypes, state.getFn().getArg_types());
    }

    private static void assertMvRouting(TOlapTableSchemaParam schema, long mvId) {
        TOlapTableIndexSchema index = schema.getIndexes().stream().filter(i -> i.getId() == mvId).findFirst().orElseThrow();
        assertTrue(index.isSetWhere_clause());
        assertEquals(1, index.getDistributed_exprsSize());
        int slot = index.getDistributed_exprs().get(0).getNodes().get(0).getSlot_ref().getSlot_id();
        String name = schema.getSlot_descs().stream().filter(s -> s.getId() == slot).findFirst().orElseThrow().getColName();
        assertEquals(index.getColumns().get(0), name);
        assertTrue(name.contains("derived"));
    }

    @Test
    public void testStructFieldProjectionUsesRootSourceForStreamLoad() throws Exception {
        starRocksAssert.withTable("CREATE TABLE struct_load_source (id INT,v BIGINT,s STRUCT<a INT,b INT>) "
                + "DUPLICATE KEY(id) ORDER BY(id) PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTable("struct_load_source");
        CreateSyncMVStmt create = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE MATERIALIZED VIEW mv_struct_load AS SELECT s.a+1 AS projected,v FROM struct_load_source", context);
        installVisibleMv(table, create);
        long transaction = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(db.getId(),
                List.of(table.getId()), "struct_stream",
                new com.starrocks.transaction.TransactionState.TxnCoordinator(
                        com.starrocks.transaction.TransactionState.TxnSourceType.BE, "127.0.0.1"),
                com.starrocks.transaction.TransactionState.LoadJobSourceType.BACKEND_STREAMING, 600);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(transaction);
        request.setLoadId(new TUniqueId(4, 5));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        StreamLoadInfo info = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        TExecPlanFragmentParams plan = new StreamLoadPlanner(context, db, table, info).plan(info.getId());
        TOlapTableSchemaParam schema = plan.getFragment().getOutput_sink().getOlap_table_sink().getSchema();
        int slot = schema.getSlot_descs().stream().filter(desc -> desc.getColName().equals("mv_projected"))
                .findFirst().orElseThrow().getId();
        TExpr contribution = plan.getParams().getPer_node_scan_ranges().get(0).get(0)
                .getScan_range().getBroker_scan_range().getParams().getExpr_of_dest_slot().get(slot);
        assertNotNull(contribution);
        assertTrue(contribution.getNodes().stream().anyMatch(node ->
                node.getNode_type() == com.starrocks.thrift.TExprNodeType.SUBFIELD_EXPR));
    }

    private OlapTable createRecoveredStructPredicateTable(String name) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + name
                + " (id INT,v BIGINT,s STRUCT<a INT,b STRUCT<c INT>>) "
                + "DUPLICATE KEY(id) ORDER BY(id) PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTable(name);
        CreateSyncMVStmt create = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE MATERIALIZED VIEW mv_" + name + " AS SELECT id,v FROM " + name
                        + " WHERE s.a > 0 AND s.b.c > 1", context);
        installVisibleMv(table, create);
        long mvId = table.getIndexMetaIdByName("mv_" + name);
        MaterializedIndexMeta restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(table.getIndexMetaByMetaId(mvId)), MaterializedIndexMeta.class);
        table.analyzeRollupIndexMeta(restored);
        table.getIndexMetaIdToMeta().put(mvId, restored);
        table.rebuildFullSchema();
        return table;
    }

    private static void assertStructPredicate(TOlapTableSchemaParam schema, long mvId) {
        TExpr predicate = schema.getIndexes().stream().filter(index -> index.getId() == mvId)
                .findFirst().orElseThrow().getWhere_clause();
        assertNotNull(predicate);
        List<List<String>> fields = predicate.getNodes().stream()
                .filter(node -> node.getNode_type() == com.starrocks.thrift.TExprNodeType.SUBFIELD_EXPR)
                .map(node -> node.getUsed_subfield_names()).collect(Collectors.toList());
        assertTrue(fields.contains(List.of("a")), fields.toString());
        assertTrue(fields.contains(List.of("b", "c")), fields.toString());
        for (com.starrocks.thrift.TExprNode node : predicate.getNodes()) {
            if (node.isSetSlot_ref()) {
                String name = schema.getSlot_descs().stream()
                        .filter(slot -> slot.getId() == node.getSlot_ref().getSlot_id())
                        .findFirst().orElseThrow().getColName();
                assertEquals("s", name);
            }
        }
    }

    @Test
    public void testRecoveredStructPredicateInsertAndBackfill() throws Exception {
        OlapTable table = createRecoveredStructPredicateTable("struct_where_insert");
        long mvId = table.getIndexMetaIdByName("mv_struct_where_insert");
        String originalPredicate = ExprToSql.toSql(table.getIndexMetaByMetaId(mvId).getWhereClause());
        for (boolean backfill : List.of(false, true)) {
            InsertStmt insert = (InsertStmt) com.starrocks.sql.parser.SqlParser.parseOneWithStarRocksDialect(
                    "INSERT INTO struct_where_insert SELECT id,v,s FROM struct_where_insert",
                    context.getSessionVariable());
            insert.setLabel("struct_where_insert_" + backfill);
            if (backfill) {
                insert.setTargetWriteIndexId(mvId);
                insert.setShadowRewrite(true);
                insert.setShadowRewriteWatershedTxnId(10);
                insert.setShadowRewriteAlterVersion(1);
            }
            ExecPlan plan = StatementPlanner.plan(insert, context);
            TOlapTableSchemaParam schema = plan.getFragments().get(0).getSink().toThrift()
                    .getOlap_table_sink().getSchema();
            assertEquals(backfill ? 1 : 2, schema.getIndexesSize());
            assertStructPredicate(schema, mvId);
            assertEquals(originalPredicate, ExprToSql.toSql(table.getIndexMetaByMetaId(mvId).getWhereClause()));
        }
    }

    @Test
    public void testRecoveredStructPredicateStreamLoad() throws Exception {
        OlapTable table = createRecoveredStructPredicateTable("struct_where_stream");
        long mvId = table.getIndexMetaIdByName("mv_struct_where_stream");
        String originalPredicate = ExprToSql.toSql(table.getIndexMetaByMetaId(mvId).getWhereClause());
        long transaction = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(db.getId(),
                List.of(table.getId()), "struct_where_stream",
                new com.starrocks.transaction.TransactionState.TxnCoordinator(
                        com.starrocks.transaction.TransactionState.TxnSourceType.BE, "127.0.0.1"),
                com.starrocks.transaction.TransactionState.LoadJobSourceType.BACKEND_STREAMING, 600);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(transaction);
        request.setLoadId(new TUniqueId(6, 7));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        StreamLoadInfo info = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        TExecPlanFragmentParams plan = new StreamLoadPlanner(context, db, table, info).plan(info.getId());
        TOlapTableSchemaParam schema = plan.getFragment().getOutput_sink().getOlap_table_sink().getSchema();
        assertEquals(2, schema.getIndexesSize());
        assertStructPredicate(schema, mvId);
        assertEquals(originalPredicate, ExprToSql.toSql(table.getIndexMetaByMetaId(mvId).getWhereClause()));
    }
}
