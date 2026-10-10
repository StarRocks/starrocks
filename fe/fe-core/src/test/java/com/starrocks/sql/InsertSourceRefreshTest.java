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


package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * INSERT ... SELECT with enable_insert_select_external_auto_refresh refreshes every filesystem-backed source it reads,
 * through views of any depth, exactly once per plan, and fails when a refresh fails -- the semantics it had when the
 * refresh ran in InsertPlanner, before the refresh moved into the unlocked pre-pass.
 */
public class InsertSourceRefreshTest extends ConnectorPlanTestBase {
    // A chain of views. The pre-pass does not expand views, so a source behind one is refreshed after analysis.
    private static final int DEEP_VIEW_LEVELS = 20;

    private static class RecordingHiveMetadata extends MockedHiveMetadata {
        final List<String> refreshed = new CopyOnWriteArrayList<>();
        private final boolean fail;

        private RecordingHiveMetadata(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void invalidateTableForRead(String srDbName, Table table) {
            refreshed.add(table.getCatalogTableName());
            if (fail) {
                throw new RuntimeException("mock refresh failure");
            }
        }
    }

    /**
     * Hands out a new table object after every refresh, as a real catalog does once its cache is dropped, optionally
     * with a different schema.
     */
    private enum SchemaChange { NONE, DROP_LAST_COLUMN, FLIP_NULLABILITY }

    private static class ReloadingHiveMetadata extends RecordingHiveMetadata {
        private final Map<String, Table> reloaded = new HashMap<>();
        private final SchemaChange schemaChange;
        private int generation;

        ReloadingHiveMetadata(boolean changeSchema) {
            this(changeSchema ? SchemaChange.DROP_LAST_COLUMN : SchemaChange.NONE);
        }

        ReloadingHiveMetadata(SchemaChange schemaChange) {
            super(false);
            this.schemaChange = schemaChange;
        }

        @Override
        public synchronized void invalidateTableForRead(String srDbName, Table table) {
            super.invalidateTableForRead(srDbName, table);
            generation++;
        }

        @Override
        public synchronized Table getTable(ConnectContext context, String dbName, String tblName) {
            Table base = super.getTable(context, dbName, tblName);
            if (generation == 0 || !(base instanceof HiveTable hive)) {
                return base;
            }
            return reloaded.computeIfAbsent(tblName + "#" + generation, k -> {
                List<Column> schema = new ArrayList<>(hive.getFullSchema());
                if (schemaChange == SchemaChange.DROP_LAST_COLUMN) {
                    schema.remove(schema.size() - 1);
                } else if (schemaChange == SchemaChange.FLIP_NULLABILITY) {
                    Column first = new Column(schema.get(0));
                    first.setIsAllowNull(!first.isAllowNull());
                    schema.set(0, first);
                }
                return new HiveTable(hive.getId(), hive.getName(), schema, hive.getResourceName(),
                        hive.getCatalogName(), hive.getCatalogDBName(), hive.getCatalogTableName(),
                        hive.getTableLocation(), hive.getComment(), hive.getCreateTime(),
                        hive.getPartitionColumnNames(), hive.getDataColumnNames(), hive.getProperties(),
                        hive.getSerdeProperties(), hive.getStorageFormat(), hive.getHiveTableType());
            });
        }

        private synchronized Table latest(String tblName) {
            return reloaded.get(tblName + "#" + generation);
        }
    }

    private static List<Table> boundSources(StatementBase stmt) {
        List<Table> tables = new ArrayList<>();
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitTable(TableRelation node, Void context) {
                if (node.getTable() != null && node.getTable().isHiveTable()) {
                    tables.add(node.getTable());
                }
                return null;
            }
        }.visit(stmt);
        return tables;
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        starRocksAssert.withView("create view test.v_lineitem as " +
                "select l_orderkey, l_partkey from hive0.tpch.lineitem");
        String previous = "v_lineitem";
        for (int i = 1; i <= DEEP_VIEW_LEVELS; i++) {
            String name = "v_lineitem_" + i;
            starRocksAssert.withView("create view test." + name + " as select * from test." + previous);
            previous = name;
        }
    }

    @AfterEach
    public void restore() {
        register(new MockedHiveMetadata());
        connectContext.getPreResolvedState().clear();
    }

    private static void register(MockedHiveMetadata metadata) {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, metadata);
    }

    private static StatementBase analyze(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
            }
        };
        StatementPlanner.analyzeStatement(stmt, connectContext, locker);
        return stmt;
    }

    @Test
    public void testDirectSourceRefreshedOnce() throws Exception {
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem");
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
    }

    @Test
    public void testSourceInViewRefreshedOnce() throws Exception {
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        // Read directly and through a view: still one refresh.
        analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem " +
                "union all select l_orderkey, l_partkey from hive0.tpch.lineitem");
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
    }

    @Test
    public void testSourceBehindDeepViewsRefreshed() throws Exception {
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem_" + DEEP_VIEW_LEVELS);
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
    }

    @Test
    public void testRefreshFailureInViewFailsStatement() throws Exception {
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(true);
        register(metadata);
        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem"));
        Assertions.assertFalse(metadata.refreshed.isEmpty());
    }

    @Test
    public void testRefreshFailureBehindDeepViewsFailsStatement() throws Exception {
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(true);
        register(metadata);
        Assertions.assertThrows(RuntimeException.class, () -> analyze(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem_" + DEEP_VIEW_LEVELS));
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
    }

    @Test
    public void testEveryPlanRefreshesAgain() throws Exception {
        // The record is per plan: INSERT OVERWRITE plans twice and refreshes before each, as it always did.
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        String sql = "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem";
        for (int i = 0; i < 2; i++) {
            // A fresh execution id, so the second plan does not reuse the first one's transaction label.
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
            StatementPlanner.plan(UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext),
                    connectContext);
        }
        Assertions.assertEquals(List.of("lineitem", "lineitem"), metadata.refreshed);
    }

    @Test
    public void testSourceInScalarSubqueryRefreshed() throws Exception {
        // A subquery in the select list or HAVING is an expression before analysis, which the pre-pass does not
        // walk; the refresh after analysis does.
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        analyze("insert into t0 (v1, v2) select (select max(l_orderkey) from hive0.tpch.lineitem), v5 from t1");
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);

        metadata.refreshed.clear();
        connectContext.getPreResolvedState().clear();
        analyze("insert into t0 (v1, v2) select v4, count(*) from t1 group by v4 " +
                "having count(*) > (select count(*) from hive0.tpch.lineitem)");
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
    }

    @Test
    public void testCtasSourceRefreshed() throws Exception {
        // CTAS analyzes its query before the table exists, and plans the INSERT it carries afterwards: by then
        // every relation is resolved, so the pre-pass skips them and the refresh after analysis must not.
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(
                "create table test.ctas_from_hive distributed by hash(l_orderkey) buckets 1 " +
                        "properties('replication_num'='1') " +
                        "as select l_orderkey, l_partkey from test.v_lineitem", connectContext);
        Assertions.assertTrue(metadata.refreshed.isEmpty(), "analyzing the CTAS itself does not refresh");
        try {
            // What StmtExecutor does: create the table, then plan the INSERT.
            GlobalStateMgr.getCurrentState().getMetadataMgr().createTable(connectContext, ctas.getCreateTableStmt());
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
            StatementPlanner.plan(ctas.getInsertStmt(), connectContext);
            Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
        } finally {
            starRocksAssert.dropTable("ctas_from_hive");
        }
    }

    @Test
    public void testReplanOfSameStatementRefreshesAgain() throws Exception {
        // A retry (ExecuteExceptionHandler) or INSERT OVERWRITE plans the already analyzed statement again.
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem", connectContext);
        for (int i = 0; i < 2; i++) {
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
            StatementPlanner.plan(stmt, connectContext);
        }
        Assertions.assertEquals(List.of("lineitem", "lineitem"), metadata.refreshed);
    }

    @Test
    public void testPlanReadsReloadedTable() throws Exception {
        // Whether the pre-pass reaches the source or only the refresh after analysis does, every relation of it is
        // bound to the table reloaded after the refresh: a table object can carry the data it reads.
        for (String source : List.of("hive0.tpch.lineitem", "test.v_lineitem",
                "test.v_lineitem_" + DEEP_VIEW_LEVELS)) {
            ReloadingHiveMetadata metadata = new ReloadingHiveMetadata(false);
            register(metadata);
            connectContext.getPreResolvedState().clear();
            StatementBase stmt = analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from " + source +
                    " union all select (select max(l_orderkey) from hive0.tpch.lineitem), 1");
            Assertions.assertEquals(List.of("lineitem"), metadata.refreshed, source);
            List<Table> bound = boundSources(stmt);
            Assertions.assertEquals(2, bound.size(), source);
            for (Table table : bound) {
                Assertions.assertSame(metadata.latest("lineitem"), table, source);
            }
        }
    }

    @Test
    public void testCtasPlanReadsReloadedTable() throws Exception {
        ReloadingHiveMetadata metadata = new ReloadingHiveMetadata(false);
        register(metadata);
        CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(
                "create table test.ctas_rebind distributed by hash(l_orderkey) buckets 1 " +
                        "properties('replication_num'='1') " +
                        "as select l_orderkey, l_partkey from hive0.tpch.lineitem", connectContext);
        Table analyzedWith = boundSources(ctas.getInsertStmt()).get(0);
        try {
            GlobalStateMgr.getCurrentState().getMetadataMgr().createTable(connectContext, ctas.getCreateTableStmt());
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
            StatementPlanner.plan(ctas.getInsertStmt(), connectContext);
            Table planned = boundSources(ctas.getInsertStmt()).get(0);
            Assertions.assertNotSame(analyzedWith, planned);
            Assertions.assertSame(metadata.latest("lineitem"), planned);
        } finally {
            starRocksAssert.dropTable("ctas_rebind");
        }
    }

    @Test
    public void testSchemaChangeAfterAnalysisFailsStatement() throws Exception {
        // The refresh after analysis finds another schema than the one the statement was analyzed against.
        register(new ReloadingHiveMetadata(true));
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> analyze(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem_" + DEEP_VIEW_LEVELS));
        Assertions.assertTrue(e.getMessage().contains("changed while the statement was being planned"),
                e.getMessage());
    }

    @Test
    public void testNullabilityChangeAfterAnalysisFailsStatement() throws Exception {
        // Same name and type is not enough: the analyzed relation carries the old column's other attributes too.
        register(new ReloadingHiveMetadata(SchemaChange.FLIP_NULLABILITY));
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> analyze(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem_" + DEEP_VIEW_LEVELS));
        Assertions.assertTrue(e.getMessage().contains("changed while the statement was being planned"),
                e.getMessage());
    }

    @Test
    public void testAliasedSourceReloadedByItsOwnName() throws Exception {
        // Only the refresh after analysis reaches a scalar subquery; its relation's resolved name is the alias.
        ReloadingHiveMetadata metadata = new ReloadingHiveMetadata(false);
        register(metadata);
        StatementBase stmt = analyze(
                "insert into t0 (v1, v2) select (select max(l.l_orderkey) from hive0.tpch.lineitem l), 1");
        Assertions.assertEquals(List.of("lineitem"), metadata.refreshed);
        Assertions.assertSame(metadata.latest("lineitem"), boundSources(stmt).get(0));
    }

    @Test
    public void testSchemaChangeBeforeAnalysisIsPickedUp() throws Exception {
        // Refreshed by the pre-pass, the statement is analyzed against the new schema in the first place.
        ReloadingHiveMetadata metadata = new ReloadingHiveMetadata(true);
        register(metadata);
        StatementBase stmt = analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem");
        Assertions.assertSame(metadata.latest("lineitem"), boundSources(stmt).get(0));
    }

    @Test
    public void testRefreshAfterAnalysisRunsBeforeLock() throws Exception {
        // A SELECT that reads no internal table takes the lock only after it is analyzed; the refresh and reload of
        // the sources the pre-pass missed belong before that, not inside the critical section.
        AtomicBoolean locked = new AtomicBoolean(false);
        List<Boolean> lockedAtRefresh = new CopyOnWriteArrayList<>();
        register(new ReloadingHiveMetadata(false) {
            @Override
            public synchronized void invalidateTableForRead(String srDbName, Table table) {
                lockedAtRefresh.add(locked.get());
                super.invalidateTableForRead(srDbName, table);
            }
        });
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem_" + DEEP_VIEW_LEVELS,
                connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
            @Override
            public void lock() {
                locked.set(true);
            }

            @Override
            public void unlock() {
            }
        };
        StatementPlanner.analyzeStatement(stmt, connectContext, locker);
        Assertions.assertEquals(List.of(false), lockedAtRefresh);
        Assertions.assertTrue(locked.get());
    }

    @Test
    public void testNoRefreshWhenDisabled() throws Exception {
        RecordingHiveMetadata metadata = new RecordingHiveMetadata(false);
        register(metadata);
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false);
        try {
            analyze("insert into t0 (v1, v2) select l_orderkey, l_partkey from test.v_lineitem_" + DEEP_VIEW_LEVELS);
        } finally {
            connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(true);
        }
        Assertions.assertTrue(metadata.refreshed.isEmpty());
    }
}
