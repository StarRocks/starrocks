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

package com.starrocks.connector;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateOrReplaceBranchClause;
import com.starrocks.sql.ast.CreateOrReplaceTagClause;
import com.starrocks.sql.ast.ReplacePartitionColumnClause;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.SnapshotRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AlterTableTest extends TableTestBase {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);

    }

    @Test
    public void testCreateBranch() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        String sql = "alter table iceberg_catalog.db.srTableName create branch test_branch_1";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assertions.assertEquals(stmt.getAlterClauseList().size(), 1);
        Assertions.assertTrue(stmt.getAlterClauseList().get(0) instanceof CreateOrReplaceBranchClause);
        CreateOrReplaceBranchClause clause = (CreateOrReplaceBranchClause) stmt.getAlterClauseList().get(0);
        Assertions.assertTrue(clause.isCreate());
        Assertions.assertEquals(clause.getBranchName(), "test_branch_1");
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 1);
        Assertions.assertTrue(mockedNativeTableB.refs().containsKey("test_branch_1"));
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();

        Long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        sql = String.format("alter table iceberg_catalog.db.srTableName create branch test_branch_2 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);

        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertTrue(mockedNativeTableB.refs().containsKey("test_branch_2"));
        SnapshotRef snapshotRef = mockedNativeTableB.refs().get("test_branch_2");
        Assertions.assertEquals(3, mockedNativeTableB.refs().size());
        Assertions.assertEquals(snapshotId.longValue(), snapshotRef.snapshotId());
        Assertions.assertEquals(2, snapshotRef.minSnapshotsToKeep().intValue());
        Assertions.assertEquals(172800000L, snapshotRef.maxSnapshotAgeMs().longValue());
        Assertions.assertEquals(604800000L, snapshotRef.maxRefAgeMs().longValue());

        sql = String.format("alter table iceberg_catalog.db.srTableName create or replace branch test_branch_2 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        Assertions.assertEquals(3, mockedNativeTableB.refs().size());

        sql = String.format("alter table iceberg_catalog.db.srTableName create or replace branch test_branch_3 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assertions.assertTrue(((CreateOrReplaceBranchClause) stmt.getAlterClauseList().get(0)).isReplace());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(4, mockedNativeTableB.refs().size());

        sql = String.format("alter table iceberg_catalog.db.srTableName create branch if not exists test_branch_3 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assertions.assertTrue(((CreateOrReplaceBranchClause) stmt.getAlterClauseList().get(0)).isIfNotExists());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        Assertions.assertEquals(4, mockedNativeTableB.refs().size());
    }

    @Test
    public void testCreateTag() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        String sql = "alter table iceberg_catalog.db.srTableName create tag test_tag_1";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assertions.assertEquals(stmt.getAlterClauseList().size(), 1);
        Assertions.assertTrue(stmt.getAlterClauseList().get(0) instanceof CreateOrReplaceTagClause);
        CreateOrReplaceTagClause clause = (CreateOrReplaceTagClause) stmt.getAlterClauseList().get(0);
        Assertions.assertTrue(clause.isCreate());
        Assertions.assertEquals(clause.getTagName(), "test_tag_1");
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 2);
        Assertions.assertTrue(mockedNativeTableB.refs().containsKey("test_tag_1"));

        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        Long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        sql = String.format("alter table iceberg_catalog.db.srTableName create tag test_tag_2 " +
                "as of version %s retain 7 days ", snapshotId);

        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertTrue(mockedNativeTableB.refs().containsKey("test_tag_2"));
        SnapshotRef snapshotRef = mockedNativeTableB.refs().get("test_tag_2");
        Assertions.assertEquals(3, mockedNativeTableB.refs().size());
        Assertions.assertEquals(2, snapshotRef.snapshotId());
        Assertions.assertEquals(604800000L, snapshotRef.maxRefAgeMs().longValue());

        sql = "alter table iceberg_catalog.db.srTableName create or replace tag test_tag_3 ";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(4, mockedNativeTableB.refs().size());

        sql = "alter table iceberg_catalog.db.srTableName create or replace tag test_tag_3 ";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assertions.assertTrue(((CreateOrReplaceTagClause) stmt.getAlterClauseList().get(0)).isReplace());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(4, mockedNativeTableB.refs().size());

        sql = "alter table iceberg_catalog.db.srTableName create tag if not exists test_tag_3 ";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assertions.assertTrue(((CreateOrReplaceTagClause) stmt.getAlterClauseList().get(0)).isIfNotExists());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(4, mockedNativeTableB.refs().size());
    }

    @Test
    public void testDropBranch() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        String sql = "alter table iceberg_catalog.db.srTableName create branch test_branch_1";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 2);
        Assertions.assertTrue(mockedNativeTableB.refs().containsKey("test_branch_1"));

        sql = "alter table iceberg_catalog.db.srTableName drop branch test_branch_1";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 1);
        Assertions.assertFalse(mockedNativeTableB.refs().containsKey("test_branch_1"));

        sql = "alter table iceberg_catalog.db.srTableName drop branch if exists test_branch_1";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 1);
        Assertions.assertFalse(mockedNativeTableB.refs().containsKey("test_branch_1"));
    }

    @Test
    public void testDropTag() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        String sql = "alter table iceberg_catalog.db.srTableName create tag test_tag";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 2);
        Assertions.assertTrue(mockedNativeTableB.refs().containsKey("test_tag"));

        sql = "alter table iceberg_catalog.db.srTableName drop tag test_tag";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 1);
        Assertions.assertFalse(mockedNativeTableB.refs().containsKey("test_branch_1"));

        sql = "alter table iceberg_catalog.db.srTableName drop tag if exists test_tag";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableB.refresh();
        Assertions.assertEquals(mockedNativeTableB.refs().size(), 1);
        Assertions.assertFalse(mockedNativeTableB.refs().containsKey("test_tag"));
    }

    @Test
    public void testReplacePartitionColumn() throws Exception {
        String sql = "alter table iceberg_catalog.db.srTableName replace partition column day(dt) with month(dt)";
        AlterTableStmt stmt =
                (AlterTableStmt) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, starRocksAssert.getCtx());
        Assertions.assertEquals(1, stmt.getAlterClauseList().size());
        Assertions.assertTrue(stmt.getAlterClauseList().get(0) instanceof ReplacePartitionColumnClause);
        ReplacePartitionColumnClause clause = (ReplacePartitionColumnClause) stmt.getAlterClauseList().get(0);
        Assertions.assertTrue(clause.getOldPartitionExpr() instanceof FunctionCallExpr);
        Assertions.assertTrue(clause.getNewPartitionExpr() instanceof FunctionCallExpr);
        Assertions.assertEquals("day", ((FunctionCallExpr) clause.getOldPartitionExpr()).getFunctionName());
        Assertions.assertEquals("month", ((FunctionCallExpr) clause.getNewPartitionExpr()).getFunctionName());
    }

    @Test
    public void testReplacePartitionColumnByFieldName() throws Exception {
        String sql = "alter table iceberg_catalog.db.srTableName replace partition column dt_day with month(dt)";
        AlterTableStmt stmt =
                (AlterTableStmt) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, starRocksAssert.getCtx());
        Assertions.assertEquals(1, stmt.getAlterClauseList().size());
        Assertions.assertTrue(stmt.getAlterClauseList().get(0) instanceof ReplacePartitionColumnClause);
        ReplacePartitionColumnClause clause = (ReplacePartitionColumnClause) stmt.getAlterClauseList().get(0);
        // field name "dt_day" is parsed as a SlotRef
        Assertions.assertTrue(clause.getOldPartitionExpr() instanceof SlotRef);
        Assertions.assertEquals("dt_day", ((SlotRef) clause.getOldPartitionExpr()).getColumnName());
        Assertions.assertTrue(clause.getNewPartitionExpr() instanceof FunctionCallExpr);
        Assertions.assertEquals("month", ((FunctionCallExpr) clause.getNewPartitionExpr()).getFunctionName());
    }

    @Test
    public void testAlterView() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        new MockUp<Table>() {
            @Mock
            boolean isConnectorView() {
                return true;
            }
        };

        String sql = "alter view iceberg_catalog.db.srTableName as select * from db.a;";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Unknown database 'db'",
                () -> UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()));

    }

    @Test
    public void testReplacePartitionColumnAnalyzerFullPath() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableFV2;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        // Use a unique table name to avoid metadata cache conflicts with other tests
        String tbl = "iceberg_catalog.db.partTestTable";

        // --- Error cases (no mutation, test these first) ---

        // Same old and new should be rejected
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " replace partition column day(dt) with day(dt)",
                        starRocksAssert.getCtx()));

        // Non-existent old partition column (table has day(dt) not month(dt))
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " replace partition column month(dt) with year(dt)",
                        starRocksAssert.getCtx()));

        // Non-existent field name should be rejected
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " replace partition column no_such_field with month(dt)",
                        starRocksAssert.getCtx()));

        // Column "nonexistent" doesn't exist in table schema
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " replace partition column day(dt) with day(nonexistent)",
                        starRocksAssert.getCtx()));

        // Drop non-existent partition column should fail (table has day(dt), not month(dt))
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " drop partition column month(dt)",
                        starRocksAssert.getCtx()));

        // Add partition column that already exists should fail (table already has day(dt))
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " add partition column day(dt)",
                        starRocksAssert.getCtx()));

        // --- Success cases (mutate table) ---

        // 1. Successful replace day(dt) with month(dt) through full analyzer + executor path
        String sql = "alter table " + tbl + " replace partition column day(dt) with month(dt)";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableFV2.refresh();

        // 2. Now table has month(dt). Replace by field name "dt_month" with year(dt)
        sql = "alter table " + tbl + " replace partition column dt_month with year(dt)";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableFV2.refresh();

        // 3. Now add month(dt) back so table has both year(dt) and month(dt)
        sql = "alter table " + tbl + " add partition column month(dt)";
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(connectContext, stmt);
        mockedNativeTableFV2.refresh();

        // 4. Replace year(dt) with month(dt) should fail because month(dt) already exists
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "alter table " + tbl + " replace partition column year(dt) with month(dt)",
                        starRocksAssert.getCtx()));
    }
}
