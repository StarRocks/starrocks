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
}
