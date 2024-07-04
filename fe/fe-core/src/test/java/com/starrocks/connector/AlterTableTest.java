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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterOpType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateOrReplaceBranchClause;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.SnapshotRef;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlterTableTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
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
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name",
                "db", "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            Database getDB(String dbName) {
                return new Database(1, "db");
            }

            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tblName) {
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(String dbName, String tblName) {
                return true;
            }
        };

        String sql = "alter table iceberg_catalog.db.srTableName create branch test_branch_1";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertEquals(stmt.getOps().size(), 1);
        Assert.assertTrue(stmt.getOps().get(0) instanceof CreateOrReplaceBranchClause);
        CreateOrReplaceBranchClause clause = (CreateOrReplaceBranchClause) stmt.getOps().get(0);
        Assert.assertTrue(clause.isCreate());
        Assert.assertEquals(clause.getBranchName(), "test_branch_1");
        Assert.assertEquals(clause.getOpType(), AlterOpType.ALTER_BRANCH);
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(stmt);
        Assert.assertEquals(mockedNativeTableB.refs().size(), 1);
        Assert.assertTrue(mockedNativeTableB.refs().containsKey("test_branch_1"));
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();

        Long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        sql = String.format("alter table iceberg_catalog.db.srTableName create branch test_branch_2 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);

        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(stmt);
        Assert.assertTrue(mockedNativeTableB.refs().containsKey("test_branch_2"));
        SnapshotRef snapshotRef = mockedNativeTableB.refs().get("test_branch_2");
        Assert.assertEquals(3, mockedNativeTableB.refs().size());
        Assert.assertEquals(2, snapshotRef.snapshotId());
        Assert.assertEquals(2, snapshotRef.minSnapshotsToKeep().intValue());
        Assert.assertEquals(172800000L, snapshotRef.maxSnapshotAgeMs().longValue());
        Assert.assertEquals(604800000L, snapshotRef.maxRefAgeMs().longValue());

        sql = String.format("alter table iceberg_catalog.db.srTableName create or replace branch test_branch_2 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(stmt);
        Assert.assertEquals(3, mockedNativeTableB.refs().size());

        sql = String.format("alter table iceberg_catalog.db.srTableName create or replace branch test_branch_3 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(((CreateOrReplaceBranchClause) stmt.getOps().get(0)).isReplace());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(stmt);
        Assert.assertEquals(4, mockedNativeTableB.refs().size());

        sql = String.format("alter table iceberg_catalog.db.srTableName create branch if not exists test_branch_3 " +
                "as of version %s " +
                "retain 7 days " +
                "with snapshot retention 2 " +
                "snapshots 2 days", snapshotId);
        stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(((CreateOrReplaceBranchClause) stmt.getOps().get(0)).isIfNotExists());
        connectContext.getGlobalStateMgr().getMetadataMgr().alterTable(stmt);
        Assert.assertEquals(4, mockedNativeTableB.refs().size());
    }
}
