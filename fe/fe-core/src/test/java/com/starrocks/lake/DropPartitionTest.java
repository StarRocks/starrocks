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

package com.starrocks.lake;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DropPartitionTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database test;";
        createDb(createDbStmtStr);

        String createLakeTableStr = "create table test.lake_table(k1 date, k2 int, k3 smallint, v1 varchar(2048), "
                + "v2 datetime default '2014-02-04 15:36:00')"
                + " duplicate key(k1, k2, k3)"
                + " PARTITION BY RANGE(k1, k2, k3)"
                + " (PARTITION p1 VALUES [(\"2014-01-01\", \"10\", \"200\"), (\"2014-01-01\", \"20\", \"300\")),"
                + " PARTITION p2 VALUES [(\"2014-06-01\", \"100\", \"200\"), (\"2014-07-01\", \"100\", \"300\")))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES ( \"datacache.enable\" = \"true\")";
        createTable(createLakeTableStr);
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    private static void dropPartition(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
    }

    @Test
    public void testNormalDropLakePartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("lake_table");
        Partition partition = table.getPartition("p1");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.lake_table drop partition p1;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p1");
        Assert.assertNull(partition);
        String recoverPartitionSql = "recover partition p1 from test.lake_table";
        RecoverPartitionStmt recoverPartitionStmt =
                (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverPartitionSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverPartitionStmt);
        partition = table.getPartition("p1");
        Assert.assertNotNull(partition);
        Assert.assertEquals("p1", partition.getName());
    }

    @Test
    public void testForceDropLakePartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("lake_table");
        Partition partition = table.getPartition("p1");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.lake_table drop partition p1 force;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p1");
        Assert.assertTrue(replicaList.isEmpty());
        Assert.assertNull(partition);
        String recoverPartitionSql = "recover partition p1 from test.lake_table";
        RecoverPartitionStmt recoverPartitionStmt =
                (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverPartitionSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "No partition named p1 in table lake_table",
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverPartitionStmt));
    }
}
