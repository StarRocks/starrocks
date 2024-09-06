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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/DropPartitionTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DropPartitionTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        createDb(createDbStmtStr);
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
    }
    @Before
    public void createTable() throws Exception {
        String createTableStr = "create table test.tbl1(d1 date, k1 int, k2 bigint) duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1) (PARTITION p20210201 VALUES [('2021-02-01'), ('2021-02-02')),"
                + "PARTITION p20210202 VALUES [('2021-02-02'), ('2021-02-03')),"
                + "PARTITION p20210203 VALUES [('2021-02-03'), ('2021-02-04'))) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableStr, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }
    @After
    public void dropTable() throws Exception {
        String dropTableStr = "drop table if exists test.tbl1 force";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropTableStr, connectContext);
        StarRocksAssert.utDropTableWithRetry(dropTableStmt);
    }

    private void dropPartition(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        DDLStmtExecutor.execute(alterTableStmt, connectContext);
    }

    @Test
    public void testNormalDropPartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");
        Partition partition = table.getPartition("p20210201");
        long tabletId = partition.getDefaultPhysicalPartition().getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.tbl1 drop partition p20210201;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210201");
        Assert.assertEquals(1, replicaList.size());
        Assert.assertNull(partition);
        String recoverPartitionSql = "recover partition p20210201 from test.tbl1";
        RecoverPartitionStmt recoverPartitionStmt =
                (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverPartitionSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverPartitionStmt);
        partition = table.getPartition("p20210201");
        Assert.assertNotNull(partition);
        Assert.assertEquals("p20210201", partition.getName());
    }

    @Test
    public void testForceDropPartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");
        Partition partition = table.getPartition("p20210202");
        long tabletId = partition.getDefaultPhysicalPartition().getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.tbl1 drop partition p20210202 force;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList;
        replicaList = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210202");
        Assert.assertFalse(replicaList.isEmpty());
        Assert.assertNull(partition);
        String recoverPartitionSql = "recover partition p20210202 from test.tbl1";
        RecoverPartitionStmt recoverPartitionStmt =
                (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverPartitionSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "No partition named 'p20210202' in recycle bin that belongs to table 'tbl1'",
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverPartitionStmt));

        GlobalStateMgr.getCurrentState().getRecycleBin().erasePartition(System.currentTimeMillis());
        replicaList = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertTrue(replicaList.isEmpty());
    }

    @Test
    public void testDropPartitionAndReserveTablets() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");
        Partition partition = table.getPartition("p20210203");
        long tabletId = partition.getDefaultPhysicalPartition().getBaseIndex().getTablets().get(0).getId();
        table.dropPartitionAndReserveTablet("p20210203");
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210203");
        Assert.assertEquals(1, replicaList.size());
        Assert.assertNull(partition);
    }

    @Test
    public void testNormalDropMultiRangePartition() throws Exception {
        String dbName = "test";
        String tableName = "tbl1";
        String dropPartitionSql = "alter table %s.%s DROP PARTITIONS " +
                "IF EXISTS START(\"2021-02-01\") END(\"2021-02-03\") EVERY (INTERVAL 1 DAY);";
        checkNormalDropPartitions(dbName, tableName, dropPartitionSql);
    }

    @Test
    public void testForceDropMultiRangePartition() throws Exception {
        String dropPartitionSql = "alter table test.tbl1 DROP PARTITIONS " +
                "IF EXISTS START(\"2021-02-01\") END(\"2021-02-03\") EVERY (INTERVAL 1 DAY) force;";
        checkForceDropPartitions(dropPartitionSql);
    }

    @Test
    public void testNormalDropIdentifierListPartition() throws Exception {
        String dbName = "test";
        String tableName = "tbl1";
        String dropPartitionSql = "alter table %s.%s DROP PARTITIONS " +
                "IF EXISTS (p20210201,p20210202) ";
        checkNormalDropPartitions(dbName, tableName, dropPartitionSql);
    }

    @Test
    public void testForceDropIdentifierListPartition() throws Exception {
        String dropPartitionSql = "alter table test.tbl1 DROP PARTITIONS " +
                "IF EXISTS (p20210201,p20210202) force;";
        checkForceDropPartitions(dropPartitionSql);
    }

    private void checkNormalDropPartitions(String dbName, String tableName, String dropPartitionSql) throws Exception {
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p20210201");
        partitionNames.add("p20210202");
        List<Long> tabletIds = new ArrayList<>();
        for (String partitionName : partitionNames) {
            Table table = getTable(dbName, tableName);
            Partition partition = table.getPartition(partitionName);
            long tabletId = partition.getDefaultPhysicalPartition().getBaseIndex().getTablets().get(0).getId();
            tabletIds.add(tabletId);
        }
        dropPartition(String.format(dropPartitionSql, dbName, tableName));
        for (int i = 0; i < partitionNames.size(); i++) {
            String partitionName = partitionNames.get(i);
            long tabletId = tabletIds.get(i);
            checkBeforeDrop(dbName, tableName, partitionName, tabletId);
            checkAfterRecover(dbName, tableName, partitionName);
        }
    }

    private void checkForceDropPartitions(String dropPartitionSql) throws Exception {
        List<String> partitionNames = new ArrayList<>();
        List<Long> tabletIds = new ArrayList<>();
        String dbName = "test";
        String tableName = "tbl1";
        partitionNames.add("p20210201");
        partitionNames.add("p20210202");
        for (String partitionName : partitionNames) {
            Table table = getTable(dbName, tableName);
            Partition partition = table.getPartition(partitionName);
            long tabletId = partition.getDefaultPhysicalPartition().getBaseIndex().getTablets().get(0).getId();
            tabletIds.add(tabletId);
        }
        dropPartition(dropPartitionSql);
        Table table = getTable(dbName, tableName);
        for (int i = 0; i < partitionNames.size(); i++) {
            String partitionName = partitionNames.get(i);
            long tabletId = tabletIds.get(i);
            List<Replica> replicaList =
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
            Partition partition = table.getPartition(partitionName);
            Assert.assertFalse(replicaList.isEmpty());
            Assert.assertNull(partition);
            String recoverPartitionSql = "recover partition %s from test.tbl1";
            RecoverPartitionStmt recoverPartitionStmt =
                    (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(String.format(recoverPartitionSql,
                            partitionName), connectContext);
            ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                    String.format("No partition named '%s' in recycle bin that belongs to table '%s'", partitionName, "tbl1"),
                    () -> GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverPartitionStmt));
        }
        //该方法会立马删除partition
        GlobalStateMgr.getCurrentState().getRecycleBin().erasePartition(System.currentTimeMillis());
        for (int i = 0; i < partitionNames.size(); i++) {
            long tabletId = tabletIds.get(i);
            List<Replica> replicaList = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
            Assert.assertTrue(replicaList.isEmpty());
        }
    }

    private void checkBeforeDrop(String dbName, String tableName, String partitionName, long tabletId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Partition partition = table.getPartition(partitionName);
        Assert.assertEquals(1, replicaList.size());
        Assert.assertNull(partition);
    }

    private void checkAfterRecover(String dbName, String tableName, String partitionName) throws Exception {
        Partition partition = recoverPartition(dbName, tableName, partitionName);
        Assert.assertNotNull(partition);
        Assert.assertEquals(partitionName, partition.getName());
    }

    private Partition recoverPartition(String db, String table, String partitionName) throws Exception {
        String recoverPartitionSql = String.format("recover partition %s from %s.%s", partitionName, db, table);
        RecoverPartitionStmt recoverPartitionStmt =
                (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverPartitionSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(recoverPartitionStmt);
        return getTable(db, table).getPartition(partitionName);
    }

    private Table getTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
    }
}
