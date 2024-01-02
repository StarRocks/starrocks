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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/CreateTableTest.java

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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class StorageMediumInferTest {
    private static ConnectContext connectContext;
    private static Backend be1;
    private static Backend be2;

    @BeforeClass
    public static void init() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        be1 = GlobalStateMgr.getCurrentSystemInfo().getBackend(10001);
        be1.getDisks().get("10001/path1").setPathHash(10001);
        be2 = UtFrameUtils.addMockBackend(10002);
        be2.getDisks().get("10002/path1").setPathHash(10002);
        Config.enable_strict_storage_medium_check = false;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database if not exists test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    private static void alterTableWithNewParser(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
    }

    @Test
    public void testCreateTable() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        be1.setStorageMediumForAllDisks(TStorageMedium.HDD);
        be2.setStorageMediumForAllDisks(TStorageMedium.HDD);
        createTable("create table test.tbl1(key1 int, key2 varchar(10)) \n" +
                "distributed by hash(key1) buckets 10 properties('replication_num' = '1');");
        OlapTable tbl1 = (OlapTable) db.getTable("tbl1");
        List<Partition> partitionList1 = Lists.newArrayList(tbl1.getPartitions());
        DataProperty dataProperty1 =
                globalStateMgr.getDataPropertyIncludeRecycleBin(tbl1.getPartitionInfo(),
                        partitionList1.get(0).getId());
        Assert.assertEquals(TStorageMedium.HDD, dataProperty1.getStorageMedium());

        be1.setStorageMediumForAllDisks(TStorageMedium.SSD);
        be2.setStorageMediumForAllDisks(TStorageMedium.SSD);
        String sql = "create table test.tbl2\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        createTable(sql);
        OlapTable tbl2 = (OlapTable) db.getTable("tbl2");
        List<Partition> partitionList2 = Lists.newArrayList(tbl2.getPartitions());
        DataProperty dataProperty2 =
                globalStateMgr.getDataPropertyIncludeRecycleBin(tbl2.getPartitionInfo(),
                        partitionList2.get(0).getId());
        Assert.assertEquals(TStorageMedium.SSD, dataProperty2.getStorageMedium());

        be1.setStorageMediumForAllDisks(TStorageMedium.SSD);
        be2.setStorageMediumForAllDisks(TStorageMedium.HDD);
        Config.tablet_sched_storage_cooldown_second = 123123213L;
        createTable("create table test.tbl3(key1 int, key2 varchar(10)) \n" +
                "distributed by hash(key1) buckets 10 properties('replication_num' = '1');");
        OlapTable tbl3 = (OlapTable) db.getTable("tbl3");
        List<Partition> partitionList3 = Lists.newArrayList(tbl3.getPartitions());
        DataProperty dataProperty3 =
                globalStateMgr.getDataPropertyIncludeRecycleBin(tbl3.getPartitionInfo(),
                        partitionList3.get(0).getId());
        Assert.assertEquals(TStorageMedium.SSD, dataProperty3.getStorageMedium());

        Config.tablet_sched_storage_cooldown_second = -1L; // default value, no storage cool down
        createTable("create table test.tbl4(key1 int, key2 varchar(10)) \n" +
                "distributed by hash(key1) buckets 10 properties('replication_num' = '1');");
        OlapTable tbl4 = (OlapTable) db.getTable("tbl4");
        List<Partition> partitionList4 = Lists.newArrayList(tbl4.getPartitions());
        DataProperty dataProperty4 =
                globalStateMgr.getDataPropertyIncludeRecycleBin(tbl4.getPartitionInfo(),
                        partitionList4.get(0).getId());
        Assert.assertEquals(TStorageMedium.HDD, dataProperty4.getStorageMedium());
    }

    @Test
    public void testAlterTableAddPartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        be1.setStorageMediumForAllDisks(TStorageMedium.SSD);
        be2.setStorageMediumForAllDisks(TStorageMedium.SSD);
        String sql = "create table test.tblp2\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        createTable(sql);
        alterTableWithNewParser("ALTER TABLE test.tblp2 ADD PARTITION IF NOT EXISTS p2 VALUES LESS THAN (\"20\")");
        OlapTable tbl2 = (OlapTable) db.getTable("tblp2");
        List<Partition> partitionList2 = Lists.newArrayList(tbl2.getPartitions());
        Assert.assertEquals(2, partitionList2.size());
        for (Partition partition : partitionList2) {
            DataProperty dataProperty2 =
                    globalStateMgr.getDataPropertyIncludeRecycleBin(tbl2.getPartitionInfo(),
                            partition.getId());
            Assert.assertEquals(TStorageMedium.SSD, dataProperty2.getStorageMedium());
        }
    }
}