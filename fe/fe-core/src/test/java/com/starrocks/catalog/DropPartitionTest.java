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

import com.google.common.collect.Lists;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
        String createTableStr = "create table test.tbl1(d1 date, k1 int, k2 bigint) duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1) (PARTITION p20210201 VALUES [('2021-02-01'), ('2021-02-02')),"
                + "PARTITION p20210202 VALUES [('2021-02-02'), ('2021-02-03')),"
                + "PARTITION p20210203 VALUES [('2021-02-03'), ('2021-02-04'))) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1');";
        createDb(createDbStmtStr);
        createTable(createTableStr);

        StarOSAgent agent = new StarOSAgent();

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/1/");
        FilePathInfo pathInfo = builder.build();

        new Expectations(agent) {
            {
                agent.allocateFilePath(anyLong);
                result = pathInfo;
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, pathInfo, (FileCacheInfo) any, anyLong);
                returns(Lists.newArrayList(10001L, 10002L, 10003L),
                        Lists.newArrayList(10004L, 10005L, 10006L),
                        Lists.newArrayList(10007L, 10008L, 10009L));
                agent.getPrimaryBackendIdByShard(anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        String createLakeTableStr = "create table test.lake_table(k1 date, k2 int, k3 smallint, v1 varchar(2048), "
                + "v2 datetime default '2014-02-04 15:36:00')"
                + " duplicate key(k1, k2, k3)"
                + " PARTITION BY RANGE(k1, k2, k3)"
                + " (PARTITION p1 VALUES [(\"2014-01-01\", \"10\", \"200\"), (\"2014-01-01\", \"20\", \"300\")),"
                + " PARTITION p2 VALUES [(\"2014-06-01\", \"100\", \"200\"), (\"2014-07-01\", \"100\", \"300\")))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES ( \"enable_storage_cache\" = \"true\", \"storage_cache_ttl\" = \"3600\")";
        createTable(createLakeTableStr);
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    private static void dropPartition(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
    }

    @Test
    public void testNormalDropPartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("tbl1");
        Partition partition = table.getPartition("p20210201");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
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
        GlobalStateMgr.getCurrentState().recoverPartition(recoverPartitionStmt);
        partition = table.getPartition("p20210201");
        Assert.assertNotNull(partition);
        Assert.assertEquals("p20210201", partition.getName());
    }

    @Test
    public void testForceDropPartition() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("tbl1");
        Partition partition = table.getPartition("p20210202");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.tbl1 drop partition p20210202 force;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210202");
        Assert.assertTrue(replicaList.isEmpty());
        Assert.assertNull(partition);
        String recoverPartitionSql = "recover partition p20210202 from test.tbl1";
        RecoverPartitionStmt recoverPartitionStmt =
                (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(recoverPartitionSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "No partition named p20210202 in table tbl1",
                () -> GlobalStateMgr.getCurrentState().recoverPartition(recoverPartitionStmt));
    }

    @Test
    public void testDropPartitionAndReserveTablets() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("tbl1");
        Partition partition = table.getPartition("p20210203");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        table.dropPartitionAndReserveTablet("p20210203");
        List<Replica> replicaList =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210203");
        Assert.assertEquals(1, replicaList.size());
        Assert.assertNull(partition);
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
        GlobalStateMgr.getCurrentState().recoverPartition(recoverPartitionStmt);
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
                () -> GlobalStateMgr.getCurrentState().recoverPartition(recoverPartitionStmt));
    }
}
