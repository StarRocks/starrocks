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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ColocateTableTest.java

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

import com.google.common.collect.Multimap;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ColocateTableTest {
    private static ConnectContext connectContext;
    private static String dbName = "testDb";
    private static String fullDbName = dbName;
    private static String tableName1 = "t1";
    private static String tableName2 = "t2";
    private static String groupName = "group1";
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Before
    public void createDb() throws Exception {
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        GlobalStateMgr.getCurrentState().setColocateTableIndex(new ColocateTableIndex());
    }

    @After
    public void dropDb() throws Exception {
        String dropDbStmtStr = "drop database " + dbName;
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(dropDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
    }

    private static void createTable(String sql) throws Exception {
        starRocksAssert.withTable(sql);
    }

    @Test
    public void testCreateOneTable() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        ColocateTableIndex index = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(fullDbName);
        long tableId = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName1).getId();

        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(tableId));

        Long dbId = db.getId();
        Assert.assertEquals(dbId, index.getGroup(tableId).dbId);

        GroupId groupId = index.getGroup(tableId);
        List<Long> backendIds = index.getBackendsPerBucketSeq(groupId).get(0);
        System.out.println(backendIds);
        Assert.assertEquals(Collections.singletonList(10001L), backendIds);

        String fullGroupName = dbId + "_" + groupName;
        Assert.assertEquals(tableId, index.getTableIdByGroup(fullGroupName));
        ColocateGroupSchema groupSchema = index.getGroupSchema(fullGroupName);
        Assert.assertNotNull(groupSchema);
        Assert.assertEquals(dbId, groupSchema.getGroupId().dbId);
        Assert.assertEquals(1, groupSchema.getBucketsNum());
        Assert.assertEquals(1, groupSchema.getReplicationNum());
    }

    @Test
    public void testCreateTwoTableWithSameGroup() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        ColocateTableIndex index = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(fullDbName);
        long firstTblId = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName1).getId();
        long secondTblId = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName2).getId();

        Assert.assertEquals(2, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(2, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));

        Assert.assertTrue(index.isSameGroup(firstTblId, secondTblId));

        // drop first
        index.removeTable(firstTblId, null, false);
        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));
        Assert.assertFalse(index.isSameGroup(firstTblId, secondTblId));

        // drop second
        index.removeTable(secondTblId, null, false);
        Assert.assertEquals(0, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(0, index.getAllGroupIds().size());
        Assert.assertEquals(0, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(0,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertFalse(index.isColocateTable(secondTblId));
    }

    @Test
    public void testBucketNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same bucket num: 1");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }

    @Test
    public void testReplicationNum() throws Exception {
        
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same replication num: 1");

        new MockUp<SystemInfoService>() {
            @Mock
            public List<Long> getAvailableBackendIds() {
                return Arrays.asList(10001L, 10002L, 10003L);       
            }
        };

        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"2\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }

    @Test
    public void testDistributionColumnsSize() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns size must be the same : 2");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }

    @Test
    public void testDistributionColumnsType() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns must have the same data type");
        expectedEx.expectMessage("current col: k2, should be: INT");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }
}
