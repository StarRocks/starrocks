// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ColocateTableIndexTest.java

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

<<<<<<< HEAD
import com.google.common.collect.Maps;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
=======
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
>>>>>>> c4ffab4d7 ([BugFix] Remove colocated index from memory (#9578))

public class ColocateTableIndexTest {

    @Test
<<<<<<< HEAD
    public void testGroupId() {
        GroupId groupId1 = new GroupId(1000, 2000);
        GroupId groupId2 = new GroupId(1000, 2000);
        Map<GroupId, Long> map = Maps.newHashMap();
        Assert.assertTrue(groupId1.equals(groupId2));
        Assert.assertTrue(groupId1.hashCode() == groupId2.hashCode());
        map.put(groupId1, 1000L);
        Assert.assertTrue(map.containsKey(groupId2));

        Set<GroupId> balancingGroups = new CopyOnWriteArraySet<GroupId>();
        balancingGroups.add(groupId1);
        Assert.assertTrue(balancingGroups.size() == 1);
        balancingGroups.remove(groupId2);
        Assert.assertTrue(balancingGroups.isEmpty());
    }
=======
    public void testDropTable() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());

        // create table1_1->group1
        String sql = "CREATE TABLE db1.table1_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        List<List<String>> infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        // group1->table1_1
        Assert.assertEquals(1, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Table table1_1 = GlobalStateMgr.getCurrentState().getDb("default_cluster:db1").getTable("table1_1");
        Assert.assertEquals(String.format("%d", table1_1.getId()), infos.get(0).get(2));
        LOG.info("after create db1.table1_1: {}", infos);

        // create table1_2->group1
        sql = "CREATE TABLE db1.table1_2 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        // group1 -> table1_1, table1_2
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(1, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Table table1_2 = GlobalStateMgr.getCurrentState().getDb("default_cluster:db1").getTable("table1_2");
        Assert.assertEquals(String.format("%d, %d", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        LOG.info("after create db1.table1_2: {}", infos);

        // create db2
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group2\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        // group1 -> table1_1, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d, %d", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Table table2_1 = GlobalStateMgr.getCurrentState().getDb("default_cluster:db2").getTable("table2_1");
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after create db2.table2_1: {}", infos);

        // drop db1.table1_1
        sql = "DROP TABLE db1.table1_1;";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d*, %d", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after drop db1.table1_1: {}", infos);

        // drop db1.table1_2
        sql = "DROP TABLE db1.table1_2;";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d*, %d*", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after drop db1.table1_2: {}", infos);

        // drop db2
        sql = "DROP DATABASE db2;";
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l*
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d*, %d*", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d*", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after drop db2: {}", infos);
    }

    @Test
    public void testCleanUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        int n = colocateTableIndex.getAllGroupIds().size();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database goodDb;", connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        Database goodDb = GlobalStateMgr.getCurrentState().getDb("goodDb");
        // create goodtable
        String sql = "CREATE TABLE " +
                "goodDb.goodTable (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        OlapTable table = (OlapTable)goodDb.getTable("goodTable");
        ColocateTableIndex.GroupId goodGroup = GlobalStateMgr.getCurrentColocateIndex().getGroup(table.getId());
        Assert.assertEquals(n + 1, colocateTableIndex.getAllGroupIds().size());

        // create a bad db
        long badDbId = 100;
        table.id = 101;
        table.name = "goodTableOfBadDb";
        colocateTableIndex.addTableToGroup(
                badDbId, table, "badGroupOfBadDb", new ColocateTableIndex.GroupId(badDbId, 102));
        // create a bad table in good db
        table.id = 200;
        table.name = "badTable";
        colocateTableIndex.addTableToGroup(
                goodDb.getId(), table, "badGroupOfBadTable", new ColocateTableIndex.GroupId(goodDb.getId(), 201));

        Assert.assertEquals(n + 3, colocateTableIndex.getAllGroupIds().size());

        colocateTableIndex.cleanupInvalidDbOrTable(GlobalStateMgr.getCurrentState());

        Assert.assertEquals(n + 1, colocateTableIndex.getAllGroupIds().size());
    }

>>>>>>> c4ffab4d7 ([BugFix] Remove colocated index from memory (#9578))
}
