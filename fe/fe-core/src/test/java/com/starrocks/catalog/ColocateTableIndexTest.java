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

import com.google.common.collect.Maps;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class ColocateTableIndexTest {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndexTest.class);

    @Test
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

    @Test
    public void testDropTable() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().createDb(createDbStmt);

        // create table1_1->group1
        String sql = "CREATE TABLE db1.table1_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
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
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
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
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().createDb(createDbStmt);
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group2\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
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
        // must use parseStmtWithNewParser() instead of parseAndAnalyzeStmt(), otherwise will cause exception
        // don't know why...
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
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        GlobalStateMgr.getCurrentState().dropDb(dropDbStmt);
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
}
