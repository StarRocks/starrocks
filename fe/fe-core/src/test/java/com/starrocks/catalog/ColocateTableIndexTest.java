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


package com.starrocks.catalog;

import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.collections.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ColocateTableIndexTest {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndexTest.class);

    @Before
    public void setUp() {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
    }

    @After
    public void teardown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * [
     *   [10002.10006, 10002_group1, 10004, 10016, 4, 1, int(11), true],
     *   [10026.10030, 10026_group2, 10028, 4, 1, int(11), true]
     *  ]
     * ->
     * {
     *   'group1': [10002.10006, 10002_group1, 10004, 10016, 4, 1, int(11), true],
     *   'group2': [10026.10030, 10026_group2, 10028, 4, 1, int(11), true]
     * }
     */
    private Map<String, List<String>> groupByName(List<List<String>> lists) {
        Map<String, List<String>> ret = new HashedMap();
        for (List<String> list : lists) {
            ret.put(list.get(1).split("_")[1], list);
        }
        return ret;
    }

    @Test
    public void testDropTable() throws Exception {
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
        // group1->table1To1
        Assert.assertEquals(1, infos.size());
        Map<String, List<String>> map = groupByName(infos);
        Table table1To1 = GlobalStateMgr.getCurrentState().getDb("db1").getTable("table1_1");
        Assert.assertEquals(String.format("%d", table1To1.getId()), map.get("group1").get(2));
        LOG.info("after create db1.table1_1: {}", infos);

        // create table1_2->group1
        sql = "CREATE TABLE db1.table1_2 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        // group1 -> table1To1, table1To2
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(1, infos.size());
        map = groupByName(infos);
        Table table1To2 = GlobalStateMgr.getCurrentState().getDb("db1").getTable("table1_2");
        Assert.assertEquals(String.format("%d, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
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
        map = groupByName(infos);
        Assert.assertEquals(String.format("%d, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Table table2To1 = GlobalStateMgr.getCurrentState().getDb("db2").getTable("table2_1");
        Assert.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after create db2.table2_1: {}", infos);

        // drop db1.table1_1
        sql = "DROP TABLE db1.table1_1;";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        map = groupByName(infos);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(String.format("%d*, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assert.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after drop db1.table1_1: {}", infos);

        // drop db1.table1_2
        sql = "DROP TABLE db1.table1_2;";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        map = groupByName(infos);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assert.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));

        Assert.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        Assert.assertEquals(String.format("table2_1", table2To1.getId()), map.get("group2").get(3));


        LOG.info("after drop db1.table1_2: {}", infos);

        // drop db2
        sql = "DROP DATABASE db2;";
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l*
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        map = groupByName(infos);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assert.assertEquals(String.format("%d*", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after drop db2: {}", infos);

        // create & drop db2 again
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_3 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group3\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Table table2To3 = GlobalStateMgr.getCurrentState().getDb("db2").getTable("table2_3");
        sql = "DROP DATABASE db2;";
        dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        infos = GlobalStateMgr.getCurrentColocateIndex().getInfos();
        map = groupByName(infos);
        LOG.info("after create & drop db2: {}", infos);
        Assert.assertEquals(3, infos.size());
        Assert.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assert.assertEquals("[deleted], [deleted]", map.get("group1").get(3));
        Assert.assertEquals(String.format("%d*", table2To1.getId()), map.get("group2").get(2));
        Assert.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));
        Assert.assertEquals(String.format("%d*", table2To3.getId()), map.get("group3").get(2));
        Assert.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));
    }

    @Test
    public void testCleanUp() throws Exception {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

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
        OlapTable table = (OlapTable) goodDb.getTable("goodTable");
        ColocateTableIndex.GroupId goodGroup = GlobalStateMgr.getCurrentColocateIndex().getGroup(table.getId());


        // create a bad db
        long badDbId = 4000;
        table.id = 4001;
        table.name = "goodTableOfBadDb";
        colocateTableIndex.addTableToGroup(
                badDbId, table, "badGroupOfBadDb", new ColocateTableIndex.GroupId(badDbId, 4002), false);
        // create a bad table in good db
        table.id = 4003;
        table.name = "badTable";
        colocateTableIndex.addTableToGroup(
                goodDb.getId(), table, "badGroupOfBadTable", new ColocateTableIndex.GroupId(goodDb.getId(), 4004), false);

        Map<String, List<String>> map = groupByName(GlobalStateMgr.getCurrentColocateIndex().getInfos());
        Assert.assertTrue(map.containsKey("goodGroup"));
        Assert.assertTrue(map.containsKey("badGroupOfBadDb"));
        Assert.assertTrue(map.containsKey("badGroupOfBadTable"));

        colocateTableIndex.cleanupInvalidDbOrTable(GlobalStateMgr.getCurrentState());
        map = groupByName(GlobalStateMgr.getCurrentColocateIndex().getInfos());

        Assert.assertTrue(map.containsKey("goodGroup"));
        Assert.assertFalse(map.containsKey("badGroupOfBadDb"));
        Assert.assertFalse(map.containsKey("badGroupOfBadTable"));
    }

    @Test
    public void testLakeTableColocation(@Mocked LakeTable olapTable, @Mocked StarOSAgent starOSAgent) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };

        long dbId = 100;
        long dbId2 = 101;
        long tableId = 200;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTable() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new HashDistributionInfo();
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return new ArrayList<>();
            }
        };

        colocateTableIndex.addTableToGroup(
                dbId, (OlapTable) olapTable, "lakeGroup", new ColocateTableIndex.GroupId(dbId, 10000), false /* isReplay */);
        Assert.assertTrue(colocateTableIndex.isLakeColocateTable(tableId));
        colocateTableIndex.addTableToGroup(
                dbId2, (OlapTable) olapTable, "lakeGroup", new ColocateTableIndex.GroupId(dbId2, 10000), false /* isReplay */);
        Assert.assertTrue(colocateTableIndex.isLakeColocateTable(tableId));

        Assert.assertTrue(colocateTableIndex.isGroupUnstable(new ColocateTableIndex.GroupId(dbId, 10000)));
        Assert.assertFalse(colocateTableIndex.isGroupUnstable(new ColocateTableIndex.GroupId(dbId, 10001)));

        colocateTableIndex.removeTable(tableId, null, false /* isReplay */);
        Assert.assertFalse(colocateTableIndex.isLakeColocateTable(tableId));
    }

    @Test
    public void testSaveLoadJsonFormatImage() throws Exception {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils
                .parseStmtWithNewParser("create database db_image;", connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getDb("db_image");
        // create goodtable
        String sql = "CREATE TABLE " +
                "db_image.tbl1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        OlapTable table = (OlapTable) db.getTable("tbl1");

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        colocateTableIndex.saveColocateTableIndexV2(image.getDataOutputStream());

        ColocateTableIndex followerIndex = new ColocateTableIndex();
        SRMetaBlockReader reader = new SRMetaBlockReader(image.getDataInputStream());
        followerIndex.loadColocateTableIndexV2(reader);
        reader.close();
        Assert.assertEquals(colocateTableIndex.getAllGroupIds(), followerIndex.getAllGroupIds());
        Assert.assertEquals(colocateTableIndex.getGroup(table.getId()), followerIndex.getGroup(table.getId()));

        UtFrameUtils.tearDownForPersisTest();
    }
}
