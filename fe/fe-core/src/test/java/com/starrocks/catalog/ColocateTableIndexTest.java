// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class ColocateTableIndexTest {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndexTest.class);
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster("fe/mocked/ColocateTableIndexTest/" + UUID.randomUUID().toString() + "/");
    }

    @Test
    public void testDropTable() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        // create table1_1->group1
        String sql = "CREATE TABLE db1.table1_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        List<List<String>> infos = Catalog.getCurrentColocateIndex().getInfos();
        // group1->table1_1
        Assert.assertEquals(1, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Table table1_1 = Catalog.getCurrentCatalog().getDb("default_cluster:db1").getTable("table1_1");
        Assert.assertEquals(String.format("%d", table1_1.getId()), infos.get(0).get(2));
        LOG.info("after create db1.table1_1: {}", infos);

        // create table1_2->group1
        sql = "CREATE TABLE db1.table1_2 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        // group1 -> table1_1, table1_2
        infos = Catalog.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(1, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Table table1_2 = Catalog.getCurrentCatalog().getDb("default_cluster:db1").getTable("table1_2");
        Assert.assertEquals(String.format("%d, %d", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        LOG.info("after create db1.table1_2: {}", infos);

        // create db2
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group2\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        // group1 -> table1_1, table1_2
        // group2 -> table2_l
        infos = Catalog.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d, %d", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Table table2_1 = Catalog.getCurrentCatalog().getDb("default_cluster:db2").getTable("table2_1");
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after create db2.table2_1: {}", infos);

        // drop db1.table1_1
        sql = "DROP TABLE db1.table1_1;";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2
        // group2 -> table2_l
        infos = Catalog.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d*, %d", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after drop db1.table1_1: {}", infos);

        // drop db1.table1_2
        sql = "DROP TABLE db1.table1_2;";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l
        infos = Catalog.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d*, %d*", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after drop db1.table1_2: {}", infos);

        // drop db2
        sql = "DROP DATABASE db2;";
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().dropDb(dropDbStmt);
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l*
        infos = Catalog.getCurrentColocateIndex().getInfos();
        Assert.assertEquals(2, infos.size());
        Assert.assertTrue(infos.get(0).get(1).contains("group1"));
        Assert.assertEquals(String.format("%d*, %d*", table1_1.getId(), table1_2.getId()), infos.get(0).get(2));
        Assert.assertTrue(infos.get(1).get(1).contains("group2"));
        Assert.assertEquals(String.format("%d*", table2_1.getId()), infos.get(1).get(2));
        LOG.info("after drop db2: {}", infos);
    }

    @Test
    public void testCleanUp() throws Exception {
        ColocateTableIndex colocateTableIndex = Catalog.getCurrentColocateIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        int n = colocateTableIndex.getAllGroupIds().size();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt("create database goodDb;", connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        Database goodDb = Catalog.getCurrentCatalog().getDb("default_cluster:goodDb");
        // create goodtable
        String sql = "CREATE TABLE " +
                "goodDb.goodTable (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        OlapTable table = (OlapTable)goodDb.getTable("goodTable");
        ColocateTableIndex.GroupId goodGroup = Catalog.getCurrentColocateIndex().getGroup(table.getId());
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

        colocateTableIndex.cleanupInvalidDbOrTable(Catalog.getCurrentCatalog());

        Assert.assertEquals(n + 1, colocateTableIndex.getAllGroupIds().size());
    }

}
