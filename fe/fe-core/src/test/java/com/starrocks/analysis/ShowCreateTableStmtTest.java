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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShowCreateTableStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
    }

    @Test
    public void testShowCreateViewUseMV() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.base\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test_mv distributed by hash(k1)" +
                        " as select k1 from base");
        String sql = "show create view test_mv";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, ctx);
        Assertions.assertEquals("test_mv", resultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("CREATE VIEW `test_mv` AS SELECT `test`.`base`.`k1`\n" +
                "FROM `test`.`base`", resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testNormal() throws Exception {
        ctx.setDatabase("testDb");
        ShowCreateTableStmt stmt =
                new ShowCreateTableStmt(new TableName("testDb", "testTbl"), ShowCreateTableStmt.CreateTableType.TABLE);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("testDb", stmt.getDb());
        Assertions.assertEquals("testTbl", stmt.getTable());
        Assertions.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assertions.assertEquals("Table", stmt.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("Create Table", stmt.getMetaData().getColumn(1).getName());
    }

    @Test
    public void testNoTbl() {
        assertThrows(SemanticException.class, () -> {
            ShowCreateTableStmt stmt = new ShowCreateTableStmt(null, ShowCreateTableStmt.CreateTableType.TABLE);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            Assertions.fail("No Exception throws.");
        });
    }

    @Test
    public void testPKShouldShowDefault() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `test_pk_current_timestamp` (\n" +
                        "  `id` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`id`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 5 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
        String sql = "show create table test_pk_current_timestamp";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, ctx);
        Assertions.assertEquals("test_pk_current_timestamp", resultSet.getResultRows().get(0).get(0));
        Assertions.assertTrue(resultSet.getResultRows().get(0).get(1).contains("datetime NOT NULL DEFAULT CURRENT_TIMESTAMP"));
    }

    @Test
    public void testHiveTableMapProperties() {
        List<Column> fullSchema = new ArrayList<>();
        fullSchema.add(new Column("id", Type.INT));
        Map<String, String> props = new HashMap<>();
        props.put("COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\"}");

        HiveTable table = new HiveTable(100, "test", fullSchema, "aa", "bb", "cc", "dd", "hdfs://xxx", "",
                0, new ArrayList<>(), fullSchema.stream().map(x -> x.getName()).collect(Collectors.toList()),
                props, new HashMap<>(),  HiveStorageFormat.ORC, HiveTable.HiveTableType.MANAGED_TABLE);
        List<String> result = new ArrayList<>();
        AstToStringBuilder.getDdlStmt(table, result, null, null, false, true);
        Assertions.assertEquals(result.size(), 1);
        String value = result.get(0);
        System.out.println(value);
        Assertions.assertTrue(value.contains("\"COLUMN_STATS_ACCURATE\"  =  \"{\\\"BASIC_STATS\\\":\\\"true\\\"}\""));
    }

    @Test
    public void testShowPartitionLiveNumber() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `aaa` (\n" +
                        "  `id` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `city` varchar(20) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "PARTITION BY (`city`) \n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 5 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"partition_live_number\" = \"1\"\n" +
                        ");");
        String sql = "show create table test.aaa";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, ctx);
        Assertions.assertTrue(resultSet.getResultRows().get(0).get(1).contains("partition_live_number"));
    }
<<<<<<< HEAD
=======

    @Test
    public void test() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE dict (\n"
                        + "                `k1`  date,\n"
                        + "                `k2`  datetime,\n"
                        + "                `k3`  varchar(20),\n"
                        + "                `k4`  varchar(20),\n"
                        + "                `k5`  boolean,\n"
                        + "                    `k6`  tinyint,\n"
                        + "                    `k7`  smallint,\n"
                        + "                    `k8`  int,\n"
                        + "                    `k9`  bigint,\n"
                        + "                    `k10` largeint,\n"
                        + "                    `k11` float,\n"
                        + "                    `k12` double,\n"
                        + "                    `k13` decimal(27,9),\n"
                        + "                map_value1 bigint,\n"
                        + "                map_value bigint not null auto_increment\n"
                        + "                )\n"
                        + "        primary KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n"
                        + "        COMMENT \"OLAP\"\n"
                        + "        DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3\n"
                        + "        PROPERTIES (\n"
                        + "                \"replication_num\" = \"1\",\n"
                        + "                \"storage_format\" = \"v2\",\n"
                        + "                \"light_schema_change\" = \"false\"\n"
                        + "        );")
                .withTable("CREATE TABLE primary_table (\n"
                        + "                `k1` date NOT NULL,\n"
                        + "                `k2` datetime NOT NULL,\n"
                        + "                `k3` string NOT NULL,\n"
                        + "                `k4` string NOT NULL,\n"
                        + "                `k5` boolean NOT NULL,\n"
                        + "                `k6` tinyint NOT NULL,\n"
                        + "                `k7` smallint NOT NULL,\n"
                        + "                `k8` int NOT NULL,\n"
                        + "                `k9` bigint NOT NULL,\n"
                        + "                `k10` largeint NOT NULL,\n"
                        + "                `k11` float NOT NULL,\n"
                        + "                `k12` double NOT NULL,\n"
                        + "                `k13` decimal(27,9) NOT NULL,\n"
                        + "                v_mapvalue bigint as dict_mapping(\"test.dict\", k1,k2,k3,k4,k5, True)\n"
                        + "                )\n"
                        + "        PRIMARY KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n"
                        + "        COMMENT \"OLAP\"\n"
                        + "        DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3\n"
                        + "        PROPERTIES (\n"
                        + "                \"replication_num\" = \"1\",\n"
                        + "                \"storage_format\" = \"v2\",\n"
                        + "                \"fast_schema_evolution\" = \"true\"\n"
                        + "        );");
        String sql = "show create table primary_table";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, ctx);
        Assertions.assertTrue(resultSet.getResultRows().get(0).get(1)
                        .contains("AS dict_mapping('test.dict', k1, k2, k3, k4, k5, TRUE) COMMENT"),
                resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testShowCreateTableWithUniqueAndForeignKeyConstraints() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.parent_uk1 (\n" +
                        "  k1 INT NOT NULL,\n" +
                        "  k2 VARCHAR(20) NOT NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "  \"replication_num\" = \"1\",\n" +
                        "  \"unique_constraints\" = \"k1,k2\"\n" +
                        ");")
                .withTable("CREATE TABLE test.parent_uk2 (\n" +
                        "  id INT NOT NULL,\n" +
                        "  name VARCHAR(50) NOT NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "  \"replication_num\" = \"1\",\n" +
                        "  \"unique_constraints\" = \"id\"\n" +
                        ");");

        // Test showing unique constraints
        String showParent1 = "show create table test.parent_uk1";
        ShowCreateTableStmt stmt1 = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showParent1, ctx);
        ShowResultSet result1 = ShowExecutor.execute(stmt1, ctx);
        String createTable1 = result1.getResultRows().get(0).get(1);
        Assertions.assertTrue(createTable1.contains("unique_constraints"),
                "SHOW CREATE TABLE should include unique_constraints. Got: " + createTable1);
        Assertions.assertTrue(createTable1.contains("k1") && createTable1.contains("k2"),
                "unique_constraints should include column names. Got: " + createTable1);

        String showParent2 = "show create table test.parent_uk2";
        ShowCreateTableStmt stmt2 = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showParent2, ctx);
        ShowResultSet result2 = ShowExecutor.execute(stmt2, ctx);
        String createTable2 = result2.getResultRows().get(0).get(1);
        Assertions.assertTrue(createTable2.contains("unique_constraints"),
                "SHOW CREATE TABLE should include unique_constraints. Got: " + createTable2);

        // Create child table with foreign key constraints
        starRocksAssert.withTable("CREATE TABLE test.child_fk (\n" +
                "  id INT,\n" +
                "  parent1_k1 INT,\n" +
                "  parent1_k2 VARCHAR(20),\n" +
                "  parent2_id INT,\n" +
                "  value VARCHAR(100)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\",\n" +
                "  \"foreign_key_constraints\" = \"(parent1_k1, parent1_k2) REFERENCES parent_uk1(k1, k2);" +
                "                                 (parent2_id) REFERENCES parent_uk2(id)\"\n" +
                ");");

        // Test showing foreign key constraints
        String showChild = "show create table test.child_fk";
        ShowCreateTableStmt stmt3 = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showChild, ctx);
        ShowResultSet result3 = ShowExecutor.execute(stmt3, ctx);
        String createTable3 = result3.getResultRows().get(0).get(1);
        Assertions.assertTrue(createTable3.contains("foreign_key_constraints"),
                "SHOW CREATE TABLE should include foreign_key_constraints. Got: " + createTable3);
        Assertions.assertTrue(createTable3.contains("parent_uk1") && createTable3.contains("parent_uk2"),
                "foreign_key_constraints should include parent table names. Got: " + createTable3);
        Assertions.assertTrue(createTable3.contains("parent1_k1") && createTable3.contains("parent1_k2"),
                "foreign_key_constraints should include child column names. Got: " + createTable3);
    }
>>>>>>> eb4b6f1edc ([BugFix] Fix foreign key constraints lost after FE restart (#66474))
}