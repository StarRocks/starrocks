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

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import mockit.Expectations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

public class InsertPlanTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testInsert() throws Exception {
        String explainString = getInsertExecPlan("insert into t0 values(1,2,3)");
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: column_0 | 2: column_1 | 3: column_2\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: t0\n" +
                "    TUPLE ID: 1\n" +
                "    RANDOM\n" +
                "\n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         1 | 2 | 3"));

        explainString = getInsertExecPlan("insert into t0(v1) values(1),(2)");
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: column_0 | 2: expr | 3: expr\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: t0\n" +
                "    TUPLE ID: 2\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: column_0\n" +
                "  |  <slot 2> : NULL\n" +
                "  |  <slot 3> : NULL\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         1\n" +
                "         2"));

        explainString = getInsertExecPlan("insert into t0(v1) select v5 from t1");
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: v5 | 4: expr | 5: expr\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: t0\n" +
                "    TUPLE ID: 2\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: v5\n" +
                "  |  <slot 4> : NULL\n" +
                "  |  <slot 5> : NULL\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t1\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n"));
    }

    @Test
    public void testInsertMvSum() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_insert_mv_sum` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        String createMVSQL = "create materialized view mvs as select v1,sum(v2) from test_insert_mv_sum group by v1";
        starRocksAssert.withMaterializedView(createMVSQL);

        String explainString = getInsertExecPlan("insert into test_insert_mv_sum values(1,2,3)");

        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: column_0 | 2: column_1 | 3: column_2"));

        explainString = getInsertExecPlan("insert into test_insert_mv_sum(v1) values(1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: column_0 | 2: expr | 3: expr"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: column_0\n" +
                        "  |  <slot 2> : NULL\n" +
                        "  |  <slot 3> : NULL"));

        explainString = getInsertExecPlan("insert into test_insert_mv_sum(v3,v1) values(3,1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:2: column_1 | 3: expr | 1: column_0"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: column_0\n" +
                        "  |  <slot 2> : 2: column_1\n" +
                        "  |  <slot 3> : NULL"));

        starRocksAssert.dropTable("test_insert_mv_sum");
    }

    @Test
    public void testInsertMvCount() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_insert_mv_count` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        String createMVSQL =
                "create materialized view mvc as select v1,count(v2) from test_insert_mv_count group by v1";
        starRocksAssert.withMaterializedView(createMVSQL);

        String explainString = getInsertExecPlan("insert into test_insert_mv_count values(1,2,3)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: column_0 | 2: column_1 | 3: column_2 | 4: if\n"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: column_0\n" +
                        "  |  <slot 2> : 2: column_1\n" +
                        "  |  <slot 3> : 3: column_2\n" +
                        "  |  <slot 4> : if(2: column_1 IS NULL, 0, 1)"));

        explainString = getInsertExecPlan("insert into test_insert_mv_count(v1) values(1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: column_0 | 2: expr | 3: expr | 4: if"));
        Assert.assertTrue(explainString, explainString.contains(
                "  |  <slot 1> : 1: column_0\n" +
                        "  |  <slot 2> : NULL\n" +
                        "  |  <slot 3> : NULL\n" +
                        "  |  <slot 4> : 0"));

        explainString = getInsertExecPlan("insert into test_insert_mv_count(v3,v1) values(3,1)");

        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:2: column_1 | 3: expr | 1: column_0 | 4: if"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: column_0\n" +
                        "  |  <slot 2> : 2: column_1\n" +
                        "  |  <slot 3> : NULL\n" +
                        "  |  <slot 4> : 0"));

        explainString = getInsertExecPlan("insert into test_insert_mv_count select 1,2,3");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:6: v1 | 7: v2 | 8: v3 | 5: if"));
        Assert.assertTrue(explainString.contains(
                "1:Project\n" +
                        "  |  <slot 5> : 1\n" +
                        "  |  <slot 6> : 1\n" +
                        "  |  <slot 7> : 2\n" +
                        "  |  <slot 8> : 3"));

        starRocksAssert.dropTable("test_insert_mv_count");
    }

    @Test
    public void testInsertFromTable() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `ti1` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `ti2` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        String explainString = getInsertExecPlan("insert into ti1 select * from ti2");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3"));

        explainString = getInsertExecPlan("insert into ti1(v1,v3) select v2,v1 from ti2");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:2: v2 | 4: expr | 1: v1"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: v1\n" +
                        "  |  <slot 2> : 2: v2\n" +
                        "  |  <slot 4> : NULL\n"));

        String createMVSQL =
                "create materialized view mvb as select v1,bitmap_union(to_bitmap(v2)) from ti2 group by v1";
        starRocksAssert.withMaterializedView(createMVSQL);

        explainString = getInsertExecPlan("insert into ti2(v2,v1,v3) select v1,2,NULL from ti1");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:7: v1 | 1: v1 | 8: v3 | 6: to_bitmap"));
        Assert.assertTrue(explainString.contains(
                "  1:Project\n" +
                        "  |  <slot 1> : 1: v1\n" +
                        "  |  <slot 6> : to_bitmap(1: v1)\n" +
                        "  |  <slot 7> : 2\n" +
                        "  |  <slot 8> : NULL\n"));

        explainString = getInsertExecPlan("insert into ti2 select * from ti2");
        Assert.assertTrue(explainString.contains("1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 5> : to_bitmap(2: v2)"));
    }

    @Test
    public void testInsertIntoMysqlTable() throws Exception {
        String sql = "insert into test.mysql_table select v1,v2 from t0";
        String explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: k1 | 5: k2\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  MYSQL TABLE SINK\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 4> : CAST(1: v1 AS INT)\n" +
                "  |  <slot 5> : CAST(2: v2 AS INT)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=4.0\n"));

        sql = "insert into test.mysql_table(k1) select v1 from t0";
        explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:5: k1 | 4: expr\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  MYSQL TABLE SINK\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 4> : NULL\n" +
                "  |  <slot 5> : CAST(1: v1 AS INT)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n"));
    }

    @Test
    public void testInsertDefaultValue() throws Exception {
        starRocksAssert.withTable(" CREATE TABLE `duplicate_table_with_default` ( " +
                "`k1`  date default \"1970-01-01\", " +
                "`k2`  datetime default \"1970-01-01 00:00:00\", " +
                "`k3`  char(20) default \"\", " +
                "`k4`  varchar(20) default \"\", " +
                "`k5`  boolean , " +
                "`k6`  tinyint default \"0\", " +
                "`k7`  smallint default \"0\", " +
                "`k8`  int default \"0\", " +
                "`k9`  bigint default \"0\", " +
                "`k10` largeint default \"12345678901234567890\", " +
                "`k11` float default \"-1\", " +
                "`k12` double default \"0\", " +
                "`k13` decimal(27,9) default \"0\" " +
                ") ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "COMMENT \"OLAP\" DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "BUCKETS 3 " +
                "PROPERTIES ( \"replication_num\" = \"1\" );");
        String sql = "insert into duplicate_table_with_default values " +
                "('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, " +
                "default, default, -18446744073709550633, default, default, -2.18);";
        String explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("constant exprs: \n" +
                "         '2020-06-25' | '2020-06-25 00:16:23' | 'beijing' | 'haidian' | FALSE | " +
                "87 | -31785 | 0 | 0 | -18446744073709550633 | -1.0 | 0.0 | -2.18"));

        // check multi rows value
        sql = "insert into duplicate_table_with_default values " +
                "('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, " +
                "default, default, -18446744073709550633, default, default, -2.18), " +
                "(default, '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, " +
                "default, default, -18446744073709550633, default, default, -2.18)";
        explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("constant exprs: \n" +
                "         '2020-06-25' | '2020-06-25 00:16:23' | 'beijing' | 'haidian' | FALSE " +
                "| 87 | -31785 | 0 | 0 | -18446744073709550633 | -1.0 | 0.0 | -2.18\n" +
                "         '1970-01-01' | '2020-06-25 00:16:23' | 'beijing' | 'haidian' | FALSE " +
                "| 87 | -31785 | 0 | 0 | -18446744073709550633 | -1.0 | 0.0 | -2.18"));

        sql = "insert into duplicate_table_with_default values " +
                "('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', default, 87, -31785, " +
                "default, default, -18446744073709550633, default, default, -2.18);";
        try {
            getInsertExecPlan(sql);
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().equals("Getting analyzing error. Detail message: " +
                    "Column has no default value, column=k5."));
        }

        sql = "insert into duplicate_table_with_default(K1,k2,k3) " +
                "values('2020-06-25', '2020-06-25 00:16:23', 'beijing')";
        explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("<slot 1> : 1: column_0"));
    }

    public static String getInsertExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);

        String ret = execPlan.getExplainString(TExplainLevel.NORMAL);
        return ret;
    }

    public static void containsKeywords(String plan, String... keywords) throws Exception {
        Assert.assertTrue(Stream.of(keywords).allMatch(plan::contains));
    }

    @Test
    public void testBitmapInsertInto() throws Exception {
        String sql = "INSERT INTO test.bitmap_table (id, id2) VALUES (1001, to_bitmap(1000)), (1001, to_bitmap(2000));";
        String plan = getInsertExecPlan(sql);
        containsKeywords(plan, "constant exprs: \n" +
                "         1001 | to_bitmap(1000)\n" +
                "         1001 | to_bitmap(2000)");

        sql = "insert into test.bitmap_table select id, bitmap_union(id2) from test.bitmap_table_2 group by id;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 3: bitmap_union",
                "OLAP TABLE SINK", "1:AGGREGATE (update finalize)", "output: bitmap_union(2: id2)", "0:OlapScanNode");

        sql = "insert into test.bitmap_table select id, id2 from test.bitmap_table_2;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 2: id2", "OLAP TABLE SINK", "0:OlapScanNode");

        Assert.assertThrows("No matching function with signature: to_bitmap(bitmap).", SemanticException.class,
                () -> getInsertExecPlan(
                        "insert into test.bitmap_table select id, to_bitmap(id2) from test.bitmap_table_2;"));

        Assert.assertThrows("No matching function with signature: bitmap_hash(bitmap).", SemanticException.class,
                () -> getInsertExecPlan(
                        "insert into test.bitmap_table select id, bitmap_hash(id2) from test.bitmap_table_2;"));

        sql = "insert into test.bitmap_table select id, id from test.bitmap_table_2;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 3: id2", "OLAP TABLE SINK",
                "1:Project", "<slot 1> : 1: id", "<slot 3> : CAST(1: id AS BITMAP)");
    }

    @Test
    public void testInsertWithColocate() throws Exception {
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `user_profile` (\n" +
                "  `distinct_id` bigint(20) NULL ,\n" +
                "  `user_id` varchar(128) NULL ,\n" +
                "  `device_id_list` varchar(512) NULL ,\n" +
                "  `first_browser_charset` varchar(512) NULL ,\n" +
                "  `first_browser_language` varchar(512) NULL ,\n" +
                "  `first_referrer` varchar(512) NULL ,\n" +
                "  `first_referrer_host` varchar(512) NULL ,\n" +
                "  `first_search_keyword` varchar(512) NULL ,\n" +
                "  `first_traffic_source_type` varchar(512) NULL ,\n" +
                "  `name` varchar(512) NULL ,\n" +
                "  `utm_campaign` varchar(512) NULL ,\n" +
                "  `utm_content` varchar(512) NULL ,\n" +
                "  `utm_matching_type` varchar(512) NULL ,\n" +
                "  `utm_medium` varchar(512) NULL ,\n" +
                "  `utm_source` varchar(512) NULL ,\n" +
                "  `utm_term` varchar(512) NULL ,\n" +
                "  `Email` varchar(512) NULL ,\n" +
                "  `child_age` int(11) NULL ,\n" +
                "  `child_gender` varchar(8) NULL ,\n" +
                "  `child_learning_age` int(11) NULL ,\n" +
                "  `first_pay_succeed_time` datetime NULL ,\n" +
                "  `first_visit_source` varchar(512) NULL ,\n" +
                "  `is_vip` boolean NULL ,\n" +
                "  `learn_purpose` varchar(512) NULL ,\n" +
                "  `level` varchar(12) NULL ,\n" +
                "  `parent_gender` varchar(8) NULL ,\n" +
                "  `parent_id` varchar(32) NULL ,\n" +
                "  `phone_number` varchar(11) NULL ,\n" +
                "  `push_id` varchar(512) NULL ,\n" +
                "  `register_channel` varchar(512) NULL ,\n" +
                "  `register_time` datetime NULL ,\n" +
                "  `relative` varchar(12) NULL ,\n" +
                "  `teacher_number` varchar(11) NULL ,\n" +
                "  `user_type` varchar(12) NULL ,\n" +
                "  `vip_duetime` datetime NULL ,\n" +
                "  `vip_type` varchar(12) NULL ,\n" +
                "  `year_of_birth` varchar(4) NULL ,\n" +
                "  `user_tag_bq002` varchar(128) NULL \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`distinct_id`)\n" +
                "DISTRIBUTED BY HASH(`distinct_id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"edu\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `user_info` (\n" +
                "  `distinct_id` bigint(20) NULL ,\n" +
                "  `distinct_sid` varchar(128) NULL ,\n" +
                "  `user_id` varchar(128) NULL ,\n" +
                "  `device_id_list` varchar(512) NULL ,\n" +
                "  `first_browser_charset` varchar(512) NULL ,\n" +
                "  `first_browser_language` varchar(512) NULL ,\n" +
                "  `first_referrer` varchar(512) NULL ,\n" +
                "  `first_referrer_host` varchar(512) NULL ,\n" +
                "  `first_search_keyword` varchar(512) NULL ,\n" +
                "  `first_traffic_source_type` varchar(512) NULL ,\n" +
                "  `name` varchar(512) NULL ,\n" +
                "  `utm_campaign` varchar(512) NULL ,\n" +
                "  `utm_content` varchar(512) NULL ,\n" +
                "  `utm_matching_type` varchar(512) NULL ,\n" +
                "  `utm_medium` varchar(512) NULL ,\n" +
                "  `utm_source` varchar(512) NULL ,\n" +
                "  `utm_term` varchar(512) NULL ,\n" +
                "  `Email` varchar(512) NULL ,\n" +
                "  `child_age` int(11) NULL ,\n" +
                "  `child_gender` varchar(8) NULL ,\n" +
                "  `child_learning_age` int(11) NULL ,\n" +
                "  `first_pay_succeed_time` datetime NULL ,\n" +
                "  `first_visit_source` varchar(512) NULL ,\n" +
                "  `is_vip` boolean NULL ,\n" +
                "  `learn_purpose` varchar(512) NULL ,\n" +
                "  `level` varchar(12) NULL ,\n" +
                "  `parent_gender` varchar(8) NULL ,\n" +
                "  `parent_id` varchar(32) NULL ,\n" +
                "  `phone_number` varchar(11) NULL ,\n" +
                "  `push_id` varchar(512) NULL ,\n" +
                "  `register_channel` varchar(512) NULL ,\n" +
                "  `register_time` datetime NULL ,\n" +
                "  `relative` varchar(12) NULL ,\n" +
                "  `teacher_number` varchar(11) NULL ,\n" +
                "  `user_type` varchar(12) NULL ,\n" +
                "  `vip_duetime` datetime NULL ,\n" +
                "  `vip_type` varchar(12) NULL ,\n" +
                "  `year_of_birth` varchar(4) NULL ,\n" +
                "  `create_date` datetime NULL \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`distinct_id`)\n" +
                "DISTRIBUTED BY HASH(`distinct_id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"edu\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `user_tag_bq002` (\n" +
                "  `base_time` date NULL ,\n" +
                "  `distinct_id` bigint(20) NULL ,\n" +
                "  `tag_value` varchar(128) NULL ,\n" +
                "  `user_id` varchar(128) NULL \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`base_time`, `distinct_id`, `tag_value`)\n" +
                "DISTRIBUTED BY HASH(`distinct_id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"edu\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `user_tag_bq003` (\n" +
                "  `base_time` date NULL ,\n" +
                "  `distinct_id` bigint(20) NULL ,\n" +
                "  `tag_value` varchar(128) NULL ,\n" +
                "  `user_id` varchar(128) NULL \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`base_time`, `distinct_id`, `tag_value`)\n" +
                "DISTRIBUTED BY HASH(`distinct_id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"edu\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("\n" +
                "CREATE TABLE `user_tag_bq004` (\n" +
                "  `base_time` date NULL ,\n" +
                "  `distinct_id` bigint(20) NULL ,\n" +
                "  `tag_value` varchar(128) NULL ,\n" +
                "  `user_id` varchar(128) NULL \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`base_time`, `distinct_id`, `tag_value`)\n" +
                "DISTRIBUTED BY HASH(`distinct_id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"edu\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");\n");
        String sql = "INSERT INTO user_profile(   distinct_id,  device_id_list,first_browser_charset," +
                "first_browser_language,first_referrer,first_referrer_host,first_search_keyword," +
                "first_traffic_source_type,name,utm_campaign,utm_content,utm_matching_type," +
                "utm_medium,utm_source,utm_term,email,child_age,child_gender,user_id," +
                "child_learning_age,first_pay_succeed_time,first_visit_source,is_vip,learn_purpose,level," +
                "parent_gender,parent_id,phone_number,push_id,register_channel,register_time,relative," +
                "teacher_number,user_type,vip_duetime,vip_type,year_of_birth , user_tag_bq002  )   " +
                "SELECT a.distinct_id,a.device_id_list,a.first_browser_charset,a.first_browser_language," +
                "a.first_referrer,a.first_referrer_host,a.first_search_keyword,a.first_traffic_source_type,a.name," +
                "a.utm_campaign,a.utm_content,a.utm_matching_type,a.utm_medium,a.utm_source,a.utm_term," +
                "a.email,a.child_age,a.child_gender,a.user_id,a.child_learning_age,a.first_pay_succeed_time," +
                "a.first_visit_source,a.is_vip,a.learn_purpose,a.level,a.parent_gender,a.parent_id,a.phone_number," +
                "a.push_id,a.register_channel,a.register_time,a.relative,a.teacher_number,a.user_type," +
                "a.vip_duetime,a.vip_type,a.year_of_birth  , a1.tag_value as user_tag_bq002    " +
                "FROM user_info a    " +
                "LEFT JOIN  (SELECT  distinct_id,tag_value  FROM user_tag_bq002 WHERE base_time ='2021-06-23' )  a1   " +
                "ON a.distinct_id =  a1.distinct_id  " +
                "LEFT JOIN  (SELECT  distinct_id,tag_value  FROM user_tag_bq003 WHERE base_time ='2021-06-23' )  a2   " +
                "ON a.distinct_id =  a2.distinct_id   " +
                "LEFT JOIN  (SELECT  distinct_id,tag_value  FROM user_tag_bq004 WHERE base_time ='2021-06-23' )  a3   " +
                "ON a.distinct_id =  a3.distinct_id;";
        String plan = getInsertExecPlan(sql);
        Assert.assertTrue(plan.contains("  11:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: distinct_id = 41: distinct_id"));
        Assert.assertTrue(plan.contains("  7:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: distinct_id = 49: distinct_id"));
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: distinct_id = 45: distinct_id"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testExplainInsert() throws Exception {
        String sql = "explain insert into t0 select * from t0";
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);
        Assert.assertTrue(((InsertStmt) statementBase).getQueryStatement().isExplain());
    }

    @Test
    public void testInsertExchange() throws Exception {
        FeConstants.runningUnitTest = true;
        InsertPlanner.enableSingleReplicationShuffle = true;
        Config.eliminate_shuffle_load_by_replicated_storage = false;
        {
            // keysType is DUP_KEYS
            String sql = "explain insert into t0 select * from t0";
            String plan = getInsertExecPlan(sql);
            assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: t0\n" +
                    "    TUPLE ID: 1\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0");

            // Specify only part of the columns
            sql = "explain insert into t0(v1, v2) select v1, v2 from t0;";
            plan = getInsertExecPlan(sql);
            assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: t0\n" +
                    "    TUPLE ID: 2\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 4> : NULL\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0");
        }
        {
            // KesType is PRIMARY_KEYS
            String sql = "explain insert into tprimary select * from tprimary";
            String plan = getInsertExecPlan(sql);
            assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: tprimary\n" +
                    "    TUPLE ID: 1\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: tprimary");

            // Group by columns is different from the key columns, so extra exchange is needed
            sql = "explain insert into tprimary select min(pk), v1, max(v2) from tprimary group by v1";
            plan = getInsertExecPlan(sql);
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 04\n" +
                    "    HASH_PARTITIONED: 4: min\n" +
                    "\n" +
                    "  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: min(4: min), max(5: max)\n" +
                    "  |  group by: 2: v1");

            // Group by columns are compatible with the key columns, so there is no need to add extra exchange node
            sql = "explain insert into tprimary select pk, min(v1), max(v2) from tprimary group by pk";
            plan = getInsertExecPlan(sql);
            assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: tprimary\n" +
                    "    TUPLE ID: 2\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: min(2: v1), max(3: v2)\n" +
                    "  |  group by: 1: pk");
        }
        {
            // KesType is AGG_KEYS
            String sql = "explain insert into baseall select * from baseall";
            String plan = getInsertExecPlan(sql);
            assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: baseall\n" +
                    "    TUPLE ID: 1\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: baseall");

            // Group by columns is different from the key columns, so extra exchange is needed
            sql =
                    "explain insert into baseall select min(k1), max(k2), min(k3), max(k4), min(k5), max(k6), " +
                            "min(k10), max(k11), min(k7), k8, k9 from baseall group by k8, k9";
            plan = getInsertExecPlan(sql);
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 04\n" +
                    "    HASH_PARTITIONED: 12: min, 13: max, 14: min, 15: max, 16: min, 17: max, 18: min, 19: max, 20: min\n" +
                    "\n" +
                    "  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: max(17: max), min(18: min), max(19: max), min(20: min), " +
                    "min(12: min), max(13: max), min(14: min), max(15: max), min(16: min)\n" +
                    "  |  group by: 10: k8, 11: k9");

            // Group by columns are compatible with the key columns, so there is no need to add extra exchange node
            sql =
                    "explain insert into baseall select k1, k2, min(k3), max(k4), min(k5), max(k6), " +
                            "min(k10), max(k11), min(k7), min(k8), max(k9) from baseall group by k1, k2";
            plan = getInsertExecPlan(sql);
            assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: baseall\n" +
                    "    TUPLE ID: 2\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: max(8: k11), min(9: k7), min(10: k8), max(11: k9), min(3: k3), " +
                    "max(4: k4), min(5: k5), max(6: k6), min(7: k10)\n" +
                    "  |  group by: 1: k1, 2: k2");

            // Group by columns are compatible with the key columns, so there is no need to add extra exchange node
            sql =
                    "explain insert into baseall select k1, k2, k3, k4, k5, k6, k10, k11, k7, min(k8), " +
                            "max(k9) from baseall group by k1, k2, k3, k4, k5, k6, k10, k11, k7";
            plan = getInsertExecPlan(sql);
            assertContains(plan, " OLAP TABLE SINK\n" +
                    "    TABLE: baseall\n" +
                    "    TUPLE ID: 2\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: min(10: k8), max(11: k9)\n" +
                    "  |  group by: 1: k1, 2: k2, 3: k3, 4: k4, 5: k5, 6: k6, 7: k10, 8: k11, 9: k7");
        }
        InsertPlanner.enableSingleReplicationShuffle = false;
        FeConstants.runningUnitTest = false;
        Config.eliminate_shuffle_load_by_replicated_storage = true;
    }

    @Test
    public void testInsertSelectWithConstant() throws Exception {
        String explainString = getInsertExecPlan("insert into tarray select 1,null,null from tarray");
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:7: v1 | 8: v2 | 9: v3\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: tarray\n" +
                "    TUPLE ID: 2\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : NULL\n" +
                "  |  <slot 9> : NULL"));
    }

    @Test
    public void testInsertMapColumn() throws Exception {
        String explainString = getInsertExecPlan("insert into tmap values (2,2,map())");
        Assert.assertTrue(explainString.contains("2 | 2 | map{}"));

        explainString = getInsertExecPlan("insert into tmap values (2,2,null)");
        Assert.assertTrue(explainString.contains("2 | 2 | NULL"));

        explainString = getInsertExecPlan("insert into tmap values (2,2,map(2,2))");
        Assert.assertTrue(explainString.contains("2 | 2 | map{2:'2'}")); // implicit cast

        explainString = getInsertExecPlan("insert into tmap values (2,2,map{})");
        Assert.assertTrue(explainString.contains("2 | 2 | map{}"));

        explainString = getInsertExecPlan("insert into tmap values (2,2,map{2:3, 2:4})");
        Assert.assertTrue(explainString.contains("2 | 2 | map{2:'3',2:'4'}")); // will distinct in BE

        explainString = getInsertExecPlan("insert into tmap values (2,2,map{2:2})");
        Assert.assertTrue(explainString.contains("2 | 2 | map{2:'2'}")); // implicit cast
    }

    @Test
    public void testInsertAggLimit() throws Exception {
        FeConstants.runningUnitTest = true;
        InsertPlanner.enableSingleReplicationShuffle = true;
        {
            // KesType is AGG_KEYS
            String sql = "explain insert into baseall select * from baseall limit 1";
            String plan = getInsertExecPlan(sql);
            assertContains(plan, "STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 01\n" +
                    "    UNPARTITIONED");

            InsertPlanner.enableSingleReplicationShuffle = false;
            FeConstants.runningUnitTest = false;
        }
    }

    @Test
    public void testInsertIcebergTableSink() throws Exception {
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);
        MetadataMgr metadata = starRocksAssert.getCtx().getGlobalStateMgr().getMetadataMgr();

        Table nativeTable = new BaseTable(null, null);

        Column k1 = new Column("k1", Type.INT);
        Column k2 = new Column("k2", Type.INT);
        IcebergTable.Builder builder = IcebergTable.builder();
        builder.setCatalogName("iceberg_catalog");
        builder.setRemoteDbName("iceberg_db");
        builder.setRemoteTableName("iceberg_table");
        builder.setSrTableName("iceberg_table");
        builder.setFullSchema(Lists.newArrayList(k1, k2));
        builder.setNativeTable(nativeTable);
        IcebergTable icebergTable = builder.build();

        new Expectations(icebergTable) {
            {
                icebergTable.getUUID();
                result = 12345566;
                minTimes = 0;

                icebergTable.isUnPartitioned();
                result = true;
                minTimes = 0;

                icebergTable.getPartitionColumnNames();
                result = new ArrayList<>();
            }
        };

        new Expectations(nativeTable) {
            {
                nativeTable.sortOrder();
                result = SortOrder.unsorted();
                minTimes = 0;

                nativeTable.location();
                result = "hdfs://fake_location";
                minTimes = 0;

                nativeTable.properties();
                result = new HashMap<String, String>();
                minTimes = 0;

                nativeTable.io();
                result = new HadoopFileIO();
                minTimes = 0;
            }
        };

        new Expectations(metadata) {
            {
                metadata.getDb("iceberg_catalog", "iceberg_db");
                result = new Database(12345566, "iceberg_db");
                minTimes = 0;

                metadata.getTable("iceberg_catalog", "iceberg_db", "iceberg_table");
                result = icebergTable;
                minTimes = 0;
            }
        };

        String actualRes = getInsertExecPlan("explain insert into iceberg_catalog.iceberg_db.iceberg_table select 1, 2");
        String expected = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: k1 | 5: k2\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  Iceberg TABLE SINK\n" +
                "    TABLE: 12345566\n" +
                "    TUPLE ID: 2\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 4> : 1\n" +
                "  |  <slot 5> : 2\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n";
        Assert.assertEquals(expected, actualRes);
    }

    @Test
    public void insertToSql() {
        String sql = "insert into test_all_type_partition_by_date " +
                "partition(p1992) (t1a, id_date) SELECT `t1a`, `id_date` FROM `test_all_type_partition_by_date` ";
        var stmts = SqlParser.parse(sql, new SessionVariable());
        Assert.assertEquals(1, stmts.size());

        // verify generated SQL
        String genSql = AstToSQLBuilder.toSQL(stmts.get(0));
        Assert.assertEquals("INSERT INTO `test_all_type_partition_by_date` " +
                "PARTITION (p1992) (`t1a`,`id_date`) " +
                "SELECT `t1a`, `id_date`\n" +
                "FROM `test_all_type_partition_by_date`", genSql);

        // parse it again
        var stmts2 = SqlParser.parse(genSql, new SessionVariable());
        Assert.assertEquals(genSql, AstToSQLBuilder.toSQL(stmts2.get(0)));
    }

    @Test
    public void testInsertNotExistTable() {
        try {
            getInsertExecPlan("insert into not_exist_table values (1)");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Table not_exist_table is not found"));
        }
    }

    @Test
    public void testInsertFiles() throws Exception {
        String actual = getInsertExecPlan("insert into files " +
                "(\"path\" = \"hdfs://127.0.0.1:9000/files/\", \"format\"=\"parquet\", \"compression\" = \"uncompressed\") " +
                "select 1 as k1");
        String expected = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: expr\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  TABLE FUNCTION TABLE SINK\n" +
                "    PATH: hdfs://127.0.0.1:9000/files/data_\n" +
                "    FORMAT: parquet\n" +
                "    PARTITION BY: []\n" +
                "    SINGLE: false\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testInsertFilesWithSingle() throws Exception {
        String actual = getInsertExecPlan("insert into files " +
                "(\"path\" = \"hdfs://127.0.0.1:9000/files/\", \"format\"=\"parquet\", \"compression\" = \"uncompressed\"," +
                " \"single\" = \"true\") " +
                "select 1 as k1, 2 as k2");
        String expected = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: expr | 3: expr\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  TABLE FUNCTION TABLE SINK\n" +
                "    PATH: hdfs://127.0.0.1:9000/files/\n" +
                "    FORMAT: parquet\n" +
                "    PARTITION BY: []\n" +
                "    SINGLE: true\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testInsertFilesWithPartition() throws Exception {
        String actual = getInsertExecPlan("insert into files " +
                "(\"path\" = \"hdfs://127.0.0.1:9000/files/\", \"format\"=\"parquet\", \"compression\" = \"uncompressed\"," +
                " \"partition_by\" = \"v1\") " +
                "select v1, v2 from t0");
        String expected = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 2: v2\n" +
                "  PARTITION: HASH_PARTITIONED: 1: v1\n" +
                "\n" +
                "  TABLE FUNCTION TABLE SINK\n" +
                "    PATH: hdfs://127.0.0.1:9000/files/\n" +
                "    FORMAT: parquet\n" +
                "    PARTITION BY: [v1]\n" +
                "    SINGLE: false\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n";
        Assert.assertEquals(expected, actual);
    }
}
