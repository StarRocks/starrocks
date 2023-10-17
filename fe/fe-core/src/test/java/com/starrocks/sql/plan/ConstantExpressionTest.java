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

package com.starrocks.sql.plan;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.ParsingException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConstantExpressionTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    private void testFragmentPlanContainsConstExpr(String sql, String result) throws Exception {
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString, explainString.contains(": " + result));
    }

    private void testFragmentPlanContains(String sql, String result) throws Exception {
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString, explainString.contains(result));
    }

    @Test
    public void testInspectMvMeta() throws Exception {
        String db = starRocksAssert.getCtx().getDatabase();
        starRocksAssert.withTable(
                "create table mv_base_table_9527 (id int, name string) " +
                        "distributed by hash(id) " +
                        "properties('replication_num'='1')");
        starRocksAssert.withMaterializedView("create materialized view mv1 " +
                "distributed by hash(id) " +
                "refresh async " +
                "properties('replication_num'='1') " +
                "as select * from mv_base_table_9527");
        testFragmentPlanContains("select inspect_mv_meta('mv1');", "MaterializedView");
        String fullName = db + ".mv1";
        testFragmentPlanContains(String.format("select inspect_mv_meta('%s');", fullName), "MaterializedView");

        // wrong arguments
        Assert.assertThrows(StarRocksPlannerException.class,
                () -> getFragmentPlan("select inspect_mv_meta('snowflake');"));
        Assert.assertThrows(StarRocksPlannerException.class,
                () -> getFragmentPlan("select inspect_mv_meta('mv_base_table_9527');"));
        Assert.assertThrows(StarRocksPlannerException.class,
                () -> getFragmentPlan("select inspect_mv_meta('a.b.c.d');"));
        Assert.assertThrows(StarRocksPlannerException.class,
                () -> getFragmentPlan("select inspect_mv_meta('db_notexists.mv1');"));

        // inspect_related_mv
        testFragmentPlanContains("select inspect_related_mv('mv_base_table_9527')", "name\":\"mv1\"");
    }

    @Test
    public void testInspectHivePartitionInfo() throws Exception {
        Assert.assertThrows(StarRocksPlannerException.class,
                () -> testFragmentPlanContains("select inspect_hive_part_info('not_exist_catalog.no_db.no_table')",
                        ""));
        testFragmentPlanContains("select inspect_hive_part_info('hive0.partitioned_db.lineitem_par')", "Project");
    }

    @Test
    public void testInspect_inspect_mv_relationships() throws Exception {
        testFragmentPlanContains("select inspect_mv_relationships()", "Project");
    }

    @Test
    public void testDate() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select date_format('2020-02-19 16:01:12','%H%i');",
                "'1601'");

        testFragmentPlanContainsConstExpr(
                "select date_format('2020-02-19 16:01:12','%Y%m%d');",
                "'20200219'");

        testFragmentPlanContainsConstExpr(
                "select date_format(date_sub('2018-07-24 07:16:19',1),'yyyyMMdd');",
                "'20180723'");

        testFragmentPlanContainsConstExpr(
                "select year('2018-07-24')*12 + month('2018-07-24');",
                "24223");

        testFragmentPlanContainsConstExpr(
                "select date_format('2018-08-08 07:16:19', 'yyyyMMdd');",
                "'20180808'");

        testFragmentPlanContainsConstExpr(
                "select date_format('2018-08-08 07:16:19', 'yyyy-MM-dd HH:mm:ss');",
                "'2018-08-08 07:16:19'");

        testFragmentPlanContainsConstExpr(
                "select datediff('2018-08-08','1970-01-01');",
                "17751");

        testFragmentPlanContainsConstExpr(
                "select date_add('2018-08-08', 1);",
                "'2018-08-09 00:00:00'");

        testFragmentPlanContainsConstExpr(
                "select date_add('2018-08-08', -1);",
                "'2018-08-07 00:00:00'");

        testFragmentPlanContainsConstExpr(
                "select date_sub('2018-08-08 07:16:19',1);",
                "'2018-08-07 07:16:19'");

        testFragmentPlanContainsConstExpr(
                "select year('2018-07-24');",
                "2018");

        testFragmentPlanContainsConstExpr(
                "select month('2018-07-24');",
                "7");

        testFragmentPlanContainsConstExpr(
                "select day('2018-07-24');",
                "24");

        testFragmentPlanContainsConstExpr(
                "select UNIX_TIMESTAMP(\"1970-01-01 08:00:01\");",
                "1");

        testFragmentPlanContainsConstExpr(
                "select now();",
                "");

        testFragmentPlanContainsConstExpr(
                "select curdate();",
                "");
    }

    @Test
    public void testCast() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select cast ('1' as int) ;",
                "1");

        testFragmentPlanContainsConstExpr(
                "select cast ('2020-01-20' as date);",
                "'2020-01-20'");
    }

    @Test
    public void testCastToDecimalLiteral() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select cast(151971657 as decimal32);",
                "NULL");

        testFragmentPlanContainsConstExpr(
                "select cast('0.5' as decimal32);",
                "0.5");
    }

    @Test
    public void testArithmetic() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 + 10;",
                "11");

        testFragmentPlanContainsConstExpr(
                "select 1 - 10;",
                "-9");

        testFragmentPlanContainsConstExpr(
                "select 1 * 10.0;",
                "10");

        testFragmentPlanContainsConstExpr(
                "select 1 / 10.0;",
                "0.1");

        testFragmentPlanContainsConstExpr(
                "select 1 ^ 0;",
                "1");

        testFragmentPlanContainsConstExpr(
                "select 1 & 0;",
                "0");

        testFragmentPlanContainsConstExpr(
                "select 1 | 0;",
                "1");

        testFragmentPlanContainsConstExpr(
                "select 10 % 3;",
                "1");

        testFragmentPlanContainsConstExpr(
                "select 3 DIV 2;",
                "1");
    }

    @Test
    public void testDecimalArithmetic() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 * 10.0;",
                "10");

        testFragmentPlanContainsConstExpr(
                "select 1 + 10.0;",
                "11");

        testFragmentPlanContainsConstExpr(
                "select 1 - 10.0;",
                "-9");

        testFragmentPlanContainsConstExpr(
                "select cast('10.11' as DECIMAL(9,2)) + cast('120.34' as DECIMAL(9,2));",
                "130.45");

        testFragmentPlanContainsConstExpr(
                "select cast('10.11' as DECIMAL(9,2)) * cast('120.34' as DECIMAL(9,2));",
                "1216.6374");
    }

    @Test
    public void testDecimalArithmeticDivide() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 / 10.0;",
                "0.1");
    }

    @Test
    public void testMath() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select floor(2.3);",
                "2");
    }

    @Test
    public void testPredicate() throws Exception {
        testFragmentPlanContainsConstExpr(
                "select 1 > 2",
                "FALSE");

        testFragmentPlanContainsConstExpr(
                "select 1 = 1",
                "TRUE");
    }

    @Test
    public void testConstantInPredicate() throws Exception {
        connectContext.setDatabase("test");
        // for constant NOT IN PREDICATE
        testFragmentPlanContains("select 1 not in (1, 2);", "FALSE");

        testFragmentPlanContains("select 1 not in (2, 3);", "TRUE");

        testFragmentPlanContains("select 1 not in (2, null);", "NULL");

        testFragmentPlanContains("select 1 not in (1, 2, null);", "FALSE");

        testFragmentPlanContains("select null not in (1, 2);", "NULL");

        testFragmentPlanContains("select null not in (null);", "NULL");

        // for constant IN PREDICATE
        testFragmentPlanContains("select 1 in (1, 2);", "TRUE");

        testFragmentPlanContains("select 1 in (2, 3);", "FALSE");

        testFragmentPlanContains("select 1 in (1, 2, NULL);", "TRUE");

        testFragmentPlanContains("select 1 in (2, NULL);", "NULL");

        testFragmentPlanContains("select null in (2);", "NULL");

        testFragmentPlanContains("select null in (null);", "NULL");
    }

    @Test
    public void testSysVariable() throws Exception {
        String sql = "SELECT  @@session.auto_increment_increment AS auto_increment_increment," +
                "@@character_set_client AS character_set_client, " +
                "@@character_set_connection AS character_set_connection, " +
                "@@character_set_results AS character_set_results, " +
                "@@character_set_server AS character_set_server, " +
                "@@init_connect AS init_connect, " +
                "@@interactive_timeout AS interactive_timeout, " +
                "@@language AS language, " +
                "@@license AS license, " +
                "@@lower_case_table_names AS lower_case_table_names, " +
                "@@max_allowed_packet AS max_allowed_packet, " +
                "@@net_buffer_length AS net_buffer_length, " +
                "@@net_write_timeout AS net_write_timeout, " +
                "@@query_cache_size AS query_cache_size, " +
                "@@query_cache_type AS query_cache_type, " +
                "@@sql_mode AS sql_mode, " +
                "@@system_time_zone AS system_time_zone, " +
                "@@time_zone AS time_zone, " +
                "@@tx_isolation AS tx_isolation, " +
                "@@wait_timeout AS wait_timeout;";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains(
                "  |  <slot 2> : 1\n" +
                        "  |  <slot 3> : 'utf8'\n" +
                        "  |  <slot 4> : 'utf8'\n" +
                        "  |  <slot 5> : 'utf8'\n" +
                        "  |  <slot 6> : 'utf8'\n" +
                        "  |  <slot 7> : ''\n" +
                        "  |  <slot 8> : 3600\n" +
                        "  |  <slot 9> : '/starrocks/share/english/'\n" +
                        "  |  <slot 10> : 'Apache License 2.0'\n" +
                        "  |  <slot 11> : 0\n" +
                        "  |  <slot 12> : 33554432\n" +
                        "  |  <slot 13> : 16384\n" +
                        "  |  <slot 14> : 60\n" +
                        "  |  <slot 15> : 1048576\n" +
                        "  |  <slot 16> : 0\n" +
                        "  |  <slot 17> : 'ONLY_FULL_GROUP_BY'\n" +
                        "  |  <slot 18> : 'Asia/Shanghai'\n" +
                        "  |  <slot 19> : 'Asia/Shanghai'\n" +
                        "  |  <slot 20> : 'REPEATABLE-READ'\n" +
                        "  |  <slot 21> : 28800"
        ));
    }

    @Test(expected = ParsingException.class)
    public void testDoubleLiteral() throws Exception {
        String sql = "select 1e309";
        getFragmentPlan(sql);
    }

    @Test
    public void testRand() throws Exception {
        String sql = "select rand(), rand() from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 4> : rand()\n" +
                "  |  <slot 5> : rand()"));

        sql = "select rand(), rand()";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : rand()\n" +
                "  |  <slot 3> : rand()"));

        sql = "select rand()+1, rand()";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : rand() + 1.0\n" +
                "  |  <slot 3> : rand()"));

        sql = "select rand()+1, rand()+1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : rand() + 1.0\n" +
                "  |  <slot 3> : rand() + 1.0"));

        sql = "select (rand()+1)+1, (rand()+1)+1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : rand() + 1.0 + 1.0\n" +
                "  |  <slot 3> : rand() + 1.0 + 1.0"));

        sql = "select rand() from t0 where rand() > 0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  <slot 4> : rand()"));
        Assert.assertTrue(plan.contains("PREDICATES: rand() > 0.0"));

        sql = "select sleep(1), sleep(1), sleep(2)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : sleep(1)\n" +
                "  |  <slot 3> : sleep(1)\n" +
                "  |  <slot 4> : sleep(2)"));

        sql = "select random(), random()";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : random()\n" +
                "  |  <slot 3> : random()"));

        sql = "select uuid(), uuid()";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : uuid()\n" +
                "  |  <slot 3> : uuid()"));
    }

    @Test
    public void testNumericLiteralComparison() throws Exception {
        String sql;
        String plan;

        final long prevSqlMode = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(prevSqlMode | SqlModeHelper.MODE_DOUBLE_LITERAL);

            sql = "SELECT percentile_approx(2.25, 0), percentile_approx(2.25, 0.)";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:Project\n" +
                    "  |  <slot 2> : 2: percentile_approx\n" +
                    "  |  <slot 3> : clone(2: percentile_approx)\n" +
                    "  |  \n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: percentile_approx(2.25, 0.0)\n" +
                    "  |  group by: ");

            sql = "SELECT COUNT(CASE WHEN 1 THEN 1 END), COUNT(CASE WHEN TRUE THEN 1 END), " +
                    "COUNT(CASE WHEN 1.0 THEN 1 END), COUNT(CASE WHEN CAST(1 AS LARGEINT) THEN 1 END)";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:Project\n" +
                    "  |  <slot 2> : 2: count\n" +
                    "  |  <slot 3> : clone(2: count)\n" +
                    "  |  <slot 4> : clone(2: count)\n" +
                    "  |  <slot 5> : clone(2: count)\n" +
                    "  |  \n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: count(1)\n" +
                    "  |  group by: ");

            sql = "SELECT 1, TRUE, 0, FALSE, 1.1, 1, 1.1, TRUE, FALSE, 0";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:Project\n" +
                    "  |  <slot 7> : 1\n" +
                    "  |  <slot 8> : 1.1\n" +
                    "  |  <slot 9> : TRUE\n" +
                    "  |  <slot 10> : FALSE\n" +
                    "  |  <slot 11> : 0");

            sql = "SELECT TRUE, 1, FALSE, 0, 1.1, 1, 1.1, TRUE, FALSE, 0";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:Project\n" +
                    "  |  <slot 7> : 1\n" +
                    "  |  <slot 8> : 1.1\n" +
                    "  |  <slot 9> : TRUE\n" +
                    "  |  <slot 10> : FALSE\n" +
                    "  |  <slot 11> : 0");

        } finally {
            connectContext.getSessionVariable().setSqlMode(prevSqlMode);
        }
    }
}
