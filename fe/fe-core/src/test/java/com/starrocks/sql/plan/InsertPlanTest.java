// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.plan;

import com.starrocks.analysis.SqlParser;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.stream.Stream;

public class InsertPlanTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        String createMVSQL = "create materialized view mvs as select v1,sum(v2) from test_insert_mv_sum group by v1";
        starRocksAssert.withMaterializedView(createMVSQL);

        String explainString = getInsertExecPlan("insert into test_insert_mv_sum values(1,2,3)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: expr | 2: expr | 3: expr"));

        explainString = getInsertExecPlan("insert into test_insert_mv_sum(v1) values(1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: expr | 2: expr | 3: expr"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: expr\n" +
                        "  |  <slot 2> : NULL\n" +
                        "  |  <slot 3> : NULL"));

        explainString = getInsertExecPlan("insert into test_insert_mv_sum(v3,v1) values(3,1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:2: expr | 3: expr | 1: expr"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: expr\n" +
                        "  |  <slot 2> : 2: expr\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        String createMVSQL =
                "create materialized view mvc as select v1,count(v2) from test_insert_mv_count group by v1";
        starRocksAssert.withMaterializedView(createMVSQL);

        String explainString = getInsertExecPlan("insert into test_insert_mv_count values(1,2,3)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: expr | 2: expr | 3: expr | 4: if\n"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: expr\n" +
                        "  |  <slot 2> : 2: expr\n" +
                        "  |  <slot 3> : 3: expr\n" +
                        "  |  <slot 4> : if(2: expr IS NULL, 0, 1)"));

        explainString = getInsertExecPlan("insert into test_insert_mv_count(v1) values(1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: expr | 2: expr | 3: expr | 4: if"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: expr\n" +
                        "  |  <slot 2> : NULL\n" +
                        "  |  <slot 3> : NULL\n" +
                        "  |  <slot 4> : if(NULL IS NULL, 0, 1)"));

        System.out.println(explainString);
        explainString = getInsertExecPlan("insert into test_insert_mv_count(v3,v1) values(3,1)");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:2: expr | 3: expr | 1: expr | 4: if"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: expr\n" +
                        "  |  <slot 2> : 2: expr\n" +
                        "  |  <slot 3> : NULL\n" +
                        "  |  <slot 4> : if(NULL IS NULL, 0, 1)"));

        explainString = getInsertExecPlan("insert into test_insert_mv_count select 1,2,3");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: expr | 2: expr | 3: expr | 4: if"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: expr\n" +
                        "  |  <slot 2> : 2: expr\n" +
                        "  |  <slot 3> : 3: expr\n" +
                        "  |  <slot 4> : if(2: expr IS NULL, 0, 1)"));

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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        String explainString = getInsertExecPlan("insert into ti1 select * from ti2");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3"));

        explainString = getInsertExecPlan("insert into ti1(v1,v3) select v2,v1 from ti2");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:2: v2 | 4: expr | 1: v1"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: v1\n" +
                        "  |  <slot 2> : 2: v2\n" +
                        "  |  <slot 4> : NULL\n" +
                        "  |  use vectorized: true"));

        String createMVSQL =
                "create materialized view mvb as select v1,bitmap_union(to_bitmap(v2)) from ti2 group by v1";
        starRocksAssert.withMaterializedView(createMVSQL);

        explainString = getInsertExecPlan("insert into ti2(v2,v1,v3) select v1,2,NULL from ti1");
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:7: v1 | 1: v1 | 8: v3 | 6: to_bitmap"));
        Assert.assertTrue(explainString.contains(
                "  |  <slot 1> : 1: v1\n" +
                        "  |  <slot 6> : to_bitmap(CAST(1: v1 AS VARCHAR))\n" +
                        "  |  <slot 7> : CAST(2 AS BIGINT)\n" +
                        "  |  <slot 8> : CAST(NULL AS BIGINT)\n" +
                        "  |  use vectorized: true"));
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
                "PROPERTIES ( \"replication_num\" = \"1\", \"storage_format\" = \"v2\" );");
        String sql = "insert into duplicate_table_with_default values " +
                "('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, default, default, -18446744073709550633, default, default, -2.18);";
        String explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("constant exprs: \n" +
                "         '2020-06-25' | '2020-06-25 00:16:23' | 'beijing' | 'haidian' | FALSE | 87 | -31785 | 0 | 0 | -18446744073709550633 | -1.0 | 0.0 | -2.18"));

        // check multi rows value
        sql = "insert into duplicate_table_with_default values " +
                "('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, default, default, -18446744073709550633, default, default, -2.18), " +
                "(default, '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, default, default, -18446744073709550633, default, default, -2.18)";
        explainString = getInsertExecPlan(sql);
        Assert.assertTrue(explainString.contains("constant exprs: \n" +
                "         '2020-06-25' | '2020-06-25 00:16:23' | 'beijing' | 'haidian' | FALSE | 87 | -31785 | 0 | 0 | -18446744073709550633 | -1.0 | 0.0 | -2.18\n" +
                "         '1970-01-01' | '2020-06-25 00:16:23' | 'beijing' | 'haidian' | FALSE | 87 | -31785 | 0 | 0 | -18446744073709550633 | -1.0 | 0.0 | -2.18"));

        sql = "insert into duplicate_table_with_default values " +
                "('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', default, 87, -31785, default, default, -18446744073709550633, default, default, -2.18);";
        try {
            getInsertExecPlan(sql);
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().equals("Column has no default value, column=k5"));
        }
    }

    public static String getInsertExecPlan(String originStmt) throws Exception {
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext.getSessionVariable()));
        SqlScanner input =
                new SqlScanner(new StringReader(originStmt), connectContext.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);

        String ret = execPlan.getExplainString(TExplainLevel.NORMAL);
        //        System.out.println(ret);
        return ret;
    }

    public static void containsKeywords(String plan, String... keywords) throws Exception {
        Assert.assertTrue(Stream.of(keywords).allMatch(plan::contains));
    }

    @Test
    public void testBitmapInsertInto() throws Exception {
        String sql = "INSERT INTO test.bitmap_table (id, id2) VALUES (1001, to_bitmap(1000)), (1001, to_bitmap(2000));";
        String plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: expr | 2: to_bitmap", "OLAP TABLE SINK",
                "constant exprs:", "1001 | to_bitmap('1000')", "1001 | to_bitmap('2000')");

        sql = "insert into test.bitmap_table select id, bitmap_union(id2) from test.bitmap_table_2 group by id;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 3: bitmap_union(2: id2)",
                "OLAP TABLE SINK", "1:AGGREGATE (update finalize)", "output: bitmap_union(2: id2)", "0:OlapScanNode");

        sql = "insert into test.bitmap_table select id, id2 from test.bitmap_table_2;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 2: id2", "OLAP TABLE SINK", "0:OlapScanNode");

        sql = "insert into test.bitmap_table select id, to_bitmap(id2) from test.bitmap_table_2;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 3: to_bitmap", "OLAP TABLE SINK",
                "1:Project", "<slot 1> : 1: id", "<slot 3> : to_bitmap(CAST(2: id2 AS VARCHAR))", "0:OlapScanNode",
                "TABLE: bitmap_table_2");

        sql = "insert into test.bitmap_table select id, bitmap_hash(id2) from test.bitmap_table_2;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 3: bitmap_hash", "OLAP TABLE SINK",
                "1:Project", "<slot 1> : 1: id", "<slot 3> : bitmap_hash(CAST(2: id2 AS VARCHAR))");

        sql = "insert into test.bitmap_table select id, id from test.bitmap_table_2;";
        plan = getInsertExecPlan(sql);
        containsKeywords(plan, "OUTPUT EXPRS:1: id | 3: id2", "OLAP TABLE SINK",
                "1:Project", "<slot 1> : 1: id", "<slot 3> : CAST(1: id AS BITMAP)");
    }

    @Test
    public void testInsertWithColocate() throws Exception {
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: distinct_id = 49: distinct_id"));
        Assert.assertTrue(plan.contains("  7:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (COLOCATE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: distinct_id = 45: distinct_id"));
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (COLOCATE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: distinct_id = 41: distinct_id"));
    }
}
