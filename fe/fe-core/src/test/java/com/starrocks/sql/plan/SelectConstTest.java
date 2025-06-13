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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.scheduler.FeExecuteCoordinator;
import com.starrocks.thrift.TResultBatch;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class SelectConstTest extends PlanTestBase {
    @Test
    public void testSelectConst() throws Exception {
        assertPlanContains("select 1,2", "  1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select a from (select 1 as a, 2 as b) t", "1:Project\n" +
                "  |  <slot 4> : 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 union all select 1,2", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 union select 1,2", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 except select 1,2", "EXCEPT", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 intersect select 1,2", "INTERSECT", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2,b from t0 inner join (select 1 as a,2 as b) t on v1 = a", "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 7> : 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 1");
    }

    @Test
    public void testValuesNodePredicate() throws Exception {
        assertPlanContains("select database()", "<slot 2> : 'test'");
        assertPlanContains("select schema()", "<slot 2> : 'test'");
        assertPlanContains("select user()", "<slot 2> : '\\'root\\'@%'");
        assertPlanContains("select current_user()", "<slot 2> : '\\'root\\'@\\'%\\''");
        assertPlanContains("select connection_id()", "<slot 2> : 0");
        assertPlanContains("select current_role()", "<slot 2> : 'root'");
    }

    @Test
    public void testFromUnixtime() throws Exception {
        assertPlanContains("select from_unixtime(10)", "'1970-01-01 08:00:10'");
        assertPlanContains("select from_unixtime(1024)", "'1970-01-01 08:17:04'");
        assertPlanContains("select from_unixtime(32678)", "'1970-01-01 17:04:38'");
        assertPlanContains("select from_unixtime(102400000)", "'1973-03-31 12:26:40'");
        assertPlanContains("select from_unixtime(253402243100)", "'9999-12-31 15:58:20'");
    }

    @Test
    public void testAggWithConstant() throws Exception {
        assertPlanContains("select case when c1=1 then 1 end from (select '1' c1  union  all select '2') a " +
                        "group by rollup(case  when c1=1 then 1 end, 1 + 1)",
                "<slot 6> : if(5: expr = '1', 1, NULL)",
                "<slot 7> : 2",
                "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[], [6], [6, 7]]\n" +
                        "  |  \n" +
                        "  1:Project\n" +
                        "  |  <slot 6> : if(5: expr = '1', 1, NULL)\n" +
                        "  |  <slot 7> : 2");
    }

    @Test
    public void testSubquery() throws Exception {
        assertPlanContains("select * from t0 where v3 in (select 2)", "LEFT SEMI JOIN", "<slot 7> : 2");
        assertPlanContains("select * from t0 where v3 not in (select 2)", "NULL AWARE LEFT ANTI JOIN",
                "<slot 7> : 2");
        assertPlanContains("select * from t0 where exists (select 9)", "  1:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select * from t0 where exists (select 9,10)", "  1:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select * from t0 where not exists (select 9)", ":UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select * from t0 where v3 = (select 6)", "  5:Project\n" +
                "  |  <slot 7> : CAST(5: expr AS BIGINT)", "equal join conjunct: 3: v3 = 7: cast");
        assertPlanContains("select case when (select max(v4) from t1) > 1 then 2 else 3 end", "  7:Project\n" +
                "  |  <slot 7> : if(5: max > 1, 2, 3)\n" +
                "  |  \n" +
                "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select 1, 2, case when (select max(v4) from t1) > 1 then 4 else 5 end", "  7:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  <slot 9> : if(7: max > 1, 4, 5)\n" +
                "  |  \n" +
                "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testDoubleCastWithoutScientificNotation() throws Exception {
        String sql = "SELECT * FROM t0 WHERE CAST(CAST(CASE WHEN TRUE THEN -1229625855 " +
                "WHEN false THEN 1 ELSE 2 / 3 END AS STRING ) AS BOOLEAN );";
        assertPlanContains(sql, "PREDICATES: CAST('-1229625855' AS BOOLEAN)");
    }

    @Test
    public void testSystemVariable() throws Exception {
        String sql = "SELECT @@session.auto_increment_increment, @@character_set_client, @@character_set_connection, " +
                "@@character_set_results, @@character_set_server, @@collation_server, @@collation_connection, " +
                "@@init_connect, @@interactive_timeout, @@language, @@license, @@lower_case_table_names, @@max_allowed_packet, " +
                "@@net_write_timeout, @@performance_schema, @@query_cache_size, @@query_cache_type, @@sql_mode, " +
                "@@system_time_zone, @@time_zone, @@tx_isolation, @@wait_timeout";
        String plan = getFragmentPlan(sql);
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 'utf8'\n" +
                "  |  <slot 4> : 'utf8'\n" +
                "  |  <slot 5> : 'utf8'\n" +
                "  |  <slot 6> : 'utf8'\n" +
                "  |  <slot 7> : 'utf8_general_ci'\n" +
                "  |  <slot 8> : 'utf8_general_ci'\n" +
                "  |  <slot 9> : ''\n" +
                "  |  <slot 10> : 3600\n" +
                "  |  <slot 11> : '/starrocks/share/english/'\n" +
                "  |  <slot 12> : 'Apache License 2.0'\n" +
                "  |  <slot 13> : 0\n" +
                "  |  <slot 14> : 33554432\n" +
                "  |  <slot 15> : 60\n" +
                "  |  <slot 16> : FALSE\n" +
                "  |  <slot 17> : 1048576\n" +
                "  |  <slot 18> : 0\n" +
                "  |  <slot 19> : 'ONLY_FULL_GROUP_BY'\n" +
                "  |  <slot 20> : 'Asia/Shanghai'\n" +
                "  |  <slot 21> : 'Asia/Shanghai'\n" +
                "  |  <slot 22> : 'REPEATABLE-READ'\n" +
                "  |  <slot 23> : 28800");
    }

    @Test
    public void testExecuteInFe() throws Exception {
        assertFeExecuteResult("select -1", "-1");
        assertFeExecuteResult("select -123456.789", "-123456.789");
        assertFeExecuteResult("select 100000000000000", "100000000000000");
        assertFeExecuteResult("select cast(0.00001 as float)", "1.0e-5");
        assertFeExecuteResult("select cast(0.00000000000001 as double)", "1.0e-14");
        assertFeExecuteResult("select '2021-01-01'", "2021-01-01");
        assertFeExecuteResult("select '2021-01-01 01:01:01.1234'", "2021-01-01 01:01:01.1234");
        assertFeExecuteResult("select cast(1.23456000 as decimalv2)", "1.23456");
        assertFeExecuteResult("select cast(1.23456000 as DECIMAL(10, 2))", "1.23");
        assertFeExecuteResult("select cast(1.234560 as DECIMAL(12, 10))", "1.2345600000");
        assertFeExecuteResult("select '\\'abc'", "'abc");
        assertFeExecuteResult("select '\"abc'", "\"abc");
        assertFeExecuteResult("select '\\\\\\'abc'", "\\'abc");
        assertFeExecuteResult("select timediff('1000-01-02 01:01:01.123456', '1000-01-01 01:01:01.000001')",
                "24:00:00");
        assertFeExecuteResult("select timediff('9999-01-02 01:01:01.123456', '1000-01-01 01:01:01.000001')",
                "78883632:00:00");
        assertFeExecuteResult("select timediff('1000-01-01 01:01:01.000001', '9999-01-02 01:01:01.123456')",
                "-78883632:00:01");
    }

    @Test
    public void testExecuteInFEWithComplexQuery() throws Exception {
        String sql = "select 1, -1, 1.23456, cast(1.123 as float), cast(1.123 as double), " +
                "cast(10 as bigint), cast(100 as largeint),\n" +
                "1000000000000, 1+1, 100 * 100, 'abc', \"中文\", '\"abc\"', " +
                "\"'abc'\", '\\'abc\\\\', \"\\\"abc\\\\\", cast(1.123000000 as decimalv2),\n" +
                "cast(1.123 as decimal(10, 7)), date '2021-01-01', " +
                "datetime '2021-01-01 00:00:00', datetime '2021-01-01 00:00:00.123456',\n" +
                "timediff('2028-01-01 11:25:36', '2000-11-21 12:12:12'), " +
                "timediff('2000-11-21 12:12:12', '2028-01-01 11:25:36'), x'123456', x'AABBCC11';";
        ExecPlan execPlan = getExecPlan(sql);
        FeExecuteCoordinator coordinator = new FeExecuteCoordinator(connectContext, execPlan);
        try {
            RowBatch rowBatch = coordinator.getNext();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private void assertFeExecuteResult(String sql, String expected) throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        FeExecuteCoordinator coordinator = new FeExecuteCoordinator(connectContext, execPlan);
        RowBatch rowBatch = coordinator.getNext();
        TResultBatch tResultBatch = rowBatch.getBatch();
        if (tResultBatch.rows.isEmpty()) {
            Assert.assertNull(expected);
        } else {
            byte[] bytes = tResultBatch.getRows().get(0).array();
            int lengthOffset = getOffset(bytes);
            String value;
            if (lengthOffset == -1) {
                value = "NULL";
            } else {
                value = new String(bytes, lengthOffset, bytes.length - lengthOffset, StandardCharsets.UTF_8);
            }
            System.out.println(value);
            Assert.assertEquals(expected, value);
        }
    }

    private static int getOffset(byte[] bytes) {
        int sw = bytes[0] & 0xff;
        switch (sw) {
            case 251:
                return -1;
            case 252:
                return 3;
            case 253:
                return 4;
            case 254:
                return 9;
            default:
                return 1;
        }
    }

    @Test
    public void testEmptyScan() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tbl0 (\n" +
                "  c1 int,\n" +
                "  c2 date,\n" +
                "  c3 varchar(10),\n" +
                "  c4 bigint,\n" +
                "  c5 varchar(3),\n" +
                "  c6 datetime,\n" +
                "  c7 string,\n" +
                "  c8 decimal(10,5),\n" +
                "  c9 boolean,\n" +
                "  c10 largeint,\n" +
                "  c11 date,\n" +
                "  c12 float,\n" +
                "  c13 double)\n" +
                "  PRIMARY KEY(c1,c2)\n" +
                "  DISTRIBUTED BY HASH(c1) BUCKETS 3\n" +
                "PROPERTIES ('replication_num' = '1');");
        String sql = "select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13," +
                "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 " +
                "from tbl0;";
        FeConstants.enablePruneEmptyOutputScan = true;
        assertFeExecuteResult(sql, null);
        FeConstants.enablePruneEmptyOutputScan = false;
    }

    @Test
    public void testUnionWithUnAlignedValues() throws Exception {
        String sql = "WITH temp AS (\n" +
                "  SELECT 'test' AS c1, 'test' AS c2, 'test' AS c3\n" +
                "  UNION ALL\n" +
                "  SELECT 'test' AS c1, 'test' AS c2, 'test' AS c3\n" +
                " )\n" +
                "SELECT c1, c2, c3\n" +
                "FROM (\n" +
                " SELECT c1, c2, c3\n" +
                " FROM temp\n" +
                " UNION ALL\n" +
                " SELECT 'test1' AS c1, 'test1' AS c2, 'test1' AS c3\n" +
                " UNION ALL\n" +
                " SELECT 'test1' AS c1, 'test2' AS c2, 'test3' AS c3\n" +
                ") t;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "     constant exprs: \n" +
                        "         'test1' | 'test1' | 'test1'\n" +
                        "         'test1' | 'test2' | 'test3'",
                "     constant exprs: \n" +
                        "         'test' | 'test' | 'test'\n" +
                        "         'test' | 'test' | 'test'");
    }
}
