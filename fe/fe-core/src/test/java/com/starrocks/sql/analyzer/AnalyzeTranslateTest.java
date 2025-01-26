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

package com.starrocks.sql.analyzer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.TranslateExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeTranslateTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAnalyzeTranslate() {
        analyzeSuccess("translate trino select * from t1");
        analyzeSuccess("translate trino select v1,v2 from t1");
        analyzeSuccess("translate trino select to_unixtime()");

        analyzeFail("translate starrocks select * from t1 where v1 = 1");
        analyzeFail("translate hive select * from t1 where v1 = 1");
        analyzeFail("translate spark select * from t1 where v1 = 1");
    }

    @Test
    public void testHandleTranslateStmt() throws Exception {
        String sql = "translate trino select to_unixtime(TIMESTAMP '2023-04-22 00:00:00')";
        ConnectContext connectContext = AnalyzeTestUtil.getConnectContext();
        StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assert.assertFalse(connectContext.getState().isError());
        Assert.assertEquals(1, connectContext.getReturnRows());
    }

    private void assertTranslateTrinoSQL(String originSQL, String translatedSQL) {
        StatementBase parsedStmt = SqlParser.parse(originSQL, AnalyzeTestUtil.getConnectContext().getSessionVariable()).get(0);
        ShowResultSet resultSet = TranslateExecutor.execute((TranslateStmt) parsedStmt);
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertEquals(1, resultSet.getResultRows().get(0).size());
        Assert.assertEquals(translatedSQL, resultSet.getResultRows().get(0).get(0));
    }

    @Test
    public void testTranslateTrinoSQL() {
        assertTranslateTrinoSQL("translate trino select to_unixtime(TIMESTAMP '2023-04-22 00:00:00')",
                "SELECT unix_timestamp('2023-04-22 00:00:00')");
        assertTranslateTrinoSQL("translate trino select day_of_week(timestamp '2022-03-06 01:02:03')",
                "SELECT dayofweek_iso('2022-03-06 01:02:03')");
        assertTranslateTrinoSQL("translate trino SELECT date_parse('20141221','%Y%m%d')",
                "SELECT str_to_date('20141221', '%Y%m%d')");

        assertTranslateTrinoSQL("translate trino select approx_percentile(v1, 0.99) from t0",
                "SELECT percentile_approx(`v1`, 0.99)\nFROM `t0`");
        assertTranslateTrinoSQL("translate trino select arbitrary(v1) from t0", "SELECT any_value(`v1`)\n" +
                "FROM `t0`");
        assertTranslateTrinoSQL("translate trino select stddev(v1) from t0;", "SELECT stddev_samp(`v1`)\n" +
                "FROM `t0`");

        assertTranslateTrinoSQL("translate trino select array_union(c1, c2) from test_array",
                "SELECT array_distinct(array_concat(`c1`, `c2`))\nFROM `test_array`");

        assertTranslateTrinoSQL("translate trino select date_add('day', 1, TIMESTAMP '2014-03-08 09:00:00')",
                "SELECT date_add('2014-03-08 09:00:00', INTERVAL 1 day)");

        assertTranslateTrinoSQL("translate trino select chr(56)", "SELECT char(56)");

        assertTranslateTrinoSQL("translate trino select approx_set(\"tc\") from tall",
                "SELECT hll_hash(`tc`)\nFROM `tall`");

        // test TPCH query
        assertTranslateTrinoSQL("translate trino select\n" +
                "  o_orderpriority,\n" +
                "  count(*) as order_count\n" +
                "from\n" +
                "  orders\n" +
                "where\n" +
                "  o_orderdate >= date '1993-07-01'\n" +
                "  and o_orderdate < date '1993-07-01' + interval '3' month\n" +
                "  and exists (\n" +
                "    select\n" +
                "      *\n" +
                "    from\n" +
                "      lineitem\n" +
                "    where\n" +
                "      l_orderkey = o_orderkey\n" +
                "      and l_commitdate < l_receiptdate\n" +
                "  )\n" +
                "group by\n" +
                "  o_orderpriority\n" +
                "order by\n" +
                "  o_orderpriority;", "SELECT `o_orderpriority`, count(*) AS `order_count`\n" +
                "FROM `orders`\n" +
                "WHERE ((`o_orderdate` >= '1993-07-01') AND (`o_orderdate` < ('1993-07-01' + INTERVAL '3' MONTH))) AND " +
                "(EXISTS (SELECT *\n" +
                "FROM `lineitem`\n" +
                "WHERE (`l_orderkey` = `o_orderkey`) AND (`l_commitdate` < `l_receiptdate`)))\n" +
                "GROUP BY `o_orderpriority` ORDER BY `o_orderpriority` ASC ");

    }

    @Test
    public void testParsedTranslateSQL() {
        String sql = "translate trino select \nto_unixtime(\nTIMESTAMP '2023-04-22 00:00:00'\n)";
        TranslateStmt parsedStmt = (TranslateStmt) SqlParser.parse(sql,
                AnalyzeTestUtil.getConnectContext().getSessionVariable()).get(0);
        Assert.assertEquals("select\nto_unixtime(\nTIMESTAMP '2023-04-22 00:00:00'\n)",
                parsedStmt.getTranslateSQL());
    }
}
