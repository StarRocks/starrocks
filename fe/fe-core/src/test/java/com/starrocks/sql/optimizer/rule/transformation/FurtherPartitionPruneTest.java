package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class FurtherPartitionPruneTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `ptest` (\n"
                + "  `k1` int(11) NOT NULL COMMENT \"\",\n"
                + "  `d2` date NOT NULL COMMENT \"\",\n"
                + "  `v1` int(11) NULL COMMENT \"\",\n"
                + "  `v2` int(11) NULL COMMENT \"\",\n"
                + "  `v3` int(11) NULL COMMENT \"\",\n"
                + "  `s1` varchar(255) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `d2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`d2`)\n"
                + "(PARTITION p202001 VALUES [('1000-01-01'), ('2020-01-01')),\n"
                + "PARTITION p202004 VALUES [('2020-01-01'), ('2020-04-01')),\n"
                + "PARTITION p202007 VALUES [('2020-04-01'), ('2020-07-01')),\n"
                + "PARTITION p202012 VALUES [('2020-07-01'), ('2020-12-01')))\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ");");
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("emptyPartitionSqlList")
    public void testEmptyPartitionSql(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertTrue(plan, plan.contains("0:EMPTYSET"));
    }


    private static Stream<Arguments> emptyPartitionSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from ptest where d2 <=> null");
        sqlList.add("select * from ptest where d2 in (null, '2021-01-01')");
        sqlList.add("select * from ptest where (d2 < '1000-01-01') or (d2 in (null, '2021-01-01'))");
        sqlList.add("select * from ptest where d2 not in ('2021-12-01', null)");
        sqlList.add("select * from ptest where d2 < '1000-01-01' and d2 >= '2020-12-01'");
        sqlList.add("select * from ptest where d2 not between '1000-01-01' and '2020-12-01'");
        sqlList.add("select * from ptest where d2 = '1000-01-01' and d2 not between '1000-01-01' and '2020-12-01'");
        sqlList.add("select * from ptest where s1 not like '1000-01-01'");
        return sqlList.stream().map(e -> Arguments.of(e));
    }
}
