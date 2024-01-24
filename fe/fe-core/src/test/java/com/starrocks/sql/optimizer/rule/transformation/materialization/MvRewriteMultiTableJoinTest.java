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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteMultiTableJoinTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        prepareDatas();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        try {
            starRocksAssert.dropTable("tbl_1");
            starRocksAssert.dropTable("tbl_2");
            starRocksAssert.dropTable("tbl_3");
            starRocksAssert.dropTable("tbl_4");
        } catch (Exception e) {
            // ignore exceptions.
        }
    }

    public static void prepareDatas() throws Exception {
        starRocksAssert.withTable("" +
                "CREATE TABLE tbl_1 (\n" +
                " dt date NULL COMMENT \"etl\",\n" +
                " p1_col1 varchar(60) NULL COMMENT \"\",\n" +
                " p1_col2 varchar(240) NULL COMMENT \"\",\n" +
                " p1_col3 varchar(30) NULL COMMENT \"\",\n" +
                " p1_col4 decimal128(22, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(dt, p1_col1)\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(PARTITION p20221230 VALUES [(\"2022-12-30\"), (\"2022-12-31\")),\n" +
                "PARTITION p20230331 VALUES [(\"2023-03-31\"), (\"2023-04-01\")))\n" +
                "DISTRIBUTED BY HASH(dt, p1_col2) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"" +
                ");");

        starRocksAssert.withTable("CREATE TABLE tbl_2 (\n" +
                " start_dt date NULL COMMENT \"\",\n" +
                " end_dt date NULL COMMENT \"\",\n" +
                " p2_col1 varchar(60) NULL COMMENT \"\",\n" +
                " p2_col2 varchar(240) NULL COMMENT \"\",\n" +
                " p2_col3 varchar(60) NULL COMMENT \"\",\n" +
                " p2_col4 varchar(90) NULL COMMENT \"\",\n" +
                " p2_col5 varchar(90) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(start_dt, end_dt)\n" +
                "DISTRIBUTED BY HASH(start_dt, end_dt, p2_col1, p2_col2) BUCKETS 50 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"" +
                ");");

        starRocksAssert.withTable("\n" +
                "CREATE TABLE tbl_3 (\n" +
                " dt date NULL COMMENT \"\",\n" +
                " p3_col1 varchar(900) NULL COMMENT \"\",\n" +
                " p3_col2 varchar(240) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(dt, p3_col1)\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(PARTITION p20191230 VALUES [(\"2019-12-30\"), (\"2019-12-31\")),\n" +
                "PARTITION p20200131 VALUES [(\"2020-01-31\"), (\"2020-02-01\")),\n" +
                "PARTITION p20200229 VALUES [(\"2020-02-29\"), (\"2020-03-01\")))\n" +
                "DISTRIBUTED BY HASH(dt, p3_col2) BUCKETS 25 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"" +
                ");");

        starRocksAssert.withTable("\n" +
                "CREATE TABLE tbl_4 (\n" +
                " dt date NULL COMMENT \"\",\n" +
                " p4_col1 varchar(240) NULL COMMENT \"\",\n" +
                " p4_col2 varchar(240) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(dt, p4_col1)\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(PARTITION p202212 VALUES [(\"2022-12-01\"), (\"2023-01-01\")),\n" +
                "PARTITION p202301 VALUES [(\"2023-01-01\"), (\"2023-02-01\")),\n" +
                "PARTITION p202302 VALUES [(\"2023-02-01\"), (\"2023-03-01\")))\n" +
                "DISTRIBUTED BY HASH(dt, p4_col2) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\",\n" +
                "\"in_memory\"=\"false\",\n" +
                "\"storage_format\"=\"DEFAULT\",\n" +
                "\"enable_persistent_index\"=\"false\",\n" +
                "\"compression\"=\"LZ4\"\n" +
                ")");
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1 \n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY HASH(dt, p1_col2) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ")\n" +
                "AS SELECT p1.p1_col3, p1.p1_col2, p1.dt, sum(p1.p1_col4) AS sum_p1_col4\n" +
                "FROM tbl_1 AS p1\n" +
                "GROUP BY 1, 2, 3;");
    }

    @Test
    public void testPartitionPrune1() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv2\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY HASH(dt, p1_col2) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ")\n" +
                "AS SELECT " +
                "p1.dt, p1.p1_col1, p1.p1_col2, p1.p1_col3, " +
                "p2.p2_col2, p2.p2_col4, " +
                "p3.p3_col1, p4.p4_col2, p4.p4_col1, " +
                "p5.sum_p1_col4, " +
                "sum(p1.p1_col4) AS p1_col4\n" +
                "FROM " +
                "tbl_1 AS p1 " +
                "INNER JOIN test_mv1 AS p5 " +
                "   ON p1.p1_col2=p5.p1_col2 and p1.dt=p5.dt and p1.p1_col3=p5.p1_col3\n" +
                "LEFT OUTER JOIN tbl_2 AS p2 " +
                "   ON p2.p2_col1='1' AND p1.p1_col2=p2.p2_col2 " +
                "   AND p2.start_dt <= p1.dt AND p2.end_dt > p1.dt " +
                "LEFT OUTER JOIN tbl_3 AS p3 ON p1.p1_col2=p3.p3_col2 AND p3.dt=p1.dt " +
                "LEFT OUTER JOIN tbl_4 AS p4 ON p1.p1_col1=p4.p4_col1 AND p4.dt=p1.dt\n" +
                "GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;");
        {
            String query = "select " +
                    " p1.p1_col2, p1.p1_col1, p3.p3_col1 \n" +
                    " ,p2.p2_col2, p2.p2_col4 \n" +
                    " ,p4.p4_col2 \n" +
                    " ,SUM(p1.p1_col4) as cvt_rmb_txn_amt \n" +
                    " from tbl_1 p1 \n" +
                    " inner join \n" +
                    " ( select p1_col2 , p1.dt, p1.p1_col3\n" +
                    " from tbl_1 p1 \n" +
                    " group by 1, 2, 3\n" +
                    " having sum(p1.p1_col4) >= 500000 \n" +
                    " ) p5 \n" +
                    " on p1.p1_col2=p5.p1_col2 and p1.dt=p5.dt and p1.p1_col3=p5.p1_col3\n" +
                    " left join tbl_2 p2 on p2.p2_col1 ='1' and p1.p1_col2 = p2.p2_col2 \n" +
                    " and p2.start_dt <= p1.dt and p2.end_dt > p1.dt \n" +
                    " left join tbl_3 p3 on p1.p1_col2 = p3.p3_col2 and p3.dt=p1.dt \n" +
                    " left join tbl_4 p4 on p1.p1_col1=p4.p4_col1 and p4.dt=p1.dt \n" +
                    " where p1.p1_col3 = '02' and p1.dt='2023-03-31' and p4.p4_col2='200105085'\n" +
                    " group by 1,2,3,4,5,6 \n" +
                    " order by p1.p1_col2\n" +
                    " limit 0, 100";

            String plan = getFragmentPlan(query, "MV");
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "test_mv2");
            PlanTestBase.assertContains(plan, "sum_p1_col4 >= 500000");
        }

        {
            String query = "select " +
                    " p1.p1_col2,p1.p1_col1, p3.p3_col1 \n" +
                    " ,p2.p2_col2, p2.p2_col4 \n" +
                    " ,p4.p4_col2 \n" +
                    " ,SUM(p1.p1_col4) as cvt_rmb_txn_amt \n" +
                    " from tbl_1 p1 \n" +
                    " inner join \n" +
                    " ( select p1_col2 , p1.dt \n" +
                    " from tbl_1 p1 \n" +
                    " where p1.p1_col3 = '02' \n" +
                    " group by 1, 2\n" +
                    " having sum(p1.p1_col4) >= 500000 \n" +
                    " ) p5 \n" +
                    " on p1.p1_col2=p5.p1_col2 and p1.dt=p5.dt \n" +
                    " left join tbl_2 p2 on p2.p2_col1 ='1' and p1.p1_col2 = p2.p2_col2 \n" +
                    " and p2.start_dt <= p1.dt and p2.end_dt > p1.dt \n" +
                    " left join tbl_3 p3 on p1.p1_col2 = p3.p3_col2 and p3.dt = p1.dt \n" +
                    " left join tbl_4 p4 on p1.p1_col1=p4.p4_col1 and p4.dt  = p1.dt \n" +
                    " where p1.p1_col3 = '02' and p1.dt = '2023-03-31' and p4.p4_col2 = '200105085'\n" +
                    " group by 1,2,3,4,5,6 \n" +
                    " order by p1.p1_col2\n" +
                    " limit 0, 100";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "rollup: test_mv2");
        }

        // TODO: support pull `p1.dt = '2023-03-31'` up to join's on predicate.
        {
            String query = "select " +
                    " p1.p1_col2,p1.p1_col1, p3.p3_col1 \n" +
                    " ,p2.p2_col2, p2.p2_col4 \n" +
                    " ,p4.p4_col2 \n" +
                    " ,SUM(p1.p1_col4) as cvt_rmb_txn_amt \n" +
                    " from tbl_1 p1 \n" +
                    " inner join \n" +
                    " ( select p1_col2  \n" +
                    " from tbl_1 p1 \n" +
                    " where p1.p1_col3 = '02' and p1.dt='2023-03-31' \n" +
                    " group by 1\n" +
                    " having sum(p1.p1_col4) >= 500000 \n" +
                    " ) p5 \n" +
                    " on p1.p1_col2=p5.p1_col2 \n" +
                    " left join tbl_2 p2 on p2.p2_col1 ='1' and p1.p1_col2 = p2.p2_col2 \n" +
                    " and p2.start_dt <= p1.dt and p2.end_dt > p1.dt \n" +
                    " left join tbl_3 p3 on p1.p1_col2 = p3.p3_col2 and p3.dt = '2023-03-31' \n" +
                    " left join tbl_4 p4 on p1.p1_col1=p4.p4_col1 and p4.dt  = '2023-03-31' \n" +
                    " where p1.p1_col3 = '02' and p1.dt = '2023-03-31' and p4.p4_col2 = '200105085'\n" +
                    " group by 1,2,3,4,5,6 \n" +
                    " order by p1.p1_col2\n" +
                    " limit 0, 100";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            // TODO: support deduce ec from scan's predicates
            PlanTestBase.assertNotContains(plan, "test_mv2");
        }
        starRocksAssert.dropMaterializedView("test_mv2");
    }

    @Test
    public void testPartitionPrune2() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv2\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY HASH(dt, p1_col2) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ")\n" +
                "AS SELECT " +
                "p1.dt, p1.p1_col1, p1.p1_col2, p1.p1_col3, " +
                "p2.p2_col2, p2.p2_col4, " +
                "p3.p3_col1, p4.p4_col2, p4.p4_col1, " +
                "p5.p1_col3 as p5_col3, p5.sum_p1_col4, " +
                "sum(p1.p1_col4) AS p1_col4\n" +
                "FROM " +
                "tbl_1 AS p1 " +
                "INNER JOIN test_mv1 AS p5 " +
                "   ON p1.p1_col2=p5.p1_col2 and p1.dt=p5.dt \n" +
                "LEFT OUTER JOIN tbl_2 AS p2 " +
                "   ON p2.p2_col1='1' AND p1.p1_col2=p2.p2_col2 " +
                "   AND p2.start_dt <= p1.dt AND p2.end_dt > p1.dt " +
                "LEFT OUTER JOIN tbl_3 AS p3 ON p1.p1_col2=p3.p3_col2 AND p3.dt=p1.dt " +
                "LEFT OUTER JOIN tbl_4 AS p4 ON p1.p1_col1=p4.p4_col1 AND p4.dt=p1.dt\n" +
                "GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;");
        {
            String query = "select " +
                    " p1.p1_col2, p1.p1_col1, p3.p3_col1 \n" +
                    " ,p2.p2_col2, p2.p2_col4 \n" +
                    " ,p4.p4_col2 \n" +
                    " ,SUM(p1.p1_col4) as cvt_rmb_txn_amt \n" +
                    " from tbl_1 p1 \n" +
                    " inner join \n" +
                    " ( select p1_col2 , p1.dt, p1.p1_col3\n" +
                    " from tbl_1 p1 \n" +
                    " group by 1, 2, 3\n" +
                    " having sum(p1.p1_col4) >= 500000 \n" +
                    " ) p5 \n" +
                    " on p1.p1_col2=p5.p1_col2 and p1.dt=p5.dt and p1.p1_col3=p5.p1_col3\n" +
                    " left join tbl_2 p2 on p2.p2_col1 ='1' and p1.p1_col2 = p2.p2_col2 \n" +
                    " and p2.start_dt <= p1.dt and p2.end_dt > p1.dt \n" +
                    " left join tbl_3 p3 on p1.p1_col2 = p3.p3_col2 and p3.dt=p1.dt \n" +
                    " left join tbl_4 p4 on p1.p1_col1=p4.p4_col1 and p4.dt=p1.dt \n" +
                    " where p1.p1_col3 = '02' and p1.dt='2023-03-31' and p4.p4_col2='200105085'\n" +
                    " group by 1,2,3,4,5,6 \n" +
                    " order by p1.p1_col2\n" +
                    " limit 0, 100";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "TABLE: test_mv2\n");
        }

        {
            String query = "select " +
                    " p1.p1_col2,p1.p1_col1, p3.p3_col1 \n" +
                    " ,p2.p2_col2, p2.p2_col4 \n" +
                    " ,p4.p4_col2 \n" +
                    " ,SUM(p1.p1_col4) as cvt_rmb_txn_amt \n" +
                    " from tbl_1 p1 \n" +
                    " inner join \n" +
                    " ( select p1_col2 , p1.dt \n" +
                    " from tbl_1 p1 \n" +
                    " where p1.p1_col3 = '02' \n" +
                    " group by 1, 2\n" +
                    " having sum(p1.p1_col4) >= 500000 \n" +
                    " ) p5 \n" +
                    " on p1.p1_col2=p5.p1_col2 and p1.dt=p5.dt \n" +
                    " left join tbl_2 p2 on p2.p2_col1 ='1' and p1.p1_col2 = p2.p2_col2 \n" +
                    " and p2.start_dt <= p1.dt and p2.end_dt > p1.dt \n" +
                    " left join tbl_3 p3 on p1.p1_col2 = p3.p3_col2 and p3.dt = p1.dt \n" +
                    " left join tbl_4 p4 on p1.p1_col1=p4.p4_col1 and p4.dt  = p1.dt \n" +
                    " where p1.p1_col3 = '02' and p1.dt = '2023-03-31' and p4.p4_col2 = '200105085'\n" +
                    " group by 1,2,3,4,5,6 \n" +
                    " order by p1.p1_col2\n" +
                    " limit 0, 100";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "rollup: test_mv2");
        }

        {
            String query = "select " +
                    " p1.p1_col2,p1.p1_col1, p3.p3_col1 \n" +
                    " ,p2.p2_col2, p2.p2_col4 \n" +
                    " ,p4.p4_col2 \n" +
                    " ,SUM(p1.p1_col4) as cvt_rmb_txn_amt \n" +
                    " from tbl_1 p1 \n" +
                    " inner join \n" +
                    " ( select p1_col2  \n" +
                    " from tbl_1 p1 \n" +
                    " where p1.p1_col3 = '02' and p1.dt='2023-03-31' \n" +
                    " group by 1\n" +
                    " having sum(p1.p1_col4) >= 500000 \n" +
                    " ) p5 \n" +
                    " on p1.p1_col2=p5.p1_col2 \n" +
                    " left join tbl_2 p2 on p2.p2_col1 ='1' and p1.p1_col2 = p2.p2_col2 \n" +
                    " and p2.start_dt <= p1.dt and p2.end_dt > p1.dt \n" +
                    " left join tbl_3 p3 on p1.p1_col2 = p3.p3_col2 and p3.dt = '2023-03-31' \n" +
                    " left join tbl_4 p4 on p1.p1_col1=p4.p4_col1 and p4.dt  = '2023-03-31' \n" +
                    " where p1.p1_col3 = '02' and p1.dt = '2023-03-31' and p4.p4_col2 = '200105085'\n" +
                    " group by 1,2,3,4,5,6 \n" +
                    " order by p1.p1_col2\n" +
                    " limit 0, 100";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            // TODO: support deduce ec from scan predicates.
            PlanTestBase.assertNotContains(plan, "rollup: test_mv2");
        }
        starRocksAssert.dropMaterializedView("test_mv2");
    }
}