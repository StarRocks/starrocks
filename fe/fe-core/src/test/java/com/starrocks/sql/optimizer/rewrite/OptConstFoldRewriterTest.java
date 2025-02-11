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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OptConstFoldRewriterTest extends MVTestBase {
    private static String R2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        // partition table by partition expression
        R2 = "CREATE TABLE r2 \n" +
                "(\n" +
                "    dt datetime,\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
    }

    @Test
    public void testRewrite() throws Exception {
        starRocksAssert.withTable(R2);
        String[] queries = {
                "SELECT curdate(), dt, k2 from r2",
                "SELECT dt, k2 from r2 where date_trunc('day', dt) < curdate()",
                "SELECT a.dt, b.k2 from r2 a join r2 b on a.k2 = curdate()",
                "SELECT curdate()  from r2 a join r2 b",
                "SELECT dt, k2, sum(v1) as agg1 from r2 where date_trunc('day', dt) < timestamp(curdate()) group by dt, k2",
                "SELECT dt, k2, sum(v1) as agg1 from r2 where date_trunc('day', dt) < curdate() group by dt, k2",
                "SELECT curdate(), dt, k2, sum(v1) as agg1 from r2 group by dt, k2 ",
                "SELECT dt, sum(v1) over(partition by k1 order by k2) as agg1 from r2 where dt < curdate()",
                "SELECT curdate(), dt, k2, sum(v1) as agg1 from r2 group by dt, k2 union all " +
                        "SELECT curdate(), dt, k2, sum(v1) as agg1 from r2 group by dt, k2",
                "SELECT curdate(), dt, k2, sum(v1) as agg1 from r2 group by dt, k2 union " +
                        "SELECT curdate(), dt, k2, sum(v1) as agg1 from r2 group by dt, k2",
                "with cte as (SELECT curdate(), dt, k2, sum(v1) as agg1 from r2 group by dt, k2) select * from cte",
                "with cte as (SELECT curdate(), dt, k2 from r2) select * from cte",
                "with cte as (SELECT dt, k2 from r2 where dt < curdate()) select * from cte",
        };
        for (String sql : queries) {
            connectContext.getSessionVariable().setDisableFunctionFoldConstants(true);
            OptExpression optExpression = getLogicalOptimizedPlan(sql);
            Assert.assertTrue(hasNonDeterministicFunction(optExpression));

            connectContext.getSessionVariable().setDisableFunctionFoldConstants(false);
            OptExpression newOptExpression = OptConstFoldRewriter.rewrite(optExpression);
            Assert.assertFalse(hasNonDeterministicFunction(newOptExpression));
        }
    }
}
