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
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * SELECT DISTINCT only emits the select-list expressions, so an ORDER BY that references anything
 * else cannot be evaluated above it. Such queries used to be accepted by the analyzer and then blew
 * up deep in the optimizer with "only found column statistics: {...}, but missing statistic of col".
 */
public class AnalyzeDistinctOrderByTest {
    private static final String ERR = "for SELECT DISTINCT, ORDER BY expressions must appear in select list";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testOrderByGroupByKeyNotInSelectList() {
        // the group by key is dropped by the DISTINCT, so it cannot be an ordering key
        analyzeFail("select distinct sum(v2) from t0 group by v1 order by v1", ERR);
        analyzeFail("select distinct sum(v2) from t0 group by v1 order by v1 + 1", ERR);
        analyzeFail("select distinct sum(v2) from t0 group by v1 having sum(v2) > 1 order by v1", ERR);
        analyzeFail("select distinct v1 + v2 as e from t0 group by v1, v2 order by v1", ERR);
        analyzeFail("select distinct v1 + v2 as e from t0 group by grouping sets((v1, v2), (v1)) order by v1", ERR);
        // the shape reported by the SQL fuzzer
        analyzeFail("select ((abs(result.co - 120)) / 120) < 0.00001 from (" +
                "select distinct covar_samp(t0.v2, t0.v3) as co from t0 " +
                "group by t0.v1 order by t0.v1 asc limit 1, 1) result", ERR);
    }

    @Test
    public void testOrderByIsCheckedRegardlessOfSqlMode() {
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        long sqlMode = ctx.getSessionVariable().getSqlMode();
        try {
            ctx.getSessionVariable().setSqlMode(sqlMode & ~SqlModeHelper.MODE_ONLY_FULL_GROUP_BY);
            analyzeFail("select distinct sum(v2) from t0 group by v1 order by v1", ERR);
            analyzeFail("select distinct v1 from t0 order by v2", ERR);
        } finally {
            ctx.getSessionVariable().setSqlMode(sqlMode);
        }
    }

    @Test
    public void testOrderByCoveredByDistinctOutputIsAccepted() {
        analyzeSuccess("select distinct v1, sum(v2) from t0 group by v1 order by v1");
        analyzeSuccess("select distinct sum(v2) as s from t0 group by v1 order by s");
        analyzeSuccess("select distinct sum(v2) from t0 group by v1 order by 'x'");
        analyzeSuccess("select distinct v1, v2 from t0 group by grouping sets((v1), (v2)) order by v1 + v2");
        analyzeSuccess("select distinct v1, v2 from t0 group by rollup(v1, v2) order by v1 + v2");
        analyzeSuccess("select distinct v1 from t0 order by v1");
        analyzeSuccess("select distinct v1 from t0 order by abs(v1) desc");
    }

    @Test
    public void testOrderByReferringToSelectListThroughAnAliasIsAccepted() {
        analyzeSuccess("select distinct v1 as b from t0 order by v1");
        analyzeSuccess("select distinct v1 as b from t0 order by b");
        analyzeSuccess("select distinct v1 from t0 order by t0.v1");
        analyzeSuccess("select distinct t0.v1 from t0 order by v1");
        analyzeSuccess("select distinct v1 + 1 from t0 order by v1 + 1");
        analyzeSuccess("select distinct v1 from t0 order by v1 + 1");
        analyzeSuccess("select distinct v1 as a, v2 as b from t0 order by v1 + v2");
        analyzeSuccess("select distinct abs(v1) as b from t0 order by abs(v1)");
        // still rejected: v2 is not part of the DISTINCT output. Under ONLY_FULL_GROUP_BY the
        // pre-existing AggregationAnalyzer check refuses it first, so it keeps its own message.
        analyzeFail("select distinct v1 as b from t0 order by v2",
                "must be an aggregate expression or appear in GROUP BY clause");
    }
}
