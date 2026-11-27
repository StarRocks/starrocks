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

package com.starrocks.planner;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// When only some usages of a view are rewritten by a view-based MV, the remaining view scans are replaced back
// with the view's original plan. The original plan must only produce the columns the view scan really uses,
// otherwise the columns which have already been pruned (e.g. aggregations nobody reads) come back into the plan.
public class ViewBasedMvRewritePruneColumnsTest extends MaterializedViewTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        starRocksAssert.withTable("CREATE TABLE t_prune (k bigint NOT NULL, a varchar(64), v bigint) " +
                "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1')");
        starRocksAssert.withView("CREATE VIEW vp_a AS " +
                "SELECT k, any_value(a) AS a, sum(v) AS s FROM t_prune WHERE k < 5 GROUP BY k");
        starRocksAssert.withView("CREATE VIEW vp_b AS " +
                "SELECT k, any_value(a) AS a, sum(v) AS s FROM t_prune WHERE k >= 5 GROUP BY k");
        // can rewrite vp_a
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_vp_a DISTRIBUTED BY RANDOM " +
                "REFRESH DEFERRED MANUAL PROPERTIES('replication_num'='1') AS SELECT * FROM vp_a");
        // lacks column s, so it can not rewrite vp_b which is then replaced back with its original plan
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_vp_b DISTRIBUTED BY RANDOM " +
                "REFRESH DEFERRED MANUAL PROPERTIES('replication_num'='1') AS SELECT k, a FROM vp_b");

        // views with a filter, the filter must survive when the view scan is replaced back
        starRocksAssert.withView("CREATE VIEW vp_f AS SELECT k, v FROM t_prune WHERE k >= 5");
        starRocksAssert.withView("CREATE VIEW vp_w AS " +
                "SELECT k, v, row_number() OVER (ORDER BY k) AS rn FROM t_prune WHERE k >= 5");
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_vp_f DISTRIBUTED BY RANDOM " +
                "REFRESH DEFERRED MANUAL PROPERTIES('replication_num'='1') AS " +
                "SELECT k, count(*) AS c FROM vp_f GROUP BY k");
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_vp_w DISTRIBUTED BY RANDOM " +
                "REFRESH DEFERRED MANUAL PROPERTIES('replication_num'='1') AS " +
                "SELECT k, count(*) AS c FROM vp_w GROUP BY k");

        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
    }

    /**
     * Plan tree which is logged right after the view based mv rewrite, the columns which are pruned later are
     * still visible here.
     */
    private String planAfterViewBasedRewrite(String sql) {
        String logs = Assertions.assertDoesNotThrow(() ->
                UtFrameUtils.getFragmentPlanWithTrace(connectContext, sql, "Optimizer").second.second);
        String tag = "after ViewBasedMvRuleRewrite:";
        int begin = logs.indexOf(tag);
        Assertions.assertTrue(begin >= 0, "view based mv rewrite did not run: " + logs);
        Matcher next = Pattern.compile("\\n\\s*\\d+ms\\|").matcher(logs);
        int end = next.find(begin) ? next.start() : logs.length();
        return logs.substring(begin, end);
    }

    @Test
    public void testReplacedBackViewDoesNotBringPrunedColumnsBack() {
        String plan = planAfterViewBasedRewrite(
                "SELECT k, sum(s) FROM (SELECT k, s FROM vp_a UNION ALL SELECT k, s FROM vp_b) x GROUP BY k");
        // vp_a is rewritten by mv_vp_a, vp_b is replaced back with its original plan
        Assertions.assertTrue(plan.contains("mv_vp_a"), plan);
        Assertions.assertTrue(plan.contains("t_prune"), plan);
        // nobody reads column a, its aggregation must not come back
        Assertions.assertFalse(plan.contains("any_value"), plan);
        Assertions.assertTrue(plan.contains("sum"), plan);
    }

    @Test
    public void testReplacedBackViewKeepsColumnsUsedByPredicate() {
        // column a is only used by the predicate of the view scan, its aggregation must be kept
        String sql = "SELECT k, s FROM vp_a UNION ALL SELECT k, s FROM vp_b WHERE a = 'n1'";
        String plan = planAfterViewBasedRewrite(sql);
        Assertions.assertTrue(plan.contains("any_value"), plan);
        String finalPlan = Assertions.assertDoesNotThrow(() -> getQueryPlan(sql));
        Assertions.assertTrue(finalPlan.contains("n1"), finalPlan);
    }

    @Test
    public void testReplacedBackViewWithExpression() {
        String sql = "SELECT k, s FROM vp_a UNION ALL SELECT k, s + 1 FROM vp_b";
        String plan = planAfterViewBasedRewrite(sql);
        Assertions.assertFalse(plan.contains("any_value"), plan);
    }

    private void assertKeepsViewFilter(String viewName) {
        // the aggregation usage is rewritten by the MV, the other usage is replaced back with the view plan
        String sql = "SELECT k, c FROM (SELECT k, count(*) AS c FROM " + viewName + " GROUP BY k) a " +
                "UNION ALL SELECT k, v FROM " + viewName + " WHERE k < 10";
        String plan = Assertions.assertDoesNotThrow(() -> getQueryPlan(sql));
        // both the filter of the view and the filter of the query must be applied on the replaced back usage
        Assertions.assertTrue(plan.contains("k >= 5"), plan);
        Assertions.assertTrue(plan.contains("k < 10"), plan);
    }

    @Test
    public void testReplacedBackViewKeepsPredicateOfInlinedPlan() {
        assertKeepsViewFilter("vp_f");
    }

    @Test
    public void testReplacedBackWindowViewKeepsPredicateOfInlinedPlan() {
        // the window function is not used by the query and is pruned, so the filtered scan becomes the root of
        // the replaced back plan, and the filter of the query must not overwrite the filter of the view
        assertKeepsViewFilter("vp_w");
    }
}
