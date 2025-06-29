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

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserVariable;
import org.junit.Assert;
import org.junit.Test;

public class SelectHintTest extends PlanTestBase {

    @Test
    public void testHintSql() throws Exception {
        String sql = "select /*+ set_user_variable(@a = 1, @b = (select max(v4) from t1))*/ @a, @b, v1 from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1\n" +
                "  |  <slot 5> : 'MOCK_HINT_VALUE'\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");

        sql = "select @a, @b, v1 from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : NULL\n" +
                "  |  <slot 5> : NULL\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "select /*+ set_user_variable(@a = 1, @b = (select array_agg(v4) from t1))*/ @a, @b, v1 from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1\n" +
                "  |  <slot 5> : 'MOCK_HINT_VALUE'\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");
    }

    @Test
    public void testRemoveEscapeCharacter() {

        String str = "[\"{\\\"week\\\":{\\\"day\\\":true,\\\"shift\\\":{\\\"begin\\\":0}},\\\"id\\\":\\\"ID1\\\"}\"]";
        String actual = UserVariable.removeEscapeCharacter(str);
        Assert.assertEquals("[\"{\"week\":{\"day\":true,\"shift\":{\"begin\":0}},\"id\":\"ID1\"}\"]", actual);

        str = "[\"{\\\"week\\\":{\\\"d\\\"ay\\\":true,\\\"shift\\\":{\\\"begin\\\":0}},\\\"id\\\":\\\"ID1\\\"}\"]";
        actual = UserVariable.removeEscapeCharacter(str);
        Assert.assertEquals("[\"{\"week\":{\"d\"ay\":true,\"shift\":{\"begin\":0}},\"id\":\"ID1\"}\"]", actual);

        str = "abc\\\\abc";
        actual = UserVariable.removeEscapeCharacter(str);
        Assert.assertEquals("abc\\abc", actual);
    }

    @Test
    public void test() throws Exception {
        String sql = "SELECT /*+ set_user_variable(@a = 1) */ col_1, col_2, LAG(col_2, @a, 0) OVER (ORDER BY col_1) " +
                "FROM (SELECT 1 AS col_1, NULL AS col_2 UNION ALL SELECT 2 AS col_1, 4 AS col_2) AS T ORDER BY col_1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, lag(9: cast, 1, 0), ]");

        sql = "SELECT /*+ set_user_variable(@a = 1, @b = 1) */ lag(v1, @a, @b) over (ORDER BY v2) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, lag(1: v1, 1, 1), ]");

        sql = "SELECT /*+ set_user_variable(@a = 1, @b = null) */ lag(@a, @a, @b) over (ORDER BY v2) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, lag(1, 1, NULL), ]");

        sql = "select /*+ set_user_variable(@a = 1, @b = 100000) */ APPROX_TOP_K(v1, @a), APPROX_TOP_K(v1, @a, @b) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "approx_top_k(1: v1, 1), approx_top_k(1: v1, 1, 100000)");

        sql = "select /*+ set_user_variable(@a = 1, @b = 10) */ ntile(@a) over (partition by v2 order by v3) " +
                "as bucket_id from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, ntile(1), ]");

        sql = "select /*+ set_user_variable(@a = 1, @b = 10) */ percentile_approx(v1, @a) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "percentile_approx(CAST(1: v1 AS DOUBLE), 1.0)");

        Exception exception = Assert.assertThrows(SemanticException.class, () -> {
            String invalidSql = "select /*+ set_user_variable(@a = 1, @b = 1000000) */ APPROX_TOP_K(v1, @a), " +
                    "APPROX_TOP_K(v1, @a, @b)from t0";
            getFragmentPlan(invalidSql);
        });
        assertContains(exception.getMessage(), "The maximum number of the third parameter is 100000");


        exception = Assert.assertThrows(SemanticException.class, () -> {
            String invalidSql = "select /*+ set_user_variable(@a = [1, 2, 3]) */ LAG(v1, @a) over (ORDER BY v2) from t0";
            getFragmentPlan(invalidSql);
        });
        assertContains(exception.getMessage(), "The offset parameter of LEAD/LAG must be a constant positive integer");

        exception = Assert.assertThrows(SemanticException.class, () -> {
            String invalidSql = "select /*+ set_user_variable(@a = 1, @b = 10) */ percentile_approx(@a, @b) from t0";
            getFragmentPlan(invalidSql);
        });
        assertContains(exception.getMessage(), " percentile_approx second parameter'value must be between 0 and 1");
    }
}
