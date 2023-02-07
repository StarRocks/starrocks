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

import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeSubqueryTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSimple() {
        analyzeSuccess("select k from (select v1 as k from t0) a");
        analyzeSuccess("select k from (select v1 + 1 as k from t0) a");
        analyzeSuccess("select k1, k2 from (select v1 as k1, v2 as k2 from t0) a");
        analyzeSuccess("select * from (select 1 from t0) a");
        analyzeSuccess("select * from (select k1, k2 from (select v1 as k1, v2 as k2 from t0) a) b");
        analyzeSuccess("select k1 from (select k1, k2 from (select v1 as k1, v2 as k2 from t0) a) b");
        analyzeSuccess("select b.k1 from (select k1, k2 from (select v1 as k1, v2 as k2 from t0) a) b");

        analyzeFail("select k_error from (select v1 + 1 as k from t0) a");
        analyzeFail("select a.k1 from (select k1, k2 from (select v1 as k1, v2 as k2 from t0) a) b");

        analyzeSuccess("select * from (select count(v1) from t0) a");
        analyzeFail("select * from (select count(v1) from t0)");

        analyzeSuccess(
                "select v1 from t0 where v2 in (select v4 from t1 where v3 = v5) or v2 = (select v4 from t1 where v3 = v5)");
        analyzeFail("select v1 from t0 order by (select v4 from t1)", "ORDER BY clause cannot contain subquery");

        analyzeSuccess("(((select * from t0)))");
        analyzeSuccess("(select * from t0) limit 1");
        analyzeSuccess("(select v1 from t0) order by v1 desc limit 1");
        analyzeSuccess("((select v1 from t0) order by v1 desc limit 1) order by v1");
        analyzeSuccess("((select v1 from t0) order by v1 desc limit 1) limit 2");
        analyzeFail("(select v1 from t0) order by err desc limit 1", "Column 'err' cannot be resolved");
    }

    @Test
    public void testInPredicate() {
        analyzeSuccess("select v1 from t0 where v2 in (select v3 from t1)");
        analyzeSuccess("select v1 from t0 where v2 in (select v4 from t1 where v3 = v5)");
        analyzeSuccess("select v1 from t0 where v2 in ((select v4 from t1 where v3 = v5))");
        analyzeSuccess("select v1 from t0 where v2 in (((select v4 from t1 where v3 = v5)))");

        analyzeSuccess("select v1 from t0 where v2 in (1, 2)");
        analyzeFail("select v1 from t0 where v2 in ((select v4 from t1 where v3 = v5), 2)",
                "In Predicate only support literal expression list");
        analyzeFail("select v1 from t0 where v2 in ((select v4 from t1 where v3 = v5), (select v4 from t1 where v3 = v5))",
                "In Predicate only support literal expression list");

        analyzeFail("SELECT * FROM T WHERE A IN 1, 2", "You have an error in your SQL syntax");
        analyzeFail("SELECT * FROM T WHERE A IN 1", "You have an error in your SQL syntax");
    }

    @Test
    public void testExistsSubquery() {
        analyzeSuccess("select v1 from t0 where exists (select v3 from t1)");
        analyzeSuccess("select v1 from t0 where exists (select v4 from t1 where v3 = v5)");
        analyzeSuccess("select v1 from t0 where exists ((select v4 from t1 where v3 = v5))");
        analyzeSuccess("select v1 from t0 where exists (((select v4 from t1 where v3 = v5)))");
    }

    @Test
    public void testScalarSubquery() {
        analyzeSuccess("select v1 from t0 where v2 = (select v3 from t1)");
        analyzeSuccess("select v1 from t0 where v2 = (select v4 from t1 where v3 = v5)");

        QueryRelation query = ((QueryStatement) analyzeSuccess(
                "select t0.*, v1+5 from t0 left join (select v4 from t1) a on v1 = a.v4")).getQueryRelation();
        Assert.assertEquals("v1,v2,v3,v1 + 5", String.join(",", query.getColumnOutputNames()));
    }
}
