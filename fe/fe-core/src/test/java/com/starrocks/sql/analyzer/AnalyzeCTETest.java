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

public class AnalyzeCTETest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSingle() {
        analyzeSuccess("with c1 as (select * from t0) select c1.* from c1");
        analyzeSuccess("with c1 as (select * from t0) select a.* from c1 a");
        analyzeSuccess("with c1 as (select * from t0) select a.* from c1 a");

        // original table name is not allowed to access when alias-name exists
        analyzeFail("with c1 as (select * from t0) select c1.* from c1 a");

        QueryRelation query = ((QueryStatement) analyzeSuccess(
                "with c1(a,b,c) as (select * from t0) select c1.* from c1")).getQueryRelation();
        Assert.assertEquals("a,b,c", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess(
                "with c1(a,b,c) as (select * from t0) select t.* from c1 t")).getQueryRelation();
        Assert.assertEquals("a,b,c", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("with c1(a,b,c) as (select * from t0), c2 as " +
                "(select * from t1) select c2.*,t.* from c1 t,c2")).getQueryRelation();
        Assert.assertEquals("v4,v5,v6,a,b,c", String.join(",", query.getColumnOutputNames()));
    }

    @Test
    public void testMulti() {
        // without alias
        QueryRelation query = ((QueryStatement) analyzeSuccess("with "
                + "tbl1 as (select v1, v2 from t0), "
                + "tbl2 as (select v4, v5 from t1)"
                + "select tbl1.*, tbl2.* from tbl1 join tbl2 on tbl1.v1 = tbl2.v4")).getQueryRelation();
        Assert.assertEquals("v1,v2,v4,v5", String.join(",", query.getColumnOutputNames()));

        // with alias
        query = ((QueryStatement) analyzeSuccess("with "
                + "tbl1 as (select v1, v2 from t0),"
                + "tbl2 as (select v4, v5 from t1)"
                + "select a.*, b.* from tbl1 a join tbl2 b on a.v1 = b.v4")).getQueryRelation();
        Assert.assertEquals("v1,v2,v4,v5", String.join(",", query.getColumnOutputNames()));

        // partial alias
        query = ((QueryStatement) analyzeSuccess("with "
                + "tbl1 as (select v1, v2 from t0),"
                + "tbl2 as (select v4, v5 from t1)"
                + "select a.*, tbl2.* from tbl1 a join tbl2 on a.v1 = tbl2.v4")).getQueryRelation();
        Assert.assertEquals("v1,v2,v4,v5", String.join(",", query.getColumnOutputNames()));
    }

    @Test
    public void testNamedQuery() {
        analyzeSuccess("with cte1 as ( with cte1 as (select * from t0) select * from cte1) select * from cte1");
        analyzeSuccess("with cte1 as (select * from test.t0), cte2 as (select * from cte1) select * from cte1");
        analyzeFail("with cte1 as (select * from test.t0), cte1 as (select * from cte1) select * from cte1",
                "Not unique table/alias: 'cte1'");
        analyzeFail("with cte1(c1,c2) as (select * from test.t0) select * from cte1",
                "View's SELECT and view's field list have different column counts");
        analyzeFail("with cte1(c1,c2) as (select * from test.t0) select c1,c2 from cte1",
                "View's SELECT and view's field list have different column counts");
        analyzeSuccess("with cte1(c1,c2) as (select v1,v2 from test.t0) select c1,c2 from cte1");
    }
}

