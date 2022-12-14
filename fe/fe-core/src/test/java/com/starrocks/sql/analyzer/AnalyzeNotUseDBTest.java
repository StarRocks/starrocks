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

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeWithoutTestView;

public class AnalyzeNotUseDBTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNotUseDatabase() {
        AnalyzeTestUtil.getConnectContext().setDatabase("");
        analyzeSuccess("select count(*) from (select v1 from db1.t0) t");
        analyzeSuccess("select t.* from (select v1 from db1.t0) t");
        analyzeFail("select db1.t.* from (select v1 from db1.t0) t", "Unknown table 'db1.t'");

        analyzeSuccess("with t as (select v4 from db1.t1) select count(*) from (select v1 from db1.t0) a,t");

        analyzeSuccess("select * from db1.t0 a join db1.t1 b on a.v1 = b.v4");
        analyzeSuccess("select a.v2,b.v5 from db1.t0 a join db1.t1 b on a.v1 = b.v4");

        analyzeSuccess("select v1,unnest from test.tarray, unnest(v3)");
    }

    @Test
    public void testStarFieldWithNotExistsDB() {
        AnalyzeTestUtil.getConnectContext().setDatabase("db1");
        analyzeSuccess("select db1.t0.* from db1.t0");
        analyzeSuccess("select db1.t0.* from t0");
        analyzeSuccess("select t0.* from db1.t0");
        analyzeSuccess("select t0.* from t0");
        analyzeSuccess("select * from db1.t0");
        analyzeSuccess("select * from t0");

        analyzeFail("select db1.foo.* from db1.t0", "Unknown table 'db1.foo'");
        analyzeFail("select db1.foo.* from t0", "Unknown table 'db1.foo'");
        analyzeFail("select err_db.foo.* from t0");
        analyzeFail("select err_db.foo.* from db1.t0");
        analyzeFail("select foo.* from db1.t0", "Unknown table 'foo'");
        analyzeFail("select foo.* from t0", "Unknown table 'foo'");
    }

    @Test
    public void testCteWithSameName() {
        AnalyzeTestUtil.getConnectContext().setDatabase("db1");

        analyzeFail("with cte as (select * from db1.t0) select * from db1.cte",
                "Unknown table 'db1.cte'");
        analyzeSuccess("with cte as (select * from db1.t0) select * from cte");
        analyzeFail("with cte as (select * from db1.t0) select * from cte_err",
                "Unknown table 'db1.cte_err'");
    }

    @Test
    public void testColumnResolveWithDifferentDB() {
        AnalyzeTestUtil.getConnectContext().setDatabase("db1");
        analyzeSuccess("select t0.v1 from t0 inner join db2.t0 t on t0.v1 = db2.t.v1");
        analyzeSuccess("select t0.v1 from t0 inner join db2.t0 t on db1.t0.v1 = db2.t.v1");
        analyzeSuccess("select t0.v1 from t0 inner join db2.t0 t on t0.v1 = t.v1");
        analyzeFail("select t0.v1 from t0 inner join db2.t0 t on t0.v1 = db1.t.v1",
                "Column '`db1`.`t`.`v1`' cannot be resolved");
        analyzeFail("select t0.v1 from t0 inner join db2.t0 t on t0.v1 = v1",
                "Column 'v1' is ambiguous");

        analyzeSuccess("select a.v1 from db1.t0 a inner join db2.t0 b on a.v1 = b.v1");
        analyzeSuccess("select a.v1 from db1.t0 a inner join db2.t0 b on db1.a.v1 = db2.b.v1");
        analyzeFail("select a.v1 from db1.t0 a inner join db2.t0 b on db1.t0.v1 = db2.b.v1");
        analyzeFail("select db1.t0.v1 from db1.t0 a inner join db2.t0 b on db1.a.v1 = db2.b.v1");

        analyzeSuccess("select a.v1 from db1.t0 a cross join db2.t0 b where a.v1 = b.v1");
        analyzeSuccess("select a.v1 from db1.t0 a cross join db2.t0 b where db1.a.v1 = db2.b.v1");
        analyzeFail("select a.v1 from db1.t0 a cross join db2.t0 b where db1.t0.v1 = db2.b.v1");
        analyzeFail("select db1.t0.v1 from db1.t0 a cross join db2.t0 b where db1.a.v1 = db2.b.v1");
    }

    @Test
    public void testDifferentDBWithSameTableName() {
        AnalyzeTestUtil.getConnectContext().setDatabase("db1");

        analyzeSuccess("select db1.t0.v1 from db1.t0 inner join db2.t0 on db1.t0.v1 = db2.t0.v1");
        analyzeSuccess("select db1.t.v1 from db1.t0 t inner join db2.t0 t on db1.t.v1 = db2.t.v1");
        analyzeSuccess("select default_catalog.db1.t.v1 from db1.t0 t inner join db2.t0 t on db1.t.v1 = db2.t.v1");
        analyzeSuccess("select default_catalog.db1.t.v1 from default_catalog.db1.t0 t " +
                "inner join default_catalog.db2.t0 t on db1.t.v1 = db2.t.v1");

        analyzeSuccess("select db1.t.v1 from t0 t join (select * from t0) t");
        analyzeFail("select t.v1 from t0 t join (select * from t0) t", "Column 'v1' is ambiguous");
        analyzeSuccess("select db1.t1.* from t0 t1 join (select * from t0) t2");
        analyzeFail("select db1.t2.* from t0 t1 join (select * from t0) t2", "Unknown table 'db1.t2'");
    }

    @Test
    public void testUniqueTableName() {
        AnalyzeTestUtil.getConnectContext().setDatabase("db1");

        analyzeFail("select * from t0, t0", "Not unique table/alias: 't0'");
        analyzeFail("select * from t0 t, t0 t", "Not unique table/alias: 't'");
        analyzeFail("select * from (select * from t0) t, (select * from t0) t",
                "Not unique table/alias: 't'");

        QueryStatement queryStatement = (QueryStatement) analyzeSuccess("select * from t0 t join unnest([1,2,3]) t");
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, unnest]", selectRelation.getColumnOutputNames().toString());

        queryStatement = (QueryStatement) analyzeSuccess("select * from t0 join unnest([1,2,3]) t0");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, unnest]", selectRelation.getColumnOutputNames().toString());

        queryStatement = (QueryStatement) analyzeSuccess("select * from t0 t join (values(1,2,3)) t");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, column_0, column_1, column_2]", selectRelation.getColumnOutputNames().toString());

        queryStatement = (QueryStatement) analyzeSuccess("select * from t0 join (values(1,2,3)) t0");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, column_0, column_1, column_2]", selectRelation.getColumnOutputNames().toString());

        queryStatement = (QueryStatement) analyzeWithoutTestView("select * from t0 t join (select * from t0) t");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, v1, v2, v3]", selectRelation.getColumnOutputNames().toString());
        analyzeFail("create view v as select * from t0 t join (select * from t0) t",
                "Duplicate column name 'v1'");

        queryStatement = (QueryStatement) analyzeSuccess("select db1.t.* from t0 t join (select * from t0) t");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3]", selectRelation.getColumnOutputNames().toString());

        queryStatement = (QueryStatement) analyzeWithoutTestView("select t.* from t0 t join (select * from t0) t");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, v1, v2, v3]", selectRelation.getColumnOutputNames().toString());
        analyzeFail("create view v as select * from t0 t join (select * from t0) t",
                "Duplicate column name 'v1'");

        queryStatement = (QueryStatement) analyzeWithoutTestView("select t0.* from t0 join (select * from t0) t0");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, v1, v2, v3]", selectRelation.getColumnOutputNames().toString());
        analyzeFail("create view v as select * from t0 join (select * from t0) t0",
                "Duplicate column name 'v1'");

        queryStatement = (QueryStatement) analyzeSuccess("select db1.t0.* from t0 join (select * from t0) t0");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertEquals("[v1, v2, v3]", selectRelation.getColumnOutputNames().toString());
    }
}