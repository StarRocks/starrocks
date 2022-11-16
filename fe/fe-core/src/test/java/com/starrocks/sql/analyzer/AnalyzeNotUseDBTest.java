// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

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
        analyzeSuccess("select db1.t0.v1 from db1.t0 inner join db2.t0 on db1.t0.v1 = db2.t0.v1");
        analyzeSuccess("select db1.t.v1 from db1.t0 t inner join db2.t0 t on db1.t.v1 = db2.t.v1");
        analyzeSuccess("select default_catalog.db1.t.v1 from db1.t0 t inner join db2.t0 t on db1.t.v1 = db2.t.v1");
        analyzeSuccess("select default_catalog.db1.t.v1 from default_catalog.db1.t0 t " +
                "inner join default_catalog.db2.t0 t on db1.t.v1 = db2.t.v1");
    }
}