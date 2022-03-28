// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeJoinTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeJoin/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        AnalyzeTestUtil.init();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testSimple() {
        analyzeSuccess("select v1, v2 from t0,t1");
        analyzeSuccess("select v1, v2 from t0 inner join t1 on t0.v1 = t1.v4");
        analyzeSuccess("select * from (select v1, v2 from t0 inner join t1 on t0.v1 = t1.v4) a");
        analyzeSuccess("select a.v1 from (select v1, v2, v5, v4 from t0 inner join t1 on t0.v1 = t1.v4) a");

        /*
         * Test alias
         */
        analyzeSuccess("select * from t0 a join t1 b on a.v1=b.v4");
        analyzeSuccess("select * from t0 a join (select * from t1) b on a.v1=b.v4");
        analyzeFail("select t0.* from t0 a join t1 b on a.v1=b.v4");
        analyzeSuccess("select a.* from t0 a join t1 b on a.v1=b.v4");
        analyzeFail("select t1.v1 from t1 inner join t2 on t1.v3 = t2.v3  where v2 = 2");
    }

    @Test
    public void testSemiJoin() {
        analyzeSuccess("select v1 from t0 left semi join t1 on t0.v1 = t1.v4");
        analyzeSuccess("select * from t0 left semi join t1 on t0.v1 = t1.v4");
        analyzeSuccess("select t0.* from t0 left semi join t1 on t0.v1 = t1.v4");
        analyzeSuccess("select t0.* from t0 left semi join t1 on t0.v1 = t1.v4 where v3 = 5");
        analyzeSuccess("select t1.* from t0 right semi join t1 on t0.v1 = t1.v4 where v4 = 5");
        analyzeSuccess("select * from t0 right outer join t1 on t0.v1 = t1.v4");

        analyzeFail("select v4 from t0 left semi join t1 on t0.v1 = t1.v4");
        analyzeFail("select v4 from t1 left semi join t0 on v1=v4 where t0.v2 = t1.v5");
        analyzeFail("select v4 from t1 left semi join t0 on v1=v4 where v2 = 5");
        analyzeFail("select sum(v1) from t0 left semi join t1 on v1 = v4 and v2 = v5 group by v2,v3,v4");
        analyzeSuccess("select sum(v1) from t0 left semi join t1 on v1 = v4 and v2 = v5 group by v2,v3");

        QueryRelation query = ((QueryStatement) analyzeSuccess(
                "select * from (select sum(v1) as v, sum(v2) from t0) a left semi join (select v1,v2 from t0 order by v3) b on a.v = b.v2")).getQueryRelation();
        Assert.assertEquals("v,sum(v2)", String.join(",", query.getColumnOutputNames()));
    }

    @Test
    public void testJoinUsing() {
        analyzeSuccess("select * from t0 a join t0 b using(v1)");
        analyzeSuccess("select * from t0 a join t0 b using(v1, v2, v3)");
        analyzeFail("select * from t0 join t0 using(v1)");
        analyzeFail("select * from t0 join t1 using(v1)");
        analyzeSuccess("select * from t0 x,t0 y inner join t0 z using(v1)");
        analyzeFail("select * from t0,t1 inner join tnotnull using(v1)", "Column '`v1`' cannot be resolved");
        analyzeFail("select * from t0,t1 inner join tnotnull using(v1,v2)");
        analyzeSuccess("select * from tnotnull inner join (select * from t0,t1) t using (v1)");
        analyzeFail("select * from (select * from t0,tnotnull) t inner join t0 using (v1)",
                "Column 'v1' is ambiguous");
        analyzeSuccess("select * from tnotnull inner join (select * from t0) t using (v1)");

        analyzeSuccess("select * from (t0 join tnotnull using(v1)) , t1");
        analyzeSuccess("select * from (t0 join tnotnull using(v1)) t , t1");
        analyzeFail("select v1 from (t0 join tnotnull using(v1)), t1","Column 'v1' is ambiguous");
        analyzeSuccess("select a.v1 from (t0 a join tnotnull b using(v1)), t1");
    }

    @Test
    public void testOuterJoin() {
        analyzeFail("select v1 from t0 left join t1 where v1 = v4");
        analyzeFail("select v1 from t0 left semi t1 where v1 = v4");
    }

    @Test
    public void testWithAggregation() {
        analyzeFail("select * from t0 join t1 on sum(v1)=v4");
    }

    @Test
    public void testColumnNames() {
        QueryRelation query = ((QueryStatement) analyzeSuccess("select * from t0 left semi join t1 on t0.v1 = t1.v4")).getQueryRelation();
        Assert.assertEquals("v1,v2,v3", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select t0.*,v1,t1.* from t0 join t1 on t0.v1=t1.v4")).getQueryRelation();
        Assert.assertEquals("v1,v2,v3,v1,v4,v5,v6", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select t1.*,v1,t0.* from t0 join t1 on t0.v1=t1.v4")).getQueryRelation();
        Assert.assertEquals("v4,v5,v6,v1,v1,v2,v3", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select a.v1 as v, a.v2 as v, b.v1 as v from t0 a,t0 b")).getQueryRelation();
        Assert.assertEquals("v,v,v", String.join(",", query.getColumnOutputNames()));
        analyzeFail("select a.v1 as v, a.v2 as v, b.v1 as v from t0 a,t0 b order by v", "Column 'v' is ambiguous");
        analyzeFail("select v1 from (select * from t0 a,t0 b) t", "Column 'v1' is ambiguous");
        analyzeFail("select v from (select a.v1 as v, b.v1 as v from t0 a,t0 b) t", "Column 'v' is ambiguous");
    }

    @Test
    public void testJoinHint() {
        //CROSS JOIN does not support SHUFFLE
        analyzeFail("select v1 from t0 inner join [shuffle] t1");
        //Right outer does not support BROADCAST
        analyzeFail("select v1 from t0 right join [broadcast] t1 on t0.v1 = t1.v4");
        //Full outer does not support BROADCAST
        analyzeFail("select v1 from t0 full outer join [broadcast] t1 on t0.v1 = t1.v4");
    }
}
