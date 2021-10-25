// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccessUseInsert;

public class AnalyzeInsertTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeInsertTest/" + UUID.randomUUID().toString() + "/";

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
    public void testInsert() {
        analyzeFail("insert into t0 select v4,v5 from t1",
                "Column count doesn't match value count");
        analyzeFail("insert into t0 select 1,2", "Column count doesn't match value count");
        analyzeFail("insert into t0 values(1,2)", "Column count doesn't match value count");

        analyzeFail("insert into tnotnull(v1) values(1)",
                "must be explicitly mentioned in column permutation");
        analyzeFail("insert into tnotnull(v1,v3) values(1,3)",
                "must be explicitly mentioned in column permutation");

        analyzeSuccessUseInsert("insert into tarray(v1,v4) values (1,[NULL,9223372036854775808])");

        analyzeFail("insert into t0 values (170141183460469231731687303715884105728)", "Number Overflow. literal");
    }
}
