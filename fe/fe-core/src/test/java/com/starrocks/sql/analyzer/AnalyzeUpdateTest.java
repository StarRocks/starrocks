// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeUpdateTest {
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeUpdate/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testSingle() {
        analyzeFail("update tjson set v_json = '' where v_int = 1",
                "only support updating primary key table");

        analyzeFail("update tprimary set pk = 2 where pk = 1",
                "primary key column cannot be updated:");

        analyzeFail("update tprimary set v1 = 'aaa'",
                "must specify where clause to prevent full table update");

        analyzeSuccess("update tprimary set v1 = 'aaa' where pk = 1");
        analyzeSuccess("update tprimary set v2 = v2 + 1 where pk = 1");
        analyzeSuccess("update tprimary set v1 = 'aaa', v2 = v2 + 1 where pk = 1");
        analyzeSuccess("update tprimary set v2 = v2 + 1 where v1 = 'aaa'");

        analyzeSuccess("update tprimary set v3 = [231,4321,42] where pk = 1");
    }
}
