// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ShowTabletTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void normalTest() {
        analyzeSuccess("SHOW TABLET 10000;");
        analyzeSuccess("SHOW TABLET FROM test_tbl;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name partition(p1, p2);");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name limit 10;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name limit 5,10;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name where " +
                "backendid=10000 and version=1 and state=\"NORMAL\";");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name where backendid=10000 order by version;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name where indexname=\"t1_rollup\";");
    }

    @Test
    public void failureTest() {
        analyzeFail("SHOW TABLET FROM example_db.table_name where backendid=10000 or ts > 10",
                "Only allow compound predicate with operator AND");
        analyzeFail("SHOW TABLET FROM example_db.table_name where abc=\"t1_rollup\";");
        analyzeFail("SHOW TABLET FROM example_db.table_name where backendid=10000 order by abc;");
    }

}
