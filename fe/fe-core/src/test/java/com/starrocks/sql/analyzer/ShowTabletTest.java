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

import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ShowTabletTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void normalTest() {
        analyzeSuccess("SHOW TABLET 10000;");
        analyzeSuccess("SHOW TABLET FROM test_tbl;");
        analyzeSuccess("SHOW TABLETS FROM test_tbl;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name partition(p1, p2);");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name limit 10;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name limit 5,10;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name where " +
                "backendid=10000 and version=1 and state=\"NORMAL\";");
        analyzeSuccess("SHOW TABLET FROM test.t0 where backendid=10000 order by version;");
        analyzeSuccess("SHOW TABLET FROM example_db.table_name where indexname=\"t1_rollup\";");
    }

    @Test
    public void failureTest() {
        analyzeFail("SHOW TABLET FROM example_db.table_name where backendid=10000 or ts > 10",
                "Only allow compound predicate with operator AND");
        analyzeFail("SHOW TABLET FROM example_db.table_name where abc=\"t1_rollup\";");
        analyzeFail("SHOW TABLET FROM test.t0 where backendid=10000 order by abc;");
        analyzeFail("SHOW TABLET FROM test.lake_table where backendid=10000 order by DataSize;",
                "Table lake_table is not found");
    }

}
