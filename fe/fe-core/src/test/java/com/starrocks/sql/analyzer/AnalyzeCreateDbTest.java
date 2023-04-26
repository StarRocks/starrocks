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

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCreateDbTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        connectContext = UtFrameUtils.createDefaultCtx();
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);
    }

    @Test
    public void testIceberg() {
        analyzeSuccess("CREATE Database `iceberg_db`");
        analyzeSuccess("CREATE Database `iceberg_catalog`.`iceberg_db`");
        analyzeSuccess("CREATE Database `iceberg_catalog`.`iceberg_db`" +
                " properties(\"location\" = \"hdfs://namenode:9000/user/warehouse/hive/iceberg_db.db\")");

        try {
            String stmt = "CREATE Database `not_exist_catalog`.`iceberg_db` properties(\"location\" = \"hdfs://namenode:9000/user/warehouse/hive/iceberg_db.db\")";
            UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().contains("Getting analyzing error. Detail message: Unknown catalog"));
        }
    }
}
