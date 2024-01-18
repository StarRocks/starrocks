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

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeDropDbTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        connectContext = starRocksAssert.getCtx();
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);

        createIcebergCatalogStmt = "create external catalog hudi_catalog properties (\"type\"=\"hudi\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);
    }

    @Test
    public void testIceberg() throws Exception {
        analyzeSuccess("DROP database `iceberg_db`");
        analyzeSuccess("DROP Database `iceberg_catalog`.`iceberg_db`");

        try {
            String stmt = "Drop database not_exist_catalog.db";
            UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().contains("Getting analyzing error." +
                    " Detail message: Unknown catalog 'not_exist_catalog'."));
        }

        String stmt = "DROP DATABASE hudi_catalog.iceberg_db";
        DropDbStmt dropDbStmt =
                (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);

        try {
            DDLStmtExecutor.execute(dropDbStmt, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DdlException);
            Assert.assertTrue(e.getMessage().contains("Can't drop database"));
        }
    }

    @Test
    public void testDropSystem() throws Exception {
        analyzeFail("DROP database `information_schema`", "Access denied;");
        analyzeFail("DROP Database `sys`", "Access denied;");
    }
}
