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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.MetadataMgr;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Expectations;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeDropTableTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);
    }

    @Test
    public void testDropIcebergTable() {
        analyzeFail("DROP TABLE not_exist_catalog.db.tbl");

        MetadataMgr metadata = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadata) {
            {
                metadata.getDb(anyString, anyString);
                result = null;
                minTimes = 0;
            }
        };
        analyzeFail("DROP TABLE iceberg_catalog.not_exist_db.tbl");

        new Expectations(metadata) {
            {
                metadata.getDb(anyString, anyString);
                result = new Database();
                minTimes = 0;

                metadata.getTable(anyString, anyString, anyString);
                result = null;
                minTimes = 0;
            }
        };
        analyzeFail("DROP TABLE iceberg_catalog.iceberg_db.iceberg_table");

        new Expectations(metadata) {
            {
                metadata.getTable(anyString, anyString, anyString);
                result = new Table(Table.TableType.ICEBERG);
                minTimes = 0;
            }
        };
        analyzeSuccess("DROP TABLE iceberg_catalog.iceberg_db.iceberg_table");
    }
}
