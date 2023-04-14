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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.server.MetadataMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeInsertTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);
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

        analyzeSuccess("insert into tarray(v1,v4) values (1,[NULL,9223372036854775808])");

        analyzeFail("insert into t0 values (170141183460469231731687303715884105728)",
                "Numeric overflow 170141183460469231731687303715884105728.");

        analyzeFail("insert into tall(ta) values(min('x'))", "Values clause cannot contain aggregations");
        analyzeFail("insert into tall(ta) values(case min('x') when 'x' then 'x' end)",
                "Values clause cannot contain aggregations");
        analyzeFail("insert into tall(ta) values(min('x') over())", "Values clause cannot contain window function");

        analyzeSuccess("INSERT INTO tp  PARTITION(p1) VALUES(1,2,3)");

        analyzeSuccess("insert into t0 with label l1 select * from t0");
        analyzeSuccess("insert into t0 with label `l1` select * from t0");
    }

    @Test
    public void testInsertIcebergUnpartitionedTable(@Mocked IcebergTable icebergTable) {
        analyzeFail("insert into err_catalog.db.tbl values(1)",
                "Unknown catalog 'err_catalog'");

        MetadataMgr metadata = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadata) {
            {
                metadata.getDb("iceberg_catalog", "err_db");
                result = null;
                minTimes = 0;
            }
        };
        analyzeFail("insert into iceberg_catalog.err_db.tbl values (1)",
                "Database err_db is not found");

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
        analyzeFail("insert into iceberg_catalog.db.err_tbl values (1)",
                "Table err_tbl is not found");

        new Expectations(metadata) {
            {
                metadata.getTable(anyString, anyString, anyString);
                result = new HiveTable();
                minTimes = 0;
            }
        };
        analyzeFail("insert into iceberg_catalog.db.hive_tbl values (1)",
                "Only support insert into olap table or mysql table or iceberg table");

        new Expectations(metadata) {
            {
                metadata.getTable(anyString, anyString, anyString);
                result = icebergTable;
                minTimes = 0;

                icebergTable.supportInsert();
                result = true;
                minTimes = 0;
            }
        };
        analyzeFail("insert into iceberg_catalog.db.tbl values (1)",
                "Column count doesn't match value count");

        new Expectations(metadata) {
            {
                icebergTable.getBaseSchema();
                result = ImmutableList.of(new Column("c1", Type.INT));
                minTimes = 0;
            }
        };
        analyzeSuccess("insert into iceberg_catalog.db.iceberg_tbl values (1)");
    }

    @Test
    public void testPartitionedIcebergTable(@Mocked IcebergTable icebergTable) {
        MetadataMgr metadata = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadata) {
            {
                metadata.getDb(anyString, anyString);
                result = new Database();
                minTimes = 0;

                metadata.getTable(anyString, anyString, anyString);
                result = icebergTable;
                minTimes = 0;

                icebergTable.supportInsert();
                result = true;
                minTimes = 0;

                icebergTable.getPartitionColumnNames();
                result = ImmutableList.of("p1", "p2");
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", Type.ARRAY_DATE));
                minTimes = 0;
            }
        };

        analyzeFail("insert into iceberg_catalog.db.tbl partition(p1=1) values (1)",
                "Must include all partition column names");

        analyzeFail("insert into iceberg_catalog.db.tbl partition(p2=1, p1=1) values (1)",
                "Expected: p1, but actual: p2");

        analyzeFail("insert into iceberg_catalog.db.tbl partition(p1=1, p2=\"aaffsssaa\") values (1)",
                "annot cast '1' from TINYINT to ARRAY<DATE>");

        new Expectations() {
            {
                icebergTable.getBaseSchema();
                result = ImmutableList.of(new Column("c1", Type.INT), new Column("p1", Type.INT), new Column("p2", Type.INT));
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", Type.INT), new Column("p2", Type.INT));
                minTimes = 0;
            }
        };

        analyzeSuccess("insert into iceberg_catalog.db.tbl partition(p1=111, p2=222) values (1)");
    }
}
