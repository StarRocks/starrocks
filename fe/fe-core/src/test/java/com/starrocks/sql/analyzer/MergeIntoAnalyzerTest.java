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

import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeIntoAnalyzerTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege());
        
        starRocksAssert.withDatabase("test").useDatabase("test");
        
        starRocksAssert.withCatalog(
                "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\n" +
                "\"type\" = \"iceberg\",\n" +
                "\"iceberg.catalog.type\" = \"hive\",\n" +
                "\"hive.metastore.uris\" = \"thrift://localhost:9083\"\n" +
                ")");
        
        starRocksAssert.useDatabase("iceberg_catalog.test");
        
        starRocksAssert.withTable(
                "CREATE EXTERNAL TABLE iceberg_catalog.test.iceberg_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string\n" +
                ") ENGINE=iceberg;");
        
        starRocksAssert.withTable(
                "CREATE EXTERNAL TABLE iceberg_catalog.test.iceberg_part_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  dt string\n" +
                ") PARTITIONED BY (dt) ENGINE=iceberg;");
        
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable(
                "CREATE TABLE source_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string\n" +
                ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisit();
    }

    @Test
    public void testBasicMergeInto() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithDelete() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age > 50 THEN DELETE " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithMultipleClauses() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age < 30 THEN UPDATE SET city = 'Young' " +
                "WHEN MATCHED AND s.age >= 30 AND s.age < 60 THEN UPDATE SET city = 'Middle' " +
                "WHEN MATCHED AND s.age >= 60 THEN UPDATE SET city = 'Senior' " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoPartitionedTable() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_part_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, '2024-01-01')";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithPartialInsert() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithSubquery() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING (SELECT id, name, age, city FROM test.source_tbl WHERE age > 20) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithoutAlias() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl " +
                "USING test.source_tbl " +
                "ON iceberg_tbl.id = source_tbl.id " +
                "WHEN MATCHED THEN UPDATE SET name = source_tbl.name";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoOnlyInsert() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoOnlyUpdate() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoOnlyDelete() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoNonIcebergTableFails() {
        String sql = "MERGE INTO test.source_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        Exception exception = assertThrows(Exception.class, () -> {
            starRocksAssert.query(sql).analysisSuccess();
        });
        assertTrue(exception.getMessage().contains("MERGE INTO is only supported for Iceberg tables"));
    }

    @Test
    public void testMergeIntoInvalidColumnCount() {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)";
        
        Exception exception = assertThrows(Exception.class, () -> {
            starRocksAssert.query(sql).analysisSuccess();
        });
        assertTrue(exception.getMessage().contains("doesn't match target column count"));
    }

    @Test
    public void testMergeIntoWithComplexConditions() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND (s.age > 30 OR s.city = 'NYC') THEN UPDATE SET name = s.name " +
                "WHEN MATCHED AND s.age <= 30 THEN DELETE " +
                "WHEN NOT MATCHED AND s.age < 60 THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithExpressions() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET age = s.age + 1, name = concat(s.name, '_updated') " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, upper(s.name), s.age * 2, s.city)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoWithConditionalInsert() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED AND s.age > 18 THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        starRocksAssert.query(sql).analysisSuccess();
    }

    @Test
    public void testMergeIntoMultipleUpdatesWithConditions() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.city = 'NYC' THEN UPDATE SET age = s.age + 5 " +
                "WHEN MATCHED AND s.city = 'LA' THEN UPDATE SET age = s.age + 3 " +
                "WHEN MATCHED THEN UPDATE SET age = s.age";
        
        starRocksAssert.query(sql).analysisSuccess();
    }
}
