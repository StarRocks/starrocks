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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MergeIntoPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        
        starRocksAssert.withCatalog(
                "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\n" +
                "\"type\" = \"iceberg\",\n" +
                "\"iceberg.catalog.type\" = \"hive\",\n" +
                "\"hive.metastore.uris\" = \"thrift://localhost:9083\"\n" +
                ")");
        
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
        
        starRocksAssert.withTable(
                "CREATE TABLE source_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string\n" +
                ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void testBasicMergeIntoPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoWithDeletePlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age > 50 THEN DELETE " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoPartitionedTablePlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_part_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoWithSubqueryPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING (SELECT id, name, age, city FROM test.source_tbl WHERE age > 20) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoOnlyInsertPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoOnlyUpdatePlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoOnlyDeletePlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoMultipleUpdatesPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age < 30 THEN UPDATE SET city = 'Young' " +
                "WHEN MATCHED AND s.age >= 30 AND s.age < 60 THEN UPDATE SET city = 'Middle' " +
                "WHEN MATCHED THEN UPDATE SET city = 'Senior' " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoWithPartialInsertPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoWithExpressionsPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET age = s.age + 1, name = concat(s.name, '_updated') " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, upper(s.name), s.age * 2, s.city)";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoComplexJoinPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id AND t.name = s.name " +
                "WHEN MATCHED THEN UPDATE SET age = s.age";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoConditionalInsertPlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED AND s.age > 18 AND s.city IS NOT NULL THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoWithCasePlan() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET city = CASE WHEN s.age < 30 THEN 'Young' ELSE 'Old' END";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testExplainMergeInto() throws Exception {
        String sql = "EXPLAIN MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }

    @Test
    public void testMergeIntoWithAggregationSource() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING (SELECT id, max(name) as name, max(age) as age, max(city) as city " +
                "       FROM test.source_tbl GROUP BY id) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age";
        
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MERGE INTO");
    }
}
