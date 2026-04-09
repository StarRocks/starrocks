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

import com.google.common.collect.Maps;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Replicates the official StarRocks testing patterns for JDBC aggregation pushdown.
 * This test uses the standard {@link MockedJDBCMetadata} which is enhanced to use 
 * actual SR production logic (ClickhouseSchemaResolver) for JDBCTable creation.
 */
public class JDBCAggregationPushdownPlanTest extends ConnectorPlanTestBase {

    private static final String CK_CATALOG = "ck_jdbc";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        setupClickHouseCatalog();
    }

    private static void setupClickHouseCatalog() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(JDBCResource.TYPE, "jdbc");
        properties.put(JDBCResource.DRIVER_CLASS, "com.clickhouse.jdbc.ClickHouseDriver");
        properties.put(JDBCResource.URI, "jdbc:clickhouse://127.0.0.1:8123");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");

        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("jdbc", CK_CATALOG, "", properties);

        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(CK_CATALOG, new MockedJDBCMetadata(properties));
    }

    @Test
    public void testSimpleAggPushdown() throws Exception {
        String sql = "SELECT id, sum(clicks) FROM ck_jdbc.ck_db.ck_agg_table GROUP BY id";
        String plan = getFragmentPlan(sql);
        
        // Verifies the "SCAN JDBC" output contains the pushed aggregation fragment
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "sumMerge(`clicks`) AS Float64");
    }

    @Test
    public void testMultipleAggPushdown() throws Exception {
        // Test SUM, MAX, MIN, COUNT and AVG together in a single query
        String sql = "SELECT id, SUM(clicks), MAX(max_price), MIN(min_price), COUNT(total_count), AVG(avg_score) " +
                        "FROM ck_jdbc.ck_db.ck_agg_table GROUP BY id";
        String plan = getFragmentPlan(sql);
        
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "sumMerge(`clicks`) AS Float64");
        assertContains(plan, "maxMerge(`max_price`) AS String");
        assertContains(plan, "minMerge(`min_price`) AS String");
        assertContains(plan, "countMerge(`total_count`) AS Int64");
        assertContains(plan, "avgMerge(`avg_score`) AS Float64");
    }

    @Test
    public void testSimpleAggregatePushdown() throws Exception {
        // Test SimpleAggregateFunction: it should push down as "sum", NOT "sumMerge"
        String sql = "SELECT SUM(simple_sum) FROM ck_jdbc.ck_db.ck_agg_table";
        String plan = getFragmentPlan(sql);
        
        assertContains(plan, "SCAN JDBC");
        // For SimpleAggregateFunction, ClickHouse uses standard sum()
        assertContains(plan, "sum(`simple_sum`) AS Float64");
    }

    @Test
    public void testFederatedJoinAggPushdown() throws Exception {
        // Create a local table
        starRocksAssert.withTable("CREATE TABLE local_olap (id int, name varchar(20)) " +
                "distributed by hash(id) properties('replication_num'='1')");
        
        String sql = "SELECT t1.name, sum(t2.clicks) " +
                     "FROM local_olap t1 JOIN ck_jdbc.ck_db.ck_agg_table t2 ON t1.id = t2.id " +
                     "GROUP BY t1.name";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "local_olap");
    }

    @Test
    public void testAggPushdownWithFilter() throws Exception {
        String sql = "SELECT SUM(clicks) FROM ck_jdbc.ck_db.ck_agg_table WHERE id > 10";
        try {
            getFragmentPlan(sql);
            org.junit.jupiter.api.Assertions.fail("Expected StarRocksPlannerException to be thrown");
        } catch (com.starrocks.sql.common.StarRocksPlannerException e) {
            org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains(
                    "Unsupported JDBC aggregation pushdown: the WHERE clause contains a column"));
        }
    }

    @Test
    public void testAggPushdownWithGroupByFilter() throws Exception {
        // 过滤字段 id 在 GROUP BY 中，应该正常下推且不报错
        String sql = "SELECT id, SUM(clicks) FROM ck_jdbc.ck_db.ck_agg_table WHERE id > 10 GROUP BY id";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "sumMerge(`clicks`) AS Float64");
        assertContains(plan, "WHERE (`id` > 10)");
    }

    @Test
    public void testGlobalAggPushdown() throws Exception {
        // 全局聚合（无 GROUP BY），验证 SQL 生成
        String sql = "SELECT SUM(clicks) FROM ck_jdbc.ck_db.ck_agg_table";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "sumMerge(`clicks`) AS Float64");
    }

    @Test
    public void testDistinctAggShouldNotPushdown() throws Exception {
        String sql = "SELECT SUM(DISTINCT clicks) FROM ck_jdbc.ck_db.ck_agg_table";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE");
        assertNotContains(plan, "sumMerge(`clicks`) AS Float64");
    }
}
