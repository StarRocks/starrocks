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

package com.starrocks.metric;

import com.starrocks.catalog.Table;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CatalogQueryMetricsTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testToCatalogTypeDefault() {
        assertEquals("default", StmtExecutor.toCatalogType(Table.TableType.OLAP));
        assertEquals("default", StmtExecutor.toCatalogType(Table.TableType.CLOUD_NATIVE));
        assertEquals("default", StmtExecutor.toCatalogType(Table.TableType.MATERIALIZED_VIEW));
        assertEquals("default", StmtExecutor.toCatalogType(Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW));
    }

    @Test
    public void testToCatalogTypeHive() {
        assertEquals("hive", StmtExecutor.toCatalogType(Table.TableType.HIVE));
        assertEquals("hive", StmtExecutor.toCatalogType(Table.TableType.HIVE_VIEW));
    }

    @Test
    public void testToCatalogTypeIceberg() {
        assertEquals("iceberg", StmtExecutor.toCatalogType(Table.TableType.ICEBERG));
        assertEquals("iceberg", StmtExecutor.toCatalogType(Table.TableType.ICEBERG_VIEW));
    }

    @Test
    public void testToCatalogTypeOther() {
        assertEquals("hudi", StmtExecutor.toCatalogType(Table.TableType.HUDI));
        assertEquals("deltalake", StmtExecutor.toCatalogType(Table.TableType.DELTALAKE));
        assertEquals("jdbc", StmtExecutor.toCatalogType(Table.TableType.JDBC));
        assertEquals("paimon", StmtExecutor.toCatalogType(Table.TableType.PAIMON));
        assertEquals("odps", StmtExecutor.toCatalogType(Table.TableType.ODPS));
        assertEquals("kudu", StmtExecutor.toCatalogType(Table.TableType.KUDU));
        assertEquals("elasticsearch", StmtExecutor.toCatalogType(Table.TableType.ELASTICSEARCH));
    }

    @Test
    public void testToCatalogTypeUnknownDefaultsToDefault() {
        assertEquals("default", StmtExecutor.toCatalogType(Table.TableType.SCHEMA));
    }

    @Test
    public void testCatalogQueryMetrics() {
        MetricRepo.COUNTER_CATALOG_QUERY_TOTAL.getMetric("hive").increase(1L);
        MetricRepo.COUNTER_CATALOG_QUERY_TOTAL.getMetric("iceberg").increase(2L);
        assertEquals(1L, MetricRepo.COUNTER_CATALOG_QUERY_TOTAL.getMetric("hive").getValue().longValue());
        assertEquals(2L, MetricRepo.COUNTER_CATALOG_QUERY_TOTAL.getMetric("iceberg").getValue().longValue());
    }

    @Test
    public void testGetMetrics() {
        MetricWithLabelGroup<LongCounterMetric> testGroup = new MetricWithLabelGroup<>("test_label",
                () -> new LongCounterMetric("test_metric", MetricUnit.REQUESTS, "test"));
        testGroup.getMetric("a").increase(1L);
        testGroup.getMetric("b").increase(2L);
        Map<String, LongCounterMetric> metrics = testGroup.getMetrics();
        assertEquals(2, metrics.size());
        assertTrue(metrics.containsKey("a"));
        assertTrue(metrics.containsKey("b"));
    }
}
