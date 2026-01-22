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

package com.starrocks.connector.benchmark;

import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BenchmarkConnectorTest {

    @Test
    public void testCatalogConfigParsesScaleFactor() {
        BenchmarkConfig config = new BenchmarkConfig();
        config.loadConfig(singleScaleConfig("2.5"));

        BenchmarkCatalogConfig catalogConfig = BenchmarkCatalogConfig.from(config);
        Assertions.assertEquals(2.5, catalogConfig.getScaleFactor(), 0.0001);
    }

    @Test
    public void testCatalogConfigRejectsInvalidScale() {
        Assertions.assertThrows(StarRocksConnectorException.class, () -> BenchmarkCatalogConfig.from(null));

        BenchmarkConfig config = new BenchmarkConfig();
        config.loadConfig(singleScaleConfig("not-a-number"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> BenchmarkCatalogConfig.from(config));

        BenchmarkConfig zeroConfig = new BenchmarkConfig();
        zeroConfig.loadConfig(singleScaleConfig("0"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> BenchmarkCatalogConfig.from(zeroConfig));
    }

    @Test
    public void testConnectorCreatesMetadata() {
        BenchmarkConnector connector = new BenchmarkConnector(
                new ConnectorContext("benchmark_catalog", "benchmark", Map.of()));
        BenchmarkConfig config = new BenchmarkConfig();
        config.loadConfig(singleScaleConfig("2"));
        connector.bindConfig(config);

        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof BenchmarkMetadata);
        Assertions.assertSame(metadata, connector.getMetadata());
    }

    @Test
    public void testMetadataAndSchemas() {
        BenchmarkConfig config = new BenchmarkConfig();
        config.loadConfig(singleScaleConfig("2.5"));
        BenchmarkMetadata metadata = new BenchmarkMetadata("benchmark_catalog", BenchmarkCatalogConfig.from(config));

        List<String> dbNames = metadata.listDbNames(new ConnectContext());
        Set<String> expected = Set.of("tpcds", "tpch", "ssb");
        Assertions.assertEquals(expected, new HashSet<>(dbNames));

        Database db = metadata.getDb(new ConnectContext(), "TPCDS");
        Assertions.assertNotNull(db);
        Assertions.assertEquals("tpcds", db.getFullName());
        Assertions.assertEquals("benchmark_catalog", db.getCatalogName());

        List<String> tables = metadata.listTableNames(new ConnectContext(), "tpch");
        Assertions.assertTrue(tables.contains("part"));

        Table table = metadata.getTable(new ConnectContext(), "TPCH", "PART");
        Assertions.assertTrue(table instanceof BenchmarkTable);
        BenchmarkTable benchmarkTable = (BenchmarkTable) table;
        Assertions.assertEquals("benchmark_catalog", benchmarkTable.getCatalogName());
        Assertions.assertEquals("tpch", benchmarkTable.getCatalogDBName());
        Assertions.assertEquals("part", benchmarkTable.getCatalogTableName());
        Assertions.assertEquals(2.5, benchmarkTable.getScaleFactor(), 0.0001);

        Assertions.assertNull(metadata.getTable(new ConnectContext(), "tpch", "missing"));

        BenchmarkTableSchemas schemas = new BenchmarkTableSchemas(null);
        List<Column> columns = schemas.getTableSchema("TPCH", "PART");
        Assertions.assertNotNull(columns);
        Column partKey = findColumn(columns, "p_partkey");
        Assertions.assertEquals(IntegerType.BIGINT, partKey.getType());

        Column retailPrice = findColumn(columns, "p_retailprice");
        Type retailType = retailPrice.getType();
        Assertions.assertTrue(retailType instanceof ScalarType);
        ScalarType decimalType = (ScalarType) retailType;
        Assertions.assertEquals(15, decimalType.decimalPrecision());
        Assertions.assertEquals(2, decimalType.decimalScale());
    }

    @Test
    public void testRowCountEstimator() {
        RowCountEstimate estimate = BenchmarkRowCountCalculator.estimateRowCount("tpcds", "store_sales", 1.0);
        Assertions.assertTrue(estimate.isKnown());
        Assertions.assertEquals(2_880_404L, estimate.getRowCount());

        long defaultEstimate = BenchmarkRowCountCalculator.estimateRowCount("store_sales", 1.0);
        Assertions.assertEquals(estimate.getRowCount(), defaultEstimate);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkRowCountCalculator.estimateRowCount("tpcds", "store_sales", 0.0));
    }

    @Test
    public void testSuiteFactoryUnknown() {
        BenchmarkSuite suite = BenchmarkSuiteFactory.getSuite("TPCDS");
        Assertions.assertEquals("tpcds", suite.getName());
        Assertions.assertNull(BenchmarkSuiteFactory.getSuiteIfExists("missing"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> BenchmarkSuiteFactory.getSuite("missing"));
    }

    private static Column findColumn(List<Column> columns, String name) {
        return columns.stream()
                .filter(column -> column.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Missing column " + name));
    }

    private static Map<String, String> singleScaleConfig(String scale) {
        Map<String, String> properties = new HashMap<>();
        properties.put(BenchmarkConfig.SCALE, scale);
        return properties;
    }
}
