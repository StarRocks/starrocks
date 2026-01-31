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

package com.starrocks.catalog;

import com.starrocks.connector.benchmark.BenchmarkCatalogConfig;
import com.starrocks.connector.benchmark.BenchmarkConfig;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkTableTest {

    @Test
    public void testBenchmarkTableToThrift() {
        BenchmarkTable table = newBenchmarkTable(101L, 3.0);

        Assertions.assertEquals("benchmark", table.getCatalogName());
        Assertions.assertEquals("tpch", table.getCatalogDBName());
        Assertions.assertEquals("part", table.getCatalogTableName());
        Assertions.assertEquals(3.0, table.getScaleFactor(), 0.0001);
        Assertions.assertTrue(table.getPartitionColumns().isEmpty());
        Assertions.assertTrue(table.getPartitionColumnNames().isEmpty());

        TTableDescriptor actual = table.toThrift(List.of());
        TTableDescriptor expected =
                new TTableDescriptor(101L, TTableType.BENCHMARK_TABLE, 1, 0, "part", "tpch");
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testBenchmarkPartitionKey() {
        BenchmarkPartitionKey key = new BenchmarkPartitionKey();
        Assertions.assertEquals(List.of("null"), key.nullPartitionValueList());
    }

    private static BenchmarkTable newBenchmarkTable(long id, double scale) {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        BenchmarkConfig config = new BenchmarkConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put(BenchmarkConfig.SCALE, Double.toString(scale));
        config.loadConfig(properties);
        BenchmarkCatalogConfig catalogConfig = BenchmarkCatalogConfig.from(config);
        return new BenchmarkTable(id, "benchmark", "tpch", "part", schema, catalogConfig);
    }
}
