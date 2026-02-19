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

package com.starrocks.sql.optimizer.rule.implementation;

import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.catalog.Column;
import com.starrocks.connector.benchmark.BenchmarkCatalogConfig;
import com.starrocks.connector.benchmark.BenchmarkConfig;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalBenchmarkScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalBenchmarkScanOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkScanImplementationRuleTest {

    @Test
    public void testTransform() {
        LogicalBenchmarkScanOperator logical =
                new LogicalBenchmarkScanOperator(newBenchmarkTable(), Map.of(), Map.of(), -1, null, null);
        List<OptExpression> output = new BenchmarkScanImplementationRule().transform(new OptExpression(logical), null);

        Assertions.assertEquals(1, output.size());
        Assertions.assertTrue(output.get(0).getOp() instanceof PhysicalBenchmarkScanOperator);
        PhysicalBenchmarkScanOperator physical = (PhysicalBenchmarkScanOperator) output.get(0).getOp();
        Assertions.assertEquals(logical.getTable(), physical.getTable());
    }

    private static BenchmarkTable newBenchmarkTable() {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        BenchmarkConfig config = new BenchmarkConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put(BenchmarkConfig.SCALE, "1");
        config.loadConfig(properties);
        BenchmarkCatalogConfig catalogConfig = BenchmarkCatalogConfig.from(config);
        return new BenchmarkTable(1L, "benchmark", "tpcds", "store_sales", schema, catalogConfig);
    }
}
