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

package com.starrocks.sql.optimizer.rule;

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalADBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalADBCScanOperator;
import com.starrocks.sql.optimizer.rule.implementation.ADBCScanImplementationRule;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ADBCScanImplementationRuleTest {

    @Test
    public void testRuleType() {
        ADBCScanImplementationRule rule = new ADBCScanImplementationRule();
        Assertions.assertEquals(RuleType.IMP_ADBC_LSCAN_TO_PSCAN, rule.type());
    }

    @Test
    public void testRulePatternMatchesLogicalADBCScan() {
        ADBCScanImplementationRule rule = new ADBCScanImplementationRule();
        Assertions.assertEquals(OperatorType.LOGICAL_ADBC_SCAN, rule.getPattern().getOpType());
    }

    @Test
    public void testTransformLogicalToPhysical() {
        ADBCScanImplementationRule rule = new ADBCScanImplementationRule();

        ADBCTable table = newADBCTable();
        LogicalADBCScanOperator logical =
                new LogicalADBCScanOperator(table, Map.of(), Map.of(), -1, null, null);
        OptExpression input = new OptExpression(logical);

        OptimizerContext context = Mockito.mock(OptimizerContext.class);
        List<OptExpression> results = rule.transform(input, context);

        Assertions.assertEquals(1, results.size());
        Assertions.assertInstanceOf(PhysicalADBCScanOperator.class, results.get(0).getOp());
        PhysicalADBCScanOperator physical = (PhysicalADBCScanOperator) results.get(0).getOp();
        Assertions.assertEquals(OperatorType.PHYSICAL_ADBC_SCAN, physical.getOpType());
    }

    private static ADBCTable newADBCTable() {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        Map<String, String> properties = new HashMap<>();
        properties.put("adbc.driver", "flight_sql");
        properties.put("adbc.uri", "grpc://localhost:12345");
        return new ADBCTable(1L, "test_table", schema, "test_db", "test_catalog", properties);
    }
}
