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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PullUpScanPredicateRuleTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPreserveScanOutputs(boolean hasOtherOutput) {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator v1 = factory.create("v1", IntegerType.BIGINT, true);
        ColumnRefOperator v2 = factory.create("v2", IntegerType.BIGINT, true);
        ColumnRefOperator autoFill = factory.create("auto_fill_col", IntegerType.TINYINT, false);
        Column c1 = new Column("v1", IntegerType.BIGINT);
        Column c2 = new Column("v2", IntegerType.BIGINT);
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(v1, c1);
        columns.put(v2, c2);
        Map<Column, ColumnRefOperator> refs = new HashMap<>();
        refs.put(c1, v1);
        refs.put(c2, v2);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GT,
                new CallOperator("add", IntegerType.BIGINT, ImmutableList.of(v1, v2)), ConstantOperator.createBigint(10));
        LogicalOlapScanOperator scan = new LogicalOlapScanOperator(new OlapTable(), columns, refs, null,
                Operator.DEFAULT_LIMIT, predicate);
        Map<ColumnRefOperator, ScalarOperator> outputs = new HashMap<>();
        outputs.put(autoFill, ConstantOperator.createTinyInt((byte) 1));
        if (hasOtherOutput) {
            outputs.put(v1, v1);
        }
        Map<ColumnRefOperator, ScalarOperator> originalOutputs = new HashMap<>(outputs);
        Projection projection = new Projection(outputs);
        scan.setProjection(projection);

        List<OptExpression> result = PullUpScanPredicateRule.OLAP_SCAN.transform(OptExpression.create(scan), context);

        Assertions.assertEquals(1, result.size());
        LogicalOlapScanOperator newScan = (LogicalOlapScanOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertTrue(newScan.getOutputColumns().containsAll(originalOutputs.keySet()));
        Assertions.assertNotNull(newScan.getProjection());
        Assertions.assertEquals(originalOutputs.get(autoFill), newScan.getProjection().getColumnRefMap().get(autoFill));
        Assertions.assertEquals(originalOutputs, projection.getColumnRefMap());
    }
}
