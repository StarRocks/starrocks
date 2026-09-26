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

import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIndexScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PushDownScanRuleTest {

    @Test
    public void transform(@Mocked OlapTable table) {
        PushDownPredicateScanRule rule = new PushDownPredicateScanRule();

        OptExpression optExpression = new OptExpression(new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryType.EQ,
                        new ColumnRefOperator(1, IntegerType.INT, "id", true),
                        ConstantOperator.createInt(1))
        ));

        OptExpression scan =
                new OptExpression(
                        new LogicalOlapScanOperator(table, Maps.newHashMap(), Maps.newHashMap(), null, -1, null));
        optExpression.getInputs().add(scan);

        assertNull(((LogicalOlapScanOperator) scan.getOp()).getPredicate());
        List<OptExpression> result =
                rule.transform(optExpression, OptimizerFactory.mockContext(new ColumnRefFactory()));

        Operator scanOperator = result.get(0).inputAt(0).getOp();

        assertEquals(OperatorType.BINARY, scanOperator.getPredicate().getOpType());

        assertEquals(OperatorType.VARIABLE,
                scanOperator.getPredicate().getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT,
                scanOperator.getPredicate().getChild(1).getOpType());
    }

    @Test
    public void transformIndexScan(@Mocked Table innerTable) {
        PushDownPredicateScanRule rule = new PushDownPredicateScanRule();
        ColumnRefOperator args = new ColumnRefOperator(1, IntegerType.INT, "args", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, args, ConstantOperator.createInt(1));
        OptExpression filter = OptExpression.create(new LogicalFilterOperator(predicate),
                OptExpression.create(new LogicalIndexScanOperator(
                        new IndexTable(innerTable), Maps.newHashMap(), Maps.newHashMap(), -1, null)));

        List<OptExpression> result = rule.transform(
                filter, OptimizerFactory.mockContext(new ColumnRefFactory()));

        Operator scanOperator = result.get(0).inputAt(0).getOp();
        assertEquals(OperatorType.LOGICAL_INDEX_SCAN, scanOperator.getOpType());
        assertEquals(predicate, scanOperator.getPredicate());
    }
}
