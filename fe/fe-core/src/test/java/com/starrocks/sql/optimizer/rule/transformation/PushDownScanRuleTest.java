// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mocked;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PushDownScanRuleTest {

    @Test
    public void transform(@Mocked OlapTable table) {
        PushDownPredicateScanRule rule = PushDownPredicateScanRule.OLAP_SCAN;

        OptExpression optExpression = new OptExpression(new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                        new ColumnRefOperator(1, Type.INT, "id", true),
                        ConstantOperator.createInt(1))
        ));

        OptExpression scan =
                new OptExpression(new LogicalOlapScanOperator(table, Maps.newHashMap()));
        optExpression.getInputs().add(scan);

        assertNull(((LogicalOlapScanOperator) scan.getOp()).getPredicate());
        List<OptExpression> result =
                rule.transform(optExpression, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(OperatorType.BINARY, ((LogicalOlapScanOperator) scan.getOp()).getPredicate().getOpType());

        assertEquals(OperatorType.VARIABLE,
                ((LogicalOlapScanOperator) scan.getOp()).getPredicate().getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT,
                ((LogicalOlapScanOperator) scan.getOp()).getPredicate().getChild(1).getOpType());
    }
}