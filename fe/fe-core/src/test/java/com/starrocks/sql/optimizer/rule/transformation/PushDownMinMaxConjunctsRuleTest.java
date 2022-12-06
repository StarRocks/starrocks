// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import mockit.Mocked;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PushDownMinMaxConjunctsRuleTest {
    @Test
    public void transformIceberg(@Mocked IcebergTable table) {
        RemoteScanPartitionPruneRule rule0 = RemoteScanPartitionPruneRule.ICEBERG_SCAN;
        PushDownMinMaxConjunctsRule rule1 = PushDownMinMaxConjunctsRule.ICEBERG_SCAN;

        PredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(
                BinaryPredicateOperator.BinaryType.EQ, new ColumnRefOperator(1, Type.INT, "id", true),
                ConstantOperator.createInt(1));

        OptExpression scan =
                new OptExpression(new LogicalIcebergScanOperator(table,
                                Maps.newHashMap(), Maps.newHashMap(), -1, binaryPredicateOperator));
        scan.getInputs().add(scan);

        assertEquals(0, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());

        rule0.transform(scan, new OptimizerContext(new Memo(), new ColumnRefFactory()));
        rule1.transform(scan, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(2, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());
    }
}
