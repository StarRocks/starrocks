// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mocked;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNull;

public class PushDownMinMaxConjunctsRuleTest {
    @Test
    public void transform(@Mocked IcebergTable table) {
        RemoteScanPartitionPruneRule rule0 = RemoteScanPartitionPruneRule.ICEBERG_SCAN;
        PushDownMinMaxConjunctsRule rule1 = PushDownMinMaxConjunctsRule.ICEBERG_SCAN;

        OptExpression optExpression = new OptExpression(new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                        new ColumnRefOperator(1, Type.INT, "id", true),
                        ConstantOperator.createInt(1))
        ));

        OptExpression scan =
                new OptExpression(new LogicalIcebergScanOperator(table, Table.TableType.ICEBERG,
                                Maps.newHashMap(), Maps.newHashMap(), -1, null));
        optExpression.getInputs().add(scan);

        assertNull(((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts());
        List<OptExpression> result =
                rule0.transform(optExpression, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        Operator scanOperator = result.get(0).inputAt(0).getOp();

        //assertEquals(OperatorType.BINARY, (LogicalIcebergScanOperator) scan.getOp()));
    }
}
