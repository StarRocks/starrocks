// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PruneHDFSScanColumnRuleTest {
    private PruneHDFSScanColumnRule icebergRule = PruneHDFSScanColumnRule.ICEBERG_SCAN;

    ColumnRefOperator intColumnOperator = new ColumnRefOperator(1, Type.INT, "id", true);
    ColumnRefOperator strColumnOperator = new ColumnRefOperator(2, Type.STRING, "name", true);

    Map<ColumnRefOperator, Column> scanColumnMap = new HashMap<ColumnRefOperator, Column>() {{
        put(intColumnOperator, new Column("id", Type.INT));
        put(strColumnOperator, new Column("name", Type.STRING));
    }};

    @Test
    public void transformIcebergWithPredicate(@Mocked IcebergTable table,
                                 @Mocked OptimizerContext context,
                                 @Mocked TaskContext taskContext) {
        OptExpression scan = new OptExpression(
                        new LogicalIcebergScanOperator(table, Table.TableType.ICEBERG,
                                scanColumnMap, Maps.newHashMap(), -1,
                                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                        new ColumnRefOperator(1, Type.INT, "id", true),
                                        ConstantOperator.createInt(1))));

        List<TaskContext> taskContextList = new ArrayList<>();
        taskContextList.add(taskContext);

        ColumnRefSet requiredOutputColumns = new ColumnRefSet(new ArrayList<>(
                Collections.singleton(new ColumnRefOperator(1, Type.INT, "id", true))));

        doIcebergTransform(scan, context, requiredOutputColumns, taskContextList, taskContext);
    }

    @Test
    public void transformIcebergWithNoScanColumn(@Mocked IcebergTable table,
                                              @Mocked OptimizerContext context,
                                              @Mocked TaskContext taskContext) {
        OptExpression scan = new OptExpression(
                        new LogicalIcebergScanOperator(table, Table.TableType.ICEBERG,
                                scanColumnMap, Maps.newHashMap(), -1, null));

        List<TaskContext> taskContextList = new ArrayList<>();
        taskContextList.add(taskContext);

        ColumnRefSet requiredOutputColumns = new ColumnRefSet(new ArrayList<>());

        doIcebergTransform(scan, context, requiredOutputColumns, taskContextList, taskContext);
    }

    private void doIcebergTransform(OptExpression scan,
                                    OptimizerContext context,
                                    ColumnRefSet requiredOutputColumns,
                                    List<TaskContext> taskContextList,
                                    TaskContext taskContext) {
        new Expectations() { {
            context.getTaskContext();
            minTimes = 0;
            result = taskContextList;

            taskContext.getRequiredColumns();
            minTimes = 0;
            result = requiredOutputColumns;
        }};
        List<OptExpression> list = icebergRule.transform(scan, context);
        Map<ColumnRefOperator, Column> transferMap = ((LogicalIcebergScanOperator)list.get(0)
                .getOp()).getColRefToColumnMetaMap();
        Assert.assertEquals(transferMap.size(), 1);
        Assert.assertEquals(transferMap.get(intColumnOperator).getName(), "id");
    }
}
