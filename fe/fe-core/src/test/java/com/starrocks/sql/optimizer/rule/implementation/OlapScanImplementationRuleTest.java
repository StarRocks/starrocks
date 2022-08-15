// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mocked;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OlapScanImplementationRuleTest {

    @Test
    public void transform(@Mocked OlapTable table) {
        LogicalOlapScanOperator logical = new LogicalOlapScanOperator(table, Maps.newHashMap(), Maps.newHashMap(),
                null, -1, ConstantOperator.createBoolean(true),
                1, Lists.newArrayList(1L, 2L, 3L), null,
                Lists.newArrayList(4L), null);

        List<OptExpression> output =
                new OlapScanImplementationRule().transform(new OptExpression(logical), new OptimizerContext(
                        new Memo(), new ColumnRefFactory()));

        assertEquals(1, output.size());

        PhysicalOlapScanOperator physical = (PhysicalOlapScanOperator) output.get(0).getOp();
        assertEquals(1, physical.getSelectedIndexId());

        assertEquals(3, physical.getSelectedPartitionId().size());
        assertEquals(1, physical.getSelectedTabletId().size());
        assertEquals(ConstantOperator.createBoolean(true), physical.getPredicate());
    }

}