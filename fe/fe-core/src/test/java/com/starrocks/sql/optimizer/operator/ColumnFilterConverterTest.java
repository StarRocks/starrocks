// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator;

import com.google.common.collect.Lists;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnFilterConverterTest {

    @Test
    public void convertColumnFilterNormal() {
        ScalarOperator root1 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                new ColumnRefOperator(1, Type.INT, "age", true),
                ConstantOperator.createInt(1));

        ScalarOperator root2 = new InPredicateOperator(new ColumnRefOperator(2, Type.INT, "name", true),
                ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3"),
                ConstantOperator.createVarchar("4"));

        ScalarOperator root3 = new IsNullPredicateOperator(new ColumnRefOperator(3, Type.BOOLEAN, "sex", true));

        ScalarOperator root4 = ConstantOperator.createBoolean(true);

        ScalarOperator root5 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                ConstantOperator.createInt(2),
                ConstantOperator.createInt(1));

        ScalarOperator root6 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                new ColumnRefOperator(4, Type.INT, "value1", true),
                new ColumnRefOperator(5, Type.INT, "value2", true));

        List<ScalarOperator> list = Lists.newArrayList(root1, root2, root3, root4, root5, root6);

        Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);

        assertEquals(3, result.size());

        assertTrue(result.containsKey("age"));
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("sex"));

        assertEquals(new IntLiteral(1), result.get("age").lowerBound);
        assertEquals(new IntLiteral(1), result.get("age").upperBound);

        assertEquals(4, result.get("name").getInPredicateLiterals().size());
        assertEquals(new StringLiteral("1"), result.get("name").getInPredicateLiterals().get(0));
        assertEquals(new StringLiteral("2"), result.get("name").getInPredicateLiterals().get(1));
        assertEquals(new StringLiteral("3"), result.get("name").getInPredicateLiterals().get(2));
        assertEquals(new StringLiteral("4"), result.get("name").getInPredicateLiterals().get(3));

        assertEquals(new NullLiteral(), result.get("sex").lowerBound);
        assertEquals(new NullLiteral(), result.get("sex").upperBound);
    }

    @Test
    public void testIsNullOnCastColumn() {
        {
            // cast(c0 as smallint) is null.
            IsNullPredicateOperator isNullPredicate = new IsNullPredicateOperator(false,
                    new CastOperator(Type.SMALLINT, new ColumnRefOperator(1, Type.INT, "c0", true)));
            List<ScalarOperator> list = Lists.newArrayList(isNullPredicate);
            Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);
            assertEquals(result.size(), 0);
        }
        {
            // c0 is null.
            IsNullPredicateOperator isNullPredicate =
                    new IsNullPredicateOperator(false, new ColumnRefOperator(1, Type.INT, "c0", true));
            List<ScalarOperator> list = Lists.newArrayList(isNullPredicate);
            Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);
            assertEquals(result.size(), 1);
            PartitionColumnFilter filter = result.get("c0");
            assertEquals(filter.lowerBound, new NullLiteral());
            assertEquals(filter.upperBound, new NullLiteral());
        }
    }
}