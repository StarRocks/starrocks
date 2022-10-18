// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ReplaceColumnRefRewriterTest {
    @Test
    public void testRecursiveWithoutChildren() {
        Map<ColumnRefOperator, ScalarOperator> operatorMap = Maps.newHashMap();
        ColumnRefOperator columnRef1 = createColumnRef(1);
        ColumnRefOperator columnRef2 = createColumnRef(2);
        ColumnRefOperator columnRef3 = createColumnRef(3);
        operatorMap.put(columnRef1, columnRef2);
        operatorMap.put(columnRef2, columnRef3);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap, true);
        ColumnRefOperator source = createColumnRef(1);
        ScalarOperator target = rewriter.rewrite(source);
        Assert.assertTrue(target instanceof ColumnRefOperator);
        ColumnRefOperator rewritten = (ColumnRefOperator) target;
        Assert.assertEquals(3, rewritten.getId());
    }

    @Test
    public void testRecursiveWithChildren() {
        Map<ColumnRefOperator, ScalarOperator> operatorMap = Maps.newHashMap();
        ColumnRefOperator columnRef1 = createColumnRef(1);
        ColumnRefOperator columnRef2 = createColumnRef(2);
        ColumnRefOperator columnRef3 = createColumnRef(3);

        BinaryPredicateOperator binary = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, columnRef2,
                ConstantOperator.createInt(1));

        operatorMap.put(columnRef1, binary);
        operatorMap.put(columnRef2, columnRef3);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap, true);
        ColumnRefOperator source = createColumnRef(1);
        ScalarOperator target = rewriter.rewrite(source);
        Assert.assertTrue(target instanceof BinaryPredicateOperator);
        BinaryPredicateOperator rewritten = (BinaryPredicateOperator) target;

        BinaryPredicateOperator result = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, columnRef3,
                ConstantOperator.createInt(1));
        Assert.assertEquals(result, rewritten);
    }

    ColumnRefOperator createColumnRef(int id) {
        return new ColumnRefOperator(id, Type.INT, "ref" + id, false);
    }
}
