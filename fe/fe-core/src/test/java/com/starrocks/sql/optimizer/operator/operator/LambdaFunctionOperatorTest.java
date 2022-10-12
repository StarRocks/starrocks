// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

public class LambdaFunctionOperatorTest {
    @Test
    public void lambdaFunction() {
        ScalarOperator lambdaExpr = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                new ColumnRefOperator(1, Type.INT, "x", true),
                ConstantOperator.createInt(1));
        ColumnRefOperator colRef = new ColumnRefOperator(1, Type.INT, "x", true, true);
        LambdaFunctionOperator lambda = new LambdaFunctionOperator(Lists.newArrayList(colRef), lambdaExpr, Type.BOOLEAN);
        Assert.assertTrue(lambda.getChild(0).equals(lambdaExpr));
        Assert.assertTrue(lambda.getLambdaExpr().equals(lambdaExpr));
        Assert.assertTrue(lambda.getRefColumns().get(0).getName() == "x");
        Assert.assertTrue(lambda.getChildren().size() == 1);
        Assert.assertTrue(lambda.getUsedColumns().getFirstId() == 1);
        Assert.assertTrue(lambda.isNullable());
        Assert.assertEquals("([1: x]->1: x = 1)", lambda.toString());
        Assert.assertTrue(lambda.equals(lambda.clone()));
    }
}
