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

package com.starrocks.connector.delta;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExpressionConvertTest {
    private static StructType tableSchema;

    @BeforeClass
    public static void setUp() {
        tableSchema = new StructType(
                Lists.newArrayList(
                        new StructField("name", BasePrimitiveType.createPrimitive("string"), true),
                        new StructField("age", BasePrimitiveType.createPrimitive("integer"), true),
                        new StructField("salary", BasePrimitiveType.createPrimitive("double"), true),
                        new StructField("birthday", BasePrimitiveType.createPrimitive("date"), true)));
    }

    @Test
    public void testIsNullPredicate() {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), "name", ScalarType.STRING, true);
        slotDescriptor.setColumn(new Column("name", ScalarType.STRING));
        SlotRef name = new SlotRef(slotDescriptor);
        // Test is not null predicate
        IsNullPredicate isNullPredicate = new IsNullPredicate(name, true);
        Expression expression = new ExpressionConverter(tableSchema).convert(isNullPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("IS_NOT_NULL(column(`name`))", expression.toString());

        // Test is null predicate
        isNullPredicate = new IsNullPredicate(name, false);
        expression = new ExpressionConverter(tableSchema).convert(isNullPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("IS_NULL(column(`name`))", expression.toString());
    }

    @Test
    public void testBinaryPredicate() throws AnalysisException {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), "age", ScalarType.INT, true);
        slotDescriptor.setColumn(new Column("age", ScalarType.INT));
        SlotRef age = new SlotRef(slotDescriptor);

        // Test equal predicate
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.EQ, age, LiteralExpr.create("1", ScalarType.INT));
        Expression expression = new ExpressionConverter(tableSchema).convert(binaryPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("(column(`age`) = 1)", expression.toString());

        // Test not equal predicate
        binaryPredicate = new BinaryPredicate(BinaryType.NE, age, LiteralExpr.create("1", ScalarType.INT));
        expression = new ExpressionConverter(tableSchema).convert(binaryPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("NOT((column(`age`) = 1))", expression.toString());

        // Test greater than predicate
        binaryPredicate = new BinaryPredicate(BinaryType.GT, age, LiteralExpr.create("1", ScalarType.INT));
        expression = new ExpressionConverter(tableSchema).convert(binaryPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("(column(`age`) > 1)", expression.toString());

        // Test greater than or equal predicate
        binaryPredicate = new BinaryPredicate(BinaryType.GE, age, LiteralExpr.create("1", ScalarType.INT));
        expression = new ExpressionConverter(tableSchema).convert(binaryPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("(column(`age`) >= 1)", expression.toString());

        // Test less than predicate
        binaryPredicate = new BinaryPredicate(BinaryType.LT, age, LiteralExpr.create("1", ScalarType.INT));
        expression = new ExpressionConverter(tableSchema).convert(binaryPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("(column(`age`) < 1)", expression.toString());

        // Test less than or equal predicate
        binaryPredicate = new BinaryPredicate(BinaryType.LE, age, LiteralExpr.create("1", ScalarType.INT));
        expression = new ExpressionConverter(tableSchema).convert(binaryPredicate);
        Assert.assertNotNull(expression);
        Assert.assertEquals("(column(`age`) <= 1)", expression.toString());
    }
}
