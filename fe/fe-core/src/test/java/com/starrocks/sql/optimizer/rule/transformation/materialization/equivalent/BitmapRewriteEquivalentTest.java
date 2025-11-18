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

package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BitmapType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BitmapRewriteEquivalentTest {

    private ScalarOperator createConstantOperator(Object object, Type type) {
        return new ConstantOperator(object, type);
    }

    private CallOperator createCallOperatorBase(CallOperator op, ScalarOperator arg0) {
        op.getArguments().add(arg0);
        return op;
    }

    private ScalarOperator createBitmapHashFunc(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.BITMAP_HASH, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_HASH, new Type[] {IntegerType.BIGINT}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    private ScalarOperator createBitmapHash64Func(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.BITMAP_HASH64, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_HASH64, new Type[] {IntegerType.BIGINT}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    private ScalarOperator createToBitmapFunc(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.TO_BITMAP, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.TO_BITMAP, new Type[] {IntegerType.BIGINT}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    private ScalarOperator createBitmapFromString(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.BITMAP_FROM_STRING, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_FROM_STRING, new Type[] {StringType.STRING}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    private ScalarOperator createBitmapUnionFunc(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.BITMAP_UNION, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_UNION, new Type[] {BitmapType.BITMAP}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    private ScalarOperator createBitmapUnionCountFunc(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.BITMAP_UNION_COUNT, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_UNION_COUNT, new Type[] {BitmapType.BITMAP}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    private CallOperator createBitmapAggFunc(ScalarOperator arg0) {
        CallOperator op = new CallOperator(FunctionSet.BITMAP_AGG, BitmapType.BITMAP, new ArrayList<>(),
                ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_AGG, new Type[] {BitmapType.BITMAP}, IS_IDENTICAL));
        return createCallOperatorBase(op, arg0);
    }

    @Test
    public void testRewriteEquivalentPrepare() {
        BitmapRewriteEquivalent bitmapRewriteEquivalent = new BitmapRewriteEquivalent();

        assertNull(bitmapRewriteEquivalent.prepare(null));
        assertNull(bitmapRewriteEquivalent.prepare(createConstantOperator("hello", StringType.STRING)));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapHashFunc(createConstantOperator("hello", StringType.STRING))));
        assertNull(bitmapRewriteEquivalent.prepare(
                createBitmapHash64Func(createConstantOperator("hello", StringType.STRING))));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapUnionFunc(null)));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapUnionFunc(createConstantOperator("hello", StringType.STRING))));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapAggFunc(null)));

        {
            ScalarOperator constant = createConstantOperator("hello", StringType.STRING);
            ScalarOperator toBitmap = createToBitmapFunc(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(toBitmap);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", StringType.STRING);
            ScalarOperator bitmapHash = createBitmapHashFunc(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapHash);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", StringType.STRING);
            ScalarOperator bitmapHash64 = createBitmapHash64Func(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapHash64);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", StringType.STRING);
            ScalarOperator bitmapFromString = createBitmapFromString(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapFromString);
            // bitmap_union(bitmap_from_string()) cannot be rewrite.
            assertNull(bitmapRewriteEquivalent.prepare(bitmapUnion));
        }

        {
            ScalarOperator constant = createConstantOperator("mocked", BitmapType.BITMAP);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(constant);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", StringType.STRING);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(constant);
            ScalarOperator bitmapAgg = createBitmapAggFunc(bitmapUnion);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapAgg);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), bitmapUnion);
            assertEquals(context.getInput(), bitmapAgg);
        }
    }

    @Test
    public void testRewriteForBitmapHash64Function(@Mocked EquivalentShuttleContext shuttleContext,
                                                   @Injectable ColumnRefOperator columnRefOperator) {
        BitmapRewriteEquivalent bitmapRewriteEquivalent = new BitmapRewriteEquivalent();

        ScalarOperator constant = createConstantOperator("hello", StringType.STRING);
        ScalarOperator bitmapHash64 = createBitmapHash64Func(constant);
        ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapHash64);
        IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
        assertNotNull(context);
        assertEquals(context.getEquivalent(), constant);
        assertEquals(context.getInput(), bitmapUnion);

        {
            ScalarOperator newInput = createBitmapUnionCountFunc(bitmapHash64);
            ScalarOperator rewrite = bitmapRewriteEquivalent.rewrite(context, shuttleContext, columnRefOperator, newInput);
            assertNotNull(rewrite);
        }

        {
            ScalarOperator newInput = createBitmapUnionCountFunc(bitmapUnion);
            ScalarOperator rewrite = bitmapRewriteEquivalent.rewrite(context, shuttleContext, columnRefOperator, newInput);
            assertNull(rewrite);
        }
    }
}
