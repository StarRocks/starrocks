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

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Test;

import java.util.Collections;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.FunctionSet.BITMAP_AGG;
import static com.starrocks.catalog.FunctionSet.BITMAP_UNION;
import static com.starrocks.catalog.FunctionSet.BITMAP_UNION_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BitmapRewriteEquivalentTest {

    private ScalarOperator createConstantOperator(Object object, Type type) {
        return new ConstantOperator(object, type);
    }

    private ScalarOperator createBitmapHashFunc(ScalarOperator arg0) {
        return new CallOperator(FunctionSet.BITMAP_HASH, Type.BITMAP, Collections.singletonList(arg0),
                Expr.getBuiltinFunction(FunctionSet.BITMAP_HASH, new Type[] {Type.BIGINT}, IS_IDENTICAL));
    }

    private ScalarOperator createBitmapHash64Func(ScalarOperator arg0) {
        return new CallOperator(FunctionSet.BITMAP_HASH64, Type.BITMAP, Collections.singletonList(arg0),
                Expr.getBuiltinFunction(FunctionSet.BITMAP_HASH64, new Type[] {Type.BIGINT}, IS_IDENTICAL));
    }

    private ScalarOperator createToBitmapFunc(ScalarOperator arg0) {
        return new CallOperator(FunctionSet.TO_BITMAP, Type.BITMAP, Collections.singletonList(arg0),
                Expr.getBuiltinFunction(FunctionSet.TO_BITMAP, new Type[] {Type.BIGINT}, IS_IDENTICAL));
    }

    private ScalarOperator createBitmapFromString(ScalarOperator arg0) {
        return new CallOperator(FunctionSet.BITMAP_FROM_STRING, Type.BITMAP, Collections.singletonList(arg0),
                Expr.getBuiltinFunction(FunctionSet.BITMAP_FROM_STRING, new Type[] {Type.STRING}, IS_IDENTICAL));
    }

    private ScalarOperator createBitmapUnionFunc(ScalarOperator arg0) {
        return new CallOperator(BITMAP_UNION, Type.BITMAP,
                Collections.singletonList(arg0), Expr.getBuiltinFunction(BITMAP_UNION, new Type[] {Type.BITMAP},
                IS_IDENTICAL));
    }

    private ScalarOperator createBitmapUnionCountFunc(ScalarOperator arg0) {
        return new CallOperator(BITMAP_UNION_COUNT, Type.BITMAP,
                Collections.singletonList(arg0), Expr.getBuiltinFunction(BITMAP_UNION, new Type[] {Type.BITMAP},
                IS_IDENTICAL));
    }

    private CallOperator createBitmapAggFunc(ScalarOperator arg0) {
        return new CallOperator(BITMAP_AGG, Type.BITMAP,
                Collections.singletonList(arg0), Expr.getBuiltinFunction(BITMAP_AGG, new Type[] {Type.BITMAP},
                IS_IDENTICAL));
    }

    @Test
    public void testRewriteEquivalentPrepare() {
        BitmapRewriteEquivalent bitmapRewriteEquivalent = new BitmapRewriteEquivalent();

        assertNull(bitmapRewriteEquivalent.prepare(null));
        assertNull(bitmapRewriteEquivalent.prepare(createConstantOperator("hello", Type.STRING)));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapHashFunc(createConstantOperator("hello", Type.STRING))));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapHash64Func(createConstantOperator("hello", Type.STRING))));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapUnionFunc(null)));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapUnionFunc(createConstantOperator("hello", Type.STRING))));
        assertNull(bitmapRewriteEquivalent.prepare(createBitmapAggFunc(null)));

        {
            ScalarOperator constant = createConstantOperator("hello", Type.STRING);
            ScalarOperator toBitmap = createToBitmapFunc(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(toBitmap);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", Type.STRING);
            ScalarOperator bitmapHash = createBitmapHashFunc(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapHash);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", Type.STRING);
            ScalarOperator bitmapHash64 = createBitmapHash64Func(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapHash64);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", Type.STRING);
            ScalarOperator bitmapFromString = createBitmapFromString(constant);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(bitmapFromString);
            // bitmap_union(bitmap_from_string()) cannot be rewrite.
            assertNull(bitmapRewriteEquivalent.prepare(bitmapUnion));
        }

        {
            ScalarOperator constant = createConstantOperator("mocked", Type.BITMAP);
            ScalarOperator bitmapUnion = createBitmapUnionFunc(constant);
            IRewriteEquivalent.RewriteEquivalentContext context = bitmapRewriteEquivalent.prepare(bitmapUnion);
            assertNotNull(context);
            assertEquals(context.getEquivalent(), constant);
            assertEquals(context.getInput(), bitmapUnion);
        }

        {
            ScalarOperator constant = createConstantOperator("hello", Type.STRING);
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

        ScalarOperator constant = createConstantOperator("hello", Type.STRING);
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
