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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FunctionSetTest {

    private FunctionSet functionSet;

    private static final Type VARCHAR_ARRAY = new ArrayType(Type.VARCHAR);
    private static final Type TINYINT_ARRAY = new ArrayType(Type.TINYINT);
    private static final Type INT_ARRAY = new ArrayType(Type.INT);
    private static final Type DOUBLE_ARRAY = new ArrayType(Type.DOUBLE);
    private static final Type INT_ARRAY_ARRAY = new ArrayType(INT_ARRAY);
    private static final Type TINYINT_ARRAY_ARRAY = new ArrayType(TINYINT_ARRAY);

    @Before
    public void setUp() {
        functionSet = new FunctionSet();
        functionSet.init();
    }

    @Test
    public void testGetLagFunction() {
        Type[] argTypes1 = {ScalarType.DECIMALV2, ScalarType.TINYINT, ScalarType.TINYINT};
        Function lagDesc1 = new Function(new FunctionName(FunctionSet.LAG), argTypes1, ScalarType.INVALID, false);
        Function newFunction = functionSet.getFunction(lagDesc1, Function.CompareMode.IS_SUPERTYPE_OF);
        Type[] newArgTypes = newFunction.getArgs();
        Assert.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assert.assertTrue(newArgTypes[0].matchesType(ScalarType.DECIMALV2));

        Type[] argTypes2 = {ScalarType.VARCHAR, ScalarType.TINYINT, ScalarType.TINYINT};
        Function lagDesc2 = new Function(new FunctionName(FunctionSet.LAG), argTypes2, ScalarType.INVALID, false);
        newFunction = functionSet.getFunction(lagDesc2, Function.CompareMode.IS_SUPERTYPE_OF);
        newArgTypes = newFunction.getArgs();
        Assert.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assert.assertTrue(newArgTypes[0].matchesType(ScalarType.VARCHAR));
    }

    @Test
    public void testPolymorphicFunction() {
        // array_append(ARRAY<INT>, INT)
        Type[] argTypes = {INT_ARRAY, Type.INT};
        Function desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, TINYINT)
        argTypes = new Type[] {INT_ARRAY, Type.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, INT)
        argTypes = new Type[] {TINYINT_ARRAY, Type.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, DOUBLE)
        argTypes = new Type[] {INT_ARRAY, Type.DOUBLE};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(DOUBLE_ARRAY, fn.getReturnType());
        Assert.assertEquals(DOUBLE_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.DOUBLE, fn.getArgs()[1]);

        // array_append(NULL, INT)
        argTypes = new Type[] {Type.NULL, Type.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, NULL)
        argTypes = new Type[] {INT_ARRAY, Type.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, VARCHAR)
        argTypes = new Type[] {TINYINT_ARRAY, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assert.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.VARCHAR, fn.getArgs()[1]);

        // array_append(NULL, NULL)
        argTypes = new Type[] {Type.NULL, Type.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(new ArrayType(Type.NULL), fn.getReturnType());
        Assert.assertEquals(new ArrayType(Type.NULL), fn.getArgs()[0]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<TINYINT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, TINYINT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<INT>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, NULL)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, Type.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(TINYINT_ARRAY_ARRAY, fn.getReturnType());
        Assert.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(TINYINT_ARRAY, fn.getArgs()[1]);

        // array_append(NULL, ARRAY<INT>)
        argTypes = new Type[] {Type.NULL, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, TINYINT)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, Type.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNull(fn);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<VARCHAR>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, VARCHAR_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getReturnType());
        Assert.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getArgs()[0]);
        Assert.assertEquals(VARCHAR_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, VARCHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assert.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.VARCHAR, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, CHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, Type.CHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assert.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assert.assertEquals(Type.VARCHAR, fn.getArgs()[1]);

        // array_append(VARCHAR, VARCHAR)
        argTypes = new Type[] {Type.VARCHAR, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNull(fn);

        // array_append(INT, VARCHAR)
        argTypes = new Type[] {Type.INT, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {Type.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {Type.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNull(fn);

        // array_length(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(Type.INT, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // array_length(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(Type.INT, fn.getReturnType());
        Assert.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_length(NULL)
        argTypes = new Type[] {Type.NULL};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assert.assertNotNull(fn);
        Assert.assertEquals(Type.INT, fn.getReturnType());
        Assert.assertEquals(new ArrayType(Type.NULL), fn.getArgs()[0]);
    }

    @Test
    public void testPolymorphicTVF() {
        // First two columns of the result are polymorphic (derived from argument type), but the last argument is of a
        // "concrete" type BIGINT, which is retained in the resolved function.
        TableFunction polymorphicTVF =
                new TableFunction(new FunctionName("three_column_tvf"), Lists.newArrayList("a", "b", "c"),
                        Lists.newArrayList(Type.ANY_ARRAY),
                        Lists.newArrayList(Type.ANY_ELEMENT, Type.ANY_ELEMENT, Type.BIGINT));

        functionSet.addBuiltin(polymorphicTVF);

        Type[] argTypes = new Type[] {VARCHAR_ARRAY};
        Function desc = new Function(new FunctionName("three_column_tvf"), argTypes, Type.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assert.assertNotNull(fn);
        Assert.assertTrue(fn instanceof TableFunction);
        TableFunction tableFunction = (TableFunction) fn;
        Assert.assertEquals(3, tableFunction.getTableFnReturnTypes().size());
        Assert.assertEquals(Type.VARCHAR, tableFunction.getTableFnReturnTypes().get(0));
        Assert.assertEquals(Type.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
        Assert.assertEquals(Type.BIGINT, tableFunction.getTableFnReturnTypes().get(2));

        // Same but for two column TVF.
        TableFunction twoColumnTVF =
                new TableFunction(new FunctionName("two_column_tvf"), Lists.newArrayList("a", "b"),
                        Lists.newArrayList(Type.ANY_ARRAY),
                        Lists.newArrayList(Type.BIGINT, Type.ANY_ELEMENT));
        functionSet.addBuiltin(twoColumnTVF);

        argTypes = new Type[] {VARCHAR_ARRAY};
        desc = new Function(new FunctionName("two_column_tvf"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assert.assertNotNull(fn);
        Assert.assertTrue(fn instanceof TableFunction);
        tableFunction = (TableFunction) fn;
        Assert.assertEquals(2, tableFunction.getTableFnReturnTypes().size());
        Assert.assertEquals(Type.BIGINT, tableFunction.getTableFnReturnTypes().get(0));
        Assert.assertEquals(Type.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
    }
}