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
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FunctionSetTest {

    private FunctionSet functionSet;

    private static final Type VARCHAR_ARRAY = new ArrayType(Type.VARCHAR);
    private static final Type TINYINT_ARRAY = new ArrayType(Type.TINYINT);
    private static final Type INT_ARRAY = new ArrayType(Type.INT);
    private static final Type DOUBLE_ARRAY = new ArrayType(Type.DOUBLE);
    private static final Type INT_ARRAY_ARRAY = new ArrayType(INT_ARRAY);
    private static final Type TINYINT_ARRAY_ARRAY = new ArrayType(TINYINT_ARRAY);
    private static final Type VARCHAR_ARRAY_ARRAY = new ArrayType(VARCHAR_ARRAY);

    @BeforeEach
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
        Assertions.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assertions.assertTrue(newArgTypes[0].matchesType(ScalarType.DECIMALV2));

        Type[] argTypes2 = {ScalarType.VARCHAR, ScalarType.TINYINT, ScalarType.TINYINT};
        Function lagDesc2 = new Function(new FunctionName(FunctionSet.LAG), argTypes2, ScalarType.INVALID, false);
        newFunction = functionSet.getFunction(lagDesc2, Function.CompareMode.IS_SUPERTYPE_OF);
        newArgTypes = newFunction.getArgs();
        Assertions.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assertions.assertTrue(newArgTypes[0].matchesType(ScalarType.VARCHAR));
    }

    @Test
    public void testPolymorphicFunction() {
        // array_append(ARRAY<INT>, INT)
        Type[] argTypes = {INT_ARRAY, Type.INT};
        Function desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, TINYINT)
        argTypes = new Type[] {INT_ARRAY, Type.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, INT)
        argTypes = new Type[] {TINYINT_ARRAY, Type.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, DOUBLE)
        argTypes = new Type[] {INT_ARRAY, Type.DOUBLE};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(DOUBLE_ARRAY, fn.getReturnType());
        Assertions.assertEquals(DOUBLE_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.DOUBLE, fn.getArgs()[1]);

        // array_append(NULL, INT)
        argTypes = new Type[] {Type.NULL, Type.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, NULL)
        argTypes = new Type[] {INT_ARRAY, Type.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, VARCHAR)
        argTypes = new Type[] {TINYINT_ARRAY, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.VARCHAR, fn.getArgs()[1]);

        // array_append(NULL, NULL)
        argTypes = new Type[] {Type.NULL, Type.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(new ArrayType(Type.BOOLEAN), fn.getReturnType());
        Assertions.assertEquals(new ArrayType(Type.BOOLEAN), fn.getArgs()[0]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<TINYINT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, TINYINT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<INT>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, NULL)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, Type.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(TINYINT_ARRAY, fn.getArgs()[1]);

        // array_append(NULL, ARRAY<INT>)
        argTypes = new Type[] {Type.NULL, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, TINYINT)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, Type.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<VARCHAR>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, VARCHAR_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getReturnType());
        Assertions.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getArgs()[0]);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, VARCHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.VARCHAR, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, CHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, Type.CHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(Type.VARCHAR, fn.getArgs()[1]);

        // array_append(VARCHAR, VARCHAR)
        argTypes = new Type[] {Type.VARCHAR, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_append(INT, VARCHAR)
        argTypes = new Type[] {Type.INT, Type.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {Type.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {Type.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.INT, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // array_length(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.INT, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_length(NULL)
        argTypes = new Type[] {Type.NULL};
        desc = new Function(new FunctionName("array_length"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.INT, fn.getReturnType());
        Assertions.assertEquals(new ArrayType(Type.BOOLEAN), fn.getArgs()[0]);

        // array_generate(SmallInt,Int,BigInt)
        argTypes = new Type[] {Type.SMALLINT, Type.INT, Type.BIGINT};
        desc = new Function(new FunctionName("array_generate"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.ARRAY_BIGINT, fn.getReturnType());
        Assertions.assertEquals(Type.BIGINT, fn.getArgs()[0]);

        // arrays_overlap
        argTypes = new Type[] {Type.ARRAY_BIGINT, Type.ARRAY_TINYINT};
        desc = new Function(new FunctionName("arrays_overlap"), argTypes, Type.BOOLEAN, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(fn.functionId, 150216L);

        // array_flatten(ARRAY<ARRAY<TINYINT>>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(TINYINT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_flatten(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_flatten(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {VARCHAR_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY_ARRAY, fn.getArgs()[0]);

        // null_or_empty(null)
        argTypes = new Type[] {Type.NULL};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(Type.VARCHAR, fn.getArgs()[0]);

        // null_or_empty(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // null_or_empty(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(Type.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // coalesce
        argTypes = new Type[] {INT_ARRAY_ARRAY, DOUBLE_ARRAY};
        desc = new Function(new FunctionName("coalesce"), argTypes, Type.INVALID, false);
        try {
            functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof SemanticException);
            Assertions.assertTrue(e.getMessage().contains("in the function [coalesce]"));
        }
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
        Assertions.assertNotNull(fn);
        Assertions.assertTrue(fn instanceof TableFunction);
        TableFunction tableFunction = (TableFunction) fn;
        Assertions.assertEquals(3, tableFunction.getTableFnReturnTypes().size());
        Assertions.assertEquals(Type.VARCHAR, tableFunction.getTableFnReturnTypes().get(0));
        Assertions.assertEquals(Type.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
        Assertions.assertEquals(Type.BIGINT, tableFunction.getTableFnReturnTypes().get(2));

        // Same but for two column TVF.
        TableFunction twoColumnTVF =
                new TableFunction(new FunctionName("two_column_tvf"), Lists.newArrayList("a", "b"),
                        Lists.newArrayList(Type.ANY_ARRAY),
                        Lists.newArrayList(Type.BIGINT, Type.ANY_ELEMENT));
        functionSet.addBuiltin(twoColumnTVF);

        argTypes = new Type[] {VARCHAR_ARRAY};
        desc = new Function(new FunctionName("two_column_tvf"), argTypes, Type.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(fn);
        Assertions.assertTrue(fn instanceof TableFunction);
        tableFunction = (TableFunction) fn;
        Assertions.assertEquals(2, tableFunction.getTableFnReturnTypes().size());
        Assertions.assertEquals(Type.BIGINT, tableFunction.getTableFnReturnTypes().get(0));
        Assertions.assertEquals(Type.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
    }
}
