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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FunctionSetTest {

    private FunctionSet functionSet;

    private static final Type VARCHAR_ARRAY = new ArrayType(VarcharType.VARCHAR);
    private static final Type TINYINT_ARRAY = new ArrayType(IntegerType.TINYINT);
    private static final Type INT_ARRAY = new ArrayType(IntegerType.INT);
    private static final Type DOUBLE_ARRAY = new ArrayType(FloatType.DOUBLE);
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
        Type[] argTypes1 = {DecimalType.DECIMALV2, IntegerType.TINYINT, IntegerType.TINYINT};
        Function lagDesc1 = new Function(new FunctionName(FunctionSet.LAG), argTypes1, InvalidType.INVALID, false);
        Function newFunction = functionSet.getFunction(lagDesc1, Function.CompareMode.IS_SUPERTYPE_OF);
        Type[] newArgTypes = newFunction.getArgs();
        Assertions.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assertions.assertTrue(newArgTypes[0].matchesType(DecimalType.DECIMALV2));

        Type[] argTypes2 = {VarcharType.VARCHAR, IntegerType.TINYINT, IntegerType.TINYINT};
        Function lagDesc2 = new Function(new FunctionName(FunctionSet.LAG), argTypes2, InvalidType.INVALID, false);
        newFunction = functionSet.getFunction(lagDesc2, Function.CompareMode.IS_SUPERTYPE_OF);
        newArgTypes = newFunction.getArgs();
        Assertions.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assertions.assertTrue(newArgTypes[0].matchesType(VarcharType.VARCHAR));
    }

    @Test
    public void testPolymorphicFunction() {
        // array_append(ARRAY<INT>, INT)
        Type[] argTypes = {INT_ARRAY, IntegerType.INT};
        Function desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, TINYINT)
        argTypes = new Type[] {INT_ARRAY, IntegerType.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, INT)
        argTypes = new Type[] {TINYINT_ARRAY, IntegerType.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, DOUBLE)
        argTypes = new Type[] {INT_ARRAY, FloatType.DOUBLE};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(DOUBLE_ARRAY, fn.getReturnType());
        Assertions.assertEquals(DOUBLE_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(FloatType.DOUBLE, fn.getArgs()[1]);

        // array_append(NULL, INT)
        argTypes = new Type[] {NullType.NULL, IntegerType.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, NULL)
        argTypes = new Type[] {INT_ARRAY, NullType.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, VARCHAR)
        argTypes = new Type[] {TINYINT_ARRAY, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[1]);

        // array_append(NULL, NULL)
        argTypes = new Type[] {NullType.NULL, NullType.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(new ArrayType(BooleanType.BOOLEAN), fn.getReturnType());
        Assertions.assertEquals(new ArrayType(BooleanType.BOOLEAN), fn.getArgs()[0]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<TINYINT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, TINYINT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<INT>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, NULL)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, NullType.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(TINYINT_ARRAY, fn.getArgs()[1]);

        // array_append(NULL, ARRAY<INT>)
        argTypes = new Type[] {NullType.NULL, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, TINYINT)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, IntegerType.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<VARCHAR>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, VARCHAR_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getReturnType());
        Assertions.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getArgs()[0]);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, VARCHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, CHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, CharType.CHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[1]);

        // array_append(VARCHAR, VARCHAR)
        argTypes = new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_append(INT, VARCHAR)
        argTypes = new Type[] {IntegerType.INT, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {IntegerType.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {IntegerType.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(IntegerType.INT, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // array_length(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(IntegerType.INT, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_length(NULL)
        argTypes = new Type[] {NullType.NULL};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(IntegerType.INT, fn.getReturnType());
        Assertions.assertEquals(new ArrayType(BooleanType.BOOLEAN), fn.getArgs()[0]);

        // array_generate(SmallInt,Int,BigInt)
        argTypes = new Type[] {IntegerType.SMALLINT, IntegerType.INT, IntegerType.BIGINT};
        desc = new Function(new FunctionName("array_generate"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(ArrayType.ARRAY_BIGINT, fn.getReturnType());
        Assertions.assertEquals(IntegerType.BIGINT, fn.getArgs()[0]);

        // arrays_overlap
        argTypes = new Type[] {ArrayType.ARRAY_BIGINT, ArrayType.ARRAY_TINYINT};
        desc = new Function(new FunctionName("arrays_overlap"), argTypes, BooleanType.BOOLEAN, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(fn.functionId, 150216L);

        // array_flatten(ARRAY<ARRAY<TINYINT>>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(TINYINT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_flatten(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_flatten(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {VARCHAR_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY_ARRAY, fn.getArgs()[0]);

        // null_or_empty(null)
        argTypes = new Type[] {NullType.NULL};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(BooleanType.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[0]);

        // null_or_empty(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(BooleanType.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // null_or_empty(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(BooleanType.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // coalesce
        argTypes = new Type[] {INT_ARRAY_ARRAY, DOUBLE_ARRAY};
        desc = new Function(new FunctionName("coalesce"), argTypes, InvalidType.INVALID, false);
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
                        Lists.newArrayList(AnyArrayType.ANY_ARRAY),
                        Lists.newArrayList(AnyElementType.ANY_ELEMENT, AnyElementType.ANY_ELEMENT, IntegerType.BIGINT));

        functionSet.addBuiltin(polymorphicTVF);

        Type[] argTypes = new Type[] {VARCHAR_ARRAY};
        Function desc = new Function(new FunctionName("three_column_tvf"), argTypes, InvalidType.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(fn);
        Assertions.assertTrue(fn instanceof TableFunction);
        TableFunction tableFunction = (TableFunction) fn;
        Assertions.assertEquals(3, tableFunction.getTableFnReturnTypes().size());
        Assertions.assertEquals(VarcharType.VARCHAR, tableFunction.getTableFnReturnTypes().get(0));
        Assertions.assertEquals(VarcharType.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
        Assertions.assertEquals(IntegerType.BIGINT, tableFunction.getTableFnReturnTypes().get(2));

        // Same but for two column TVF.
        TableFunction twoColumnTVF =
                new TableFunction(new FunctionName("two_column_tvf"), Lists.newArrayList("a", "b"),
                        Lists.newArrayList(AnyArrayType.ANY_ARRAY),
                        Lists.newArrayList(IntegerType.BIGINT, AnyElementType.ANY_ELEMENT));
        functionSet.addBuiltin(twoColumnTVF);

        argTypes = new Type[] {VARCHAR_ARRAY};
        desc = new Function(new FunctionName("two_column_tvf"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(fn);
        Assertions.assertTrue(fn instanceof TableFunction);
        tableFunction = (TableFunction) fn;
        Assertions.assertEquals(2, tableFunction.getTableFnReturnTypes().size());
        Assertions.assertEquals(IntegerType.BIGINT, tableFunction.getTableFnReturnTypes().get(0));
        Assertions.assertEquals(VarcharType.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
    }
}
