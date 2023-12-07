// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/FunctionSetTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.starrocks.analysis.FunctionName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FunctionSetTest {

    private FunctionSet functionSet;

    private static final Type VARCHAR_ARRAY = new ArrayType(Type.VARCHAR);
    private static final Type TINYINT_ARRAY = new ArrayType(Type.TINYINT);
    private static final Type INT_ARRAY = new ArrayType(Type.INT);
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
        Assert.assertNull(fn);

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
        Assert.assertNull(fn);

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
        Assert.assertNull(fn);

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
    public void testCopyFunction() {
        Type[] argTypes1 = {ScalarType.DECIMALV2, ScalarType.TINYINT, ScalarType.TINYINT};
        Function lagDesc1 = new Function(new FunctionName(FunctionSet.LAG), argTypes1, ScalarType.INVALID, true);
        Function copy = lagDesc1.copy();
        Assert.assertEquals(copy, lagDesc1);
    }
}