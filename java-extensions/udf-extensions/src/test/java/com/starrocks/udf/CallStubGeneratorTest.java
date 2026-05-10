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

package com.starrocks.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CallStubGeneratorTest {
    public static class IntSumfunc {
        public static class State {
            public long val = 0;
        }

        public void update(State state, Integer val) {
            state.val += val;
        }
    }

    public static class SumStringConcat {
        public static class State {
            public String val = "";
        }

        public void update(State state, String v1, Integer v2) {
            state.val += v1 + v2;
        }
    }

    public static class TestClassLoader extends ClassLoader {
        public TestClassLoader(String clazzName, byte[] bytes) {
            defineClass(clazzName, bytes, 0, bytes.length);
        }
    }

    private static Method getFirstMethod(Class<?> clazz, String name) {
        Method call = null;
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (declaredMethod.getName().equals(name)) {
                call = declaredMethod;
            }
        }
        return call;
    }

    // Test Agg batch Call Single
    @Test
    public void testAggCallSingleStub()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = IntSumfunc.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        Method m = clazz.getMethod("update", IntSumfunc.State.class, Integer.class);

        final byte[] updates =
                CallStubGenerator.generateCallStubV(clazz, m);

        ClassLoader classLoader = new TestClassLoader(genClassName, updates);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        IntSumfunc sum = new IntSumfunc();
        IntSumfunc.State state = new IntSumfunc.State();

        int testSize = 1000;
        Integer[] inputs = new Integer[testSize];
        long expect = 0;
        for (int i = 0; i < testSize; i++) {
            inputs[i] = i;
            expect += inputs[i];
        }

        assert batchCall != null;
        batchCall.invoke(null, testSize, sum, state, inputs);

        Assertions.assertEquals(expect, state.val);
    }

    @Test
    public void testAggCallSingleStubWithMultiParameters()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = SumStringConcat.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        Method m = clazz.getMethod("update", SumStringConcat.State.class, String.class, Integer.class);
        final byte[] updates =
                CallStubGenerator.generateCallStubV(clazz, m);

        ClassLoader classLoader = new TestClassLoader(genClassName, updates);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        SumStringConcat concat = new SumStringConcat();
        SumStringConcat.State state = new SumStringConcat.State();

        int testSize = 1000;
        String expect = "";

        String[] inputs1 = new String[testSize];
        Integer[] inputs2 = new Integer[testSize];

        for (int i = 0; i < testSize; i++) {
            inputs1[i] = i + "";
            inputs2[i] = i;
            expect += inputs1[i] + inputs2[i];
        }

        batchCall.invoke(null, testSize, concat, state, inputs1, inputs2);
        Assertions.assertEquals(expect, state.val);
    }

    public static class ScalarAdd {
        public String evaluate(String v1, Integer v2) {
            return v1 + v2;
        }
    }
    
    public static class VarargsConcat {
        public String evaluate(String... args) {
            if (args == null || args.length == 0) {
                return "";
            }
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                if (args[i] != null) {
                    if (i > 0) {
                        result.append(" ");
                    }
                    result.append(args[i]);
                }
            }
            return result.toString();
        }
    }
    
    public static class VarargsSum {
        public static class State {
            public int sum = 0;
        }
        
        public void update(State state, Integer... values) {
            if (values != null) {
                for (Integer val : values) {
                    if (val != null) {
                        state.sum += val;
                    }
                }
            }
        }
    }
    
    @Test
    public void testScalarCallStub()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = ScalarAdd.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        Method m = clazz.getMethod("evaluate", String.class, Integer.class);
        final byte[] updates =
                CallStubGenerator.generateScalarCallStub(clazz, m);

        ClassLoader classLoader = new TestClassLoader(genClassName, updates);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        ScalarAdd concat = new ScalarAdd();
        int testSize = 1000;
        String[] inputs1 = new String[testSize];
        Integer[] inputs2 = new Integer[testSize];
        String[] expects = new String[testSize];

        for (int i = 0; i < testSize; i++) {
            inputs1[i] = i + "";
            inputs2[i] = i;
            expects[i] = inputs1[i] + inputs2[i];
        }

        final String[] res = (String[])batchCall.invoke(null, testSize, concat, inputs1, inputs2);
        for (int i = 0; i < testSize; i++) {
            Assertions.assertEquals(expects[i], res[i]);
        }
    }
    
    @Test
    public void testVarargsScalarUDF()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = VarargsConcat.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        // numActualVarArgs = 3: the UDF is called with 3 string columns
        Method m = clazz.getMethod("evaluate", String[].class);
        final byte[] updates =
                CallStubGenerator.generateScalarCallStub(clazz, m, 3);

        ClassLoader classLoader = new TestClassLoader(genClassName, updates);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        VarargsConcat concat = new VarargsConcat();
        int testSize = 100;

        // Stub signature: (int rows, VarargsConcat obj, String[] col0, String[] col1, String[] col2) String[]
        String[] inputs1 = new String[testSize];
        String[] inputs2 = new String[testSize];
        String[] inputs3 = new String[testSize];
        String[] expects = new String[testSize];

        for (int i = 0; i < testSize; i++) {
            inputs1[i] = "a" + i;
            inputs2[i] = "b" + i;
            inputs3[i] = "c" + i;
            expects[i] = inputs1[i] + " " + inputs2[i] + " " + inputs3[i];
        }

        // batchCallV(rows, obj, col0, col1, col2) - each colN is a String[] column
        final String[] res = (String[]) batchCall.invoke(null, testSize, concat, inputs1, inputs2, inputs3);
        for (int i = 0; i < testSize; i++) {
            Assertions.assertEquals(expects[i], res[i]);
        }
    }

    @Test
    public void testVarargsAggUDF()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = VarargsSum.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        // numActualVarArgs = 3: the UDAF is called with 3 integer columns
        Method m = clazz.getMethod("update", VarargsSum.State.class, Integer[].class);
        final byte[] updates =
                CallStubGenerator.generateCallStubV(clazz, m, 3);

        ClassLoader classLoader = new TestClassLoader(genClassName, updates);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        VarargsSum sum = new VarargsSum();
        VarargsSum.State state = new VarargsSum.State();

        int testSize = 100;
        Integer[] inputs1 = new Integer[testSize];
        Integer[] inputs2 = new Integer[testSize];
        Integer[] inputs3 = new Integer[testSize];
        int expect = 0;

        for (int i = 0; i < testSize; i++) {
            inputs1[i] = i;
            inputs2[i] = i * 2;
            inputs3[i] = i * 3;
            expect += inputs1[i] + inputs2[i] + inputs3[i];
        }

        // batchCallV(rows, obj, state, col0, col1, col2) - each colN is an Integer[] column
        assert batchCall != null;
        batchCall.invoke(null, testSize, sum, state, inputs1, inputs2, inputs3);

        Assertions.assertEquals(expect, state.sum);
    }

    // Mixed varargs: fixed param(s) before the varargs parameter

    public static class MixedVarargsConcat {
        // Fixed param 'prefix', then varargs 'parts'
        public String evaluate(String prefix, String... parts) {
            StringBuilder sb = new StringBuilder(prefix != null ? prefix : "");
            if (parts != null) {
                for (String p : parts) {
                    if (p != null) {
                        sb.append(p);
                    }
                }
            }
            return sb.toString();
        }
    }

    public static class MixedVarargsSum {
        public static class State {
            public String result = "";
        }

        // Fixed param 'sep', then varargs 'values'
        public void update(State state, String sep, Integer... values) {
            if (values != null) {
                for (Integer v : values) {
                    if (v != null) {
                        if (!state.result.isEmpty()) {
                            state.result += sep;
                        }
                        state.result += v;
                    }
                }
            }
        }
    }

    @Test
    public void testMixedVarargsScalarUDF()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = MixedVarargsConcat.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        // Mixed: 1 fixed param (prefix) + 2 actual varargs columns
        Method m = clazz.getMethod("evaluate", String.class, String[].class);
        final byte[] bytes = CallStubGenerator.generateScalarCallStub(clazz, m, 2);

        ClassLoader classLoader = new TestClassLoader(genClassName, bytes);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        MixedVarargsConcat udf = new MixedVarargsConcat();
        int testSize = 10;

        // Stub signature: (int rows, UDF obj, String[] prefixCol, String[] partCol0, String[] partCol1) String[]
        String[] prefixCol = new String[testSize];
        String[] partCol0  = new String[testSize];
        String[] partCol1  = new String[testSize];
        String[] expects   = new String[testSize];

        for (int i = 0; i < testSize; i++) {
            prefixCol[i] = "P" + i + ":";
            partCol0[i]  = "A" + i;
            partCol1[i]  = "B" + i;
            expects[i]   = prefixCol[i] + partCol0[i] + partCol1[i];
        }

        assert batchCall != null;
        final String[] res = (String[]) batchCall.invoke(null, testSize, udf, prefixCol, partCol0, partCol1);
        for (int i = 0; i < testSize; i++) {
            Assertions.assertEquals(expects[i], res[i]);
        }
    }

    @Test
    public void testMixedVarargsAggUDF()
            throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = MixedVarargsSum.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        // Mixed: state + 1 fixed param (sep) + 2 actual varargs columns
        Method m = clazz.getMethod("update", MixedVarargsSum.State.class, String.class, Integer[].class);
        final byte[] bytes = CallStubGenerator.generateCallStubV(clazz, m, 2);

        ClassLoader classLoader = new TestClassLoader(genClassName, bytes);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");

        MixedVarargsSum udaf = new MixedVarargsSum();
        MixedVarargsSum.State state = new MixedVarargsSum.State();

        int testSize = 3;
        // Stub signature: (int rows, UDAF obj, State state, String[] sepCol, Integer[] valCol0, Integer[] valCol1) V
        String[]  sepCol  = new String[]  {",", ",", ","};
        Integer[] valCol0 = new Integer[] {1, 2, 3};
        Integer[] valCol1 = new Integer[] {10, 20, 30};

        assert batchCall != null;
        batchCall.invoke(null, testSize, udaf, state, sepCol, valCol0, valCol1);

        // update called 3 times: row0=(sep=",",vals=1,10), row1=(sep=",",vals=2,20), row2=(sep=",",vals=3,30)
        // state.result = "1,10,2,20,3,30"
        Assertions.assertEquals("1,10,2,20,3,30", state.result);
    }
}
