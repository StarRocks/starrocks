// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.udf;

import org.junit.Assert;
import org.junit.Test;

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

        Assert.assertEquals(expect, state.val);
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
        Assert.assertEquals(expect, state.val);
    }

    public static class ScalarAdd {
        public String evaluate(String v1, Integer v2) {
            return v1 + v2;
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
            Assert.assertEquals(expects[i], res[i]);
        }
    }
}
