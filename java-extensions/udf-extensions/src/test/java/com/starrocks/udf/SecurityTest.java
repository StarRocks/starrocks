package com.starrocks.udf;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.AccessControlException;

public class SecurityTest {

    public static class TestClassLoader extends ClassLoader {
        public TestClassLoader(String clazzName, byte[] bytes) {
            defineClass(clazzName, bytes, 0, bytes.length);
        }
    }

    public static class ScalarAdd {
        public String evaluate(String path) {
            File file = new File(path);
            return file.getAbsolutePath();
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

    @AfterEach
    public void resetSecurityManager() {
        System.setSecurityManager(null);
    }

    @Test
    public void testUDFCreateFile() throws NoSuchMethodException, ClassNotFoundException {
        System.setSecurityManager(new UDFSecurityManager(TestClassLoader.class));

        Class<?> clazz = ScalarAdd.class;
        final String genClassName = CallStubGenerator.CLAZZ_NAME.replace("/", ".");
        Method m = clazz.getMethod("evaluate", String.class);
        final byte[] updates =
                CallStubGenerator.generateScalarCallStub(clazz, m);
        ClassLoader classLoader = new TestClassLoader(genClassName, updates);
        final Class<?> stubClazz = classLoader.loadClass(genClassName);
        Method batchCall = getFirstMethod(stubClazz, "batchCallV");
        String[] inputs1 = new String[1];
        inputs1[0] = "prefix";
        ScalarAdd concat = new ScalarAdd();
        Assertions.assertThrows(AccessControlException.class, () -> {
            try {
                batchCall.invoke(null, 1, concat, inputs1);
            } catch (Exception e) {
                // AccessControlException is root cause
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                if (rootCause instanceof AccessControlException) {
                    throw rootCause;
                }
            }
        });
    }

    @Test
    public void testNoUDFCreateFile() {
        System.setSecurityManager(new UDFSecurityManager(TestClassLoader.class));
        ScalarAdd concat = new ScalarAdd();
        Assertions.assertTrue(concat.evaluate("./test").contains("test"));
    }
}
