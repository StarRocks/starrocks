package com.starrocks.udf;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlException;

public class SecurityTest {

    public static class TestClassLoader extends ClassLoader {
        public TestClassLoader(String clazzName, byte[] bytes) {
            defineClass(clazzName, bytes, 0, bytes.length);
        }
    }

    public static class ScalarAdd {
        public String evaluate(String v1) throws IOException {
            File.createTempFile(v1, ".txt");
            return v1;
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

    @Test(expected = AccessControlException.class)
    public void testUDFCreateFile()
            throws NoSuchMethodException, ClassNotFoundException, IOException, InvocationTargetException,
            IllegalAccessException {
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
        batchCall.invoke(null, 1, concat, inputs1);

        System.setSecurityManager(null);
    }

    @Test
    public void testNoUDFCreateFile() throws IOException {
        System.setSecurityManager(new UDFSecurityManager(TestClassLoader.class));
        ScalarAdd concat = new ScalarAdd();
        concat.evaluate("test");
        System.setSecurityManager(null);
    }

}
