package com.starrocks.udf;

<<<<<<< HEAD
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
=======
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.lang.reflect.Method;
import java.security.AccessControlException;

public class SecurityTest {

    public static class TestClassLoader extends ClassLoader {
        public TestClassLoader(String clazzName, byte[] bytes) {
            defineClass(clazzName, bytes, 0, bytes.length);
        }
    }

    public static class ScalarAdd {
<<<<<<< HEAD
        public String evaluate(String v1) throws IOException {
            File.createTempFile(v1, ".txt");
            return v1;
=======
        public String evaluate(String path) {
            File file = new File(path);
            return file.getAbsolutePath();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
    @Test(expected = AccessControlException.class)
    public void testUDFCreateFile()
            throws NoSuchMethodException, ClassNotFoundException, IOException, InvocationTargetException,
            IllegalAccessException {
=======
    @AfterEach
    public void resetSecurityManager() {
        System.setSecurityManager(null);
    }

    @Test
    public void testUDFCreateFile() throws NoSuchMethodException, ClassNotFoundException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
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

=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
