// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.udf;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

// UDF ClassLoader
// Used to isolate classes between UDFs to avoid the influence between static variables.
// BE UDF's Expr creates a new instance of ClassLoader
// each time it opens. It can ensure no influence on each other.

public class UDFClassLoader extends URLClassLoader {

    private Map<String, Class<?>> genClazzMap = new HashMap<>();

    public UDFClassLoader(String UDFPath) throws IOException {
        super(new URL[] {new URL("file://" + UDFPath)});
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        String clazzName = name.replace("/", ".");
        if (genClazzMap.containsKey(clazzName)) {
            return genClazzMap.get(clazzName);
        }
        return super.findClass(clazzName);
    }

    // (Boxed[]...)V update
    public Class<?> generateCallStubV(String name, Class<?> clazz, Method method) {
        String clazzName = name.replace("/", ".");
        if (!clazzName.startsWith(CallStubGenerator.GEN_KEYWORD)) {
            throw new UnsupportedOperationException(
                    "generate class name should start with " + CallStubGenerator.GEN_KEYWORD);
        }
        final byte[] bytes = CallStubGenerator.generateCallStubV(clazz, method);
        final Class<?> genClazz = defineClass(clazzName, bytes, 0, bytes.length);
        genClazzMap.put(name, genClazz);
        return genClazz;
    }
}

