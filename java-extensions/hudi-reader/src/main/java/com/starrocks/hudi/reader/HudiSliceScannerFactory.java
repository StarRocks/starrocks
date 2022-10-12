// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.ScannerFactory;
import com.starrocks.utils.loader.ChildFirstClassLoader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;

public class HudiSliceScannerFactory implements ScannerFactory {
    static ChildFirstClassLoader classLoader;
    static {
        String basePath = System.getenv("STARROCKS_HOME");
        File dir = new File(basePath + "/lib/hudi-reader-lib");
        URL[] jars = Arrays.stream(Objects.requireNonNull(dir.listFiles()))
                .map(f -> {
                    try {
                        return f.toURI().toURL();
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Cannot init hudi slice classloader.", e);
                    }
                }).toArray(URL[]::new);
        classLoader = new ChildFirstClassLoader(jars, ClassLoader.getSystemClassLoader());
    }

    /**
     * Hudi scanner uses own independent classloader to find all classes
     * due to hadoop class conflicts with JNI launcher of libhdfs (hadoop-3.x).
     */
    @Override
    public Class getScannerClass() throws ClassNotFoundException {
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return classLoader.loadClass(HudiSliceScanner.class.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
