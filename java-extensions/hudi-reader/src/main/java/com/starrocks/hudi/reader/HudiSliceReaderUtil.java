// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.hudi.reader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;

public class HudiSliceReaderUtil {
    static ChildFirstClassLoader classLoader;
    static {
        ClassLoader originLoader = Thread.currentThread().getContextClassLoader();
        String basePath = System.getenv("STARROCKS_HOME");
        File dir = new File(basePath + "/lib/hudi-reader-lib");
        URL[] jars = Arrays.stream(Objects.requireNonNull(dir.listFiles())).map(f -> {
            try {
                return f.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }).toArray(URL[]::new);
        classLoader = new ChildFirstClassLoader(jars, originLoader);
    }

    public static Class getHudiSliceRecordReaderClass() throws Exception {
        Thread.currentThread().setContextClassLoader(classLoader);
        return classLoader.loadClass(HudiSliceRecordReader.class.getName());
    }
}
