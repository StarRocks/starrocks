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

package com.starrocks.hive.reader;

import com.starrocks.jni.connector.ScannerFactory;
import com.starrocks.utils.loader.ChildFirstClassLoader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;

public class HiveScannerFactory implements ScannerFactory {
    static ChildFirstClassLoader classLoader;

    static {
        String basePath = System.getenv("STARROCKS_HOME");
        File dir = new File(basePath + "/lib/hive-reader-lib");
        URL[] jars = Arrays.stream(Objects.requireNonNull(dir.listFiles()))
                .map(f -> {
                    try {
                        return f.toURI().toURL();
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Cannot init hive classloader.", e);
                    }
                }).toArray(URL[]::new);
        classLoader = new ChildFirstClassLoader(jars, ClassLoader.getSystemClassLoader());
    }

    /**
     * Hudi scanner uses own independent classloader to find all classes
     * due to hadoop version (hadoop-2.x) conflicts with JNI launcher of libhdfs (hadoop-3.x).
     */
    @Override
    public Class getScannerClass() throws ClassNotFoundException {
        try {
            return classLoader.loadClass("com.starrocks.hive.reader.HiveScanner");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}