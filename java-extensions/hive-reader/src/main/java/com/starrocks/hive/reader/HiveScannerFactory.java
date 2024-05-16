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
import com.starrocks.jni.connector.ScannerHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HiveScannerFactory implements ScannerFactory {
    static ClassLoader classLoader;

    static {
        String basePath = System.getenv("STARROCKS_HOME");
        List<File> preloadFiles = new ArrayList<>();
        preloadFiles.add(new File(basePath + "/lib/jni-packages/starrocks-hadoop-ext.jar"));
        File dir = new File(basePath + "/lib/hive-reader-lib");
        preloadFiles.addAll(Arrays.asList(Objects.requireNonNull(dir.listFiles())));
        dir = new File(basePath + "/lib/common-runtime-lib");
        preloadFiles.addAll(Arrays.asList(Objects.requireNonNull(dir.listFiles())));
        classLoader = ScannerHelper.createChildFirstClassLoader(preloadFiles, "hive scanner");
    }

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