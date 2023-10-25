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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.ScannerFactory;
import com.starrocks.jni.connector.ScannerHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PaimonSplitScannerFactory implements ScannerFactory {
    static ClassLoader classLoader;

    static {
        String basePath = System.getenv("STARROCKS_HOME");
        List<File> preloadFiles = new ArrayList();
        //        preloadFiles.add(new File(basePath + "/lib/jni-packages/starrocks-hadoop-ext.jar"));
        File dir = new File(basePath + "/lib/paimon-reader-lib");
        for (File f : dir.listFiles()) {
            preloadFiles.add(f);
        }
        classLoader = ScannerHelper.createChildFirstClassLoader(preloadFiles, "paimon scanner");
    }

    /**
     * Hudi scanner uses own independent classloader to find all classes
     * due to hadoop version (hadoop-2.x) conflicts with JNI launcher of libhdfs (hadoop-3.x).
     */
    @Override
    public Class getScannerClass() throws ClassNotFoundException {
        try {
            return classLoader.loadClass("com.starrocks.paimon.reader.PaimonSplitScanner");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
