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

package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.ScannerFactory;
import com.starrocks.jni.connector.ScannerHelper;

import java.io.File;
import java.util.ArrayList;
<<<<<<< HEAD
import java.util.List;
=======
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public class HudiSliceScannerFactory implements ScannerFactory {
    static ClassLoader classLoader;

    static {
        String basePath = System.getenv("STARROCKS_HOME");
<<<<<<< HEAD
        List<File> preloadFiles = new ArrayList();
        preloadFiles.add(new File(basePath + "/lib/jni-packages/starrocks-hadoop-ext.jar"));
        File dir = new File(basePath + "/lib/hudi-reader-lib");
        for (File f : dir.listFiles()) {
            preloadFiles.add(f);
        }
=======
        List<File> preloadFiles = new ArrayList<>();
        preloadFiles.add(new File(basePath + "/lib/jni-packages/starrocks-hadoop-ext.jar"));
        File dir = new File(basePath + "/lib/hudi-reader-lib");
        preloadFiles.addAll(Arrays.asList(Objects.requireNonNull(dir.listFiles())));
        dir = new File(basePath + "/lib/common-runtime-lib");
        preloadFiles.addAll(Arrays.asList(Objects.requireNonNull(dir.listFiles())));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        classLoader = ScannerHelper.createChildFirstClassLoader(preloadFiles, "hudi scanner");
    }

    /**
     * Hudi scanner uses own independent classloader to find all classes
     * due to hadoop version (hadoop-2.x) conflicts with JNI launcher of libhdfs (hadoop-3.x).
     */
    @Override
<<<<<<< HEAD
    public Class getScannerClass() throws ClassNotFoundException {
=======
    public Class getScannerClass(String scannerType) throws ClassNotFoundException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        try {
            return classLoader.loadClass("com.starrocks.hudi.reader.HudiSliceScanner");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
