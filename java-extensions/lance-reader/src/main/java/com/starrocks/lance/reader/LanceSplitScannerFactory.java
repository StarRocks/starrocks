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

package com.starrocks.lance.reader;

import com.starrocks.jni.connector.ScannerFactory;
import com.starrocks.jni.connector.ScannerHelper;

public class LanceSplitScannerFactory implements ScannerFactory {
    private static final ClassLoader CLASS_LOADER;

    static {
        CLASS_LOADER = ScannerHelper.createModuleClassLoader("lance-reader-lib");
    }

    @Override
    public Class getScannerClass(String scannerType) throws ClassNotFoundException {
        return CLASS_LOADER.loadClass("com.starrocks.lance.reader.LanceSplitScanner");
    }
}
