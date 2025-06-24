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

package com.starrocks.connector.iceberg;

import com.starrocks.jni.connector.ScannerFactory;
import com.starrocks.jni.connector.ScannerHelper;

public class IcebergMetadataScannerFactory implements ScannerFactory {
    static ClassLoader classLoader;

    static {
        classLoader = ScannerHelper.createModuleClassLoader("iceberg-reader-lib");
    }

    @Override
    public Class getScannerClass(String scannerType) throws ClassNotFoundException {
        try {
            String loadClass;
            switch (scannerType.toLowerCase()) {
                case "logical_iceberg_metadata":
                    loadClass = "com.starrocks.connector.iceberg.IcebergMetadataScanner";
                    break;
                case "refs":
                    loadClass = "com.starrocks.connector.iceberg.IcebergRefsTableScanner";
                    break;
                case "history":
                    loadClass = "com.starrocks.connector.iceberg.IcebergHistoryTableScanner";
                    break;
                case "metadata_log_entries":
                    loadClass = "com.starrocks.connector.iceberg.IcebergMetadataLogEntriesScanner";
                    break;
                case "snapshots":
                    loadClass = "com.starrocks.connector.iceberg.IcebergSnapshotsTableScanner";
                    break;
                case "manifests":
                    loadClass = "com.starrocks.connector.iceberg.IcebergManifestsTableScanner";
                    break;
                case "files":
                    loadClass = "com.starrocks.connector.iceberg.IcebergFilesTableScanner";
                    break;
                case "partitions":
                    loadClass = "com.starrocks.connector.iceberg.IcebergPartitionsTableScanner";
                    break;
                default:
                    throw new IllegalArgumentException("unknown iceberg scanner type " + scannerType);
            }
            return classLoader.loadClass(loadClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
