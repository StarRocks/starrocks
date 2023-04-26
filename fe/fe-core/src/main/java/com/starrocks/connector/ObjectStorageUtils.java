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


package com.starrocks.connector;

@Deprecated
public class ObjectStorageUtils {
    private static final String SCHEME_S3A = "s3a://";
    private static final String SCHEME_S3 = "s3://";
    private static final String SCHEME_S3N = "s3n://";
    private static final String SCHEME_COS = "cos://";
    private static final String SCHEME_COSN = "cosn://";

    /**
     * Replace the scheme of object storage path to use S3 filesystem interface in FE and native S3 client in BE.
     * Note:
     * Not all object storage paths require this type of conversion.
     * This is kept for backward compatibility and will be removed in the future.
     * In fact, almost all paths do not require this conversion
     * and developers need to evaluate this conversion carefully before using.
     */
    public static String formatObjectStoragePath(String path) {
        if (path.startsWith(SCHEME_S3)) {
            return SCHEME_S3A + path.substring(SCHEME_S3.length());
        }
        if (path.startsWith(SCHEME_S3N)) {
            return SCHEME_S3A + path.substring(SCHEME_S3N.length());
        }
        if (path.startsWith(SCHEME_COSN)) {
            return SCHEME_S3A + path.substring(SCHEME_COSN.length());
        }
        if (path.startsWith(SCHEME_COS)) {
            return SCHEME_S3A + path.substring(SCHEME_COS.length());
        }
        return path;
    }
}
