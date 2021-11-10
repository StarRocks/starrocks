// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external;

public class ObejctStorageUtils {
    private static final String SCHEME_S3A = "s3a://";
    private static final String SCHEME_S3 = "s3://";
    private static final String SCHEME_S3N = "s3n://";
    private static final String SCHEME_S3_PREFIX = "s3";
    private static final String SCHEME_OSS_PREFIX = "oss";

    public static boolean isObjectStorage(String path) {
        return path.startsWith(SCHEME_S3_PREFIX) || path.startsWith(SCHEME_OSS_PREFIX);
    }

    public static String formatObjectStoragePath(String path) {
        if (path.startsWith(SCHEME_S3)) {
            return SCHEME_S3A + path.substring(5);
        }
        if (path.startsWith(SCHEME_S3N)) {
            return SCHEME_S3A + path.substring(6);
        }

        return path;
    }
}
