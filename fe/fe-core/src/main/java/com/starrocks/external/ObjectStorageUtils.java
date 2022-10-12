// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

public class ObjectStorageUtils {
    private static final String SCHEME_S3A = "s3a://";
    private static final String SCHEME_S3 = "s3://";
    private static final String SCHEME_S3N = "s3n://";
    private static final String SCHEME_KS3 = "ks3://";
    private static final String SCHEME_COS = "cos://";
    private static final String SCHEME_COSN = "cosn://";
    private static final String SCHEME_S3_PREFIX = "s3";
    private static final String SCHEME_OSS_PREFIX = "oss";
    private static final String SCHEME_KS3_PREFIX = "ks3";

    public static boolean isObjectStorage(String path) {
        return path.startsWith(SCHEME_S3_PREFIX) || path.startsWith(SCHEME_OSS_PREFIX) || path.startsWith(SCHEME_KS3_PREFIX);
    }

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
        if (path.startsWith(SCHEME_KS3)) {
            return SCHEME_KS3 + path.substring(SCHEME_KS3.length());
        }
        return path;
    }
}
