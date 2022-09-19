// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.external.ObjectStorageUtils;
import org.junit.Assert;
import org.junit.Test;

public class ObjectStorageUtilsTest {
    @Test
    public void testFormatObjectStoragePath() {
        String s3aPath = "s3a://xxx";
        String s3Path = "s3://xxx";
        String s3nPath = "s3n://xxx";
        String cosPath = "cos://xxx";
        String cosnPath = "cosn://xxx";
        String ks3Path = "ks3://xxx";
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(s3Path));
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(s3nPath));
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(cosnPath));
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(cosPath));
        Assert.assertEquals(ks3Path, ObjectStorageUtils.formatObjectStoragePath(ks3Path));
    }
}
