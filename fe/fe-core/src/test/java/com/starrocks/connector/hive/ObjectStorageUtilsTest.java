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


package com.starrocks.connector.hive;

import com.starrocks.connector.ObjectStorageUtils;
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
        String ossPath = "oss://xxx";
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(s3Path));
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(s3nPath));
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(cosnPath));
        Assert.assertEquals(s3aPath, ObjectStorageUtils.formatObjectStoragePath(cosPath));
        Assert.assertEquals(ks3Path, ObjectStorageUtils.formatObjectStoragePath(ks3Path));
        Assert.assertEquals(ossPath, ObjectStorageUtils.formatObjectStoragePath(ossPath));
    }
}
