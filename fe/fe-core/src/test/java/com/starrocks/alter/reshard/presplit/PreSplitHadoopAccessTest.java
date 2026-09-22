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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

class PreSplitHadoopAccessTest {

    @Test
    void hadoopConfigurationDisablesFileSystemCacheForCredentialedSchemes() {
        // Hadoop's FileSystem.CACHE ignores the passed Configuration on a cache
        // hit; without disable-cache the second concurrent load with different
        // credentials would silently reuse the first load's filesystem. Pin
        // the contract for every scheme StarRocks's HdfsFsManager supports.
        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(Collections.emptyMap());
        for (String scheme : PreSplitHadoopAccess.SCHEMES_TO_BUILD_FRESH_FILESYSTEM) {
            Assertions.assertTrue(
                    hadoopConfig.getBoolean("fs." + scheme + ".impl.disable.cache", /*defaultValue=*/ false),
                    "fs." + scheme + ".impl.disable.cache must be true");
        }
    }

    @Test
    void hadoopConfigurationTranslatesCanonicalAwsInstanceProfileProperties() {
        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(Map.of(
                CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true",
                CloudConfigurationConstants.AWS_S3_REGION, "us-west-2"));

        Assertions.assertEquals("org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
                hadoopConfig.get(Constants.AWS_CREDENTIALS_PROVIDER));
        Assertions.assertEquals("us-west-2", hadoopConfig.get(Constants.AWS_REGION));
        Assertions.assertEquals(S3AFileSystem.class.getName(), hadoopConfig.get("fs.s3.impl"));
        Assertions.assertEquals(S3AFileSystem.class.getName(), hadoopConfig.get("fs.s3a.impl"));
        Assertions.assertEquals(S3AFileSystem.class.getName(), hadoopConfig.get("fs.oss.impl"));
    }

    @Test
    void hadoopConfigurationTranslatesCanonicalAliyunProperties() {
        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(Map.of(
                CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, "access-key",
                CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, "secret-key",
                CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, "oss-cn-hangzhou.aliyuncs.com"));

        Assertions.assertEquals(S3AFileSystem.class.getName(), hadoopConfig.get("fs.oss.impl"));
        Assertions.assertEquals("access-key", hadoopConfig.get(Constants.ACCESS_KEY));
        Assertions.assertEquals("secret-key", hadoopConfig.get(Constants.SECRET_KEY));
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", hadoopConfig.get(Constants.ENDPOINT));
    }

    @Test
    void hadoopConfigurationPreservesRawBrokerHadoopProperties() {
        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(Map.of(
                "fs.s3a.access.key", "broker-access-key",
                "fs.s3a.secret.key", "broker-secret-key",
                "fs.s3a.endpoint", "s3-compatible.example.com",
                "fs.s3a.impl.disable.cache", "false"));

        Assertions.assertEquals("broker-access-key", hadoopConfig.get("fs.s3a.access.key"));
        Assertions.assertEquals("broker-secret-key", hadoopConfig.get("fs.s3a.secret.key"));
        Assertions.assertEquals("s3-compatible.example.com", hadoopConfig.get("fs.s3a.endpoint"));
        Assertions.assertTrue(hadoopConfig.getBoolean("fs.s3a.impl.disable.cache", false));
    }

    @Test
    void hadoopFileStatusPreservesPathAndSize() {
        TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus(
                "oss://bucket/load/data.parquet", /*isDir=*/ false, /*size=*/ 1024L, /*isSplitable=*/ true);

        FileStatus hadoopFileStatus = PreSplitHadoopAccess.toHadoopFileStatus(brokerFileStatus);

        Assertions.assertEquals(1024L, hadoopFileStatus.getLen());
        Assertions.assertFalse(hadoopFileStatus.isDirectory());
        Assertions.assertEquals("oss://bucket/load/data.parquet", hadoopFileStatus.getPath().toString());
    }
}
