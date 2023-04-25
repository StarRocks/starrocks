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

package com.starrocks.storagevolume;

import com.staros.proto.AwsAssumeIamRoleCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_EXTERNAL_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE;

public class S3StorageParamsTest {
    @Test
    public void testDefaultCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");

        S3StorageParams sp = new S3StorageParams(storageParams);
        CloudConfiguration cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStore();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasDefaultCredential());
        Assert.assertEquals("region", sp.getRegion());
        Assert.assertEquals("endpoint", sp.getEndpoint());
    }

    @Test
    public void testSimpleCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_ACCESS_KEY, "access_key");
        storageParams.put(AWS_S3_SECRET_KEY, "secret_key");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");

        S3StorageParams sp = new S3StorageParams(storageParams);
        CloudConfiguration cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStore();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasSimpleCredential());
        AwsSimpleCredentialInfo simpleCredentialInfo = s3FileStoreInfo.getCredential().getSimpleCredential();
        Assert.assertEquals("access_key", simpleCredentialInfo.getAccessKey());
        Assert.assertEquals("secret_key", simpleCredentialInfo.getAccessKeySecret());
        Assert.assertEquals("region", sp.getRegion());
        Assert.assertEquals("endpoint", sp.getEndpoint());
    }

    @Test
    public void testInstanceProfile() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_INSTANCE_PROFILE, "true");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");

        S3StorageParams sp = new S3StorageParams(storageParams);
        CloudConfiguration cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStore();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasProfileCredential());
        Assert.assertEquals("region", sp.getRegion());
        Assert.assertEquals("endpoint", sp.getEndpoint());
    }

    @Test
    public void testAssumeIamRole() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_INSTANCE_PROFILE, "true");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
        storageParams.put(AWS_S3_IAM_ROLE_ARN, "iam_role_arn");
        storageParams.put(AWS_S3_EXTERNAL_ID, "iam_role_arn");

        S3StorageParams sp = new S3StorageParams(storageParams);
        CloudConfiguration cloudConfiguration = sp.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStore();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasAssumeRoleCredential());
        AwsAssumeIamRoleCredentialInfo assumeIamRoleCredentialInfo = s3FileStoreInfo.getCredential()
                .getAssumeRoleCredential();
        Assert.assertEquals("iam_role_arn", assumeIamRoleCredentialInfo.getIamRoleArn());
        Assert.assertEquals("iam_role_arn", assumeIamRoleCredentialInfo.getIamRoleArn());
        Assert.assertEquals("region", sp.getRegion());
        Assert.assertEquals("endpoint", sp.getEndpoint());
    }
}
