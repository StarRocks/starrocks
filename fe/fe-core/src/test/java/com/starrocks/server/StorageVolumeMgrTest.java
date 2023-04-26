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

package com.starrocks.server;

import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aws.AWSCloudConfiguration;
import com.starrocks.storagevolume.StorageVolume;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class StorageVolumeMgrTest {
    @Test
    public void testStorageVolumeCRUD() throws AnalysisException, AlreadyExistsException {
        String svKey = "test";
        String svKey1 = "test1";
        // create
        StorageVolumeMgr svm = new StorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume(svKey, "S3", locations, storageParams, Optional.empty(), "");
        Assert.assertEquals(true, svm.exists(svKey));
        StorageVolume sv = svm.getStorageVolume(svKey);
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
        try {
            svm.createStorageVolume(svKey, "S3", locations, storageParams, Optional.empty(), "");
        } catch (AlreadyExistsException e) {
            Assert.assertTrue(e.getMessage().contains("Storage Volume 'test' already exists"));
        }

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        try {
            svm.updateStorageVolume(svKey1, storageParams, Optional.of(false), "test update");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage Volume 'test1' does not exist"));
        }
        svm.updateStorageVolume(svKey, storageParams, Optional.of(true), "test update");
        cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region1", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint1", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
        Assert.assertEquals("test update", sv.getComment());
        Assert.assertEquals(true, sv.getEnabled());

        // set default storage volume
        try {
            svm.setDefaultStorageVolume(svKey1);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage Volume 'test1' does not exist"));
        }
        svm.setDefaultStorageVolume(svKey);
        Assert.assertEquals(svKey, svm.getDefaultSV());
        try {
            svm.updateStorageVolume(svKey, storageParams, Optional.of(false), "");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Default volume can not be disabled"));
        }

        // remove
        try {
            svm.removeStorageVolume(svKey);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("default storage volume can not be removed"));
        }
        try {
            svm.removeStorageVolume(svKey1);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage Volume 'test1' does not exist"));
        }

        svm.createStorageVolume(svKey1, "S3", locations, storageParams, Optional.empty(), "");
        svm.updateStorageVolume(svKey1, storageParams, Optional.empty(), "test update");
        svm.setDefaultStorageVolume(svKey1);
        svm.removeStorageVolume(svKey);
        Assert.assertFalse(svm.exists(svKey));
    }
}
