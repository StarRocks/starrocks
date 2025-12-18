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
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class SharedNothingStorageVolumeMgrTest {

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testStorageVolumeCRUD() throws AlreadyExistsException, DdlException, MetaNotFoundException {
        String svName = "test";
        String svName1 = "test1";
        // create
        StorageVolumeMgr svm = new SharedNothingStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("aaa", "bbb");
        storageParams.put(AWS_S3_REGION, "region");
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), ""));
        storageParams.remove("aaa");
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String storageVolumeId = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assertions.assertEquals(true, svm.exists(svName));
        StorageVolume sv = svm.getStorageVolumeByName(svName);
        Assertions.assertEquals(sv.getId(), svm.getStorageVolume(storageVolumeId).getId());
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
        try {
            svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        } catch (AlreadyExistsException e) {
            Assertions.assertTrue(e.getMessage().contains("Storage volume 'test' already exists"));
        }

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_ACCESS_KEY, "ak");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        try {
            svm.updateStorageVolume(svName1, null, null, storageParams, Optional.of(false), "test update");
            Assertions.fail();
        } catch (IllegalStateException e) {
            Assertions.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        storageParams.put("aaa", "bbb");
        Assertions.assertThrows(DdlException.class, () ->
                svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(true), "test update"));
        storageParams.remove("aaa");
        svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(true), "test update");
        sv = svm.getStorageVolumeByName(svName);
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals("region1", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint1", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
        Assertions.assertEquals("test update", sv.getComment());
        Assertions.assertEquals(true, sv.getEnabled());

        // set default storage volume
        try {
            svm.setDefaultStorageVolume(svName1);
            Assertions.fail();
        } catch (IllegalStateException e) {
            Assertions.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        svm.setDefaultStorageVolume(svName);
        Assertions.assertEquals(sv.getId(), svm.getDefaultStorageVolumeId());
        try {
            svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(false), "");
            Assertions.fail();
        } catch (IllegalStateException e) {
            Assertions.assertTrue(e.getMessage().contains("Default volume can not be disabled"));
        }

        // bind/unbind db and table to storage volume
        Assertions.assertTrue(svm.bindDbToStorageVolume(svName, 1L));
        Assertions.assertTrue(svm.bindTableToStorageVolume(svName, 1L, 1L));

        // remove
        try {
            svm.removeStorageVolume(svName);
            Assertions.fail();
        } catch (IllegalStateException e) {
            Assertions.assertTrue(e.getMessage().contains("default storage volume can not be removed"));
        }
        Throwable ex = Assertions.assertThrows(MetaNotFoundException.class, () -> svm.removeStorageVolume(svName1));
        Assertions.assertEquals("Storage volume 'test1' does not exist", ex.getMessage());

        svm.createStorageVolume(svName1, "S3", locations, storageParams, Optional.empty(), "");
        svm.updateStorageVolume(svName1, null, null, storageParams, Optional.empty(), "test update");
        svm.setDefaultStorageVolume(svName1);

        sv = svm.getStorageVolumeByName(svName);
        svm.unbindDbToStorageVolume(1L);
        svm.unbindTableToStorageVolume(1L);
        svm.removeStorageVolume(svName);
        Assertions.assertFalse(svm.exists(svName));
    }

    @Test
    public void testCreateOrUpdateBuiltinStorageVolume() throws DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedNothingStorageVolumeMgr();
        Assertions.assertEquals("", svm.createBuiltinStorageVolume());
        Assertions.assertNull(svm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
    }
}
