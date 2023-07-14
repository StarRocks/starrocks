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
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aws.AWSCloudConfiguration;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.storagevolume.StorageVolume;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class SharedNothingStorageVolumeMgrTest {
    @Mocked
    private EditLog editLog;

    @Test
    public void testStorageVolumeCRUD() throws AlreadyExistsException, DdlException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
                editLog.logCreateStorageVolume((StorageVolume) any);
                editLog.logUpdateStorageVolume((StorageVolume) any);
                editLog.logDropStorageVolume((DropStorageVolumeLog) any);
            }
        };

        String svName = "test";
        String svName1 = "test1";
        // create
        StorageVolumeMgr svm = new SharedNothingStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("aaa", "bbb");
        storageParams.put(AWS_S3_REGION, "region");
        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), ""));
        storageParams.remove("aaa");
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String storageVolumeId = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assert.assertEquals(true, svm.exists(svName));
        StorageVolume sv = svm.getStorageVolumeByName(svName);
        Assert.assertEquals(sv.getId(), svm.getStorageVolume(storageVolumeId).getId());
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
        try {
            svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        } catch (AlreadyExistsException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test' already exists"));
        }

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_ACCESS_KEY, "ak");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        try {
            svm.updateStorageVolume(svName1, storageParams, Optional.of(false), "test update");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        storageParams.put("aaa", "bbb");
        Assert.assertThrows(DdlException.class, () ->
                svm.updateStorageVolume(svName, storageParams, Optional.of(true), "test update"));
        storageParams.remove("aaa");
        svm.updateStorageVolume(svName, storageParams, Optional.of(true), "test update");
        sv = svm.getStorageVolumeByName(svName);
        cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region1", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint1", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
        Assert.assertEquals("test update", sv.getComment());
        Assert.assertEquals(true, sv.getEnabled());

        // set default storage volume
        try {
            svm.setDefaultStorageVolume(svName1);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        svm.setDefaultStorageVolume(svName);
        Assert.assertEquals(sv.getId(), svm.getDefaultStorageVolumeId());
        try {
            svm.updateStorageVolume(svName, storageParams, Optional.of(false), "");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Default volume can not be disabled"));
        }

        // bind/unbind db and table to storage volume
        Assert.assertTrue(svm.bindDbToStorageVolume(svName, 1L));
        Assert.assertTrue(svm.bindTableToStorageVolume(svName, 1L, 1L));

        // remove
        try {
            svm.removeStorageVolume(svName);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("default storage volume can not be removed"));
        }
        try {
            svm.removeStorageVolume(svName1);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }

        svm.createStorageVolume(svName1, "S3", locations, storageParams, Optional.empty(), "");
        svm.updateStorageVolume(svName1, storageParams, Optional.empty(), "test update");
        svm.setDefaultStorageVolume(svName1);

        sv = svm.getStorageVolumeByName(svName);
        svm.unbindDbToStorageVolume(1L);
        svm.unbindTableToStorageVolume(1L);
        svm.removeStorageVolume(svName);
        Assert.assertFalse(svm.exists(svName));
    }

    @Test
    public void testCreateOrUpdateBuiltinStorageVolume() throws DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedNothingStorageVolumeMgr();
        Assert.assertEquals("", svm.createBuiltinStorageVolume());
        Assert.assertNull(svm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
    }
}
