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

import com.staros.proto.FileStoreInfo;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aws.AWSCloudConfiguration;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.storagevolume.StorageVolume;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class SharedDataStorageVolumeMgrTest {
    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private EditLog editLog;

    @Before
    public void setUp() {
        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "access_key";
        Config.aws_s3_secret_key = "secret_key";
        Config.aws_s3_region = "region";
        Config.aws_s3_endpoint = "endpoint";
        Config.aws_s3_path = "default-bucket/1";

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            Map<String, FileStoreInfo> fileStores = new HashMap<>();
            private long id = 1;
            @Mock
            public String addFileStore(FileStoreInfo fsInfo) {
                fsInfo = fsInfo.toBuilder().setFsKey(String.valueOf(id++)).build();
                fileStores.put(fsInfo.getFsKey(), fsInfo);
                return fsInfo.getFsKey();
            }

            @Mock
            public void removeFileStoreByName(String fsName) throws DdlException {
                FileStoreInfo fsInfo = getFileStoreByName(fsName);
                if (fsInfo == null) {
                    throw new DdlException("Failed to remove file store");
                }
                fileStores.remove(fsInfo.getFsKey());
            }

            @Mock
            public FileStoreInfo getFileStoreByName(String fsName) {
                for (FileStoreInfo fsInfo : fileStores.values()) {
                    if (fsInfo.getFsName().equals(fsName)) {
                        return fsInfo;
                    }
                }
                return null;
            }

            @Mock
            public FileStoreInfo getFileStore(String fsKey) {
                return fileStores.get(fsKey);
            }

            @Mock
            public void updateFileStore(FileStoreInfo fsInfo) {
                FileStoreInfo fileStoreInfo = fileStores.get(fsInfo.getFsKey());
                fileStores.put(fsInfo.getFsKey(), fileStoreInfo.toBuilder().mergeFrom(fsInfo).build());
            }
        };
    }

    @After
    public void tearDown() {
        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "";
        Config.aws_s3_secret_key = "";
        Config.aws_s3_region = "";
        Config.aws_s3_endpoint = "";
        Config.aws_s3_path = "";
    }

    @Test
    public void testStorageVolumeCRUD() throws AnalysisException, AlreadyExistsException, DdlException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        String svKey = "test";
        String svKey1 = "test1";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume(svKey, "S3", locations, storageParams, Optional.empty(), "");
        Assert.assertEquals(true, svm.exists(svKey));
        StorageVolume sv = svm.getStorageVolumeByName(svKey);
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
        StorageVolume sv1 = svm.getStorageVolume(sv.getId());
        Assert.assertEquals(sv1.getId(), sv.getId());
        try {
            svm.createStorageVolume(svKey, "S3", locations, storageParams, Optional.empty(), "");
        } catch (AlreadyExistsException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test' already exists"));
        }

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_ACCESS_KEY, "ak");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        try {
            svm.updateStorageVolume(svKey1, storageParams, Optional.of(false), "test update");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        svm.updateStorageVolume(svKey, storageParams, Optional.of(true), "test update");
        sv = svm.getStorageVolumeByName(svKey);
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
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        svm.setDefaultStorageVolume(svKey);
        Assert.assertEquals(sv.getId(), svm.getDefaultStorageVolumeId());
        try {
            svm.updateStorageVolume(svKey, storageParams, Optional.of(false), "");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Default volume can not be disabled"));
        }

        // bind/unbind db and table to storage volume
        svm.bindDbToStorageVolume(sv.getId(), 1L);
        svm.bindTableToStorageVolume(sv.getId(), 1L);

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
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }

        svm.createStorageVolume(svKey1, "S3", locations, storageParams, Optional.empty(), "");
        svm.updateStorageVolume(svKey1, storageParams, Optional.empty(), "test update");
        svm.setDefaultStorageVolume(svKey1);

        sv = svm.getStorageVolumeByName(svKey);

        try {
            svm.removeStorageVolume(svKey);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test' is referenced by dbs or tables, " +
                    "dbs: [1], tables: [1]"));
        }
        svm.unbindDbToStorageVolume(1L);
        svm.unbindTableToStorageVolume(1L);
        svm.removeStorageVolume(svKey);
        Assert.assertFalse(svm.exists(svKey));
    }

    @Test
    public void testParseParamsFromConfig() throws AnalysisException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Map<String, String> params = Deencapsulation.invoke(sdsvm, "parseParamsFromConfig");
        Assert.assertEquals("access_key", params.get(AWS_S3_ACCESS_KEY));
        Assert.assertEquals("secret_key", params.get(AWS_S3_SECRET_KEY));
        Assert.assertEquals("region", params.get(AWS_S3_REGION));
        Assert.assertEquals("endpoint", params.get(AWS_S3_ENDPOINT));

        Config.cloud_native_storage_type = "aaa";
        params = Deencapsulation.invoke(sdsvm, "parseParamsFromConfig");
        Assert.assertEquals(0, params.size());
    }

    @Test
    public void testParseLocationsFromConfig() throws AnalysisException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        List<String> locations = Deencapsulation.invoke(sdsvm, "parseLocationsFromConfig");
        Assert.assertEquals(1, locations.size());
        Assert.assertEquals("s3://default-bucket/1", locations.get(0));

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        locations = Deencapsulation.invoke(sdsvm, "parseLocationsFromConfig");
        Assert.assertEquals(1, locations.size());
        Assert.assertEquals("hdfs://url", locations.get(0));
    }

    @Test
    public void testCreateOrUpdateBuiltinStorageVolume() throws AnalysisException, DdlException, AlreadyExistsException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Assert.assertFalse(sdsvm.exists(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
        sdsvm.createOrUpdateBuiltinStorageVolume();
        Assert.assertTrue(sdsvm.exists(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
        StorageVolume sv = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals(sv.getId(), sdsvm.getDefaultStorageVolumeId());
        Assert.assertEquals("region", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getRegion());
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getEndpoint());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasSimpleCredential());

        Config.aws_s3_region = "region1";
        Config.aws_s3_endpoint = "endpoint1";
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertEquals("region1", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getRegion());
        Assert.assertEquals("endpoint1", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getEndpoint());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasSimpleCredential());

        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_use_aws_sdk_default_behavior = false;
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasProfileCredential());

        Config.aws_s3_iam_role_arn = "role_arn";
        Config.aws_s3_external_id = "external_id";
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasAssumeRoleCredential());

        String svKey = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svKey, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svKey);

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        sdsvm.removeStorageVolume(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().hasHdfsFsInfo());

        Config.cloud_native_storage_type = "azblob";
        Config.azure_blob_shared_key = "shared_key";
        Config.azure_blob_sas_token = "sas_token";
        Config.azure_blob_endpoint = "endpoint";
        Config.azure_blob_path = "path";
        sdsvm.removeStorageVolume(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getEndpoint());
        Assert.assertEquals("shared_key",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSharedKey());
        Assert.assertEquals("sas_token",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSasToken());
    }

    @Test
    public void testGetDefaultStorageVolume() throws IllegalAccessException, AnalysisException, AlreadyExistsException,
            DdlException, NoSuchFieldException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.createOrUpdateBuiltinStorageVolume();
        FieldUtils.writeField(sdsvm, "defaultStorageVolumeId", "", true);
        Assert.assertEquals(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME, sdsvm.getDefaultStorageVolume().getName());

        String svKey = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svKey, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svKey);
        Assert.assertEquals(svKey, sdsvm.getDefaultStorageVolume().getName());
    }
}
