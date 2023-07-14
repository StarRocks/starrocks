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
                fileStores.put(fsInfo.getFsKey(), fsInfo);
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
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
    public void testStorageVolumeCRUD() throws AlreadyExistsException, DdlException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        String svName = "test";
        String svName1 = "test1";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("aaa", "bbb");
        storageParams.put(AWS_S3_REGION, "region");
        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), ""));
        storageParams.remove("aaa");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assert.assertEquals(true, svm.exists(svName));
        Assert.assertEquals(svName, svm.getStorageVolumeName(svKey));
        StorageVolume sv = svm.getStorageVolumeByName(svName);
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
        StorageVolume sv1 = svm.getStorageVolume(sv.getId());
        Assert.assertEquals(sv1.getId(), sv.getId());
        try {
            svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
            Assert.fail();
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

        Throwable ex = Assert.assertThrows(IllegalStateException.class,
                () -> svm.updateStorageVolume(svName, storageParams, Optional.of(false), ""));
        Assert.assertEquals("Default volume can not be disabled", ex.getMessage());

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

        svm.createStorageVolume(svName1, "S3", locations, storageParams, Optional.of(false), "");
        svm.updateStorageVolume(svName1, storageParams, Optional.of(true), "test update");
        svm.setDefaultStorageVolume(svName1);

        sv = svm.getStorageVolumeByName(svName);
        svm.removeStorageVolume(svName);
        Assert.assertFalse(svm.exists(svName));
    }

    @Test
    public void testBindAndUnbind() throws DdlException, AlreadyExistsException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        Assert.assertThrows(DdlException.class, () -> svm.bindDbToStorageVolume("0", 1L));
        Assert.assertThrows(DdlException.class, () -> svm.bindTableToStorageVolume("0", 1L, 1L));
        // bind/unbind db and table to storage volume
        Assert.assertTrue(svm.bindDbToStorageVolume(svName, 1L));
        Assert.assertTrue(svm.bindTableToStorageVolume(svName, 1L, 1L));

        svm.updateStorageVolume(svName, storageParams, Optional.of(false), "test update");
        // disabled storage volume can not be bound.
        Throwable ex = Assert.assertThrows(DdlException.class, () -> svm.bindDbToStorageVolume(svName, 1L));
        Assert.assertEquals(String.format("Storage volume %s is disabled", svName), ex.getMessage());
        ex = Assert.assertThrows(DdlException.class, () -> svm.bindTableToStorageVolume(svName, 1L, 1L));
        Assert.assertEquals(String.format("Storage volume %s is disabled", svName), ex.getMessage());

        ex = Assert.assertThrows(IllegalStateException.class, () -> svm.removeStorageVolume(svName));
        Assert.assertEquals("Storage volume 'test' is referenced by dbs or tables, dbs: [1], tables: [1]", ex.getMessage());
        svm.unbindDbToStorageVolume(1L);
        svm.unbindTableToStorageVolume(1L);
        svm.removeStorageVolume(svName);
        Assert.assertFalse(svm.exists(svName));
    }

    @Test
    public void testParseParamsFromConfig() {
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
    public void testParseLocationsFromConfig() {
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
    public void testCreateOrUpdateBuiltinStorageVolume() throws DdlException, AlreadyExistsException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Assert.assertFalse(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
        String id = sdsvm.createOrUpdateBuiltinStorageVolume();
        Assert.assertTrue(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
        StorageVolume sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals(id, sdsvm.getDefaultStorageVolumeId());
        Assert.assertEquals("region", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getRegion());
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getEndpoint());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasSimpleCredential());

        Config.aws_s3_region = "region1";
        Config.aws_s3_endpoint = "endpoint1";
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertEquals("region1", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getRegion());
        Assert.assertEquals("endpoint1", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getEndpoint());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasSimpleCredential());

<<<<<<< HEAD
        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_use_aws_sdk_default_behavior = false;
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasProfileCredential());

        Config.aws_s3_iam_role_arn = "role_arn";
        Config.aws_s3_external_id = "external_id";
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasAssumeRoleCredential());

        String svKey = "test";
=======
        String svName = "test";
>>>>>>> a5c4b014a ([BugFix] Check whether storage volume is enabled (#27234))
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svName);

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().hasHdfsFsInfo());

        Config.cloud_native_storage_type = "azblob";
        Config.azure_blob_shared_key = "shared_key";
        Config.azure_blob_sas_token = "sas_token";
        Config.azure_blob_endpoint = "endpoint";
        Config.azure_blob_path = "path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createOrUpdateBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getEndpoint());
        Assert.assertEquals("shared_key",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSharedKey());
        Assert.assertEquals("sas_token",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSasToken());
    }

    @Test
    public void testGetDefaultStorageVolume() throws IllegalAccessException, AlreadyExistsException,
            DdlException, NoSuchFieldException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.createOrUpdateBuiltinStorageVolume();
        FieldUtils.writeField(sdsvm, "defaultStorageVolumeId", "", true);
        Assert.assertEquals(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME, sdsvm.getDefaultStorageVolume().getName());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svName);
        Assert.assertEquals(svName, sdsvm.getDefaultStorageVolume().getName());
    }

    @Test
    public void testGetStorageVolumeOfDb() throws DdlException, AlreadyExistsException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.createOrUpdateBuiltinStorageVolume();
        String defaultSVId = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME).getId();

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String testSVId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        StorageVolume sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfDb", StorageVolumeMgr.DEFAULT);
        Assert.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfDb", svName);
        Assert.assertEquals(testSVId, sv.getId());
    }

    @Test
    public void testGetStorageVolumeOfTable()
            throws DdlException, AlreadyExistsException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.createOrUpdateBuiltinStorageVolume();
        String defaultSVId = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME).getId();

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String testSVId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        new MockUp<SharedDataStorageVolumeMgr>() {
            @Mock
            public String getStorageVolumeIdOfDb(long dbId) {
                if (dbId == 1L) {
                    return testSVId;
                }
                return null;
            }
        };

        StorageVolume sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", "", 1L);
        Assert.assertEquals(testSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", "", 1L);
        Assert.assertEquals(testSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", "", 2L);
        Assert.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", StorageVolumeMgr.DEFAULT, 1L);
        Assert.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", svName, 1L);
        Assert.assertEquals(testSVId, sv.getId());
    }

    @Test
    public void testReplayBindDbToStorageVolume() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        sdsvm.replayBindDbToStorageVolume(svId, 1L);
        Assert.assertEquals(svId, sdsvm.getStorageVolumeIdOfDb(1L));
    }

    @Test
    public void testReplayBindTableToStorageVolume() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        sdsvm.replayBindTableToStorageVolume(svId, 1L);
        Assert.assertEquals(svId, sdsvm.getStorageVolumeIdOfTable(1L));
    }
}
