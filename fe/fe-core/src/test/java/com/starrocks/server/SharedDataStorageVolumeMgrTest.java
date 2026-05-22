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

import com.google.common.collect.Lists;
import com.google.gson.stream.JsonReader;
import com.staros.client.StarClientException;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.storagevolume.CompositeStorageVolume;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class SharedDataStorageVolumeMgrTest {
    @Mocked
    private StarOSAgent starOSAgent;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();

        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "access_key";
        Config.aws_s3_secret_key = "secret_key";
        Config.aws_s3_region = "region";
        Config.aws_s3_endpoint = "endpoint";
        Config.aws_s3_path = "default-bucket/1";
        Config.enable_load_volume_from_conf = true;

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
                if (fsInfo.getFsKey().isEmpty()) {
                    fsInfo = fsInfo.toBuilder().setFsKey(String.valueOf(id++)).build();
                }
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

            @Mock
            public List<FileStoreInfo> listFileStore() {
                return new ArrayList<>(fileStores.values());
            }
        };
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();

        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "";
        Config.aws_s3_secret_key = "";
        Config.aws_s3_region = "";
        Config.aws_s3_endpoint = "";
        Config.aws_s3_path = "";
        Config.enable_load_volume_from_conf = false;
    }

    @Test
    public void testStorageVolumeCRUD() throws AlreadyExistsException, DdlException, MetaNotFoundException {
        String svName = "test";
        String svName1 = "test1";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("aaa", "bbb");
        storageParams.put(AWS_S3_REGION, "region");
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), ""));
        storageParams.remove("aaa");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assertions.assertEquals(true, svm.exists(svName));
        Assertions.assertEquals(svName, svm.getStorageVolumeName(svKey));
        StorageVolume sv = svm.getStorageVolumeByName(svName);
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
        StorageVolume sv1 = svm.getStorageVolume(sv.getId());
        Assertions.assertEquals(sv1.getId(), sv.getId());
        try {
            svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
            Assertions.fail();
        } catch (AlreadyExistsException e) {
            Assertions.assertTrue(e.getMessage().contains("Storage volume 'test' already exists"));
        }

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_ACCESS_KEY, "ak");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        Assertions.assertThrows(MetaNotFoundException.class, () ->
                svm.updateStorageVolume(svName1, null, null, storageParams, Optional.of(true), "test update"));
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

        Throwable ex = Assertions.assertThrows(IllegalStateException.class,
                () -> svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(false), ""));
        Assertions.assertEquals("Default volume can not be disabled", ex.getMessage());

        // remove
        try {
            svm.removeStorageVolume(svName);
            Assertions.fail();
        } catch (IllegalStateException e) {
            Assertions.assertTrue(e.getMessage().contains("default storage volume can not be removed"));
        }

        ex = Assertions.assertThrows(MetaNotFoundException.class, () -> svm.removeStorageVolume(svName1));
        Assertions.assertEquals("Storage volume 'test1' does not exist", ex.getMessage());

        svm.createStorageVolume(svName1, "S3", locations, storageParams, Optional.of(false), "");
        svm.updateStorageVolume(svName1, null, null, storageParams, Optional.of(true), "test update");
        svm.setDefaultStorageVolume(svName1);

        sv = svm.getStorageVolumeByName(svName);
        svm.removeStorageVolume(svName);
        Assertions.assertFalse(svm.exists(svName));
    }

    @Test
    public void testImmutableProperties() throws DdlException, AlreadyExistsException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = List.of("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assertions.assertTrue(svm.exists(svName));

        {
            Map<String, String> modifyParams = new HashMap<>();
            modifyParams.put(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "true");
            Assertions.assertThrows(DdlException.class, () ->
                    svm.updateStorageVolume(svName, null, null, modifyParams, Optional.of(false), ""));
        }

        {
            Map<String, String> modifyParams = new HashMap<>();
            modifyParams.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "12");
            Assertions.assertThrows(DdlException.class, () ->
                    svm.updateStorageVolume(svName, null, null, modifyParams, Optional.of(false), ""));
        }
    }

    @Test
    public void testBindAndUnbind() throws DdlException, AlreadyExistsException, MetaNotFoundException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        Assertions.assertThrows(DdlException.class, () -> svm.bindDbToStorageVolume("0", 1L));
        Assertions.assertThrows(DdlException.class, () -> svm.bindTableToStorageVolume("0", 1L, 1L));
        // bind/unbind db and table to storage volume
        Assertions.assertTrue(svm.bindDbToStorageVolume(svName, 1L));
        Assertions.assertTrue(svm.bindTableToStorageVolume(svName, 1L, 1L));

        svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(false), "test update");
        // disabled storage volume can not be bound.
        Throwable ex = Assertions.assertThrows(DdlException.class, () -> svm.bindDbToStorageVolume(svName, 1L));
        Assertions.assertEquals(String.format("Storage volume %s is disabled", svName), ex.getMessage());
        ex = Assertions.assertThrows(DdlException.class, () -> svm.bindTableToStorageVolume(svName, 1L, 1L));
        Assertions.assertEquals(String.format("Storage volume %s is disabled", svName), ex.getMessage());

        ex = Assertions.assertThrows(IllegalStateException.class, () -> svm.removeStorageVolume(svName));
        Assertions.assertEquals("Storage volume 'test' is referenced by dbs or tables, dbs: [1], tables: [1]", ex.getMessage());
        svm.unbindDbToStorageVolume(1L);
        svm.unbindTableToStorageVolume(1L);
        svm.removeStorageVolume(svName);
        Assertions.assertFalse(svm.exists(svName));
    }

    @Test
    public void testParseParamsFromConfig() {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Map<String, String> params = Deencapsulation.invoke(sdsvm, "parseParamsFromConfig");
        Assertions.assertEquals("access_key", params.get(AWS_S3_ACCESS_KEY));
        Assertions.assertEquals("secret_key", params.get(AWS_S3_SECRET_KEY));
        Assertions.assertEquals("region", params.get(AWS_S3_REGION));
        Assertions.assertEquals("endpoint", params.get(AWS_S3_ENDPOINT));

        Config.cloud_native_storage_type = "aaa";
        params = Deencapsulation.invoke(sdsvm, "parseParamsFromConfig");
        Assertions.assertEquals(0, params.size());
    }

    @Test
    public void testParseLocationsFromConfig() throws InvalidConfException {
        Config.cloud_native_storage_type = "s3";
        Config.aws_s3_path = "default-bucket/1";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assertions.assertEquals(1, locations.size());
            Assertions.assertEquals("s3://default-bucket/1", locations.get(0));
        }

        // with s3:// prefix
        Config.aws_s3_path = "s3://default-bucket/1";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assertions.assertEquals(1, locations.size());
            Assertions.assertEquals("s3://default-bucket/1", locations.get(0));
        }

        Config.aws_s3_path = "s3://default-bucket";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assertions.assertEquals(1, locations.size());
            Assertions.assertEquals("s3://default-bucket", locations.get(0));
        }

        Config.aws_s3_path = "s3://default-bucket/";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assertions.assertEquals(1, locations.size());
            Assertions.assertEquals("s3://default-bucket/", locations.get(0));
        }

        // with invalid prefix
        Config.aws_s3_path = "://default-bucket/1";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);
        // with wrong prefix
        Config.aws_s3_path = "hdfs://default-bucket/1";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "s3://";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "bucketname:30/b";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "s3://bucketname:9030/b";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "/";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "";
        Assertions.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assertions.assertEquals(1, locations.size());
            Assertions.assertEquals("hdfs://url", locations.get(0));
        }
        Config.cloud_native_hdfs_url = "viewfs://host:9030/a/b/c";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assertions.assertEquals(1, locations.size());
            Assertions.assertEquals("viewfs://host:9030/a/b/c", locations.get(0));
        }
    }

    @Test
    public void testCreateBuiltinStorageVolume() throws DdlException, AlreadyExistsException, MetaNotFoundException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Assertions.assertFalse(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));

        Config.enable_load_volume_from_conf = false;
        sdsvm.createBuiltinStorageVolume();
        Assertions.assertFalse(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));

        Config.enable_load_volume_from_conf = true;
        String id = sdsvm.createBuiltinStorageVolume();
        String[] bucketAndPrefix = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assertions.assertEquals(bucketAndPrefix[0], id);
        Assertions.assertTrue(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
        StorageVolume sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertEquals(id, sdsvm.getDefaultStorageVolumeId());

        FileStoreInfo fsInfo = sv.getCloudConfiguration().toFileStoreInfo();
        Assertions.assertEquals("region", fsInfo.getS3FsInfo().getRegion());
        Assertions.assertEquals("endpoint", fsInfo.getS3FsInfo().getEndpoint());
        Assertions.assertTrue(fsInfo.getS3FsInfo().hasCredential());
        Assertions.assertTrue(fsInfo.getS3FsInfo().getCredential().hasSimpleCredential());
        Assertions.assertFalse(fsInfo.getS3FsInfo().getPartitionedPrefixEnabled());
        Assertions.assertEquals(0, fsInfo.getS3FsInfo().getNumPartitionedPrefix());

        // Builtin storage volume has existed, the conf will be ignored
        Config.aws_s3_region = "region1";
        Config.aws_s3_endpoint = "endpoint1";
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assertions.assertEquals("region", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getRegion());
        Assertions.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getEndpoint());
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasSimpleCredential());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svName);

        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_use_aws_sdk_default_behavior = false;
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasProfileCredential());

        Config.aws_s3_iam_role_arn = "role_arn";
        Config.aws_s3_external_id = "external_id";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assertions.assertTrue(
                sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasAssumeRoleCredential());

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        id = sdsvm.createBuiltinStorageVolume();
        Assertions.assertEquals(Config.cloud_native_hdfs_url, id);
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().hasHdfsFsInfo());

        Config.cloud_native_storage_type = "azblob";
        Config.azure_blob_shared_key = "shared_key";
        Config.azure_blob_sas_token = "sas_token";
        Config.azure_blob_endpoint = "endpoint";
        Config.azure_blob_path = "path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getEndpoint());
        Assertions.assertEquals("shared_key",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSharedKey());
        Assertions.assertEquals("sas_token",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSasToken());

        Config.cloud_native_storage_type = "adls2";
        Config.azure_adls2_shared_key = "shared_key";
        Config.azure_adls2_sas_token = "sas_token";
        Config.azure_adls2_endpoint = "endpoint";
        Config.azure_adls2_path = "path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getAdls2FsInfo().getEndpoint());
        Assertions.assertEquals("shared_key",
                sv.getCloudConfiguration().toFileStoreInfo().getAdls2FsInfo().getCredential().getSharedKey());
        Assertions.assertEquals("sas_token",
                sv.getCloudConfiguration().toFileStoreInfo().getAdls2FsInfo().getCredential().getSasToken());

        Config.cloud_native_storage_type = "GS";
        Config.gcp_gcs_use_compute_engine_service_account = "true";
        Config.gcp_gcs_path = "gs://gs_path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assertions.assertEquals(true,
                sv.getCloudConfiguration().toFileStoreInfo().getGsFsInfo().getUseComputeEngineServiceAccount());
    }

    @Test
    public void testGetDefaultStorageVolume() throws IllegalAccessException, AlreadyExistsException,
            DdlException, NoSuchFieldException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.createBuiltinStorageVolume();
        FieldUtils.writeField(sdsvm, "defaultStorageVolumeId", "", true);
        Assertions.assertEquals(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME, sdsvm.getDefaultStorageVolume().getName());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svName);
        Assertions.assertEquals(svName, sdsvm.getDefaultStorageVolume().getName());
    }

    @Test
    public void testGetStorageVolumeOfDb() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class, () -> Deencapsulation.invoke(sdsvm,
                "getStorageVolumeOfDb", StorageVolumeMgr.DEFAULT));
        Assertions.assertEquals(ErrorCode.ERR_NO_DEFAULT_STORAGE_VOLUME, ex.getErrorCode());
        sdsvm.createBuiltinStorageVolume();
        String defaultSVId = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME).getId();

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String testSVId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        StorageVolume sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfDb", StorageVolumeMgr.DEFAULT);
        Assertions.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfDb", svName);
        Assertions.assertEquals(testSVId, sv.getId());
    }

    @Test
    public void testGetStorageVolumeOfTable()
            throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();

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
        Assertions.assertEquals(testSVId, sv.getId());
        Config.enable_load_volume_from_conf = false;
        ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class, () -> Deencapsulation.invoke(sdsvm,
                "getStorageVolumeOfTable", "", 2L));
        Assertions.assertEquals(ErrorCode.ERR_NO_DEFAULT_STORAGE_VOLUME, ex.getErrorCode());
        Config.enable_load_volume_from_conf = true;
        ex = Assertions.assertThrows(ErrorReportException.class, () -> Deencapsulation.invoke(sdsvm,
                "getStorageVolumeOfTable", "", 2L));
        Assertions.assertEquals(ErrorCode.ERR_NO_DEFAULT_STORAGE_VOLUME, ex.getErrorCode());
        sdsvm.createBuiltinStorageVolume();
        String defaultSVId = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME).getId();
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", StorageVolumeMgr.DEFAULT, 1L);
        Assertions.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", svName, 1L);
        Assertions.assertEquals(testSVId, sv.getId());
    }

    @Test
    public void testReplayBindDbToStorageVolume() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.replayBindDbToStorageVolume(null, 2L);
        Assertions.assertTrue(sdsvm.dbToStorageVolume.isEmpty());
        Assertions.assertTrue(sdsvm.storageVolumeToDbs.isEmpty());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        sdsvm.replayBindDbToStorageVolume(svId, 1L);
        Assertions.assertEquals(svId, sdsvm.getStorageVolumeIdOfDb(1L));
    }

    @Test
    public void testReplayBindTableToStorageVolume() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.replayBindTableToStorageVolume(null, 2L);
        Assertions.assertTrue(sdsvm.tableToStorageVolume.isEmpty());
        Assertions.assertTrue(sdsvm.storageVolumeToTables.isEmpty());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        sdsvm.replayBindTableToStorageVolume(svId, 1L);
        Assertions.assertEquals(svId, sdsvm.getStorageVolumeIdOfTable(1L));
    }

    @Test
    public void testGetBucketAndPrefix() throws Exception {
        String oldAwsS3Path = Config.aws_s3_path;

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Config.aws_s3_path = "bucket/dir1/dir2";
        String[] bucketAndPrefix1 = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assertions.assertEquals(2, bucketAndPrefix1.length);
        Assertions.assertEquals("bucket", bucketAndPrefix1[0]);
        Assertions.assertEquals("dir1/dir2", bucketAndPrefix1[1]);

        Config.aws_s3_path = "bucket";
        String[] bucketAndPrefix2 = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assertions.assertEquals(2, bucketAndPrefix2.length);
        Assertions.assertEquals("bucket", bucketAndPrefix2[0]);
        Assertions.assertEquals("", bucketAndPrefix2[1]);

        Config.aws_s3_path = "bucket/";
        String[] bucketAndPrefix3 = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assertions.assertEquals(2, bucketAndPrefix3.length);
        Assertions.assertEquals("bucket", bucketAndPrefix3[0]);
        Assertions.assertEquals("", bucketAndPrefix3[1]);

        // allow leading s3:// in configuration, will be just ignored.
        Config.aws_s3_path = "s3://a-bucket/b";
        {
            String[] bucketAndPrefix = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
            Assertions.assertEquals(2, bucketAndPrefix.length);
            Assertions.assertEquals("a-bucket", bucketAndPrefix[0]);
            Assertions.assertEquals("b", bucketAndPrefix[1]);
        }

        Config.aws_s3_path = oldAwsS3Path;
    }

    @Test
    public void testGetAwsCredentialType() throws Exception {
        boolean oldAwsS3UseAwsSdkDefaultBehavior = Config.aws_s3_use_aws_sdk_default_behavior;
        boolean oldAwsS3UseInstanceProfile = Config.aws_s3_use_instance_profile;
        String oldAwsS3AccessKey = Config.aws_s3_access_key;
        String oldAwsS3SecretKey = Config.aws_s3_secret_key;
        String oldAwsS3IamRoleArn = Config.aws_s3_iam_role_arn;

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Config.aws_s3_use_aws_sdk_default_behavior = true;
        String credentialType1 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assertions.assertEquals("default", credentialType1);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_iam_role_arn = "";
        String credentialType2 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assertions.assertEquals("instance_profile", credentialType2);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_iam_role_arn = "abc";
        String credentialType3 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assertions.assertEquals("assume_role", credentialType3);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = false;
        Config.aws_s3_access_key = "";
        String credentialType4 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assertions.assertNull(credentialType4);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = false;
        Config.aws_s3_access_key = "abc";
        Config.aws_s3_secret_key = "abc";
        Config.aws_s3_iam_role_arn = "";
        String credentialType5 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assertions.assertEquals("simple", credentialType5);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = false;
        Config.aws_s3_access_key = "abc";
        Config.aws_s3_secret_key = "abc";
        Config.aws_s3_iam_role_arn = "abc";
        String credentialType6 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assertions.assertNull(credentialType6);

        Config.aws_s3_use_aws_sdk_default_behavior = oldAwsS3UseAwsSdkDefaultBehavior;
        Config.aws_s3_use_instance_profile = oldAwsS3UseInstanceProfile;
        Config.aws_s3_access_key = oldAwsS3AccessKey;
        Config.aws_s3_secret_key = oldAwsS3SecretKey;
        Config.aws_s3_iam_role_arn = oldAwsS3IamRoleArn;
    }

    @Test
    public void testValidateStorageVolumeConfig() {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();

        Config.cloud_native_storage_type = "s3";
        Config.aws_s3_path = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.aws_s3_path = "path";
        Config.aws_s3_region = "";
        Config.aws_s3_endpoint = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.cloud_native_storage_type = "azblob";
        Config.azure_blob_path = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.azure_blob_path = "blob";
        Config.azure_blob_endpoint = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.cloud_native_storage_type = "adls2";
        Config.azure_adls2_path = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.azure_adls2_path = "adls2";
        Config.azure_adls2_endpoint = "";
        Assertions.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);
    }

    @Test
    public void testSerialization() throws IOException, SRMetaBlockException,
            SRMetaBlockEOFException, DdlException, AlreadyExistsException {
        String svName = "test";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        svm.setDefaultStorageVolume("test");
        svm.bindDbToStorageVolume(svName, 1L);
        svm.bindTableToStorageVolume(svName, 1L, 1L);
        Map<String, Set<Long>> storageVolumeToDbs = svm.storageVolumeToDbs;
        Map<String, Set<Long>> storageVolumeToTables = svm.storageVolumeToTables;
        Map<Long, String> dbToStorageVolume = svm.dbToStorageVolume;
        Map<Long, String> tableToStorageVolume = svm.tableToStorageVolume;

        // v4
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        ImageWriter imageWriter = new ImageWriter("", 0);
        imageWriter.setOutputStream(dos);
        svm.save(imageWriter);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(in)));
        StorageVolumeMgr svm1 = new SharedDataStorageVolumeMgr();
        svm1.load(reader);
        Assertions.assertEquals(svId, svm1.getDefaultStorageVolumeId());
        Assertions.assertEquals(storageVolumeToDbs, svm1.storageVolumeToDbs);
        Assertions.assertEquals(storageVolumeToTables, svm1.storageVolumeToTables);
        Assertions.assertEquals(dbToStorageVolume, svm1.dbToStorageVolume);
        Assertions.assertEquals(tableToStorageVolume, svm1.tableToStorageVolume);
    }

    @Test
    public void testGetTableBindingsOfBuiltinStorageVolume() throws DdlException, AlreadyExistsException {
        new MockUp<LocalMetastore>() {
            @Mock
            public List<Long> getDbIdsIncludeRecycleBin() {
                return Arrays.asList(10001L);
            }

            @Mock
            public Database getDbIncludeRecycleBin(long dbId) {
                return new Database(dbId, "");
            }

            @Mock
            public List<Table> getTablesIncludeRecycleBin(Database db) {
                long dbId = 10001L;
                long tableId = 10002L;
                long partitionId = 10003L;
                long indexId = 10004L;
                long tablet1Id = 10010L;
                long tablet2Id = 10011L;

                // Schema
                List<Column> columns = Lists.newArrayList();
                Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
                columns.add(k1);
                columns.add(new Column("k2", IntegerType.BIGINT, true, null, "", ""));
                columns.add(new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

                // Tablet
                Tablet tablet1 = new LakeTablet(tablet1Id);
                Tablet tablet2 = new LakeTablet(tablet2Id);

                // Index
                MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.HDD, true);
                index.addTablet(tablet1, tabletMeta);
                index.addTablet(tablet2, tabletMeta);

                // Partition
                DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
                PartitionInfo partitionInfo = new SinglePartitionInfo();
                partitionInfo.setReplicationNum(partitionId, (short) 3);

                // Lake table
                LakeTable table = new LakeTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
                return Arrays.asList(table);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Assertions.assertEquals(Arrays.asList(Arrays.asList(10001L), Arrays.asList(10002L)),
                sdsvm.getBindingsOfBuiltinStorageVolume());

        sdsvm.createBuiltinStorageVolume();
        sdsvm.bindDbToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 10001L);
        Assertions.assertEquals(Arrays.asList(new ArrayList(), new ArrayList()), sdsvm.getBindingsOfBuiltinStorageVolume());

        sdsvm.unbindDbToStorageVolume(10001L);
        sdsvm.bindTableToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 10001L, 10002L);
        Assertions.assertEquals(Arrays.asList(Arrays.asList(10001L), new ArrayList()), sdsvm.getBindingsOfBuiltinStorageVolume());
    }

    @Test
    public void testGetStorageVolumeNameOfTable() throws DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.createBuiltinStorageVolume();
        svm.bindTableToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 1L, 1L);
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfTable(1L));
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfTable(2L));
    }

    @Test
    public void testGetStorageVolumeNameOfDb() throws DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.createBuiltinStorageVolume();
        svm.bindDbToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 1L);
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfDb(1L));
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfDb(2L));
    }

    @Test
    public void testCreateHDFS() throws DdlException, AlreadyExistsException, MetaNotFoundException {
        String svName = "test";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("hdfs://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        String svKey = svm.createStorageVolume(svName, "hdfs", locations, storageParams, Optional.empty(), "");
        Assertions.assertEquals(true, svm.exists(svName));

        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(false), "");
        Assertions.assertEquals(false, svm.getStorageVolumeByName(svName).getEnabled());
    }

    @Test
    public void testCreateStorageVolumeWithInvalidParams() throws DdlException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = new ArrayList<>(Arrays.asList("{hdfs://abc}"));
        Map<String, String> storageParams = new HashMap<>();
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "hdfs", locations, storageParams, Optional.empty(), ""));

        locations.clear();
        locations.add("ablob://abc");
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "s3", locations, storageParams, Optional.empty(), ""));

        locations.clear();
        locations.add("s3://abc");
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "azblob", locations, storageParams, Optional.empty(), ""));

        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "abc", locations, storageParams, Optional.empty(), ""));

        {
            // only for s3
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "32");
            Assertions.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "azblob", locations, params, Optional.empty(), ""));
        }
        {
            // only for s3
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "true");
            Assertions.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "azblob", locations, params, Optional.empty(), ""));
        }
        {
            // should be a number
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "not_a_number");
            Assertions.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "s3", locations, params, Optional.empty(), ""));
        }
        {
            // should be a positive integer
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "-1");
            Assertions.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "s3", locations, params, Optional.empty(), ""));
        }
    }

    @Test
    public void testUpgrade() throws IOException, SRMetaBlockException, SRMetaBlockEOFException,
            DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.createBuiltinStorageVolume();
        Set<Long> dbs = new HashSet<>();
        dbs.add(1L);
        svm.storageVolumeToDbs.put(null, dbs);
        Set<Long> tables = new HashSet<>();
        tables.add(2L);
        svm.storageVolumeToTables.put(null, tables);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        ImageWriter imageWriter = new ImageWriter("", 0);
        imageWriter.setOutputStream(dos);
        svm.save(imageWriter);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(in)));
        StorageVolumeMgr svm1 = new SharedDataStorageVolumeMgr();
        svm1.load(reader);
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm1.getDefaultStorageVolume().getName());
        Assertions.assertTrue(svm1.storageVolumeToDbs.isEmpty());
        Assertions.assertTrue(svm1.storageVolumeToTables.isEmpty());
        Assertions.assertTrue(svm1.dbToStorageVolume.isEmpty());
        Assertions.assertTrue(svm1.tableToStorageVolume.isEmpty());
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm1.getStorageVolumeNameOfDb(1L));
        Assertions.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm1.getStorageVolumeNameOfTable(1L));
    }

    @Test
    public void testDropSnapshotStorageVolume(@Mocked GlobalStateMgr globalStateMgr)
            throws AlreadyExistsException, DdlException, MetaNotFoundException {
        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;

                globalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshotSvName();
                result = "test_snap";
            }
        };

        String svName = "test_snap";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("aaa", "bbb");
        storageParams.put(AWS_S3_REGION, "region");
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), ""));
        storageParams.remove("aaa");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assertions.assertEquals(true, svm.exists(svName));
        Assertions.assertEquals(svName, svm.getStorageVolumeName(svKey));
        // remove
        DdlException ex = Assertions.assertThrows(DdlException.class, () -> svm.removeStorageVolume(svName));
        Assertions.assertEquals("Snapshot enabled on storage volume 'test_snap', drop volume failed.", ex.getMessage());
    }

    @Test
    public void testRestoreStorageVolumeToVTabletGroupMappings() {
        new MockUp<GlobalStateMgr>() {

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };


        // Test normal
        new MockUp<StarOSAgent>() {
            private long id = 1;
            final long existedShardId = 10001L;
            final long existedGroupId = 20001L;

            @Mock
            public List<FileStoreInfo> listFileStore() throws DdlException {
                Map<String, String> properties = new HashMap<>();
                properties.put(StorageVolume.V_SHARD_ID, String.valueOf(existedShardId));
                properties.put(StorageVolume.V_SHARD_GROUP_ID, String.valueOf(existedGroupId));
                FileStoreInfo fsInfo1 = FileStoreInfo.newBuilder().setFsKey(String.valueOf(id++))
                        .putAllProperties(properties).build();

                FileStoreInfo fsInfo2 = FileStoreInfo.newBuilder().setFsKey(String.valueOf(id++)).build();

                properties = new HashMap<>();
                properties.put(StorageVolume.V_SHARD_ID, String.valueOf(existedShardId));
                properties.put(StorageVolume.V_SHARD_GROUP_ID, "not valid format number");
                FileStoreInfo fsInfo3 = FileStoreInfo.newBuilder().setFsKey(String.valueOf(id++))
                        .putAllProperties(properties).build();
                return List.of(fsInfo1, fsInfo2, fsInfo3);
            }
        };

        SharedDataStorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.restoreStorageVolumeToVTabletGroupMappings();

        Assert.assertTrue(svm.hasStorageVolumeBindAsVirtualGroup(20001L));

        // corner case: listFileStore failed with DDL exception
        // storage mapping should keep unmodified
        new MockUp<StarOSAgent>() {

            @Mock
            public List<FileStoreInfo> listFileStore() throws DdlException {
                throw new DdlException("Mocked exception");
            }
        };

        svm.restoreStorageVolumeToVTabletGroupMappings();
        Assert.assertTrue(svm.hasStorageVolumeBindAsVirtualGroup(20001L));
    }

    @Test
    public void testGetOrCreateVirtualTabletIdStorageVolumeNotExist() {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();

        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "Unknown src storage volume while creating virtual tablet: " + storageVolumeName,
                () -> svm.getOrCreateVirtualTabletId(storageVolumeName, srcServiceId));
    }

    @Test
    public void testGetOrCreateVirtualTabletIdNormal()
            throws DdlException, AlreadyExistsException, StarClientException, MetaNotFoundException {

        long expectedVirtualTabletId = 20001;
        new MockUp<GlobalStateMgr>() {

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }

            @Mock
            public long getNextId() {
                return expectedVirtualTabletId;
            }
        };

        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();

        // create
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(storageVolumeName, "S3", locations, storageParams, Optional.empty(), "");
        Assertions.assertTrue(svm.exists(storageVolumeName));
        Assertions.assertEquals(storageVolumeName, svm.getStorageVolumeName(svKey));

        long groupId = 1001L;
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        new Expectations() {
            {
                starOSAgent.allocateFilePath(anyString, srcServiceId);
                result = pathInfo;

                starOSAgent.createShardGroupForVirtualTablet();
                result = groupId;

                starOSAgent.createShardWithVirtualTabletId(pathInfo, (FileCacheInfo) any, groupId, (HashMap) any,
                        expectedVirtualTabletId, WarehouseManager.DEFAULT_RESOURCE);
                result = null;
            }
        };

        ExceptionChecker.expectThrowsNoException(() -> {
            Assertions.assertEquals(expectedVirtualTabletId, svm.getOrCreateVirtualTabletId(storageVolumeName, srcServiceId));
        });

        svm.removeStorageVolume(storageVolumeName);
    }

    @Test
    public void testGetOrCreateVirtualTabletIdWhileStorageVolumeAlreadyHasVirtualTabletId()
            throws DdlException, AlreadyExistsException, MetaNotFoundException {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            Map<String, FileStoreInfo> fileStores = new HashMap<>();
            private long id = 1;
            final long existedShardId = 10001L;
            final long existedGroupId = 20001L;

            @Mock
            public String addFileStore(FileStoreInfo fsInfo) {
                if (fsInfo.getFsKey().isEmpty()) {
                    Map<String, String> properties = new HashMap<>();
                    properties.put(StorageVolume.V_SHARD_ID, String.valueOf(existedShardId));
                    properties.put(StorageVolume.V_SHARD_GROUP_ID, String.valueOf(existedGroupId));
                    fsInfo = fsInfo.toBuilder().setFsKey(String.valueOf(id++)).putAllProperties(properties).build();
                }
                fileStores.put(fsInfo.getFsKey(), fsInfo);
                return fsInfo.getFsKey();
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
        };

        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";

        // create
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");

        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        String svKey = svm.createStorageVolume(storageVolumeName, "S3", locations, storageParams, Optional.empty(), "");
        Assertions.assertTrue(svm.exists(storageVolumeName));
        Assertions.assertEquals(storageVolumeName, svm.getStorageVolumeName(svKey));

        long existedShardId = 10001L;
        long existedGroupId = 20001L;
        StorageVolume storageVolume = svm.getStorageVolumeByName(storageVolumeName);
        storageVolume.setVTabletId(existedShardId);
        storageVolume.setVTabletGroupId(existedGroupId);

        ExceptionChecker.expectThrowsNoException(() -> {
            Assertions.assertEquals(existedShardId, svm.getOrCreateVirtualTabletId(storageVolumeName, srcServiceId));
        });

        long notExistedGroupId = 30001L;
        svm.updateStorageVolumeVTabletMapping(storageVolumeName, existedShardId, existedGroupId);
        Assertions.assertTrue(svm.hasStorageVolumeBindAsVirtualGroup(existedGroupId));
        Assertions.assertFalse(svm.hasStorageVolumeBindAsVirtualGroup(notExistedGroupId));

        svm.removeStorageVolume(storageVolumeName);
    }

    // ========== Composite Storage Volume Tests ==========
    // These tests verify that Composite SV definitions are persisted solely in StarOS FileStore
    // (via the mocked StarOSAgent) and that FE has no local cache for them.

    /**
     * Helper: create two child SVs and return their keys. Shared setup for composite SV tests.
     */
    private String[] createChildSVs(StorageVolumeMgr svm) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(AWS_S3_REGION, "region");
        params.put(AWS_S3_ENDPOINT, "endpoint");
        params.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String child1Key = svm.createStorageVolume("child1", "S3",
                Arrays.asList("s3://bucket1"), params, Optional.of(true), "child 1");
        String child2Key = svm.createStorageVolume("child2", "S3",
                Arrays.asList("s3://bucket2"), params, Optional.of(true), "child 2");
        return new String[] {child1Key, child2Key};
    }

    @Test
    public void testCreateCompositeStorageVolumeViaTypeRoute() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);

        Map<String, String> properties = new HashMap<>();
        properties.put("child_volumes", "child1,child2");
        String csvId = svm.createStorageVolume("comp_by_type", "COMPOSITE", Collections.emptyList(),
                properties, Optional.of(true), "created via generic route");
        Assertions.assertNotNull(csvId);

        CompositeStorageVolume csv = svm.getCompositeStorageVolumeByName("comp_by_type");
        Assertions.assertNotNull(csv);
        Assertions.assertEquals(2, csv.getChildVolumeIds().size());

        Map<String, String> missingChildProps = new HashMap<>();
        Assertions.assertThrows(DdlException.class,
                () -> svm.createStorageVolume("comp_missing_child", "COMPOSITE", Collections.emptyList(),
                        missingChildProps, Optional.of(true), ""));
    }

    @Test
    public void testCompositeStorageVolumeAddChildValidation() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);

        Map<String, String> hdfsParams = new HashMap<>();
        svm.createStorageVolume("hdfs_child", "HDFS", Arrays.asList("hdfs://cluster/path"),
                hdfsParams, Optional.of(true), "hdfs child");

        svm.createCompositeStorageVolume("comp_add_validation", Arrays.asList("child1"), true, "");

        // Composite not found
        Assertions.assertThrows(DdlException.class,
                () -> svm.addChildToCompositeStorageVolume("not_exists_comp", "child2"));
        // Child not found
        Assertions.assertThrows(DdlException.class,
                () -> svm.addChildToCompositeStorageVolume("comp_add_validation", "not_exists_child"));
        // Nested composite is not allowed
        Assertions.assertThrows(DdlException.class,
                () -> svm.addChildToCompositeStorageVolume("comp_add_validation", "comp_add_validation"));
        // Type mismatch with existing children is not allowed
        Assertions.assertThrows(DdlException.class,
                () -> svm.addChildToCompositeStorageVolume("comp_add_validation", "hdfs_child"));
    }

    @Test
    public void testCompositeStorageVolumeRemoveChildValidation() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);
        Map<String, String> params = new HashMap<>();
        params.put(AWS_S3_REGION, "region");
        params.put(AWS_S3_ENDPOINT, "endpoint");
        params.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume("child3", "S3", Arrays.asList("s3://bucket3"), params, Optional.of(true), "");
        svm.createCompositeStorageVolume("comp_remove_validation", Arrays.asList("child1", "child2"), true, "");

        // Composite not found
        Assertions.assertThrows(DdlException.class,
                () -> svm.removeChildFromCompositeStorageVolume("not_exists_comp", "child1"));
        // Child not found
        Assertions.assertThrows(DdlException.class,
                () -> svm.removeChildFromCompositeStorageVolume("comp_remove_validation", "not_exists_child"));
        // Child not in composite
        Assertions.assertThrows(DdlException.class,
                () -> svm.removeChildFromCompositeStorageVolume("comp_remove_validation", "child3"));
    }

    @Test
    public void testRemoveChildFromDefaultCompositeStorageVolume() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);
        svm.createCompositeStorageVolume("comp_default", Arrays.asList("child1", "child2"), true, "");
        svm.setDefaultStorageVolume("comp_default");

        Assertions.assertThrows(DdlException.class,
                () -> svm.removeChildFromCompositeStorageVolume("comp_default", "child2"));
    }

    @Test
    public void testDropCompositeStorageVolumeWithBindings() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);
        svm.createCompositeStorageVolume("comp_drop_bindings", Arrays.asList("child1", "child2"), true, "");

        svm.bindTableToStorageVolume("comp_drop_bindings", 10001L, 10002L);
        Assertions.assertThrows(IllegalStateException.class,
                () -> svm.removeStorageVolume("comp_drop_bindings"));

        svm.unbindTableToStorageVolume(10002L);
        svm.removeStorageVolume("comp_drop_bindings");
        Assertions.assertFalse(svm.exists("comp_drop_bindings"));
    }

    @Test
    public void testGetStorageVolumeCompositeWrapperById() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);
        String csvId = svm.createCompositeStorageVolume("comp_wrapper", Arrays.asList("child1", "child2"), true, "");

        StorageVolume wrapper = svm.getStorageVolume(csvId);
        Assertions.assertNotNull(wrapper);
        Assertions.assertTrue(wrapper.isComposite());
        Assertions.assertEquals("comp_wrapper", wrapper.getName());
    }

    @Test
    public void testCompositeStorageVolumeCRUD() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);

        // CREATE composite SV
        String csvId = svm.createCompositeStorageVolume("comp1",
                Arrays.asList("child1", "child2"), true, "my composite");
        Assertions.assertNotNull(csvId);
        Assertions.assertTrue(svm.exists("comp1"));

        // GET by name
        CompositeStorageVolume csv = svm.getCompositeStorageVolumeByName("comp1");
        Assertions.assertNotNull(csv);
        Assertions.assertEquals("comp1", csv.getName());
        Assertions.assertEquals(2, csv.getChildVolumeIds().size());
        Assertions.assertTrue(csv.isEnabled());
        Assertions.assertEquals("my composite", csv.getComment());

        // GET by id
        CompositeStorageVolume csvById = svm.getCompositeStorageVolume(csvId);
        Assertions.assertNotNull(csvById);
        Assertions.assertEquals(csv.getName(), csvById.getName());
        Assertions.assertEquals(csv.getChildVolumeIds(), csvById.getChildVolumeIds());

        // UPDATE: change comment and disable
        svm.updateStorageVolume("comp1", "", Collections.emptyList(),
                new HashMap<>(), Optional.of(false), "updated comment");
        csv = svm.getCompositeStorageVolumeByName("comp1");
        Assertions.assertFalse(csv.isEnabled());
        Assertions.assertEquals("updated comment", csv.getComment());

        // UPDATE: reject cloud config params on composite SV
        Map<String, String> invalidParams = new HashMap<>();
        invalidParams.put(AWS_S3_REGION, "us-east-1");
        Assertions.assertThrows(DdlException.class,
                () -> svm.updateStorageVolume("comp1", "", Collections.emptyList(),
                        invalidParams, Optional.empty(), ""));

        // REMOVE composite SV
        svm.removeStorageVolume("comp1");
        Assertions.assertFalse(svm.exists("comp1"));
        Assertions.assertNull(svm.getCompositeStorageVolumeByName("comp1"));
    }

    @Test
    public void testCompositeStorageVolumeCreateValidation() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        createChildSVs(svm);

        // Cannot create with empty children
        Assertions.assertThrows(DdlException.class,
                () -> svm.createCompositeStorageVolume("comp_empty",
                        Collections.emptyList(), true, ""));

        // Cannot create with non-existent child
        Assertions.assertThrows(DdlException.class,
                () -> svm.createCompositeStorageVolume("comp_bad",
                        Arrays.asList("child1", "nonexistent"), true, ""));

        // Create a composite, then cannot create another with the same name
        svm.createCompositeStorageVolume("comp1",
                Arrays.asList("child1", "child2"), true, "");
        Assertions.assertThrows(AlreadyExistsException.class,
                () -> svm.createCompositeStorageVolume("comp1",
                        Arrays.asList("child1"), true, ""));

        // Cannot create a regular SV with the same name as an existing composite SV
        Map<String, String> params = new HashMap<>();
        params.put(AWS_S3_REGION, "region");
        params.put(AWS_S3_ENDPOINT, "endpoint");
        params.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        Assertions.assertThrows(AlreadyExistsException.class,
                () -> svm.createStorageVolume("comp1", "S3",
                        Arrays.asList("s3://bucket3"), params, Optional.of(true), ""));
    }

    @Test
    public void testCompositeStorageVolumeAddRemoveChild() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        String[] childKeys = createChildSVs(svm);

        // Create composite with only child1
        String csvId = svm.createCompositeStorageVolume("comp1",
                Arrays.asList("child1"), true, "");

        CompositeStorageVolume csv = svm.getCompositeStorageVolumeByName("comp1");
        Assertions.assertEquals(1, csv.getChildVolumeIds().size());

        // ADD child2
        svm.addChildToCompositeStorageVolume("comp1", "child2");
        csv = svm.getCompositeStorageVolumeByName("comp1");
        Assertions.assertEquals(2, csv.getChildVolumeIds().size());

        // ADD duplicate child — should fail
        Assertions.assertThrows(DdlException.class,
                () -> svm.addChildToCompositeStorageVolume("comp1", "child2"));

        // REMOVE child2 — should succeed (no bound tables)
        svm.removeChildFromCompositeStorageVolume("comp1", "child2");
        csv = svm.getCompositeStorageVolumeByName("comp1");
        Assertions.assertEquals(1, csv.getChildVolumeIds().size());
        Assertions.assertEquals(childKeys[0], csv.getChildVolumeIds().get(0));

        // REMOVE child that is not in composite — should fail
        Assertions.assertThrows(DdlException.class,
                () -> svm.removeChildFromCompositeStorageVolume("comp1", "child2"));

        // Bind a table to the composite SV, then REMOVE should fail
        svm.storageVolumeToTables.computeIfAbsent(csvId, k -> new HashSet<>()).add(100L);
        svm.addChildToCompositeStorageVolume("comp1", "child2");
        Assertions.assertThrows(DdlException.class,
                () -> svm.removeChildFromCompositeStorageVolume("comp1", "child2"));

        // Clean up binding
        svm.storageVolumeToTables.get(csvId).clear();
    }

    @Test
    public void testCheckNotReferencedAsChildSv() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        String[] childKeys = createChildSVs(svm);

        // Before creating composite, dropping child should succeed (no composite references it)
        svm.checkNotReferencedAsChildSv(childKeys[0], "child1");

        // Create composite SV referencing both children
        svm.createCompositeStorageVolume("comp1",
                Arrays.asList("child1", "child2"), true, "");

        // Now dropping child1 should be blocked
        Assertions.assertThrows(DdlException.class,
                () -> svm.checkNotReferencedAsChildSv(childKeys[0], "child1"));

        // Dropping child2 should also be blocked
        Assertions.assertThrows(DdlException.class,
                () -> svm.checkNotReferencedAsChildSv(childKeys[1], "child2"));

        // Remove the composite SV
        svm.removeStorageVolume("comp1");

        // Now dropping children should succeed
        svm.checkNotReferencedAsChildSv(childKeys[0], "child1");
        svm.checkNotReferencedAsChildSv(childKeys[1], "child2");
    }

    @Test
    public void testCompositePartitionRoundRobin() throws Exception {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        String[] childKeys = createChildSVs(svm);

        // Create composite SV with 2 children
        String csvId = svm.createCompositeStorageVolume("comp_rr",
                Arrays.asList("child1", "child2"), true, "round-robin test");

        // Bind table 1000 to the composite SV
        long tableId = 1000L;
        svm.bindTableToStorageVolume("comp_rr", 100L, tableId);

        StorageVolume child1 = svm.getStorageVolumeByName("child1");
        StorageVolume child2 = svm.getStorageVolumeByName("child2");

        // Build distinguishable FilePathInfo for each child SV
        FilePathInfo pathInfoChild1 = FilePathInfo.newBuilder()
                .setFullPath("s3://bucket1/path").build();
        FilePathInfo pathInfoChild2 = FilePathInfo.newBuilder()
                .setFullPath("s3://bucket2/path").build();

        new Expectations() {
            {
                starOSAgent.allocatePartitionFilePathInfoForChildSv(child1.getId(), 100L, tableId, anyLong);
                result = pathInfoChild1;
                minTimes = 0;

                starOSAgent.allocatePartitionFilePathInfoForChildSv(child2.getId(), 100L, tableId, anyLong);
                result = pathInfoChild2;
                minTimes = 0;
            }
        };

        // Verify round-robin: partitionId % 2 determines child selection
        // Even partitionId → child[0] (child1), Odd partitionId → child[1] (child2)
        FilePathInfo result0 = svm.resolveCompositePartitionFilePathInfo(
                createMockOlapTable(tableId), 100L, 0L, 100L);
        Assertions.assertNotNull(result0);
        Assertions.assertEquals("s3://bucket1/path", result0.getFullPath());

        FilePathInfo result1 = svm.resolveCompositePartitionFilePathInfo(
                createMockOlapTable(tableId), 100L, 1L, 101L);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals("s3://bucket2/path", result1.getFullPath());

        FilePathInfo result2 = svm.resolveCompositePartitionFilePathInfo(
                createMockOlapTable(tableId), 100L, 2L, 102L);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals("s3://bucket1/path", result2.getFullPath());

        FilePathInfo result3 = svm.resolveCompositePartitionFilePathInfo(
                createMockOlapTable(tableId), 100L, 3L, 103L);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals("s3://bucket2/path", result3.getFullPath());

        // Verify non-composite table returns null
        long regularTableId = 2000L;
        FilePathInfo resultNonComposite = svm.resolveCompositePartitionFilePathInfo(
                createMockOlapTable(regularTableId), 100L, 0L, 200L);
        Assertions.assertNull(resultNonComposite);
    }

    /**
     * Helper: create a mock OlapTable with the given table ID for testing.
     */
    private com.starrocks.catalog.OlapTable createMockOlapTable(long tableId) {
        com.starrocks.catalog.OlapTable table = new com.starrocks.catalog.OlapTable();
        table.setId(tableId);
        return table;
    }

    @Test
    public void testCompositeStorageVolumeNoLocalCache() throws Exception {
        // Verifies that Composite SV state is read from StarOS (mocked), not from FE memory.
        // After creating a Composite SV with one StorageVolumeMgr instance and creating a
        // fresh instance, the new instance should still find the Composite SV because the
        // StarOS mock (fileStores map) is shared across instances.
        StorageVolumeMgr svm1 = new SharedDataStorageVolumeMgr();
        createChildSVs(svm1);

        String csvId = svm1.createCompositeStorageVolume("comp_persist",
                Arrays.asList("child1", "child2"), true, "persistent");

        // "Simulate FE failover" — create a brand-new StorageVolumeMgr instance.
        // Since composite SV definitions are in StarOS (mocked via static MockUp), the new
        // instance should see the composite SV without any EditLog replay or cache transfer.
        StorageVolumeMgr svm2 = new SharedDataStorageVolumeMgr();

        Assertions.assertTrue(svm2.exists("comp_persist"));
        CompositeStorageVolume csv = svm2.getCompositeStorageVolumeByName("comp_persist");
        Assertions.assertNotNull(csv);
        Assertions.assertEquals(csvId, csv.getId());
        Assertions.assertEquals(2, csv.getChildVolumeIds().size());
        Assertions.assertEquals("persistent", csv.getComment());
    }

}
