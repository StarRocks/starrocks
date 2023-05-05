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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AWSCloudConfiguration;
import com.starrocks.credential.hdfs.HDFSCloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_EXTERNAL_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE;

public class StorageVolumeTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
    }

    @Test
    public void testAWSDefaultCredential() throws AnalysisException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");

        StorageVolume sv = new StorageVolume("test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasDefaultCredential());
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSSimpleCredential() throws AnalysisException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_ACCESS_KEY, "access_key");
        storageParams.put(AWS_S3_SECRET_KEY, "secret_key");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");

        StorageVolume sv = new StorageVolume("test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasSimpleCredential());
        AwsSimpleCredentialInfo simpleCredentialInfo = s3FileStoreInfo.getCredential().getSimpleCredential();
        Assert.assertEquals("access_key", simpleCredentialInfo.getAccessKey());
        Assert.assertEquals("secret_key", simpleCredentialInfo.getAccessKeySecret());
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSInstanceProfile() throws AnalysisException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_INSTANCE_PROFILE, "true");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");

        StorageVolume sv = new StorageVolume("test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasProfileCredential());
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSAssumeIamRole() throws AnalysisException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_INSTANCE_PROFILE, "true");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
        storageParams.put(AWS_S3_IAM_ROLE_ARN, "iam_role_arn");
        storageParams.put(AWS_S3_EXTERNAL_ID, "iam_role_arn");

        StorageVolume sv = new StorageVolume("test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assert.assertTrue(s3FileStoreInfo.getCredential().hasAssumeRoleCredential());
        AwsAssumeIamRoleCredentialInfo assumeIamRoleCredentialInfo = s3FileStoreInfo.getCredential()
                .getAssumeRoleCredential();
        Assert.assertEquals("iam_role_arn", assumeIamRoleCredentialInfo.getIamRoleArn());
        Assert.assertEquals("iam_role_arn", assumeIamRoleCredentialInfo.getIamRoleArn());
        Assert.assertEquals("region", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AWSCloudConfiguration) cloudConfiguration).getAWSCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSInvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_INSTANCE_PROFILE, "true");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        storageParams.put(AWS_S3_IAM_ROLE_ARN, "iam_role_arn");
        storageParams.put(AWS_S3_EXTERNAL_ID, "iam_role_arn");

        try {
            StorageVolume sv = new StorageVolume("test", "s3", Arrays.asList("s3://abc"),
                    storageParams, true, "");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Storage params is not valid"));
        }
    }

    @Test
    public void testHDFSSimpleCredential() throws AnalysisException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("hadoop.security.authentication", "simple");
        storageParams.put("username", "username");
        storageParams.put("password", "password");
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        StorageVolume sv = new StorageVolume("test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("simple", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());

        Map<String, String> storageParams1 = new HashMap<>();
        storageParams1.put("hadoop.security.authentication", "simple");
        storageParams1.put("username", "username");
        storageParams1.put("password", "password");
        sv = new StorageVolume("test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams1, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("simple", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(0, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());
    }

    @Test
    public void testHDFSKerberosCredential() throws AnalysisException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("hadoop.security.authentication", "kerberos");
        storageParams.put("kerberos_principal", "nn/abc@ABC.COM");
        storageParams.put("kerberos_keytab", "/keytab/hive.keytab");
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        StorageVolume sv = new StorageVolume("test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("kerberos", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());
    }

    @Test
    public void testHDFSInvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("hadoop.security.authentication", "kerberos");
        storageParams.put("kerberos_principal", "nn/abc@ABC.COM");

        try {
            StorageVolume sv = new StorageVolume("test", "hdfs", Arrays.asList("hdfs://abc"),
                    storageParams, true, "");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Storage params is not valid"));
        }
    }

    @Test
    public void testCreate() throws DdlException, AnalysisException {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_create type = s3 " +
                            "LOCATIONS = ('s3://xxx') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                            "\"aws.s3.region\"=\"us-west-2\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\", " +
                            "\"enabled\"=\"false\")";
                    CreateStorageVolumeStmt createStmt = (CreateStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(createStmt, connectContext);
                }
        );
        Assert.assertEquals(true, storageVolumeMgr.exists("storage_volume_create"));
        StorageVolume sv = storageVolumeMgr.getStorageVolume("storage_volume_create");
        Assert.assertEquals(false, sv.getEnabled());
        Assert.assertEquals(false, sv.isDefault());

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "SHOW STORAGE VOLUMES like 'storage_volume_create'";
                    ShowStorageVolumesStmt showStmt = (ShowStorageVolumesStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    ShowExecutor executor = new ShowExecutor(connectContext, showStmt);
                    ShowResultSet resultSet = executor.execute();
                    Set<String> allStorageVolumes = resultSet.getResultRows().stream().map(k -> k.get(0))
                            .collect(Collectors.toSet());
                    Assert.assertEquals(new HashSet<>(Arrays.asList("storage_volume_create")), allStorageVolumes);
                }
        );

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "DROP STORAGE VOLUME IF EXISTS storage_volume_create";
                    DropStorageVolumeStmt dropStmt = (DropStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(dropStmt, connectContext);
                }
        );
        Assert.assertEquals(false, storageVolumeMgr.exists("storage_volume_create"));
    }

    @Test
    public void testAlter() throws DdlException, AnalysisException {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_alter type = s3 " +
                            "LOCATIONS = ('s3://xxx') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                            "\"aws.s3.region\"=\"us-west-2\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\", " +
                            "\"enabled\"=\"false\")";
                    CreateStorageVolumeStmt createStmt = (CreateStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(createStmt, connectContext);
                }
        );

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "ALTER STORAGE VOLUME storage_volume_alter SET (\"aws.s3.region\"=\"us-west-1\", " +
                            "\"aws.s3.endpoint\"=\"endpoint1\", \"enabled\"=\"true\" )";
                    AlterStorageVolumeStmt alterStmt = (AlterStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(alterStmt, connectContext);
                }
        );
        StorageVolume sv = storageVolumeMgr.getStorageVolume("storage_volume_alter");
        Assert.assertEquals(true, sv.getEnabled());

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "DESC STORAGE VOLUME storage_volume_alter";
                    DescStorageVolumeStmt descStmt = (DescStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    ShowExecutor executor = new ShowExecutor(connectContext, descStmt);
                    ShowResultSet resultSet = executor.execute();
                    Assert.assertEquals(1, resultSet.getResultRows().size());
                    Assert.assertEquals("storage_volume_alter", resultSet.getResultRows().get(0).get(0));
                    Assert.assertEquals("S3", resultSet.getResultRows().get(0).get(1));
                    Assert.assertEquals("false", resultSet.getResultRows().get(0).get(2));
                    Assert.assertEquals("s3://xxx", resultSet.getResultRows().get(0).get(3));
                    Assert.assertEquals("true", resultSet.getResultRows().get(0).get(5));
                    Assert.assertEquals("comment", resultSet.getResultRows().get(0).get(6));
                }
        );

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "DROP STORAGE VOLUME IF EXISTS storage_volume_alter";
                    DropStorageVolumeStmt dropStmt = (DropStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(dropStmt, connectContext);
                }
        );
        Assert.assertEquals(false, storageVolumeMgr.exists("storage_volume_alter"));
    }

    @Test
    public void testSetDefault() {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_1 type = s3 " +
                            "LOCATIONS = ('s3://xxx') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                            "\"aws.s3.region\"=\"us-west-2\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\", " +
                            "\"enabled\"=\"false\")";
                    CreateStorageVolumeStmt createStmt = (CreateStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(createStmt, connectContext);
                }
        );
        Assert.assertEquals(true, storageVolumeMgr.exists("storage_volume_1"));

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "CREATE STORAGE VOLUME IF NOT EXISTS storage_volume_2 type = s3 " +
                            "LOCATIONS = ('s3://xxx') COMMENT 'comment' PROPERTIES (\"aws.s3.endpoint\"=\"endpoint\", " +
                            "\"aws.s3.region\"=\"us-west-2\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\", " +
                            "\"enabled\"=\"false\")";
                    CreateStorageVolumeStmt createStmt = (CreateStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(createStmt, connectContext);
                }
        );
        Assert.assertEquals(true, storageVolumeMgr.exists("storage_volume_2"));

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "SET storage_volume_1 AS DEFAULT STORAGE VOLUME";
                    SetDefaultStorageVolumeStmt setStmt = (SetDefaultStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(setStmt, connectContext);
                }
        );
        Assert.assertEquals("storage_volume_1", storageVolumeMgr.getDefaultSV());

        ExceptionChecker.expectThrows(
                IllegalStateException.class,
                () -> {
                    String sql = "DROP STORAGE VOLUME IF EXISTS storage_volume_1";
                    DropStorageVolumeStmt dropStmt = (DropStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(dropStmt, connectContext);
                }
        );
        Assert.assertEquals(true, storageVolumeMgr.exists("storage_volume_1"));

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "SHOW STORAGE VOLUMES";
                    ShowStorageVolumesStmt showStmt = (ShowStorageVolumesStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    ShowExecutor executor = new ShowExecutor(connectContext, showStmt);
                    ShowResultSet resultSet = executor.execute();
                    Set<String> allStorageVolumes = resultSet.getResultRows().stream().map(k -> k.get(0))
                            .collect(Collectors.toSet());
                    Assert.assertEquals(new HashSet<>(Arrays.asList("storage_volume_1", "storage_volume_2")),
                            allStorageVolumes);
                    sql = "SHOW STORAGE VOLUMES like 'storage_volume_1'";
                    showStmt = (ShowStorageVolumesStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    executor = new ShowExecutor(connectContext, showStmt);
                    resultSet = executor.execute();
                    allStorageVolumes = resultSet.getResultRows().stream().map(k -> k.get(0))
                            .collect(Collectors.toSet());
                    Assert.assertEquals(new HashSet<>(Arrays.asList("storage_volume_1")),
                            allStorageVolumes);
                }
        );

        ExceptionChecker.expectThrowsNoException(
                () -> {
                    String sql = "SET storage_volume_2 AS DEFAULT STORAGE VOLUME";
                    SetDefaultStorageVolumeStmt setStmt = (SetDefaultStorageVolumeStmt) UtFrameUtils.
                            parseStmtWithNewParser(sql, connectContext);
                    DDLStmtExecutor.execute(setStmt, connectContext);
                }
        );
        Assert.assertEquals("storage_volume_2", storageVolumeMgr.getDefaultSV());
    }
}
