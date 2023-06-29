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
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AwsInstanceProfileCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AWSCloudConfiguration;
import com.starrocks.credential.hdfs.HDFSCloudConfiguration;
import com.starrocks.credential.hdfs.HDFSCloudCredential;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
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
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY;

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

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
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

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
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

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
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

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
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
        storageParams.put(AWS_S3_IAM_ROLE_ARN, "iam_role_arn");
        storageParams.put(AWS_S3_EXTERNAL_ID, "iam_role_arn");

        Assert.assertThrows(SemanticException.class, () ->
                new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"), storageParams, true, "")
        );
    }

    @Test
    public void testHDFSSimpleCredential() {
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

        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
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
        sv = new StorageVolume("2", "test", "hdfs", Arrays.asList("hdfs://abc"),
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

        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals("kerberos", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assert.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHaConfigurations().size());
    }

    @Test
    public void testHDFSEmptyCredential() {
        Map<String, String> storageParams = new HashMap<>();
        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assert.assertEquals(HDFSCloudCredential.EMPTY,
                hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assert.assertTrue(fileStore.hasHdfsFsInfo());
    }

    @Test
    public void testHDFSInvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("hadoop.security.authentication", "kerberos");
        storageParams.put("kerberos_principal", "nn/abc@ABC.COM");

        Assert.assertThrows(SemanticException.class, () ->
                new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"), storageParams, true, ""));
    }

    @Test
    public void testAzureSharedKeyCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AZURE_BLOB_ENDPOINT, "endpoint");
        storageParams.put(AZURE_BLOB_SHARED_KEY, "shared_key");
        StorageVolume sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertTrue(fileStore.hasAzblobFsInfo());
        AzBlobFileStoreInfo azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assert.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assert.assertEquals("shared_key", azBlobFileStoreInfo.getCredential().getSharedKey());

        sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa/bbb"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertTrue(fileStore.hasAzblobFsInfo());
        azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assert.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assert.assertEquals("shared_key", azBlobFileStoreInfo.getCredential().getSharedKey());
    }

    @Test
    public void testAzureSasTokenCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AZURE_BLOB_ENDPOINT, "endpoint");
        storageParams.put(AZURE_BLOB_SAS_TOKEN, "sas_token");
        StorageVolume sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertTrue(fileStore.hasAzblobFsInfo());
        AzBlobFileStoreInfo azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assert.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assert.assertEquals("sas_token", azBlobFileStoreInfo.getCredential().getSasToken());

        sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa/bbb"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assert.assertTrue(fileStore.hasAzblobFsInfo());
        azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assert.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assert.assertEquals("sas_token", azBlobFileStoreInfo.getCredential().getSasToken());
    }

    @Test
    public void testAzureInvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        Assert.assertThrows(SemanticException.class, () ->
                new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa"), storageParams, true, ""));
    }

    @Test
    public void testFromFileStoreInfo() {
        AwsSimpleCredentialInfo simpleCredentialInfo = AwsSimpleCredentialInfo.newBuilder()
                .setAccessKey("ak").setAccessKeySecret("sk").build();
        AwsCredentialInfo credentialInfo = AwsCredentialInfo.newBuilder().setSimpleCredential(simpleCredentialInfo).build();
        S3FileStoreInfo s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        FileStoreInfo fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        StorageVolume sv = StorageVolume.fromFileStoreInfo(fs);
        Assert.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());

        AwsAssumeIamRoleCredentialInfo assumeIamRoleCredentialInfo = AwsAssumeIamRoleCredentialInfo.newBuilder()
                .setIamRoleArn("role-Arn").setExternalId("externId").build();
        credentialInfo = AwsCredentialInfo.newBuilder().setAssumeRoleCredential(assumeIamRoleCredentialInfo).build();
        s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assert.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());

        AwsDefaultCredentialInfo defaultCredentialInfo = AwsDefaultCredentialInfo.newBuilder().build();
        credentialInfo = AwsCredentialInfo.newBuilder().setDefaultCredential(defaultCredentialInfo).build();
        s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assert.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());

        AwsInstanceProfileCredentialInfo instanceProfileCredentialInfo = AwsInstanceProfileCredentialInfo.newBuilder().build();
        credentialInfo = AwsCredentialInfo.newBuilder().setProfileCredential(instanceProfileCredentialInfo).build();
        s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assert.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());
    }

    @Test
    public void testSerializationAndDeserialization() throws IOException, DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");

        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            sv.write(out);
            out.flush();
        }

        StorageVolume sv1 = null;
        try (DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream())) {
            sv1 = StorageVolume.read(in);
        }
        byteArrayOutputStream.close();
        Assert.assertEquals(sv.getId(), sv1.getId());
        Assert.assertEquals(sv.getComment(), sv1.getComment());
        Assert.assertEquals(sv.getName(), sv1.getName());
        Assert.assertEquals(sv.getEnabled(), sv1.getEnabled());
        Assert.assertEquals(CloudType.AWS, sv1.getCloudConfiguration().getCloudType());
    }
}
