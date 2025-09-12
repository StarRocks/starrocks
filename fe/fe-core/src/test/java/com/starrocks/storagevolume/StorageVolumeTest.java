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

import com.staros.proto.ADLS2FileStoreInfo;
import com.staros.proto.AwsAssumeIamRoleCredentialInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AwsInstanceProfileCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.hdfs.HDFSCloudConfiguration;
import com.starrocks.credential.hdfs.HDFSCloudCredential;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.MockedFrontend;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_EXTERNAL_ID;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_BLOB_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.GCP_GCS_IMPERSONATION_SERVICE_ACCOUNT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_AUTHENTICATION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_DEPRECATED;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_KERBEROS_PRINCIPAL_DEPRECATED;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_KERBEROS_TICKET_CACHE_PATH;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_PASSWORD;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.HDFS_USERNAME;

public class StorageVolumeTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
    }

    @Test
    public void testAWSDefaultCredential() throws AnalysisException, DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assertions.assertTrue(s3FileStoreInfo.getCredential().hasDefaultCredential());
        Assertions.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSSimpleCredential() throws AnalysisException, DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_ACCESS_KEY, "access_key");
        storageParams.put(AWS_S3_SECRET_KEY, "secret_key");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
        storageParams.put(AWS_S3_ENABLE_PATH_STYLE_ACCESS, "true");

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assertions.assertTrue(s3FileStoreInfo.getCredential().hasSimpleCredential());
        Assertions.assertEquals(s3FileStoreInfo.getPathStyleAccess(), 1);
        AwsSimpleCredentialInfo simpleCredentialInfo = s3FileStoreInfo.getCredential().getSimpleCredential();
        Assertions.assertEquals("access_key", simpleCredentialInfo.getAccessKey());
        Assertions.assertEquals("secret_key", simpleCredentialInfo.getAccessKeySecret());
        Assertions.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSInstanceProfile() throws AnalysisException, DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_INSTANCE_PROFILE, "true");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");

        StorageVolume sv = new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assertions.assertTrue(s3FileStoreInfo.getCredential().hasProfileCredential());
        Assertions.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSAssumeIamRole() throws AnalysisException, DdlException {
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
        Assertions.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.S3, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasS3FsInfo());
        S3FileStoreInfo s3FileStoreInfo = fileStore.getS3FsInfo();
        Assertions.assertTrue(s3FileStoreInfo.getCredential().hasAssumeRoleCredential());
        AwsAssumeIamRoleCredentialInfo assumeIamRoleCredentialInfo = s3FileStoreInfo.getCredential()
                .getAssumeRoleCredential();
        Assertions.assertEquals("iam_role_arn", assumeIamRoleCredentialInfo.getIamRoleArn());
        Assertions.assertEquals("iam_role_arn", assumeIamRoleCredentialInfo.getIamRoleArn());
        Assertions.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assertions.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
    }

    @Test
    public void testAWSInvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_IAM_ROLE_ARN, "iam_role_arn");
        storageParams.put(AWS_S3_EXTERNAL_ID, "iam_role_arn");

        Assertions.assertThrows(SemanticException.class, () ->
                new StorageVolume("1", "test", "s3", Arrays.asList("s3://abc"), storageParams, true, "")
        );
    }

    @Test
    public void testHDFSSimpleCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(HDFS_AUTHENTICATION, HDFSCloudCredential.SIMPLE_AUTH);
        storageParams.put(HDFS_USERNAME, "username");
        storageParams.put(HDFS_PASSWORD, "password");
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assertions.assertEquals(HDFSCloudCredential.SIMPLE_AUTH,
                hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assertions.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHadoopConfiguration().size());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());
        HDFSFileStoreInfo hdfsFileStoreInfo = fileStore.getHdfsFsInfo();
        Assertions.assertEquals("username", hdfsFileStoreInfo.getUsername());
        Assertions.assertEquals("simple", hdfsFileStoreInfo.getConfigurationMap().get(HDFS_AUTHENTICATION));
        Assertions.assertEquals("ha_cluster", hdfsFileStoreInfo.getConfigurationMap().get("dfs.nameservices"));
        Assertions.assertEquals("ha_n1,ha_n2", hdfsFileStoreInfo.getConfigurationMap().get("dfs.ha.namenodes.ha_cluster"));
        Assertions.assertEquals("<hdfs_host>:<hdfs_port>",
                hdfsFileStoreInfo.getConfiguration().get("dfs.namenode.rpc-address.ha_cluster.ha_n1"));
        Assertions.assertEquals("<hdfs_host>:<hdfs_port>",
                hdfsFileStoreInfo.getConfiguration().get("dfs.namenode.rpc-address.ha_cluster.ha_n2"));
        Assertions.assertEquals("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                hdfsFileStoreInfo.getConfiguration().get("dfs.client.failover.proxy.provider"));

        Map<String, String> storageParams1 = new HashMap<>();
        storageParams1.put(HDFS_AUTHENTICATION, HDFSCloudCredential.SIMPLE_AUTH);
        storageParams1.put(HDFS_USERNAME, "username");
        storageParams1.put(HDFS_PASSWORD, "password");
        sv = new StorageVolume("2", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams1, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assertions.assertEquals(HDFSCloudCredential.SIMPLE_AUTH,
                hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assertions.assertEquals(0, hdfsCloudConfiguration.getHdfsCloudCredential().getHadoopConfiguration().size());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());
        hdfsFileStoreInfo = fileStore.getHdfsFsInfo();
        Assertions.assertEquals("username", hdfsFileStoreInfo.getUsername());
    }

    @Test
    public void testHDFSKerberosCredential() throws AnalysisException, DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(HDFS_AUTHENTICATION, HDFSCloudCredential.KERBEROS_AUTH);
        storageParams.put(HDFS_KERBEROS_PRINCIPAL_DEPRECATED, "nn/abc@ABC.COM");
        storageParams.put(HDFS_KERBEROS_KEYTAB_DEPRECATED, "/keytab/hive.keytab");
        storageParams.put(HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED, "YWFhYWFh");
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assertions.assertEquals("kerberos", hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assertions.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHadoopConfiguration().size());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());
        HDFSFileStoreInfo hdfsFileStoreInfo = fileStore.getHdfsFsInfo();
        Assertions.assertEquals(HDFSCloudCredential.KERBEROS_AUTH,
                hdfsFileStoreInfo.getConfigurationMap().get(HDFS_AUTHENTICATION));
        Assertions.assertEquals(5, hdfsCloudConfiguration.getHdfsCloudCredential().getHadoopConfiguration().size());
        Assertions.assertEquals("ha_cluster", hdfsFileStoreInfo.getConfigurationMap().get("dfs.nameservices"));
        Assertions.assertEquals("ha_n1,ha_n2", hdfsFileStoreInfo.getConfigurationMap().get("dfs.ha.namenodes.ha_cluster"));
        Assertions.assertEquals("<hdfs_host>:<hdfs_port>",
                hdfsFileStoreInfo.getConfigurationMap().get("dfs.namenode.rpc-address.ha_cluster.ha_n1"));
        Assertions.assertEquals("<hdfs_host>:<hdfs_port>",
                hdfsFileStoreInfo.getConfigurationMap().get("dfs.namenode.rpc-address.ha_cluster.ha_n2"));
        Assertions.assertEquals("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                hdfsFileStoreInfo.getConfigurationMap().get("dfs.client.failover.proxy.provider"));

        storageParams.clear();
        storageParams.put(HDFS_AUTHENTICATION, HDFSCloudCredential.KERBEROS_AUTH);
        storageParams.put(HDFS_KERBEROS_TICKET_CACHE_PATH, "/path/to/ticket/cache/path");
        sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        Assertions.assertEquals(HDFSCloudCredential.KERBEROS_AUTH,
                hdfsCloudConfiguration.getHdfsCloudCredential().getAuthentication());
        Assertions.assertEquals(1, hdfsCloudConfiguration.getHdfsCloudCredential().getHadoopConfiguration().size());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());
        hdfsFileStoreInfo = fileStore.getHdfsFsInfo();
        Assertions.assertEquals("kerberos", hdfsFileStoreInfo.getConfigurationMap().get(HDFS_AUTHENTICATION));
        Assertions.assertEquals("/path/to/ticket/cache/path",
                hdfsFileStoreInfo.getConfigurationMap().get(HDFS_KERBEROS_TICKET_CACHE_PATH));
    }

    @Test
    public void testHDFSEmptyCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());
    }

    @Test
    public void testHDFSViewFS() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("fs.viewfs.mounttable.ClusterX.link./data", "hdfs://nn1-clusterx.example.com:8020/data");
        storageParams.put("fs.viewfs.mounttable.ClusterX.link./project", "hdfs://nn2-clusterx.example.com:8020/project");
        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());
        HDFSFileStoreInfo hdfsFileStoreInfo = fileStore.getHdfsFsInfo();
        Assertions.assertEquals("hdfs://nn1-clusterx.example.com:8020/data",
                hdfsFileStoreInfo.getConfigurationMap().get("fs.viewfs.mounttable.ClusterX.link./data"));
        Assertions.assertEquals("hdfs://nn2-clusterx.example.com:8020/project",
                hdfsFileStoreInfo.getConfigurationMap().get("fs.viewfs.mounttable.ClusterX.link./project"));
    }

    @Test
    public void testHDFSAddConfigResources() throws DdlException {
        String runningDir = MockedFrontend.getInstance().getRunningDir();
        String confFile = runningDir + "/conf/hdfs-site.xml";
        String content = "<configuration>\n" +
                "   <property>\n" +
                "      <name>XXX</name>\n" +
                "      <value>YYY</value>\n" +
                "   </property>\n" +
                "   </configuration>";
        Path path = Paths.get(confFile);
        try {
            Files.write(path, content.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(HadoopExt.HADOOP_CONFIG_RESOURCES, confFile);
        StorageVolume sv = new StorageVolume("1", "test", "hdfs", Arrays.asList("hdfs://abc"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.HDFS, cloudConfiguration.getCloudType());
        HDFSCloudConfiguration hdfsCloudConfiguration = (HDFSCloudConfiguration) cloudConfiguration;
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.HDFS, fileStore.getFsType());
        Assertions.assertTrue(fileStore.hasHdfsFsInfo());

        Configuration conf = new Configuration();
        hdfsCloudConfiguration.applyToConfiguration(conf);
        Assertions.assertEquals(conf.get("XXX"), null);
    }

    @Test
    public void testAzureBlobSharedKeyCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AZURE_BLOB_ENDPOINT, "endpoint");
        storageParams.put(AZURE_BLOB_SHARED_KEY, "shared_key");
        StorageVolume sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAzblobFsInfo());
        AzBlobFileStoreInfo azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assertions.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assertions.assertEquals("shared_key", azBlobFileStoreInfo.getCredential().getSharedKey());

        sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa/bbb"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAzblobFsInfo());
        azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assertions.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assertions.assertEquals("shared_key", azBlobFileStoreInfo.getCredential().getSharedKey());
    }

    @Test
    public void testAzureBlobSasTokenCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AZURE_BLOB_ENDPOINT, "endpoint");
        storageParams.put(AZURE_BLOB_SAS_TOKEN, "sas_token");
        StorageVolume sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAzblobFsInfo());
        AzBlobFileStoreInfo azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assertions.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assertions.assertEquals("sas_token", azBlobFileStoreInfo.getCredential().getSasToken());

        sv = new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa/bbb"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAzblobFsInfo());
        azBlobFileStoreInfo = fileStore.getAzblobFsInfo();
        Assertions.assertEquals("endpoint", azBlobFileStoreInfo.getEndpoint());
        Assertions.assertEquals("sas_token", azBlobFileStoreInfo.getCredential().getSasToken());
    }

    @Test
    public void testAzureBlobInvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        Assertions.assertThrows(SemanticException.class, () ->
                new StorageVolume("1", "test", "azblob", Arrays.asList("azblob://aaa"), storageParams, true, ""));
    }

    @Test
    public void testAzureADLS2SharedKeyCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AZURE_ADLS2_ENDPOINT, "endpoint");
        storageParams.put(AZURE_ADLS2_SHARED_KEY, "shared_key");
        StorageVolume sv = new StorageVolume("1", "test", "adls2", Arrays.asList("adls2://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAdls2FsInfo());
        ADLS2FileStoreInfo adls2FileStoreInfo = fileStore.getAdls2FsInfo();
        Assertions.assertEquals("endpoint", adls2FileStoreInfo.getEndpoint());
        Assertions.assertEquals("shared_key", adls2FileStoreInfo.getCredential().getSharedKey());

        sv = new StorageVolume("1", "test", "adls2", Arrays.asList("adls2://aaa/bbb"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAdls2FsInfo());
        adls2FileStoreInfo = fileStore.getAdls2FsInfo();
        Assertions.assertEquals("endpoint", adls2FileStoreInfo.getEndpoint());
        Assertions.assertEquals("shared_key", adls2FileStoreInfo.getCredential().getSharedKey());
    }

    @Test
    public void testAzureADLS2SasTokenCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AZURE_ADLS2_ENDPOINT, "endpoint");
        storageParams.put(AZURE_ADLS2_SAS_TOKEN, "sas_token");
        StorageVolume sv = new StorageVolume("1", "test", "adls2", Arrays.asList("adls2://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAdls2FsInfo());
        ADLS2FileStoreInfo adls2FileStoreInfo = fileStore.getAdls2FsInfo();
        Assertions.assertEquals("endpoint", adls2FileStoreInfo.getEndpoint());
        Assertions.assertEquals("sas_token", adls2FileStoreInfo.getCredential().getSasToken());

        sv = new StorageVolume("1", "test", "adls2", Arrays.asList("adls2://aaa/bbb"),
                storageParams, true, "");
        cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasAdls2FsInfo());
        adls2FileStoreInfo = fileStore.getAdls2FsInfo();
        Assertions.assertEquals("endpoint", adls2FileStoreInfo.getEndpoint());
        Assertions.assertEquals("sas_token", adls2FileStoreInfo.getCredential().getSasToken());
    }

    @Test
    public void testGCPGSCredential() throws DdlException {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, "false");
        storageParams.put(GCP_GCS_SERVICE_ACCOUNT_EMAIL, "demo@demo.com");
        storageParams.put(GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID, "xxxxxxxxxxxx");
        storageParams.put(GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY, "-------xxxx------");
        storageParams.put(GCP_GCS_IMPERSONATION_SERVICE_ACCOUNT, "iuser@demo.com");
        StorageVolume sv = new StorageVolume("1", "test", "gs", Arrays.asList("gs://aaa"),
                storageParams, true, "");
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assertions.assertEquals(CloudType.GCP, cloudConfiguration.getCloudType());
        FileStoreInfo fileStore = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStore.hasGsFsInfo());
        Assertions.assertEquals("iuser@demo.com", fileStore.getGsFsInfo().getImpersonation());
        Assertions.assertEquals("demo@demo.com", fileStore.getGsFsInfo().getServiceAccountEmail());
        Assertions.assertEquals("-------xxxx------", fileStore.getGsFsInfo().getServiceAccountPrivateKey());
    }

    @Test
    public void testAzureADLS2InvalidCredential() {
        Map<String, String> storageParams = new HashMap<>();
        Assertions.assertThrows(SemanticException.class, () ->
                new StorageVolume("1", "test", "adls2", Arrays.asList("adls2://aaa"), storageParams, true, ""));
    }

    @Test
    public void testFromFileStoreInfo() throws DdlException {
        AwsSimpleCredentialInfo simpleCredentialInfo = AwsSimpleCredentialInfo.newBuilder()
                .setAccessKey("ak").setAccessKeySecret("sk").build();
        AwsCredentialInfo credentialInfo = AwsCredentialInfo.newBuilder().setSimpleCredential(simpleCredentialInfo).build();
        S3FileStoreInfo s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        FileStoreInfo fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        StorageVolume sv = StorageVolume.fromFileStoreInfo(fs);
        Assertions.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());

        AwsAssumeIamRoleCredentialInfo assumeIamRoleCredentialInfo = AwsAssumeIamRoleCredentialInfo.newBuilder()
                .setIamRoleArn("role-Arn").setExternalId("externId").build();
        credentialInfo = AwsCredentialInfo.newBuilder().setAssumeRoleCredential(assumeIamRoleCredentialInfo).build();
        s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assertions.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());
        AwsDefaultCredentialInfo defaultCredentialInfo = AwsDefaultCredentialInfo.newBuilder().build();
        credentialInfo = AwsCredentialInfo.newBuilder().setDefaultCredential(defaultCredentialInfo).build();
        s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assertions.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());

        AwsInstanceProfileCredentialInfo instanceProfileCredentialInfo = AwsInstanceProfileCredentialInfo.newBuilder().build();
        credentialInfo = AwsCredentialInfo.newBuilder().setProfileCredential(instanceProfileCredentialInfo).build();
        s3fs = S3FileStoreInfo.newBuilder().setBucket("/bucket")
                .setEndpoint("endpoint").setRegion("region").setCredential(credentialInfo).build();
        fs = FileStoreInfo.newBuilder().setS3FsInfo(s3fs).setFsKey("0").setFsType(FileStoreType.S3).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assertions.assertEquals(CloudType.AWS, sv.getCloudConfiguration().getCloudType());

        HDFSFileStoreInfo hdfs = HDFSFileStoreInfo.newBuilder().setUsername("username")
                .putConfiguration(HDFS_AUTHENTICATION, "simple")
                .putConfiguration("dfs.nameservices", "ha_cluster")
                .putConfiguration("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2")
                .putConfiguration("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>")
                .putConfiguration("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>")
                .putConfiguration("dfs.client.failover.proxy.provider",
                        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider").build();
        fs = FileStoreInfo.newBuilder().setHdfsFsInfo(hdfs).setFsKey("0").setFsType(FileStoreType.HDFS).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assertions.assertEquals(CloudType.HDFS, sv.getCloudConfiguration().getCloudType());

        hdfs = HDFSFileStoreInfo.newBuilder().putConfiguration(HDFS_AUTHENTICATION, "kerberos")
                .putConfiguration(HDFS_KERBEROS_PRINCIPAL_DEPRECATED, "nn/abc@ABC.COM")
                .putConfiguration(HDFS_KERBEROS_KEYTAB_DEPRECATED, "/keytab/hive.keytab")
                .putConfiguration(HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED, "YWFhYWFh").build();
        fs = FileStoreInfo.newBuilder().setHdfsFsInfo(hdfs).setFsKey("0").setFsType(FileStoreType.HDFS).build();
        sv = StorageVolume.fromFileStoreInfo(fs);
        Assertions.assertEquals(CloudType.HDFS, sv.getCloudConfiguration().getCloudType());
    }

    @Test
    public void testGetParamsFromFileStoreInfo() {
        AwsCredentialInfo.Builder awsCredBuilder = AwsCredentialInfo.newBuilder();
        awsCredBuilder.getSimpleCredentialBuilder()
                .setAccessKey("ak")
                .setAccessKeySecret("sk")
                .build();

        FileStoreInfo.Builder fsInfoBuilder = FileStoreInfo.newBuilder();
        fsInfoBuilder.getS3FsInfoBuilder()
                .setBucket("bucket")
                .setEndpoint("endpoint")
                .setRegion("region")
                .setCredential(awsCredBuilder);

        fsInfoBuilder.setFsKey("0")
                .setFsType(FileStoreType.S3)
                .addLocations("s3://bucket");

        {
            FileStoreInfo fs = fsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
        }

        fsInfoBuilder.getS3FsInfoBuilder()
                .setPartitionedPrefixEnabled(true)
                .setNumPartitionedPrefix(32);

        {
            FileStoreInfo fs = fsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertTrue(params.containsKey(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX));
            Assertions.assertTrue(params.containsKey(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
            Assertions.assertEquals("32", params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
        }

        fsInfoBuilder.getS3FsInfoBuilder()
                .setPathStyleAccess(1);

        {
            FileStoreInfo fs = fsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("true", params.get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS));
        }

        fsInfoBuilder.getS3FsInfoBuilder()
                .setPathStyleAccess(2);

        {
            FileStoreInfo fs = fsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("false", params.get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS));
        }

        // It's OK to have trailing '/' after bucket name
        fsInfoBuilder.addLocations("s3://bucket/");
        {
            FileStoreInfo fs = fsInfoBuilder.build();
            ExceptionChecker.expectThrowsNoException(() -> StorageVolume.fromFileStoreInfo(fs));
        }

        // can't have more after bucket name
        fsInfoBuilder.addLocations("s3://bucket/abc");
        {
            FileStoreInfo fs = fsInfoBuilder.build();
            Assertions.assertThrows(DdlException.class, () -> StorageVolume.fromFileStoreInfo(fs));
        }

        // Test GS case
        FileStoreInfo.Builder gsFsInfoBuilder = FileStoreInfo.newBuilder();
        gsFsInfoBuilder.setFsKey("1")
                .setFsType(FileStoreType.GS)
                .addLocations("gs://gs_bucket");

        // Case 1: Use Compute Engine Service Account
        gsFsInfoBuilder.getGsFsInfoBuilder()
                .setEndpoint("http://gs_endpointnt_1")
                .setUseComputeEngineServiceAccount(true);
        {
            FileStoreInfo fs = gsFsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("http://gs_endpointnt_1", params.get(CloudConfigurationConstants.GCP_GCS_ENDPOINT));
            Assertions.assertEquals("true", params.get(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY));
        }

        // Case 2: Don't use Compute Engine Service Account (use service account credentials)
        gsFsInfoBuilder.getGsFsInfoBuilder()
                .setEndpoint("http://gs_endpointnt_2")
                .setUseComputeEngineServiceAccount(false)
                .setServiceAccountEmail("test_email@example.com")
                .setServiceAccountPrivateKeyId("test_key_id")
                .setServiceAccountPrivateKey("test_private_key");
        {
            FileStoreInfo fs = gsFsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("http://gs_endpointnt_2", params.get(CloudConfigurationConstants.GCP_GCS_ENDPOINT));
            Assertions.assertEquals("false", params.get(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT));
            Assertions.assertEquals("test_email@example.com",
                    params.get(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL));
            Assertions.assertEquals("test_key_id",
                    params.get(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID));
            Assertions.assertEquals("test_private_key",
                    params.get(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY));
        }

        // Case 3: With Impersonation (overrides useComputeEngineServiceAccount setting for the specific key)
        gsFsInfoBuilder.getGsFsInfoBuilder()
                .setEndpoint("http://gs_endpointnt_3")
                .setUseComputeEngineServiceAccount(true) // This will be overridden by impersonation for the key
                .setImpersonation("impersonation_account@example.com");
        {
            FileStoreInfo fs = gsFsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("http://gs_endpointnt_3", params.get(CloudConfigurationConstants.GCP_GCS_ENDPOINT));
            // The GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT key is set to the impersonation string
            Assertions.assertEquals("impersonation_account@example.com",
                    params.get(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT));
            // Other service account keys should not be present if impersonation is used,
            // even if useComputeEngineServiceAccount was false initially.
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY));
        }

        // Case 4: Don't use Compute Engine Service Account AND with Impersonation
        gsFsInfoBuilder.getGsFsInfoBuilder()
                .setEndpoint("http://gs_endpointnt_4")
                .setUseComputeEngineServiceAccount(false)
                .setServiceAccountEmail("original_email@example.com") // These should be ignored
                .setServiceAccountPrivateKeyId("original_key_id")
                .setServiceAccountPrivateKey("original_private_key")
                .setImpersonation("another_impersonation@example.com");
        {
            FileStoreInfo fs = gsFsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("http://gs_endpointnt_4", params.get(CloudConfigurationConstants.GCP_GCS_ENDPOINT));
            // The GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT key is set to the impersonation string
            Assertions.assertEquals("another_impersonation@example.com",
                    params.get(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT));
            // Specific service account keys should NOT be present because impersonation takes precedence.
            Assertions.assertTrue(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL));
            Assertions.assertTrue(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID));
            Assertions.assertTrue(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY));
        }

        // Case 5: Impersonation is empty string (should behave like no impersonation)
        // Resetting impersonation
        gsFsInfoBuilder.getGsFsInfoBuilder().clearImpersonation();
        gsFsInfoBuilder.getGsFsInfoBuilder()
                .setEndpoint("http://gs_endpointnt_5")
                .setUseComputeEngineServiceAccount(true)
                .setImpersonation(""); // Empty string
        {
            FileStoreInfo fs = gsFsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("http://gs_endpointnt_5", params.get(CloudConfigurationConstants.GCP_GCS_ENDPOINT));
            Assertions.assertEquals("true", params.get(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT));
            Assertions.assertFalse(params.containsKey(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL));
        }

        // Case 6: Impersonation is empty string and useComputeEngineServiceAccount is false
        gsFsInfoBuilder.getGsFsInfoBuilder().clearImpersonation();
        gsFsInfoBuilder.getGsFsInfoBuilder()
                .setEndpoint("http://gs_endpointnt_6")
                .setUseComputeEngineServiceAccount(false)
                .setServiceAccountEmail("final_email@example.com")
                .setServiceAccountPrivateKeyId("final_key_id")
                .setServiceAccountPrivateKey("final_private_key")
                .setImpersonation(""); // Empty string
        {
            FileStoreInfo fs = gsFsInfoBuilder.build();
            Map<String, String> params = StorageVolume.getParamsFromFileStoreInfo(fs);
            Assertions.assertEquals("http://gs_endpointnt_6", params.get(CloudConfigurationConstants.GCP_GCS_ENDPOINT));
            Assertions.assertEquals("false", params.get(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT));
            Assertions.assertEquals("final_email@example.com",
                    params.get(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL));
            Assertions.assertEquals("final_key_id",
                    params.get(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID));
            Assertions.assertEquals("final_private_key",
                    params.get(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY));
        }
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
        Assertions.assertEquals(sv.getId(), sv1.getId());
        Assertions.assertEquals(sv.getComment(), sv1.getComment());
        Assertions.assertEquals(sv.getName(), sv1.getName());
        Assertions.assertEquals(sv.getEnabled(), sv1.getEnabled());
        Assertions.assertEquals(CloudType.AWS, sv1.getCloudConfiguration().getCloudType());
    }

    @Test
    public void testAddMaskForCredential() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_ACCESS_KEY, "accessKey");
        storageParams.put(AWS_S3_SECRET_KEY, "secretKey");
        storageParams.put(AZURE_BLOB_SAS_TOKEN, "sasToken");
        storageParams.put(AZURE_BLOB_SHARED_KEY, "sharedKey");
        storageParams.put(AZURE_ADLS2_SAS_TOKEN, "sasToken");
        storageParams.put(AZURE_ADLS2_SHARED_KEY, "sharedKey");
        Deencapsulation.invoke(StorageVolume.class, "addMaskForCredential", storageParams);
        Assertions.assertEquals(StorageVolume.CREDENTIAL_MASK, storageParams.get(AWS_S3_ACCESS_KEY));
        Assertions.assertEquals(StorageVolume.CREDENTIAL_MASK, storageParams.get(AWS_S3_SECRET_KEY));
        Assertions.assertEquals(StorageVolume.CREDENTIAL_MASK, storageParams.get(AZURE_BLOB_SAS_TOKEN));
        Assertions.assertEquals(StorageVolume.CREDENTIAL_MASK, storageParams.get(AZURE_BLOB_SHARED_KEY));
        Assertions.assertEquals(StorageVolume.CREDENTIAL_MASK, storageParams.get(AZURE_ADLS2_SAS_TOKEN));
        Assertions.assertEquals(StorageVolume.CREDENTIAL_MASK, storageParams.get(AZURE_ADLS2_SHARED_KEY));
    }

    @Test
    public void testAddMaskInvalidForInvalidCredential() {
        String awsSecretKey = "SomeAWSSecretKey";
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_ACCESS_KEY, "accessKey");
        storageParams.put(AWS_S3_SECRET_KEY, awsSecretKey);
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        Exception exception = Assertions.assertThrows(SemanticException.class, () -> new StorageVolume(
                "1", "test", "obs", Collections.singletonList("s3://foobar"), storageParams, true, ""
        ));
        Assertions.assertFalse(exception.getMessage().contains(awsSecretKey));
        Assertions.assertTrue(exception.getMessage().contains(StorageVolume.CREDENTIAL_MASK));
    }
}
