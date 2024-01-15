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

package com.starrocks.credential;

import com.starrocks.credential.aws.AWSCloudCredential;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CloudConfigurationFactoryTest {

    @Test
    public void testBuildCloudConfigurationForTabular() {
        Map<String, String> map = new HashMap<>();
        map.put(S3FileIOProperties.ACCESS_KEY_ID, "ak");
        map.put(S3FileIOProperties.SECRET_ACCESS_KEY, "sk");
        map.put(S3FileIOProperties.SESSION_TOKEN, "token");
        map.put(AwsClientProperties.CLIENT_REGION, "region");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForTabular(map);
        Assert.assertNotNull(cloudConfiguration);
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        Assert.assertEquals(
                "AWSCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AWSCloudCredential{useAWSSDKDefaultBehavior=false, " +
                        "useInstanceProfile=false, accessKey='ak', secretKey='sk', " +
                        "sessionToken='token', iamRoleArn='', stsRegion='', stsEndpoint='', externalId='', " +
                        "region='region', endpoint=''}, enablePathStyleAccess=false, enableSSL=true}",
                cloudConfiguration.toConfString());
    }

    @Test
    public void testAWSCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "XX");
                put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, "YY");
                put(CloudConfigurationConstants.AWS_S3_REGION, "ZZ");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.AWS);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Assert.assertEquals(tc.getCloud_properties_v2().get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL), "true");
        Assert.assertEquals(tc.getCloud_properties_v2().get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS),
                "false");
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "AWSCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AWSCloudCredential{useAWSSDKDefaultBehavior=false, useInstanceProfile=false, " +
                        "accessKey='XX', secretKey='YY', sessionToken='', iamRoleArn='', stsRegion='', " +
                        "stsEndpoint='', externalId='', region='ZZ', endpoint=''}, " +
                        "enablePathStyleAccess=false, enableSSL=true}");
    }

    @Test
    public void testAliyunCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, "XX");
                put(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, "YY");
                put(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, "ZZ");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.ALIYUN);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Assert.assertEquals(tc.getCloud_properties_v2().get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL), "true");
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "AliyunCloudConfiguration{resources='', jars='', hdpuser='', cred=AliyunCloudCredential{accessKey='XX', " +
                        "secretKey='YY', endpoint='ZZ'}}");
    }

    @Test
    public void testAzureBlobCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, "XX");
                put(CloudConfigurationConstants.AZURE_BLOB_CONTAINER, "XX");
                put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, "XX");
                put(CloudConfigurationConstants.AZURE_BLOB_STORAGE_ACCOUNT, "XX");
                put(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.AZURE);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "AzureCloudConfiguration{resources='', jars='', hdpuser='', cred=AzureBlobCloudCredential{endpoint='XX', " +
                        "storageAccount='XX', sharedKey='XX', container='XX', sasToken='XX'}}");
    }

    @Test
    public void testAzureASLS1eCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_ENDPOINT, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CLIENT_ID, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS1_USE_MANAGED_SERVICE_IDENTITY, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CREDENTIAL, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.AZURE);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "AzureCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AzureADLS1CloudCredential{useManagedServiceIdentity=false," +
                        " oauth2ClientId='XX', oauth2Credential='XX', oauth2Endpoint='XX'}}");
    }

    @Test
    public void testAzureADLS2CloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS2_STORAGE_ACCOUNT, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET, "XX");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.AZURE);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "AzureCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AzureADLS2CloudCredential{oauth2ManagedIdentity=false, " +
                        "oauth2TenantId='XX', oauth2ClientId='XX', storageAccount='XX', sharedKey='XX', " +
                        "oauth2ClientSecret='XX', oauth2ClientEndpoint='XX'}}");
    }

    @Test
    public void testGCPCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY, "XX");
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID, "XX");
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL, "XX");
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_IMPERSONATION_SERVICE_ACCOUNT, "XX");
                put(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.GCP);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "GCPCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=GCPCloudCredential{useComputeEngineServiceAccount=false, " +
                        "serviceAccountEmail='XX', serviceAccountPrivateKeyId='XX', serviceAccountPrivateKey='XX', " +
                        "impersonationServiceAccount='XX'}}");
    }

    @Test
    public void testHDFSCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.HDFS_AUTHENTICATION, "simple");
                put(CloudConfigurationConstants.HDFS_USERNAME, "XX");
                put(CloudConfigurationConstants.HDFS_PASSWORD, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.HDFS);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "HDFSCloudConfiguration{resources='', jars='', hdpuser='XX', cred=HDFSCloudCredential{authentication='simple', " +
                        "username='XX', password='XX', krbPrincipal='', krbKeyTabFile='', krbKeyTabData=''}}");

        map.clear();
        cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(CloudType.DEFAULT, cc.getCloudType());

        cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map, true);
        Assert.assertEquals(CloudType.HDFS, cc.getCloudType());
    }

    @Test
    public void testTencentCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.TENCENT_COS_ACCESS_KEY, "XX");
                put(CloudConfigurationConstants.TENCENT_COS_SECRET_KEY, "YY");
                put(CloudConfigurationConstants.TENCENT_COS_ENDPOINT, "ZZ");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertNotNull(cc);
        Assert.assertEquals(cc.getCloudType(), CloudType.TENCENT);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Assert.assertEquals(tc.getCloud_properties_v2().get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL), "true");
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(),
                "TencentCloudConfiguration{resources='', jars='', hdpuser='', cred=TencentCloudCredential{accessKey='XX', " +
                        "secretKey='YY', endpoint='ZZ'}}");
    }

    @Test
    public void testDefaultCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>();
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assert.assertEquals(cc.getCloudType(), CloudType.DEFAULT);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assert.assertEquals(cc.toConfString(), "CloudConfiguration{resources='', jars='', hdpuser=''}");
    }

    @Test
    public void testGlueCredential() {
        HiveConf conf = new HiveConf();
        conf.set(CloudConfigurationConstants.AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        AWSCloudCredential cred = CloudConfigurationFactory.buildGlueCloudCredential(conf);
        Assert.assertNotNull(cred);
        Assert.assertEquals("AWSCloudCredential{useAWSSDKDefaultBehavior=true, useInstanceProfile=false, " +
                        "accessKey='', secretKey='', sessionToken='', iamRoleArn='', stsRegion='', " +
                        "stsEndpoint='', externalId='', region='us-east-1', endpoint=''}", cred.toCredString());
    }
}
