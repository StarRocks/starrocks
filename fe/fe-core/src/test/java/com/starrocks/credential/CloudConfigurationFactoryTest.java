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

import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_ENDPOINT;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_SAS_TOKEN;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.BLOB_ENDPOINT;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN;

public class CloudConfigurationFactoryTest {

    @Test
    public void testBuildCloudConfigurationForAWSVendedCredentials() {
        Map<String, String> map = new HashMap<>();
        map.put(S3FileIOProperties.ACCESS_KEY_ID, "ak");
        map.put(S3FileIOProperties.SECRET_ACCESS_KEY, "sk");
        map.put(S3FileIOProperties.SESSION_TOKEN, "token");
        map.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
        map.put(AwsClientProperties.CLIENT_REGION, "region");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(map,
                "");
        Assertions.assertNotNull(cloudConfiguration);
        Assertions.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        Assertions.assertEquals(
                "AWSCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AWSCloudCredential{useAWSSDKDefaultBehavior=false, " +
                        "useInstanceProfile=false, accessKey='ak', secretKey='sk', " +
                        "sessionToken='token', iamRoleArn='', stsRegion='', stsEndpoint='', externalId='', " +
                        "region='region', endpoint=''}, enablePathStyleAccess=true, enableSSL=true}",
                cloudConfiguration.toConfString());

        map.remove(AwsClientProperties.CLIENT_REGION);
        map.remove(S3FileIOProperties.PATH_STYLE_ACCESS);
        map.put(S3FileIOProperties.ENDPOINT, "endpoint");
        map.put(CloudConfigurationConstants.AWS_S3_REGION, "us-west-2");
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(map, "");
        Assertions.assertNotNull(cloudConfiguration);
        Assertions.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        Assertions.assertEquals(
                "AWSCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AWSCloudCredential{useAWSSDKDefaultBehavior=false, " +
                        "useInstanceProfile=false, accessKey='ak', secretKey='sk', " +
                        "sessionToken='token', iamRoleArn='', stsRegion='', stsEndpoint='', externalId='', " +
                        "region='us-west-2', endpoint='endpoint'}, enablePathStyleAccess=false, enableSSL=true}",
                cloudConfiguration.toConfString());
    }

    @Test
    public void testBuildCloudConfigurationForAzureVendedCredentials() {
        Map<String, String> map = new HashMap<>();
        map.put(ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT, "sas_token");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(map,
                "abfss://container@account.dfs.core.windows.net/path/1/2");
        Assertions.assertNotNull(cloudConfiguration);
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        Assertions.assertEquals(
                "AzureCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AzureADLS2CloudCredential{oauth2ManagedIdentity=false, oauth2TenantId='', oauth2ClientId='', " +
                        "endpoint='account.dfs.core.windows.net', storageAccount='', sharedKey='', " +
                        "sasToken='sas_token', oauth2ClientSecret='', oauth2ClientEndpoint='', oauth2TokenFile=''}}",
                cloudConfiguration.toConfString());

        map = new HashMap<>();
        map.put(ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT, "sas_token");
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(map,
                "wasbs://container@account.blob.core.windows.net/path/1/2");
        Assertions.assertNotNull(cloudConfiguration);
        Assertions.assertEquals(CloudType.AZURE, cloudConfiguration.getCloudType());
        Assertions.assertEquals(
                "AzureCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AzureBlobCloudCredential{endpoint='', storageAccount='account', sharedKey='', " +
                        "container='container', sasToken='sas_token', useManagedIdentity='false', clientId='', " +
                        "clientSecret='', tenantId=''}}",
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
        Assertions.assertEquals(cc.getCloudType(), CloudType.AWS);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Assertions.assertEquals(tc.getCloud_properties().get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL), "true");
        Assertions.assertEquals(tc.getCloud_properties().get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS),
                "false");
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
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
        Assertions.assertEquals(cc.getCloudType(), CloudType.ALIYUN);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Assertions.assertEquals(tc.getCloud_properties().get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL), "true");
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
                "AliyunCloudConfiguration{resources='', jars='', hdpuser='', cred=AliyunCloudCredential{accessKey='XX', " +
                        "secretKey='YY', endpoint='ZZ'}}");
    }

    @Test
    public void testAzureBlobCloudConfiguration() {
        {
            Map<String, String> map = new HashMap<String, String>() {
                {
                    put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, "XX0");
                    put(CloudConfigurationConstants.AZURE_BLOB_CONTAINER, "XX1");
                    put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, "XX2");
                    put(CloudConfigurationConstants.AZURE_BLOB_STORAGE_ACCOUNT, "XX3");
                    put(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT, "XX4");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_USE_MANAGED_IDENTITY, "true");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, "XX5");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET, "XX6");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID, "XX7");
                }
            };
            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
            Assertions.assertEquals(cc.getCloudType(), CloudType.AZURE);
            TCloudConfiguration tc = new TCloudConfiguration();
            cc.toThrift(tc);
            Assertions.assertEquals(TCloudType.AZURE, tc.getCloud_type());

            Configuration conf = new Configuration();
            cc.applyToConfiguration(conf);
            cc.toFileStoreInfo();
            Assertions.assertEquals(cc.toConfString(),
                    "AzureCloudConfiguration{resources='', jars='', hdpuser='', cred=AzureBlobCloudCredential{endpoint='XX4', " +
                            "storageAccount='XX3', sharedKey='XX0', container='XX1', sasToken='XX2', " +
                            "useManagedIdentity='true', clientId='XX5', clientSecret='XX6', tenantId='XX7'}}");
        }

        // For azure native sdk
        {
            Map<String, String> map = new HashMap<String, String>() {
                {
                    put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, "shared_key_xxx");
                }
            };

            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
            TCloudConfiguration tc = new TCloudConfiguration();
            cc.toThrift(tc);
            Map<String, String> cloudProperties = tc.getCloud_properties();
            Assertions.assertEquals("shared_key_xxx", cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY));
        }

        {
            Map<String, String> map = new HashMap<String, String>() {
                {
                    put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, "sas_token_xxx");
                }
            };

            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
            TCloudConfiguration tc = new TCloudConfiguration();
            cc.toThrift(tc);
            Map<String, String> cloudProperties = tc.getCloud_properties();
            Assertions.assertEquals("sas_token_xxx", cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN));
        }

        {
            Map<String, String> map = new HashMap<String, String>() {
                {
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_xxx");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET, "client_secret_xxx");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID, "tenant_id_xxx");
                }
            };

            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
            TCloudConfiguration tc = new TCloudConfiguration();
            cc.toThrift(tc);
            Map<String, String> cloudProperties = tc.getCloud_properties();
            Assertions.assertEquals("client_id_xxx",
                    cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID));
            Assertions.assertEquals("client_secret_xxx",
                    cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET));
            Assertions.assertEquals("tenant_id_xxx",
                    cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID));
        }

        {
            Map<String, String> map = new HashMap<String, String>() {
                {
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_USE_MANAGED_IDENTITY, "true");
                    put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_xxx");
                }
            };

            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
            TCloudConfiguration tc = new TCloudConfiguration();
            cc.toThrift(tc);
            Map<String, String> cloudProperties = tc.getCloud_properties();
            Assertions.assertEquals("client_id_xxx",
                    cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID));
        }
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
        Assertions.assertEquals(cc.getCloudType(), CloudType.AZURE);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
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
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TOKEN_FILE, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(cc.getCloudType(), CloudType.AZURE);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
                "AzureCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=AzureADLS2CloudCredential{oauth2ManagedIdentity=false, oauth2TenantId='XX', " +
                        "oauth2ClientId='XX', endpoint='', storageAccount='XX', sharedKey='XX', sasToken='', " +
                        "oauth2ClientSecret='XX', oauth2ClientEndpoint='XX', oauth2TokenFile='XX'}}");
    }

    @Test
    public void testAzureADLS2ManagedIdentity() {
        Map<String, String> map = new HashMap<>() {
            {
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT, "endpoint");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET, "client-secret");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID, "client-id");
            }
        };

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(cc.getCloudType(), CloudType.AZURE);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        Assertions.assertEquals("OAuth", conf.get("fs.azure.account.auth.type"));
        Assertions.assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                conf.get("fs.azure.account.oauth.provider.type"));
        Assertions.assertEquals("client-secret", conf.get("fs.azure.account.oauth2.client.secret"));
        Assertions.assertEquals("client-id", conf.get("fs.azure.account.oauth2.client.id"));
        Assertions.assertEquals("endpoint", conf.get("fs.azure.account.oauth2.client.endpoint"));
    }

    @Test
    public void testAzureADLS2Oauth2() {
        Map<String, String> map = new HashMap<>() {
            {
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, "true");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID, "client-id");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID, "tenant-id");
            }
        };

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(cc.getCloudType(), CloudType.AZURE);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        Assertions.assertEquals("OAuth", conf.get("fs.azure.account.auth.type"));
        Assertions.assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
                conf.get("fs.azure.account.oauth.provider.type"));
        Assertions.assertEquals("tenant-id", conf.get("fs.azure.account.oauth2.msi.tenant"));
        Assertions.assertEquals("client-id", conf.get("fs.azure.account.oauth2.client.id"));
    }

    @Test
    public void testAzureADLS2WorkloadIdentity() {
        Map<String, String> map = new HashMap<>() {
            {                
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID, "client-id");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID, "tenant-id");
                put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TOKEN_FILE, "/path/to/token");
            }
        };

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(cc.getCloudType(), CloudType.AZURE);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        Assertions.assertEquals("OAuth", conf.get("fs.azure.account.auth.type"));
        Assertions.assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
                conf.get("fs.azure.account.oauth.provider.type"));
        Assertions.assertEquals("tenant-id", conf.get("fs.azure.account.oauth2.msi.tenant"));
        Assertions.assertEquals("/path/to/token", conf.get("fs.azure.account.oauth2.token.file"));
    }

    @Test
    public void testGCPCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>() {
            {
                put(CloudConfigurationConstants.GCP_GCS_ENDPOINT, "http://xx");
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY, "XX");
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID, "XX");
                put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL, "XX");
                put(CloudConfigurationConstants.GCP_GCS_IMPERSONATION_SERVICE_ACCOUNT, "XX");
                put(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, "XX");
            }
        };
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(cc.getCloudType(), CloudType.GCP);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
                "GCPCloudConfiguration{resources='', jars='', hdpuser='', " +
                        "cred=GCPCloudCredential{endpoint='http://xx', useComputeEngineServiceAccount=false, " +
                        "serviceAccountEmail='XX', serviceAccountPrivateKeyId='XX', serviceAccountPrivateKey='XX', " +
                        "impersonationServiceAccount='XX', accessToken='', accessTokenExpiresAt=''}}");
    }

    @Test
    public void testBuildCloudConfigurationForGCPVendedCredentials() {
        Map<String, String> map = new HashMap<>();
        map.put(GCS_ACCESS_TOKEN, "access_token");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(map,
                "gs://iceberg_gcp/iceberg_catalog/path/1/2");
        Assertions.assertNotNull(cloudConfiguration);
        Assertions.assertEquals(CloudType.GCP, cloudConfiguration.getCloudType());
        Assertions.assertEquals(
                "GCPCloudConfiguration{resources='', jars='', hdpuser='', cred=GCPCloudCredential{endpoint='', " +
                        "useComputeEngineServiceAccount=false, serviceAccountEmail='', serviceAccountPrivateKeyId='', " +
                        "serviceAccountPrivateKey='', impersonationServiceAccount='', accessToken='access_token', " +
                        "accessTokenExpiresAt='9223372036854775807'}}",
                cloudConfiguration.toConfString());
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
        Assertions.assertEquals(cc.getCloudType(), CloudType.HDFS);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
                "HDFSCloudConfiguration{resources='', jars='', hdpuser='XX', cred=HDFSCloudCredential{authentication='simple', " +
                        "username='XX', password='XX', krbPrincipal='', krbKeyTabFile='', krbKeyTabData=''}}");

        map.clear();
        cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(CloudType.DEFAULT, cc.getCloudType());

        cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map, true);
        Assertions.assertEquals(CloudType.HDFS, cc.getCloudType());
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
        Assertions.assertNotNull(cc);
        Assertions.assertEquals(cc.getCloudType(), CloudType.TENCENT);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Assertions.assertEquals(tc.getCloud_properties().get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL), "true");
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(),
                "TencentCloudConfiguration{resources='', jars='', hdpuser='', cred=TencentCloudCredential{accessKey='XX', " +
                        "secretKey='YY', endpoint='ZZ'}}");
    }

    @Test
    public void testDefaultCloudConfiguration() {
        Map<String, String> map = new HashMap<String, String>();
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(map);
        Assertions.assertEquals(cc.getCloudType(), CloudType.DEFAULT);
        TCloudConfiguration tc = new TCloudConfiguration();
        cc.toThrift(tc);
        Configuration conf = new Configuration();
        cc.applyToConfiguration(conf);
        cc.toFileStoreInfo();
        Assertions.assertEquals(cc.toConfString(), "CloudConfiguration{resources='', jars='', hdpuser=''}");
    }

    @Test
    public void testGlueCredential() {
        HiveConf conf = new HiveConf();
        conf.set(CloudConfigurationConstants.AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        AwsCloudCredential cred = CloudConfigurationFactory.buildGlueCloudCredential(conf);
        Assertions.assertNotNull(cred);
        Assertions.assertEquals("AWSCloudCredential{useAWSSDKDefaultBehavior=true, useInstanceProfile=false, " +
                        "accessKey='', secretKey='', sessionToken='', iamRoleArn='', stsRegion='', " +
                        "stsEndpoint='', externalId='', region='us-east-1', endpoint=''}", cred.toCredString());
    }
}
