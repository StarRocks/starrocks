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

import com.google.common.collect.ImmutableList;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.aliyun.AliyunCloudConfigurationProvider;
import com.starrocks.credential.aws.AwsCloudConfigurationProvider;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.credential.azure.AzureCloudConfigurationProvider;
import com.starrocks.credential.gcp.GCPCloudConfigurationProvoder;
import com.starrocks.credential.hdfs.HDFSCloudConfigurationProvider;
import com.starrocks.credential.hdfs.StrictHDFSCloudConfigurationProvider;
import com.starrocks.credential.tencent.TencentCloudConfigurationProvider;
import com.starrocks.fs.azure.AzBlobURI;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_ENDPOINT;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_SAS_TOKEN;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.BLOB_ENDPOINT;

public class CloudConfigurationFactory {
    private static final Logger LOG = LogManager.getLogger(CloudConfigurationFactory.class);
    static ImmutableList<CloudConfigurationProvider> cloudConfigurationFactoryChain = ImmutableList.of(
            new AwsCloudConfigurationProvider(),
            new AzureCloudConfigurationProvider(),
            new GCPCloudConfigurationProvoder(),
            new AliyunCloudConfigurationProvider(),
            new TencentCloudConfigurationProvider(),
            new HDFSCloudConfigurationProvider(),
            (Map<String, String> properties) -> new CloudConfiguration());

    static ImmutableList<CloudConfigurationProvider> strictCloudConfigurationFactoryChain = ImmutableList.of(
            new AwsCloudConfigurationProvider(),
            new AzureCloudConfigurationProvider(),
            new GCPCloudConfigurationProvoder(),
            new AliyunCloudConfigurationProvider(),
            new TencentCloudConfigurationProvider(),
            new HDFSCloudConfigurationProvider(),
            new StrictHDFSCloudConfigurationProvider(),
            (Map<String, String> properties) -> new CloudConfiguration());

    public static CloudConfiguration buildCloudConfigurationForStorage(Map<String, String> properties) {
        return buildCloudConfigurationForStorage(properties, false);
    }

    public static CloudConfiguration buildCloudConfigurationForStorage(Map<String, String> properties, boolean strictMode) {
        ImmutableList<CloudConfigurationProvider> factories = cloudConfigurationFactoryChain;
        if (strictMode) {
            factories = strictCloudConfigurationFactoryChain;
        }
        for (CloudConfigurationProvider factory : factories) {
            CloudConfiguration cloudConfiguration = factory.build(properties);
            if (cloudConfiguration != null) {
                cloudConfiguration.loadCommonFields(properties);
                return cloudConfiguration;
            }
        }
        // Should never reach here.
        return null;
    }

    public static AwsCloudCredential buildGlueCloudCredential(HiveConf hiveConf) {
        for (CloudConfigurationProvider factory : cloudConfigurationFactoryChain) {
            if (factory instanceof AwsCloudConfigurationProvider) {
                AwsCloudConfigurationProvider provider = ((AwsCloudConfigurationProvider) factory);
                return provider.buildGlueCloudCredential(hiveConf);
            }
        }
        // Should never reach here.
        return null;
    }

    public static CloudConfiguration buildCloudConfigurationForVendedCredentials(Map<String, String> properties,
                                                                                 String path) {
        CloudConfiguration cloudConfiguration = buildCloudConfigurationForAWSVendedCredentials(properties);
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return cloudConfiguration;
        }

        cloudConfiguration = buildCloudConfigurationForAzureVendedCredentials(properties, path);
        return cloudConfiguration;
    }

    public static CloudConfiguration buildCloudConfigurationForAWSVendedCredentials(Map<String, String> properties) {
        Map<String, String> copiedProperties = new HashMap<>();
        String sessionAk = properties.getOrDefault(S3FileIOProperties.ACCESS_KEY_ID, null);
        String sessionSk = properties.getOrDefault(S3FileIOProperties.SECRET_ACCESS_KEY, null);
        String sessionToken = properties.getOrDefault(S3FileIOProperties.SESSION_TOKEN, null);
        String region = properties.getOrDefault(AwsClientProperties.CLIENT_REGION,
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_REGION, null));
        String enablePathStyle = properties.getOrDefault(S3FileIOProperties.PATH_STYLE_ACCESS,
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS, null));
        String endpoint = properties.getOrDefault(S3FileIOProperties.ENDPOINT,
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_ENDPOINT, null));
        if (sessionAk != null && sessionSk != null && sessionToken != null) {
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, sessionAk);
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, sessionSk);
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, sessionToken);
            if (region != null) {
                copiedProperties.put(CloudConfigurationConstants.AWS_S3_REGION, region);
            }
            if (endpoint != null) {
                copiedProperties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, endpoint);
            }
            if (enablePathStyle != null) {
                copiedProperties.put(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS, enablePathStyle);
            }
        }
        return buildCloudConfigurationForStorage(copiedProperties);
    }

    public static CloudConfiguration buildCloudConfigurationForAzureVendedCredentials(Map<String, String> properties,
                                                                                      String path) {
        Map<String, String> copiedProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(ADLS_SAS_TOKEN) && entry.getKey().endsWith(ADLS_ENDPOINT)) {
                String endpoint = entry.getKey().substring(ADLS_SAS_TOKEN.length());
                copiedProperties.put(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT, endpoint);
                copiedProperties.put(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN, entry.getValue());
            } else if (entry.getKey().startsWith(ADLS_SAS_TOKEN) && entry.getKey().endsWith(BLOB_ENDPOINT)) {
                try {
                    AzBlobURI uri = AzBlobURI.parse(path);
                    copiedProperties.put(CloudConfigurationConstants.AZURE_BLOB_STORAGE_ACCOUNT, uri.getAccount());
                    copiedProperties.put(CloudConfigurationConstants.AZURE_BLOB_CONTAINER, uri.getContainer());
                    copiedProperties.put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, entry.getValue());
                } catch (StarRocksException e) {
                    String errorMessage = String.format("Failed to parse azure blob file for path: %s, properties: %s", path,
                            properties);
                    LOG.error(errorMessage, e);
                }
            }
        }
        return buildCloudConfigurationForStorage(copiedProperties);
    }
}