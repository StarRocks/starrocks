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

import com.staros.proto.FileStoreInfo;
import com.starrocks.credential.aliyun.AliyunCloudConfigurationFactory;
import com.starrocks.credential.aws.AWSCloudConfigurationFactory;
import com.starrocks.credential.azure.AzureCloudConfigurationFactory;
import com.starrocks.credential.gcp.GCPCloudConfigurationFactory;
import com.starrocks.credential.hdfs.HDFSCloudConfigurationFactory;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsProperties;

import java.util.HashMap;
import java.util.Map;

public abstract class CloudConfigurationFactory {
    public static CloudConfiguration buildCloudConfigurationForStorage(Map<String, String> properties) {
        CloudConfigurationFactory factory = new AWSCloudConfigurationFactory(properties);
        CloudConfiguration cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }

        factory = new AzureCloudConfigurationFactory(properties);
        cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }

        factory = new GCPCloudConfigurationFactory(properties);
        cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }

        factory = new AliyunCloudConfigurationFactory(properties);
        cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }

        factory = new HDFSCloudConfigurationFactory(properties);
        cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }

        return buildDefaultCloudConfiguration();
    }

    public static CloudConfiguration buildCloudConfigurationForTabular(Map<String, String> properties) {
        Map<String, String> copiedProperties = new HashMap<>();
        String sessionAk = properties.getOrDefault(AwsProperties.S3FILEIO_ACCESS_KEY_ID, null);
        String sessionSk = properties.getOrDefault(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, null);
        String sessionToken = properties.getOrDefault(AwsProperties.S3FILEIO_SESSION_TOKEN, null);
        String region = properties.getOrDefault(AwsProperties.CLIENT_REGION, null);
        if (sessionAk != null && sessionSk != null && sessionToken != null && region != null) {
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, sessionAk);
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, sessionSk);
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, sessionToken);
            copiedProperties.put(CloudConfigurationConstants.AWS_S3_REGION, region);
            CloudConfigurationFactory factory = new AWSCloudConfigurationFactory(copiedProperties);
            CloudConfiguration cloudConfiguration = factory.buildForStorage();
            if (cloudConfiguration != null) {
                return cloudConfiguration;
            }
        }

        return buildDefaultCloudConfiguration();
    }

    // If user didn't specific any credential, we create DefaultCloudConfiguration instead.
    // It will use Hadoop default constructor instead, user can put core-site.xml into java CLASSPATH to control
    // authentication manually
    public static CloudConfiguration buildDefaultCloudConfiguration() {
        return new CloudConfiguration() {
            @Override
            public void toThrift(TCloudConfiguration tCloudConfiguration) {
                tCloudConfiguration.cloud_type = TCloudType.DEFAULT;
            }

            @Override
            public void applyToConfiguration(Configuration configuration) {

            }

            @Override
            public CloudType getCloudType() {
                return CloudType.DEFAULT;
            }

            @Override
            public FileStoreInfo toFileStoreInfo() {
                return null;
            }

            @Override
            public String getCredentialString() {
                return "default";
            }
        };
    }

    protected abstract CloudConfiguration buildForStorage();
}