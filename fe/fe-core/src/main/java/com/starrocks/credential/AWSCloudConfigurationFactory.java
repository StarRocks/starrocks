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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;
public class AWSCloudConfigurationFactory extends CloudConfigurationFactory {
    private final Map<String, String> properties;
    private final HiveConf hiveConf;

    public AWSCloudConfigurationFactory(Map<String, String> properties) {
        this(properties, null);
    }

    public AWSCloudConfigurationFactory(HiveConf hiveConf) {
        this(null, hiveConf);
    }

    public AWSCloudConfigurationFactory(Map<String, String> properties, HiveConf hiveConf) {
        this.properties = properties;
        this.hiveConf = hiveConf;
    }

    public CloudCredential buildGlueCloudCredential() {
        Preconditions.checkNotNull(hiveConf);
        AWSCloudCredential awsCloudCredential = new AWSCloudCredential(
                hiveConf.getBoolean(CloudConfigurationConstants.AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR, false),
                hiveConf.getBoolean(CloudConfigurationConstants.AWS_GLUE_USE_INSTANCE_PROFILE, false),
                hiveConf.get(CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY, ""),
                hiveConf.get(CloudConfigurationConstants.AWS_GLUE_SECRET_KEY, ""),
                hiveConf.get(CloudConfigurationConstants.AWS_GLUE_IAM_ROLE_ARN, ""),
                hiveConf.get(CloudConfigurationConstants.AWS_GLUE_EXTERNAL_ID, ""),
                hiveConf.get(CloudConfigurationConstants.AWS_GLUE_REGION, ""),
                hiveConf.get(CloudConfigurationConstants.AWS_GLUE_ENDPOINT, "")
        );
        if (!awsCloudCredential.validate()) {
            return null;
        }
        return awsCloudCredential;
    }

    @Override
    protected CloudConfiguration buildForStorage() {
        Preconditions.checkNotNull(properties);
        AWSCloudCredential awsCloudCredential = new AWSCloudCredential(
                Boolean.parseBoolean(
                        properties.getOrDefault(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR,
                                "false")),
                Boolean.parseBoolean(
                        properties.getOrDefault(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false")),
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, ""),
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_SECRET_KEY, ""),
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN, ""),
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, ""),
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_REGION, ""),
                properties.getOrDefault(CloudConfigurationConstants.AWS_S3_ENDPOINT, "")
        );
        if (!awsCloudCredential.validate()) {
            return null;
        }

        AWSCloudConfiguration awsCloudConfiguration = new AWSCloudConfiguration(awsCloudCredential);
        // put cloud configuration
        if (properties.containsKey(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS)) {
            awsCloudConfiguration.setEnablePathStyleAccess(
                    Boolean.parseBoolean(properties.get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS))
            );
        }

        if (properties.containsKey(CloudConfigurationConstants.AWS_S3_ENABLE_SSL)) {
            awsCloudConfiguration.setEnableSSL(
                    Boolean.parseBoolean(properties.get(CloudConfigurationConstants.AWS_S3_ENABLE_SSL))
            );
        }

        return awsCloudConfiguration;
    }
}