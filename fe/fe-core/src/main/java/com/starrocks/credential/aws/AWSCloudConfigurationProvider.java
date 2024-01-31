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

package com.starrocks.credential.aws;

import com.google.common.base.Preconditions;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationProvider;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_EXTERNAL_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_IAM_ROLE_ARN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_SESSION_TOKEN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_STS_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_STS_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_USE_INSTANCE_PROFILE;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENABLE_SSL;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_EXTERNAL_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SESSION_TOKEN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_STS_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_STS_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE;
public class AWSCloudConfigurationProvider implements CloudConfigurationProvider {

    /**
     * In S3 SDK this region is implicitly the default one.
     * So setting it by default to `us-east-1` simulates S3 better
     * Refer this issue: <a href="https://issues.apache.org/jira/browse/HADOOP-17771">S3AFS creation fails "Unable to find a region via the region provider chain."</a>
     */
    public static final String DEFAULT_AWS_REGION = "us-east-1";

    public AWSCloudCredential buildGlueCloudCredential(HiveConf hiveConf) {
        Preconditions.checkNotNull(hiveConf);
        AWSCloudCredential awsCloudCredential = new AWSCloudCredential(
                hiveConf.getBoolean(AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR, false),
                hiveConf.getBoolean(AWS_GLUE_USE_INSTANCE_PROFILE, false),
                hiveConf.get(AWS_GLUE_ACCESS_KEY, ""),
                hiveConf.get(AWS_GLUE_SECRET_KEY, ""),
                hiveConf.get(AWS_GLUE_SESSION_TOKEN, ""),
                hiveConf.get(AWS_GLUE_IAM_ROLE_ARN, ""),
                hiveConf.get(AWS_GLUE_STS_REGION, ""),
                hiveConf.get(AWS_GLUE_STS_ENDPOINT, ""),
                hiveConf.get(AWS_GLUE_EXTERNAL_ID, ""),
                hiveConf.get(AWS_GLUE_REGION, DEFAULT_AWS_REGION),
                hiveConf.get(AWS_GLUE_ENDPOINT, "")
        );
        if (!awsCloudCredential.validate()) {
            return null;
        }
        return awsCloudCredential;
    }

    @Override
    public CloudConfiguration build(Map<String, String> properties) {
        Preconditions.checkNotNull(properties);
        AWSCloudCredential awsCloudCredential = new AWSCloudCredential(
                Boolean.parseBoolean(properties.getOrDefault(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false")),
                Boolean.parseBoolean(properties.getOrDefault(AWS_S3_USE_INSTANCE_PROFILE, "false")),
                properties.getOrDefault(AWS_S3_ACCESS_KEY, ""),
                properties.getOrDefault(AWS_S3_SECRET_KEY, ""),
                properties.getOrDefault(AWS_S3_SESSION_TOKEN, ""),
                properties.getOrDefault(AWS_S3_IAM_ROLE_ARN, ""),
                properties.getOrDefault(AWS_S3_STS_REGION, ""),
                properties.getOrDefault(AWS_S3_STS_ENDPOINT, ""),
                properties.getOrDefault(AWS_S3_EXTERNAL_ID, ""),
                properties.getOrDefault(AWS_S3_REGION, DEFAULT_AWS_REGION),
                properties.getOrDefault(AWS_S3_ENDPOINT, "")
        );
        if (!awsCloudCredential.validate()) {
            return null;
        }

        AWSCloudConfiguration awsCloudConfiguration = new AWSCloudConfiguration(awsCloudCredential);
        // put cloud configuration
        if (properties.containsKey(AWS_S3_ENABLE_PATH_STYLE_ACCESS)) {
            awsCloudConfiguration.setEnablePathStyleAccess(
                    Boolean.parseBoolean(properties.get(AWS_S3_ENABLE_PATH_STYLE_ACCESS)));
        }

        if (properties.containsKey(AWS_S3_ENABLE_SSL)) {
            awsCloudConfiguration.setEnableSSL(Boolean.parseBoolean(properties.get(AWS_S3_ENABLE_SSL)));
        }

        return awsCloudConfiguration;
    }
}