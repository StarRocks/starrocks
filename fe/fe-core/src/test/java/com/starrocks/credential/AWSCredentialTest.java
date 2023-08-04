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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AWSCredentialTest {

    @Test
    public void testUseAWSSDKDefaultBehaviorPlusAssumeRole() {
        // Test hadoop configuration
        Map<String, String>  properties = new HashMap<>();
        properties.put("aws.s3.use_aws_sdk_default_behavior", "true");
        properties.put("aws.s3.iam_role_arn", "smith");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assert.assertNotNull(cloudConfiguration);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assert.assertEquals("com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                configuration.get("fs.s3a.assumed.role.credentials.provider"));
        Assert.assertEquals("com.starrocks.credential.provider.AssumedRoleCredentialProvider",
                configuration.get("fs.s3a.aws.credentials.provider"));
        Assert.assertEquals("smith", configuration.get("fs.s3a.assumed.role.arn"));
    }
}
