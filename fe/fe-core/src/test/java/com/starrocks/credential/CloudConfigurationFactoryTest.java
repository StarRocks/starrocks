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

import org.apache.iceberg.aws.AwsProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CloudConfigurationFactoryTest {

    @Test
    public void testBuildCloudConfigurationForTabular() {
        Map<String, String> map = new HashMap<>();
        map.put(AwsProperties.S3FILEIO_ACCESS_KEY_ID, "ak");
        map.put(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, "sk");
        map.put(AwsProperties.S3FILEIO_SESSION_TOKEN, "token");
        map.put(AwsProperties.CLIENT_REGION, "region");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForTabular(map);
        Assert.assertNotNull(cloudConfiguration);
        Assert.assertEquals(CloudType.AWS, cloudConfiguration.getCloudType());
        Assert.assertEquals("AWSCloudConfiguration{awsCloudCredential=AWSCloudCredential" +
                "{useAWSSDKDefaultBehavior=false, useInstanceProfile=false, " +
                "accessKey='ak', secretKey='sk', sessionToken='token', iamRoleArn='', " +
                "externalId='', region='region', endpoint=''}, enablePathStyleAccess=false, " +
                "enableSSL=true}", cloudConfiguration.getCredentialString());
    }
}
