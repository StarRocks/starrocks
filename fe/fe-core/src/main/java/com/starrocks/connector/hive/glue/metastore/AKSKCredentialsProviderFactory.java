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


package com.starrocks.connector.hive.glue.metastore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.hadoop.hive.conf.HiveConf;

import static com.google.common.base.Preconditions.checkArgument;

public class AKSKCredentialsProviderFactory implements AWSCredentialsProviderFactory {

    public static final String AWS_ACCESS_KEY_CONF_VAR = "aws.hive.metastore.glue.aws-access-key";
    public static final String AWS_SECRET_KEY_CONF_VAR = "aws.hive.metastore.glue.aws-secret-key";

    @Override
    public AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf) {

        checkArgument(hiveConf != null, "hiveConf cannot be null.");

        String accessKey = hiveConf.get(AWS_ACCESS_KEY_CONF_VAR);
        String secretKey = hiveConf.get(AWS_SECRET_KEY_CONF_VAR);

        checkArgument(accessKey != null, AWS_ACCESS_KEY_CONF_VAR + " must be set.");
        checkArgument(secretKey != null, AWS_SECRET_KEY_CONF_VAR + " must be set.");

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        return new AWSStaticCredentialsProvider(credentials);
    }
}
