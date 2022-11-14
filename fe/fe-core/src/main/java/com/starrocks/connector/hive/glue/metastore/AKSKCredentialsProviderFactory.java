// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.metastore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.hadoop.hive.conf.HiveConf;

import static com.google.common.base.Preconditions.checkArgument;

public class AKSKCredentialsProviderFactory implements AWSCredentialsProviderFactory {

    public static final String AWS_ACCESS_KEY_CONF_VAR = "hive.aws_session_access_id";
    public static final String AWS_SECRET_KEY_CONF_VAR = "hive.aws_session_secret_key";

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
