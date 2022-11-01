// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.metastore;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.hive.conf.HiveConf;

public interface AWSCredentialsProviderFactory {

    AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf);
}
