// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
        return null;
    }

    @Override
    protected CloudConfiguration buildForStorage() {
        return null;
    }
}