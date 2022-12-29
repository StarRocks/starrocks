// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import java.util.Map;

public abstract class CloudConfigurationFactory {
    public static CloudConfiguration tryBuildForStorage(Map<String, String> properties) {
        CloudConfigurationFactory factory = new AWSCloudConfigurationFactory(properties);
        CloudConfiguration cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }

        factory = new AliyunCloudConfigurationFactory(properties);
        cloudConfiguration = factory.buildForStorage();
        if (cloudConfiguration != null) {
            return cloudConfiguration;
        }
        return cloudConfiguration;
    }

    protected abstract CloudConfiguration buildForStorage();
}