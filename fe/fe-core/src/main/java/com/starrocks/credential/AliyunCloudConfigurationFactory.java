// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import java.util.Map;

public class AliyunCloudConfigurationFactory extends CloudConfigurationFactory {
    private final Map<String, String> properties;

    public AliyunCloudConfigurationFactory(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected CloudConfiguration buildForStorage() {
        // TODO
        return null;
    }
}
