// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import java.util.Map;

public abstract class CloudConfigurationFactory {
    public static CloudConfiguration tryBuildForStorage(Map<String, String> properties) {
        return new HdfsConfiguration(properties);
    }

    protected abstract CloudConfiguration buildForStorage();
}