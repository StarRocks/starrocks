// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;

public interface CloudConfiguration {

    void toThrift(TCloudConfiguration tCloudConfiguration);

    void applyToConfiguration(Configuration configuration);
}
