// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import com.starrocks.thrift.TCloudProperty;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public interface CloudCredential {

    /**
    * Set credentials into Hadoop configuration
    */
    void applyToConfiguration(Configuration configuration);

    /**
     * Check CloudCredential is valid.
     * We only check whether the credential format is legal,
     * and do not check whether its value is correct!
     */
    boolean validate();

    /**
     * Write credential into thrift.
     */
    void toThrift(List<TCloudProperty> properties);
}
