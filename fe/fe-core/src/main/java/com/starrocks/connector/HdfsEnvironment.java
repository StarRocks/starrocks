// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class HdfsEnvironment {
    private final HdfsConfiguration hdfsConfiguration;

    /**
     * Create default Hdfs environment
     */
    public HdfsEnvironment() {
        this.hdfsConfiguration = new HdfsConfiguration();
    }

    public HdfsEnvironment(Map<String, String> properties, CloudConfiguration cloudConfiguration) {
        this.hdfsConfiguration = new HdfsConfiguration();
        if (cloudConfiguration != null) {
            this.hdfsConfiguration.applyToCloudConfiguration(cloudConfiguration);
        } else if (properties != null) {
            CloudConfiguration tmp = CloudConfigurationFactory.tryBuildForStorage(properties);
            if (tmp != null) {
                this.hdfsConfiguration.applyToCloudConfiguration(tmp);
            }
        }
        // If both is null, we just create a default HdfsEnvironment
    }

    public Configuration getConfiguration() {
        return hdfsConfiguration.getConfiguration();
    }

    private static class HdfsConfiguration {
        private final Configuration configuration;

        public HdfsConfiguration() {
            this.configuration = new Configuration();
        }

        public void applyToCloudConfiguration(CloudConfiguration cloudConfiguration) {
            cloudConfiguration.applyToConfiguration(configuration);
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }
}




