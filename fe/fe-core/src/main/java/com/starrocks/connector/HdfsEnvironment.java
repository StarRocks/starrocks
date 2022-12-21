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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class HdfsEnvironment {
    private final HdfsConfiguration hdfsConfiguration;

    /**
     * Create default Hdfs environment
     */
    private HdfsEnvironment() {
        this.hdfsConfiguration = new HdfsConfiguration();
    }

    /**
     * Apply CloudConfiguration into HdfsConfiguration
     */
    private void applyToCloudConfiguration(CloudConfiguration cloudConfiguration) {
        Preconditions.checkNotNull(cloudConfiguration);
        hdfsConfiguration.applyToCloudConfiguration(cloudConfiguration);
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

    public static class HdfsEnvironmentFactory {
        public static HdfsEnvironment build(Map<String, String> properties) {
            return build(properties, null);
        }

        public static HdfsEnvironment build(CloudConfiguration cloudConfiguration) {
            return build(null, cloudConfiguration);
        }

        public static HdfsEnvironment build() {
            return build(null, null);
        }

        public static HdfsEnvironment build(Map<String, String> properties, CloudConfiguration cloudConfiguration) {
            HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
            if (properties != null) {
                CloudConfiguration tmpCloudConfiguration =
                        CloudConfigurationFactory.buildStorageCloudConfiguration(properties);
                if (tmpCloudConfiguration != null) {
                    hdfsEnvironment.applyToCloudConfiguration(tmpCloudConfiguration);
                }
            }

            if (cloudConfiguration != null) {
                hdfsEnvironment.applyToCloudConfiguration(cloudConfiguration);
            }
            return hdfsEnvironment;
        }
    }
}




