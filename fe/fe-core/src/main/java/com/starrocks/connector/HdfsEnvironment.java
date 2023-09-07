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

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class HdfsEnvironment {
    private final Configuration hdfsConfiguration;
    private CloudConfiguration cloudConfiguration;

    /**
     * Create default Hdfs environment
     */
    public HdfsEnvironment() {
        hdfsConfiguration = new Configuration();
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
    }

    public HdfsEnvironment(Map<String, String> properties) {
        this();
        if (properties != null) {
            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            this.cloudConfiguration = cc;
            cc.applyToConfiguration(hdfsConfiguration);
        }
    }

    public HdfsEnvironment(CloudConfiguration cc) {
        this();
        if (cloudConfiguration != null) {
            this.cloudConfiguration = cc;
            cc.applyToConfiguration(hdfsConfiguration);
        }
    }

    public Configuration getConfiguration() {
        return hdfsConfiguration;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}




