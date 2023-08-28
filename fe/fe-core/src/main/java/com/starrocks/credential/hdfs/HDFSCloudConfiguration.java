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

package com.starrocks.credential.hdfs;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.staros.proto.FileStoreInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.HDFS_CONFIG_RESOURCES;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_RUNTIME_JARS;

public class HDFSCloudConfiguration implements CloudConfiguration {
    private final HDFSCloudCredential hdfsCloudCredential;
    private String configResources;
    private String runtimeJars;

    public HDFSCloudConfiguration(HDFSCloudCredential hdfsCloudCredential) {
        Preconditions.checkNotNull(hdfsCloudCredential);
        this.hdfsCloudCredential = hdfsCloudCredential;
    }

    public void setConfigResources(String configResources) {
        this.configResources = configResources;
    }

    public void setRuntimeJars(String runtimeJars) {
        this.runtimeJars = runtimeJars;
    }

    public HDFSCloudCredential getHdfsCloudCredential() {
        return hdfsCloudCredential;
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        Map<String, String> properties = new HashMap<>();
        hdfsCloudCredential.toThrift(properties);
        if (!properties.isEmpty()) {
            tCloudConfiguration.setCloud_properties_v2(properties);
        }
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        hdfsCloudCredential.applyToConfiguration(configuration);
        configuration.set(HDFS_CONFIG_RESOURCES, configResources);
        configuration.set(HDFS_RUNTIME_JARS, configResources);
    }

    // NOTE(yanz): hdfs credential is quite special. In most cases, people write auth/username/password etc.
    // in XML file instead of in string literals. So here we use `configResources` and `runtimeJars` as cred string.
    // And `hdfsCloudCredential` this field is only for broker load, has nothing to do with catalog connector.
    @Override
    public String getCredentialString() {
        return "HDFSCloudConfiguration{" +
                "configResources=" + configResources +
                ", runtimeJars=" + runtimeJars +
                '}';
    }

    @Override
    public CloudType getCloudType() {
        return CloudType.HDFS;
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        return hdfsCloudCredential.toFileStoreInfo();
    }
}
