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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.staros.proto.FileStoreInfo;
import com.starrocks.StarRocksFE;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.HDFS_CONFIG_RESOURCES;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_CONFIG_RESOURCES_LOADED;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_FS_CACHE_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_RUNTIME_JARS;

public class HDFSCloudConfiguration implements CloudConfiguration {
    private static final Logger LOG = LogManager.getLogger(HDFSCloudConfiguration.class);

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

    public void addConfigResourcesToConfiguration(String configResources, Configuration conf) {
        if (Strings.isNullOrEmpty(configResources)) {
            return;
        }
        String[] parts = configResources.split(",");
        for (String p : parts) {
            Path path = new Path(StarRocksFE.STARROCKS_HOME_DIR + "/conf/", p);
            LOG.debug(String.format("Add path '%s' to configuration", path.toString()));
            conf.addResource(path);
        }
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.setCloud_type(TCloudType.HDFS);
        Map<String, String> properties = new HashMap<>();
        hdfsCloudCredential.toThrift(properties);
        properties.put(HDFS_CONFIG_RESOURCES, configResources);
        properties.put(HDFS_RUNTIME_JARS, runtimeJars);
        properties.put(HDFS_FS_CACHE_KEY, getCredentialString());
        if (!properties.isEmpty()) {
            tCloudConfiguration.setCloud_properties_v2(properties);
        }
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        hdfsCloudCredential.applyToConfiguration(configuration);
        configuration.set(HDFS_CONFIG_RESOURCES, configResources);
        configuration.set(HDFS_RUNTIME_JARS, runtimeJars);
        addConfigResourcesToConfiguration(configResources, configuration);
        configuration.setBoolean(HDFS_CONFIG_RESOURCES_LOADED, true);
        configuration.set(HDFS_FS_CACHE_KEY, getCredentialString());
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
