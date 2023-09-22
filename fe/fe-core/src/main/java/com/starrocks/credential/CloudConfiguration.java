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

package com.starrocks.credential;

import com.google.common.base.Strings;
import com.staros.proto.FileStoreInfo;
import com.starrocks.StarRocksFE;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.HDFS_CLOUD_CONFIGURATION_STRING;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_CONFIG_RESOURCES;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_CONFIG_RESOURCES_LOADED;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_RUNTIME_JARS;

public class CloudConfiguration {
    private static final Logger LOG = LogManager.getLogger(CloudConfiguration.class);

    private String configResources;
    private String runtimeJars;

    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.cloud_type = TCloudType.DEFAULT;
        Map<String, String> properties = new HashMap<>();
        properties.put(HDFS_CONFIG_RESOURCES, configResources);
        properties.put(HDFS_RUNTIME_JARS, runtimeJars);
        properties.put(HDFS_CLOUD_CONFIGURATION_STRING, toConfString());
        tCloudConfiguration.setCloud_properties_v2(properties);
    }

    public static void addConfigResourcesToConfiguration(String configResources, Configuration conf) {
        if (Strings.isNullOrEmpty(configResources)) {
            return;
        }
        String[] parts = configResources.split(",");
        for (String p : parts) {
            Path path = new Path(StarRocksFE.STARROCKS_HOME_DIR + "/conf/", p);
            LOG.debug(String.format("Add path '%s' to configuration", path.toString()));
            conf.addResource(path);
        }
        conf.setBoolean(HDFS_CONFIG_RESOURCES_LOADED, true);
    }

    public void applyToConfiguration(Configuration configuration) {
        addConfigResourcesToConfiguration(configResources, configuration);
        configuration.set(HDFS_CLOUD_CONFIGURATION_STRING, toConfString());
    }

    // Hadoop FileSystem has a cache itself, it used request uri as a cache key by default,
    // so it cannot sense the CloudCredential changed.
    // So we need to generate an identifier for different CloudCredential, and used it as cache key.
    // toConfString() Method just like toString()
    public String toConfString() {
        return "CloudConfiguration{" + getCommonFieldsString() + "}";
    }

    public CloudType getCloudType() {
        return CloudType.DEFAULT;
    }

    // Convert to the protobuf used by staros.
    public FileStoreInfo toFileStoreInfo() {
        return null;
    }

    public void loadCommonFields(Map<String, String> properties) {
        configResources = properties.getOrDefault(HDFS_CONFIG_RESOURCES, "");
        runtimeJars = properties.getOrDefault(HDFS_RUNTIME_JARS, "");
    }

    public String getCommonFieldsString() {
        return String.format("resources='%s', jars='%s'", configResources, runtimeJars);
    }
}
