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

import com.staros.proto.FileStoreInfo;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class CloudConfiguration {
    private static final Logger LOG = LogManager.getLogger(CloudConfiguration.class);

    private String configResources;
    private String runtimeJars;

    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.cloud_type = TCloudType.DEFAULT;
        Map<String, String> properties = new HashMap<>();
        properties.put(HadoopExt.HDFS_CONFIG_RESOURCES, configResources);
        properties.put(HadoopExt.HDFS_RUNTIME_JARS, runtimeJars);
        properties.put(HadoopExt.HDFS_CLOUD_CONFIGURATION_STRING, toConfString());
        tCloudConfiguration.setCloud_properties_v2(properties);
    }

    public void applyToConfiguration(Configuration configuration) {
        HadoopExt.addConfigResourcesToConfiguration(configResources, configuration);
        configuration.set(HadoopExt.HDFS_CLOUD_CONFIGURATION_STRING, toConfString());
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
        configResources = properties.getOrDefault(HadoopExt.HDFS_CONFIG_RESOURCES, "");
        runtimeJars = properties.getOrDefault(HadoopExt.HDFS_RUNTIME_JARS, "");
    }

    public String getCommonFieldsString() {
        return String.format("resources='%s', jars='%s'", configResources, runtimeJars);
    }
}
