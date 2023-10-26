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

package com.starrocks.connector.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopExt {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HadoopExt.class);

    public static final String HDFS_CONFIG_RESOURCES = "hadoop.config.resources";
    public static final String HDFS_CONFIG_RESOURCES_LOADED = "hadoop.config.resources.loaded";
    public static final String HDFS_RUNTIME_JARS = "hadoop.runtime.jars";
    public static final String HDFS_CLOUD_CONFIGURATION_STRING = "hadoop.cloud.configuration.string";
    public static final String STARROCKS_HOME_ENV = "STARROCKS_HOME";
    public static final String LOGGER_MESSAGE_PREFIX = "[hadoop-ext]";

    public static void addConfigResourcesToConfiguration(Configuration conf) {
        String configResources = conf.get(HDFS_CONFIG_RESOURCES);
        addConfigResourcesToConfiguration(configResources, conf);
    }

    public static void addConfigResourcesToConfiguration(String configResources, Configuration conf) {
        if (configResources == null || configResources.isEmpty()) {
            return;
        }
        if (conf.getBoolean(HDFS_CONFIG_RESOURCES_LOADED, false)) {
            return;
        }
        final String STARROCKS_HOME_DIR = System.getenv(STARROCKS_HOME_ENV);
        if (STARROCKS_HOME_DIR == null) {
            LOGGER.warn(String.format("%s env '%s' is not defined", LOGGER_MESSAGE_PREFIX, STARROCKS_HOME_ENV));
            return;
        }
        String[] parts = configResources.split(",");
        for (String p : parts) {
            Path path = new Path(STARROCKS_HOME_DIR + "/conf/", p);
            LOGGER.info(String.format("%s Add path '%s' to configuration", LOGGER_MESSAGE_PREFIX, path.toString()));
            conf.addResource(path);
        }
        conf.setBoolean(HDFS_CONFIG_RESOURCES_LOADED, true);
    }

    public static String getCloudConfString(Configuration conf) {
        return conf.get(HDFS_CLOUD_CONFIGURATION_STRING, "");
    }
}