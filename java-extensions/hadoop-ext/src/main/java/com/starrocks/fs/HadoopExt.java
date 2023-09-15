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

package com.starrocks.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class HadoopExt {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HadoopExt.class);

    // For s3 scheme, we have to set impl 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    public static String[] S3_SCHEMES = new String[] {"s3", "s3a", "oss", "ks3", "obs", "tos", "cosn", "oss"};
    // For those schemes, we don't need to set explictly.
    public static String[] REST_SCHEMES = new String[] {"wasb", "wasbs", "adl", "abfs", "abfss", "gs", "hdfs", "viewfs", "har"};

    public static List<String> ALL_SCHEMES = new ArrayList<String>();

    static {
        ALL_SCHEMES.addAll(Arrays.asList(S3_SCHEMES));
        ALL_SCHEMES.addAll(Arrays.asList(REST_SCHEMES));
    }

    public static String FS_IMPL_FMT = "fs.%s.impl";
    public static String FS_IMPL_DISABLE_CACHE_FMT = "fs.%s.impl.disable.cache";
    public static String FS_IMPL_STARROCKS_CACHE_FILESYSTEM = "com.starrocks.fs.CacheFileSystem";

    public static String FS_S3A_FILESYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem";

    public static final String HDFS_CONFIG_RESOURCES = "hadoop.config.resources";
    public static final String HDFS_CONFIG_RESOURCES_LOADED = "hadoop.config.resources.loaded";
    public static final String HDFS_RUNTIME_JARS = "hadoop.runtime.jars";
    public static final String HDFS_CLOUD_CONFIGURATION_STRING = "hadoop.cloud.configuration.string";
    public static final String STARROCKS_HOME_ENV = "STARROCKS_HOME";
    public static final String LOGGER_MESSAGE_PREFIX = "[hadoop-ext]";

    public static void addConfigResourcesToConfiguration(Configuration conf) {
        if (conf.getBoolean(HDFS_CONFIG_RESOURCES_LOADED, false)) {
            return;
        }
        String configResources = conf.get(HDFS_CONFIG_RESOURCES);
        if (configResources == null || configResources.isEmpty()) {
            return;
        }
        final String STARROCKS_HOME_DIR = System.getenv(STARROCKS_HOME_ENV);
        if (STARROCKS_HOME_DIR == null) {
            LOGGER.info(String.format("%s env '%s' is not defined", LOGGER_MESSAGE_PREFIX, STARROCKS_HOME_ENV));
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

    public static boolean isS3Scheme(String scheme) {
        for (String a : S3_SCHEMES) {
            if (scheme.equals(a)) {
                return true;
            }
        }
        return false;
    }

    public static String getCloudConfString(Configuration conf) {
        return conf.get(HDFS_CLOUD_CONFIGURATION_STRING, "");
    }
}