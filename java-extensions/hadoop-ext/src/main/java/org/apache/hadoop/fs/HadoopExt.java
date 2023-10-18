package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HadoopExt {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HadoopExt.class);

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