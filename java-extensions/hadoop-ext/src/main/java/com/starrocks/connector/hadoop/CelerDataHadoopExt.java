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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CelerDataHadoopExt extends HadoopExt {
    public static final String HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
    public static final String HIVE_METASTORE_KERBEROS_KEYTAB_FILE = "hive.metastore.kerberos.keytab.file";
    public static final String HIVE_METASTORE_CLIENT_KERBEROS_PRINCIPAL = "hive.metastore.client.kerberos.principal";

    public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String HADOOP_HDFS_KERBEROS_KEYTAB_FILE = "hadoop.hdfs.kerberos.keytab.file";
    public static final String HADOOP_HDFS_CLIENT_KERBEROS_PRINCIPAL = "hadoop.hdfs.client.kerberos.principal";

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CelerDataHadoopExt.class);
    public static final String HADOOP_CONFIG_RESOURCES_LOADED = "hadoop.config.resources.loaded";
    public static final String STARROCKS_HOME_ENV = "STARROCKS_HOME";

    private CelerDataUGIManager globalUGIManager;

    public static void addConfigResourcesToConfiguration(Configuration conf) {
        String configResources = conf.get(HADOOP_CONFIG_RESOURCES);
        addConfigResourcesToConfiguration(configResources, conf);
    }

    public CelerDataHadoopExt() {
        if (!UserGroupInformation.isInitialized()) {
            Configuration conf = new Configuration();
            UserGroupInformation.setConfiguration(conf);
        }
        globalUGIManager = new CelerDataUGIManager();
    }

    public static void addConfigResourcesToConfiguration(String configResources, Configuration conf) {
        if (configResources == null || configResources.isEmpty()) {
            return;
        }
        if (conf.getBoolean(HADOOP_CONFIG_RESOURCES_LOADED, false)) {
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
            LOGGER.info(String.format("%s add path '%s' to configuration", LOGGER_MESSAGE_PREFIX, path.toString()));
            conf.addResource(path);
        }
        conf.setBoolean(HADOOP_CONFIG_RESOURCES_LOADED, true);
    }

    private static boolean isHMSKerberosEnabled(Configuration conf) {
        return conf.getBoolean(HIVE_METASTORE_SASL_ENABLED, false);
    }

    private static String getHMSKerberosKeytabFile(Configuration conf) {
        return conf.get(HIVE_METASTORE_KERBEROS_KEYTAB_FILE);
    }

    private static String getHMSKerberosPrincipal(Configuration conf) {
        return conf.get(HIVE_METASTORE_CLIENT_KERBEROS_PRINCIPAL);
    }

    private static boolean isHDFSKerberosEnabled(Configuration conf) {
        return conf.get(HADOOP_SECURITY_AUTHENTICATION, "").equalsIgnoreCase("kerberos");
    }

    public static String getHDFSKerberosKeytabFile(Configuration conf) {
        return conf.get(HADOOP_HDFS_KERBEROS_KEYTAB_FILE);
    }

    public static String getHDFSKerberosPrincipal(Configuration conf) {
        return conf.get(HADOOP_HDFS_CLIENT_KERBEROS_PRINCIPAL);
    }

    @Override
    public void rewriteConfiguration(Configuration conf) {
        addConfigResourcesToConfiguration(conf);
    }

    @Override
    public UserGroupInformation getHMSUGI(Configuration conf) {
        if (!isHMSKerberosEnabled(conf)) {
            return null;
        }
        String keytab = getHMSKerberosKeytabFile(conf);
        String principal = getHMSKerberosPrincipal(conf);
        if (keytab == null) {
            LOGGER.warn(LOGGER_MESSAGE_PREFIX + " hms kerberos enabled, but keytab is null");
            return null;
        }
        if (principal == null) {
            LOGGER.warn(LOGGER_MESSAGE_PREFIX + " hms kerberos enabled, but principal is null");
            return null;
        }
        try {
            return globalUGIManager.getOrCreate(keytab, principal);
        } catch (IOException e) {
            LOGGER.warn(LOGGER_MESSAGE_PREFIX + " create hms ugi failed", e);
        }
        return null;
    }

    @Override
    public UserGroupInformation getHDFSUGI(Configuration conf) {
        if (!isHDFSKerberosEnabled(conf)) {
            return null;
        }
        String keytab = getHDFSKerberosKeytabFile(conf);
        String principal = getHDFSKerberosPrincipal(conf);
        if (keytab == null) {
            LOGGER.warn(LOGGER_MESSAGE_PREFIX + " hdfs kerberos enabled, but keytab is null");
            return null;
        }
        if (principal == null) {
            LOGGER.warn(LOGGER_MESSAGE_PREFIX + " hdfs kerberos enabled, but principal is null");
            return null;
        }
        try {
            return globalUGIManager.getOrCreate(keytab, principal);
        } catch (IOException e) {
            LOGGER.warn(LOGGER_MESSAGE_PREFIX + " create hdfs ugi failed", e);
        }
        return null;
    }

    @Override
    public FileSystem bindUGIToFileSystem(FileSystem fs, UserGroupInformation ugi) {
        return FileSystemByteBuddyProxy.createFSProxy(fs, ugi);
    }
}
