// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.fs.hdfs;

import com.amazonaws.util.AwsHostNameUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.UserException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.azure.AzureCloudConfigurationProvider;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TObjectStoreType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class ConfigurationWrap extends Configuration {
    private static final Logger LOG = LogManager.getLogger(ConfigurationWrap.class);

    public String parseRegionFromEndpoint(TObjectStoreType tObjectStoreType, String endPoint) {
        if (tObjectStoreType == TObjectStoreType.S3) {
            return AwsHostNameUtils.parseRegionFromAwsPartitionPattern(endPoint);
        } else if (tObjectStoreType == TObjectStoreType.OSS) {
            String[] hostSplit = endPoint.split("\\.");
            String regionId = hostSplit[0];
            if (regionId.contains("-internal")) {
                return regionId.substring(0, regionId.length() - "-internal".length());
            } else {
                return regionId;
            }
        } else if (tObjectStoreType == TObjectStoreType.KS3) {
            String[] hostSplit = endPoint.split("\\.");
            String regionId = hostSplit[0];
            if (regionId.contains("-internal")) {
                return regionId.substring(4, regionId.length() - "-internal".length() - 4);
            } else {
                return regionId.substring(4);
            }
        } else if (tObjectStoreType == TObjectStoreType.TOS) {
            String[] hostSplit = endPoint.split("\\.");
            return hostSplit[0].replace("tos-s3-", "").replace("tos-", "");
        } else {
            String[] hostSplit = endPoint.split("\\.");
            return hostSplit[1];
        }
    }

    public void convertObjectStoreConfToProperties(String path, THdfsProperties tProperties, TObjectStoreType tObjectStoreType) {
        Properties props = this.getProps();
        Enumeration<String> enums = (Enumeration<String>) props.propertyNames();
        tProperties.setObject_store_type(tObjectStoreType);
        tProperties.setObject_store_path(path);
        while (enums.hasMoreElements()) {
            String key = enums.nextElement();
            String value = props.getProperty(key);
            switch (tObjectStoreType) {
                case UNIVERSAL_FS:
                case S3:
                    switch (key) {
                        case HdfsFsManager.FS_S3A_ACCESS_KEY:
                            tProperties.setAccess_key(value);
                            break;
                        case HdfsFsManager.FS_S3A_SECRET_KEY:
                            tProperties.setSecret_key(value);
                            break;
                        case HdfsFsManager.FS_S3A_ENDPOINT:
                            tProperties.setEnd_point(value);
                            tProperties.setRegion(parseRegionFromEndpoint(tObjectStoreType, value));
                            break;
                        case HdfsFsManager.FS_S3A_IMPL_DISABLE_CACHE:
                            tProperties.setDisable_cache(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_S3A_CONNECTION_SSL_ENABLED:
                            tProperties.setSsl_enable(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_S3A_MAX_CONNECTION:
                            tProperties.setMax_connection(Integer.parseInt(value));
                    }
                    break;
                case OSS:
                    switch (key) {
                        case HdfsFsManager.FS_OSS_ACCESS_KEY:
                            tProperties.setAccess_key(value);
                            break;
                        case HdfsFsManager.FS_OSS_SECRET_KEY:
                            tProperties.setSecret_key(value);
                            break;
                        case HdfsFsManager.FS_OSS_ENDPOINT:
                            tProperties.setEnd_point(value);
                            tProperties.setRegion(parseRegionFromEndpoint(tObjectStoreType, value));
                            break;
                        case HdfsFsManager.FS_OSS_IMPL_DISABLE_CACHE:
                            tProperties.setDisable_cache(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_OSS_CONNECTION_SSL_ENABLED:
                            tProperties.setSsl_enable(Boolean.parseBoolean(value));
                            break;
                    }
                    break;
                case COS:
                    switch (key) {
                        case HdfsFsManager.FS_COS_ACCESS_KEY:
                            tProperties.setAccess_key(value);
                            break;
                        case HdfsFsManager.FS_COS_SECRET_KEY:
                            tProperties.setSecret_key(value);
                            break;
                        case HdfsFsManager.FS_COS_ENDPOINT:
                            tProperties.setEnd_point(value);
                            tProperties.setRegion(parseRegionFromEndpoint(tObjectStoreType, value));
                            break;
                        case HdfsFsManager.FS_COS_IMPL_DISABLE_CACHE:
                            tProperties.setDisable_cache(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_COS_CONNECTION_SSL_ENABLED:
                            tProperties.setSsl_enable(Boolean.parseBoolean(value));
                            break;
                    }
                    break;
                case KS3:
                    switch (key) {
                        case HdfsFsManager.FS_KS3_ACCESS_KEY:
                            tProperties.setAccess_key(value);
                            break;
                        case HdfsFsManager.FS_KS3_SECRET_KEY:
                            tProperties.setSecret_key(value);
                            break;
                        case HdfsFsManager.FS_KS3_ENDPOINT:
                            tProperties.setEnd_point(value);
                            tProperties.setRegion(parseRegionFromEndpoint(tObjectStoreType, value));
                            break;
                        case HdfsFsManager.FS_KS3_IMPL_DISABLE_CACHE:
                            tProperties.setDisable_cache(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_KS3_CONNECTION_SSL_ENABLED:
                            tProperties.setSsl_enable(Boolean.parseBoolean(value));
                            break;
                    }
                    break;
                case OBS:
                    switch (key) {
                        case HdfsFsManager.FS_OBS_ACCESS_KEY:
                            tProperties.setAccess_key(value);
                            break;
                        case HdfsFsManager.FS_OBS_SECRET_KEY:
                            tProperties.setSecret_key(value);
                            break;
                        case HdfsFsManager.FS_OBS_ENDPOINT:
                            tProperties.setEnd_point(value);
                            tProperties.setRegion(parseRegionFromEndpoint(tObjectStoreType, value));
                            break;
                        case HdfsFsManager.FS_OBS_IMPL_DISABLE_CACHE:
                            tProperties.setDisable_cache(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_OBS_CONNECTION_SSL_ENABLED:
                            tProperties.setSsl_enable(Boolean.parseBoolean(value));
                            break;
                    }
                case TOS:
                    switch (key) {
                        case HdfsFsManager.FS_TOS_ACCESS_KEY:
                            tProperties.setAccess_key(value);
                            break;
                        case HdfsFsManager.FS_TOS_SECRET_KEY:
                            tProperties.setSecret_key(value);
                            break;
                        case HdfsFsManager.FS_TOS_ENDPOINT:
                            tProperties.setEnd_point(value);
                            tProperties.setRegion(parseRegionFromEndpoint(tObjectStoreType, value));
                            break;
                        case HdfsFsManager.FS_TOS_IMPL_DISABLE_CACHE:
                            tProperties.setDisable_cache(Boolean.parseBoolean(value));
                            break;
                        case HdfsFsManager.FS_TOS_CONNECTION_SSL_ENABLED:
                            tProperties.setSsl_enable(Boolean.parseBoolean(value));
                            break;
                    }
            }
        }
    }
}

class HDFSConfigurationWrap extends HdfsConfiguration {
    public HDFSConfigurationWrap() {
    }

    public void convertHDFSConfToProperties(THdfsProperties tProperties) {
        Properties props = this.getProps();
        Enumeration<String> enums = (Enumeration<String>) props.propertyNames();
        while (enums.hasMoreElements()) {
            String key = enums.nextElement();
            String value = props.getProperty(key);
            if (key.equals(HdfsFsManager.FS_HDFS_IMPL_DISABLE_CACHE)) {
                tProperties.setDisable_cache(Boolean.parseBoolean(value));
            }
        }
    }
}

public class HdfsFsManager {

    private static final Logger LOG = LogManager.getLogger(HdfsFsManager.class);

    // supported scheme
    private static final String HDFS_SCHEME = "hdfs";
    private static final String VIEWFS_SCHEME = "viewfs";
    private static final String S3_SCHEMA = "s3";
    private static final String S3A_SCHEME = "s3a";
    private static final String OSS_SCHEME = "oss";
    private static final String COS_SCHEME = "cosn";
    private static final String KS3_SCHEME = "ks3";
    private static final String OBS_SCHEME = "obs";
    private static final String TOS_SCHEME = "tos";

    private static final String ABFS_SCHEMA = "abfs";
    private static final String ABFSS_SCHEMA = "abfss";
    private static final String ADL_SCHEMA = "adl";
    private static final String WASB_SCHEMA = "wasb";
    private static final String WASBS_SCHEMA = "wasbs";
    private static final String GCS_SCHEMA = "gs";
    private static final String USER_NAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    // arguments for ha hdfs
    private static final String DFS_NAMESERVICES_KEY = "dfs.nameservices";
    private static final String FS_DEFAULTFS_KEY = "fs.defaultFS";
    protected static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    // If this property is not set to "true", FileSystem instance will be returned
    // from cache
    // which is not thread-safe and may cause 'Filesystem closed' exception when it
    // is closed by other thread.

    // arguments for s3a
    protected static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    protected static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    protected static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    protected static final String FS_S3A_IMPL_DISABLE_CACHE = "fs.s3a.impl.disable.cache";
    protected static final String FS_S3_IMPL_DISABLE_CACHE = "fs.s3.impl.disable.cache";
    protected static final String FS_S3A_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
    protected static final String FS_S3A_MAX_CONNECTION = "fs.s3a.connection.maximum";
    protected static final String FS_S3A_AWS_CRED_PROVIDER = "fs.s3a.aws.credentials.provider";

    // arguments for ks3
    protected static final String FS_KS3_ACCESS_KEY = "fs.ks3.AccessKey";
    protected static final String FS_KS3_SECRET_KEY = "fs.ks3.AccessSecret";
    protected static final String FS_KS3_ENDPOINT = "fs.ks3.endpoint";
    protected static final String FS_KS3_IMPL = "fs.ks3.impl";
    // This property is used like 'fs.ks3.impl.disable.cache'
    protected static final String FS_KS3_CONNECTION_SSL_ENABLED = "fs.ks3.connection.ssl.enabled";
    protected static final String FS_KS3_IMPL_DISABLE_CACHE = "fs.ks3.impl.disable.cache";

    // arguments for oss
    protected static final String FS_OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    protected static final String FS_OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    protected static final String FS_OSS_ENDPOINT = "fs.oss.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    protected static final String FS_OSS_IMPL_DISABLE_CACHE = "fs.oss.impl.disable.cache";
    protected static final String FS_OSS_CONNECTION_SSL_ENABLED = "fs.oss.connection.secure.enabled";
    protected static final String FS_OSS_IMPL = "fs.oss.impl";

    // arguments for cos
    protected static final String FS_COS_ACCESS_KEY = "fs.cosn.userinfo.secretId";
    protected static final String FS_COS_SECRET_KEY = "fs.cosn.userinfo.secretKey";
    protected static final String FS_COS_ENDPOINT = "fs.cosn.bucket.endpoint_suffix";
    protected static final String FS_COS_IMPL_DISABLE_CACHE = "fs.cosn.impl.disable.cache";
    protected static final String FS_COS_CONNECTION_SSL_ENABLED = "fs.cos.connection.ssl.enabled";
    protected static final String FS_COS_IMPL = "fs.cosn.impl";

    // arguments for obs
    protected static final String FS_OBS_ACCESS_KEY = "fs.obs.access.key";
    protected static final String FS_OBS_SECRET_KEY = "fs.obs.secret.key";
    protected static final String FS_OBS_ENDPOINT = "fs.obs.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    protected static final String FS_OBS_IMPL_DISABLE_CACHE = "fs.obs.impl.disable.cache";
    protected static final String FS_OBS_CONNECTION_SSL_ENABLED = "fs.obs.connection.ssl.enabled";
    protected static final String FS_OBS_IMPL = "fs.obs.impl";
    protected static final String FS_ABFS_IMPL_DISABLE_CACHE = "fs.abfs.impl.disable.cache";
    protected static final String FS_ABFSS_IMPL_DISABLE_CACHE = "fs.abfss.impl.disable.cache";
    protected static final String FS_ADL_IMPL_DISABLE_CACHE = "fs.adl.impl.disable.cache";
    protected static final String FS_WASB_IMPL_DISABLE_CACHE = "fs.wasb.impl.disable.cache";
    protected static final String FS_WASBS_IMPL_DISABLE_CACHE = "fs.wasbs.impl.disable.cache";
    protected static final String FS_GS_IMPL_DISABLE_CACHE = "fs.gs.impl.disable.cache";

    // arguments for tos
    protected static final String FS_TOS_ACCESS_KEY = "fs.tos.access.key";
    protected static final String FS_TOS_SECRET_KEY = "fs.tos.secret.key";
    protected static final String FS_TOS_ENDPOINT = "fs.tos.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    protected static final String FS_TOS_IMPL_DISABLE_CACHE = "fs.tos.impl.disable.cache";
    protected static final String FS_TOS_CONNECTION_SSL_ENABLED = "fs.tos.connection.ssl.enabled";
    protected static final String FS_TOS_IMPL = "fs.tos.impl";
    protected static final String FS_TOS_REGION = "fs.tos.region";

    private final ScheduledExecutorService handleManagementPool = Executors.newScheduledThreadPool(1);

    private int readBufferSize = 128 << 10; // 128k
    private int writeBufferSize = 128 << 10; // 128k

    private final ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cachedFileSystem;
    private final HdfsFsStreamManager ioStreamManager;

    public HdfsFsManager() {
        cachedFileSystem = new ConcurrentHashMap<>();
        ioStreamManager = new HdfsFsStreamManager();
        readBufferSize = Config.hdfs_read_buffer_size_kb << 10;
        writeBufferSize = Config.hdfs_write_buffer_size_kb << 10;
        handleManagementPool.schedule(new FileSystemExpirationChecker(), 0, TimeUnit.SECONDS);
    }

    private static void convertHDFSConfToProperties(Configuration conf, THdfsProperties tProperties) {
        ((HDFSConfigurationWrap) conf).convertHDFSConfToProperties(tProperties);
    }

    private static void convertObjectStoreConfToProperties(String path, Configuration conf, THdfsProperties tProperties,
                                                           TObjectStoreType tObjectStoreType) {
        ((ConfigurationWrap) conf).convertObjectStoreConfToProperties(path, tProperties, tObjectStoreType);
    }

    /**
     * visible for test
     *
     * @return BrokerFileSystem with different FileSystem based on scheme
     */
    public HdfsFs getFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        String scheme = pathUri.getUri().getScheme();
        if (Strings.isNullOrEmpty(scheme)) {
            throw new UserException("invalid path. scheme is null");
        }
        switch (scheme) {
            case HDFS_SCHEME:
            case VIEWFS_SCHEME:
                return getDistributedFileSystem(scheme, path, loadProperties, tProperties);
            case S3A_SCHEME:
                return getS3AFileSystem(path, loadProperties, tProperties);
            case S3_SCHEMA:
                return getS3FileSystem(path, loadProperties, tProperties);
            case OSS_SCHEME:
                return getOSSFileSystem(path, loadProperties, tProperties);
            case COS_SCHEME:
                return getCOSFileSystem(path, loadProperties, tProperties);
            case KS3_SCHEME:
                return getKS3FileSystem(path, loadProperties, tProperties);
            case OBS_SCHEME:
                return getOBSFileSystem(path, loadProperties, tProperties);
            case TOS_SCHEME:
                return getTOSFileSystem(path, loadProperties, tProperties);
            case ABFS_SCHEMA:
            case ABFSS_SCHEMA:
            case ADL_SCHEMA:
            case WASB_SCHEMA:
            case WASBS_SCHEMA:
                return getAzureFileSystem(path, loadProperties, tProperties);
            case GCS_SCHEMA:
                return getGoogleFileSystem(path, loadProperties, tProperties);
            default:
                // If all above match fails, then we will read the settings from hdfs-site.xml, core-site.xml of FE,
                // and try to create a universal file system. The reason why we can do this is because hadoop/s3
                // SDK is compatible with nearly all file/object storage system
                return getUniversalFileSystem(path, loadProperties, tProperties);
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is host + username_password
     * it will have safety problem if only hostname is used because one user may
     * specify username and password
     * and then access hdfs, another user may not specify username and password but
     * could also access data
     * <p>
     * Configs related to viewfs in core-site.xml and hdfs-site.xml should be copied
     * to the broker conf directory.
     */
    public HdfsFs getDistributedFileSystem(String scheme, String path, Map<String, String> loadProperties,
                                           THdfsProperties tProperties) throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        String host = scheme + "://" + pathUri.getAuthority();
        if (Strings.isNullOrEmpty(pathUri.getAuthority())) {
            if (loadProperties.containsKey(FS_DEFAULTFS_KEY)) {
                host = loadProperties.get(FS_DEFAULTFS_KEY);
                LOG.info("no schema and authority in path. use fs.defaultFs");
            } else {
                LOG.warn("invalid hdfs path. authority is null,path:" + path);
                throw new UserException("invalid hdfs path. authority is null");
            }
        }
        String username = loadProperties.getOrDefault(USER_NAME_KEY, "");
        String password = loadProperties.getOrDefault(PASSWORD_KEY, "");
        String dfsNameServices = loadProperties.getOrDefault(DFS_NAMESERVICES_KEY, "");
        String authentication = loadProperties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                "");
        String disableCache = loadProperties.getOrDefault(FS_HDFS_IMPL_DISABLE_CACHE, "true");
        String disableCacheLowerCase = disableCache.toLowerCase();
        if (!(disableCacheLowerCase.equals("true") || disableCacheLowerCase.equals("false"))) {
            LOG.warn("invalid disable cache: " + disableCache);
            throw new UserException("invalid disable cache: " + disableCache);
        }
        if (!dfsNameServices.equals("")) {
            LOG.warn("Invalid load_properties, namenode HA should be set in hdfs/core-site.xml for" +
                    "broker load without broke. For broker load with broker, you can set namenode HA in the load_properties");
            throw new UserException("invalid load_properties, namenode HA should be set in hdfs/core-site.xml" +
                    "for load without broker. For broker load with broker, you can set namenode HA in the load_properties");
        }

        if (!authentication.equals("") && !authentication.equals("simple")) {
            LOG.warn("Invalid load_properties, kerberos should be set in hdfs/core-site.xml for broker " +
                    "load without broker. For broker load with broker, you can set namenode HA in the load_properties");
            throw new UserException("invalid load_properties, kerberos should be set in hdfs/core-site.xml " +
                    "for load without broker. For broker load with broker, you can set namenode HA in the load_properties");
        }

        String hdfsUgi = username + "," + password;
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, hdfsUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new HDFSConfigurationWrap();
                conf.set(FS_HDFS_IMPL_DISABLE_CACHE, disableCache);
                UserGroupInformation ugi = null;
                if (!Strings.isNullOrEmpty(username) && conf.get("hadoop.security.authentication").equals("simple")) {
                    ugi = UserGroupInformation.createRemoteUser(username);
                }
                FileSystem dfsFileSystem = null;
                if (ugi != null) {
                    dfsFileSystem = ugi.doAs(
                            (PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(pathUri.getUri(), conf));
                } else {
                    dfsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                }
                fileSystem.setFileSystem(dfsFileSystem);
                fileSystem.setConfiguration(conf);
                if (ugi != null) {
                    fileSystem.setUserName(username);
                }
                if (tProperties != null) {
                    convertHDFSConfToProperties(conf, tProperties);
                    if (ugi != null) {
                        tProperties.setHdfs_username(username);
                    }
                }
            } else {
                if (tProperties != null) {
                    convertHDFSConfToProperties(fileSystem.getConfiguration(), tProperties);
                    if (fileSystem.getUserName() != null) {
                        tProperties.setHdfs_username(fileSystem.getUserName());
                    }
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e.getMessage());
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket +
     * accessKey_secretKey
     */
    public HdfsFs getS3AFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        // If we don't set new authenticate parameters, we use original way (just for compatible)
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
        }

        WildcardURI pathUri = new WildcardURI(path);

        String accessKey = loadProperties.getOrDefault(FS_S3A_ACCESS_KEY, "");
        String secretKey = loadProperties.getOrDefault(FS_S3A_SECRET_KEY, "");
        String endpoint = loadProperties.getOrDefault(FS_S3A_ENDPOINT, "");
        String disableCache = loadProperties.getOrDefault(FS_S3A_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = loadProperties.getOrDefault(FS_S3A_CONNECTION_SSL_ENABLED, "false");
        String awsCredProvider = loadProperties.getOrDefault(FS_S3A_AWS_CRED_PROVIDER, null);

        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will
        // cache both.
        String host = S3A_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String s3aUgi = accessKey + "," + secretKey;
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, s3aUgi);

        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                if (!accessKey.isEmpty()) {
                    conf.set(FS_S3A_ACCESS_KEY, accessKey);
                }
                if (!secretKey.isEmpty()) {
                    conf.set(FS_S3A_SECRET_KEY, secretKey);
                }
                if (!endpoint.isEmpty()) {
                    conf.set(FS_S3A_ENDPOINT, endpoint);
                }
                // Only set ssl for origin logic
                conf.set(FS_S3A_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                if (awsCredProvider != null) {
                    conf.set(FS_S3A_AWS_CRED_PROVIDER, awsCredProvider);
                }

                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCache);
                FileSystem s3AFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(s3AFileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.S3);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties,
                            TObjectStoreType.S3);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    public HdfsFs getS3FileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
    }

    /**
     * Support for Azure Storage File System
     * Support abfs://, abfs://, adl://, wasb://, wasbs://
     */
    public HdfsFs getAzureFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        // Put path into fileProperties, so that we can get storage account in AzureStorageCloudConfiguration
        loadProperties.put(AzureCloudConfigurationProvider.AZURE_PATH_KEY, path);

        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
    }

    /**
     * Support for Google Cloud Storage File System
     * Support gs://
     */
    public HdfsFs getGoogleFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
    }

    /**
     * This function create FileSystem by CloudConfiguration
     * Support s3://, s3a://, abfs://, abfss://, adl://, wasb://, wasbs://, gs://, oss://, obs://, cosn://,
     * tos://, ks3://
     */
    private HdfsFs getFileSystemByCloudConfiguration(CloudConfiguration cloudConfiguration, String path,
                                                     THdfsProperties tProperties)
            throws UserException {
        Preconditions.checkArgument(cloudConfiguration != null);
        WildcardURI pathUri = new WildcardURI(path);

        String host = pathUri.getUri().getScheme() + "://" + pathUri.getUri().getHost();
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, cloudConfiguration.toConfString());

        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                cloudConfiguration.applyToConfiguration(conf);

                // Disable cache for AWS
                conf.set(FS_S3_IMPL_DISABLE_CACHE, "true");
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, "true");
                // Disable cache for Azure
                conf.set(FS_ABFS_IMPL_DISABLE_CACHE, "true");
                conf.set(FS_ABFSS_IMPL_DISABLE_CACHE, "true");
                conf.set(FS_ADL_IMPL_DISABLE_CACHE, "true");
                conf.set(FS_WASB_IMPL_DISABLE_CACHE, "true");
                conf.set(FS_WASBS_IMPL_DISABLE_CACHE, "true");
                // Disable cache for GCS
                conf.set(FS_GS_IMPL_DISABLE_CACHE, "true");
                // Disable cache for OSS
                conf.set(FS_OSS_IMPL_DISABLE_CACHE, "true");
                // Disable cache for OBS
                conf.set(FS_OBS_IMPL_DISABLE_CACHE, "true");
                // Disable cache for COS
                conf.set(FS_COS_IMPL_DISABLE_CACHE, "true");
                // Disable cache for TOS
                conf.set(FS_TOS_IMPL_DISABLE_CACHE, "true");
                // Disable cache for KS3
                conf.set(FS_KS3_IMPL_DISABLE_CACHE, "true");

                FileSystem innerFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(innerFileSystem);
                fileSystem.setConfiguration(conf);
            }
            if (tProperties != null) {
                TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
                cloudConfiguration.toThrift(tCloudConfiguration);
                tProperties.setCloud_configuration(tCloudConfiguration);
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket +
     * accessKey_secretKey
     */
    public HdfsFs getKS3FileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        // If we don't set new authenticate parameters, we use original way (just for compatible)
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
        }
        // TODO(SmithCruise) Maybe we can delete below code, because KS3 is never support officially
        // We can remove it relevant jars, using S3AFileSystem instead, it can save for 15MB space.
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = loadProperties.getOrDefault(FS_KS3_ACCESS_KEY, "");
        String secretKey = loadProperties.getOrDefault(FS_KS3_SECRET_KEY, "");
        String endpoint = loadProperties.getOrDefault(FS_KS3_ENDPOINT, "");
        String disableCache = loadProperties.getOrDefault(FS_KS3_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = loadProperties.getOrDefault(FS_KS3_CONNECTION_SSL_ENABLED, "false");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will
        // cache both.
        String host = KS3_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String ks3aUgi = accessKey + "," + secretKey;
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, ks3aUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                if (!accessKey.isEmpty()) {
                    conf.set(FS_KS3_ACCESS_KEY, accessKey);
                }
                if (!secretKey.isEmpty()) {
                    conf.set(FS_KS3_SECRET_KEY, secretKey);
                }
                if (!endpoint.isEmpty()) {
                    conf.set(FS_KS3_ENDPOINT, endpoint);
                }
                conf.set(FS_KS3_IMPL, "com.ksyun.kmr.hadoop.fs.ks3.Ks3FileSystem");
                conf.set(FS_KS3_IMPL_DISABLE_CACHE, disableCache);
                conf.set(FS_KS3_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                FileSystem ks3FileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(ks3FileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.KS3);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties, TObjectStoreType.KS3);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket +
     * accessKey_secretKey
     */
    public HdfsFs getOBSFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        // If we don't set new authenticate parameters, we use original way (just for compatible)
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
        }
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = loadProperties.getOrDefault(FS_OBS_ACCESS_KEY, "");
        String secretKey = loadProperties.getOrDefault(FS_OBS_SECRET_KEY, "");
        String endpoint = loadProperties.getOrDefault(FS_OBS_ENDPOINT, "");
        String disableCache = loadProperties.getOrDefault(FS_OBS_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = loadProperties.getOrDefault(FS_OBS_CONNECTION_SSL_ENABLED, "false");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will
        // cache both.
        String host = OBS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String obsUgi = accessKey + "," + secretKey;

        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, obsUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);

        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }

            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                if (!accessKey.isEmpty()) {
                    conf.set(FS_OBS_ACCESS_KEY, accessKey);
                }
                if (!secretKey.isEmpty()) {
                    conf.set(FS_OBS_SECRET_KEY, secretKey);
                }
                if (!endpoint.isEmpty()) {
                    conf.set(FS_OBS_ENDPOINT, endpoint);
                }
                conf.set(FS_OBS_IMPL, "org.apache.hadoop.fs.obs.OBSFileSystem");
                conf.set(FS_OBS_IMPL_DISABLE_CACHE, disableCache);
                conf.set(FS_OBS_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                FileSystem obsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(obsFileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.OBS);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties, TObjectStoreType.OBS);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     */
    public HdfsFs getUniversalFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {

        String disableCacheHDFS = loadProperties.getOrDefault(FS_HDFS_IMPL_DISABLE_CACHE, "true");
        String disableCacheHDFSLowerCase = disableCacheHDFS.toLowerCase();
        if (!(disableCacheHDFSLowerCase.equals("true") || disableCacheHDFSLowerCase.equals("false"))) {
            LOG.warn("invalid disable cache: " + disableCacheHDFS);
            throw new UserException("invalid disable cache: " + disableCacheHDFS);
        }
        String disableCacheS3 = loadProperties.getOrDefault(FS_HDFS_IMPL_DISABLE_CACHE, "true");
        String disableCacheS3LowerCase = disableCacheS3.toLowerCase();
        if (!(disableCacheS3LowerCase.equals("true") || disableCacheS3LowerCase.equals("false"))) {
            LOG.warn("invalid disable cache: " + disableCacheS3);
            throw new UserException("invalid disable cache: " + disableCacheS3);
        }

        // skip xxx:// first
        int bucketEndIndex = path.indexOf("://");

        // find the end of bucket, for example for xxx://abc/def, we will take xxx://abc as host
        if (bucketEndIndex != -1) {
            bucketEndIndex = path.indexOf("/", bucketEndIndex + 3);
        }
        String host = path;
        if (bucketEndIndex != -1) {
            host = path.substring(0, bucketEndIndex);
        }

        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, "");
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);

        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }

            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCacheHDFS);
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCacheS3);
                FileSystem genericFileSystem = FileSystem.get(new Path(path).toUri(), conf);
                fileSystem.setFileSystem(genericFileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.UNIVERSAL_FS);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties,
                            TObjectStoreType.UNIVERSAL_FS);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket +
     * accessKey_secretKey
     */
    public HdfsFs getOSSFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        // If we don't set new authenticate parameters, we use original way (just for compatible)
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
        }

        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = loadProperties.getOrDefault(FS_OSS_ACCESS_KEY, "");
        String secretKey = loadProperties.getOrDefault(FS_OSS_SECRET_KEY, "");
        String endpoint = loadProperties.getOrDefault(FS_OSS_ENDPOINT, "");
        String disableCache = loadProperties.getOrDefault(FS_OSS_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = loadProperties.getOrDefault(FS_OSS_CONNECTION_SSL_ENABLED, "false");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will
        // cache both.
        String host = OSS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String ossUgi = accessKey + "," + secretKey;
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, ossUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                if (!accessKey.isEmpty()) {
                    conf.set(FS_OSS_ACCESS_KEY, accessKey);
                }

                if (!secretKey.isEmpty()) {
                    conf.set(FS_OSS_SECRET_KEY, secretKey);
                }

                if (!endpoint.isEmpty()) {
                    conf.set(FS_OSS_ENDPOINT, endpoint);
                }
                conf.set(FS_OSS_IMPL, "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
                conf.set(FS_OSS_IMPL_DISABLE_CACHE, disableCache);
                conf.set(FS_OSS_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                FileSystem ossFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(ossFileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.OSS);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties, TObjectStoreType.OSS);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * file system handle is cached, the identity is endpoint + bucket +
     * accessKey_secretKey
     * for cos
     */
    public HdfsFs getCOSFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        // If we don't set new authenticate parameters, we use original way (just for compatible)
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
        }

        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = loadProperties.getOrDefault(FS_COS_ACCESS_KEY, "");
        String secretKey = loadProperties.getOrDefault(FS_COS_SECRET_KEY, "");
        String endpoint = loadProperties.getOrDefault(FS_COS_ENDPOINT, "");
        String disableCache = loadProperties.getOrDefault(FS_COS_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = loadProperties.getOrDefault(FS_COS_CONNECTION_SSL_ENABLED, "false");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will
        // cache both.
        String host = COS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String cosUgi = accessKey + "," + secretKey;
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, cosUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                if (!accessKey.isEmpty()) {
                    conf.set(FS_COS_ACCESS_KEY, accessKey);
                }
                if (!secretKey.isEmpty()) {
                    conf.set(FS_COS_SECRET_KEY, secretKey);
                }
                if (!endpoint.isEmpty()) {
                    conf.set(FS_COS_ENDPOINT, endpoint);
                }

                conf.set(FS_COS_IMPL, "org.apache.hadoop.fs.CosFileSystem");
                conf.set(FS_COS_IMPL_DISABLE_CACHE, disableCache);
                conf.set(FS_COS_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                FileSystem cosFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(cosFileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.COS);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties, TObjectStoreType.COS);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * file system handle is cached, the identity is endpoint + bucket +
     * accessKey secretKey
     * for tos
     */
    public HdfsFs getTOSFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(loadProperties);
        // If we don't set new authenticate parameters, we use original way (just for compatible)
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return getFileSystemByCloudConfiguration(cloudConfiguration, path, tProperties);
        }
        // TODO(SmithCruise) Maybe we can delete below code, because TOS is never support officially
        // We can remove it relevant jars, using S3AFileSystem instead.
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = loadProperties.getOrDefault(FS_TOS_ACCESS_KEY, "");
        String secretKey = loadProperties.getOrDefault(FS_TOS_SECRET_KEY, "");
        String endpoint = loadProperties.getOrDefault(FS_TOS_ENDPOINT, "");
        String disableCache = loadProperties.getOrDefault(FS_TOS_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = loadProperties.getOrDefault(FS_TOS_CONNECTION_SSL_ENABLED, "false");
        String region = loadProperties.getOrDefault(FS_TOS_REGION, "");
        if (accessKey.equals("")) {
            LOG.warn("Invalid load_properties, TOS must provide access_key");
            throw new UserException("Invalid load_properties, TOS must provide access_key");
        }
        if (secretKey.equals("")) {
            LOG.warn("Invalid load_properties, TOS must provide secret_key");
            throw new UserException("Invalid load_properties, TOS must provide secret_key");
        }
        if (endpoint.equals("")) {
            LOG.warn("Invalid load_properties, TOS must provide endpoint");
            throw new UserException("Invalid load_properties, TOS must provide endpoint");
        }
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will
        // cache both.
        String host = TOS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String tosUgi = accessKey + "," + secretKey;
        HdfsFsIdentity fileSystemIdentity = new HdfsFsIdentity(host, tosUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new HdfsFs(fileSystemIdentity));
        HdfsFs fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                LOG.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new ConfigurationWrap();
                conf.set(FS_TOS_ACCESS_KEY, accessKey);
                conf.set(FS_TOS_SECRET_KEY, secretKey);
                conf.set(FS_TOS_ENDPOINT, endpoint);
                conf.set(FS_TOS_IMPL, "com.volcengine.cloudfs.fs.TosFileSystem");
                conf.set(FS_TOS_IMPL_DISABLE_CACHE, disableCache);
                conf.set(FS_TOS_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                conf.set(FS_S3A_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                conf.set(FS_TOS_REGION, region);
                FileSystem tosFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(tosFileSystem);
                fileSystem.setConfiguration(conf);
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, conf, tProperties, TObjectStoreType.TOS);
                }
            } else {
                if (tProperties != null) {
                    convertObjectStoreConfToProperties(path, fileSystem.getConfiguration(), tProperties, TObjectStoreType.TOS);
                }
            }
            return fileSystem;
        } catch (Exception e) {
            LOG.error("errors while connect to " + path, e);
            throw new UserException(e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }
    
    public void getTProperties(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
            throws UserException {
        getFileSystem(path, loadProperties, tProperties);
    }

<<<<<<< HEAD
=======
    public List<FileStatus> listFileMeta(String path, Map<String, String> properties) throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        HdfsFs fileSystem = getFileSystem(path, properties, null);
        Path pathPattern = new Path(pathUri.getPath());
        try {
            FileStatus[] files = fileSystem.getDFSFileSystem().globStatus(pathPattern);
            return Lists.newArrayList(files);
        } catch (FileNotFoundException e) {
            LOG.info("file not found: " + path, e);
            throw new UserException("file not found: " + path, e);
        } catch (Exception e) {
            LOG.error("errors while get file status ", e);
            throw new UserException("Fail to get file status: " + e.getMessage(), e);
        }
    }

>>>>>>> c1259dee3a ([Enhancement] Refine error prompt (#34322))
    public List<TBrokerFileStatus> listPath(String path, boolean fileNameOnly, Map<String, String> loadProperties)
            throws UserException {
        List<TBrokerFileStatus> resultFileStatus = null;
        WildcardURI pathUri = new WildcardURI(path);
        HdfsFs fileSystem = getFileSystem(path, loadProperties, null);
        Path pathPattern = new Path(pathUri.getPath());
        try {
            FileStatus[] files = fileSystem.getDFSFileSystem().globStatus(pathPattern);
            if (files == null) {
                resultFileStatus = new ArrayList<>(0);
                return resultFileStatus;
            }
            resultFileStatus = new ArrayList<>(files.length);
            for (FileStatus fileStatus : files) {
                TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus();
                brokerFileStatus.setIsDir(fileStatus.isDirectory());
                if (fileStatus.isDirectory()) {
                    brokerFileStatus.setIsSplitable(false);
                    brokerFileStatus.setSize(-1);
                } else {
                    brokerFileStatus.setSize(fileStatus.getLen());
                    brokerFileStatus.setIsSplitable(true);
                }
                if (fileNameOnly) {
                    // return like this: file.txt
                    brokerFileStatus.setPath(fileStatus.getPath().getName());
                } else {
                    // return like this: //path/to/your/file.txt
                    brokerFileStatus.setPath(fileStatus.getPath().toString());
                }
                resultFileStatus.add(brokerFileStatus);
            }
        } catch (FileNotFoundException e) {
            LOG.info("file not found: " + path, e);
            throw new UserException("file not found: " + path, e);
        } catch (IllegalArgumentException e) {
            LOG.error("The arguments of blob store(S3/Azure) may be wrong. You can check " +
                    "the arguments like region, IAM, instance profile and so on.");
            throw new UserException("The arguments of blob store(S3/Azure) may be wrong. " +
                    "You can check the arguments like region, IAM, instance profile and so on.", e);
        } catch (Exception e) {
            LOG.error("errors while get file status ", e);
            throw new UserException("Fail to get file status: " + e.getMessage(), e);
        }
        return resultFileStatus;
    }

    public void deletePath(String path, Map<String, String> loadProperties) throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        HdfsFs fileSystem = getFileSystem(path, loadProperties, null);
        Path filePath = new Path(pathUri.getPath());
        try {
            fileSystem.getDFSFileSystem().delete(filePath, true);
        } catch (IOException e) {
            LOG.error("errors while delete path " + path);
            throw new UserException("delete path " + path + "error");
        }
    }

    public void renamePath(String srcPath, String destPath, Map<String, String> loadProperties) throws UserException {
        WildcardURI srcPathUri = new WildcardURI(srcPath);
        WildcardURI destPathUri = new WildcardURI(destPath);

        boolean srcAuthorityNull = (srcPathUri.getAuthority() == null);
        boolean destAuthorityNull = (destPathUri.getAuthority() == null);
        if (srcAuthorityNull != destAuthorityNull) {
            throw new UserException("Different authority info between srcPath: " + srcPath + " and destPath: " + destPath);
        }
        if (!srcAuthorityNull && !destAuthorityNull &&
                !srcPathUri.getAuthority().trim().equals(destPathUri.getAuthority().trim())) {
            throw new UserException("only allow rename in same file system");

        }

        HdfsFs fileSystem = getFileSystem(srcPath, loadProperties, null);
        Path srcfilePath = new Path(srcPathUri.getPath());
        Path destfilePath = new Path(destPathUri.getPath());
        try {
            boolean isRenameSuccess = fileSystem.getDFSFileSystem().rename(srcfilePath, destfilePath);
            if (!isRenameSuccess) {
                throw new UserException("failed to rename path from " + srcPath + " to " + destPath);
            }
        } catch (IOException e) {
            LOG.error("errors while rename path from " + srcPath + " to " + destPath);
            throw new UserException("errors while rename " + srcPath + "to " + destPath);
        }
    }

    public boolean checkPathExist(String path, Map<String, String> loadProperties) throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        HdfsFs fileSystem = getFileSystem(path, loadProperties, null);
        Path filePath = new Path(pathUri.getPath());
        try {
            return fileSystem.getDFSFileSystem().exists(filePath);
        } catch (IOException e) {
            LOG.error("errors while check path exist: " + path);
            throw new UserException("errors while check if path " + path + " exist");
        }
    }

    public TBrokerFD openReader(String path, long startOffset, Map<String, String> loadProperties) throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        Path inputFilePath = new Path(pathUri.getPath());
        HdfsFs fileSystem = getFileSystem(path, loadProperties, null);
        try {
            FSDataInputStream fsDataInputStream = fileSystem.getDFSFileSystem().open(inputFilePath, readBufferSize);
            fsDataInputStream.seek(startOffset);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            ioStreamManager.putNewInputStream(fd, fsDataInputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            LOG.error("errors while open path", e);
            throw new UserException("could not open file " + path);
        }
    }

    public byte[] pread(TBrokerFD fd, long offset, long length) throws UserException {
        FSDataInputStream fsDataInputStream = ioStreamManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataInputStream.getPos();
            } catch (IOException e) {
                LOG.error("errors while get file pos from output stream", e);
                throw new UserException("errors while get file pos from output stream");
            }
            if (currentStreamOffset != offset) {
                // it's ok, when reading some format like parquet, it is not a sequential read
                LOG.debug("invalid offset, current read offset is "
                        + currentStreamOffset + " is not equal to request offset "
                        + offset + " seek to it");
                try {
                    fsDataInputStream.seek(offset);
                } catch (IOException e) {
                    throw new UserException("current read offset " + currentStreamOffset + " is not equal to "
                            + offset + ", and could not seek to it");
                }
            }
            byte[] buf;
            if (length > readBufferSize) {
                buf = new byte[readBufferSize];
            } else {
                buf = new byte[(int) length];
            }
            try {
                int readLength = readByteArrayFully(fsDataInputStream, buf);
                if (readLength < 0) {
                    throw new UserException("end of file reached");
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "read buffer from input stream, buffer size:" + buf.length + ", read length:" + readLength);
                }
                if (readLength == readBufferSize) {
                    return buf;
                } else {
                    byte[] smallerBuf = new byte[readLength];
                    System.arraycopy(buf, 0, smallerBuf, 0, readLength);
                    return smallerBuf;
                }
            } catch (IOException e) {
                LOG.error("errors while read data from stream", e);
                throw new UserException("errors while read data from stream");
            }
        }
    }

    public void seek(TBrokerFD fd, long offset) throws NotImplementedException {
        throw new NotImplementedException("seek this method is not supported");
    }

    public void closeReader(TBrokerFD fd) throws UserException {
        FSDataInputStream fsDataInputStream = ioStreamManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                LOG.error("errors while close file input stream", e);
                throw new UserException("errors while close file input stream");
            } finally {
                ioStreamManager.removeInputStream(fd);
            }
        }
    }

    public TBrokerFD openWriter(String path, Map<String, String> loadProperties) throws UserException {
        WildcardURI pathUri = new WildcardURI(path);
        Path inputFilePath = new Path(pathUri.getPath());
        HdfsFs fileSystem = getFileSystem(path, loadProperties, null);
        try {
            FSDataOutputStream fsDataOutputStream = fileSystem.getDFSFileSystem().create(inputFilePath,
                    true, writeBufferSize);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            LOG.info("finish a open writer request. fd: " + fd);
            ioStreamManager.putNewOutputStream(fd, fsDataOutputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            LOG.error("errors while open path", e);
            throw new UserException("could not open file " + path);
        }
    }

    public void pwrite(TBrokerFD fd, long offset, byte[] data) throws UserException {
        FSDataOutputStream fsDataOutputStream = ioStreamManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            long currentStreamOffset = fsDataOutputStream.getPos();
            if (currentStreamOffset != offset) {
                throw new UserException("current outputstream offset is " + currentStreamOffset
                        + " not equal to request " + offset);
            }
            try {
                fsDataOutputStream.write(data);
            } catch (IOException e) {
                LOG.error("errors while write file " + fd + " to output stream", e);
                throw new UserException("errors while write data to output stream");
            }
        }
    }

    public void closeWriter(TBrokerFD fd) throws UserException {
        FSDataOutputStream fsDataOutputStream = ioStreamManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            try {
                fsDataOutputStream.hsync();
                fsDataOutputStream.close();
            } catch (IOException e) {
                LOG.error("errors while close file " + fd + " output stream", e);
                throw new UserException("errors while close file output stream");
            } finally {
                ioStreamManager.removeOutputStream(fd);
            }
        }
    }

    private static TBrokerFD parseUUIDToFD(UUID uuid) {
        return new TBrokerFD(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private int readByteArrayFully(FSDataInputStream is, byte[] dest) throws IOException {
        int readLength = 0;
        while (readLength < dest.length) {
            int n = is.read(dest, readLength, dest.length - readLength);
            if (n <= 0) {
                break;
            }
            readLength += n;
        }
        return readLength;
    }

    class FileSystemExpirationChecker implements Runnable {
        @Override
        public void run() {
            try {
                for (HdfsFs fileSystem : cachedFileSystem.values()) {
                    if (fileSystem.isExpired(Config.hdfs_file_system_expire_seconds)) {
                        LOG.info("file system " + fileSystem + " is expired, close and remove it");
                        fileSystem.getLock().lock();
                        try {
                            fileSystem.closeFileSystem();
                        } catch (Throwable t) {
                            LOG.error("errors while close file system", t);
                        } finally {
                            cachedFileSystem.remove(fileSystem.getIdentity());
                            fileSystem.getLock().unlock();
                        }
                    }
                }
            } finally {
                HdfsFsManager.this.handleManagementPool.schedule(this, 60, TimeUnit.SECONDS);
            }
        }

    }
}
