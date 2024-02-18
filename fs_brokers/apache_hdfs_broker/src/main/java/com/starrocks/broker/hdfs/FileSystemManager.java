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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master
//                     /fs_brokers/apache_hdfs_broker/src/main/java
//                     /org/apache/doris/broker/hdfs/FileSystemManager.java

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

package com.starrocks.broker.hdfs;

import com.google.common.collect.Sets;
import com.starrocks.common.WildcardURI;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerOperationStatusCode;

import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileSystemManager {

    private static Logger logger = Logger
            .getLogger(FileSystemManager.class.getName());
    // supported scheme
    private static final String HDFS_SCHEME = "hdfs";
    private static final String VIEWFS_SCHEME = "viewfs";
    private static final String S3A_SCHEME = "s3a";
    private static final String OSS_SCHEME = "oss";
    private static final String COS_SCHEME = "cosn";
    private static final String KS3_SCHEME = "ks3";
    private static final String OBS_SCHEME = "obs";
    private static final String TOS_SCHEME = "tos";

    private static final String USER_NAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String AUTHENTICATION_SIMPLE = "simple";
    private static final String AUTHENTICATION_KERBEROS = "kerberos";
    private static final String KERBEROS_PRINCIPAL = "kerberos_principal";
    private static final String KERBEROS_KEYTAB = "kerberos_keytab";
    private static final String KERBEROS_KEYTAB_CONTENT = "kerberos_keytab_content";
    private static final String DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN =
            "dfs.namenode.kerberos.principal.pattern";
    // arguments for ha hdfs
    private static final String DFS_NAMESERVICES_KEY = "dfs.nameservices";
    private static final String DFS_HA_NAMENODES_PREFIX = "dfs.ha.namenodes.";
    private static final String DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX = "dfs.namenode.rpc-address.";
    private static final String DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX =
            "dfs.client.failover.proxy.provider.";
    private static final String DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER =
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
    private static final String FS_DEFAULTFS_KEY = "fs.defaultFS";
    // If this property is not set to "true", FileSystem instance will be returned from cache
    // which is not thread-safe and may cause 'Filesystem closed' exception when it is closed by other thread.
    private static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";

    // https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
    // arguments for s3a
    private static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    private static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    private static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    private static final String FS_S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    private static final String FS_S3A_IMPL_DISABLE_CACHE = "fs.s3a.impl.disable.cache";
    private static final String FS_S3A_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
    private static final String FS_S3A_AWS_CRED_PROVIDER = "fs.s3a.aws.credentials.provider";

    // arguments for ks3
    private static final String FS_KS3_ACCESS_KEY = "fs.ks3.AccessKey";
    private static final String FS_KS3_SECRET_KEY = "fs.ks3.AccessSecret";
    private static final String FS_KS3_ENDPOINT = "fs.ks3.endpoint";
    private static final String FS_KS3_IMPL = "fs.ks3.impl";
    // This property is used like 'fs.ks3.impl.disable.cache'
    private static final String FS_KS3_IMPL_DISABLE_CACHE = "fs.ks3.impl.disable.cache";

    //arguments for oss
    private static final String FS_OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    private static final String FS_OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    private static final String FS_OSS_ENDPOINT = "fs.oss.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    private static final String FS_OSS_IMPL_DISABLE_CACHE = "fs.oss.impl.disable.cache";
    private static final String FS_OSS_IMPL = "fs.oss.impl";

    // arguments for cos
    private static final String FS_COS_ACCESS_KEY = "fs.cosn.userinfo.secretId";
    private static final String FS_COS_SECRET_KEY = "fs.cosn.userinfo.secretKey";
    private static final String FS_COS_ENDPOINT = "fs.cosn.bucket.endpoint_suffix";
    private static final String FS_COS_IMPL_DISABLE_CACHE = "fs.cosn.impl.disable.cache";
    private static final String FS_COS_IMPL = "fs.cosn.impl";

    // arguments for obs
    private static final String FS_OBS_ACCESS_KEY = "fs.obs.access.key";
    private static final String FS_OBS_SECRET_KEY = "fs.obs.secret.key";
    private static final String FS_OBS_ENDPOINT = "fs.obs.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    private static final String FS_OBS_IMPL_DISABLE_CACHE = "fs.obs.impl.disable.cache";
    private static final String FS_OBS_IMPL = "fs.obs.impl";

    // arguments for tos
    private static final String FS_TOS_ACCESS_KEY = "fs.tos.access.key";
    private static final String FS_TOS_SECRET_KEY = "fs.tos.secret.key";
    private static final String FS_TOS_ENDPOINT = "fs.tos.endpoint";
    private static final String FS_TOS_REGION = "fs.tos.region";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    private static final String FS_TOS_IMPL_DISABLE_CACHE = "fs.tos.impl.disable.cache";
    private static final String FS_TOS_CONNECTION_SSL_ENABLED = "fs.tos.connection.ssl.enabled";
    private static final String FS_TOS_IMPL = "fs.tos.impl";

    private ScheduledExecutorService handleManagementPool = Executors.newScheduledThreadPool(2);

    private int readBufferSize = 128 << 10; // 128k
    private int writeBufferSize = 128 << 10; // 128k

    private ConcurrentHashMap<FileSystemIdentity, BrokerFileSystem> cachedFileSystem;
    private ClientContextManager clientContextManager;

    private boolean hasSetGlobalUGI = false;

    public FileSystemManager() {
        cachedFileSystem = new ConcurrentHashMap<>();
        clientContextManager = new ClientContextManager(handleManagementPool);
        readBufferSize = BrokerConfig.hdfs_read_buffer_size_kb << 10;
        writeBufferSize = BrokerConfig.hdfs_write_buffer_size_kb << 10;
        handleManagementPool.schedule(new FileSystemExpirationChecker(), 0, TimeUnit.SECONDS);
    }

    private static String preparePrincipal(String originalPrincipal) throws UnknownHostException {
        String finalPrincipal = originalPrincipal;
        String[] components = originalPrincipal.split("[/@]");
        if (components != null && components.length == 3) {
            if (components[1].equals("_HOST")) {
                // Convert hostname(fqdn) to lower case according to SecurityUtil.getServerPrincipal
                finalPrincipal = components[0] + "/" +
                        StringUtils.toLowerCase(InetAddress.getLocalHost().getCanonicalHostName())
                        + "@" + components[2];
            } else if (components[1].equals("_IP")) {
                finalPrincipal = components[0] + "/" +
                        InetAddress.getByName(InetAddress.getLocalHost().getCanonicalHostName()).getHostAddress()
                        + "@" + components[2];
            }
        }

        return finalPrincipal;
    }

    /**
     * visible for test
     *
     * @param path
     * @param properties
     * @return BrokerFileSystem with different FileSystem based on scheme
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String scheme = pathUri.getUri().getScheme();
        if (Strings.isNullOrEmpty(scheme)) {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
                    "invalid path. scheme is null");
        }
        BrokerFileSystem brokerFileSystem = null;
        if (scheme.equals(HDFS_SCHEME) || scheme.equals(VIEWFS_SCHEME)) {
            brokerFileSystem = getDistributedFileSystem(scheme, path, properties);
        } else if (scheme.equals(S3A_SCHEME)) {
            brokerFileSystem = getS3AFileSystem(path, properties);
        } else if (scheme.equals(OSS_SCHEME)) {
            brokerFileSystem = getOSSFileSystem(path, properties);
        } else if (scheme.equals(COS_SCHEME)) {
            brokerFileSystem = getCOSFileSystem(path, properties);
        } else if (scheme.equals(KS3_SCHEME)) {
            brokerFileSystem = getKS3FileSystem(path, properties);
        } else if (scheme.equals(OBS_SCHEME)) {
            brokerFileSystem = getOBSFileSystem(path, properties);
        } else if (scheme.equals(TOS_SCHEME)) {
            brokerFileSystem = getTOSFileSystem(path, properties);
        } else {
            // If all above match fails, then we will read the settings from hdfs-site.xml, core-site.xml of FE,
            // and try to create a universal file system. The reason why we can do this is because hadoop/s3 
            // SDK is compatible with nearly all file/object storage system
            brokerFileSystem = getUniversalFileSystem(path, properties);
        }
        return brokerFileSystem;
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is host + username_password
     * it will have safety problem if only hostname is used because one user may specify username and password
     * and then access hdfs, another user may not specify username and password but could also access data
     * <p>
     * Configs related to viewfs in core-site.xml and hdfs-site.xml should be copied to the broker conf directory.
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getDistributedFileSystem(String scheme, String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String host = scheme + "://" + pathUri.getAuthority();
        if (Strings.isNullOrEmpty(pathUri.getAuthority())) {
            if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                host = properties.get(FS_DEFAULTFS_KEY);
                logger.info("no schema and authority in path. use fs.defaultFs");
            } else {
                logger.warn("invalid hdfs path. authority is null,path:" + path);
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "invalid hdfs path. authority is null");
            }
        }
        String username = properties.getOrDefault(USER_NAME_KEY, "");
        String password = properties.getOrDefault(PASSWORD_KEY, "");
        String dfsNameServices = properties.getOrDefault(DFS_NAMESERVICES_KEY, "");
        String authentication = properties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                AUTHENTICATION_SIMPLE);
        String disableCache = properties.getOrDefault(FS_HDFS_IMPL_DISABLE_CACHE, "true");
        if (Strings.isNullOrEmpty(authentication) || (!authentication.equals(AUTHENTICATION_SIMPLE)
                && !authentication.equals(AUTHENTICATION_KERBEROS))) {
            logger.warn("invalid authentication: " + authentication);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "invalid authentication: " + authentication);
        }
        String disableCacheLowerCase = disableCache.toLowerCase();
        if (!(disableCacheLowerCase.equals("true") || disableCacheLowerCase.equals("false"))) {
            logger.warn("invalid disable cache: " + disableCache);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "invalid disable cache: " + disableCache);
        }
        String hdfsUgi = username + "," + password;
        FileSystemIdentity fileSystemIdentity = null;
        BrokerFileSystem fileSystem = null;
        if (authentication.equals(AUTHENTICATION_SIMPLE)) {
            fileSystemIdentity = new FileSystemIdentity(host, hdfsUgi);
        } else {
            // for kerberos, use host + principal + keytab as filesystemindentity
            String kerberosContent = "";
            if (properties.containsKey(KERBEROS_KEYTAB)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB);
            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB_CONTENT);
            } else {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "keytab is required for kerberos authentication");
            }
            if (!properties.containsKey(KERBEROS_PRINCIPAL)) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "principal is required for kerberos authentication");
            } else {
                kerberosContent = kerberosContent + properties.get(KERBEROS_PRINCIPAL);
            }
            try {
                MessageDigest digest = MessageDigest.getInstance("md5");
                byte[] result = digest.digest(kerberosContent.getBytes());
                String kerberosUgi = new String(result);
                fileSystemIdentity = new FileSystemIdentity(host, kerberosUgi);
            } catch (NoSuchAlgorithmException e) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        e.getMessage());
            }
        }
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
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
            UserGroupInformation ugi = null;
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new HdfsConfiguration();
                // TODO get this param from properties
                // conf.set("dfs.replication", "2");
                String tmpFilePath = null;
                if (authentication.equals(AUTHENTICATION_KERBEROS)) {
                    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                            AUTHENTICATION_KERBEROS);
                    conf.set(CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED,
                            "true");

                    String principal = preparePrincipal(properties.get(KERBEROS_PRINCIPAL));
                    String keytab = "";
                    if (properties.containsKey(KERBEROS_KEYTAB)) {
                        keytab = properties.get(KERBEROS_KEYTAB);
                    } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        // pass kerberos keytab content use base64 encoding
                        // so decode it and write it to tmp path under /tmp
                        // because ugi api only accept a local path as argument
                        String keytab_content = properties.get(KERBEROS_KEYTAB_CONTENT);
                        byte[] base64decodedBytes = Base64.getDecoder().decode(keytab_content);
                        long currentTime = System.currentTimeMillis();
                        Random random = new Random(currentTime);
                        int randNumber = random.nextInt(10000);
                        tmpFilePath = "/tmp/." + Long.toString(currentTime) + "_" + Integer.toString(randNumber);
                        File tmpFile = new File(tmpFilePath);
                        if (!tmpFile.exists() && !tmpFile.createNewFile()) {
                            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, "create tmp file failed");
                        }
                        Files.setPosixFilePermissions(tmpFile.toPath(),
                                Sets.newHashSet(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
                        FileOutputStream fileOutputStream = new FileOutputStream(tmpFilePath);
                        fileOutputStream.write(base64decodedBytes);
                        fileOutputStream.close();
                        keytab = tmpFilePath;
                    } else {
                        throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "keytab is required for kerberos authentication");
                    }
                    UserGroupInformation.setConfiguration(conf);

                    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
                    if (!hasSetGlobalUGI) {
                        // set a global ugi so that other components(kms for example) can get the kerberos token.
                        UserGroupInformation.loginUserFromKeytab(principal, keytab);
                        hasSetGlobalUGI = true;
                    }
                    if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        try {
                            File file = new File(tmpFilePath);
                            if (!file.delete()) {
                                logger.warn("delete tmp file:" + tmpFilePath + " failed");
                            }
                        } catch (Exception e) {
                            throw new BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                                    e.getMessage());
                        }
                    }
                }
                if (!Strings.isNullOrEmpty(dfsNameServices)) {
                    conf.set(DFS_NAMESERVICES_KEY, dfsNameServices);
                    logger.info("dfs.name.services:" + dfsNameServices);
                    String[] dfsNameServiceArray = dfsNameServices.split("\\s*,\\s*");
                    for (String dfsNameService : dfsNameServiceArray) {
                        // ha hdfs arguments
                        dfsNameService = dfsNameService.trim();
                        final String dfsHaNameNodesKey = DFS_HA_NAMENODES_PREFIX + dfsNameService;
                        if (!properties.containsKey(dfsHaNameNodesKey)) {
                            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                    "load request missed necessary arguments for ha mode");
                        }
                        String dfsHaNameNodes = properties.get(dfsHaNameNodesKey);
                        conf.set(dfsHaNameNodesKey, dfsHaNameNodes);
                        String[] nameNodes = dfsHaNameNodes.split(",");
                        if (nameNodes == null) {
                            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                    "invalid " + dfsHaNameNodesKey + " configuration");
                        } else {
                            for (String nameNode : nameNodes) {
                                nameNode = nameNode.trim();
                                String nameNodeRpcAddress =
                                        DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX + dfsNameService + "." + nameNode;
                                if (!properties.containsKey(nameNodeRpcAddress)) {
                                    throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                            "missed " + nameNodeRpcAddress + " configuration");
                                } else {
                                    conf.set(nameNodeRpcAddress, properties.get(nameNodeRpcAddress));
                                }
                            }
                        }
                        final String dfsClientFailoverProxyProviderKey =
                                DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX + dfsNameService;
                        if (properties.containsKey(dfsClientFailoverProxyProviderKey)) {
                            conf.set(dfsClientFailoverProxyProviderKey,
                                    properties.get(dfsClientFailoverProxyProviderKey));
                        } else {
                            conf.set(dfsClientFailoverProxyProviderKey,
                                    DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER);
                        }
                        if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                            conf.set(FS_DEFAULTFS_KEY, properties.get(FS_DEFAULTFS_KEY));
                        }
                        if (properties.containsKey(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN)) {
                            conf.set(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN,
                                    properties.get(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN));
                        }
                    }
                }

                conf.set(FS_HDFS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem dfsFileSystem = null;
                if (authentication.equals(AUTHENTICATION_SIMPLE) &&
                        properties.containsKey(USER_NAME_KEY) && !Strings.isNullOrEmpty(username)) {
                    // Use the specified 'username' as the login name
                    ugi = UserGroupInformation.createRemoteUser(username);
                }
                if (ugi != null) {
                    dfsFileSystem = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                        @Override
                        public FileSystem run() throws Exception {
                            return FileSystem.get(pathUri.getUri(), conf);
                        }
                    });
                } else {
                    dfsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                }
                fileSystem.setFileSystem(dfsFileSystem, authentication.equals(AUTHENTICATION_KERBEROS));
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getS3AFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_S3A_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_S3A_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_S3A_ENDPOINT, "");
        String pathStyleAccess = properties.getOrDefault(FS_S3A_PATH_STYLE_ACCESS, "false");
        String disableCache = properties.getOrDefault(FS_S3A_IMPL_DISABLE_CACHE, "true");
        String connectionSSLEnabled = properties.getOrDefault(FS_S3A_CONNECTION_SSL_ENABLED, "true");
        String awsCredProvider = properties.getOrDefault(FS_S3A_AWS_CRED_PROVIDER, null);
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = S3A_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String s3aUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, s3aUgi);
        BrokerFileSystem fileSystem = null;
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_S3A_ACCESS_KEY, accessKey);
                conf.set(FS_S3A_SECRET_KEY, secretKey);
                conf.set(FS_S3A_ENDPOINT, endpoint);
                conf.set(FS_S3A_PATH_STYLE_ACCESS, pathStyleAccess);
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCache);
                conf.set(FS_S3A_CONNECTION_SSL_ENABLED, connectionSSLEnabled);
                if (awsCredProvider != null) {
                    conf.set(FS_S3A_AWS_CRED_PROVIDER, awsCredProvider);
                }
                FileSystem s3AFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(s3AFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getKS3FileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_KS3_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_KS3_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_KS3_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_KS3_IMPL_DISABLE_CACHE, "true");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = KS3_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String ks3aUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, ks3aUgi);
        BrokerFileSystem fileSystem = null;
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_KS3_ACCESS_KEY, accessKey);
                conf.set(FS_KS3_SECRET_KEY, secretKey);
                conf.set(FS_KS3_ENDPOINT, endpoint);
                conf.set(FS_KS3_IMPL, "com.ksyun.kmr.hadoop.fs.ks3.Ks3FileSystem");
                conf.set(FS_KS3_IMPL_DISABLE_CACHE, disableCache);
                FileSystem ks3FileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(ks3FileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getOBSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_OBS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_OBS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_OBS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_OBS_IMPL_DISABLE_CACHE, "true");

        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = OBS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String obsUgi = accessKey + "," + secretKey;

        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, obsUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        BrokerFileSystem fileSystem = cachedFileSystem.get(fileSystemIdentity);

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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_OBS_ACCESS_KEY, accessKey);
                conf.set(FS_OBS_SECRET_KEY, secretKey);
                conf.set(FS_OBS_ENDPOINT, endpoint);
                conf.set(FS_OBS_IMPL, "org.apache.hadoop.fs.obs.OBSFileSystem");
                conf.set(FS_OBS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem obsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(obsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getUniversalFileSystem(String path, Map<String, String> properties) {
        String disableCacheHDFS = properties.getOrDefault(FS_HDFS_IMPL_DISABLE_CACHE, "true");
        String disableCacheS3 = properties.getOrDefault(FS_HDFS_IMPL_DISABLE_CACHE, "true");

        // skip xxx:// first
        int bucketEndIndex = path.indexOf("://", 0);

        // find the end of bucket, for example for xxx://abc/def, we will take xxx://abc as host
        if (bucketEndIndex != -1) {
            bucketEndIndex = path.indexOf("/", bucketEndIndex + 3);
        }
        String host = path;
        if (bucketEndIndex != -1) {
            host = path.substring(0, bucketEndIndex);
        }

        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, "");
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        BrokerFileSystem fileSystem = cachedFileSystem.get(fileSystemIdentity);

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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCacheHDFS);
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCacheS3);
                FileSystem genericFileSystem = FileSystem.get(new Path(path).toUri(), conf);
                fileSystem.setFileSystem(genericFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getOSSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_OSS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_OSS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_OSS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_OSS_IMPL_DISABLE_CACHE, "true");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = OSS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String ossUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, ossUgi);
        BrokerFileSystem fileSystem = null;
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
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
                FileSystem ossFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(ossFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     * for cos
     */
    public BrokerFileSystem getCOSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_COS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_COS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_COS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_COS_IMPL_DISABLE_CACHE, "true");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = COS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String cosUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, cosUgi);
        BrokerFileSystem fileSystem = null;
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_COS_ACCESS_KEY, accessKey);
                conf.set(FS_COS_SECRET_KEY, secretKey);
                conf.set(FS_COS_ENDPOINT, endpoint);
                conf.set(FS_COS_IMPL, "org.apache.hadoop.fs.CosFileSystem");
                conf.set(FS_COS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem cosFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(cosFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    public BrokerFileSystem getTOSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_TOS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_TOS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_TOS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_TOS_IMPL_DISABLE_CACHE, "true");
        String region = properties.getOrDefault(FS_TOS_REGION, "");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = TOS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String tosUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, tosUgi);
        BrokerFileSystem fileSystem = null;
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_TOS_ACCESS_KEY, accessKey);
                conf.set(FS_TOS_SECRET_KEY, secretKey);
                conf.set(FS_TOS_ENDPOINT, endpoint);
                conf.set(FS_TOS_REGION, region);
                conf.set(FS_TOS_IMPL, "com.volcengine.cloudfs.fs.TosFileSystem");
                conf.set(FS_TOS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem tosFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(tosFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    public List<TBrokerFileStatus> listPath(String path, boolean fileNameOnly, Map<String, String> properties) {
        List<TBrokerFileStatus> resultFileStatus = null;
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
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
            logger.info("file not found: " + e.getMessage());
            throw new BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                    e, "file not found");
        } catch (IllegalArgumentException e) {
            logger.error("The arguments of blob store(S3/Azure) may be wrong. You can check " +
                      "the arguments like region, IAM, instance profile and so on.");
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                e, "The arguments of blob store(S3/Azure) may be wrong. " +
                   "You can check the arguments like region, IAM, " +
                   "instance profile and so on.");
        } catch (Exception e) {
            logger.error("errors while get file status ", e);
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "unknown error when get file status");
        }
        return resultFileStatus;
    }

    public void deletePath(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            fileSystem.getDFSFileSystem().delete(filePath, true);
        } catch (IOException e) {
            logger.error("errors while delete path " + path);
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "delete path {} error", path);
        }
    }

    public void renamePath(String srcPath, String destPath, Map<String, String> properties) {
        WildcardURI srcPathUri = new WildcardURI(srcPath);
        WildcardURI destPathUri = new WildcardURI(destPath);
        if (!srcPathUri.getAuthority().trim().equals(destPathUri.getAuthority().trim())) {
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    "only allow rename in same file system");
        }
        BrokerFileSystem fileSystem = getFileSystem(srcPath, properties);
        Path srcfilePath = new Path(srcPathUri.getPath());
        Path destfilePath = new Path(destPathUri.getPath());
        try {
            boolean isRenameSuccess = fileSystem.getDFSFileSystem().rename(srcfilePath, destfilePath);
            if (!isRenameSuccess) {
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        "failed to rename path from {} to {}", srcPath, destPath);
            }
        } catch (IOException e) {
            logger.error("errors while rename path from " + srcPath + " to " + destPath);
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "errors while rename {} to {}", srcPath, destPath);
        }
    }

    public boolean checkPathExist(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            boolean isPathExist = fileSystem.getDFSFileSystem().exists(filePath);
            return isPathExist;
        } catch (IOException e) {
            logger.error("errors while check path exist: " + path);
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "errors while check if path {} exist", path);
        }
    }

    public TBrokerFD openReader(String clientId, String path, long startOffset, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        Path inputFilePath = new Path(pathUri.getPath());
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        try {
            FSDataInputStream fsDataInputStream = fileSystem.getDFSFileSystem().open(inputFilePath, readBufferSize);
            fsDataInputStream.seek(startOffset);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            clientContextManager.putNewInputStream(clientId, fd, fsDataInputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            logger.error("errors while open path", e);
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "could not open file {}", path);
        }
    }

    public ByteBuffer pread(TBrokerFD fd, long offset, long length) {
        FSDataInputStream fsDataInputStream = clientContextManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataInputStream.getPos();
            } catch (IOException e) {
                logger.error("errors while get file pos from output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        "errors while get file pos from output stream");
            }
            if (currentStreamOffset != offset) {
                // it's ok, when reading some format like parquet, it is not a sequential read
                logger.debug("invalid offset, current read offset is "
                        + currentStreamOffset + " is not equal to request offset "
                        + offset + " seek to it");
                try {
                    fsDataInputStream.seek(offset);
                } catch (IOException e) {
                    throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET,
                            e, "current read offset {} is not equal to {}, and could not seek to it",
                            currentStreamOffset, offset);
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
                    throw new BrokerException(TBrokerOperationStatusCode.END_OF_FILE,
                            "end of file reached");
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "read buffer from input stream, buffer size:" + buf.length + ", read length:" + readLength);
                }
                return ByteBuffer.wrap(buf, 0, readLength);
            } catch (IOException e) {
                logger.error("errors while read data from stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while read data from stream");
            }
        }
    }

    public void seek(TBrokerFD fd, long offset) {
        throw new BrokerException(TBrokerOperationStatusCode.OPERATION_NOT_SUPPORTED,
                "seek this method is not supported");
    }

    public void closeReader(TBrokerFD fd) {
        FSDataInputStream fsDataInputStream = clientContextManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                logger.error("errors while close file input stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while close file input stream");
            } finally {
                clientContextManager.removeInputStream(fd);
            }
        }
    }

    public TBrokerFD openWriter(String clientId, String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        Path inputFilePath = new Path(pathUri.getPath());
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        try {
            FSDataOutputStream fsDataOutputStream = fileSystem.getDFSFileSystem().create(inputFilePath,
                    true, writeBufferSize);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            clientContextManager.putNewOutputStream(clientId, fd, fsDataOutputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            logger.error("errors while open path", e);
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "could not open file {}", path);
        }
    }

    public void pwrite(TBrokerFD fd, long offset, byte[] data) {
        FSDataOutputStream fsDataOutputStream = clientContextManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            long currentStreamOffset = fsDataOutputStream.getPos();
            if (currentStreamOffset != offset) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET,
                        "current outputstream offset is {} not equal to request {}",
                        currentStreamOffset, offset);
            }
            try {
                fsDataOutputStream.write(data);
            } catch (IOException e) {
                logger.error("errors while write file " + fd + " to output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while write data to output stream");
            }
        }
    }

    public void closeWriter(TBrokerFD fd) {
        FSDataOutputStream fsDataOutputStream = clientContextManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            try {
                fsDataOutputStream.hsync();
                fsDataOutputStream.close();
            } catch (IOException e) {
                logger.error("errors while close file " + fd + " output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while close file output stream");
            } finally {
                clientContextManager.removeOutputStream(fd);
            }
        }
    }

    public void ping(String clientId) {
        clientContextManager.onPing(clientId);
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
                for (BrokerFileSystem fileSystem : cachedFileSystem.values()) {
                    if (fileSystem.isExpired()) {
                        logger.info("file system " + fileSystem + " is expired, close and remove it");
                        fileSystem.getLock().lock();
                        try {
                            fileSystem.closeFileSystem();
                        } catch (Throwable t) {
                            logger.error("errors while close file system", t);
                        } finally {
                            cachedFileSystem.remove(fileSystem.getIdentity());
                            fileSystem.getLock().unlock();
                        }
                    }
                }
            } finally {
                FileSystemManager.this.handleManagementPool.schedule(this, 60, TimeUnit.SECONDS);
            }
        }

    }
}
