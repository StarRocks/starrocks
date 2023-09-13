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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/hive/HiveMetaStoreThriftClient.java

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

package org.apache.hadoop.hive.metastore;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.hooks.URIResolverHook;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * Modified from apache hive  org.apache.hadoop.hive.metastore.HiveMetaStoreClient.java
 * Newly added method should cover hive0/1/2/3 metastore server.
 */
public class HiveMetaStoreClient implements IMetaStoreClient, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreClient.class);

    ThriftHiveMetastore.Iface client = null;
    private TTransport transport = null;
    private boolean isConnected = false;
    private URI[] metastoreUris;
    protected final Configuration conf;
    // Keep a copy of HiveConf so if Session conf changes, we may need to get a new HMS client.
    private String tokenStrForm;
    private final boolean localMetaStore;
    private final URIResolverHook uriResolverHook;

    private Map<String, String> currentMetaVars;

    private static final AtomicInteger connCount = new AtomicInteger(0);

    // for thrift connects
    private int retries = 5;
    private long retryDelaySeconds = 0;

    /**
     * Capabilities of the current client. If this client talks to a MetaStore server in a manner
     * implying the usage of some expanded features that require client-side support that this client
     * doesn't have (e.g. a getting a table of a new type), it will get back failures when the
     * capability checking is enabled (the default).
     */
    public final static ClientCapabilities VERSION = new ClientCapabilities(
            Lists.newArrayList(ClientCapability.INSERT_ONLY_TABLES));

    // Test capability for tests.
    public final static ClientCapabilities TEST_VERSION = new ClientCapabilities(
            Lists.newArrayList(ClientCapability.INSERT_ONLY_TABLES, ClientCapability.TEST_CAPABILITY));

    private final ClientCapabilities version;

    public HiveMetaStoreClient(Configuration conf) throws MetaException {
        this(conf, null, true);
    }

    public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
        this(conf, hookLoader, true);
    }

    public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
            throws MetaException {

        if (conf == null) {
            conf = MetastoreConf.newMetastoreConf();
            this.conf = conf;
        } else {
            this.conf = new Configuration(conf);
        }

        version = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ? TEST_VERSION : VERSION;

        uriResolverHook = loadUriResolverHook();

        String msUri = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
        localMetaStore = MetastoreConf.isEmbeddedMetaStore(msUri);
        if (localMetaStore) {
            throw new MetaException("Embedded metastore is not allowed here. Please configure "
                    + ConfVars.THRIFT_URIS.toString() + "; it is currently set to [" + msUri + "]");
        }

        // get the number retries
        retries = MetastoreConf.getIntVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES);
        retryDelaySeconds = MetastoreConf.getTimeVar(conf,
                ConfVars.CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

        // user wants file store based configuration
        if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS) != null) {
            resolveUris();
        } else {
            LOG.error("NOT getting uris from conf");
            throw new MetaException("MetaStoreURIs not found in conf file");
        }

        //If HADOOP_PROXY_USER is set in env or property,
        //then need to create metastore client that proxies as that user.
        String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";
        String proxyUser = System.getenv(HADOOP_PROXY_USER);
        if (proxyUser == null) {
            proxyUser = System.getProperty(HADOOP_PROXY_USER);
        }
        //if HADOOP_PROXY_USER is set, create DelegationToken using real user
        if (proxyUser != null) {
            LOG.info(HADOOP_PROXY_USER + " is set. Using delegation "
                    + "token for HiveMetaStore connection.");
            try {
                UserGroupInformation.getLoginUser().getRealUser().doAs(
                        (PrivilegedExceptionAction<Void>) () -> {
                            open();
                            return null;
                        });
                String delegationTokenPropString = "DelegationTokenForHiveMetaStoreServer";
                String delegationTokenStr = getDelegationToken(proxyUser, proxyUser);
                SecurityUtils.setTokenStr(UserGroupInformation.getCurrentUser(), delegationTokenStr,
                        delegationTokenPropString);
                MetastoreConf.setVar(this.conf, ConfVars.TOKEN_SIGNATURE, delegationTokenPropString);
                close();
            } catch (Exception e) {
                LOG.error("Error while setting delegation token for " + proxyUser, e);
                if (e instanceof MetaException) {
                    throw (MetaException) e;
                } else {
                    throw new MetaException(e.getMessage());
                }
            }
        }
        // finally open the store
        open();
    }

    private void resolveUris() throws MetaException {
        String[] metastoreUrisString = MetastoreConf.getVar(conf,
                ConfVars.THRIFT_URIS).split(",");

        List<URI> metastoreURIArray = new ArrayList<URI>();
        try {
            for (String s : metastoreUrisString) {
                URI tmpUri = new URI(s);
                if (tmpUri.getScheme() == null) {
                    throw new IllegalArgumentException("URI: " + s
                            + " does not have a scheme");
                }
                if (uriResolverHook != null) {
                    metastoreURIArray.addAll(uriResolverHook.resolveURI(tmpUri));
                } else {
                    metastoreURIArray.add(new URI(
                            tmpUri.getScheme(),
                            tmpUri.getUserInfo(),
                            HadoopThriftAuthBridge.getBridge().getCanonicalHostName(tmpUri.getHost()),
                            tmpUri.getPort(),
                            tmpUri.getPath(),
                            tmpUri.getQuery(),
                            tmpUri.getFragment()
                    ));
                }
            }
            metastoreUris = new URI[metastoreURIArray.size()];
            for (int j = 0; j < metastoreURIArray.size(); j++) {
                metastoreUris[j] = metastoreURIArray.get(j);
            }

            if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
                List<URI> uriList = Arrays.asList(metastoreUris);
                Collections.shuffle(uriList);
                metastoreUris = uriList.toArray(metastoreUris);
            }
        } catch (IllegalArgumentException e) {
            throw (e);
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
    }

    //multiple clients may initialize the hook at the same time
    synchronized private URIResolverHook loadUriResolverHook() throws IllegalStateException {

        String uriResolverClassName =
                MetastoreConf.getAsString(conf, ConfVars.URI_RESOLVER);
        if (uriResolverClassName.equals("")) {
            return null;
        } else {
            LOG.info("Loading uri resolver" + uriResolverClassName);
            try {
                Class<?> uriResolverClass = Class.forName(uriResolverClassName, true,
                        JavaUtils.getClassLoader());
                return (URIResolverHook) ReflectionUtils.newInstance(uriResolverClass, null);
            } catch (Exception e) {
                LOG.error("Exception loading uri resolver hook" + e);
                return null;
            }
        }
    }

    /**
     * Swaps the first element of the metastoreUris array with a random element from the
     * remainder of the array.
     */
    private void promoteRandomMetaStoreURI() {
        if (metastoreUris.length <= 1) {
            return;
        }
        int index = ThreadLocalRandom.current().nextInt(metastoreUris.length - 1) + 1;
        URI tmp = metastoreUris[0];
        metastoreUris[0] = metastoreUris[index];
        metastoreUris[index] = tmp;
    }

    @Override
    public boolean isLocalMetaStore() {
        return localMetaStore;
    }

    @Override
    public boolean isCompatibleWith(Configuration conf) {
        // Make a copy of currentMetaVars, there is a race condition that
        // currentMetaVars might be changed during the execution of the method
        Map<String, String> currentMetaVarsCopy = currentMetaVars;
        if (currentMetaVarsCopy == null) {
            return false; // recreate
        }
        boolean compatible = true;
        for (ConfVars oneVar : MetastoreConf.metaVars) {
            // Since metaVars are all of different types, use string for comparison
            String oldVar = currentMetaVarsCopy.get(oneVar.getVarname());
            String newVar = MetastoreConf.getAsString(conf, oneVar);
            if (oldVar == null ||
                    (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
                LOG.info("Mestastore configuration " + oneVar.toString() +
                        " changed from " + oldVar + " to " + newVar);
                compatible = false;
            }
        }
        return compatible;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {
        MetastoreConf.setVar(conf, ConfVars.ADDED_JARS, addedJars);
    }

    @Override
    public void reconnect() throws MetaException {
        if (localMetaStore) {
            // For direct DB connections we don't yet support reestablishing connections.
            throw new MetaException("For direct MetaStore DB connections, we don't support retries" +
                    " at the client level.");
        } else {
            close();
            // If the user passes in an address of 'hive.metastore.uris' similar to nginx, fe may only resolve to one url.
            // If the user's ip changes, thrift client can't use other url to access. Therefore, we need to resolve uris
            // for each reconnect. After all, reconnect is a rare behavior.
            resolveUris();

            if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
                // Swap the first element of the metastoreUris[] with a random element from the rest
                // of the array. Rationale being that this method will generally be called when the default
                // connection has died and the default connection is likely to be the first array element.
                promoteRandomMetaStoreURI();
            }
            open();
        }
    }

    private void open() throws MetaException {
        isConnected = false;
        TTransportException tte = null;
        boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
        boolean useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
        boolean useFramedTransport = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
        boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
        int clientSocketTimeout = (int) MetastoreConf.getTimeVar(conf,
                ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

        for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
            for (URI store : metastoreUris) {
                LOG.info("Trying to connect to metastore with URI " + store);

                try {
                    if (useSSL) {
                        try {
                            String trustStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_PATH).trim();
                            if (trustStorePath.isEmpty()) {
                                throw new IllegalArgumentException(ConfVars.SSL_TRUSTSTORE_PATH.toString()
                                        + " Not configured for SSL connection");
                            }
                            String trustStorePassword =
                                    MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD);

                            // Create an SSL socket and connect
                            transport =
                                    SecurityUtils.getSSLSocket(store.getHost(), store.getPort(), clientSocketTimeout,
                                            trustStorePath, trustStorePassword);
                            LOG.info("Opened an SSL connection to metastore, current connections: " +
                                    connCount.incrementAndGet());
                        } catch (IOException e) {
                            throw new IllegalArgumentException(e);
                        } catch (TTransportException e) {
                            tte = e;
                            throw new MetaException(e.toString());
                        }
                    } else {
                        transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
                    }

                    if (useSasl) {
                        // Wrap thrift connection with SASL for secure connection.
                        try {
                            HadoopThriftAuthBridge.Client authBridge =
                                    HadoopThriftAuthBridge.getBridge().createClient();

                            // check if we should use delegation tokens to authenticate
                            // the call below gets hold of the tokens if they are set up by hadoop
                            // this should happen on the map/reduce tasks if the client added the
                            // tokens into hadoop's credential store in the front end during job
                            // submission.
                            String tokenSig = MetastoreConf.getVar(conf, ConfVars.TOKEN_SIGNATURE);
                            // tokenSig could be null
                            tokenStrForm = SecurityUtils.getTokenStrForm(tokenSig);

                            if (tokenStrForm != null) {
                                LOG.info(
                                        "HMSC::open(): Found delegation token. Creating DIGEST-based thrift connection.");
                                // authenticate using delegation tokens via the "DIGEST" mechanism
                                transport = authBridge.createClientTransport(null, store.getHost(),
                                        "DIGEST", tokenStrForm, transport,
                                        MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
                            } else {
                                LOG.info(
                                        "HMSC::open(): Could not find delegation token. Creating KERBEROS-based thrift connection.");
                                String principalConfig =
                                        MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL);
                                transport = authBridge.createClientTransport(
                                        principalConfig, store.getHost(), "KERBEROS", null,
                                        transport, MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
                            }
                        } catch (IOException ioe) {
                            LOG.error("Couldn't create client transport", ioe);
                            throw new MetaException(ioe.toString());
                        }
                    } else {
                        if (useFramedTransport) {
                            transport = new TFramedTransport(transport);
                        }
                    }

                    final TProtocol protocol;
                    if (useCompactProtocol) {
                        protocol = new TCompactProtocol(transport);
                    } else {
                        protocol = new TBinaryProtocol(transport);
                    }
                    client = new ThriftHiveMetastore.Client(protocol);
                    try {
                        if (!transport.isOpen()) {
                            transport.open();
                            LOG.info("Opened a connection to metastore, current connections: " +
                                    connCount.incrementAndGet());
                        }
                        isConnected = true;
                    } catch (TTransportException e) {
                        tte = e;
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("Failed to connect to the MetaStore Server...", e);
                        } else {
                            // Don't print full exception trace if DEBUG is not on.
                            LOG.warn("Failed to connect to the MetaStore Server...");
                        }
                    }

                    if (isConnected && !useSasl && MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)) {
                        // Call set_ugi, only in unsecure mode.
                        try {
                            UserGroupInformation ugi = SecurityUtils.getUGI();
                            client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
                        } catch (LoginException e) {
                            LOG.warn("Failed to do login. set_ugi() is not successful, " +
                                    "Continuing without it.", e);
                        } catch (IOException e) {
                            LOG.warn("Failed to find ugi of client set_ugi() is not successful, " +
                                    "Continuing without it.", e);
                        } catch (TException e) {
                            LOG.warn("set_ugi() not successful, Likely cause: new client talking to old server. "
                                    + "Continuing without it.", e);
                        }
                    }
                } catch (MetaException e) {
                    LOG.error("Unable to connect to metastore with URI " + store
                            + " in attempt " + attempt, e);
                }
                if (isConnected) {
                    break;
                }
            }
            // Wait before launching the next round of connection retries.
            if (!isConnected && retryDelaySeconds > 0) {
                try {
                    LOG.info("Waiting " + retryDelaySeconds + " seconds before next connection attempt.");
                    Thread.sleep(retryDelaySeconds * 1000);
                } catch (InterruptedException ignore) {
                }
            }
        }

        if (!isConnected) {
            throw new MetaException("Could not connect to meta store using any of the URIs provided." +
                    " Most recent failure: " + StringUtils.stringifyException(tte));
        }

        snapshotActiveConf();

        LOG.info("Connected to metastore.");
    }

    private void snapshotActiveConf() {
        currentMetaVars = new HashMap<>(MetastoreConf.metaVars.length);
        for (ConfVars oneVar : MetastoreConf.metaVars) {
            currentMetaVars.put(oneVar.getVarname(), MetastoreConf.getAsString(conf, oneVar));
        }
    }

    @Override
    public void close() {
        isConnected = false;
        currentMetaVars = null;
        try {
            if (null != client) {
                client.shutdown();
            }
        } catch (TException e) {
            LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
        }
        // Transport would have got closed via client.shutdown(), so we dont need this, but
        // just in case, we make this call.
        if ((transport != null) && transport.isOpen()) {
            transport.close();
            LOG.info("Closed a connection to metastore, current connections: " + connCount.decrementAndGet());
        }
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException, TException, NoSuchObjectException {
        return getTable(null, dbName, tableName);
    }

    @Override
    public Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
        try {
            // Using get_table() first, if user's Hive forbidden this request,
            // then fail over to use get_table_req() instead.
            return client.get_table(dbName, tableName);
        } catch (NoSuchObjectException e) {
            // NoSuchObjectException need to be thrown when creating iceberg table.
            LOG.warn("Failed to get table {}.{}", dbName, tableName, e);
            throw e;
        } catch (Exception e) {
            LOG.warn("Using get_table() failed, fail over to use get_table_req()", e);
            GetTableRequest req = new GetTableRequest(dbName, tableName);
            req.setCapabilities(version);
            return client.get_table_req(req).getTable();
        }
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partVals)
            throws NoSuchObjectException, MetaException, TException {
        return getPartition(null, dbName, tblName, partVals);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
            throws NoSuchObjectException, MetaException, TException {
        return client.get_partition(dbName, tblName, partVals);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String name)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return getPartition(null, dbName, tblName, name);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tblName, String name)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return client.get_partition_by_name(dbName, tblName, name);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short maxParts)
            throws NoSuchObjectException, MetaException, TException {
        return listPartitionNames(null, dbName, tblName, maxParts);
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName, String tblName, int maxParts)
            throws NoSuchObjectException, MetaException, TException {
        return client.get_partition_names(dbName, tblName, shrinkMaxtoShort(maxParts));
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, List<String> partVals, short maxParts)
            throws MetaException, TException, NoSuchObjectException {
        return listPartitionNames(null, dbName, tblName, partVals, maxParts);
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName, String tblName, List<String> partVals,
                                           int maxParts) throws MetaException, TException, NoSuchObjectException {
        return client.get_partition_names_ps(dbName, tblName, partVals, shrinkMaxtoShort(maxParts));
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return client.partition_name_to_vals(name);
    }

    private short shrinkMaxtoShort(int max) {
        if (max < 0) {
            return -1;
        } else if (max <= Short.MAX_VALUE) {
            return (short) max;
        } else {
            return Short.MAX_VALUE;
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tblName,
                                                List<String> partNames) throws TException {
        return getPartitionsByNames(null, dbName, tblName, partNames);
    }

    @Override
    public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
                                                List<String> partNames) throws TException {
        return client.get_partitions_by_names(dbName, tblName, partNames);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
                                                              List<String> colNames) throws TException {
        return getTableColumnStatistics(null, dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
                                                              String tableName,
                                                              List<String> colNames) throws TException {
        TableStatsRequest rqst = new TableStatsRequest(dbName, tableName, colNames);
        return client.get_table_statistics_req(rqst).getTableStats();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tableName, List<String> partNames, List<String> colNames)
            throws TException {
        return getPartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partNames, colNames);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String catName, String dbName, String tableName, List<String> partNames,
            List<String> colNames) throws TException {
        PartitionsStatsRequest rqst = new PartitionsStatsRequest(dbName, tableName, colNames,
                partNames);
        return client.get_partitions_statistics_req(rqst).getPartStats();
    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter)
            throws TException {
        NotificationEventRequest eventRequest = new NotificationEventRequest();
        eventRequest.setMaxEvents(maxEvents);
        eventRequest.setLastEvent(lastEventId);
        return client.get_next_notification(eventRequest);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return client.get_current_notificationEventId();
    }

    /**
     * Creates a synchronized wrapper for any {@link IMetaStoreClient}.
     * This may be used by multi-threaded applications until we have
     * fixed all reentrancy bugs.
     *
     * @param client unsynchronized client
     *
     * @return synchronized client
     */
    public static IMetaStoreClient newSynchronizedClient(
            IMetaStoreClient client) {
        return (IMetaStoreClient) Proxy.newProxyInstance(
                HiveMetaStoreClient.class.getClassLoader(),
                new Class [] { IMetaStoreClient.class },
                new SynchronizedHandler(client));
    }

    private static class SynchronizedHandler implements InvocationHandler {
        private final IMetaStoreClient client;

        SynchronizedHandler(IMetaStoreClient client) {
            this.client = client;
        }

        @Override
        public synchronized Object invoke(Object proxy, Method method, Object [] args)
                throws Throwable {
            try {
                return method.invoke(client, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    public void setMetaConf(String key, String value) throws MetaException, TException {

        throw new TException("method not implemented");
    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void createCatalog(Catalog catalog)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {

        throw new TException("method not implemented");
    }

    @Override
    public void alterCatalog(String s, Catalog catalog) throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Catalog getCatalog(String catName) throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void dropCatalog(String catName)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public List<String> getDatabases(String databasePattern) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getDatabases(String catName, String databasePattern) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return client.get_all_databases();
    }

    @Override
    public List<String> getAllDatabases(String catName) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String dbName)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                                        List<String> tableTypes) throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
        return client.get_all_tables(dbName);
    }

    @Override
    public List<String> getAllTables(String catName, String dbName)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
            throws TException, InvalidOperationException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
            throws TException, InvalidOperationException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public void dropTable(String dbname, String name, boolean deleteData,
                          boolean ignoreUnknownTab) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, null);
    }

    @Override
    public void dropTable(String dbname, String name, boolean deleteData,
                          boolean ignoreUnknownTab, boolean ifPurge) throws TException {
        dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, ifPurge);
    }

    @Override
    public void dropTable(String dbname, String name) throws TException {
        dropTable(getDefaultCatalog(conf), dbname, name, true, true, null);
    }

    @Override
    public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
                          boolean ignoreUnknownTable, boolean ifPurge) throws TException {
        //build new environmentContext with ifPurge;
        EnvironmentContext envContext = null;
        if(ifPurge){
            Map<String, String> warehouseOptions;
            warehouseOptions = new HashMap<>();
            warehouseOptions.put("ifPurge", "TRUE");
            envContext = new EnvironmentContext(warehouseOptions);
        }
        dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, envContext);

    }

    /**
     * Drop the table and choose whether to: delete the underlying table data;
     * throw if the table doesn't exist; save the data in the trash.
     *
     * @param catName catalog name
     * @param dbname database name
     * @param name table name
     * @param deleteData
     *          delete the underlying data or just delete the table in metadata
     * @param ignoreUnknownTab
     *          don't throw if the requested table doesn't exist
     * @param envContext
     *          for communicating with thrift
     * @throws MetaException
     *           could not drop table properly
     * @throws NoSuchObjectException
     *           the table wasn't found
     * @throws TException
     *           a thrift communication error occurred
     * @throws UnsupportedOperationException
     *           dropping an index table is not allowed
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
     *      java.lang.String, boolean)
     */
    public void dropTable(String catName, String dbname, String name, boolean deleteData,
                          boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        Table tbl;
        try {
            tbl = getTable(catName, dbname, name);
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownTab) {
                throw e;
            }
            return;
        }
        HiveMetaHook hook = getHook(tbl);
        if (hook != null) {
            hook.preDropTable(tbl);
        }
        boolean success = false;
        try {
            drop_table_with_environment_context(dbname, name, deleteData, envContext);
            if (hook != null) {
                hook.commitDropTable(tbl, deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
            }
            success=true;
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownTab) {
                throw e;
            }
        } finally {
            if (!success && (hook != null)) {
                hook.rollbackDropTable(tbl);
            }
        }
    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partNames)
            throws MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
            throws MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean tableExists(String catName, String dbName, String tableName)
            throws MetaException, TException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
        return client.get_database(databaseName);
    }

    @Override
    public Database getDatabase(String catalogName, String databaseName)
            throws NoSuchObjectException, MetaException, TException {
        return client.get_database(databaseName);
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return getTableObjectsByName(null, dbName, tableNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return client.get_table_objects_by_name(dbName, tableNames);
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String s) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
            throws MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
            throws MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public Partition appendPartition(String dbName, String tableName, List<String> partVals)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition appendPartition(String dbName, String tableName, String name)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition appendPartition(String catName, String dbName, String tableName, String name)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    public int add_partitions(List<Partition> new_parts) throws TException {
        if (new_parts != null && !new_parts.isEmpty() && !((Partition)new_parts.get(0)).isSetCatName()) {
            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
            new_parts.forEach((p) -> {
                p.setCatName(defaultCat);
            });
        }

        return this.client.add_partitions(new_parts);
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpec)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
                                        String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat, String sourceDb,
                                        String sourceTable, String destCat, String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
                                               String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat, String sourceDb,
                                               String sourceTable, String destCat, String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals, String userName,
                                              List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName, List<String> pvals,
                                              String userName, List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitions(String catName, String db_name, String tbl_name, int max_parts)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals, short max_parts)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitions(String catName, String db_name, String tbl_name, List<String> part_vals,
                                          int max_parts) throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
            throws MetaException, TException, NoSuchObjectException {
        throw new TException("method not implemented");
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name, String filter,
                                                  int max_parts)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name, String filter,
                                                         int max_parts)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr, String default_partition_name,
                                        short max_parts, List<Partition> result) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
                                        String default_partition_name, int max_parts, List<Partition> result)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short maxParts, String userName,
                                                      List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName, int maxParts,
                                                      String userName, List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, List<String> partialPvals,
                                                      short maxParts, String userName, List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                      List<String> partialPvals, int maxParts, String userName,
                                                      List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        throw new TException("method not implemented");
    }

    @Override
    public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
                                      PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        throw new TException("method not implemented");

    }

    @Override
    public void markPartitionForEvent(String catName, String db_name, String tbl_name, Map<String, String> partKVs,
                                      PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        throw new TException("method not implemented");

    }

    @Override
    public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
                                             PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name,
                                             Map<String, String> partKVs, PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        throw new TException("method not implemented");
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {
        throw new TException("method not implemented");

    }

    private HiveMetaHook getHook(Table tbl) {
        return null;
    }


    @Override
    public void createTable(Table tbl) throws TException {
        createTable(tbl, null);
    }

    public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        if (!tbl.isSetCatName()) {
            tbl.setCatName(getDefaultCatalog(conf));
        }
        HiveMetaHook hook = getHook(tbl);
        if (hook != null) {
            hook.preCreateTable(tbl);
        }
        boolean success = false;
        try {
            // Subclasses can override this step (for example, for temporary tables)
            create_table_with_environment_context(tbl, envContext);
            if (hook != null) {
                hook.commitCreateTable(tbl);
            }
            success = true;
        }
        finally {
            if (!success && (hook != null)) {
                try {
                    hook.rollbackCreateTable(tbl);
                } catch (Exception e){
                    LOG.error("Create rollback failed with", e);
                }
            }
        }
    }

    protected void create_table_with_environment_context(Table tbl, EnvironmentContext envContext)
            throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {
        client.create_table_with_environment_context(tbl, envContext);
    }

    protected void drop_table_with_environment_context(String dbname, String name,
                                                       boolean deleteData, EnvironmentContext envContext) throws TException {
        client.drop_table_with_environment_context(dbname,
                name, deleteData, envContext);
    }

    public void alter_table(String dbname, String tbl_name, Table new_tbl) throws TException {
        this.alter_table_with_environmentContext(dbname, tbl_name, new_tbl, (EnvironmentContext)null);
    }

    public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade) throws TException {
        EnvironmentContext environmentContext = new EnvironmentContext();
        if (cascade) {
            environmentContext.putToProperties("CASCADE", "true");
        }

        this.alter_table_with_environmentContext(defaultDatabaseName, tblName, table, environmentContext);
    }

    public void alter_table(String catName, String dbName, String tblName, Table newTable, EnvironmentContext envContext) throws TException {
        this.client.alter_table_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, newTable, envContext);
    }

    @Override
    public void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
                                                    EnvironmentContext environmentContext)
            throws TException {
        HiveMetaHook hook = getHook(table);
        if (hook != null) {
            hook.preAlterTable(table, environmentContext);
        }

        client.alter_table_with_environment_context(databaseName, tblName, table, environmentContext);
    }

    @Override
    public void createDatabase(Database db)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        if (!db.isSetCatalogName()) {
            db.setCatalogName(getDefaultCatalog(conf));
        }
        client.create_database(db);
    }

    /**
     * @param name
     * @throws NoSuchObjectException
     * @throws InvalidOperationException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String, boolean, boolean)
     */
    @Override
    public void dropDatabase(String name)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(getDefaultCatalog(conf), name, true, false, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, cascade);
    }

    @Override
    public void dropDatabase(String catalogName, String dbName, boolean deleteData,
                             boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        try {
            getDatabase(catalogName, dbName);
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownDb) {
                throw e;
            }
            return;
        }

        if (cascade) {
            // Note that this logic may drop some of the tables of the database
            // even if the drop database fail for any reason
            // TODO: Fix this
            List<String> materializedViews = getTables(dbName, ".*", TableType.MATERIALIZED_VIEW);
            for (String table : materializedViews) {
                // First we delete the materialized views
                dropTable(dbName, table, deleteData, true);
            }
            List<String> tableList = getAllTables(dbName);
            for (String table : tableList) {
                // Now we delete the rest of tables
                try {
                    // Subclasses can override this step (for example, for temporary tables)
                    dropTable(dbName, table, deleteData, true);
                } catch (UnsupportedOperationException e) {
                    // Ignore Index tables, those will be dropped with parent tables
                }
            }
        }
        client.drop_database(dbName, deleteData, cascade);
    }

    @Override
    public void alterDatabase(String name, Database db) throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alterDatabase(String catName, String dbName, Database newDb)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData) throws TException {
        return this.dropPartition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, partName, deleteData);
    }

    public boolean dropPartition(String catName, String db_name, String tbl_name, String name, boolean deleteData) throws TException {
        return this.client.drop_partition_by_name_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, name, deleteData, (EnvironmentContext)null);
    }

    private static EnvironmentContext getEnvironmentContextWithIfPurgeSet() {
        Map<String, String> warehouseOptions = new HashMap();
        warehouseOptions.put("ifPurge", "TRUE");
        return new EnvironmentContext(warehouseOptions);
    }

    /** @deprecated */
    @Deprecated
    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, EnvironmentContext env_context) throws TException {
        return this.client.drop_partition_with_environment_context(MetaStoreUtils.prependCatalogToDbName(db_name, this.conf), tbl_name, part_vals, true, env_context);
    }

    /** @deprecated */
    @Deprecated
    public boolean dropPartition(String dbName, String tableName, String partName, boolean dropData, EnvironmentContext ec) throws TException {
        return this.client.drop_partition_by_name_with_environment_context(MetaStoreUtils.prependCatalogToDbName(dbName, this.conf), tableName, partName, dropData, ec);
    }

    /** @deprecated */
    @Deprecated
    public boolean dropPartition(String dbName, String tableName, List<String> partVals) throws TException {
        return this.client.drop_partition(MetaStoreUtils.prependCatalogToDbName(dbName, this.conf), tableName, partVals, true);
    }

    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws TException {
        return this.dropPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, PartitionDropOptions.instance().deleteData(deleteData));
    }

    public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws TException {
        return this.dropPartition(catName, db_name, tbl_name, part_vals, PartitionDropOptions.instance().deleteData(deleteData));
    }

    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options) throws TException {
        return this.dropPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, options);
    }

    public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options) throws TException {
        if (options == null) {
            options = PartitionDropOptions.instance();
        }

        if (part_vals != null) {
            Iterator var6 = part_vals.iterator();

            while(var6.hasNext()) {
                String partVal = (String)var6.next();
                if (partVal == null) {
                    throw new MetaException("The partition value must not be null.");
                }
            }
        }

        return this.client.drop_partition_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, part_vals, options.deleteData, options.purgeData ? getEnvironmentContextWithIfPurgeSet() : null);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
                                          boolean deleteData, boolean ifExists)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                          List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                          boolean ifExists) throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
                                          boolean deleteData, boolean ifExists, boolean needResults)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                          List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                          boolean ifExists, boolean needResults)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
                                          PartitionDropOptions options)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                          List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    public void alter_partition(String dbName, String tblName, Partition newPart) throws InvalidOperationException, MetaException, TException {
        this.alter_partition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, newPart, (EnvironmentContext)null);
    }

    public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        this.alter_partition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, newPart, environmentContext);
    }

    public void alter_partition(String catName, String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext) throws TException {
        this.client.alter_partition_with_environment_context(dbName, tblName, newPart, environmentContext);
    }
    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
            throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
                                 EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts)
            throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
                                 EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void renamePartition(String dbname, String tableName, List<String> part_vals, Partition newPart)
            throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
                                Partition newPart) throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<FieldSchema> getFields(String catName, String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public List<FieldSchema> getSchema(String catName, String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        throw new TException("method not implemented");
    }

    @Override
    public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
        throw new TException("method not implemented");
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName, String partName,
                                                   String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean drop_role(String role_name) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean grant_role(String role_name, String user_name, PrincipalType principalType, String grantor,
                              PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean revoke_role(String role_name, String user_name, PrincipalType principalType, boolean grantOption)
            throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Role> list_roles(String principalName, PrincipalType principalType) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
            throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type,
                                                     HiveObjectRef hiveObject) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
            throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public String getTokenStrForm() throws IOException {
        return tokenStrForm;
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public String[] getMasterKeys() throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void createFunction(Function func) throws InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void dropFunction(String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void dropFunction(String catName, String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public long openTxn(String user) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void replRollbackTxn(long srcTxnid, String replPolicy) throws NoSuchTxnException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void replCommitTxn(long srcTxnid, String replPolicy)
            throws NoSuchTxnException, TxnAbortedException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void abortTxns(List<Long> txnids) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
            throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
                                                             List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
        return client.lock(request);
    }

    @Override
    public LockResponse checkLock(long lockid)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {
        client.unlock(new UnlockRequest(lockid));
    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void heartbeat(long txnid, long lockid)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type,
                        Map<String, String> tblproperties) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
                                       Map<String, String> tblproperties) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames)
            throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames,
                                     DataOperationType operationType) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void insertTable(Table table, boolean overwrite) throws MetaException {
        throw new MetaException("method not implemented");
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
            throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
                                        List<String> partNames)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        throw new TException("method not implemented");
    }

    @Override
    public void flushCache() {

    }

    @Override
    public Iterable<Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> fileIds, ByteBuffer sarg,
                                                                          boolean doGetFooters) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public boolean isSameConfObj(Configuration c) {
        return false;
    }

    @Override
    public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void createTableWithConstraints(Table tbl,
                                           List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
                                           List<SQLUniqueConstraint> uniqueConstraints,
                                           List<SQLNotNullConstraint> notNullConstraints,
                                           List<SQLDefaultConstraint> defaultConstraints,
                                           List<SQLCheckConstraint> checkConstraints)
            throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {

        if (!tbl.isSetCatName()) {
            String defaultCat = getDefaultCatalog(conf);
            tbl.setCatName(defaultCat);
            if (primaryKeys != null) {
                primaryKeys.forEach(pk -> pk.setCatName(defaultCat));
            }
            if (foreignKeys != null) {
                foreignKeys.forEach(fk -> fk.setCatName(defaultCat));
            }
            if (uniqueConstraints != null) {
                uniqueConstraints.forEach(uc -> uc.setCatName(defaultCat));
            }
            if (notNullConstraints != null) {
                notNullConstraints.forEach(nn -> nn.setCatName(defaultCat));
            }
            if (defaultConstraints != null) {
                defaultConstraints.forEach(def -> def.setCatName(defaultCat));
            }
            if (checkConstraints != null) {
                checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
            }
        }
        HiveMetaHook hook = getHook(tbl);
        if (hook != null) {
            hook.preCreateTable(tbl);
        }
        boolean success = false;
        try {
            // Subclasses can override this step (for example, for temporary tables)
            client.create_table_with_constraints(tbl, primaryKeys, foreignKeys,
                    uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
            if (hook != null) {
                hook.commitCreateTable(tbl);
            }
            success = true;
        } finally {
            if (!success && (hook != null)) {
                hook.rollbackCreateTable(tbl);
            }
        }
    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints)
            throws MetaException, NoSuchObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
            throws InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public WMFullResourcePlan getResourcePlan(String resourcePlanName)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void dropResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan resourcePlan,
                                                boolean canActivateDisabled, boolean isForceDeactivate,
                                                boolean isReplace)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void createWMTrigger(WMTrigger trigger) throws InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alterWMTrigger(WMTrigger trigger)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void dropWMTrigger(String resourcePlanName, String triggerName)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan)
            throws NoSuchObjectException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void createWMPool(WMPool pool)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alterWMPool(WMNullablePool pool, String poolPath)
            throws NoSuchObjectException, InvalidObjectException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void dropWMPool(String resourcePlanName, String poolPath) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void dropWMMapping(WMMapping mapping) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath,
                                                 boolean shouldDrop)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new TException("method not implemented");

    }

    @Override
    public void createISchema(ISchema schema) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public ISchema getISchema(String catName, String dbName, String name) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void dropISchema(String catName, String dbName, String name) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName)
            throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName)
            throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void setSchemaVersionState(String catName, String dbName, String schemaName, int version,
                                      SchemaVersionState state) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {
        throw new TException("method not implemented");

    }

    @Override
    public SerDeInfo getSerDe(String serDeName) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void addRuntimeStat(RuntimeStat stat) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
        throw new TException("method not implemented");
    }
}

