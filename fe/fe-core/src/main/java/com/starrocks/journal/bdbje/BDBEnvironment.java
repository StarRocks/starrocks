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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/journal/bdbje/BDBEnvironment.java

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

package com.starrocks.journal.bdbje;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.DbResetRepGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.NetUtils;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.BDBStateChangeListener;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.HAProtocol;
import com.starrocks.journal.JournalException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;

/* this class contains the reference to bdb environment.
 * including all the opened databases and the replicationGroupAdmin.
 * we can get the information of this bdb group through the API of replicationGroupAdmin
 */
public class BDBEnvironment {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironment.class);
    protected static int RETRY_TIME = 3;
    protected static int SLEEP_INTERVAL_SEC = 5;
    private static final int MEMORY_CACHE_PERCENT = 20;
    // wait at most 10 seconds after environment initialized for state change
    private static final int INITIAL_STATE_CHANGE_WAIT_SEC = 10;

    public static final String STARROCKS_JOURNAL_GROUP = "PALO_JOURNAL_GROUP";
    private static final String BDB_DIR = "/bdb";

    private ReplicatedEnvironment replicatedEnvironment;
    private EnvironmentConfig environmentConfig;
    private ReplicationConfig replicationConfig;
    private DatabaseConfig dbConfig;
    private TransactionConfig txnConfig;
    private CloseSafeDatabase epochDB = null;  // used for fencing

    // mark whether environment is closing, if true, all calling to environment will fail
    private volatile boolean closing = false;

    private final File envHome;
    private final String selfNodeName;
    private final String selfNodeHostPort;
    private final String helperHostPort;
    private final boolean isElectable;

    /**
     * init & return bdb environment
     */
    public static BDBEnvironment initBDBEnvironment(String nodeName) throws JournalException, InterruptedException {
        // check for port use
        Pair<String, Integer> selfNode = GlobalStateMgr.getCurrentState().getSelfNode();
        try {
            if (NetUtils.isPortUsing(selfNode.first, selfNode.second)) {
                String errMsg = String.format("edit_log_port %d is already in use. will exit.", selfNode.second);
                LOG.error(errMsg);
                throw new JournalException(errMsg);
            }
        } catch (IOException e) {
            String errMsg = String.format("failed to check if %s:%s is used!", selfNode.first, selfNode.second);
            LOG.error(errMsg, e);
            JournalException journalException = new JournalException(errMsg);
            journalException.initCause(e);
            throw journalException;
        }

        // constructor
        String selfNodeHostPort = selfNode.first + ":" + selfNode.second;

        File dbEnv = new File(getBdbDir());
        if (!dbEnv.exists()) {
            dbEnv.mkdirs();
        }

        Pair<String, Integer> helperNode = GlobalStateMgr.getCurrentState().getHelperNode();
        String helperHostPort = helperNode.first + ":" + helperNode.second;

        BDBEnvironment bdbEnvironment = new BDBEnvironment(dbEnv, nodeName, selfNodeHostPort,
                helperHostPort, GlobalStateMgr.getCurrentState().isElectable());

        // setup
        bdbEnvironment.setup();
        return bdbEnvironment;
    }

    public static String getBdbDir() {
        return Config.meta_dir + BDB_DIR;
    }

    protected BDBEnvironment(File envHome, String selfNodeName, String selfNodeHostPort,
                          String helperHostPort, boolean isElectable) {
        this.envHome = envHome;
        this.selfNodeName = selfNodeName;
        this.selfNodeHostPort = selfNodeHostPort;
        this.helperHostPort = helperHostPort;
        this.isElectable = isElectable;
    }

    // The setup() method opens the environment and database
    protected void setup() throws JournalException, InterruptedException {
        this.closing = false;
        ensureHelperInLocal();
        initConfigs(isElectable);
        setupEnvironment();
    }

    protected void initConfigs(boolean isElectable) throws JournalException {
        // Almost never used, just in case the master can not restart
        if (Config.metadata_failure_recovery.equals("true")) {
            if (!isElectable) {
                String errMsg = "Current node is not in the electable_nodes list. will exit";
                LOG.error(errMsg);
                throw new JournalException(errMsg);
            }
            DbResetRepGroup resetUtility = new DbResetRepGroup(envHome, STARROCKS_JOURNAL_GROUP, selfNodeName,
                    selfNodeHostPort);
            resetUtility.reset();
            LOG.info("group has been reset.");
        }

        // set replication config
        replicationConfig = new ReplicationConfig();
        replicationConfig.setNodeName(selfNodeName);
        replicationConfig.setNodeHostPort(selfNodeHostPort);
        replicationConfig.setHelperHosts(helperHostPort);
        replicationConfig.setGroupName(STARROCKS_JOURNAL_GROUP);
        replicationConfig.setConfigParam(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "10");
        replicationConfig.setMaxClockDelta(Config.max_bdbje_clock_delta_ms, TimeUnit.MILLISECONDS);
        replicationConfig.setConfigParam(ReplicationConfig.TXN_ROLLBACK_LIMIT,
                String.valueOf(Config.txn_rollback_limit));
        replicationConfig
                .setConfigParam(ReplicationConfig.REPLICA_TIMEOUT, Config.bdbje_heartbeat_timeout_second + " s");
        replicationConfig
                .setConfigParam(ReplicationConfig.FEEDER_TIMEOUT, Config.bdbje_heartbeat_timeout_second + " s");
        replicationConfig
                .setConfigParam(ReplicationConfig.REPLAY_COST_PERCENT,
                        String.valueOf(Config.bdbje_replay_cost_percent));

        if (isElectable) {
            replicationConfig.setReplicaAckTimeout(Config.bdbje_replica_ack_timeout_second, TimeUnit.SECONDS);
            replicationConfig.setConfigParam(ReplicationConfig.REPLICA_MAX_GROUP_COMMIT, "0");
            replicationConfig.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        } else {
            replicationConfig.setNodeType(NodeType.SECONDARY);
            replicationConfig.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        }

        java.util.logging.Logger parent = java.util.logging.Logger.getLogger("com.sleepycat.je");
        parent.setLevel(Level.parse(Config.bdbje_log_level));

        // set environment config
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTransactional(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setCachePercent(MEMORY_CACHE_PERCENT);
        environmentConfig.setLockTimeout(Config.bdbje_lock_timeout_second, TimeUnit.SECONDS);
        environmentConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, Config.bdbje_log_level);
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS,
                String.valueOf(Config.bdbje_cleaner_threads));
        environmentConfig.setConfigParam(EnvironmentConfig.RESERVED_DISK,
                String.valueOf(Config.bdbje_reserved_disk_size));

        if (isElectable) {
            Durability durability = new Durability(getSyncPolicy(Config.master_sync_policy),
                    getSyncPolicy(Config.replica_sync_policy), getAckPolicy(Config.replica_ack_policy));
            environmentConfig.setDurability(durability);
        }

        // set database config
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        if (isElectable) {
            dbConfig.setAllowCreate(true);
            dbConfig.setReadOnly(false);
        } else {
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
        }

        // set transaction config
        txnConfig = new TransactionConfig();
        if (isElectable) {
            txnConfig.setDurability(new Durability(
                    getSyncPolicy(Config.master_sync_policy),
                    getSyncPolicy(Config.replica_sync_policy),
                    getAckPolicy(Config.replica_ack_policy)));
        }
    }

    protected void setupEnvironment() throws JournalException, InterruptedException {
        // open environment and epochDB
        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            if (i > 0) {
                Thread.sleep(SLEEP_INTERVAL_SEC * 1000L);
            }
            try {
                LOG.info("start to setup bdb environment for {} times", i + 1);
                replicatedEnvironment = new ReplicatedEnvironment(envHome, replicationConfig, environmentConfig);

                // get a BDBHA object and pass the reference to GlobalStateMgr
                HAProtocol protocol = new BDBHA(this, selfNodeName);
                GlobalStateMgr.getCurrentState().setHaProtocol(protocol);

                // start state change listener
                BDBStateChangeListener listener = new BDBStateChangeListener(isElectable);
                replicatedEnvironment.setStateChangeListener(listener);

                LOG.info("replicated environment is all set, wait for state change...");
                // wait for master change, otherwise a ReplicaWriteException exception will be thrown
                for (int j = 0; j < INITIAL_STATE_CHANGE_WAIT_SEC; j++) {
                    if (FrontendNodeType.UNKNOWN != listener.getNewType()) {
                        break;
                    }
                    Thread.sleep(1000);
                }
                LOG.info("state change done, current role {}", listener.getNewType());

                // open epochDB. the first parameter null means auto-commit
                epochDB = new CloseSafeDatabase(replicatedEnvironment.openDatabase(null, "epochDB", dbConfig));
                LOG.info("end setup bdb environment after {} times", i + 1);
                return;
            } catch (DatabaseException e) {
                if (i == 0 && e instanceof UnknownMasterException) {
                    // The node may be unable to join the group because the Master could not be determined because a
                    // master was present but lacked a {@link QuorumPolicy#SIMPLE_MAJORITY} needed to update the
                    // environment with information about this node, if it's a new node and is joining the group for
                    // the first time.
                    LOG.warn(
                            "failed to setup environment because of UnknownMasterException for the first time, ignore it.");
                    continue;
                }
                String errMsg = String.format("failed to setup environment after retried %d times", i + 1);
                LOG.error(errMsg, e);
                exception = new JournalException(errMsg);
                exception.initCause(e);
                if (e instanceof InsufficientLogException) {
                    refreshLog((InsufficientLogException) e);
                }
                // close before next attempt
                if (replicatedEnvironment != null && ! replicatedEnvironment.isValid()) {
                    close();
                }
            }
        }

        // failed after retry
        throw exception;
    }

    /**
     * This method is used to check if the local replicated environment matches that of the helper.
     * This could happen in a situation like this:
     * 1. User adds a follower and starts the new follower without helper.
     *    --> The new follower will run as a master in a standalone environment.
     * 2. User restarts this follower with a helper.
     *    --> Sometimes this new follower will join the group successfully, making master crash.
     * This method only init the replicated environment through a handshake.
     * It will not read or write any data.
     */
    protected void ensureHelperInLocal() throws JournalException, InterruptedException {
        if (!isElectable) {
            LOG.info("skip check local environment for observer");
            return;
        }

        if (selfNodeHostPort.equals(helperHostPort)) {
            LOG.info("skip check local environment because helper node and local node are identical.");
            return;
        }

        // Almost never used, just in case the master can not restart
        if (Config.metadata_failure_recovery.equals("true")) {
            LOG.info("skip check local environment because metadata_failure_recovery = true");
            return;
        }

        LOG.info("start to check if local replica environment from {} contains {}", envHome, helperHostPort);

        // 1. init environment as an observer
        initConfigs(false);

        HostAndPort hostAndPort = HostAndPort.fromString(helperHostPort);

        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            if (i > 0) {
                Thread.sleep(SLEEP_INTERVAL_SEC * 1000L);
            }

            try {
                // 2. get local nodes
                replicatedEnvironment = new ReplicatedEnvironment(envHome, replicationConfig, environmentConfig);
                Set<ReplicationNode> localNodes = replicatedEnvironment.getGroup().getNodes();
                if (localNodes.isEmpty()) {
                    LOG.info("skip check empty environment");
                    return;
                }

                // 3. found if match
                for (ReplicationNode node : localNodes) {
                    if (node.getHostName().equals(hostAndPort.getHost()) && node.getPort() == hostAndPort.getPort()) {
                        LOG.info("found {} in local environment!", helperHostPort);
                        return;
                    }
                }

                // 4. helper not found in local, raise exception
                throw new JournalException(
                        String.format("bad environment %s! helper host %s not in local %s",
                                envHome, helperHostPort, localNodes));
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to check if helper in local after retried %d times", i + 1);
                LOG.error(errMsg, e);
                exception = new JournalException(errMsg);
                exception.initCause(e);
                if (e instanceof InsufficientLogException) {
                    refreshLog((InsufficientLogException) e);
                }
            } finally {
                if (replicatedEnvironment != null) {
                    replicatedEnvironment.close();
                }
            }
        }

        // failed after retry
        throw exception;
    }


    public void refreshLog(InsufficientLogException insufficientLogEx) {
        try {
            NetworkRestore restore = new NetworkRestore();
            NetworkRestoreConfig config = new NetworkRestoreConfig();
            config.setRetainLogFiles(false); // delete obsolete log files.
            // Use the members returned by insufficientLogEx.getLogProviders()
            // to select the desired subset of members and pass the resulting
            // list as the argument to config.setLogProviders(), if the
            // default selection of providers is not suitable.
            restore.execute(insufficientLogEx, config);
        } catch (Throwable t) {
            LOG.warn("refresh log failed", t);
        }
    }

    public ReplicationGroupAdmin getReplicationGroupAdmin() {
        Set<InetSocketAddress> addrs = GlobalStateMgr.getCurrentState()
                .getFrontends(FrontendNodeType.FOLLOWER)
                .stream()
                .filter(Frontend::isAlive)
                .map(fe -> new InetSocketAddress(fe.getHost(), fe.getEditLogPort()))
                .collect(Collectors.toSet());
        return new ReplicationGroupAdmin(STARROCKS_JOURNAL_GROUP, addrs);
    }

    // Return a handle to the epochDB
    public CloseSafeDatabase getEpochDB() {
        return epochDB;
    }

    // Return a handle to the environment
    public ReplicatedEnvironment getReplicatedEnvironment() {
        return replicatedEnvironment;
    }

    /**
     * open database and return a CloseSafeDatabase instance
     * We should make sure no database conflict from upper level
     */
    public CloseSafeDatabase openDatabase(String dbName) {
        return new CloseSafeDatabase(replicatedEnvironment.openDatabase(null, dbName, dbConfig));
    }

    /**
     * Remove the database whose name is dbName
     * We should make sure no database conflict from upper level
     **/
    public void removeDatabase(String dbName) {
        if (closing) {
            return;
        }

        try {
            // the first parameter null means auto-commit
            replicatedEnvironment.removeDatabase(null, dbName);
            LOG.info("remove database {} from replicatedEnvironment successfully", dbName);
        } catch (DatabaseNotFoundException e) {
            LOG.warn("Exception: {} does not exist", dbName, e);
        }
    }

    public List<Long> getDatabaseNames() {
        return getDatabaseNamesWithPrefix("");
    }

    // get journal db names and sort the names
    // let the caller retry from outside.
    // return null only if environment is closing
    public List<Long> getDatabaseNamesWithPrefix(String prefix) {
        if (closing) {
            return null;
        }

        List<Long> ret = new ArrayList<>();
        List<String> names = replicatedEnvironment.getDatabaseNames();
        for (String name : names) {
            // We don't count epochDB
            if (name.equals("epochDB")) {
                continue;
            }

            if (Strings.isNullOrEmpty(prefix)) { // default GlobalStateMgr db
                if (StringUtils.isNumeric(name)) {
                    long db = Long.parseLong(name);
                    ret.add(db);
                } else {
                    // skip non default GlobalStateMgr db
                }
            } else {
                if (name.startsWith(prefix)) {
                    String dbStr = name.substring(prefix.length());
                    if (StringUtils.isNumeric(dbStr)) {
                        long db = Long.parseLong(dbStr);
                        ret.add(db);
                    } else {
                        // prefix does not fully match, ignore
                    }
                } else {
                    // prefix does not match, ignore
                }
            }
        }

        Collections.sort(ret);
        return ret;
    }

    // Close the store and environment
    public boolean close() {
        boolean closeSuccess = true;
        try {
            closing = true;

            LOG.info("start to close epoch database");
            if (epochDB != null) {
                try {
                    epochDB.close();
                } catch (DatabaseException exception) {
                    LOG.error("Error closing db {}", epochDB.getDatabaseName(), exception);
                    closeSuccess = false;
                }
            }
            LOG.info("close epoch database end");

            LOG.info("start to close replicated environment");
            if (replicatedEnvironment != null) {
                try {
                    // Finally, close the store and environment.
                    replicatedEnvironment.close();
                } catch (DatabaseException exception) {
                    LOG.error("Error closing replicatedEnvironment", exception);
                    closeSuccess = false;
                }
            }
            LOG.info("close replicated environment end");
        } finally {
            closing = false;
        }
        return closeSuccess;
    }

    public void flushVLSNMapping() {
        if (replicatedEnvironment != null) {
            RepInternal.getRepImpl(replicatedEnvironment).getVLSNIndex()
                    .flushToDatabase(Durability.COMMIT_SYNC);
        }
    }

    private SyncPolicy getSyncPolicy(String policy) {
        if (policy.equalsIgnoreCase("SYNC")) {
            return Durability.SyncPolicy.SYNC;
        }
        if (policy.equalsIgnoreCase("NO_SYNC")) {
            return Durability.SyncPolicy.NO_SYNC;
        }
        // default value is WRITE_NO_SYNC
        return Durability.SyncPolicy.WRITE_NO_SYNC;
    }

    private ReplicaAckPolicy getAckPolicy(String policy) {
        if (policy.equalsIgnoreCase("ALL")) {
            return Durability.ReplicaAckPolicy.ALL;
        }
        if (policy.equalsIgnoreCase("NONE")) {
            return Durability.ReplicaAckPolicy.NONE;
        }
        // default value is SIMPLE_MAJORITY
        return Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
    }

    /**
     * package private, used within com.starrocks.journal.bdbje
     */
    TransactionConfig getTxnConfig() {
        return txnConfig;
    }
}
