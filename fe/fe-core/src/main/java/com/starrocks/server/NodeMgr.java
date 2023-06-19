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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

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

package com.starrocks.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.NetUtils;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.http.meta.MetaBaseAction;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.Storage;
import com.starrocks.persist.StorageInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUpdateResourceUsageResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NodeMgr {
    private static final Logger LOG = LogManager.getLogger(NodeMgr.class);
    private static final int HTTP_TIMEOUT_SECOND = 5;

    /**
     * LeaderInfo
     */
    @SerializedName(value = "r")
    private int leaderRpcPort;
    @SerializedName(value = "h")
    private int leaderHttpPort;
    @SerializedName(value = "ip")
    private String leaderIp;

    /**
     * Frontends
     * <p>
     * frontends : name -> Frontend
     * removedFrontends: removed frontends' name. used for checking if name is duplicated in bdbje
     */
    @SerializedName(value = "f")
    private ConcurrentHashMap<String, Frontend> frontends = new ConcurrentHashMap<>();
    @SerializedName(value = "rf")
    private ConcurrentLinkedQueue<String> removedFrontends = new ConcurrentLinkedQueue<>();

    /**
     * Backends and Compute Node
     */
    @SerializedName(value = "s")
    private SystemInfoService systemInfo;

    /**
     * Broker
     */
    @SerializedName(value = "b")
    private BrokerMgr brokerMgr;

    private boolean isFirstTimeStartUp = false;
    private boolean isElectable;

    // node name is used for bdbje NodeName.
    private String nodeName;
    private FrontendNodeType role;

    private int clusterId;
    private String token;
    private String runMode;
    private String imageDir;

    private final List<Pair<String, Integer>> helperNodes = Lists.newArrayList();
    private Pair<String, Integer> selfNode = null;

    private final Map<Integer, SystemInfoService> systemInfoMap = new ConcurrentHashMap<>();

    public NodeMgr() {
        this.role = FrontendNodeType.UNKNOWN;
        this.leaderRpcPort = 0;
        this.leaderHttpPort = 0;
        this.leaderIp = "";
        this.systemInfo = new SystemInfoService();

        this.brokerMgr = new BrokerMgr();
    }

    public void initialize(String[] args) throws Exception {
        getCheckedSelfHostPort();
        getHelperNodes(args);
    }

    private boolean tryLock(boolean mustLock) {
        return GlobalStateMgr.getCurrentState().tryLock(mustLock);
    }

    private void unlock() {
        GlobalStateMgr.getCurrentState().unlock();
    }

    public List<Frontend> getFrontends(FrontendNodeType nodeType) {
        if (nodeType == null) {
            // get all
            return Lists.newArrayList(frontends.values());
        }

        List<Frontend> result = Lists.newArrayList();
        for (Frontend frontend : frontends.values()) {
            if (frontend.getRole() == nodeType) {
                result.add(frontend);
            }
        }

        return result;
    }

    public List<String> getRemovedFrontendNames() {
        return Lists.newArrayList(removedFrontends);
    }

    public SystemInfoService getOrCreateSystemInfo(Integer clusterId) {
        SystemInfoService systemInfoService = systemInfoMap.get(clusterId);
        if (systemInfoService == null) {
            systemInfoService = new SystemInfoService();
            systemInfoMap.put(clusterId, systemInfoService);
        }
        return systemInfoService;
    }

    public SystemInfoService getClusterInfo() {
        return this.systemInfo;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
    }

    public void getClusterIdAndRoleOnStartup() throws IOException {
        File roleFile = new File(this.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(this.imageDir, Storage.VERSION_FILE);

        boolean isVersionFileChanged = false;

        Storage storage = new Storage(this.imageDir);

        // if helper node is point to self, or there is ROLE and VERSION file in local.
        // get the node type from local
        if (isMyself() || (roleFile.exists() && versionFile.exists())) {

            if (!isMyself()) {
                LOG.info("find ROLE and VERSION file in local, ignore helper nodes: {}", helperNodes);
            }

            // check file integrity, if has.
            if ((roleFile.exists() && !versionFile.exists())
                    || (!roleFile.exists() && versionFile.exists())) {
                LOG.error("role file and version file must both exist or both not exist. "
                        + "please specific one helper node to recover. will exit.");
                System.exit(-1);
            }

            // ATTN:
            // If the version file and role file does not exist and the helper node is itself,
            // this should be the very beginning startup of the cluster, so we create ROLE and VERSION file,
            // set isFirstTimeStartUp to true, and add itself to frontends list.
            // If ROLE and VERSION file is deleted for some reason, we may arbitrarily start this node as
            // FOLLOWER, which may cause UNDEFINED behavior.
            // Everything may be OK if the origin role is exactly FOLLOWER,
            // but if not, FE process will exit somehow.
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should became a Master node (Master node's role is also FOLLOWER, which means electable)

                // For compatibility. Because this is the very first time to start, so we arbitrarily choose
                // a new name for this node
                role = FrontendNodeType.FOLLOWER;
                nodeName = GlobalStateMgr.genFeNodeName(selfNode.first, selfNode.second, false /* new style */);
                storage.writeFrontendRoleAndNodeName(role, nodeName);
                LOG.info("very first time to start this node. role: {}, node name: {}", role.name(), nodeName);
            } else {
                role = storage.getRole();
                nodeName = storage.getNodeName();
                if (Strings.isNullOrEmpty(nodeName)) {
                    // In normal case, if ROLE file exist, role and nodeName should both exist.
                    // But we will get a empty nodeName after upgrading.
                    // So for forward compatibility, we use the "old-style" way of naming: "ip_port",
                    // and update the ROLE file.
                    nodeName = GlobalStateMgr.genFeNodeName(selfNode.first, selfNode.second, true/* old style */);
                    storage.writeFrontendRoleAndNodeName(role, nodeName);
                    LOG.info("forward compatibility. role: {}, node name: {}", role.name(), nodeName);
                }
            }
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            if (!versionFile.exists()) {
                clusterId = Config.cluster_id == -1 ? Storage.newClusterID() : Config.cluster_id;
                token = Strings.isNullOrEmpty(Config.auth_token) ?
                        Storage.newToken() : Config.auth_token;
                storage = new Storage(clusterId, token, this.imageDir);
                isVersionFileChanged = true;

                isFirstTimeStartUp = true;
                Frontend self = new Frontend(role, nodeName, selfNode.first, selfNode.second);
                // We don't need to check if frontends already contains self.
                // frontends must be empty cause no image is loaded and no journal is replayed yet.
                // And this frontend will be persisted later after opening bdbje environment.
                frontends.put(nodeName, self);
            } else {
                clusterId = storage.getClusterID();
                if (storage.getToken() == null) {
                    token = Strings.isNullOrEmpty(Config.auth_token) ?
                            Storage.newToken() : Config.auth_token;
                    LOG.info("new token={}", token);
                    storage.setToken(token);
                    isVersionFileChanged = true;
                } else {
                    token = storage.getToken();
                }
                runMode = storage.getRunMode();
                isFirstTimeStartUp = false;
            }
        } else {
            // try to get role and node name from helper node,
            // this loop will not end until we get certain role type and name
            while (true) {
                if (!getFeNodeTypeAndNameFromHelpers()) {
                    LOG.warn("current node is not added to the group. please add it first. "
                            + "sleep 5 seconds and retry, current helper nodes: {}", helperNodes);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e) {
                        LOG.warn(e);
                        System.exit(-1);
                    }
                }

                break;
            }

            Preconditions.checkState(helperNodes.size() == 1);
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            Pair<String, Integer> rightHelperNode = helperNodes.get(0);

            storage = new Storage(this.imageDir);
            if (roleFile.exists() && (role != storage.getRole() || !nodeName.equals(storage.getNodeName()))
                    || !roleFile.exists()) {
                storage.writeFrontendRoleAndNodeName(role, nodeName);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper node
                if (!getVersionFileFromHelper(rightHelperNode)) {
                    System.exit(-1);
                }

                // NOTE: cluster_id will be init when Storage object is constructed,
                //       so we new one.
                storage = new Storage(this.imageDir);
                clusterId = storage.getClusterID();
                token = storage.getToken();
                runMode = storage.getRunMode();
                if (Strings.isNullOrEmpty(token)) {
                    token = Config.auth_token;
                    isVersionFileChanged = true;
                }
                if (Strings.isNullOrEmpty(runMode)) {
                    // The version of helper node is less than 3.0, run at SAHRED_NOTHING mode and save the run
                    // mode in version file later.
                    runMode = RunMode.SHARED_NOTHING.getName();
                    storage.setRunMode(runMode);
                    isVersionFileChanged = true;
                }
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node to make sure they are identical
                clusterId = storage.getClusterID();
                token = storage.getToken();
                runMode = storage.getRunMode();
                if (Strings.isNullOrEmpty(runMode)) {
                    // No run mode saved in the version file, we're upgrading an old cluster of version less than 3.0.
                    runMode = RunMode.SHARED_NOTHING.getName();
                    storage.setRunMode(runMode);
                    isVersionFileChanged = true;
                }
                try {
                    URL idURL = new URL("http://" + rightHelperNode.first + ":" + Config.http_port + "/check");
                    HttpURLConnection conn = null;
                    conn = (HttpURLConnection) idURL.openConnection();
                    conn.setConnectTimeout(2 * 1000);
                    conn.setReadTimeout(2 * 1000);
                    String clusterIdString = conn.getHeaderField(MetaBaseAction.CLUSTER_ID);
                    int remoteClusterId = Integer.parseInt(clusterIdString);
                    if (remoteClusterId != clusterId) {
                        LOG.error("cluster id is not equal with helper node {}. will exit.", rightHelperNode.first);
                        System.exit(-1);
                    }

                    String remoteToken = conn.getHeaderField(MetaBaseAction.TOKEN);
                    if (token == null && remoteToken != null) {
                        LOG.info("get token from helper node. token={}.", remoteToken);
                        token = remoteToken;
                        isVersionFileChanged = true;
                        storage.reload();
                    }
                    if (Config.enable_token_check) {
                        Preconditions.checkNotNull(token);
                        Preconditions.checkNotNull(remoteToken);
                        if (!token.equals(remoteToken)) {
                            LOG.error("token is not equal with helper node {}. will exit.", rightHelperNode.first);
                            System.exit(-1);
                        }
                    }

                    String remoteRunMode = conn.getHeaderField(MetaBaseAction.RUN_MODE);
                    if (Strings.isNullOrEmpty(remoteRunMode)) {
                        // The version of helper node is less than 3.0
                        remoteRunMode = RunMode.SHARED_NOTHING.getName();
                    }

                    if (!runMode.equalsIgnoreCase(remoteRunMode)) {
                        LOG.error("Unmatched run mode with helper node {}: {} vs {}, will exit .",
                                rightHelperNode.first, runMode, remoteRunMode);
                        System.exit(-1);
                    }
                } catch (Exception e) {
                    LOG.warn("fail to check cluster_id and token with helper node.", e);
                    System.exit(-1);
                }
            }
            getNewImageOnStartup(rightHelperNode, "");
            if (RunMode.allowCreateLakeTable()) { // get star mgr image
                // subdir might not exist
                String subDir = this.imageDir + StarMgrServer.IMAGE_SUBDIR;
                File dir = new File(subDir);
                if (!dir.exists()) { // subDir might not exist
                    LOG.info("create image dir for {}.", dir.getAbsolutePath());
                    if (!dir.mkdir()) {
                        LOG.error("create image dir for star mgr failed! exit now.");
                        System.exit(-1);
                    }
                }
                getNewImageOnStartup(rightHelperNode, StarMgrServer.IMAGE_SUBDIR);
            }
        }

        if (Config.cluster_id != -1 && clusterId != Config.cluster_id) {
            LOG.error("cluster id is not equal with config item cluster_id. will exit.");
            System.exit(-1);
        }


        if (Strings.isNullOrEmpty(runMode)) {
            if (isFirstTimeStartUp) {
                runMode = RunMode.name();
                storage.setRunMode(runMode);
                isVersionFileChanged = true;
            } else if (RunMode.allowCreateLakeTable()) {
                LOG.error("Upgrading from a cluster with version less than 3.0 to a cluster with run mode {} of " +
                        "version 3.0 or above is disallowed. will exit", RunMode.name());
                System.exit(-1);
            }
        } else if (!runMode.equalsIgnoreCase(RunMode.name())) {
            LOG.error("Unmatched run mode between config file and version file: {} vs {}. will exit! ",
                    RunMode.name(), runMode);
            System.exit(-1);
        } // else nothing to do

        if (isVersionFileChanged) {
            storage.writeVersionFile();
        }

        // Tell user current run_mode
        LOG.info("Current run_mode is {}", runMode);

        isElectable = role.equals(FrontendNodeType.FOLLOWER);

        systemInfoMap.put(clusterId, systemInfo);

        Preconditions.checkState(helperNodes.size() == 1);
        LOG.info("Got cluster id: {}, role: {}, node name: {} and run_mode: {}",
                clusterId, role.name(), nodeName, runMode);
    }

    // Get the role info and node name from helper node.
    // return false if failed.
    private boolean getFeNodeTypeAndNameFromHelpers() {
        // we try to get info from helper nodes, once we get the right helper node,
        // other helper nodes will be ignored and removed.
        Pair<String, Integer> rightHelperNode = null;
        for (Pair<String, Integer> helperNode : helperNodes) {
            try {
                URL url = new URL("http://" + helperNode.first + ":" + Config.http_port
                        + "/role?host=" + selfNode.first + "&port=" + selfNode.second);
                HttpURLConnection conn = null;
                conn = (HttpURLConnection) url.openConnection();
                if (conn.getResponseCode() != 200) {
                    LOG.warn("failed to get fe node type from helper node: {}. response code: {}",
                            helperNode, conn.getResponseCode());
                    continue;
                }

                String type = conn.getHeaderField("role");
                if (type == null) {
                    LOG.warn("failed to get fe node type from helper node: {}.", helperNode);
                    continue;
                }
                role = FrontendNodeType.valueOf(type);
                nodeName = conn.getHeaderField("name");

                // get role and node name before checking them, because we want to throw any exception
                // as early as we encounter.

                if (role == FrontendNodeType.UNKNOWN) {
                    LOG.warn("frontend {} is not added to cluster yet. role UNKNOWN", selfNode);
                    return false;
                }

                if (Strings.isNullOrEmpty(nodeName)) {
                    // For forward compatibility, we use old-style name: "ip_port"
                    nodeName = GlobalStateMgr.genFeNodeName(selfNode.first, selfNode.second, true /* old style */);
                }
            } catch (Exception e) {
                LOG.warn("failed to get fe node type from helper node: {}.", helperNode, e);
                continue;
            }

            LOG.info("get fe node type {}, name {} from {}:{}", role, nodeName, helperNode.first, Config.http_port);
            rightHelperNode = helperNode;
            break;
        }

        if (rightHelperNode == null) {
            return false;
        }

        helperNodes.clear();
        helperNodes.add(rightHelperNode);
        return true;
    }

    private boolean isMyself() {
        Preconditions.checkNotNull(selfNode);
        Preconditions.checkNotNull(helperNodes);
        LOG.debug("self: {}. helpers: {}", selfNode, helperNodes);
        // if helper nodes contain it self, remove other helpers
        boolean containSelf = false;
        for (Pair<String, Integer> helperNode : helperNodes) {
            if (selfNode.equals(helperNode)) {
                containSelf = true;
            }
        }
        if (containSelf) {
            helperNodes.clear();
            helperNodes.add(selfNode);
        }

        return containSelf;
    }

    public long loadFrontends(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        for (int i = 0; i < size; i++) {
            Frontend fe = Frontend.read(dis);
            replayAddFrontend(fe);
        }

        size = dis.readInt();
        newChecksum ^= size;
        for (int i = 0; i < size; i++) {
            removedFrontends.add(Text.readString(dis));
        }
        LOG.info("finished replay frontends from image");
        return newChecksum;
    }

    public long saveFrontends(DataOutputStream dos, long checksum) throws IOException {
        int size = frontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (Frontend fe : frontends.values()) {
            fe.write(dos);
        }

        size = removedFrontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (String feName : removedFrontends) {
            Text.writeString(dos, feName);
        }

        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        return systemInfo.loadBackends(dis, checksum);
    }

    public long saveBackends(DataOutputStream dos, long checksum) throws IOException {
        return systemInfo.saveBackends(dos, checksum);
    }

    public long loadComputeNodes(DataInputStream dis, long checksum) throws IOException {
        return systemInfo.loadComputeNodes(dis, checksum);
    }

    public long saveComputeNodes(DataOutputStream dos, long checksum) throws IOException {
        return systemInfo.saveComputeNodes(dos, checksum);
    }

    private StorageInfo getStorageInfo(URL url) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(HTTP_TIMEOUT_SECOND * 1000);
            connection.setReadTimeout(HTTP_TIMEOUT_SECOND * 1000);
            return mapper.readValue(connection.getInputStream(), StorageInfo.class);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void getHelperNodes(String[] args) throws AnalysisException {
        String helpers = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-helper")) {
                if (i + 1 >= args.length) {
                    System.out.println("-helper need parameter host:port,host:port");
                    System.exit(-1);
                }
                helpers = args[i + 1];
                if (!helpers.contains(":")) {
                    System.out.print("helper's format seems was wrong [" + helpers + "]");
                    System.out.println(", eg. host:port,host:port");
                    System.exit(-1);
                }
                break;
            }
        }

        if (helpers != null) {
            String[] splittedHelpers = helpers.split(",");
            for (String helper : splittedHelpers) {
                Pair<String, Integer> helperHostPort = SystemInfoService.validateHostAndPort(helper);
                if (helperHostPort.equals(selfNode)) {
                    /*
                     * If user specified the helper node to this FE itself,
                     * we will stop the starting FE process and report an error.
                     * First, it is meaningless to point the helper to itself.
                     * Secondly, when some users add FE for the first time, they will mistakenly
                     * point the helper that should have pointed to the Master to themselves.
                     * In this case, some errors have caused users to be troubled.
                     * So here directly exit the program and inform the user to avoid unnecessary trouble.
                     */
                    throw new AnalysisException(
                            "Do not specify the helper node to FE itself. "
                                    + "Please specify it to the existing running Leader or Follower FE");
                }
                helperNodes.add(helperHostPort);
            }
        } else {
            // If helper node is not designated, use local node as helper node.
            helperNodes.add(Pair.create(selfNode.first, Config.edit_log_port));
        }

        LOG.info("get helper nodes: {}", helperNodes);
    }

    private void getCheckedSelfHostPort() {
        selfNode = new Pair<>(FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
        /*
         * For the first time, if the master start up failed, it will also fail to restart.
         * Check port using before create meta files to avoid this problem.
         */
        try {
            if (NetUtils.isPortUsing(selfNode.first, selfNode.second)) {
                LOG.error("edit_log_port {} is already in use. will exit.", selfNode.second);
                System.exit(-1);
            }
        } catch (UnknownHostException e) {
            LOG.error(e);
            System.exit(-1);
        }
        LOG.debug("get self node: {}", selfNode);
    }

    public Pair<String, Integer> getHelperNode() {
        Preconditions.checkState(helperNodes.size() >= 1);
        return this.helperNodes.get(0);
    }

    public List<Pair<String, Integer>> getHelperNodes() {
        return Lists.newArrayList(helperNodes);
    }

    /*
     * If the current node is not in the frontend list, then exit. This may
     * happen when this node is removed from frontend list, and the drop
     * frontend log is deleted because of checkpoint.
     */
    public void checkCurrentNodeExist() {
        if (Config.metadata_failure_recovery.equals("true")) {
            return;
        }

        Frontend fe = checkFeExist(selfNode.first, selfNode.second);
        if (fe == null) {
            LOG.error("current node is not added to the cluster, will exit");
            System.exit(-1);
        } else if (fe.getRole() != role) {
            LOG.error("current node role is {} not match with frontend recorded role {}. will exit", role,
                    fe.getRole());
            System.exit(-1);
        }
    }

    private boolean getVersionFileFromHelper(Pair<String, Integer> helperNode) throws IOException {
        String url = "http://" + helperNode.first + ":" + Config.http_port + "/version";
        LOG.info("Downloading version file from {}", url);
        try {
            File dir = new File(this.imageDir);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    MetaHelper.getOutputStream(Storage.VERSION_FILE, dir));
            MetaHelper.complete(Storage.VERSION_FILE, dir);
            return true;
        } catch (Exception e) {
            LOG.warn("Fail to download version file from {}:{}", url, e.getMessage());
        }

        return false;
    }

    /**
     * When a new node joins in the cluster for the first time, it will download image from the helper at the very beginning
     * Exception are free to raise on initialized phase
     */
    private void getNewImageOnStartup(Pair<String, Integer> helperNode, String subDir) throws IOException {
        long localImageVersion = 0;
        String dirStr = this.imageDir + subDir;
        Storage storage = new Storage(dirStr);
        localImageVersion = storage.getImageJournalId();

        URL infoUrl = new URL("http://" + helperNode.first + ":" + Config.http_port + "/info?subdir=" + subDir);
        StorageInfo info = getStorageInfo(infoUrl);
        long version = info.getImageJournalId();
        if (version > localImageVersion) {
            String url = "http://" + helperNode.first + ":" + Config.http_port
                    + "/image?version=" + version + "&subdir=" + subDir;
            LOG.info("start to download image.{} from {}", version, url);
            String filename = Storage.IMAGE + "." + version;
            File dir = new File(dirStr);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000, MetaHelper.getOutputStream(filename, dir));
            MetaHelper.complete(filename, dir);
        } else {
            LOG.info("skip download image for {}, current version {} >= version {} from {}",
                    dirStr, localImageVersion, version, helperNode);
        }
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            try {
                if (checkFeExistByIpOrFqdn(host)) {
                    throw new DdlException("FE with the same host: " + host + " already exists");
                }
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", host, e);
                throw new DdlException("unknown fqdn host: " + host);
            }

            String nodeName = GlobalStateMgr.genFeNodeName(host, editLogPort, false /* new name style */);

            if (removedFrontends.contains(nodeName)) {
                throw new DdlException("frontend name already exists " + nodeName + ". Try again");
            }

            Frontend fe = new Frontend(role, nodeName, host, editLogPort);
            frontends.put(nodeName, fe);
            if (role == FrontendNodeType.FOLLOWER) {
                helperNodes.add(Pair.create(host, editLogPort));
            }
            if (GlobalStateMgr.getCurrentState().getHaProtocol() instanceof BDBHA) {
                BDBHA bdbha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
                if (role == FrontendNodeType.FOLLOWER) {
                    bdbha.addUnstableNode(host, getFollowerCnt());
                }

                // In some cases, for example, fe starts with the outdated meta, the node name that has been dropped
                // will remain in bdb.
                // So we should remove those nodes before joining the group,
                // or it will throws NodeConflictException (New or moved node:xxxx, is configured with the socket address:
                // xxx. It conflicts with the socket already used by the member: xxxx)
                bdbha.removeNodeIfExist(host, editLogPort, nodeName);
            }

            GlobalStateMgr.getCurrentState().getEditLog().logAddFrontend(fe);
        } finally {
            unlock();
        }
    }

    public void modifyFrontendHost(ModifyFrontendAddressClause modifyFrontendAddressClause) throws DdlException {
        String toBeModifyHost = modifyFrontendAddressClause.getSrcHost();
        String fqdn = modifyFrontendAddressClause.getDestHost();
        if (toBeModifyHost.equals(selfNode.first) && role == FrontendNodeType.LEADER) {
            throw new DdlException("can not modify current master node.");
        }
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            Frontend preUpdateFe = getFeByHost(toBeModifyHost);
            if (preUpdateFe == null) {
                throw new DdlException(String.format("frontend [%s] not found", toBeModifyHost));
            }

            Frontend existFe = null;
            for (Frontend fe : frontends.values()) {
                if (fe.getHost().equals(fqdn)) {
                    existFe = fe;
                }
            }

            if (null != existFe) {
                throw new DdlException("frontend with host [" + fqdn + "] already exists ");
            }

            // step 1 update the fe information stored in bdb
            BDBHA bdbha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
            bdbha.updateFrontendHostAndPort(preUpdateFe.getNodeName(), fqdn, preUpdateFe.getEditLogPort());
            // step 2 update the fe information stored in memory
            preUpdateFe.updateHostAndEditLogPort(fqdn, preUpdateFe.getEditLogPort());
            frontends.put(preUpdateFe.getNodeName(), preUpdateFe);

            // editLog
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateFrontend(preUpdateFe);
            LOG.info("send update fe editlog success, fe info is [{}]", preUpdateFe.toString());
        } finally {
            unlock();
        }
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        if (host.equals(selfNode.first) && port == selfNode.second &&
                GlobalStateMgr.getCurrentState().getFeType() == FrontendNodeType.LEADER) {
            throw new DdlException("can not drop current master node.");
        }
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            Frontend fe = unprotectCheckFeExist(host, port);
            if (fe == null) {
                throw new DdlException("frontend does not exist[" + host + ":" + port + "]");
            }
            if (fe.getRole() != role) {
                throw new DdlException(role.toString() + " does not exist[" + host + ":" + port + "]");
            }
            frontends.remove(fe.getNodeName());
            removedFrontends.add(fe.getNodeName());

            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                GlobalStateMgr.getCurrentState().getHaProtocol().removeElectableNode(fe.getNodeName());
                helperNodes.remove(Pair.create(host, port));

                BDBHA ha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
                ha.removeUnstableNode(host, getFollowerCnt());
            }
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveFrontend(fe);
        } finally {
            unlock();
        }
    }

    public void replayAddFrontend(Frontend fe) {
        tryLock(true);
        try {
            Frontend existFe = unprotectCheckFeExist(fe.getHost(), fe.getEditLogPort());
            if (existFe != null) {
                LOG.warn("fe {} already exist.", existFe);
                if (existFe.getRole() != fe.getRole()) {
                    /*
                     * This may happen if:
                     * 1. first, add a FE as OBSERVER.
                     * 2. This OBSERVER is restarted with ROLE and VERSION file being DELETED.
                     *    In this case, this OBSERVER will be started as a FOLLOWER, and add itself to the frontends.
                     * 3. this "FOLLOWER" begin to load image or replay journal,
                     *    then find the origin OBSERVER in image or journal.
                     * This will cause UNDEFINED behavior, so it is better to exit and fix it manually.
                     */
                    System.err.println("Try to add an already exist FE with different role" + fe.getRole());
                    System.exit(-1);
                }
                return;
            }
            frontends.put(fe.getNodeName(), fe);
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                helperNodes.add(Pair.create(fe.getHost(), fe.getEditLogPort()));
            }
        } finally {
            unlock();
        }
    }

    public void replayUpdateFrontend(Frontend frontend) {
        tryLock(true);
        try {
            Frontend fe = frontends.get(frontend.getNodeName());
            if (fe == null) {
                LOG.error("try to update frontend, but " + frontend.toString() + " does not exist.");
                return;
            }
            fe.updateHostAndEditLogPort(frontend.getHost(), frontend.getEditLogPort());
            frontends.put(fe.getNodeName(), fe);
            LOG.info("update fe successfully, fe info is [{}]", frontend.toString());
        } finally {
            unlock();
        }
    }

    public void replayDropFrontend(Frontend frontend) {
        tryLock(true);
        try {
            Frontend removedFe = frontends.remove(frontend.getNodeName());
            if (removedFe == null) {
                LOG.error(frontend.toString() + " does not exist.");
                return;
            }
            if (removedFe.getRole() == FrontendNodeType.FOLLOWER) {
                helperNodes.remove(Pair.create(removedFe.getHost(), removedFe.getEditLogPort()));
            }

            removedFrontends.add(removedFe.getNodeName());
        } finally {
            unlock();
        }
    }

    public boolean checkFeExistByRPCPort(String host, int rpcPort) {
        try {
            tryLock(true);
            return frontends
                    .values()
                    .stream()
                    .anyMatch(fe -> fe.getHost().equals(host) && fe.getRpcPort() == rpcPort);
        } finally {
            unlock();
        }
    }

    public Frontend checkFeExist(String host, int port) {
        tryLock(true);
        try {
            return unprotectCheckFeExist(host, port);
        } finally {
            unlock();
        }
    }

    public Frontend unprotectCheckFeExist(String host, int port) {
        for (Frontend fe : frontends.values()) {
            if (fe.getHost().equals(host) && fe.getEditLogPort() == port) {
                return fe;
            }
        }
        return null;
    }

    protected boolean checkFeExistByIpOrFqdn(String ipOrFqdn) throws UnknownHostException {
        Pair<String, String> targetIpAndFqdn = NetUtils.getIpAndFqdnByHost(ipOrFqdn);

        for (Frontend fe : frontends.values()) {
            Pair<String, String> curIpAndFqdn;
            try {
                curIpAndFqdn = NetUtils.getIpAndFqdnByHost(fe.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", fe.getHost(), e);
                if (targetIpAndFqdn.second.equals(fe.getHost())
                        && !Strings.isNullOrEmpty(targetIpAndFqdn.second)) {
                    return true;
                }
                continue;
            }
            // target, cur has same ip
            if (targetIpAndFqdn.first.equals(curIpAndFqdn.first)) {
                return true;
            }
            // target, cur has same fqdn and both of them are not equal ""
            if (targetIpAndFqdn.second.equals(curIpAndFqdn.second)
                    && !Strings.isNullOrEmpty(targetIpAndFqdn.second)) {
                return true;
            }
        }

        return false;
    }

    public Frontend getFeByHost(String ipOrFqdn) {
        // This host could be Ip, or fqdn
        Pair<String, String> targetPair;
        try {
            targetPair = NetUtils.getIpAndFqdnByHost(ipOrFqdn);
        } catch (UnknownHostException e) {
            LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
            return null;
        }
        for (Frontend fe : frontends.values()) {
            Pair<String, String> curPair;
            try {
                curPair = NetUtils.getIpAndFqdnByHost(fe.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
                continue;
            }
            // target, cur has same ip
            if (targetPair.first.equals(curPair.first)) {
                return fe;
            }
            // target, cur has same fqdn and both of them are not equal ""
            if (targetPair.second.equals(curPair.second) && !curPair.second.equals("")) {
                return fe;
            }
        }
        return null;
    }

    public Frontend getFeByName(String name) {
        for (Frontend fe : frontends.values()) {
            if (fe.getNodeName().equals(name)) {
                return fe;
            }
        }
        return null;
    }

    public int getFollowerCnt() {
        int cnt = 0;
        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                cnt++;
            }
        }
        return cnt;
    }

    public int getClusterId() {
        return this.clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public String getToken() {
        return token;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }

    public Pair<String, Integer> getSelfNode() {
        return this.selfNode;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public Pair<String, Integer> getLeaderIpAndRpcPort() {
        if (GlobalStateMgr.getServingState().isReady()) {
            return new Pair<>(this.leaderIp, this.leaderRpcPort);
        } else {
            String leaderNodeName = GlobalStateMgr.getServingState().getHaProtocol().getLeaderNodeName();
            Frontend frontend = frontends.get(leaderNodeName);
            return new Pair<>(frontend.getHost(), frontend.getRpcPort());
        }
    }

    public Pair<String, Integer> getLeaderIpAndHttpPort() {
        if (GlobalStateMgr.getServingState().isReady()) {
            return new Pair<>(this.leaderIp, this.leaderHttpPort);
        } else {
            String leaderNodeName = GlobalStateMgr.getServingState().getHaProtocol().getLeaderNodeName();
            Frontend frontend = frontends.get(leaderNodeName);
            return new Pair<>(frontend.getHost(), Config.http_port);
        }
    }

    public String getLeaderIp() {
        if (GlobalStateMgr.getServingState().isReady()) {
            return this.leaderIp;
        } else {
            String leaderNodeName = GlobalStateMgr.getServingState().getHaProtocol().getLeaderNodeName();
            return frontends.get(leaderNodeName).getHost();
        }
    }

    public void setLeader(LeaderInfo info) {
        this.leaderIp = info.getIp();
        this.leaderHttpPort = info.getHttpPort();
        this.leaderRpcPort = info.getRpcPort();
    }

    public void updateResourceUsage(long backendId, TResourceUsage usage) {
        List<Frontend> allFrontends = getFrontends(null);
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(getSelfNode().first)) {
                continue;
            }

            TUpdateResourceUsageRequest request = new TUpdateResourceUsageRequest();
            request.setBackend_id(backendId);
            request.setResource_usage(usage);

            try {
                TUpdateResourceUsageResponse response = FrontendServiceProxy
                        .call(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                                Config.thrift_rpc_timeout_ms,
                                Config.thrift_rpc_retry_times,
                                client -> client.updateResourceUsage(request));
                if (response.getStatus().getStatus_code() != TStatusCode.OK) {
                    LOG.warn("UpdateResourceUsage to remote fe: {} failed", fe.getHost());
                }
            } catch (Exception e) {
                LOG.warn("UpdateResourceUsage to remote fe: {} failed", fe.getHost(), e);
            }
        }
    }

    public void setConfig(AdminSetConfigStmt stmt) throws DdlException {
        setFrontendConfig(stmt.getConfig().getMap());

        List<Frontend> allFrontends = getFrontends(null);
        int timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000
                + Config.thrift_rpc_timeout_ms;
        StringBuilder errMsg = new StringBuilder();
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(getSelfNode().first)) {
                continue;
            }

            TSetConfigRequest request = new TSetConfigRequest();
            request.setKeys(Lists.newArrayList(stmt.getConfig().getKey()));
            request.setValues(Lists.newArrayList(stmt.getConfig().getValue()));
            try {
                TSetConfigResponse response = FrontendServiceProxy
                        .call(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                                timeout,
                                Config.thrift_rpc_retry_times,
                                client -> client.setConfig(request));
                TStatus status = response.getStatus();
                if (status.getStatus_code() != TStatusCode.OK) {
                    errMsg.append("set config for fe[").append(fe.getHost()).append("] failed: ");
                    if (status.getError_msgs() != null && status.getError_msgs().size() > 0) {
                        errMsg.append(String.join(",", status.getError_msgs()));
                    }
                    errMsg.append(";");
                }
            } catch (Exception e) {
                LOG.warn("set remote fe: {} config failed", fe.getHost(), e);
                errMsg.append("set config for fe[").append(fe.getHost()).append("] failed: ").append(e.getMessage());
            }
        }
        if (errMsg.length() > 0) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_SET_CONFIG_FAILED, errMsg.toString());
        }
    }

    public void setFrontendConfig(Map<String, String> configs) throws DdlException {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            ConfigBase.setMutableConfig(entry.getKey(), entry.getValue());
        }
    }

    public Frontend getMySelf() {
        return frontends.get(nodeName);
    }

    public ConcurrentHashMap<String, Frontend> getFrontends() {
        return frontends;
    }

    public long loadBrokers(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (long i = 0; i < count; ++i) {
            String brokerName = Text.readString(dis);
            int size = dis.readInt();
            checksum ^= size;
            List<FsBroker> addrs = Lists.newArrayList();
            for (int j = 0; j < size; j++) {
                FsBroker addr = FsBroker.readIn(dis);
                addrs.add(addr);
            }
            brokerMgr.replayAddBrokers(brokerName, addrs);
        }
        LOG.info("finished replay brokerMgr from image");
        return checksum;
    }

    public long saveBrokers(DataOutputStream dos, long checksum) throws IOException {
        Map<String, List<FsBroker>> addressListMap = brokerMgr.getBrokerListMap();
        int size = addressListMap.size();
        checksum ^= size;
        dos.writeInt(size);

        for (Map.Entry<String, List<FsBroker>> entry : addressListMap.entrySet()) {
            Text.writeString(dos, entry.getKey());
            final List<FsBroker> addrs = entry.getValue();
            size = addrs.size();
            checksum ^= size;
            dos.writeInt(size);
            for (FsBroker addr : addrs) {
                addr.write(dos);
            }
        }

        return checksum;
    }

    public long loadLeaderInfo(DataInputStream dis, long checksum) throws IOException {
        leaderIp = Text.readString(dis);
        leaderRpcPort = dis.readInt();
        long newChecksum = checksum ^ leaderRpcPort;
        leaderHttpPort = dis.readInt();
        newChecksum ^= leaderHttpPort;

        LOG.info("finished replay masterInfo from image");
        return newChecksum;
    }

    public long saveLeaderInfo(DataOutputStream dos, long checksum) throws IOException {
        Text.writeString(dos, leaderIp);

        checksum ^= leaderRpcPort;
        dos.writeInt(leaderRpcPort);

        checksum ^= leaderHttpPort;
        dos.writeInt(leaderHttpPort);

        return checksum;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.NODE_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        NodeMgr nodeMgr = reader.readJson(NodeMgr.class);

        leaderRpcPort = nodeMgr.leaderRpcPort;
        leaderHttpPort = nodeMgr.leaderHttpPort;
        leaderIp = nodeMgr.leaderIp;

        frontends = nodeMgr.frontends;
        removedFrontends = nodeMgr.removedFrontends;

        systemInfo = nodeMgr.systemInfo;
        systemInfoMap.put(clusterId, systemInfo);
        brokerMgr = nodeMgr.brokerMgr;
    }

    public void setLeaderInfo() {
        this.leaderIp = FrontendOptions.getLocalHostAddress();
        this.leaderRpcPort = Config.rpc_port;
        this.leaderHttpPort = Config.http_port;
        LeaderInfo info = new LeaderInfo(this.leaderIp, this.leaderHttpPort, this.leaderRpcPort);
        GlobalStateMgr.getCurrentState().getEditLog().logLeaderInfo(info);
    }

    public boolean isFirstTimeStartUp() {
        return isFirstTimeStartUp;
    }

    public boolean isElectable() {
        return isElectable;
    }

    public void setImageDir(String imageDir) {
        this.imageDir = imageDir;
    }
}
