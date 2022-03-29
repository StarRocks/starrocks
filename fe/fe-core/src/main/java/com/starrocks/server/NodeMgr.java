package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.NetUtils;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.MasterInfo;
import com.starrocks.http.meta.MetaBaseAction;
import com.starrocks.master.MetaHelper;
import com.starrocks.persist.Storage;
import com.starrocks.persist.StorageInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.system.Frontend;
import com.starrocks.system.HeartbeatMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NodeMgr {
    private static final Logger LOG = LogManager.getLogger(NodeMgr.class);
    private static final int HTTP_TIMEOUT_SECOND = 5;


    // node name is used for bdbje NodeName.
    private String nodeName;
    private FrontendNodeType role;

    private int clusterId;
    private String token;
    private boolean isFirstTimeStartUp = false;
    private boolean isElectable;

    private int masterRpcPort;
    private int masterHttpPort;
    private String masterIp;

    private HeartbeatMgr heartbeatMgr;

    private String imageDir;
    private Pair<String, Integer> selfNode = null;
    private List<Pair<String, Integer>> helperNodes = Lists.newArrayList();

    // node name -> Frontend
    private ConcurrentHashMap<String, Frontend> frontends;
    // removed frontends' name. used for checking if name is duplicated in bdbje
    private ConcurrentLinkedQueue<String> removedFrontends;

    private Map<Integer, SystemInfoService> systemInfoMap;
    private SystemInfoService systemInfo;
    private BrokerMgr brokerMgr;

    public NodeMgr(boolean isCheckpoint) {
        this.masterRpcPort = 0;
        this.masterHttpPort = 0;
        this.masterIp = "";
        this.frontends = new ConcurrentHashMap<>();
        this.removedFrontends = new ConcurrentLinkedQueue<>();
        this.systemInfo = new SystemInfoService();
        this.systemInfoMap = new ConcurrentHashMap();
        this.heartbeatMgr = new HeartbeatMgr(systemInfo, !isCheckpoint);
        this.brokerMgr = new BrokerMgr();
    }

    public void initialize(String[] args) throws Exception {
        getCheckedSelfHostPort();
        getHelperNodes(args);
    }

    public void getCheckedSelfHostPort() {
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

    public SystemInfoService getOrCreateSystemInfo(Integer clusterId) {
        SystemInfoService systemInfoService = systemInfoMap.get(clusterId);
        if (systemInfoService == null) {
            systemInfoService = new SystemInfoService();
            systemInfoMap.put(clusterId, systemInfoService);
        }
        return systemInfoService;
    }

    public Pair<String, Integer> getHelperNode() {
        Preconditions.checkState(helperNodes.size() >= 1);
        return this.helperNodes.get(0);
    }

    public void getHelperNodes(String[] args) throws AnalysisException {
        String helpers = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-helper")) {
                if (i + 1 >= args.length) {
                    System.out.println("-helper need parameter host:port,host:port");
                    System.exit(-1);
                }
                helpers = args[i + 1];
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
                                    + "Please specify it to the existing running Master or Follower FE");
                }
                helperNodes.add(helperHostPort);
            }
        } else {
            // If helper node is not designated, use local node as helper node.
            helperNodes.add(Pair.create(selfNode.first, Config.edit_log_port));
        }

        LOG.info("get helper nodes: {}", helperNodes);
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

    public List<String> getRemovedFrontendNames() {
        return Lists.newArrayList(removedFrontends);
    }

    public void addHelperSockets() {
        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                ((BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol()).addHelperSocket(fe.getHost(), fe.getEditLogPort());
            }
        }
    }

    public void startHearbeat(long epoch) {
        heartbeatMgr.setMaster(clusterId, token, epoch);
        heartbeatMgr.start();
    }

    public void getClusterIdAndRole() throws IOException {
        File roleFile = new File(this.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(this.imageDir, Storage.VERSION_FILE);

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
            Storage storage = new Storage(this.imageDir);
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should became a Master node (Master node's role is also FOLLOWER, which means electable)

                // For compatibility. Because this is the very first time to start, so we arbitrarily choose
                // a new name for this node
                role = FrontendNodeType.FOLLOWER;
                nodeName = genFeNodeName(selfNode.first, selfNode.second, false /* new style */);
                storage.writeFrontendRoleAndNodeName(role, nodeName);
                LOG.info("very first time to start this node. role: {}, node name: {}", role.name(), nodeName);
            } else {
                role = storage.getRole();
                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }

                nodeName = storage.getNodeName();
                if (Strings.isNullOrEmpty(nodeName)) {
                    // In normal case, if ROLE file exist, role and nodeName should both exist.
                    // But we will get a empty nodeName after upgrading.
                    // So for forward compatibility, we use the "old-style" way of naming: "ip_port",
                    // and update the ROLE file.
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, true/* old style */);
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
                storage.writeClusterIdAndToken();

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
                    storage.writeClusterIdAndToken();
                } else {
                    token = storage.getToken();
                }
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
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }

                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }
                break;
            }

            Preconditions.checkState(helperNodes.size() == 1);
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            Pair<String, Integer> rightHelperNode = helperNodes.get(0);

            Storage storage = new Storage(this.imageDir);
            if (roleFile.exists() && (role != storage.getRole() || !nodeName.equals(storage.getNodeName()))
                    || !roleFile.exists()) {
                storage.writeFrontendRoleAndNodeName(role, nodeName);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper node
                if (!getVersionFileFromHelper(rightHelperNode)) {
                    LOG.error("fail to download version file from " + rightHelperNode.first + " will exit.");
                    System.exit(-1);
                }

                // NOTE: cluster_id will be init when Storage object is constructed,
                //       so we new one.
                storage = new Storage(this.imageDir);
                clusterId = storage.getClusterID();
                token = storage.getToken();
                if (Strings.isNullOrEmpty(token)) {
                    token = Config.auth_token;
                }
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node to make sure they are identical
                clusterId = storage.getClusterID();
                token = storage.getToken();
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
                        storage.writeClusterIdAndToken();
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
                } catch (Exception e) {
                    LOG.warn("fail to check cluster_id and token with helper node.", e);
                    System.exit(-1);
                }
            }
            getNewImage(rightHelperNode);
        }

        if (Config.cluster_id != -1 && clusterId != Config.cluster_id) {
            LOG.error("cluster id is not equal with config item cluster_id. will exit.");
            System.exit(-1);
        }

        isElectable = role.equals(FrontendNodeType.FOLLOWER);

        systemInfoMap.put(clusterId, systemInfo);

        Preconditions.checkState(helperNodes.size() == 1);
        LOG.info("finished to get cluster id: {}, role: {} and node name: {}",
                clusterId, role.name(), nodeName);
    }

    public void replayDropFrontend(Frontend frontend) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            Frontend removedFe = frontends.remove(frontend.getNodeName());
            if (removedFe == null) {
                LOG.error(frontend.toString() + " does not exist.");
                return;
            }
            if (removedFe.getRole() == FrontendNodeType.FOLLOWER
                    || removedFe.getRole() == FrontendNodeType.REPLICA) {
                helperNodes.remove(Pair.create(removedFe.getHost(), removedFe.getEditLogPort()));
            }

            removedFrontends.add(removedFe.getNodeName());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    public FrontendNodeType getRole() {
        return role;
    }

    public String getToken() {
        return token;
    }

    public boolean isElectable() {
        return isElectable;
    }

    public int getMasterRpcPort() {
        return masterRpcPort;
    }

    public int getMasterHttpPort() {
        return masterHttpPort;
    }

    public String getMasterIp() {
        return masterIp;
    }

    public String getImageDir() {
        return imageDir;
    }

    public List<Pair<String, Integer>> getHelperNodes() {
        return helperNodes;
    }

    public ConcurrentLinkedQueue<String> getRemovedFrontends() {
        return removedFrontends;
    }

    public Map<Integer, SystemInfoService> getSystemInfoMap() {
        return systemInfoMap;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
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

    public static String genFeNodeName(String host, int port, boolean isOldStyle) {
        String name = host + "_" + port;
        if (isOldStyle) {
            return name;
        } else {
            return name + "_" + System.currentTimeMillis();
        }
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
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, true /* old style */);
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

    private void getNewImage(Pair<String, Integer> helperNode) throws IOException {
        long localImageVersion = 0;
        Storage storage = new Storage(this.imageDir);
        localImageVersion = storage.getImageJournalId();

        try {
            URL infoUrl = new URL("http://" + helperNode.first + ":" + Config.http_port + "/info");
            StorageInfo info = getStorageInfo(infoUrl);
            long version = info.getImageJournalId();
            if (version > localImageVersion) {
                String url = "http://" + helperNode.first + ":" + Config.http_port
                        + "/image?version=" + version;
                String filename = Storage.IMAGE + "." + version;
                File dir = new File(this.imageDir);
                MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000, MetaHelper.getOutputStream(filename, dir));
                MetaHelper.complete(filename, dir);
            }
        } catch (Exception e) {
            return;
        }
    }

    private boolean getVersionFileFromHelper(Pair<String, Integer> helperNode) throws IOException {
        try {
            String url = "http://" + helperNode.first + ":" + Config.http_port + "/version";
            File dir = new File(this.imageDir);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    MetaHelper.getOutputStream(Storage.VERSION_FILE, dir));
            MetaHelper.complete(Storage.VERSION_FILE, dir);
            return true;
        } catch (Exception e) {
            LOG.warn(e);
        }

        return false;
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



    public long loadMasterInfo(DataInputStream dis, long checksum) throws IOException {
        masterIp = Text.readString(dis);
        masterRpcPort = dis.readInt();
        long newChecksum = checksum ^ masterRpcPort;
        masterHttpPort = dis.readInt();
        newChecksum ^= masterHttpPort;

        LOG.info("finished replay masterInfo from image");
        return newChecksum;
    }

    public long loadFrontends(DataInputStream dis, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            int size = dis.readInt();
            long newChecksum = checksum ^ size;
            for (int i = 0; i < size; i++) {
                Frontend fe = Frontend.read(dis);
                replayAddFrontend(fe);
            }

            size = dis.readInt();
            newChecksum ^= size;
            for (int i = 0; i < size; i++) {
                if (GlobalStateMgr.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_41) {
                    Frontend fe = Frontend.read(dis);
                    removedFrontends.add(fe.getNodeName());
                } else {
                    removedFrontends.add(Text.readString(dis));
                }
            }
            return newChecksum;
        }
        LOG.info("finished replay frontends from image");
        return checksum;
    }

    public Frontend checkFeExist(String host, int port) {
        for (Frontend fe : frontends.values()) {
            if (fe.getHost().equals(host) && fe.getEditLogPort() == port) {
                return fe;
            }
        }
        return null;
    }

    public void replayAddFrontend(Frontend fe) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            Frontend existFe = checkFeExist(fe.getHost(), fe.getEditLogPort());
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
            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                // DO NOT add helper sockets here, cause BDBHA is not instantiated yet.
                // helper sockets will be added after start BDBHA
                // But add to helperNodes, just for show
                helperNodes.add(Pair.create(fe.getHost(), fe.getEditLogPort()));
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    public void setFrontendConfig(Map<String, String> configs) throws DdlException {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            ConfigBase.setMutableConfig(entry.getKey(), entry.getValue());
        }
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

    public Frontend getMySelf() {
        return frontends.get(nodeName);
    }
    public ConcurrentHashMap<String, Frontend> getFrontends() {
        return frontends;
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            Frontend fe = checkFeExist(host, editLogPort);
            if (fe != null) {
                throw new DdlException("frontend already exists " + fe);
            }

            String nodeName = genFeNodeName(host, editLogPort, false /* new name style */);

            if (removedFrontends.contains(nodeName)) {
                throw new DdlException("frontend name already exists " + nodeName + ". Try again");
            }

            fe = new Frontend(role, nodeName, host, editLogPort);
            frontends.put(nodeName, fe);
            if (role == FrontendNodeType.FOLLOWER || role == FrontendNodeType.REPLICA) {
                ((BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol()).addHelperSocket(host, editLogPort);
                helperNodes.add(Pair.create(host, editLogPort));
            }
            GlobalStateMgr.getCurrentState().getEditLog().logAddFrontend(fe);
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        if (host.equals(selfNode.first) && port == selfNode.second && GlobalStateMgr.getCurrentState().getFeType() == FrontendNodeType.MASTER) {
            throw new DdlException("can not drop current master node.");
        }
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            Frontend fe = checkFeExist(host, port);
            if (fe == null) {
                throw new DdlException("frontend does not exist[" + host + ":" + port + "]");
            }
            if (fe.getRole() != role) {
                throw new DdlException(role.toString() + " does not exist[" + host + ":" + port + "]");
            }
            frontends.remove(fe.getNodeName());
            removedFrontends.add(fe.getNodeName());

            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                GlobalStateMgr.getCurrentState().getHaProtocol().removeElectableNode(fe.getNodeName());
                helperNodes.remove(Pair.create(host, port));
            }
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveFrontend(fe);
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    public void setMaster(MasterInfo info) {
        this.masterIp = info.getIp();
        this.masterHttpPort = info.getHttpPort();
        this.masterRpcPort = info.getRpcPort();
    }

    public void setMasterInfo() {
        // MUST set master ip before starting checkpoint thread.
        // because checkpoint thread need this info to select non-master FE to push image
        this.masterIp = FrontendOptions.getLocalHostAddress();
        this.masterRpcPort = Config.rpc_port;
        this.masterHttpPort = Config.http_port;
        MasterInfo info = new MasterInfo(this.masterIp, this.masterHttpPort, this.masterRpcPort);
        GlobalStateMgr.getCurrentState().getEditLog().logMasterInfo(info);
    }

    public void setConfig(AdminSetConfigStmt stmt) throws DdlException {
        Map<String, String> configs = stmt.getConfigs();
        Preconditions.checkState(configs.size() == 1);

        setFrontendConfig(configs);

        List<Frontend> allFrontends = getFrontends(null);
        int timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000;
        StringBuilder errMsg = new StringBuilder();
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(selfNode.first)) {
                continue;
            }

            TSetConfigRequest request = new TSetConfigRequest();
            request.setKeys(new ArrayList<>(configs.keySet()));
            request.setValues(new ArrayList<>(configs.values()));
            try {
                TSetConfigResponse response = FrontendServiceProxy
                        .call(new TNetworkAddress(fe.getHost(),
                                        fe.getRpcPort()),
                                timeout,
                                client -> client.setConfig(request)
                        );
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

    public void setImageDir(String imageDir) {
        this.imageDir = imageDir;
    }

    public String getNodeName() {
        return nodeName;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public SystemInfoService getSystemInfo() {
        return systemInfo;
    }

    public Pair<String, Integer> getSelfNode() {
        return this.selfNode;
    }

    public boolean isFirstTimeStartUp() {
        return isFirstTimeStartUp;
    }

    public void setFirstTimeStartUp(boolean firstTimeStartUp) {
        isFirstTimeStartUp = firstTimeStartUp;
    }

    public HeartbeatMgr getHeartbeatMgr() {
        return heartbeatMgr;
    }

    public long saveMasterInfo(DataOutputStream dos, long checksum) throws IOException {
        Text.writeString(dos, masterIp);

        checksum ^= masterRpcPort;
        dos.writeInt(masterRpcPort);

        checksum ^= masterHttpPort;
        dos.writeInt(masterHttpPort);

        return checksum;
    }

    public Frontend getFeByHost(String host) {
        for (Frontend fe : frontends.values()) {
            if (fe.getHost().equals(host)) {
                return fe;
            }
        }
        return null;
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

    public Frontend getFeByName(String name) {
        for (Frontend fe : frontends.values()) {
            if (fe.getNodeName().equals(name)) {
                return fe;
            }
        }
        return null;
    }
}
