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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/HeartbeatMgr.java

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

package com.starrocks.system;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.Version;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.Util;
import com.starrocks.http.rest.BootstrapFinishAction;
import com.starrocks.persist.HbPackage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TBackendInfo;
import com.starrocks.thrift.TBrokerOperationStatus;
import com.starrocks.thrift.TBrokerOperationStatusCode;
import com.starrocks.thrift.TBrokerPingBrokerRequest;
import com.starrocks.thrift.TBrokerVersion;
import com.starrocks.thrift.TFileBrokerService;
import com.starrocks.thrift.THeartbeatResult;
import com.starrocks.thrift.TMasterInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Heartbeat manager run as a daemon at a fix interval.
 * For now, it will send heartbeat to all Frontends, Backends and Brokers
 */
public class HeartbeatMgr extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(HeartbeatMgr.class);
    private static final AtomicReference<TMasterInfo> MASTER_INFO = new AtomicReference<>();

    private final ExecutorService executor;
    private final HeartbeatFlags heartbeatFlags;

    public HeartbeatMgr(boolean needRegisterMetric) {
        super("heartbeat mgr", Config.heartbeat_timeout_second * 1000L);
        this.executor = ThreadPoolManager.newDaemonFixedThreadPool(Config.heartbeat_mgr_threads_num,
                Config.heartbeat_mgr_blocking_queue_size, "heartbeat-mgr-pool", needRegisterMetric);
        this.heartbeatFlags = new HeartbeatFlags();
    }

    private long computeMinActiveTxnId() {
        long a = GlobalStateMgr.getCurrentGlobalTransactionMgr().getMinActiveTxnId();
        Optional<Long> b = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getMinActiveTxnId();
        return Math.min(a, b.orElse(Long.MAX_VALUE));
    }

    public void setLeader(int clusterId, String token, long epoch) {
        TMasterInfo tMasterInfo = new TMasterInfo(
                new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port), clusterId, epoch);
        tMasterInfo.setToken(token);
        tMasterInfo.setHttp_port(Config.http_port);
        long flags = heartbeatFlags.getHeartbeatFlags();
        tMasterInfo.setHeartbeat_flags(flags);
        tMasterInfo.setMin_active_txn_id(computeMinActiveTxnId());
        MASTER_INFO.set(tMasterInfo);
    }

    /**
     * At each round:
     * 1. send heartbeat to all nodes
     * 2. collect the heartbeat response from all nodes, and handle them
     */
    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> idToBackendRef = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();
        if (idToBackendRef == null) {
            return;
        }

        List<Future<HeartbeatResponse>> hbResponses = Lists.newArrayList();

        // send backend heartbeat
        for (Backend backend : idToBackendRef.values()) {
            BackendHeartbeatHandler handler = new BackendHeartbeatHandler(backend);
            hbResponses.add(executor.submit(handler));
        }

        // send compute node heartbeat
        for (ComputeNode computeNode : GlobalStateMgr.getCurrentSystemInfo().getIdComputeNode().values()) {
            BackendHeartbeatHandler handler = new BackendHeartbeatHandler(computeNode);
            hbResponses.add(executor.submit(handler));
        }

        // send frontend heartbeat
        List<Frontend> frontends = GlobalStateMgr.getCurrentState().getFrontends(null);
        String masterFeNodeName = "";
        for (Frontend frontend : frontends) {
            if (frontend.getHost().equals(MASTER_INFO.get().getNetwork_address().getHostname())) {
                masterFeNodeName = frontend.getNodeName();
            }
            FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(frontend,
                    GlobalStateMgr.getCurrentState().getClusterId(),
                    GlobalStateMgr.getCurrentState().getToken());
            hbResponses.add(executor.submit(handler));
        }

        // send broker heartbeat;
        Map<String, List<FsBroker>> brokerMap = Maps.newHashMap(
                GlobalStateMgr.getCurrentState().getBrokerMgr().getBrokerListMap());
        for (Map.Entry<String, List<FsBroker>> entry : brokerMap.entrySet()) {
            for (FsBroker brokerAddress : entry.getValue()) {
                BrokerHeartbeatHandler handler = new BrokerHeartbeatHandler(entry.getKey(), brokerAddress,
                        MASTER_INFO.get().getNetwork_address().getHostname());
                hbResponses.add(executor.submit(handler));
            }
        }

        // collect all heartbeat responses and handle them.
        // and we also find the node whose info has been changed, if changed, we need to collect them and write
        // an edit log to synchronize the info to fe followers.
        HbPackage hbPackage = new HbPackage();
        for (Future<HeartbeatResponse> future : hbResponses) {
            boolean isChanged = false;
            try {
                // the heartbeat rpc's timeout is 5 seconds, so we will not be blocked here too long.
                HeartbeatResponse response = future.get();
                if (response.getStatus() != HbStatus.OK) {
                    LOG.warn("get bad heartbeat response: {}", response);
                }
                isChanged = handleHbResponse(response, false);

                if (isChanged) {
                    hbPackage.addHbResponse(response);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("got exception when doing heartbeat", e);
            }
        } // end for all results

        // we also add a 'mocked' master Frontend heartbeat response to synchronize master info to other Frontends.
        Map<Long, Integer> backendId2cpuCores = Maps.newHashMap();
        idToBackendRef.values().forEach(
                backend -> backendId2cpuCores.put(backend.getId(), BackendCoreStat.getCoresOfBe(backend.getId())));
        hbPackage.addHbResponse(new FrontendHbResponse(masterFeNodeName, Config.query_port, Config.rpc_port,
                GlobalStateMgr.getCurrentState().getMaxJournalId(),
                System.currentTimeMillis(), GlobalStateMgr.getCurrentState().getFeStartTime(),
                Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH, backendId2cpuCores));

        // write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logHeartbeat(hbPackage);
    }

    private boolean handleHbResponse(HeartbeatResponse response, boolean isReplay) {
        switch (response.getType()) {
            case FRONTEND: {
                FrontendHbResponse hbResponse = (FrontendHbResponse) response;

                // Synchronize cpu cores of backends when synchronizing master info to other Frontends.
                // It is non-empty, only when replaying a 'mocked' master Frontend heartbeat response to other Frontends.
                hbResponse.getBackendId2cpuCores().forEach((backendId, cpuCores) -> {
                    Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
                    if (be != null && be.getCpuCores() != cpuCores) {
                        be.setCpuCores(cpuCores);
                        BackendCoreStat.setNumOfHardwareCoresOfBe(backendId, cpuCores);
                    }
                });

                Frontend fe = GlobalStateMgr.getCurrentState().getFeByName(hbResponse.getName());
                if (fe != null) {
                    return fe.handleHbResponse(hbResponse, isReplay);
                }
                break;
            }
            case BACKEND: {
                BackendHbResponse hbResponse = (BackendHbResponse) response;
                ComputeNode computeNode = GlobalStateMgr.getCurrentSystemInfo().getBackend(hbResponse.getBeId());
                boolean isBackend = true;
                if (computeNode == null) {
                    computeNode = GlobalStateMgr.getCurrentSystemInfo().getComputeNode(hbResponse.getBeId());
                    isBackend = false;
                }
                if (computeNode != null) {
                    boolean isChanged = computeNode.handleHbResponse(hbResponse, isReplay);
                    if (hbResponse.getStatus() != HbStatus.OK) {
                        // invalid all connections cached in ClientPool
                        ClientPool.backendPool.clearPool(new TNetworkAddress(computeNode.getHost(), computeNode.getBePort()));
                        if (!isReplay && !computeNode.isAlive()) {
                            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                    .abortTxnWhenCoordinateBeDown(computeNode.getHost(), 100);
                        }
                    } else {
                        if (RunMode.allowCreateLakeTable() && !isReplay && isBackend) {
                            // addWorker
                            int starletPort = computeNode.getStarletPort();
                            if (starletPort != 0) {
                                String workerAddr = computeNode.getHost() + ":" + starletPort;
                                GlobalStateMgr.getCurrentState().getStarOSAgent().addWorker(computeNode.getId(), workerAddr);
                            }
                        }
                    }
                    return isChanged;
                }
                break;
            }
            case BROKER: {
                BrokerHbResponse hbResponse = (BrokerHbResponse) response;
                FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(
                        hbResponse.getName(), hbResponse.getHost(), hbResponse.getPort());
                if (broker != null) {
                    boolean isChanged = broker.handleHbResponse(hbResponse, isReplay);
                    if (hbResponse.getStatus() != HbStatus.OK) {
                        // invalid all connections cached in ClientPool
                        ClientPool.brokerPool.clearPool(new TNetworkAddress(broker.ip, broker.port));
                    }
                    return isChanged;
                }
                break;
            }
            default:
                break;
        }
        return false;
    }

    // backend heartbeat
    private class BackendHeartbeatHandler implements Callable<HeartbeatResponse> {
        private ComputeNode computeNode;

        public BackendHeartbeatHandler(ComputeNode computeNode) {
            this.computeNode = computeNode;
        }

        @Override
        public HeartbeatResponse call() {
            long computeNodeId = computeNode.getId();
            HeartbeatService.Client client = null;
            TNetworkAddress beAddr = new TNetworkAddress(computeNode.getHost(), computeNode.getHeartbeatPort());
            boolean ok = false;
            try {
                client = ClientPool.heartbeatPool.borrowObject(beAddr);

                TMasterInfo copiedMasterInfo = new TMasterInfo(MASTER_INFO.get());
                copiedMasterInfo.setBackend_ip(computeNode.getHost());
                long flags = heartbeatFlags.getHeartbeatFlags();
                copiedMasterInfo.setHeartbeat_flags(flags);
                copiedMasterInfo.setBackend_id(computeNodeId);
                copiedMasterInfo.setMin_active_txn_id(computeMinActiveTxnId());
                THeartbeatResult result = client.heartbeat(copiedMasterInfo);

                ok = true;
                if (result.getStatus().getStatus_code() == TStatusCode.OK) {
                    TBackendInfo tBackendInfo = result.getBackend_info();
                    int bePort = tBackendInfo.getBe_port();
                    int httpPort = tBackendInfo.getHttp_port();
                    int brpcPort = -1;
                    int starletPort = 0;
                    if (tBackendInfo.isSetBrpc_port()) {
                        brpcPort = tBackendInfo.getBrpc_port();
                    }
                    if (tBackendInfo.isSetStarlet_port()) {
                        starletPort = tBackendInfo.getStarlet_port();
                    }
                    String version = "";
                    if (tBackendInfo.isSetVersion()) {
                        version = tBackendInfo.getVersion();
                    }

                    // Update number of hardare of cores of corresponding backend.
                    int cpuCores = tBackendInfo.isSetNum_hardware_cores() ? tBackendInfo.getNum_hardware_cores() : 0;
                    if (tBackendInfo.isSetNum_hardware_cores()) {
                        BackendCoreStat.setNumOfHardwareCoresOfBe(computeNodeId, cpuCores);
                    }

                    // backend.updateOnce(bePort, httpPort, beRpcPort, brpcPort);
                    BackendHbResponse backendHbResponse = new BackendHbResponse(
                            computeNodeId, bePort, httpPort, brpcPort, starletPort,
                            System.currentTimeMillis(), version, cpuCores);
                    if (tBackendInfo.isSetReboot_time()) {
                        backendHbResponse.setRebootTime(tBackendInfo.getReboot_time());
                    }
                    return backendHbResponse;
                } else {
                    return new BackendHbResponse(computeNodeId,
                            result.getStatus().getError_msgs().isEmpty() ? "Unknown error"
                                    : result.getStatus().getError_msgs().get(0));
                }
            } catch (Exception e) {
                LOG.warn("backend heartbeat got exception, addr: {}:{}",
                        computeNode.getHost(), computeNode.getHeartbeatPort(), e);
                return new BackendHbResponse(computeNodeId,
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            } finally {
                if (ok) {
                    ClientPool.heartbeatPool.returnObject(beAddr, client);
                } else {
                    ClientPool.heartbeatPool.invalidateObject(beAddr, client);
                }
            }
        }
    }

    // frontend heartbeat
    public static class FrontendHeartbeatHandler implements Callable<HeartbeatResponse> {
        private Frontend fe;
        private int clusterId;
        private String token;

        public FrontendHeartbeatHandler(Frontend fe, int clusterId, String token) {
            this.fe = fe;
            this.clusterId = clusterId;
            this.token = token;
        }

        @Override
        public HeartbeatResponse call() {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getSelfNode().first)) {
                // heartbeat to self
                if (GlobalStateMgr.getCurrentState().isReady()) {
                    return new FrontendHbResponse(fe.getNodeName(), Config.query_port, Config.rpc_port,
                            GlobalStateMgr.getCurrentState().getReplayedJournalId(), System.currentTimeMillis(),
                            GlobalStateMgr.getCurrentState().getFeStartTime(),
                            Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH);
                } else {
                    return new FrontendHbResponse(fe.getNodeName(), "not ready");
                }
            }

            String url = "http://" + fe.getHost() + ":" + Config.http_port
                    + "/api/bootstrap?cluster_id=" + clusterId + "&token=" + token;
            try {
                String result = Util.getResultForUrl(url, null, 2000, 2000);
                /*
                 * return:
                 * {"replayedJournalId":191224,"queryPort":9131,"rpcPort":9121,"status":"OK","msg":"Success"}
                 * {"replayedJournalId":0,"queryPort":0,"rpcPort":0,"status":"FAILED","msg":"not ready"}
                 */
                JSONObject root = new JSONObject(result);
                String status = root.getString("status");
                if (!"OK".equals(status)) {
                    return new FrontendHbResponse(fe.getNodeName(), root.getString("msg"));
                } else {
                    long replayedJournalId = root.getLong(BootstrapFinishAction.REPLAYED_JOURNAL_ID);
                    int queryPort = root.getInt(BootstrapFinishAction.QUERY_PORT);
                    int rpcPort = root.getInt(BootstrapFinishAction.RPC_PORT);
                    long feStartTime = root.getLong(BootstrapFinishAction.FE_START_TIME);
                    String feVersion = root.getString(BootstrapFinishAction.FE_VERSION);
                    return new FrontendHbResponse(fe.getNodeName(), queryPort, rpcPort, replayedJournalId,
                            System.currentTimeMillis(), feStartTime, feVersion);
                }
            } catch (Exception e) {
                return new FrontendHbResponse(fe.getNodeName(),
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            }
        }
    }

    // broker heartbeat handler
    public static class BrokerHeartbeatHandler implements Callable<HeartbeatResponse> {
        private String brokerName;
        private FsBroker broker;
        private String clientId;

        public BrokerHeartbeatHandler(String brokerName, FsBroker broker, String clientId) {
            this.brokerName = brokerName;
            this.broker = broker;
            this.clientId = clientId;
        }

        @Override
        public HeartbeatResponse call() {
            TFileBrokerService.Client client = null;
            TNetworkAddress addr = new TNetworkAddress(broker.ip, broker.port);
            boolean ok = false;
            try {
                client = ClientPool.brokerPool.borrowObject(addr);
                TBrokerPingBrokerRequest request = new TBrokerPingBrokerRequest(TBrokerVersion.VERSION_ONE,
                        clientId);
                TBrokerOperationStatus status = client.ping(request);
                ok = true;

                if (status.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    return new BrokerHbResponse(brokerName, broker.ip, broker.port, status.getMessage());
                } else {
                    return new BrokerHbResponse(brokerName, broker.ip, broker.port, System.currentTimeMillis());
                }

            } catch (Exception e) {
                return new BrokerHbResponse(brokerName, broker.ip, broker.port,
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            } finally {
                if (ok) {
                    ClientPool.brokerPool.returnObject(addr, client);
                } else {
                    ClientPool.brokerPool.invalidateObject(addr, client);
                }
            }
        }
    }

    public void replayHearbeat(HbPackage hbPackage) {
        for (HeartbeatResponse hbResult : hbPackage.getHbResults()) {
            handleHbResponse(hbResult, true);
        }
    }

}

