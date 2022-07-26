// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.pseudocluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.LakeServiceAsync;
import com.starrocks.rpc.PBackendService;
import com.starrocks.rpc.PBackendServiceAsync;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class PseudoCluster {
    private static final Logger LOG = LogManager.getLogger(PseudoCluster.class);

    private static volatile PseudoCluster instance;

    PseudoFrontend frontend;
    Map<String, PseudoBackend> backends;
    Map<Long, String> backendIdToHost = new HashMap<>();
    HeatBeatPool heartBeatPool = new HeatBeatPool("heartbeat");
    BackendThriftPool backendThriftPool = new BackendThriftPool("backend");
    PseudoBrpcRroxy brpcProxy = new PseudoBrpcRroxy();

    private class HeatBeatPool extends PseudoGenericPool<HeartbeatService.Client> {
        public HeatBeatPool(String name) {
            super(name);
        }

        @Override
        public HeartbeatService.Client borrowObject(TNetworkAddress address) throws Exception {
            Preconditions.checkState(backends.containsKey(address.getHostname()));
            return backends.get(address.getHostname()).heatBeatClient;
        }
    }

    private class BackendThriftPool extends PseudoGenericPool<BackendService.Client> {
        public BackendThriftPool(String name) {
            super(name);
        }

        @Override
        public BackendService.Client borrowObject(TNetworkAddress address) throws Exception {
            Preconditions.checkState(backends.containsKey(address.getHostname()));
            return backends.get(address.getHostname()).backendClient;
        }

    }

    private class PseudoBrpcRroxy extends BrpcProxy {
        public PBackendServiceAsync getBackendService(TNetworkAddress address) {
            Preconditions.checkState(backends.containsKey(address.getHostname()));
            LOG.warn("get PseudoBrpcRroxy: {}", address.getHostname());
            return backends.get(address.getHostname()).pBackendService;
        }

        public LakeServiceAsync getLakeService(TNetworkAddress address) {
            Preconditions.checkState(backends.containsKey(address.getHostname()));
            Preconditions.checkState(false, "not implemented");
            return null;
        }
    }

    public PseudoBackend getBackend(long beId) {
        return backends.get(backendIdToHost.get(beId));
    }

    /**
     * build cluster at specified dir
     *
     * @param runDir      must be an absolute path
     * @param numBackends num backends
     * @return PseudoCluster
     * @throws Exception
     */
    private static PseudoCluster build(String runDir, int numBackends) throws Exception {
        PseudoCluster cluster = new PseudoCluster();
        cluster.frontend = PseudoFrontend.getInstance();

        ClientPool.heartbeatPool = cluster.heartBeatPool;
        ClientPool.backendPool = cluster.backendThriftPool;
        BrpcProxy.setInstance(cluster.brpcProxy);

        Config.plugin_dir = runDir + "/plugins";
        Map<String, String> feConfMap = Maps.newHashMap();

        feConfMap.put("tablet_create_timeout_second", "10");
        cluster.frontend.init(runDir + "/fe", feConfMap);
        cluster.frontend.start(new String[0]);

        cluster.backends = Maps.newConcurrentMap();
        long backendIdStart = 10001;
        int port = 12100;
        for (int i = 0; i < numBackends; i++) {
            String host = String.format("127.0.0.%d", i + 10);
            long beId = backendIdStart + i;
            String beRunPath = runDir + "/be" + beId;
            PseudoBackend backend = new PseudoBackend(cluster, beRunPath, beId, host, port++, port++, port++, port++,
                    cluster.frontend.getFrontendService());
            cluster.backends.put(backend.getHost(), backend);
            cluster.backendIdToHost.put(beId, backend.getHost());
        }
        int retry = 0;
        while (GlobalStateMgr.getCurrentSystemInfo().getBackend(10001).getBePort() == -1 &&
                retry++ < 600) {
            Thread.sleep(100);
        }
        return cluster;
    }

    public static synchronized PseudoCluster getOrCreate(String runDir, int numBackends) throws Exception {
        if (instance == null) {
            instance = build(runDir, numBackends);
        }
        return instance;
    }

    public static void main(String[] args) throws Exception {
        String currentPath = new java.io.File(".").getCanonicalPath();
        String runDir = currentPath + "/pseudo_cluster";
        PseudoCluster cluster = PseudoCluster.getOrCreate(runDir, 3);
        for (int i = 0; i < 3; i++) {
            System.out.println(GlobalStateMgr.getCurrentSystemInfo().getBackend(10001 + i).getBePort());
        }
        while (true) {
            Thread.sleep(1000);
        }
    }
}
