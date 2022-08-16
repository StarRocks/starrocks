// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.ibm.icu.impl.Assert;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeServiceAsync;
import com.starrocks.rpc.PBackendServiceAsync;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PseudoCluster {
    private static final Logger LOG = LogManager.getLogger(PseudoCluster.class);

    private static volatile PseudoCluster instance;

    ClusterConfig config = new ClusterConfig();

    String runDir;
    int queryPort;

    PseudoFrontend frontend;
    Map<String, PseudoBackend> backends;
    Map<Long, String> backendIdToHost = new HashMap<>();
    HeatBeatPool heartBeatPool = new HeatBeatPool("heartbeat");
    BackendThriftPool backendThriftPool = new BackendThriftPool("backend");
    PseudoBrpcRroxy brpcProxy = new PseudoBrpcRroxy();

    private BasicDataSource dataSource;

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
        @Override
        protected PBackendServiceAsync getBackendServiceImpl(TNetworkAddress address) {
            Preconditions.checkState(backends.containsKey(address.getHostname()));
            return backends.get(address.getHostname()).pBackendService;
        }

        @Override
        protected LakeServiceAsync getLakeServiceImpl(TNetworkAddress address) {
            Preconditions.checkState(backends.containsKey(address.getHostname()));
            Preconditions.checkState(false, "not implemented");
            return null;
        }
    }

    public ClusterConfig getConfig() {
        return config;
    }

    public PseudoBackend getBackend(long beId) {
        String host = backendIdToHost.get(beId);
        if (host == null) {
            return null;
        }
        return backends.get(host);
    }

    public PseudoBackend getBackendByHost(String host) {
        return backends.get(host);
    }

    public Connection getQueryConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public List<Long> listTablets(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            return null;
        }
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                return null;
            }
            OlapTable olapTable = (OlapTable) table;
            List<Long> ret = Lists.newArrayList();
            for (Partition partition : olapTable.getPartitions()) {
                for (MaterializedIndex index : partition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        ret.add(tablet.getId());
                    }
                }
            }
            return ret;
        } finally {
            db.readUnlock();
        }
    }

    public void runSql(String db, String sql) throws SQLException {
        Connection connection = getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            stmt.execute(sql);
        } finally {
            stmt.close();
            connection.close();
        }
    }

    public void runSqlList(String db, String... sqls) throws SQLException {
        Connection connection = getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            for (String sql : sqls) {
                stmt.execute(sql);
            }
        } finally {
            stmt.close();
            connection.close();
        }
    }

    public String getRunDir() {
        return runDir;
    }

    public void shutdown(boolean deleteRunDir) {
        if (deleteRunDir) {
            try {
                FileUtils.forceDelete(new File(getRunDir()));
            } catch (IOException e) {
                Assert.fail(e);
            }
        }
    }

    /**
     * build cluster at specified dir
     *
     * @param runDir      must be an absolute path
     * @param numBackends num backends
     * @return PseudoCluster
     * @throws Exception
     */
    private static PseudoCluster build(String runDir, boolean fakeJournal, int queryPort, int numBackends) throws Exception {
        PseudoCluster cluster = new PseudoCluster();
        cluster.runDir = runDir;
        cluster.queryPort = queryPort;
        cluster.frontend = new PseudoFrontend();

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(
                "jdbc:mysql://localhost:" + queryPort + "/?permitMysqlScheme&usePipelineAuth=false&useBatchMultiSend=false");
        dataSource.setUsername("root");
        dataSource.setPassword("");
        dataSource.setMaxTotal(40);
        dataSource.setMaxIdle(40);
        cluster.dataSource = dataSource;

        ClientPool.heartbeatPool = cluster.heartBeatPool;
        ClientPool.backendPool = cluster.backendThriftPool;
        BrpcProxy.setInstance(cluster.brpcProxy);

        Config.plugin_dir = runDir + "/plugins";
        Map<String, String> feConfMap = Maps.newHashMap();

        feConfMap.put("tablet_create_timeout_second", "10");
        feConfMap.put("query_port", Integer.toString(queryPort));
        cluster.frontend.init(fakeJournal, runDir + "/fe", feConfMap);
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
        Thread.sleep(2000);
        return cluster;
    }

    public static synchronized PseudoCluster getOrCreateWithRandomPort(boolean fakeJournal, int numBackends) throws Exception {
        int queryPort = UtFrameUtils.findValidPort();
        return getOrCreate("pseudo_cluster_" + queryPort, fakeJournal, queryPort, numBackends);
    }

    public static synchronized PseudoCluster getOrCreate(String runDir, boolean fakeJournal, int queryPort, int numBackends)
            throws Exception {
        if (instance == null) {
            instance = build(runDir, fakeJournal, queryPort, numBackends);
        }
        return instance;
    }

    public static synchronized PseudoCluster getInstance() {
        return instance;
    }

    public static void main(String[] args) throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        for (int i = 0; i < 3; i++) {
            System.out.println(GlobalStateMgr.getCurrentSystemInfo().getBackend(10001 + i).getBePort());
        }
        while (true) {
            Thread.sleep(1000);
        }
    }
}
