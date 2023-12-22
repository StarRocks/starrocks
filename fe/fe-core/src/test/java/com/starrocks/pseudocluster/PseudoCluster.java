// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ibm.icu.impl.Assert;
import com.staros.proto.ObjectStorageInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.PBackendService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PseudoCluster {
    private static final Logger LOG = LogManager.getLogger(PseudoCluster.class);

    private static volatile PseudoCluster instance;

    public static boolean logToConsole = false;

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

    private static long backendIdStart = 10001;
    private static int backendPortStart = 12100;
    private static int backendHostStart = 10;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void setQueryTimeout(int timeout) {
        dataSource.setDefaultQueryTimeout(timeout);
    }

    private class HeatBeatPool extends PseudoGenericPool<HeartbeatService.Client> {
        public HeatBeatPool(String name) {
            super(name);
        }

        @Override
        public HeartbeatService.Client borrowObject(TNetworkAddress address) throws Exception {
            return getBackendByHost(address.getHostname()).heatBeatClient;
        }
    }

    private class BackendThriftPool extends PseudoGenericPool<BackendService.Client> {
        public BackendThriftPool(String name) {
            super(name);
        }

        @Override
        public BackendService.Client borrowObject(TNetworkAddress address) throws Exception {
            return getBackendByHost(address.getHostname()).backendClient;
        }

    }

    private class PseudoBrpcRroxy extends BrpcProxy {
        @Override
        protected PBackendService getBackendServiceImpl(TNetworkAddress address) {
            return getBackendByHost(address.getHostname()).pBackendService;
        }

        @Override
        protected LakeService getLakeServiceImpl(TNetworkAddress address) {
            Preconditions.checkNotNull(getBackendByHost(address.getHostname()));
            Preconditions.checkState(false, "not implemented");
            return null;
        }
    }

    private static class PseudoStarOSAgent extends StarOSAgent {
        private class Worker {
            long backendId;
            long workerId;
            String hostAndPort;

            Worker(long backendId, long workerId, String hostAndPort) {
                this.backendId = backendId;
                this.workerId = workerId;
                this.hostAndPort = hostAndPort;
            }
        }

        private long nextId = 65535;
        private final List<Worker> workers = new ArrayList<>();
        private final List<ShardInfo> shardInfos = new ArrayList<>();

        @Override
        public ShardStorageInfo getServiceShardStorageInfo() throws DdlException {
            ObjectStorageInfo objectStorageInfo = ObjectStorageInfo.newBuilder()
                    .setObjectUri("s3://bucket")
                    .setAccessKey("testaccesskey")
                    .setAccessKeySecret("testaccesskeysecret")
                    .setEndpoint("http://127.0.0.1")
                    .build();
            return ShardStorageInfo.newBuilder().setObjectStorageInfo(objectStorageInfo).build();
        }

        @Override
        public void addWorker(long backendId, String hostAndPort) {
            workers.add(new Worker(backendId, nextId++, hostAndPort));
        }

        @Override
        public void removeWorker(String hostAndPort) throws DdlException {
            workers.removeIf(w -> Objects.equals(w.hostAndPort, hostAndPort));
        }

        @Override
        public long getWorkerIdByBackendId(long backendId) {
            Optional<Worker> worker = workers.stream().filter(w -> w.backendId == backendId).findFirst();
            return worker.map(value -> value.workerId).orElse(-1L);
        }

        @Override
        public void createShardGroup(long groupId) throws DdlException {
        }

        @Override
        public List<Long> createShards(int numShards, ShardStorageInfo shardStorageInfo, long groupId) throws DdlException {
            List<Long> shardIds = new ArrayList<>();
            for (int i = 0; i < numShards; i++) {
                long id = nextId++;
                shardIds.add(id);
                ShardInfo shardInfo = ShardInfo.newBuilder().setShardStorageInfo(shardStorageInfo).setShardId(id).build();
                shardInfos.add(shardInfo);
            }
            return shardIds;
        }

        @Override
        public void deleteShards(Set<Long> shardIds) throws DdlException {
            shardInfos.removeIf(s -> shardIds.contains(s.getShardId()));
        }

        @Override
        public long getPrimaryBackendIdByShard(long shardId) throws UserException {
            return workers.isEmpty() ? -1 : workers.get((int) (shardId % workers.size())).backendId;
        }

        @Override
        public Set<Long> getBackendIdsByShard(long shardId) throws UserException {
            return Sets.newHashSet(getPrimaryBackendIdByShard(shardId));
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
        PseudoBackend be = backends.get(host);
        if (be == null) {
            LOG.warn("no backend found for host {} hosts:{}", host, backends.keySet());
        }
        return be;
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

    private static void runSingleSql(Statement stmt, String sql, boolean verbose) throws SQLException {
        while (true) {
            try {
                long start = System.nanoTime();
                stmt.execute(sql);
                if (verbose) {
                    long end = System.nanoTime();
                    System.out.printf("runSql(%.3fs): %s\n", (end - start) / 1e9, sql);
                }
                break;
            } catch (SQLSyntaxErrorException e) {
                if (e.getMessage().startsWith("rpc failed, host")) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                    }
                    System.out.println("retry execute " + sql);
                    continue;
                }
                throw e;
            }
        }
    }

    public void runSql(String db, String sql, boolean verbose) throws SQLException {
        Connection connection = getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            runSingleSql(stmt, sql, verbose);
        } finally {
            stmt.close();
            connection.close();
        }
    }

    public void runSql(String db, String sql) throws SQLException {
        runSql(db, sql, false);
    }

    public void runSqlList(String db, List<String> sqls, boolean verbose) throws SQLException {
        Connection connection = getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            for (String sql : sqls) {
                runSingleSql(stmt, sql, verbose);
            }
        } finally {
            stmt.close();
            connection.close();
        }
    }

    public void runSqls(String db, String... sqls) throws SQLException {
        runSqlList(db, Arrays.stream(sqls).collect(Collectors.toList()), true);
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
                "jdbc:mysql://127.0.0.1:" + queryPort + "/?permitMysqlScheme" +
                        "&usePipelineAuth=false&useBatchMultiSend=false&" +
                        "autoReconnect=true&failOverReadOnly=false&maxReconnects=10");
        dataSource.setUsername("root");
        dataSource.setPassword("");
        dataSource.setMaxTotal(40);
        dataSource.setMaxIdle(40);
        cluster.dataSource = dataSource;

        ClientPool.beHeartbeatPool = cluster.heartBeatPool;
        ClientPool.backendPool = cluster.backendThriftPool;
        BrpcProxy.setInstance(cluster.brpcProxy);

        GlobalStateMgr.getCurrentState().setStarOSAgent(new PseudoStarOSAgent());

        // statistics affects table read times counter, so disable it
        Config.enable_statistic_collect = false;
        Config.plugin_dir = runDir + "/plugins";
        Config.use_staros = true;
        Map<String, String> feConfMap = Maps.newHashMap();
        feConfMap.put("tablet_create_timeout_second", "10");
        feConfMap.put("query_port", Integer.toString(queryPort));
        cluster.frontend.init(fakeJournal, runDir, feConfMap);
        cluster.frontend.start(new String[0]);

        if (logToConsole) {
            System.out.println("start add console appender");
            logAddConsoleAppender();
        }

        LOG.info("start create and start backends");
        cluster.backends = Maps.newConcurrentMap();
        for (int i = 0; i < numBackends; i++) {
            String host = genBackendHost();
            long beId = backendIdStart++;
            String beRunPath = runDir + "/be" + beId;
            PseudoBackend backend = new PseudoBackend(cluster, beRunPath, beId, host,
                    backendPortStart++, backendPortStart++, backendPortStart++, backendPortStart++,
                    cluster.frontend.getFrontendService());
            cluster.backends.put(backend.getHost(), backend);
            cluster.backendIdToHost.put(beId, backend.getHost());
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend.be);
            GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .addWorker(beId, String.format("%s:%d", backend.getHost(), backendPortStart - 1));
            LOG.info("add PseudoBackend {} {}", beId, host);
        }
        int retry = 0;
        while (GlobalStateMgr.getCurrentSystemInfo().getBackend(10001).getBePort() == -1 &&
                retry++ < 600) {
            Thread.sleep(100);
        }
        Thread.sleep(2000);
        return cluster;
    }

    public List<Long> addBackends(int numBackends) {
        List<Long> beIds = new ArrayList<>();
        for (int i = 0; i < numBackends; i++) {
            String host = genBackendHost();
            long beId = backendIdStart++;
            String beRunPath = runDir + "/be" + beId;
            PseudoBackend backend = new PseudoBackend(this, beRunPath, beId, host,
                    backendPortStart++, backendPortStart++, backendPortStart++, backendPortStart++,
                    this.frontend.getFrontendService());
            this.backends.put(backend.getHost(), backend);
            this.backendIdToHost.put(beId, backend.getHost());
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend.be);
            GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .addWorker(beId, String.format("%s:%d", backend.getHost(), backendPortStart - 1));
            LOG.info("add PseudoBackend {} {}", beId, host);
            beIds.add(beId);
        }
        int retry = 0;
        while (GlobalStateMgr.getCurrentSystemInfo().getBackend(beIds.get(0)).getBePort() == -1 &&
                retry++ < 600) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return beIds;
    }

    private static String genBackendHost() {
        int i = backendHostStart % 128;
        int j = (backendHostStart >> 7) % 128;
        int k = (backendHostStart >> 14) % 128;
        backendHostStart++;
        return String.format("127.%d.%d.%d", k, j, i);
    }

    private static void logAddConsoleAppender() {
        PatternLayout layout =
                PatternLayout.newBuilder().withPattern("%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid) [%C{1}.%M():%L] %m%n")
                        .build();
        ConsoleAppender ca = ConsoleAppender.newBuilder()
                .setName("console")
                .setLayout(layout)
                .setTarget(ConsoleAppender.Target.SYSTEM_OUT)
                .build();
        ca.start();
        ((org.apache.logging.log4j.core.Logger) LogManager.getRootLogger()).addAppender(ca);
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

    public static class CreateTableSqlBuilder {
        private String tableName = "test_table";
        private int buckets = 3;
        private int replication = 3;
        private String quorum = "MAJORITY";
        private String colocateGroup = "";

        private boolean ssd = true;

        public CreateTableSqlBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public CreateTableSqlBuilder setBuckets(int buckets) {
            this.buckets = buckets;
            return this;
        }

        public CreateTableSqlBuilder setReplication(int replication) {
            this.replication = replication;
            return this;
        }

        public CreateTableSqlBuilder setSsd(boolean ssd) {
            this.ssd = ssd;
            return this;
        }

        public CreateTableSqlBuilder setWriteQuorum(String quorum) {
            this.quorum = quorum;
            return this;
        }

        public CreateTableSqlBuilder setColocateGroup(String colocateGroup) {
            this.colocateGroup = colocateGroup;
            return this;
        }

        public String build() {
            return String.format("create table %s (id bigint not null, name varchar(64) not null, age int null) " +
                            "primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS %d " +
                            "PROPERTIES(" +
                                "\"write_quorum\" = \"%s\", " +
                                "\"replication_num\" = \"%d\", " +
                                "\"storage_medium\" = \"%s\", " +
                                "\"colocate_with\" = \"%s\")",
                    tableName,
                    buckets, quorum, replication,
                    ssd ? "SSD" : "HDD",
                    colocateGroup);
        }
    }

    public static CreateTableSqlBuilder newCreateTableSqlBuilder() {
        return new CreateTableSqlBuilder();
    }

    public static String buildInsertSql(String db, String table) {
        return "insert into " + (db == null ? "" : db + ".") + table + " values (1,\"1\", 1), (2,\"2\", 2), (3,\"3\", 3)";
    }

    public static void main(String[] args) throws Exception {
        PseudoCluster.getOrCreate("pseudo_cluster", false, 9030, 4);
        for (int i = 0; i < 4; i++) {
            System.out.println(GlobalStateMgr.getCurrentSystemInfo().getBackend(10001 + i).getBePort());
        }
        while (true) {
            Thread.sleep(1000);
        }
    }
}
