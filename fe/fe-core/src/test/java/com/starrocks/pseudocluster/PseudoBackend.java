// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.PTabletInfo;
import com.starrocks.proto.PTabletWithPartition;
import com.starrocks.proto.PTabletWriterAddBatchResult;
import com.starrocks.proto.PTabletWriterAddChunkRequest;
import com.starrocks.proto.PTabletWriterCancelRequest;
import com.starrocks.proto.PTabletWriterCancelResult;
import com.starrocks.proto.PTabletWriterOpenRequest;
import com.starrocks.proto.PTabletWriterOpenResult;
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.PBackendService;
import com.starrocks.rpc.PExecBatchPlanFragmentsRequest;
import com.starrocks.system.Backend;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TAgentPublishRequest;
import com.starrocks.thrift.TAgentResult;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TBackendInfo;
import com.starrocks.thrift.TCancelPlanFragmentParams;
import com.starrocks.thrift.TCancelPlanFragmentResult;
import com.starrocks.thrift.TCloneReq;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TDeleteEtlFilesRequest;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TExecBatchPlanFragmentsParams;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TExecPlanFragmentResult;
import com.starrocks.thrift.TExportState;
import com.starrocks.thrift.TExportStatusResult;
import com.starrocks.thrift.TExportTaskRequest;
import com.starrocks.thrift.TFetchDataParams;
import com.starrocks.thrift.TFetchDataResult;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.THeartbeatResult;
import com.starrocks.thrift.TMasterInfo;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TMiniLoadEtlStatusRequest;
import com.starrocks.thrift.TMiniLoadEtlStatusResult;
import com.starrocks.thrift.TMiniLoadEtlTaskRequest;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TPublishVersionRequest;
import com.starrocks.thrift.TPushReq;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TReportExecStatusResult;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanCloseResult;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTransmitDataParams;
import com.starrocks.thrift.TTransmitDataResult;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class PseudoBackend {
    private static final Logger LOG = LogManager.getLogger(PseudoBackend.class);
    public static final long PATH_HASH = 123456;

    public static volatile long reportIntervalMs = 5000L;

    public static volatile long tabletCheckIntervalMs = 5000L;

    private final PseudoCluster cluster;
    private final String runPath;
    private final String host;
    private final int brpcPort;
    private final int heartBeatPort;
    private final int beThriftPort;
    private final int httpPort;
    private final FrontendService.Iface frontendService;

    private final TBackend tBackend;
    private AtomicLong reportVersion = new AtomicLong(0);
    private final BeTabletManager tabletManager = new BeTabletManager(this);
    private final BeLakeTabletManager lakeTabletManager = new BeLakeTabletManager(this);
    private final BeTxnManager txnManager = new BeTxnManager(this);
    private final BlockingQueue<TAgentTaskRequest> taskQueue = Queues.newLinkedBlockingQueue();
    private final Map<TTaskType, Set<Long>> taskSignatures = new EnumMap(TTaskType.class);
    private volatile boolean stopped = false;

    private volatile boolean shutdown = false;

    private TreeMap<Long, Runnable> maintenanceTasks = new TreeMap<>(Long::compare);
    private Thread maintenanceWorkerThread;

    private volatile float writeFailureRate = 0.0f;
    private volatile float publishFailureRate = 0.0f;

    public static final long DEFAULT_TOTA_CAP_B = 100000000000L;
    public static final long DEFAULT_AVAI_CAP_B = 50000000000L;
    public static final long DEFAULT_USED_CAP_B = 20000000000L;
    public static final long DEFAULT_SIZE_ON_DISK_PER_ROWSET_B = 1 << 20;

    private long currentDataUsedCapacityB;
    private long currentAvailableCapacityB;
    private long currentTotalCapacityB;

    Backend be;
    HeartBeatClient heatBeatClient;
    BeThriftClient backendClient;
    PseudoPBackendService pBackendService;

    private AtomicLong nextRowsetId = new AtomicLong(0);

    private Random random;

    private AtomicLong numSchemaScan = new AtomicLong(0);

    private static ThreadLocal<PseudoBackend> currentBackend = new ThreadLocal<>();

    static class QueryProgress {
        String queryId;
        ReentrantLock lock = new ReentrantLock();
        Condition fragmentComplete = lock.newCondition();
        int waitFragmentCount = 0;

        QueryProgress(String id) {
            this.queryId = id;
        }

        void addFragment(int count) {
            lock.lock();
            try {
                waitFragmentCount += count;
            } finally {
                lock.unlock();
            }
        }

        void completeFragment(int count) {
            lock.lock();
            try {
                waitFragmentCount -= count;
                if (waitFragmentCount == 0) {
                    fragmentComplete.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        void waitComplete() {
            lock.lock();
            try {
                while (waitFragmentCount > 0) {
                    try {
                        fragmentComplete.await();
                    } catch (InterruptedException e) {
                        LOG.error("waitComplete interrupted", e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        Future<PFetchDataResult> getFetchDataResult() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    waitComplete();
                } catch (Exception e) {
                    LOG.error("getFetchDataResult error", e);
                }
                PFetchDataResult result = new PFetchDataResult();
                StatusPB pStatus = new StatusPB();
                pStatus.statusCode = 0;
                PQueryStatistics pQueryStatistics = new PQueryStatistics();
                pQueryStatistics.scanRows = 0L;
                pQueryStatistics.scanBytes = 0L;
                pQueryStatistics.cpuCostNs = 0L;
                pQueryStatistics.memCostBytes = 0L;
                result.status = pStatus;
                result.packetSeq = 0L;
                result.queryStatistics = pQueryStatistics;
                result.eos = true;
                return result;
            });
        }
    }

    // Those 2 maps will grow as queries are executed until test is finished(process exited)
    private static Map<String, QueryProgress> queryProgresses = new ConcurrentHashMap<>();
    private static Map<String, String> resultSinkInstanceToQueryId = new ConcurrentHashMap<>();

    public static PseudoBackend getCurrentBackend() {
        return currentBackend.get();
    }

    String genRowsetId() {
        return String.format("rowset-%d-%d", be.getId(), nextRowsetId.incrementAndGet());
    }

    private void maintenanceWorker() {
        currentBackend.set(PseudoBackend.this);
        while (!stopped) {
            try {
                Runnable task = null;
                synchronized (maintenanceTasks) {
                    if (!maintenanceTasks.isEmpty()) {
                        long ts = maintenanceTasks.firstKey();
                        long now = System.nanoTime();
                        if (ts <= now) {
                            task = maintenanceTasks.pollFirstEntry().getValue();
                        }
                    }
                }
                if (task != null) {
                    task.run();
                } else {
                    Thread.sleep(500);
                }
            } catch (Throwable e) {
                LOG.error("Error in maintenance worker be:" + getId(), e);
            }
        }
        LOG.info("Maintenance worker stopped be:" + getId());
    }

    private void addMaintenanceTask(long ts, Runnable task) {
        synchronized (maintenanceTasks) {
            maintenanceTasks.put(ts, task);
        }
    }

    private long nextScheduleTime(long intervalMs) {
        // add -500 ~ 500 ms jitter
        long randomMs = (random.nextInt(11) - 5) * 100;
        return System.nanoTime() + intervalMs * 1000000 + randomMs;
    }

    public PseudoBackend(PseudoCluster cluster, String runPath, long backendId, String host, int brpcPort, int heartBeatPort,
                         int beThriftPort, int httpPort,
                         FrontendService.Iface frontendService) {
        this.cluster = cluster;
        this.runPath = runPath;
        this.host = host;
        this.brpcPort = brpcPort;
        this.heartBeatPort = heartBeatPort;
        this.beThriftPort = beThriftPort;
        this.httpPort = httpPort;
        this.frontendService = frontendService;
        this.random = new Random(backendId);

        this.be = new Backend(backendId, host, heartBeatPort);
        this.tBackend = new TBackend(host, beThriftPort, httpPort);

        // TODO: maintain disk info with tablet/rowset count
        setInitialCapacity(PseudoBackend.DEFAULT_TOTA_CAP_B, PseudoBackend.DEFAULT_AVAI_CAP_B,
                PseudoBackend.DEFAULT_USED_CAP_B);
        be.setAlive(true);
        be.setBePort(beThriftPort);
        be.setBrpcPort(brpcPort);
        be.setHttpPort(httpPort);
        this.heatBeatClient = new HeartBeatClient();
        this.backendClient = new BeThriftClient();
        this.pBackendService = new PseudoPBackendService();

        maintenanceWorkerThread = new Thread(() -> maintenanceWorker(), "be-" + getId());
        maintenanceWorkerThread.start();

        addMaintenanceTask(nextScheduleTime(reportIntervalMs), this::reportTablets);
        addMaintenanceTask(nextScheduleTime(reportIntervalMs), this::reportDisks);
        addMaintenanceTask(nextScheduleTime(reportIntervalMs), this::reportTasks);
        addMaintenanceTask(nextScheduleTime(tabletCheckIntervalMs), this::tabletMaintenance);
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }

    public void setInitialCapacity(long totalCapacityB, long availableCapacityB, long usedCapacityB) {
        Preconditions.checkState(usedCapacityB < availableCapacityB);
        Preconditions.checkState(availableCapacityB + usedCapacityB <= totalCapacityB);

        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo(runPath + "/storage");
        diskInfo1.setTotalCapacityB(totalCapacityB);
        currentTotalCapacityB = totalCapacityB;
        diskInfo1.setAvailableCapacityB(availableCapacityB);
        currentAvailableCapacityB = availableCapacityB;
        diskInfo1.setDataUsedCapacityB(usedCapacityB);
        currentDataUsedCapacityB = usedCapacityB;
        diskInfo1.setPathHash(PATH_HASH);
        diskInfo1.setStorageMedium(TStorageMedium.SSD);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        be.setDisks(ImmutableMap.copyOf(disks));
    }

    public String getHost() {
        return host;
    }

    public String getHostHeartbeatPort() {
        return host + ":" + heartBeatPort;
    }

    public long getId() {
        return be.getId();
    }

    public BeTxnManager getTxnManager() {
        return txnManager;
    }

    public BeTabletManager getTabletManager() {
        return tabletManager;
    }

    public Tablet getTablet(long tabletId) {
        return tabletManager.getTablet(tabletId);
    }

    public List<Tablet> getTabletsByTable(long tableId) {
        return tabletManager.getTabletsByTable(tableId);
    }

    public void setWriteFailureRate(float rate) {
        writeFailureRate = rate;
    }

    public float getWriteFailureRate() {
        return writeFailureRate;
    }

    public void setPublishFailureRate(float rate) {
        publishFailureRate = rate;
    }

    public float getPublishFailureRate() {
        return publishFailureRate;
    }

    public long getNumSchemaScan() {
        return numSchemaScan.get();
    }

    private void reportTablets() {
        // report tablets
        TReportRequest request = new TReportRequest();
        request.setTablets(tabletManager.getAllTabletInfo());
        request.setTablet_max_compaction_score(100);
        request.setBackend(tBackend);
        reportVersion.incrementAndGet();
        request.setReport_version(reportVersion.get());
        try {
            if (!shutdown) {
                TMasterResult result = frontendService.report(request);
                LOG.info("report {} tablets", request.tablets.size());
                if (result.status.status_code != TStatusCode.OK) {
                    LOG.warn("Report tablets failed, status:" + result.status.error_msgs.get(0));
                }
            }
        } catch (TException e) {
            LOG.error("report tablets error", e);
        }
        addMaintenanceTask(nextScheduleTime(reportIntervalMs), this::reportTablets);
    }

    private void reportDisks() {
        // report disks
        TReportRequest request = new TReportRequest();
        Map<String, TDisk> tdisks = new HashMap<>();
        ImmutableMap<String, DiskInfo> diskInfos = be.getDisks();
        for (Map.Entry<String, DiskInfo> entry : diskInfos.entrySet()) {
            TDisk tdisk = new TDisk();
            tdisk.setRoot_path(entry.getKey());
            tdisk.setPath_hash(entry.getValue().getPathHash());
            tdisk.setDisk_total_capacity(currentTotalCapacityB);
            tdisk.setDisk_available_capacity(currentAvailableCapacityB);
            tdisk.setData_used_capacity(currentDataUsedCapacityB);
            tdisk.setStorage_medium(entry.getValue().getStorageMedium());
            tdisk.setUsed(true);
            tdisks.put(entry.getKey(), tdisk);
        }
        request.setDisks(tdisks);
        request.setBackend(tBackend);
        request.setReport_version(reportVersion.get());
        try {
            if (!shutdown) {
                TMasterResult result = frontendService.report(request);
                LOG.info("report {} disks", request.disks.size());
                if (result.status.status_code != TStatusCode.OK) {
                    LOG.warn("Report disks failed, status:" + result.status.error_msgs.get(0));
                }
            }
        } catch (TException e) {
            LOG.error("report disk error", e);
        }
        addMaintenanceTask(nextScheduleTime(reportIntervalMs), this::reportDisks);
    }

    private void reportTasks() {
        // report tasks
        TReportRequest request = new TReportRequest();
        request.setTasks(new HashMap<>());
        request.setBackend(tBackend);
        request.setReport_version(reportVersion.get());
        Map<TTaskType, Set<Long>> tasks = new HashMap<>();
        synchronized (taskSignatures) {
            for (TTaskType type : taskSignatures.keySet()) {
                HashSet<Long> ts = new HashSet<>();
                ts.addAll(taskSignatures.get(type));
                tasks.put(type, ts);
            }
        }
        request.setTasks(tasks);
        try {
            if (!shutdown) {
                TMasterResult result = frontendService.report(request);
                LOG.info("report {} tasks", request.tasks.size());
                if (result.status.status_code != TStatusCode.OK) {
                    LOG.warn("Report tasks failed, status:" + result.status.error_msgs.get(0));
                }
            }
        } catch (TException e) {
            LOG.error("report tasks error", e);
        }
        addMaintenanceTask(nextScheduleTime(reportIntervalMs), this::reportTasks);
    }

    private void tabletMaintenance() {
        tabletManager.maintenance();
        addMaintenanceTask(nextScheduleTime(tabletCheckIntervalMs), this::tabletMaintenance);
    }

    public static TStatus toStatus(Exception e) {
        TStatus ts = new TStatus();
        ts.setError_msgs(Lists.newArrayList(e.getMessage()));
        if (e instanceof NotImplementedException) {
            ts.setStatus_code(TStatusCode.NOT_IMPLEMENTED_ERROR);
        } else if (e instanceof AlreadyExistsException) {
            ts.setStatus_code(TStatusCode.ALREADY_EXIST);
        } else {
            ts.setStatus_code(TStatusCode.INTERNAL_ERROR);
        }
        return ts;
    }

    void handleCreateTablet(TAgentTaskRequest request, TFinishTaskRequest finish) throws UserException {
        // Ignore the initial disk usage of tablet
        if (request.create_tablet_req.tablet_type == TTabletType.TABLET_TYPE_LAKE) {
            lakeTabletManager.createTablet(request.create_tablet_req);
        } else {
            tabletManager.createTablet(request.create_tablet_req);
        }
    }

    void handleDropTablet(TAgentTaskRequest request, TFinishTaskRequest finish) {
        tabletManager.dropTablet(request.drop_tablet_req.tablet_id, request.drop_tablet_req.force);
    }

    void handlePublish(TAgentTaskRequest request, TFinishTaskRequest finish) {
        cluster.getConfig().injectPublishTaskLatency();
        TPublishVersionRequest task = request.publish_version_req;
        txnManager.publish(task.transaction_id, task.partition_version_infos, finish);
    }

    void handleClone(TAgentTaskRequest request, TFinishTaskRequest finish) throws Exception {
        TCloneReq task = request.clone_req;
        if (task.src_backends.size() != 1) {
            throw new Exception("bad src backends size " + task.src_backends.size());
        }
        TBackend backend = task.src_backends.get(0);
        PseudoBackend srcBackend = cluster.getBackendByHost(backend.host);
        if (srcBackend == null) {
            throw new Exception("clone failed src backend " + backend.host + " not found");
        }
        if (srcBackend.getId() == getId()) {
            throw new Exception("clone failed src backend " + backend.host + " is same as dest backend " + be.getId());
        }
        Tablet srcTablet = srcBackend.getTabletManager().getTablet(task.tablet_id);
        if (srcTablet == null) {
            throw new Exception("clone failed src tablet " + task.tablet_id + " on " + srcBackend.be.getId() + " not found");
        }
        Tablet destTablet = tabletManager.getTablet(task.tablet_id);
        if (destTablet == null) {
            destTablet = new Tablet(task.tablet_id, srcTablet.tableId, srcTablet.partitionId, srcTablet.schemaHash,
                    srcTablet.enablePersistentIndex);
            destTablet.fullCloneFrom(srcTablet, srcBackend.getId());
            tabletManager.addClonedTablet(destTablet);
        } else {
            destTablet.cloneFrom(srcTablet, srcBackend.getId());
        }
        finish.finish_tablet_infos = Lists.newArrayList(destTablet.getTabletInfo());
    }

    private String alterTaskError = null;

    public void injectAlterTaskError(String errMsg) {
        alterTaskError = errMsg;
    }

    private void handleAlter(TAgentTaskRequest request, TFinishTaskRequest finishTaskRequest) throws Exception {
        if (alterTaskError != null) {
            String err = alterTaskError;
            alterTaskError = null;
            throw new Exception(err);
        }
        TAlterTabletReqV2 task = request.alter_tablet_req_v2;
        Tablet baseTablet = tabletManager.getTablet(task.base_tablet_id);
        if (baseTablet == null) {
            throw new Exception(
                    String.format("alter (base:%d, new:%d version:%d) failed base tablet not found", task.base_tablet_id,
                            task.new_tablet_id, task.alter_version));
        }
        Tablet newTablet = tabletManager.getTablet(task.new_tablet_id);
        if (newTablet == null) {
            throw new Exception(
                    String.format("alter (base:%d, new:%d version:%d) failed new tablet not found", task.base_tablet_id,
                            task.new_tablet_id, task.alter_version));
        }
        if (newTablet.isRunning() == true) {
            throw new Exception(
                    String.format("alter (base:%d, new:%d version:%d) failed new tablet is running", task.base_tablet_id,
                            task.new_tablet_id, task.alter_version));
        }
        newTablet.convertFrom(baseTablet, task.alter_version);
        newTablet.setRunning(true);
        finishTaskRequest.finish_tablet_infos = Lists.newArrayList(newTablet.getTabletInfo());
    }

    void handleTask(TAgentTaskRequest request) {
        TFinishTaskRequest finishTaskRequest = new TFinishTaskRequest(tBackend,
                request.getTask_type(), request.getSignature(),
                new TStatus(TStatusCode.OK));
        long v = reportVersion.incrementAndGet();
        finishTaskRequest.setReport_version(v);
        try {
            switch (finishTaskRequest.task_type) {
                case CREATE:
                    handleCreateTablet(request, finishTaskRequest);
                    break;
                case DROP:
                    handleDropTablet(request, finishTaskRequest);
                    break;
                case PUBLISH_VERSION:
                    handlePublish(request, finishTaskRequest);
                    break;
                case CLONE:
                    handleClone(request, finishTaskRequest);
                    break;
                case REALTIME_PUSH:
                    handleRealtimePush(request, finishTaskRequest);
                    break;
                case ALTER:
                    handleAlter(request, finishTaskRequest);
                    break;
                default:
                    LOG.info("ignore task type:" + finishTaskRequest.task_type + " signature:" + finishTaskRequest.signature);
            }
        } catch (Exception e) {
            LOG.warn("Exception in handleTask " + finishTaskRequest.task_type + " " + finishTaskRequest.signature, e);
            finishTaskRequest.setTask_status(toStatus(e));
        }
        try {
            frontendService.finishTask(finishTaskRequest);
        } catch (TException e) {
            LOG.warn("error call finishTask", e);
        }
        synchronized (taskSignatures) {
            Set<Long> signatures = taskSignatures.get(request.task_type);
            if (signatures != null) {
                signatures.remove(request.getSignature());
            }
        }
    }

    private void handleRealtimePush(TAgentTaskRequest request, TFinishTaskRequest finish) {
        TPushReq pushReq = request.getPush_req();
        TPushType pushType = pushReq.getPush_type();
        switch (pushType) {
            case DELETE:
                handleRealtimePushTypeDelete(pushReq, finish);
                break;
            default:
                throw new RuntimeException("pushType:" + pushType + " is not implement");
        }
    }

    private void handleRealtimePushTypeDelete(TPushReq pushReq, TFinishTaskRequest finish) {
        List<TTabletInfo> finishTabletInfos = Lists.newArrayList();
        long tabletId = pushReq.getTablet_id();
        Tablet tablet = tabletManager.getTablet(tabletId);
        finishTabletInfos.add(tablet.getTabletInfo());
        finish.setFinish_tablet_infos(finishTabletInfos);
        finish.setRequest_version(pushReq.getVersion());
    }

    private class HeartBeatClient extends HeartbeatService.Client {
        public HeartBeatClient() {
            super(null);
        }

        @Override
        public THeartbeatResult heartbeat(TMasterInfo masterInfo) throws TException {
            if (shutdown) {
                throw new TTransportException(TTransportException.NOT_OPEN, "backend " + getId() + " shutdown");
            }
            TBackendInfo backendInfo = new TBackendInfo(beThriftPort, httpPort);
            backendInfo.setBrpc_port(brpcPort);
            return new THeartbeatResult(new TStatus(TStatusCode.OK), backendInfo);
        }
    }

    private class BeThriftClient extends BackendService.Client {

        BeThriftClient() {
            super(null);
            new Thread(() -> {
                currentBackend.set(PseudoBackend.this);
                while (true) {
                    try {
                        TAgentTaskRequest request = taskQueue.take();
                        handleTask(request);
                    } catch (InterruptedException e) {
                        LOG.warn("error get task", e);
                    }
                }
            }, "backend-worker-" + be.getId()).start();
        }

        @Override
        public TAgentResult submit_tasks(List<TAgentTaskRequest> tasks) throws TException {
            if (shutdown) {
                throw new TTransportException(TTransportException.NOT_OPEN, "backend " + getId() + " shutdown");
            }
            synchronized (taskSignatures) {
                for (TAgentTaskRequest task : tasks) {
                    Set<Long> signatures = taskSignatures.computeIfAbsent(task.getTask_type(), k -> new HashSet<>());
                    signatures.add(task.getSignature());
                }
            }
            taskQueue.addAll(tasks);
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TExecPlanFragmentResult exec_plan_fragment(TExecPlanFragmentParams params) {
            return null;
        }

        @Override
        public TCancelPlanFragmentResult cancel_plan_fragment(TCancelPlanFragmentParams params) {
            return null;
        }

        @Override
        public TTransmitDataResult transmit_data(TTransmitDataParams params) {
            return null;
        }

        @Override
        public TFetchDataResult fetch_data(TFetchDataParams params) {
            return null;
        }

        @Override
        public TAgentResult make_snapshot(TSnapshotRequest snapshotRequest) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult release_snapshot(String snapshotPath) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult publish_cluster_state(TAgentPublishRequest request) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult submit_etl_task(TMiniLoadEtlTaskRequest request) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TMiniLoadEtlStatusResult get_etl_status(TMiniLoadEtlStatusRequest request) {
            return new TMiniLoadEtlStatusResult(new TStatus(TStatusCode.OK), TEtlState.FINISHED);
        }

        @Override
        public TAgentResult delete_etl_files(TDeleteEtlFilesRequest request) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TStatus submit_export_task(TExportTaskRequest request) {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TExportStatusResult get_export_status(TUniqueId taskId) {
            return new TExportStatusResult(new TStatus(TStatusCode.OK), TExportState.FINISHED);
        }

        @Override
        public TStatus erase_export_task(TUniqueId taskId) {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TTabletStatResult get_tablet_stat() throws TException {
            if (shutdown) {
                throw new TTransportException(TTransportException.NOT_OPEN, "backend " + getId() + " shutdown");
            }
            TTabletStatResult stats = new TTabletStatResult();
            tabletManager.getTabletStat(stats);
            return stats;
        }

        @Override
        public TStatus submit_routine_load_task(List<TRoutineLoadTask> tasks) {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TScanOpenResult open_scanner(TScanOpenParams params) {
            return null;
        }

        @Override
        public TScanBatchResult get_next(TScanNextBatchParams params) {
            return null;
        }

        @Override
        public TScanCloseResult close_scanner(TScanCloseParams params) {
            return null;
        }

    }

    private class PseudoPBackendService implements PBackendService {
        private final ExecutorService executor;

        PseudoPBackendService() {
            executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return new Thread(r, "PBackendService-" + be.getId());
                }
            });
            executor.submit(() -> {
                currentBackend.set(PseudoBackend.this);
            });
        }

        @Override
        public Future<PExecPlanFragmentResult> execPlanFragmentAsync(
                com.starrocks.rpc.PExecPlanFragmentRequest request) {
            if (shutdown) {
                throw new RuntimeException("backend " + getId() + " shutdown");
            }
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            final TExecPlanFragmentParams params = new TExecPlanFragmentParams();
            try {
                deserializer.deserialize(params, request.getSerializedRequest());
            } catch (TException e) {
                LOG.warn("error deserialize request", e);
                PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                result.status = statusPB(TStatusCode.INTERNAL_ERROR, e.getMessage());
                return CompletableFuture.completedFuture(result);
            }
            String queryid = DebugUtil.printId(params.params.query_id);
            final QueryProgress progress = queryProgresses.computeIfAbsent(queryid, k -> new QueryProgress(k));
            if (params.fragment.output_sink != null && params.fragment.output_sink.type == TDataSinkType.RESULT_SINK) {
                resultSinkInstanceToQueryId.put(
                        DebugUtil.printId(params.params.fragment_instance_id), queryid);
            }
            progress.addFragment(1);
            executor.submit(() -> {
                execPlanFragmentWithReport(params);
                progress.completeFragment(1);
            });
            PExecPlanFragmentResult result = new PExecPlanFragmentResult();
            result.status = new StatusPB();
            result.status.statusCode = 0;
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(
                PExecBatchPlanFragmentsRequest request) {
            if (shutdown) {
                throw new RuntimeException("backend " + getId() + " shutdown");
            }
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            final TExecBatchPlanFragmentsParams params = new TExecBatchPlanFragmentsParams();
            try {
                deserializer.deserialize(params, request.getSerializedRequest());
            } catch (TException e) {
                LOG.warn("error deserialize request", e);
                PExecBatchPlanFragmentsResult result = new PExecBatchPlanFragmentsResult();
                result.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                result.status.errorMsgs = Lists.newArrayList(e.getMessage());
                return CompletableFuture.completedFuture(result);
            }

            String queryid = DebugUtil.printId(params.common_param.params.query_id);
            final QueryProgress progress = queryProgresses.computeIfAbsent(queryid, k -> new QueryProgress(k));
            progress.addFragment(params.unique_param_per_instance.size());
            if (params.common_param.fragment.output_sink != null &&
                    params.common_param.fragment.output_sink.type == TDataSinkType.RESULT_SINK) {
                if (params.unique_param_per_instance.size() != 1) {
                    LOG.warn("should only have 1 fragment with RESULT_SINK");
                    PExecBatchPlanFragmentsResult result = new PExecBatchPlanFragmentsResult();
                    result.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                    result.status.errorMsgs = Lists.newArrayList("should only have 1 fragment with RESULT_SINK");
                    return CompletableFuture.completedFuture(result);
                }
                resultSinkInstanceToQueryId.put(
                        DebugUtil.printId(params.unique_param_per_instance.get(0).params.fragment_instance_id), queryid);
            }
            executor.submit(() -> {
                execBatchPlanFragmentsWithReport(params);
                progress.completeFragment(params.unique_param_per_instance.size());
            });
            PExecBatchPlanFragmentsResult result = new PExecBatchPlanFragmentsResult();
            result.status = new StatusPB();
            result.status.statusCode = 0;
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
            if (shutdown) {
                throw new RuntimeException("backend " + getId() + " shutdown");
            }
            return executor.submit(() -> {
                PCancelPlanFragmentResult result = new PCancelPlanFragmentResult();
                StatusPB pStatus = new StatusPB();
                pStatus.statusCode = 0;
                result.status = pStatus;
                return result;
            });
        }

        @Override
        public Future<PTriggerProfileReportResult> triggerProfileReport(
                com.starrocks.rpc.PTriggerProfileReportRequest request) {
            return null;
        }

        @Override
        public Future<PFetchDataResult> fetchDataAsync(com.starrocks.rpc.PFetchDataRequest request) {
            if (shutdown) {
                throw new RuntimeException("backend " + getId() + " shutdown");
            }
            String fid = DebugUtil.printId(request.finstId);
            String queryId = resultSinkInstanceToQueryId.get(fid);
            if (queryId == null) {
                LOG.warn("no queryId found for finstId {}", fid);
                PFetchDataResult result = new PFetchDataResult();
                result.status = statusPB(TStatusCode.INTERNAL_ERROR, "no queryId found for finstId " + fid);
                return CompletableFuture.completedFuture(result);
            }
            final QueryProgress progress = queryProgresses.get(queryId);
            if (progress == null) {
                LOG.warn("no progress found for finstId {} queryId", fid, queryId);
                PFetchDataResult result = new PFetchDataResult();
                result.status = statusPB(TStatusCode.INTERNAL_ERROR,
                        "no progress found for finstId " + request.finstId + " queryId " + queryId);
                return CompletableFuture.completedFuture(result);
            }
            return progress.getFetchDataResult();
        }

        @Override
        public Future<PProxyResult> getInfo(PProxyRequest request) {
            return null;
        }

        @Override
        public Future<PPulsarProxyResult> getPulsarInfo(PPulsarProxyRequest request) {
            return null;
        }

        @Override
        public Future<ExecuteCommandResultPB> executeCommandAsync(ExecuteCommandRequestPB request) {
            ExecuteCommandResultPB result = new ExecuteCommandResultPB();
            StatusPB pStatus = new StatusPB();
            pStatus.statusCode = 0;
            result.status = pStatus;
            if (request.command.equals("execute_script")) {
                result.result = "dummy result";
            } else if (request.command.equals("set_config")) {
                result.result = "";
            } else {
                throw new org.apache.commons.lang3.NotImplementedException("TODO");
            }
            return CompletableFuture.completedFuture(result);
        }
    }

    static TStatus status(TStatusCode code, String msg) {
        TStatus status = new TStatus();
        status.status_code = code;
        status.error_msgs = Lists.newArrayList(msg);
        return status;
    }

    static StatusPB statusPB(TStatusCode code, String msg) {
        StatusPB status = new StatusPB();
        status.statusCode = code.getValue();
        status.errorMsgs = Lists.newArrayList(msg);
        return status;
    }

    void execPlanFragmentWithReport(TExecPlanFragmentParams params) {
        TReportExecStatusParams report = new TReportExecStatusParams();
        report.setProtocol_version(FrontendServiceVersion.V1);
        report.setQuery_id(params.params.query_id);
        report.setBackend_num(params.backend_num);
        report.setBackend_id(be.getId());
        report.setFragment_instance_id(params.params.fragment_instance_id);
        // TODO: support error injection
        report.setStatus(new TStatus(TStatusCode.OK));
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("exec_plan_fragment query: %s fragment: %s", DebugUtil.printId(params.params.query_id),
                    DebugUtil.printId(params.params.fragment_instance_id)));
            int numTabletScan = 0;
            for (TPlanNode planNode : params.fragment.plan.nodes) {
                if (planNode.node_type == TPlanNodeType.OLAP_SCAN_NODE) {
                    List<TScanRangeParams> scanRanges = params.params.per_node_scan_ranges.get(planNode.getNode_id());
                    if (scanRanges != null) {
                        numTabletScan += scanRanges.size();
                        runOlapScan(planNode, scanRanges);
                    }
                    Map<Integer, List<TScanRangeParams>> scanRangesPerDriver =
                            params.params.node_to_per_driver_seq_scan_ranges.get(planNode.getNode_id());
                    if (scanRangesPerDriver != null) {
                        scanRanges = scanRangesPerDriver.values().stream().flatMap(List::stream)
                                .collect(Collectors.toList());
                        numTabletScan += scanRanges.size();
                        runOlapScan(planNode, scanRanges);
                        System.out.printf("per_driver_seq_scan_range not empty numTablets: %d\n", numTabletScan);
                    }
                } else if (planNode.node_type == TPlanNodeType.SCHEMA_SCAN_NODE) {
                    numSchemaScan.incrementAndGet();
                    sb.append(" SchemaScanNode:" + planNode.schema_scan_node.table_name);
                }
            }
            if (numTabletScan > 0) {
                scansByQueryId.computeIfAbsent(DebugUtil.printId(params.params.query_id), k -> new AtomicInteger(0))
                        .addAndGet(numTabletScan);
                sb.append(String.format(" #TabletScan: %d", numTabletScan));
            }
            TDataSink tDataSink = params.fragment.output_sink;
            if (tDataSink != null && tDataSink.type == TDataSinkType.OLAP_TABLE_SINK) {
                runSink(report, tDataSink);
                sb.append(String.format(" sink: %s %s", tDataSink.olap_table_sink.table_id,
                        Objects.toString(tDataSink.olap_table_sink.table_name, "")));
            }
            LOG.info(sb.toString());
        } catch (Throwable e) {
            e.printStackTrace();
            LOG.warn("error execPlanFragmentWithReport", e);
            report.setStatus(status(TStatusCode.INTERNAL_ERROR, e.getMessage()));
        }
        report.setDone(true);
        try {
            TReportExecStatusResult ret = frontendService.reportExecStatus(report);
            if (ret.status.status_code != TStatusCode.OK) {
                LOG.warn("error report exec status " + (ret.status.error_msgs.isEmpty() ? "" : ret.status.error_msgs.get(0)));
            }
        } catch (TException e) {
            LOG.warn("error report exec status", e);
        }
    }

    public static Map<String, AtomicInteger> scansByQueryId = new ConcurrentHashMap<>();

    private void execBatchPlanFragment(TExecPlanFragmentParams commonParams, TExecPlanFragmentParams uniqueParams) {
        final TReportExecStatusParams report = new TReportExecStatusParams();
        report.setProtocol_version(FrontendServiceVersion.V1);
        report.setQuery_id(commonParams.params.query_id);
        report.setBackend_num(uniqueParams.backend_num);
        report.setBackend_id(be.getId());
        report.setFragment_instance_id(uniqueParams.params.fragment_instance_id);
        // TODO: support error injection
        report.setStatus(new TStatus(TStatusCode.OK));
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("exec_batch_plan_fragment query: %s fragment: %s",
                    DebugUtil.printId(commonParams.params.query_id),
                    DebugUtil.printId(uniqueParams.params.fragment_instance_id)));
            int numTabletScan = 0;
            int allScans = uniqueParams.params.per_node_scan_ranges.values().stream().mapToInt(List::size).sum();
            for (TPlanNode planNode : commonParams.fragment.plan.nodes) {
                if (planNode.node_type == TPlanNodeType.OLAP_SCAN_NODE) {
                    List<TScanRangeParams> scanRanges = uniqueParams.params.per_node_scan_ranges.get(planNode.getNode_id());
                    if (scanRanges != null) {
                        numTabletScan += scanRanges.size();
                        runOlapScan(planNode, scanRanges);
                    }
                    Map<Integer, List<TScanRangeParams>> scanRangesPerDriver =
                            uniqueParams.params.node_to_per_driver_seq_scan_ranges.get(planNode.getNode_id());
                    if (scanRangesPerDriver != null) {
                        scanRanges = scanRangesPerDriver.values().stream().flatMap(List::stream)
                                .collect(Collectors.toList());
                        numTabletScan += scanRanges.size();
                        runOlapScan(planNode, scanRanges);
                        System.out.printf("per_driver_seq_scan_range not empty numTablets: %d\n", numTabletScan);
                    }
                } else if (planNode.node_type == TPlanNodeType.SCHEMA_SCAN_NODE) {
                    numSchemaScan.incrementAndGet();
                    sb.append(" SchemaScanNode:" + planNode.schema_scan_node.table_name);
                }
            }
            if (allScans != numTabletScan) {
                System.out.printf(" not all scanrange used: all:%d used:%d\n", allScans, numTabletScan);
            }
            if (numTabletScan > 0) {
                scansByQueryId.computeIfAbsent(DebugUtil.printId(commonParams.params.query_id), k -> new AtomicInteger(0))
                        .addAndGet(numTabletScan);
                sb.append(String.format(" #TabletScan: %d", numTabletScan));
            }
            TDataSink tDataSink = null;
            if (uniqueParams.fragment.output_sink != null) {
                throw new RuntimeException("not support output sink in uniqueParams");
            } else if (commonParams.fragment.output_sink != null) {
                tDataSink = commonParams.fragment.output_sink;
            }
            if (tDataSink != null) {
                if (tDataSink.type == TDataSinkType.OLAP_TABLE_SINK) {
                    runSink(report, tDataSink);
                    sb.append(String.format(" sink: %s %s", tDataSink.olap_table_sink.table_id,
                            Objects.toString(tDataSink.olap_table_sink.table_name, "")));
                } else if (tDataSink.type == TDataSinkType.RESULT_SINK) {
                    LOG.info("query {} fragment {} has RESULT_SINK", DebugUtil.printId(commonParams.params.query_id),
                            DebugUtil.printId(uniqueParams.params.fragment_instance_id));
                }
            }
            LOG.info(sb.toString());
        } catch (Throwable e) {
            e.printStackTrace();
            LOG.warn("error execBatchPlanFragment", e);
            report.setStatus(status(TStatusCode.INTERNAL_ERROR, e.getMessage()));
        }
        report.setDone(true);
        try {
            TReportExecStatusResult ret = frontendService.reportExecStatus(report);
            if (ret.status.status_code != TStatusCode.OK) {
                LOG.warn("error report exec status " + (ret.status.error_msgs.isEmpty() ? "" : ret.status.error_msgs.get(0)));
            }
        } catch (TException e) {
            LOG.warn("error report exec status", e);
        }
    }

    private void execBatchPlanFragmentsWithReport(TExecBatchPlanFragmentsParams params) {
        for (TExecPlanFragmentParams uniqueParams : params.unique_param_per_instance) {
            execBatchPlanFragment(params.common_param, uniqueParams);
        }
    }

    private void runOlapScan(TPlanNode olapScanNode, List<TScanRangeParams> tScanRangeParams)
            throws Exception {
        for (TScanRangeParams scanRangeParams : tScanRangeParams) {
            long tabletId = scanRangeParams.scan_range.internal_scan_range.tablet_id;
            long version = Long.parseLong(scanRangeParams.scan_range.internal_scan_range.version);
            Tablet tablet = tabletManager.getTablet(tabletId);
            if (tablet == null) {
                String msg = String.format("olapScan(be:%d tablet:%d version:%d) failed: tablet not found ", getId(), tabletId,
                        version);
                System.out.println(msg);
                throw new Exception(msg);
            }
            tablet.read(version);
        }
    }

    private void runSink(TReportExecStatusParams report, TDataSink tDataSink) throws Exception {
        if (tDataSink.type != TDataSinkType.OLAP_TABLE_SINK) {
            return;
        }
        cluster.getConfig().injectTableSinkWriteLatency();
        PseudoOlapTableSink sink = new PseudoOlapTableSink(cluster, tDataSink);
        if (!sink.open()) {
            sink.cancel();
            throw new Exception(
                    String.format("open sink failed, backend:%s txn:%d %s", be.getId(), sink.txnId, sink.getErrorMessage()));
        }
        if (!sink.close()) {
            sink.cancel();
            throw new Exception(
                    String.format("open sink failed, backend:%s txn:%d %s", be.getId(), sink.txnId, sink.getErrorMessage()));
        }
        List<TTabletCommitInfo> commitInfos = sink.getTabletCommitInfos();
        report.setCommitInfos(commitInfos);
        report.setLoad_counters(sink.getLoadCounters());
    }

    class LoadChannel {
        class TabletsChannel {
            long indexId;
            List<PTabletWithPartition> tablets;

            TabletsChannel(long indexId) {
                this.indexId = indexId;
            }

            void open(PTabletWriterOpenRequest request) throws Exception {
                tablets = request.tablets;
                for (PTabletWithPartition tabletWithPartition : tablets) {
                    Tablet tablet = tabletManager.getTablet(tabletWithPartition.tabletId);
                    if (tablet == null) {
                        LOG.warn("tablet not found {}", tabletWithPartition.tabletId);
                        throw new Exception("tablet not found " + tabletWithPartition.tabletId);
                    }
                    if (tablet.getRowsetCount() >= Tablet.maxVersions) {
                        throw new Exception(String.format("Too many versions. tablet_id: %d, version_count: %d, limit: %d",
                                tablet.id, tablet.getRowsetCount(), Tablet.maxVersions));
                    }
                }
            }

            void cancel() {
            }

            void close(PTabletWriterAddChunkRequest request, PTabletWriterAddBatchResult result) throws UserException {
                for (PTabletWithPartition tabletWithPartition : tablets) {
                    Tablet tablet = tabletManager.getTablet(tabletWithPartition.tabletId);
                    if (tablet == null) {
                        LOG.warn("tablet not found {}", tabletWithPartition.tabletId);
                        continue;
                    }
                    Rowset rowset = new Rowset(txnId, genRowsetId(), 100, 100000);
                    txnManager.commit(txnId, tablet.partitionId, tablet, rowset);
                    PTabletInfo info = new PTabletInfo();
                    info.tabletId = tablet.id;
                    info.schemaHash = tablet.schemaHash;
                    result.tabletVec.add(info);
                }
            }
        }

        PUniqueId id;
        long txnId;
        Map<Long, TabletsChannel> indexToTabletsChannel = new HashMap<>();

        LoadChannel(PUniqueId id) {
            this.id = id;
        }

        void open(PTabletWriterOpenRequest request, PTabletWriterOpenResult result) throws Exception {
            this.txnId = request.txnId;
            if (indexToTabletsChannel.containsKey(request.indexId)) {
                return;
            }
            TabletsChannel tabletsChannel = indexToTabletsChannel.computeIfAbsent(request.indexId, k -> new TabletsChannel(k));
            tabletsChannel.open(request);
        }

        void cancel() {
            for (TabletsChannel tabletsChannel : indexToTabletsChannel.values()) {
                tabletsChannel.cancel();
            }
        }

        void close(PTabletWriterAddChunkRequest request, PTabletWriterAddBatchResult result) throws UserException {
            TabletsChannel tabletsChannel = indexToTabletsChannel.get(request.indexId);
            if (tabletsChannel == null) {
                result.status =
                        statusPB(TStatusCode.INTERNAL_ERROR, "cannot find the tablets channel associated with the index id");
            } else {
                tabletsChannel.close(request, result);
            }
        }
    }

    Map<String, LoadChannel> loadChannels = new HashMap<>();

    synchronized PTabletWriterOpenResult tabletWriterOpen(PTabletWriterOpenRequest request) {
        PTabletWriterOpenResult result = new PTabletWriterOpenResult();
        result.status = new StatusPB();
        result.status.statusCode = 0;
        result.status.errorMsgs = new ArrayList<>();
        String loadIdString = String.format("%d-%d", request.id.hi, request.id.lo);
        LoadChannel loadChannel = loadChannels.computeIfAbsent(loadIdString, k -> new LoadChannel(request.id));
        try {
            loadChannel.open(request, result);
        } catch (Exception e) {
            loadChannel.cancel();
            loadChannels.remove(loadIdString);
            result.status = statusPB(TStatusCode.INTERNAL_ERROR, e.getMessage());
        }
        return result;
    }

    synchronized PTabletWriterCancelResult tabletWriterCancel(PTabletWriterCancelRequest request) {
        PTabletWriterCancelResult result = new PTabletWriterCancelResult();
        String loadIdString = String.format("%d-%d", request.id.hi, request.id.lo);
        LoadChannel loadChannel = loadChannels.get(loadIdString);
        if (loadChannel != null) {
            loadChannel.cancel();
        }
        return result;
    }

    /**
     * We update the disk usage when a txn is published successfully.
     * Currently, we update it in rowset granularity, i.e. no matter how many bytes we write in a txn,
     * the cost of disk space is the same.
     *
     * @param delta the number of bytes to increase or decrease
     */
    public void updateDiskUsage(long delta) {
        if ((currentAvailableCapacityB - delta) < currentTotalCapacityB * 0.05) {
            return;
        }
        currentDataUsedCapacityB += delta;
        currentAvailableCapacityB -= delta;
    }

    synchronized PTabletWriterAddBatchResult tabletWriterAddChunk(PTabletWriterAddChunkRequest request) {
        PTabletWriterAddBatchResult result = new PTabletWriterAddBatchResult();
        result.status = new StatusPB();
        result.status.statusCode = 0;
        result.status.errorMsgs = new ArrayList<>();
        result.tabletVec = new ArrayList<>();
        if (request.eos) {
            String loadIdString = String.format("%d-%d", request.id.hi, request.id.lo);
            LoadChannel channel = loadChannels.get(loadIdString);
            if (channel != null) {
                if (random.nextFloat() < writeFailureRate) {
                    channel.cancel();
                    loadChannels.remove(loadIdString);
                    result.status = statusPB(TStatusCode.INTERNAL_ERROR, "inject error writeFailureRate:" + writeFailureRate);
                    LOG.warn("inject write failure txn:{} backend:{}", request.txnId, be.getId());
                    return result;
                }
                try {
                    channel.close(request, result);
                } catch (UserException e) {
                    LOG.warn("error close load channel", e);
                    channel.cancel();
                    loadChannels.remove(loadIdString);
                    result.status = statusPB(TStatusCode.INTERNAL_ERROR, e.getMessage());
                }
            } else {
                result.status = statusPB(TStatusCode.INTERNAL_ERROR, "no associated load channel");
            }
        }
        return result;
    }
}
