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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.DictionaryMgrInfo;
import com.starrocks.persist.DropDictionaryInfo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.UpdateDictionaryLog;
import com.starrocks.persist.UpdateDictionaryMgrLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.DictionaryCacheSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheRequestType;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DictionaryMgr implements Writable, GsonPostProcessable {
    private static final Logger LOG = LoggerFactory.getLogger(DictionaryMgr.class);

    @SerializedName(value = "dictionariesMapById")
    private Map<Long, Dictionary> dictionariesMapById = new HashMap<>();
    @SerializedName(value = "dictionariesIdMapByName")
    private Map<String, Long> dictionariesIdMapByName = new HashMap<>();
    @SerializedName(value = "nextTxnId")
    private volatile long nextTxnId = 1L;
    @SerializedName(value = "nextDictionaryId")
    private volatile long nextDictionaryId = 1L;

    private Set<Long> unfinishedRefreshTasks = Sets.newHashSet();
    private final Set<Long> runningRefreshTasks = Sets.newHashSet();

    // Leader memory only. For each dictionary: the nodes that committed the current version, with their start time.
    private final Map<Long, Map<TNetworkAddress, Long>> syncedNodeStartTimes = new HashMap<>();
    private final Map<Long, Long> lastNodeResyncTime = new HashMap<>();
    // Early refreshes in a row that did not give every node a copy. Each one doubles the wait before the next.
    private final Map<Long, Integer> nodeResyncCount = new HashMap<>();
    // Refreshes that CANCEL REFRESH stopped. They must not go on with the nodes that the cancel did not reach.
    private final Set<Long> cancelledRefreshTasks = Sets.newHashSet();
    // Refreshes that the scheduler started because a node had no copy. Kept until the refresh ends, so a
    // refresh that is submitted again later is still an early refresh. The REFRESHING log entry carries the
    // mark, so followers keep it too and a new leader that resumes the refresh still rolls it back on failure.
    private final Set<Long> earlyRefreshTasks = Sets.newHashSet();
    // Dictionaries with no record, for example on a new leader. The leader asks the nodes for their version.
    private final Set<Long> pendingNodeProbes = Sets.newHashSet();
    private final Set<Long> runningNodeProbes = Sets.newHashSet();

    private final Lock lock = new ReentrantLock();

    private final ExecutorService executor =
            ThreadPoolManager.newDaemonFixedThreadPool(
                    Config.refresh_dictionary_cache_thread_num, Integer.MAX_VALUE, "refresh-dictionary-cache-pool",
                    true);

    public DictionaryMgr() {
    }

    public void scheduleTasks() {
        lock.lock();
        try {
            if (!GlobalStateMgr.getCurrentState().isLeader()) {
                return;
            }

            List<UpdateDictionaryLog> updateDictionaryLogList = Lists.newArrayList();
            long ts = System.currentTimeMillis();
            Map<TNetworkAddress, Long> availableNodes =
                    dictionariesMapById.isEmpty() ? Map.of() : getAvailableNodeStartTimes();
            for (Map.Entry<Long, Dictionary> entry : dictionariesMapById.entrySet()) {
                long id = entry.getKey();
                Dictionary dictionary = dictionariesMapById.get(id);
                // regular schedule
                boolean regular = (dictionary.getNextSchedulableTime() <= System.currentTimeMillis() &&
                        !unfinishedRefreshTasks.contains(id)) ||
                        // follower -> leader when dictionary is refreshing.
                        (dictionary.isRefreshing() && !runningRefreshTasks.contains(id));
                // a node has no copy of the current version
                boolean early = !regular && needNodeResyncUnlocked(dictionary, availableNodes, ts);
                if (regular || early) {
                    UpdateDictionaryLog updateDictionaryLog = new UpdateDictionaryLog(id, ts);
                    updateDictionaryLog.setState(Dictionary.DictionaryState.REFRESHING);
                    // A refresh that a new leader resumes stays an early refresh.
                    updateDictionaryLog.setEarlyRefresh(early || earlyRefreshTasks.contains(id));
                    updateDictionaryLogList.add(updateDictionaryLog);
                }
            }

            if (!updateDictionaryLogList.isEmpty()) {
                GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(
                        new UpdateDictionaryMgrLog(updateDictionaryLogList),
                        wal -> {
                            for (UpdateDictionaryLog log : updateDictionaryLogList) {
                                Dictionary dict = dictionariesMapById.get(log.getDictionaryId());
                                dict.setRefreshing(log.getTs());
                                unfinishedRefreshTasks.add(dict.getDictionaryId());
                                if (log.isEarlyRefresh()) {
                                    earlyRefreshTasks.add(dict.getDictionaryId());
                                }
                            }
                        });
            }

            for (Long dictionaryId : unfinishedRefreshTasks) {
                // new added task
                if (!runningRefreshTasks.contains(dictionaryId)) {
                    resigerUnfinishedToRunningUnlocked(dictionaryId);

                    RefreshDictionaryCacheWorker task =
                            new RefreshDictionaryCacheWorker(dictionariesMapById.get(dictionaryId),
                                    getAndIncrementTxnIdUnlocked());
                    task.setEarlyRefresh(earlyRefreshTasks.contains(dictionaryId));
                    try {
                        submit(task);
                    } catch (RejectedExecutionException e) {
                        runningRefreshTasks.remove(dictionaryId); // re-schedule later
                    }
                }
            }

            for (Long dictionaryId : pendingNodeProbes) {
                Dictionary dictionary = dictionariesMapById.get(dictionaryId);
                try {
                    executor.submit(() -> probeNodeVersions(dictionary));
                    runningNodeProbes.add(dictionaryId);
                } catch (RejectedExecutionException e) {
                    // The next pass asks again.
                }
            }
            pendingNodeProbes.clear();
        } finally {
            lock.unlock();
        }
    }

    public static void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
        List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableBackends();
        for (Backend backend : backends) {
            nodes.add(backend.getBrpcAddress());
        }

        List<ComputeNode> computeNodes =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableComputeNodes();
        for (ComputeNode cn : computeNodes) {
            nodes.add(cn.getBrpcAddress());
        }
    }

    // The start time of each available node. A new start time means that the node restarted and lost its cache.
    public static Map<TNetworkAddress, Long> getAvailableNodeStartTimes() {
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        Map<TNetworkAddress, Long> startTimes = new HashMap<>();
        for (Backend backend : clusterInfo.getAvailableBackends()) {
            startTimes.put(backend.getBrpcAddress(), backend.getLastStartTime());
        }
        for (ComputeNode cn : clusterInfo.getAvailableComputeNodes()) {
            startTimes.put(cn.getBrpcAddress(), cn.getLastStartTime());
        }
        return startTimes;
    }

    // True if an available node restarted, joined, or missed the last commit. Queries on that node fail
    // until the next refresh, so refresh now instead of at the next regular time.
    boolean needNodeResyncUnlocked(Dictionary dictionary, Map<TNetworkAddress, Long> availableNodes, long now) {
        long id = dictionary.getDictionaryId();
        // With dictionary_read_latest, every query fails while the dictionary refreshes. An early refresh only
        // repairs nodes, so it must not cause that. Such a node waits for the regular refresh, as before.
        if (Config.dictionary_node_resync_min_interval_second < 0 ||
                dictionary.getReadLatest() ||
                dictionary.getState() != Dictionary.DictionaryState.FINISHED ||
                unfinishedRefreshTasks.contains(id)) {
            return false;
        }
        Map<TNetworkAddress, Long> synced = syncedNodeStartTimes.get(id);
        if (synced == null) {
            // No record on a new leader. Ask the nodes, and decide in a later pass.
            if (!runningNodeProbes.contains(id)) {
                pendingNodeProbes.add(id);
            }
            return false;
        }
        TNetworkAddress missing = null;
        for (Map.Entry<TNetworkAddress, Long> node : availableNodes.entrySet()) {
            if (!node.getValue().equals(synced.get(node.getKey()))) {
                missing = node.getKey();
                break;
            }
        }
        if (missing == null) {
            lastNodeResyncTime.remove(id);
            nodeResyncCount.remove(id);
            return false;
        }
        // A node that fails every refresh must not cause a full refresh every few seconds.
        int count = nodeResyncCount.getOrDefault(id, 0);
        Long lastResync = lastNodeResyncTime.get(id);
        if (lastResync != null && now - lastResync < nodeResyncWaitMs(count)) {
            return false;
        }
        LOG.info("refresh dictionary {} because node {} has no copy of version {}, early refresh {} in a row",
                dictionary.getDictionaryName(), missing, dictionary.getLastSuccessVersion(), count + 1);
        lastNodeResyncTime.put(id, now);
        nodeResyncCount.put(id, count + 1);
        return true;
    }

    // One scheduler pass. Below it, a min of 0 would keep the doubled wait at 0.
    private static final long MIN_NODE_RESYNC_WAIT_MS = 10_000L;

    private static long nodeResyncWaitMs(int count) {
        long minMs = Math.max(Config.dictionary_node_resync_min_interval_second * 1000L, MIN_NODE_RESYNC_WAIT_MS);
        long maxMs = Math.max(minMs, Config.dictionary_node_resync_max_interval_second * 1000L);
        return Math.min(minMs << Math.min(Math.max(count - 1, 0), 20), maxMs);
    }

    private void recordSyncedNodes(long dictionaryId, List<TNetworkAddress> committedNodes,
                                   Map<TNetworkAddress, Long> nodeStartTimes) {
        recordSyncedNodes(dictionaryId, committedNodes, nodeStartTimes, false);
    }

    private void recordSyncedNodes(long dictionaryId, List<TNetworkAddress> committedNodes,
                                   Map<TNetworkAddress, Long> nodeStartTimes, boolean onlyIfAbsent) {
        Map<TNetworkAddress, Long> synced = new HashMap<>();
        for (TNetworkAddress node : committedNodes) {
            // A node with no start time is left out, so the next check refreshes it.
            Long startTime = nodeStartTimes.get(node);
            if (startTime != null) {
                synced.put(node, startTime);
            }
        }
        lock.lock();
        try {
            if (dictionariesMapById.containsKey(dictionaryId) &&
                    !(onlyIfAbsent && syncedNodeStartTimes.containsKey(dictionaryId))) {
                syncedNodeStartTimes.put(dictionaryId, synced);
            }
        } finally {
            lock.unlock();
        }
    }

    private void clearNodeResyncWait(long dictionaryId) {
        lock.lock();
        try {
            lastNodeResyncTime.remove(dictionaryId);
            nodeResyncCount.remove(dictionaryId);
        } finally {
            lock.unlock();
        }
    }

    boolean isNodeProbePending(long dictionaryId) {
        lock.lock();
        try {
            return pendingNodeProbes.contains(dictionaryId) || runningNodeProbes.contains(dictionaryId);
        } finally {
            lock.unlock();
        }
    }

    // A node with the current version counts as synced. A node with another version, with no copy (-1), or that
    // does not answer, does not, so the next pass refreshes it. An old BE does not report its version and counts
    // as synced, as before.
    void probeNodeVersions(Dictionary dictionary) {
        long id = dictionary.getDictionaryId();
        try {
            Map<TNetworkAddress, Long> startTimes = getAvailableNodeStartTimes();
            List<TNetworkAddress> nodes = Lists.newArrayList(startTimes.keySet());
            PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
            request.dictId = id;
            request.type = PProcessDictionaryCacheRequestType.STATISTIC;
            List<TNetworkAddress> failedNodes = Lists.newArrayList();
            List<PProcessDictionaryCacheResult> results = Lists.newArrayList();
            processDictionaryCacheInteranl(request, nodes, results, failedNodes);

            long version = dictionary.getLastSuccessVersion();
            List<TNetworkAddress> synced = Lists.newArrayList();
            int next = 0;
            for (TNetworkAddress node : nodes) {
                if (failedNodes.contains(node)) {
                    continue;
                }
                Long nodeVersion = results.get(next++).dictionaryVersion;
                if (nodeVersion == null || nodeVersion == version) {
                    synced.add(node);
                }
            }
            // A refresh that ended in the meantime has a newer record. Keep it.
            recordSyncedNodes(id, synced, startTimes, true);
        } finally {
            lock.lock();
            try {
                pendingNodeProbes.remove(id);
                runningNodeProbes.remove(id);
            } finally {
                lock.unlock();
            }
        }
    }

    Map<TNetworkAddress, Long> getSyncedNodeStartTimes(long dictionaryId) {
        return syncedNodeStartTimes.get(dictionaryId);
    }

    public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                       List<TNetworkAddress> beNodes,
                                                                       List<PProcessDictionaryCacheResult> results) {
        return processDictionaryCacheInteranl(request, beNodes, results, null);
    }

    // If failedNodes is null, the first failed node stops the loop. If not, the failed nodes go into failedNodes,
    // the other nodes still get the request, and the first error is returned.
    public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                       List<TNetworkAddress> beNodes,
                                                                       List<PProcessDictionaryCacheResult> results,
                                                                       List<TNetworkAddress> failedNodes) {
        Pair<Boolean, String> firstError = null;
        for (TNetworkAddress address : beNodes) {
            Pair<Boolean, String> ret = processDictionaryCacheOnNode(request, address, results);
            if (!ret.first) {
                continue;
            }
            if (failedNodes == null) {
                return ret;
            }
            failedNodes.add(address);
            if (firstError == null) {
                firstError = ret;
            }
        }
        if (firstError != null) {
            return firstError;
        }
        LOG.info("finish processDictionaryCache dictionary id: {}, txn id: {}, request type: {}",
                request.dictId, request.txnId, request.type);
        return new Pair<>(false, "");
    }

    private static Pair<Boolean, String> processDictionaryCacheOnNode(PProcessDictionaryCacheRequest request,
                                                                      TNetworkAddress address,
                                                                      List<PProcessDictionaryCacheResult> results) {
        PProcessDictionaryCacheResult result = null;
        try {
            Future<PProcessDictionaryCacheResult> future =
                    BackendServiceClient.getInstance().processDictionaryCache(address, request);
            result = future.get();
        } catch (Exception e) {
            LOG.warn(" processDictionaryCache failed in: " + address + " rpc error :" + e.getMessage());
            return new Pair<>(true, e.getMessage());
        }

        TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
        if (code != TStatusCode.OK) {
            LOG.warn(" processDictionaryCache failed in: " + address + " err msg " + result.status.errorMsgs);
            String errMsg = result.status.errorMsgs.size() == 0 ? "" : result.status.errorMsgs.get(0);
            return new Pair<>(true, errMsg);
        }

        if (results != null) {
            results.add(result);
        }
        return new Pair<>(false, "");
    }

    public void createDictionary(CreateDictionaryStmt stmt, String catalogName, String dbName) throws DdlException {
        Dictionary dictionary = new Dictionary(getAndIncrementDictionaryId(), stmt.getDictionaryName(),
                stmt.getQueryableObject(), catalogName, dbName, stmt.getDictionaryKeys(),
                stmt.getDictionaryValues(), stmt.getProperties());
        dictionary.buildDictionaryProperties();
        GlobalStateMgr.getCurrentState().getEditLog()
                .logCreateDictionary(dictionary, wal -> addDictionary(dictionary));

        if (dictionary.needWarmUp()) {
            try {
                refreshDictionary(stmt.getDictionaryName());
            } catch (MetaNotFoundException e) {
                throw new DdlException("create dictionary failed: " + e.getMessage());
            }
        } else {
            dictionary.updateNextSchedulableTime(dictionary.getRefreshInterval());
        }
    }

    public void dropDictionary(String dictionaryName, boolean isCacheOnly)
            throws MetaNotFoundException {
        Dictionary dictionary = null;
        lock.lock();
        try {
            dictionary = getDictionaryByName(dictionaryName);
            if (dictionary == null) {
                throw new MetaNotFoundException("refreshed dictionary not found");
            }

            if (!isCacheOnly) {
                DropDictionaryInfo info = new DropDictionaryInfo(dictionaryName);
                final Dictionary finalDictionary = dictionary;
                GlobalStateMgr.getCurrentState().getEditLog().logDropDictionary(info, wal -> {
                    dropDictionaryInternal(finalDictionary);
                });
            }
        } finally {
            lock.unlock();
        }
        if (isCacheOnly) {
            // reset dictionary state if just clear the dictionary cache
            getDictionaryByName(dictionaryName).resetState();
        }
        clearDictionaryCache(dictionary, false);
    }

    private void dropDictionaryInternal(Dictionary dictionary) {
        dictionariesMapById.remove(dictionary.getDictionaryId());
        dictionariesIdMapByName.remove(dictionary.getDictionaryName());
        unfinishedRefreshTasks.remove(dictionary.getDictionaryId());
        syncedNodeStartTimes.remove(dictionary.getDictionaryId());
        lastNodeResyncTime.remove(dictionary.getDictionaryId());
        nodeResyncCount.remove(dictionary.getDictionaryId());
        cancelledRefreshTasks.remove(dictionary.getDictionaryId());
        earlyRefreshTasks.remove(dictionary.getDictionaryId());
        pendingNodeProbes.remove(dictionary.getDictionaryId());
        runningNodeProbes.remove(dictionary.getDictionaryId());
    }

    public void replayDropDictionary(String dictionaryName) {
        Dictionary dictionary;
        lock.lock();
        try {
            dictionary = getDictionaryByName(dictionaryName);
            if (dictionary != null) {
                dropDictionaryInternal(dictionary);
            }
        } finally {
            lock.unlock();
        }
        if (dictionary != null) {
            clearDictionaryCache(dictionary, false);
        }
    }

    public void refreshDictionary(String dictionaryName) throws MetaNotFoundException {
        lock.lock();
        try {
            Dictionary dictionary = getDictionaryByName(dictionaryName);
            if (dictionary == null) {
                throw new MetaNotFoundException("refreshed dictionary not found");
            }

            long ts = System.currentTimeMillis();
            UpdateDictionaryLog updateDictionaryLog = new UpdateDictionaryLog(dictionary.getDictionaryId(), ts);
            updateDictionaryLog.setState(Dictionary.DictionaryState.REFRESHING);
            GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(
                    new UpdateDictionaryMgrLog(Lists.newArrayList(updateDictionaryLog)),
                    wal -> dictionary.setRefreshing(ts));
            unfinishedRefreshTasks.add(dictionary.getDictionaryId());
        } finally {
            lock.unlock();
        }
    }

    public void cancelRefreshDictionary(String dictionaryName) {
        lock.lock();
        try {
            Dictionary dictionary = getDictionaryByName(dictionaryName);
            if (unfinishedRefreshTasks.contains(dictionary.getDictionaryId())) {
                cancelledRefreshTasks.add(dictionary.getDictionaryId());
            }
            clearDictionaryCache(dictionary, true);
        } finally {
            lock.unlock();
        }
    }

    public void clearDictionaryCache(Dictionary dictionary, boolean cancel) {
        PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
        request.dictId = dictionary.getDictionaryId();
        request.isCancel = cancel;
        request.type = PProcessDictionaryCacheRequestType.CLEAR;

        List<TNetworkAddress> beNodes = Lists.newArrayList();
        fillBackendsOrComputeNodes(beNodes);

        DictionaryMgr.processDictionaryCacheInteranl(request, beNodes, null);
    }

    public void addDictionary(Dictionary dictionary) {
        lock.lock();
        try {
            dictionariesMapById.put(dictionary.getDictionaryId(), dictionary);
            dictionariesIdMapByName.put(dictionary.getDictionaryName(), dictionary.getDictionaryId());
            // init for every dictionary
            dictionary.setLastSuccessVersion(0L);
        } finally {
            lock.unlock();
        }
    }

    public boolean isExist(String dictionaryName) {
        lock.lock();
        boolean isExist = false;
        try {
            for (Map.Entry<Long, Dictionary> entry : dictionariesMapById.entrySet()) {
                Dictionary dictionary = entry.getValue();
                if (dictionaryName.equals(dictionary.getDictionaryName())) {
                    isExist = true;
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return isExist;
    }

    public void unresigerRunningAndUnfinised(long dictionaryId) {
        lock.lock();
        try {
            runningRefreshTasks.remove(dictionaryId);
            unfinishedRefreshTasks.remove(dictionaryId);
            cancelledRefreshTasks.remove(dictionaryId);
            earlyRefreshTasks.remove(dictionaryId);
        } finally {
            lock.unlock();
        }
    }

    boolean isEarlyRefreshTask(long dictionaryId) {
        lock.lock();
        try {
            return earlyRefreshTasks.contains(dictionaryId);
        } finally {
            lock.unlock();
        }
    }

    private boolean isRefreshCancelled(long dictionaryId) {
        lock.lock();
        try {
            return cancelledRefreshTasks.contains(dictionaryId);
        } finally {
            lock.unlock();
        }
    }

    public Dictionary getDictionaryByName(String dictionaryName) {
        return dictionariesMapById.get(dictionariesIdMapByName.get(dictionaryName));
    }

    private void resigerUnfinishedToRunningUnlocked(long dictionaryId) {
        Preconditions.checkState(unfinishedRefreshTasks.contains(dictionaryId));
        Preconditions.checkState(!runningRefreshTasks.contains(dictionaryId));

        runningRefreshTasks.add(dictionaryId);
    }

    public long getAndIncrementTxnIdUnlocked() {
        long curTxnId = nextTxnId;
        GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(
                new UpdateDictionaryMgrLog(this.nextTxnId + 1, this.nextDictionaryId),
                wal -> ++nextTxnId);
        return curTxnId;
    }

    private long getAndIncrementDictionaryId() {
        lock.lock();
        try {
            long curDictionaryId = nextDictionaryId;
            GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(
                    new UpdateDictionaryMgrLog(this.nextTxnId, this.nextDictionaryId + 1),
                    wal -> ++nextDictionaryId);
            return curDictionaryId;
        } finally {
            lock.unlock();
        }
    }

    public Map<Long, Dictionary> getDictionariesMapById() {
        return dictionariesMapById;
    }

    public Map<String, Long> getDictionariesIdMapByName() {
        return dictionariesIdMapByName;
    }

    public Set<Long> getUnfinishedRefreshTasks() {
        return unfinishedRefreshTasks;
    }

    public long getNextTxnId() {
        return nextTxnId;
    }

    public long getNextDictionaryId() {
        return nextDictionaryId;
    }

    public long getLastSuccessTxnId(long dictionaryId) {
        return dictionariesMapById.get(dictionaryId).getLastSuccessVersion();
    }

    private Coordinator.Factory getCoordinatorFactory() {
        return new DefaultCoordinator.Factory();
    }

    private void submit(RefreshDictionaryCacheWorker task) throws RejectedExecutionException {
        if (task == null) {
            return;
        }
        executor.submit(task);
    }

    public Pair<Map<TNetworkAddress, PProcessDictionaryCacheResult>, String> getDictionaryStatistic(Dictionary dictionary) {
        PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
        request.dictId = dictionary.getDictionaryId();
        request.type = PProcessDictionaryCacheRequestType.STATISTIC;

        List<TNetworkAddress> beNodes = Lists.newArrayList();
        fillBackendsOrComputeNodes(beNodes);

        List<PProcessDictionaryCacheResult> results = Lists.newArrayList();
        Pair<Boolean, String> ret = DictionaryMgr.processDictionaryCacheInteranl(request, beNodes, results);
        Map<TNetworkAddress, PProcessDictionaryCacheResult> resultMap = new HashMap<>();
        if (results.size() < beNodes.size()) {
            Preconditions.checkState(ret.first);
            return new Pair<>(null, ret.second);
        }
        Preconditions.checkState(results.size() == beNodes.size());

        for (int i = 0; i < results.size(); i++) {
            resultMap.put(beNodes.get(i), results.get(i));
        }
        return new Pair<>(resultMap, "");
    }

    public List<List<String>> getAllInfo(String dictionaryName) throws Exception {
        List<List<String>> allInfo = Lists.newArrayList();
        lock.lock();
        try {
            for (Map.Entry<Long, Dictionary> entry : dictionariesMapById.entrySet()) {
                Dictionary dictionary = entry.getValue();
                if (dictionaryName != null && !dictionary.getDictionaryName().equals(dictionaryName)) {
                    continue;
                }

                allInfo.add(dictionary.getInfo());

                Pair<Map<TNetworkAddress, PProcessDictionaryCacheResult>, String> ret = getDictionaryStatistic(dictionary);
                Map<TNetworkAddress, PProcessDictionaryCacheResult> resultMap = ret.first;
                String errMsg = ret.second;
                if (resultMap == null) {
                    allInfo.get(allInfo.size() - 1).add("Can not get memory info, errMsg: " + errMsg);
                    continue;
                }

                String memoryUsage = "";
                for (Map.Entry<TNetworkAddress, PProcessDictionaryCacheResult> result : resultMap.entrySet()) {
                    TNetworkAddress address = result.getKey();
                    memoryUsage += address.getHostname() + ":" + String.valueOf(address.getPort()) + " : ";

                    if (result.getValue() != null) {
                        memoryUsage += String.valueOf(result.getValue().dictionaryMemoryUsage) + ", ";
                    } else {
                        memoryUsage += "Can not get Memory info" + ", ";
                    }
                }
                allInfo.get(allInfo.size() - 1).add(memoryUsage.substring(0, memoryUsage.length() - 2));
            }
        } finally {
            lock.unlock();
        }
        return allInfo;
    }

    public void replayCreateDictionary(Dictionary dictionary) {
        dictionary.updateNextSchedulableTime(dictionary.getRefreshInterval());
        addDictionary(dictionary);
    }

    public void replayModifyDictionaryMgr(DictionaryMgrInfo info) {
        long newNextTxnId = info.getNextTxnId();
        long newNextDictionaryId = info.getNextDictionaryId();
        List<Dictionary> dictionaries = info.getDictionaries();

        if (newNextTxnId > this.nextTxnId) {
            this.nextTxnId = newNextTxnId;
        }

        if (newNextDictionaryId > this.nextDictionaryId) {
            this.nextDictionaryId = newNextDictionaryId;
        }

        if (dictionaries != null && !dictionaries.isEmpty()) {
            lock.lock();
            try {
                for (Dictionary dictionary : dictionaries) {
                    // update dictionary object state
                    if (dictionariesMapById.containsKey(dictionary.getDictionaryId())) {
                        dictionariesMapById.put(dictionary.getDictionaryId(), dictionary);
                    } else {
                        LOG.warn("dictionary {}, id {} has been deleted",
                                dictionary.getDictionaryName(), dictionary.getDictionaryId());
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void replayModifyDictionaryMgr(UpdateDictionaryMgrLog log) {
        long newNextTxnId = log.getNextTxnId();
        long newNextDictionaryId = log.getNextDictionaryId();

        if (newNextTxnId > this.nextTxnId) {
            this.nextTxnId = newNextTxnId;
        }

        if (newNextDictionaryId > this.nextDictionaryId) {
            this.nextDictionaryId = newNextDictionaryId;
        }

        List<UpdateDictionaryLog> dictionaryLogList = log.getDictionaryLogList();
        if (dictionaryLogList != null && !dictionaryLogList.isEmpty()) {
            lock.lock();
            try {
                for (UpdateDictionaryLog dictionaryLog : dictionaryLogList) {
                    Dictionary dictionary = dictionariesMapById.get(dictionaryLog.getDictionaryId());
                    if (dictionary != null) {
                        if (dictionaryLog.getState() != null) {
                            switch (dictionaryLog.getState()) {
                                case COMMITTING:
                                    dictionary.setCommitting();
                                    break;
                                case FINISHED:
                                    dictionary.setFinished(dictionaryLog.getTs(), dictionaryLog.getLastSuccessVersion());
                                    earlyRefreshTasks.remove(dictionaryLog.getDictionaryId());
                                    break;
                                case CANCELLED:
                                    dictionary.setCancelled();
                                    earlyRefreshTasks.remove(dictionaryLog.getDictionaryId());
                                    break;
                                case REFRESHING:
                                    dictionary.setRefreshing(dictionaryLog.getTs());
                                    if (dictionaryLog.isEarlyRefresh()) {
                                        earlyRefreshTasks.add(dictionaryLog.getDictionaryId());
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (dictionaryLog.isResetStateBeforeRefresh()) {
                            dictionary.resetStateBeforeRefresh();
                            earlyRefreshTasks.remove(dictionaryLog.getDictionaryId());
                        }
                        if (dictionaryLog.getErrorMsg() != null) {
                            dictionary.setErrorMsg(dictionaryLog.getErrorMsg());
                        }
                    } else {
                        LOG.warn("dictionary id {} has been deleted", dictionaryLog.getDictionaryId());
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.DICTIONARY_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        DictionaryMgr data = reader.readJson(DictionaryMgr.class);

        this.dictionariesMapById = data.getDictionariesMapById();
        this.dictionariesIdMapByName = data.getDictionariesIdMapByName();
        this.nextTxnId = data.getNextTxnId();
        this.nextDictionaryId = data.getNextDictionaryId();
    }



    @Override
    public void gsonPostProcess() throws IOException {
        lock.lock();
        try {
            for (Map.Entry<Long, Dictionary> entry : dictionariesMapById.entrySet()) {
                // init for every dictionary
                entry.getValue().resetState();
            }
        } finally {
            lock.unlock();
        }
    }

    public class RefreshDictionaryCacheWorker implements Runnable {
        private Dictionary dictionary;
        private long txnId;
        private List<TNetworkAddress> beNodes = Lists.newArrayList();
        private Map<TNetworkAddress, Long> nodeStartTimes = new HashMap<>();
        private boolean error;
        private String errMsg;
        // Read once, so that every step of this refresh follows the same rule.
        private final boolean leaveOutFailedNodes;
        // True if the scheduler started this refresh because a node had no copy.
        private boolean earlyRefresh = false;

        public RefreshDictionaryCacheWorker(Dictionary dictionary, long txnId) {
            this.dictionary = dictionary;
            this.txnId = txnId;
            this.error = false;
            this.errMsg = "";
            this.leaveOutFailedNodes = Config.dictionary_refresh_leave_out_failed_nodes;
            initializeBeNodesAddress();
        }

        private void initializeBeNodesAddress() {
            fillBackendsOrComputeNodes(this.beNodes);
            this.nodeStartTimes = getAvailableNodeStartTimes();
        }

        void setEarlyRefresh(boolean earlyRefresh) {
            this.earlyRefresh = earlyRefresh;
        }

        boolean isEarlyRefresh() {
            return earlyRefresh;
        }

        protected void setError(boolean isError, String errMsg) {
            this.error = isError;
            this.errMsg = errMsg;
        }

        private ConnectContext buildConnectContext() {
            ConnectContext context = ConnectContext.buildInner();
            context.setCurrentCatalog(dictionary.getCatalogName());
            context.setDatabase(dictionary.getDbName());
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.setQualifiedUser(UserIdentity.ROOT.getUser());
            // Set warehouse FIRST: ConnectContext.setCurrentWarehouse() replaces sessionVariable
            // with a fresh clone of defaultSessionVariable, which would discard every override
            // applied below.
            WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = manager.getBackgroundWarehouse();
            context.setCurrentWarehouse(warehouse.getName());
            context.getSessionVariable().setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            context.getSessionVariable().setEnablePipelineEngine(true);
            context.getSessionVariable().setPipelineDop(0);
            context.getSessionVariable().setEnableProfile(false);
            context.getSessionVariable().setEnableMaterializedViewRewrite(false);
            context.getSessionVariable().setParallelExecInstanceNum(1);
            context.getSessionVariable().setQueryTimeoutS(3600); // 1h
            context.setQueryId(UUIDUtil.genUUID());
            context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
            context.setStartTime();
            return context;
        }

        private QueryStatement getStatement(String sqlString, ConnectContext context) throws AnalysisException {
            Preconditions.checkNotNull(sqlString);
            List<StatementBase> stmts = null;
            try {
                stmts = com.starrocks.sql.parser.SqlParser.parse(sqlString, context.getSessionVariable());
            } catch (ParsingException parsingException) {
                throw new AnalysisException(parsingException.getMessage());
            }
            Preconditions.checkState(stmts.size() == 1);
            QueryStatement parsedStmt = (QueryStatement) stmts.get(0);
            parsedStmt.setOrigStmt(new OriginStatement(sqlString, 0));

            return parsedStmt;
        }

        private ExecPlan plan(StatementBase stmt, ConnectContext context) throws Exception {
            ExecPlan execPlan = null;
            execPlan = StatementPlanner.plan(stmt, context);
            DataSink dataSink = new DictionaryCacheSink(this.beNodes, dictionary, txnId, canLeaveOutBeforeCommit());
            PlanFragment sinkFragment = execPlan.getFragments().get(0);
            sinkFragment.setSink(dataSink);

            return execPlan;
        }

        private void execute(ExecPlan execPlan, ConnectContext context) throws Exception {
            TUniqueId queryId = context.getExecutionId();

            List<PlanFragment> fragments = execPlan.getFragments();
            List<ScanNode> scanNodes = execPlan.getScanNodes();
            DescriptorTable descTable = execPlan.getDescTbl();
            Coordinator coord = getCoordinatorFactory().createRefreshDictionaryCacheScheduler(
                    context, queryId, descTable, fragments, scanNodes, execPlan);

            QeProcessorImpl.INSTANCE.registerQuery(queryId, coord);
            int leftTimeSecond = context.getExecTimeout();
            coord.setTimeoutSecond(leftTimeSecond);
            coord.exec();

            if (coord.join(leftTimeSecond)) {
                Status status = coord.getExecStatus();
                if (!status.ok()) {
                    error = true;
                    LOG.warn("execute dictionary cache sink failed " + status.getErrorMsg());
                    throw new StarRocksException(status.getErrorMsg());
                }
            } else {
                throw new StarRocksException("refresh dictionary cache timeout");
            }

            LOG.info("execute dictionary cache sink success, dictionary id: {}", dictionary.getDictionaryId());
        }

        private void refresh() throws Exception {
            if (error) {
                return;
            }

            // 1. context for plan
            ConnectContext context = buildConnectContext();
            try (var scope = context.bindScope()) {
                // 2. get statement througth sql string
                QueryStatement stmt = getStatement(dictionary.buildQuery(), context);

                // 3. get the exec plan with dictionary cache sink
                ExecPlan execPlan = plan(stmt, context);

                // 4. exec the query plan
                try {
                    execute(execPlan, context);
                } finally {
                    QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
                }
            }
        }

        protected void begin() {
            Preconditions.checkState(!error);
            PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
            request.dictId = dictionary.getDictionaryId();
            request.txnId = txnId;
            request.type = PProcessDictionaryCacheRequestType.BEGIN;

            processOnAllNodes(request, canLeaveOutBeforeCommit());
        }

        // With dictionary_ignore_failed_refresh, a failed refresh keeps the old version on every node.
        // A node left out before COMMIT would keep the old version while queries ask it for the new one.
        private boolean canLeaveOutBeforeCommit() {
            return leaveOutFailedNodes && !dictionary.getIgnoreFailedRefresh();
        }

        // With dictionary_refresh_leave_out_failed_nodes, one node that fails, for example a CN that restarts,
        // must not fail the refresh for all nodes. The failed nodes are left out of the rest of this refresh.
        // The refresh fails if no node is left, if the caller does not allow to leave nodes out, or if
        // CANCEL REFRESH stopped it. Without the switch, the first failed node fails the refresh.
        private void processOnAllNodes(PProcessDictionaryCacheRequest request, boolean allowLeaveOut) {
            if (!this.leaveOutFailedNodes) {
                Pair<Boolean, String> ret = DictionaryMgr.processDictionaryCacheInteranl(request, beNodes, null);
                error = ret.first;
                errMsg = ret.second;
                return;
            }
            List<TNetworkAddress> failedNodes = Lists.newArrayList();
            Pair<Boolean, String> ret =
                    DictionaryMgr.processDictionaryCacheInteranl(request, beNodes, null, failedNodes);
            if (!ret.first) {
                return;
            }
            if (!allowLeaveOut) {
                error = true;
                errMsg = ret.second;
                return;
            }
            leaveOutNodes(request.type, failedNodes, ret.second);
        }

        private void leaveOutNodes(PProcessDictionaryCacheRequestType step, List<TNetworkAddress> nodes,
                                   String reason) {
            if (nodes.size() >= beNodes.size() || isRefreshCancelled(dictionary.getDictionaryId())) {
                error = true;
                errMsg = reason;
                return;
            }
            LOG.warn("dictionary {} txn {}: {} failed on nodes {}, go on with the other nodes, first error: {}",
                    dictionary.getDictionaryName(), txnId, step, nodes, reason);
            beNodes.removeAll(nodes);
            // A left-out node has no copy of the new version, so the next scheduler pass refreshes it without
            // a wait. If an early refresh leaves it out again, the wait grows as before. If this refresh fails,
            // the dictionary is not FINISHED and no early refresh starts.
            if (!earlyRefresh) {
                clearNodeResyncWait(dictionary.getDictionaryId());
            }
        }

        // The data push goes on without a node that fails, so that node can hold part of the data.
        // Its COMMIT would then serve wrong content. Leave out every node with fewer rows than the others.
        protected void leaveOutNodesWithMissingRows() {
            if (error || !canLeaveOutBeforeCommit()) {
                return;
            }
            PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
            request.dictId = dictionary.getDictionaryId();
            request.txnId = txnId;
            request.type = PProcessDictionaryCacheRequestType.STATISTIC;

            List<TNetworkAddress> failedNodes = Lists.newArrayList();
            List<PProcessDictionaryCacheResult> results = Lists.newArrayList();
            Pair<Boolean, String> ret =
                    DictionaryMgr.processDictionaryCacheInteranl(request, beNodes, results, failedNodes);
            String reason = ret.first ? ret.second : "";

            // results holds one entry for each node that did not fail, in the order of beNodes.
            Map<TNetworkAddress, Long> rows = new HashMap<>();
            int next = 0;
            for (TNetworkAddress node : beNodes) {
                if (!failedNodes.contains(node)) {
                    rows.put(node, results.get(next++).txnRows);
                }
            }
            // An old BE does not report rows, so it cannot show that it got every chunk: the FE cannot tell which node
            // ran the sink, and a new one skips failed nodes. Always leave it out, so with only old nodes the refresh
            // fails instead of committing a partial copy. A node without the txn reports -1, so it is below the
            // others and is left out too.
            long maxRows = rows.values().stream().filter(r -> r != null).mapToLong(Long::longValue).max().orElse(0);
            for (Map.Entry<TNetworkAddress, Long> node : rows.entrySet()) {
                Long nodeRows = node.getValue();
                if (nodeRows != null && nodeRows >= maxRows) {
                    continue;
                }
                failedNodes.add(node.getKey());
                if (reason.isEmpty()) {
                    reason = nodeRows == null ? "node " + node.getKey() + " does not report its rows"
                            : "node " + node.getKey() + " has " + nodeRows + " rows of " + maxRows;
                }
            }
            if (!failedNodes.isEmpty()) {
                leaveOutNodes(request.type, failedNodes, reason);
            }
        }

        protected List<TNetworkAddress> getBeNodes() {
            return beNodes;
        }

        protected void commit() {
            if (error) {
                return;
            }
            UpdateDictionaryLog updateDictionaryLog =
                    new UpdateDictionaryLog(dictionary.getDictionaryId(), System.currentTimeMillis());
            updateDictionaryLog.setState(Dictionary.DictionaryState.COMMITTING);
            GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(
                    new UpdateDictionaryMgrLog(Lists.newArrayList(updateDictionaryLog)),
                    wal -> dictionary.setCommitting());

            PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
            request.dictId = dictionary.getDictionaryId();
            request.txnId = txnId;
            request.type = PProcessDictionaryCacheRequestType.COMMIT;

            // Nodes that committed already serve the new version, so the refresh cannot roll back from here.
            processOnAllNodes(request, true);
        }

        protected void finish() {
            long ts = System.currentTimeMillis();
            UpdateDictionaryLog updateDictionaryLog = new UpdateDictionaryLog(dictionary.getDictionaryId(), ts);
            WALApplier walApplier;
            if (!error) {
                updateDictionaryLog.setState(Dictionary.DictionaryState.FINISHED);
                updateDictionaryLog.setErrorMsg("");
                updateDictionaryLog.setLastSuccessVersion(txnId);
                walApplier = wal -> {
                    dictionary.setFinished(ts, txnId);
                    dictionary.setErrorMsg(""); // reset error msg
                };
            } else if ((dictionary.getIgnoreFailedRefresh() || earlyRefresh) &&
                    dictionary.getState() == Dictionary.DictionaryState.REFRESHING) {
                // Before COMMIT, every node still has the old version. An early refresh only repairs nodes,
                // so a failed one keeps the old version, as with dictionary_ignore_failed_refresh. CANCELLED
                // would fail every query on every node.
                updateDictionaryLog.setResetStateBeforeRefresh(true);
                updateDictionaryLog.setErrorMsg("Cancelled and rollback to previous state, errMsg: " + errMsg);
                walApplier = wal -> {
                    dictionary.resetStateBeforeRefresh();
                    dictionary.setErrorMsg(updateDictionaryLog.getErrorMsg());
                };
            } else {
                updateDictionaryLog.setState(Dictionary.DictionaryState.CANCELLED);
                updateDictionaryLog.setErrorMsg(errMsg);
                walApplier = wal -> {
                    dictionary.setCancelled();
                    dictionary.setErrorMsg(errMsg);
                };
            }

            GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(
                    new UpdateDictionaryMgrLog(Lists.newArrayList(updateDictionaryLog)), walApplier);
            if (!error) {
                recordSyncedNodes(dictionary.getDictionaryId(), beNodes, nodeStartTimes);
            }
            unresigerRunningAndUnfinised(dictionary.getDictionaryId());
        }

        @Override
        public void run() {
            // begin refresh dictionary cache txn
            begin();

            // refresh dictionary cache by executing query plan
            try {
                refresh();
            } catch (Exception e) {
                LOG.warn("refresh dictionary cache failed, ", e.getMessage());
                errMsg = e.getMessage();
                error = true;
            }

            leaveOutNodesWithMissingRows();

            // commit refresh dictionary cache txn
            commit();

            // finish 
            finish();
        }
    }
}
