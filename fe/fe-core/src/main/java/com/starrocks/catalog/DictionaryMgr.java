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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.authz.authorization.PrivilegeBuiltinConstants;
import com.starrocks.common.concurrent.ThreadPoolManager;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.exception.MetaNotFoundException;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.structure.Status;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.DictionaryMgrInfo;
import com.starrocks.persist.DropDictionaryInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DictionaryCacheSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheRequestType;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private long nextTxnId = 1L;
    @SerializedName(value = "nextDictionaryId")
    private long nextDictionaryId = 1L;

    private Set<Long> unfinishedRefreshTasks = Sets.newHashSet();
    private final Set<Long> runningRefreshTasks = Sets.newHashSet();

    // use last successful txn id as next readable version
    private ConcurrentHashMap<Long, Long> dictionaryIdTolastSuccessVersion = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();

    private final ExecutorService executor =
            ThreadPoolManager.newDaemonFixedThreadPool(
                Config.refresh_dictionary_cache_thread_num, Integer.MAX_VALUE, "refresh-dictionary-cache-pool", true);

    public DictionaryMgr() {}

    // single thread execution
    public void scheduleTasks() {
        lock.lock();
        try {
            for (Map.Entry<Long, Dictionary> entry : dictionariesMapById.entrySet()) {
                long id = entry.getKey();
                Dictionary dictionary = dictionariesMapById.get(id);
                // regular schedule
                if (dictionary.getNextSchedulableTime() <= System.currentTimeMillis() &&
                        !unfinishedRefreshTasks.contains(id)) {
                    unfinishedRefreshTasks.add(id);
                    dictionary.setRefreshing();
                    dictionary.updateNextSchedulableTime(dictionary.getRefreshInterval());
                }
            }
            for (Long dictionaryId : unfinishedRefreshTasks) {
                // new added task
                if (!runningRefreshTasks.contains(dictionaryId)) {
                    resigerUnfinishedToRunningUnlocked(dictionaryId);

                    RefreshDictionaryCacheWorker task =
                            new RefreshDictionaryCacheWorker(dictionariesMapById.get(dictionaryId), getAndIncrementTxnId());
                    try {
                        submit(task);
                    } catch (RejectedExecutionException e) {
                        runningRefreshTasks.remove(dictionaryId); // re-schedule later
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static boolean processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request, String errMsg,
                                                         List<TNetworkAddress> beNodes,
                                                         List<PProcessDictionaryCacheResult> results) {
        for (TNetworkAddress address : beNodes) {
            PProcessDictionaryCacheResult result = null;
            try {
                Future<PProcessDictionaryCacheResult> future =
                        BackendServiceClient.getInstance().processDictionaryCache(address, request);
                result = future.get();
            } catch (Exception e) {
                LOG.warn(" processDictionaryCache failed in: " + address + " rpc error :" + e.getMessage());
                if (errMsg != null) {
                    errMsg = e.getMessage();
                }
                return true;
            }

            TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
            if (code != TStatusCode.OK) {
                LOG.warn(" processDictionaryCache failed in: " + address + " err msg " + result.status.errorMsgs);
                if (errMsg != null) {
                    errMsg = result.status.errorMsgs.size() == 0 ? "" : result.status.errorMsgs.get(0);
                }
                return true;
            }

            if (results != null) {
                results.add(result);
            }
        }
        LOG.info("finish processDictionaryCache dictionary id: {}, request type: {}",
                 request.dictId, request.txnId, request.type);
        return false;
    }

    public void createDictionary(CreateDictionaryStmt stmt, String dbName) throws DdlException {
        Dictionary dictionary = new Dictionary(getAndIncrementDictionaryId(), stmt.getDictionaryName(),
                                               stmt.getQueryableObject(), dbName, stmt.getDictionaryKeys(),
                                               stmt.getDictionaryValues(), stmt.getProperties());
        dictionary.buildDictionaryProperties();
        GlobalStateMgr.getCurrentState().getEditLog().logCreateDictionary(dictionary);
        addDictionary(dictionary);

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

    public void dropDictionary(String dictionaryName, boolean isCacheOnly, boolean isReplay) throws MetaNotFoundException {
        if (!isReplay && !isCacheOnly) {
            DropDictionaryInfo info = new DropDictionaryInfo(dictionaryName);
            GlobalStateMgr.getCurrentState().getEditLog().logDropDictionary(info);
        }
        Dictionary dictionary = null;
        lock.lock();
        try {
            dictionary = getDictionaryByName(dictionaryName);
            if (dictionary == null) {
                throw new MetaNotFoundException("refreshed dictionary not found");
            }
            
            if (!isCacheOnly) {
                dictionariesMapById.remove(dictionary.getDictionaryId());
                dictionariesIdMapByName.remove(dictionary.getDictionaryName());
                unfinishedRefreshTasks.remove(dictionary.getDictionaryId());
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

    public void refreshDictionary(String dictionaryName) throws MetaNotFoundException {
        lock.lock();
        try {
            Dictionary dictionary = getDictionaryByName(dictionaryName);
            if (dictionary == null) {
                throw new MetaNotFoundException("refreshed dictionary not found");
            }
            unfinishedRefreshTasks.add(dictionary.getDictionaryId());
            dictionary.setRefreshing();
            dictionary.updateNextSchedulableTime(dictionary.getRefreshInterval());
        } finally {
            lock.unlock();
        }
    }

    public void cancelRefreshDictionary(String dictionaryName) {
        lock.lock();
        try {
            Dictionary dictionary = getDictionaryByName(dictionaryName);
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
        final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentSystemInfo();
        List<Backend> backends = currentSystemInfo.getBackends();
        for (Backend backend : backends) {
            beNodes.add(backend.getBrpcAddress());
        }

        DictionaryMgr.processDictionaryCacheInteranl(request, null, beNodes, null);
    }

    public void addDictionary(Dictionary dictionary) {
        lock.lock();
        try {
            dictionariesMapById.put(dictionary.getDictionaryId(), dictionary);
            dictionariesIdMapByName.put(dictionary.getDictionaryName(), dictionary.getDictionaryId());
            // init for every dictionary
            dictionaryIdTolastSuccessVersion.put(dictionary.getDictionaryId(), 0L);
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

    // single thread execution
    public long getAndIncrementTxnId() {
        long curTxnId = nextTxnId;
        logModify(this.nextTxnId + 1, this.nextDictionaryId);
        ++nextTxnId;
        return curTxnId;
    }

    public synchronized void updateLastSuccessTxnId(long dictionaryId, long txnId) {
        dictionaryIdTolastSuccessVersion.put(dictionaryId, txnId);
    }

    private synchronized long getAndIncrementDictionaryId() {
        long curDictionaryId = nextDictionaryId;
        logModify(this.nextTxnId, nextDictionaryId + 1);
        ++nextDictionaryId;
        return curDictionaryId;
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
        return dictionaryIdTolastSuccessVersion.get(dictionaryId);
    }

    public ConcurrentHashMap<Long, Long> getDictionaryIdTolastSuccessVersion() {
        return dictionaryIdTolastSuccessVersion;
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

    public Map<TNetworkAddress, PProcessDictionaryCacheResult> getDictionaryStatistic(Dictionary dictionary) {
        PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
        request.dictId = dictionary.getDictionaryId();
        request.type = PProcessDictionaryCacheRequestType.STATISTIC;

        List<TNetworkAddress> beNodes = Lists.newArrayList();
        final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentSystemInfo();
        List<Backend> backends = currentSystemInfo.getBackends();
        for (Backend backend : backends) {
            beNodes.add(backend.getBrpcAddress());
        }

        List<PProcessDictionaryCacheResult> results = Lists.newArrayList();
        DictionaryMgr.processDictionaryCacheInteranl(request, null, beNodes, results);
        Map<TNetworkAddress, PProcessDictionaryCacheResult> resultMap = new HashMap<>();
        if (results.size() < beNodes.size()) {
            return resultMap;
        }
        Preconditions.checkState(results.size() == beNodes.size());

        for (int i = 0; i < results.size(); i++) {
            resultMap.put(beNodes.get(i), results.get(i));
        }
        return resultMap;
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

                Map<TNetworkAddress, PProcessDictionaryCacheResult> resultMap = getDictionaryStatistic(dictionary);
                
                String memoryUsage = "";
                for (Map.Entry<TNetworkAddress, PProcessDictionaryCacheResult> result : resultMap.entrySet()) {
                    TNetworkAddress address = result.getKey();
                    memoryUsage += address.getHostname() + ":" + String.valueOf(address.getPort()) + " : ";

                    if (result.getValue() != null) {
                        memoryUsage += String.valueOf(result.getValue().dictionaryMemoryUsage) + "\n";
                    } else {
                        memoryUsage += "Can not get Memory info" + "\n";
                    }
                }
                allInfo.get(allInfo.size() - 1).add(memoryUsage.substring(0, memoryUsage.length() - 1));
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

    public void replayDropDictionary(String dictionaryName) {
        try {
            dropDictionary(dictionaryName, false, true);
        } catch (MetaNotFoundException e) {
            /* nothing to do */
        }
    }

    public void replayModifyDictionaryMgr(DictionaryMgrInfo info) {
        long newNextTxnId = info.getNextTxnId();
        long newNextDictionaryId = info.getNextDictionaryId();
        
        if (newNextTxnId > this.nextTxnId) {
            this.nextTxnId = newNextTxnId;
        }

        if (newNextDictionaryId > this.nextDictionaryId) {
            this.nextDictionaryId = newNextDictionaryId;
        }
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.DICTIONARY_MGR, 1);
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
        this.dictionaryIdTolastSuccessVersion = data.getDictionaryIdTolastSuccessVersion();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        lock.lock();
        try {
            for (Map.Entry<Long, Dictionary> entry : dictionariesMapById.entrySet()) {
                // init for every dictionary
                dictionaryIdTolastSuccessVersion.put(entry.getKey(), 0L);
            }
        } finally {
            lock.unlock(); 
        }
    }

    // This function is used to log the modification for some extra meta data for
    // dictionaryMgr.
    private void logModify(long nextTxnId, long nextDictionaryId) {
        DictionaryMgrInfo info = new DictionaryMgrInfo(nextTxnId, nextDictionaryId);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyDictionaryMgr(info);
    }

    public class RefreshDictionaryCacheWorker implements Runnable {
        private Dictionary dictionary;
        private long txnId;
        private List<TNetworkAddress> beNodes = Lists.newArrayList();
        private boolean error;
        private String errMsg;

        public RefreshDictionaryCacheWorker(Dictionary dictionary, long txnId) {
            this.dictionary = dictionary;
            this.txnId = txnId;
            this.error = false;
            this.errMsg = "";
            initializeBeNodesAddress();
        }

        private void initializeBeNodesAddress() {
            final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentSystemInfo();
            List<Backend> backends = currentSystemInfo.getBackends();
            for (Backend backend : backends) {
                this.beNodes.add(backend.getBrpcAddress());
            }
        }

        private ConnectContext buildConnectContext() {
            ConnectContext context = new ConnectContext();
            context.setDatabase(dictionary.getDbName());
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.setQualifiedUser(UserIdentity.ROOT.getUser());
            context.getSessionVariable().setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            context.getSessionVariable().setEnablePipelineEngine(true);
            context.getSessionVariable().setPipelineDop(0);
            context.getSessionVariable().setEnableProfile(false);
            context.getSessionVariable().setParallelExecInstanceNum(1);
            context.getSessionVariable().setQueryTimeoutS(3600); // 1h
            context.setQueryId(UUIDUtil.genUUID());
            context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
            context.setStartTime();
            context.setThreadLocalInfo();
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
            DataSink dataSink = new DictionaryCacheSink(this.beNodes, dictionary, txnId);
            PlanFragment sinkFragment = execPlan.getFragments().get(0);
            sinkFragment.setSink(dataSink);

            return execPlan;
        }

        private void execute(ExecPlan execPlan, ConnectContext context) throws Exception {
            TUniqueId queryId = context.getExecutionId();

            List<PlanFragment> fragments = execPlan.getFragments();
            List<ScanNode> scanNodes = execPlan.getScanNodes();
            DescriptorTable descTable = execPlan.getDescTbl();
            Coordinator coord = getCoordinatorFactory().createRefreshDictionaryCacheScheduler(context, queryId, descTable,
                                                                                              fragments, scanNodes);

            QeProcessorImpl.INSTANCE.registerQuery(queryId, coord);
            int leftTimeSecond = context.getSessionVariable().getQueryTimeoutS();
            coord.setTimeoutSecond(leftTimeSecond);
            coord.exec();

            if (coord.join(leftTimeSecond)) {
                Status status = coord.getExecStatus();
                if (!status.ok()) {
                    error = true;
                    LOG.warn("execute dictionary cache sink failed " + status.getErrorMsg());
                    throw new UserException(status.getErrorMsg());
                }
            } else {
                throw new UserException("refresh dictionary cache timeout");
            }

            LOG.info("execute dictionary cache sink success, dictionary id: {}", dictionary.getDictionaryId());
        }

        private void refresh() throws Exception {
            if (error) {
                return;
            }

            // 1. context for plan
            ConnectContext context = buildConnectContext();

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

        private void begin() {
            Preconditions.checkState(!error);
            PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
            request.dictId = dictionary.getDictionaryId();
            request.txnId = txnId;
            request.type = PProcessDictionaryCacheRequestType.BEGIN;

            error = DictionaryMgr.processDictionaryCacheInteranl(request, errMsg, beNodes, null);
        }

        private void commit() {
            if (error) {
                return;
            }
            dictionary.setCommitting();

            PProcessDictionaryCacheRequest request = new PProcessDictionaryCacheRequest();
            request.dictId = dictionary.getDictionaryId();
            request.txnId = txnId;
            request.type = PProcessDictionaryCacheRequestType.COMMIT;

            error = DictionaryMgr.processDictionaryCacheInteranl(request, errMsg, beNodes, null);
        }

        private void finish(long dictionaryId) {
            GlobalStateMgr.getCurrentState().getDictionaryMgr().unresigerRunningAndUnfinised(dictionaryId);
            if (!error) {
                GlobalStateMgr.getCurrentState().getDictionaryMgr().updateLastSuccessTxnId(dictionaryId, txnId);
                dictionary.setFinished();
                dictionary.setErrorMsg(""); // reset error msg
            } else if (dictionary.getIgnoreFailedRefresh() && dictionary.getState() == Dictionary.DictionaryState.REFRESHING) {
                dictionary.resetStateBeforeRefresh();
                dictionary.setErrorMsg("Cancelled and rollback to previous state, errMsg: " +
                                       errMsg);
            } else {
                dictionary.setCancelled();
                dictionary.setErrorMsg(errMsg);
            }
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

            // commit refresh dictionary cache txn
            commit();

            // finish 
            finish(dictionary.getDictionaryId());
        }
    }
}
