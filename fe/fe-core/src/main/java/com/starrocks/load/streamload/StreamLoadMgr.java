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

package com.starrocks.load.streamload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.WarehouseLoadInfoBuilder;
import com.starrocks.warehouse.WarehouseLoadStatusInfo;
import com.starrocks.warehouse.cngroup.ComputeResource;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StreamLoadMgr implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(StreamLoadMgr.class);
    private static final int MEMORY_JOB_SAMPLES = 10;

    // label -> AbstractStreamLoadTask (unified management)
    private Map<String, AbstractStreamLoadTask> idToStreamLoadTask;

    // Only used for sync stream load
    // txnId -> StreamLoadTask (only StreamLoadTask can be sync)
    private Map<Long, StreamLoadTask> txnIdToSyncStreamLoadTasks;

    private Map<Long, Map<String, AbstractStreamLoadTask>> dbToLabelToStreamLoadTask;

    protected final WarehouseLoadInfoBuilder warehouseLoadStatusInfoBuilder =
            new WarehouseLoadInfoBuilder();

    private ReentrantReadWriteLock lock;

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public StreamLoadMgr() {
        init();
    }

    public void init() {
        LOG.debug("begin to init stream load manager");
        idToStreamLoadTask = Maps.newConcurrentMap();
        txnIdToSyncStreamLoadTasks = Maps.newConcurrentMap();
        dbToLabelToStreamLoadTask = Maps.newConcurrentMap();
        lock = new ReentrantReadWriteLock(true);
    }

    public void beginMultiStatementLoadTask(String dbName, String label, String user,
                                            String clientIp, long timeoutMillis,
                                            TransactionResult resp, ComputeResource computeResource) throws StarRocksException {
        AbstractStreamLoadTask task = null;
        Database db = checkDbName(dbName);
        long dbId = db.getId();

        readLock();
        try {
            task = idToStreamLoadTask.get(label);
            if (task != null) {
                task.beginTxnFromFrontend(resp);
                return;
            }
        } finally {
            readUnlock();
        }

        boolean createTask = false;
        writeLock();
        try {
            task = idToStreamLoadTask.get(label);
            if (task != null) {
                task.beginTxnFromFrontend(resp);
                return;
            }
            task = createMultiStatementLoadTask(db, label, user, clientIp, timeoutMillis, computeResource);
            addLoadTask(task);
            LOG.info("create multi statment task {}", task);
            task.beginTxnFromFrontend(resp);
            createTask = true;
        } finally {
            writeUnlock();
        }

        if (createTask) {
            GlobalStateMgr.getCurrentState().getEditLog().logCreateMultiStmtStreamLoadJob((StreamLoadMultiStmtTask) task);
            LOG.info("create multi statement task success");
        }
    }

    public void prepareMultiStatementLoadTask(String label, String tableName, HttpHeaders headers, TransactionResult resp)
            throws StarRocksException {
    }

    public void beginLoadTaskFromFrontend(String dbName, String tableName, String label, String user,
                                          String clientIp, long timeoutMillis, int channelNum,
                                          int channelId, TransactionResult resp) throws StarRocksException {
        beginLoadTaskFromFrontend(dbName, tableName, label, user, clientIp, timeoutMillis, channelNum, channelId, resp,
                WarehouseManager.DEFAULT_RESOURCE);
    }

    public void beginLoadTaskFromFrontend(String dbName, String tableName, String label, String user,
                                          String clientIp, long timeoutMillis, int channelNum, int channelId,
                                          TransactionResult resp, ComputeResource computeResource) throws StarRocksException {
        AbstractStreamLoadTask task = null;
        Database db = checkDbName(dbName);
        long dbId = db.getId();
        // if task is already created, return directly
        readLock();
        try {
            task = idToStreamLoadTask.get(label);
            if (task != null) {
                task.beginTxnFromFrontend(channelId, channelNum, resp);
                return;
            }
        } finally {
            readUnlock();
        }
        Table table = checkMeta(db, tableName);

        boolean createTask = true;

        writeLock();
        try {
            // double check here
            task = idToStreamLoadTask.get(label);
            if (task != null) {
                task.beginTxnFromFrontend(channelId, channelNum, resp);
                return;
            }
            task = createLoadTaskWithoutLock(db, table, label, user, clientIp, timeoutMillis, channelNum, channelId,
                    computeResource);
            LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, task.getId())
                    .add("msg", "create load task").build());
            addLoadTask(task);
            task.beginTxnFromFrontend(channelId, channelNum, resp);
            createTask = true;
        } finally {
            writeUnlock();
        }
        if (createTask) {
            GlobalStateMgr.getCurrentState().getEditLog().logCreateStreamLoadJob((StreamLoadTask) task);
        }
    }

    // for sync stream load task
    public void beginLoadTaskFromBackend(String dbName, String tableName, String label, TUniqueId requestId,
                                         String user, String clientIp, long timeoutMillis,
                                         TransactionResult resp, boolean isRoutineLoad,
                                         ComputeResource computeResource, long backendId)
            throws StarRocksException {
        AbstractStreamLoadTask task = null;
        Database db = checkDbName(dbName);
        long dbId = db.getId();
        Table table = checkMeta(db, tableName);

        writeLock();
        try {
            task = createLoadTaskWithoutLock(db, table, label, user, clientIp, timeoutMillis, isRoutineLoad,
                    computeResource);
            LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, task.getId())
                    .add("msg", "create load task").build());

            task.beginTxnFromBackend(requestId, clientIp, backendId, resp);
            addLoadTask(task);
        } finally {
            writeUnlock();
        }
    }

    public StreamLoadMultiStmtTask createMultiStatementLoadTask(Database db, String label, String user, String clientIp,
                                                                long timeoutMillis, ComputeResource computeResource) {
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadMultiStmtTask streamLoadTask = new StreamLoadMultiStmtTask(id, db, label, user, clientIp,
                timeoutMillis, System.currentTimeMillis(), computeResource);
        return streamLoadTask;
    }

    public StreamLoadTask createLoadTaskWithoutLock(Database db, Table table, String label, String user, String clientIp,
                                                    long timeoutMillis, boolean isRoutineLoad, ComputeResource computeResource) {
        // init stream load task
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask streamLoadTask = new StreamLoadTask(id, db, (OlapTable) table,
                label, user, clientIp, timeoutMillis, System.currentTimeMillis(), isRoutineLoad, computeResource);
        return streamLoadTask;
    }

    private StreamLoadTask createLoadTaskWithoutLock(Database db, Table table, String label, String user,
                                                     String clientIp, long timeoutMillis, int channelNum,
                                                     int channelId, ComputeResource computeResource) {
        // init stream load task
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask streamLoadTask = new StreamLoadTask(id, db, (OlapTable) table,
                label, user, clientIp, timeoutMillis, channelNum, channelId, System.currentTimeMillis(), computeResource);
        return streamLoadTask;
    }

    private Table checkMeta(Database db, String tblName) throws StarRocksException {
        if (tblName == null) {
            throw new AnalysisException("Table name must be specified when calling /begin/transaction/ first time");
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tblName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
        }

        if (table instanceof MaterializedView) {
            throw new AnalysisException(String.format(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tblName, tblName));
        }

        if (!table.isOlapOrCloudNativeTable()) {
            throw new AnalysisException("Only olap/lake table support stream load");
        }
        return table;
    }

    public void replayCreateLoadTask(AbstractStreamLoadTask loadJob) {
        addLoadTask(loadJob);
        LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, loadJob.getId())
                .add("msg", "replay create load job")
                .build());
    }

    private Database checkDbName(String dbName) throws StarRocksException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.warn("Database {} does not exist", dbName);
            throw new StarRocksException("Database[" + dbName + "] does not exist");
        }
        return db;
    }

    // add load tasks and also add callback factory
    public void addLoadTask(AbstractStreamLoadTask task) {
        if (task instanceof StreamLoadTask && ((StreamLoadTask) task).isSyncStreamLoad()) {
            txnIdToSyncStreamLoadTasks.put(task.getTxnId(), (StreamLoadTask) task);
        }

        // Clear the stream load tasks manually
        if (idToStreamLoadTask.size() > Config.stream_load_task_keep_max_num) {
            LOG.info("trigger cleanSyncStreamLoadTasks when add load task label:{}", task.getLabel());
            cleanSyncStreamLoadTasks();
            if (idToStreamLoadTask.size() > Config.stream_load_task_keep_max_num / 2) {
                LOG.info("trigger cleanOldStreamLoadTasks when add load task label{}", task.getLabel());
                cleanOldStreamLoadTasks(true);
            }
        }

        long dbId = task.getDBId();
        String label = task.getLabel();
        Map<String, AbstractStreamLoadTask> labelToStreamLoadTask = null;
        if (dbToLabelToStreamLoadTask.containsKey(dbId)) {
            labelToStreamLoadTask = dbToLabelToStreamLoadTask.get(dbId);
        } else {
            labelToStreamLoadTask = Maps.newConcurrentMap();
            dbToLabelToStreamLoadTask.put(dbId, labelToStreamLoadTask);
        }
        labelToStreamLoadTask.put(label, task);
        idToStreamLoadTask.put(label, task);

        // register txn state listener
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().addCallback(task);
    }

    public TNetworkAddress executeLoadTask(String label, int channelId, HttpHeaders headers,
                                           TransactionResult resp, String dbName, String tableName)
            throws StarRocksException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new StarRocksException("stream load task " + label + " does not exist");
            }
            AbstractStreamLoadTask task = idToStreamLoadTask.get(label);

            // check whether the database is consistent with the transaction
            if (!task.getDBName().equals(dbName)) {
                throw new StarRocksException(
                        String.format("Request database %s not equal transaction database %s", dbName, task.getDBName()));
            }

            readUnlock();
            needUnLock = false;
            TNetworkAddress redirectAddress = task.tryLoad(channelId, tableName, resp);
            if (redirectAddress != null || !resp.stateOK() || resp.containMsg()) {
                return redirectAddress;
            }
            return task.executeTask(channelId, tableName, headers, resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void prepareLoadTask(String label, String tableName, int channelId, HttpHeaders headers, TransactionResult resp)
            throws StarRocksException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new StarRocksException("stream load task " + label + " does not exist");
            }
            AbstractStreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            task.prepareChannel(channelId, tableName, headers, resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void tryPrepareLoadTaskTxn(String label, long preparedTimeoutMs, TransactionResult resp)
            throws StarRocksException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new StarRocksException("stream load task " + label + " does not exist");
            }
            AbstractStreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            if (task.checkNeedPrepareTxn()) {
                task.waitCoordFinishAndPrepareTxn(preparedTimeoutMs, resp);
            }
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void commitLoadTask(String label, HttpHeaders headers, TransactionResult resp)
            throws StarRocksException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new StarRocksException("stream load task " + label + " does not exist");
            }
            AbstractStreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            task.commitTxn(headers, resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void rollbackLoadTask(String label, TransactionResult resp)
            throws StarRocksException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new StarRocksException("stream load task" + label + "does not exist");
            }
            AbstractStreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            task.manualCancelTask(resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    // Remove old stream load tasks from idToStreamLoadTask and dbToLabelToStreamLoadTask
    // This function is called periodically.
    // Cancelled and Committed task will be removed after Config.stream_load_task_keep_max_second seconds
    public void cleanOldStreamLoadTasks(boolean isForce) {
        LOG.debug("begin to clean old stream load tasks");
        writeLock();
        try {
            Iterator<Map.Entry<String, AbstractStreamLoadTask>> iterator = idToStreamLoadTask.entrySet().iterator();
            long currentMs = System.currentTimeMillis();
            while (iterator.hasNext()) {
                AbstractStreamLoadTask streamLoadTask = iterator.next().getValue();
                if (streamLoadTask.checkNeedRemove(currentMs, isForce)) {
                    unprotectedRemoveTaskFromDb(streamLoadTask);
                    iterator.remove();
                    if (streamLoadTask instanceof StreamLoadTask && ((StreamLoadTask) streamLoadTask).isSyncStreamLoad()) {
                        txnIdToSyncStreamLoadTasks.remove(streamLoadTask.getTxnId());
                    }
                    LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, streamLoadTask.getId())
                            .add("label", streamLoadTask.getLabel())
                            .add("end_timestamp", streamLoadTask.endTimeMs())
                            .add("current_timestamp", currentMs)
                            .add("task_state", streamLoadTask.getStateName())
                            .add("msg", "old task has been cleaned")
                    );
                }
            }
        } finally {
            writeUnlock();
        }
    }

    // There maybe many streamLoadTasks in memory when enable_load_profile = true,
    // StreamLoadTask which type is SyncStreamLoad should be clean up firstly
    public void cleanSyncStreamLoadTasks() {
        writeLock();
        try {
            Iterator<Map.Entry<String, AbstractStreamLoadTask>> iterator = idToStreamLoadTask.entrySet().iterator();
            long currentMs = System.currentTimeMillis();
            while (iterator.hasNext()) {
                AbstractStreamLoadTask streamLoadTask = iterator.next().getValue();
                if (streamLoadTask instanceof StreamLoadTask && ((StreamLoadTask) streamLoadTask).isSyncStreamLoad()
                        && streamLoadTask.isFinalState()) {
                    unprotectedRemoveTaskFromDb(streamLoadTask);
                    iterator.remove();
                    txnIdToSyncStreamLoadTasks.remove(streamLoadTask.getTxnId());
                    LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, streamLoadTask.getId())
                            .add("label", streamLoadTask.getLabel())
                            .add("end_timestamp", streamLoadTask.endTimeMs())
                            .add("current_timestamp", currentMs)
                            .add("task_state", streamLoadTask.getStateName())
                            .add("msg", "old task has been cleaned")
                    );
                }
            }

        } finally {
            writeUnlock();
        }
    }

    private void unprotectedRemoveTaskFromDb(AbstractStreamLoadTask streamLoadTask) {
        long dbId = streamLoadTask.getDBId();
        String label = streamLoadTask.getLabel();

        if (dbToLabelToStreamLoadTask.containsKey(dbId)) {
            dbToLabelToStreamLoadTask.get(dbId).remove(label);
            if (dbToLabelToStreamLoadTask.get(dbId).isEmpty()) {
                dbToLabelToStreamLoadTask.remove(dbId);
            }
        }

        if (streamLoadTask instanceof StreamLoadTask) {
            warehouseLoadStatusInfoBuilder.withRemovedJob((StreamLoadTask) streamLoadTask);
        }
    }

    /*
      if dbFullName is null, result = all of stream load task in all of db
      else if label is null, result =  all of stream load task in dbFullName

      if includeHistory is false, filter not running load task in result
      else return all of result
     */
    public List<AbstractStreamLoadTask> getTask(String dbFullName, String label, boolean includeHistory)
            throws MetaNotFoundException {
        readLock();
        try {
            // return all of stream load task
            List<AbstractStreamLoadTask> result;
            RESULT:
            {
                if (dbFullName == null) {
                    result = new ArrayList<>(idToStreamLoadTask.values());
                    sortStreamLoadTask(result);
                    break RESULT;
                }

                long dbId = 0L;
                Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbFullName);
                if (database == null) {
                    throw new MetaNotFoundException("failed to find database by dbFullName " + dbFullName);
                }
                dbId = database.getId();
                if (!dbToLabelToStreamLoadTask.containsKey(dbId)) {
                    result = new ArrayList<>();
                    break RESULT;
                }
                if (label == null) {
                    result = Lists.newArrayList(dbToLabelToStreamLoadTask.get(dbId).values());
                    sortStreamLoadTask(result);
                    break RESULT;
                }
                if (dbToLabelToStreamLoadTask.get(dbId).containsKey(label)) {
                    result = new ArrayList<>();
                    result.add(dbToLabelToStreamLoadTask.get(dbId).get(label));
                    break RESULT;
                }
                return null;
            }

            if (!includeHistory) {
                result = result.stream().filter(entity -> !entity.isFinalState())
                        .collect(Collectors.toList());
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public Map<Long, WarehouseLoadStatusInfo> getWarehouseLoadInfo() {
        readLock();
        try {
            List<StreamLoadTask> streamLoadTasks = idToStreamLoadTask.values().stream()
                    .filter(task -> task instanceof StreamLoadTask)
                    .map(task -> (StreamLoadTask) task)
                    .collect(Collectors.toList());
            return warehouseLoadStatusInfoBuilder.buildFromJobs(streamLoadTasks);
        } finally {
            readUnlock();
        }
    }

    public StreamLoadTask getSyncSteamLoadTaskByTxnId(long txnId) {
        return txnIdToSyncStreamLoadTasks.getOrDefault(txnId, null);
    }

    // put history task in the end
    private void sortStreamLoadTask(List<AbstractStreamLoadTask> streamLoadTaskList) {
        if (streamLoadTaskList == null) {
            return;
        }
        Collections.sort(streamLoadTaskList, new Comparator<AbstractStreamLoadTask>() {
            @Override
            public int compare(AbstractStreamLoadTask t1, AbstractStreamLoadTask t2) {
                return (int) (t1.createTimeMs() - t2.createTimeMs());
            }
        });
    }

    // for each label, we can have only one task
    public AbstractStreamLoadTask getTaskByLabel(String label) {
        return idToStreamLoadTask.get(label);
    }

    public StreamLoadTask getTaskById(long id) {
        readLock();
        try {
            List<AbstractStreamLoadTask> taskList =
                    idToStreamLoadTask.values().stream().filter(streamLoadTask -> id == streamLoadTask.getId())
                            .collect(Collectors.toList());
            return taskList.isEmpty() ? null : (StreamLoadTask) taskList.get(0);
        } finally {
            readUnlock();
        }
    }

    // return all of stream load task named label in all of db
    // return all tasks if label is null
    public List<AbstractStreamLoadTask> getTaskByName(String label) {
        List<AbstractStreamLoadTask> result = Lists.newArrayList();
        readLock();
        try {
            if (label != null) {
                AbstractStreamLoadTask task = idToStreamLoadTask.get(label);
                if (task != null) {
                    result.add(task);
                }
            } else {
                // return all stream load tasks
                result.addAll(idToStreamLoadTask.values());
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public void cancelUnDurableTaskAfterRestart() {
        for (AbstractStreamLoadTask streamLoadTask : idToStreamLoadTask.values()) {
            if (!streamLoadTask.isDurableLoadState()) {
                streamLoadTask.cancelAfterRestart();
            }
        }
    }

    public synchronized long getChecksum() {
        return (long) idToStreamLoadTask.size() + (long) dbToLabelToStreamLoadTask.size();
    }

    public long getStreamLoadTaskCount() {
        return idToStreamLoadTask.size();
    }

    public int numOfStreamLoadTask() {
        int count = 0;
        for (AbstractStreamLoadTask streamLoadTask : idToStreamLoadTask.values()) {
            if (streamLoadTask instanceof StreamLoadTask) {
                ++count;
            }
        }
        return count;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 2 + idToStreamLoadTask.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.STREAM_LOAD_MGR, numJson);
        writer.writeInt(numOfStreamLoadTask());
        for (AbstractStreamLoadTask streamLoadTask : idToStreamLoadTask.values()) {
            if (streamLoadTask instanceof StreamLoadTask) {
                writer.writeJson(streamLoadTask);
            }
        }
        writer.writeInt(idToStreamLoadTask.size() - numOfStreamLoadTask());
        for (AbstractStreamLoadTask streamLoadTask : idToStreamLoadTask.values()) {
            if (!(streamLoadTask instanceof StreamLoadTask)) {
                writer.writeJson(streamLoadTask);
            }
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        long currentMs = System.currentTimeMillis();
        reader.readCollection(StreamLoadTask.class, loadTask -> {
            loadTask.init();
            // discard expired task right away
            if (loadTask.checkNeedRemove(currentMs, false)) {
                LOG.info("discard expired task: {}", loadTask.getLabel());
                return;
            }

            addLoadTask(loadTask);
        });
        reader.readCollection(StreamLoadMultiStmtTask.class, loadTask -> {
            loadTask.init();
            // discard expired task right away
            if (loadTask.checkNeedRemove(currentMs, false)) {
                LOG.info("discard expired task: {}", loadTask.getLabel());
                return;
            }

            addLoadTask(loadTask);
        });
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("StreamLoad", (long) idToStreamLoadTask.size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> samples = idToStreamLoadTask.values()
                .stream()
                .limit(MEMORY_JOB_SAMPLES)
                .collect(Collectors.toList());
        return Lists.newArrayList(Pair.create(samples, (long) idToStreamLoadTask.size()));
    }

    public long getLatestFinishTime() {
        long latestTime = -1L;
        readLock();
        try {
            for (AbstractStreamLoadTask task : idToStreamLoadTask.values()) {
                if (task instanceof StreamLoadTask && ((StreamLoadTask) task).isFinal()) {
                    latestTime = Math.max(latestTime, ((StreamLoadTask) task).getFinishTimestampMs());
                }
            }
        } finally {
            readUnlock();
        }
        return latestTime;
    }

    public Map<Long, Long> getRunningTaskCount() {
        readLock();
        try {
            Map<Long, Long> result = new HashMap<>();
            for (AbstractStreamLoadTask task : idToStreamLoadTask.values()) {
                if (!task.isFinalState()) {
                    result.compute(task.getCurrentWarehouseId(), (key, value) -> value == null ? 1L : value + 1);
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }
}
