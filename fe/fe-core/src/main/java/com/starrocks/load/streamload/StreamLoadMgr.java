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
import com.starrocks.common.UserException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
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
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StreamLoadMgr implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(StreamLoadMgr.class);
    private static final int MEMORY_JOB_SAMPLES = 10;

    // label -> streamLoadTask
    private Map<String, StreamLoadTask> idToStreamLoadTask;

    // Only used for sync stream load
    // txnId -> streamLoadTask
    private Map<Long, StreamLoadTask> txnIdToSyncStreamLoadTasks;

    private Map<Long, Map<String, StreamLoadTask>> dbToLabelToStreamLoadTask;
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

    public void beginLoadTask(String dbName, String tableName, String label, String user, String clientIp, long timeoutMillis,
                              int channelNum, int channelId, TransactionResult resp) throws UserException {
        beginLoadTask(dbName, tableName, label, user, clientIp, timeoutMillis, channelNum, channelId, resp,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public void beginLoadTask(String dbName, String tableName, String label, String user, String clientIp, long timeoutMillis,
                              int channelNum, int channelId, TransactionResult resp, long warehouseId) throws UserException {
        StreamLoadTask task = null;
        Database db = checkDbName(dbName);
        long dbId = db.getId();
        // if task is already created, return directly
        readLock();
        try {
            task = idToStreamLoadTask.get(label);
            if (task != null) {
                task.beginTxn(channelId, channelNum, resp);
                return;
            }
        } finally {
            readUnlock();
        }

        boolean createTask = true;

        writeLock();
        try {
            // double check here
            task = idToStreamLoadTask.get(label);
            if (task != null) {
                task.beginTxn(channelId, channelNum, resp);
                return;
            }
            task = createLoadTask(db, tableName, label, user, clientIp, timeoutMillis, channelNum, channelId, warehouseId);
            LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, task.getId())
                    .add("msg", "create load task").build());
            addLoadTask(task);
            task.beginTxn(channelId, channelNum, resp);
            createTask = true;
        } finally {
            writeUnlock();
        }
        if (createTask) {
            GlobalStateMgr.getCurrentState().getEditLog().logCreateStreamLoadJob(task);
        }
    }

    // for sync stream load task
    public void beginLoadTask(String dbName, String tableName, String label, String user, String clientIp, long timeoutMillis,
                              TransactionResult resp, boolean isRoutineLoad, long warehouseId) throws UserException {
        StreamLoadTask task = null;
        Database db = checkDbName(dbName);
        long dbId = db.getId();

        writeLock();
        try {
            task = createLoadTask(db, tableName, label, user, clientIp, timeoutMillis, isRoutineLoad, warehouseId);
            LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, task.getId())
                    .add("msg", "create load task").build());

            task.beginTxn(0, 1, resp);
            addLoadTask(task);
        } finally {
            writeUnlock();
        }
    }

    // for sync stream load
    public StreamLoadTask createLoadTask(Database db, String tableName, String label, String user, String clientIp,
                                         long timeoutMillis, boolean isRoutineLoad, long warehouseId)
            throws UserException {
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            unprotectedCheckMeta(db, tableName);
            table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // init stream load task
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask streamLoadTask = new StreamLoadTask(id, db, (OlapTable) table,
                label, user, clientIp, timeoutMillis, System.currentTimeMillis(), isRoutineLoad, warehouseId);
        return streamLoadTask;
    }

    public StreamLoadTask createLoadTaskWithoutLock(Database db, String tableName, String label, String user, String clientIp,
                                         long timeoutMillis, boolean isRoutineLoad, long warehouseId)
            throws UserException {
        // init stream load task
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask streamLoadTask = new StreamLoadTask(id, db,
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName),
                label, user, clientIp, timeoutMillis, System.currentTimeMillis(), isRoutineLoad, warehouseId);
        return streamLoadTask;
    }

    public StreamLoadTask createLoadTask(Database db, String tableName, String label, String user, String clientIp,
                                         long timeoutMillis, int channelNum,
                                         int channelId, long warehouseId) throws UserException {
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            unprotectedCheckMeta(db, tableName);
            table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // init stream load task
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask streamLoadTask = new StreamLoadTask(id, db, (OlapTable) table,
                label, user, clientIp, timeoutMillis, channelNum, channelId, System.currentTimeMillis(), warehouseId);
        return streamLoadTask;
    }

    public void unprotectedCheckMeta(Database db, String tblName)
            throws UserException {
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
    }

    public void replayCreateLoadTask(StreamLoadTask loadJob) {
        addLoadTask(loadJob);
        LOG.info(new LogBuilder(LogKey.STREAM_LOAD_TASK, loadJob.getId())
                .add("msg", "replay create load job")
                .build());
    }

    public Database checkDbName(String dbName) throws UserException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.warn("Database {} does not exist", dbName);
            throw new UserException("Database[" + dbName + "] does not exist");
        }
        return db;
    }

    // add load tasks and also add callback factory
    public void addLoadTask(StreamLoadTask task) {
        if (task.isSyncStreamLoad()) {
            txnIdToSyncStreamLoadTasks.put(task.getTxnId(), task);
        }

        // Clear the stream load tasks manually
        if (idToStreamLoadTask.size() > Config.stream_load_task_keep_max_num) {
            // If enable_load_profile = true,
            // most stream load tasks are generated through flink-cdc and routine load generally,
            // so clearing the syncStreamLoadTask is preferred.
            LOG.info("trigger cleanSyncStreamLoadTasks when add load task label:{}", task.getLabel());
            cleanSyncStreamLoadTasks();
            // The size of idToStreamLoadTask is still huge, indicates that the type of most tasks is PARALLEL,
            // so clean all the streamLoadTasks manaully not waitting for Config.stream_load_task_keep_max_second.
            if (idToStreamLoadTask.size() > Config.stream_load_task_keep_max_num / 2) {
                LOG.info("trigger cleanOldStreamLoadTasks when add load task label{}", task.getLabel());
                cleanOldStreamLoadTasks(true);
            }
        }

        long dbId = task.getDBId();
        String label = task.getLabel();
        Map<String, StreamLoadTask> labelToStreamLoadTask = null;
        if (dbToLabelToStreamLoadTask.containsKey(dbId)) {
            labelToStreamLoadTask = dbToLabelToStreamLoadTask.get(dbId);
        } else {
            labelToStreamLoadTask = Maps.newConcurrentMap();
            dbToLabelToStreamLoadTask.put(dbId, labelToStreamLoadTask);
        }
        labelToStreamLoadTask.put(label, task);
        idToStreamLoadTask.put(label, task);

        // add callback before txn created, because callback will be performed on replay without txn begin
        // register txn state listener
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().addCallback(task);
    }

    public TNetworkAddress executeLoadTask(String label, int channelId, HttpHeaders headers,
                                           TransactionResult resp, String dbName, String tableName)
            throws UserException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new UserException("stream load task " + label + " does not exist");
            }
            StreamLoadTask task = idToStreamLoadTask.get(label);

            // check whether the database and table are consistent with the transaction,
            // for single database and single table are supported so far
            if (!task.getDBName().equals(dbName)) {
                throw new UserException(
                        String.format("Request table %s not equal transaction table %s", dbName, task.getDBName()));
            }
            if (!task.getTableName().equals(tableName)) {
                throw new UserException(
                        String.format("Request table %s not equal transaction table %s", tableName, task.getTableName()));
            }

            readUnlock();
            needUnLock = false;
            TNetworkAddress redirectAddress = task.tryLoad(channelId, resp);
            if (redirectAddress != null || !resp.stateOK() || resp.containMsg()) {
                return redirectAddress;
            }
            return task.executeTask(channelId, headers, resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void prepareLoadTask(String label, int channelId, HttpHeaders headers, TransactionResult resp)
            throws UserException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new UserException("stream load task " + label + " does not exist");
            }
            StreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            task.prepareChannel(channelId, headers, resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void tryPrepareLoadTaskTxn(String label, TransactionResult resp)
            throws UserException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new UserException("stream load task " + label + " does not exist");
            }
            StreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            if (task.checkNeedPrepareTxn()) {
                task.waitCoordFinishAndPrepareTxn(resp);
            }
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void commitLoadTask(String label, TransactionResult resp)
            throws UserException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new UserException("stream load task " + label + " does not exist");
            }
            StreamLoadTask task = idToStreamLoadTask.get(label);
            readUnlock();
            needUnLock = false;
            task.commitTxn(resp);
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
    }

    public void rollbackLoadTask(String label, TransactionResult resp)
            throws UserException {
        boolean needUnLock = true;
        readLock();
        try {
            if (!idToStreamLoadTask.containsKey(label)) {
                throw new UserException("stream load task" + label + "does not exist");
            }
            StreamLoadTask task = idToStreamLoadTask.get(label);
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
            Iterator<Map.Entry<String, StreamLoadTask>> iterator = idToStreamLoadTask.entrySet().iterator();
            long currentMs = System.currentTimeMillis();
            while (iterator.hasNext()) {
                StreamLoadTask streamLoadTask = iterator.next().getValue();
                if (streamLoadTask.checkNeedRemove(currentMs, isForce)) {
                    unprotectedRemoveTaskFromDb(streamLoadTask);
                    iterator.remove();
                    if (streamLoadTask.isSyncStreamLoad()) {
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
            Iterator<Map.Entry<String, StreamLoadTask>> iterator = idToStreamLoadTask.entrySet().iterator();
            long currentMs = System.currentTimeMillis();
            while (iterator.hasNext()) {
                StreamLoadTask streamLoadTask = iterator.next().getValue();
                if (streamLoadTask.isSyncStreamLoad() && streamLoadTask.isFinalState()) {
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

    private void unprotectedRemoveTaskFromDb(StreamLoadTask streamLoadTask) {
        long dbId = streamLoadTask.getDBId();
        String label = streamLoadTask.getLabel();

        if (dbToLabelToStreamLoadTask.containsKey(dbId)) {
            dbToLabelToStreamLoadTask.get(dbId).remove(label);
            if (dbToLabelToStreamLoadTask.get(dbId).isEmpty()) {
                dbToLabelToStreamLoadTask.remove(dbId);
            }
        }
    }

    /*
      if dbFullName is null, result = all of stream load task in all of db
      else if label is null, result =  all of stream load task in dbFullName

      if includeHistory is false, filter not running load task in result
      else return all of result
     */
    public List<StreamLoadTask> getTask(String dbFullName, String label, boolean includeHistory)
            throws MetaNotFoundException {
        readLock();
        try {
            // return all of stream load task
            List<StreamLoadTask> result;
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

    public StreamLoadTask getSyncSteamLoadTaskByTxnId(long txnId) {
        return txnIdToSyncStreamLoadTasks.getOrDefault(txnId, null);
    }

    // put history task in the end
    private void sortStreamLoadTask(List<StreamLoadTask> streamLoadTaskList) {
        if (streamLoadTaskList == null) {
            return;
        }
        Collections.sort(streamLoadTaskList, new Comparator<StreamLoadTask>() {
            @Override
            public int compare(StreamLoadTask t1, StreamLoadTask t2) {
                return (int) (t1.createTimeMs() - t2.createTimeMs());
            }
        });
    }

    // for each label, we can have only one task
    public StreamLoadTask getTaskByLabel(String label) {
        return idToStreamLoadTask.get(label);
    }

    public StreamLoadTask getTaskById(long id) {
        readLock();
        try {
            List<StreamLoadTask> taskList =
                    idToStreamLoadTask.values().stream().filter(streamLoadTask -> id == streamLoadTask.getId())
                            .collect(Collectors.toList());
            return taskList.isEmpty() ? null : taskList.get(0);
        } finally {
            readUnlock();
        }
    }

    // return all of stream load task named label in all of db
    // return all tasks if label is null
    public List<StreamLoadTask> getTaskByName(String label) {
        List<StreamLoadTask> result = Lists.newArrayList();
        readLock();
        try {
            if (label != null) {
                StreamLoadTask task = idToStreamLoadTask.get(label);
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
        for (StreamLoadTask streamLoadTask : idToStreamLoadTask.values()) {
            if (!streamLoadTask.isDurableLoadState()) {
                streamLoadTask.cancelAfterRestart();
            }
        }
    }

    public synchronized long saveStreamLoadManager(DataOutputStream out, long checksum) throws IOException {
        List<StreamLoadTask> loadTasks = idToStreamLoadTask.values().stream().collect(Collectors.toList());

        out.writeInt(loadTasks.size());
        for (StreamLoadTask loadTask : loadTasks) {
            loadTask.write(out);
        }
        checksum ^= getChecksum();
        return checksum;
    }

    public synchronized long getChecksum() {
        return (long) idToStreamLoadTask.size() + (long) dbToLabelToStreamLoadTask.size();
    }

    public long getStreamLoadTaskCount() {
        return idToStreamLoadTask.size();
    }

    // for ut
    public Map<String, StreamLoadTask> getIdToStreamLoadTask() {
        return idToStreamLoadTask;
    }

    public static StreamLoadMgr loadStreamLoadManager(DataInput in) throws IOException {
        int size = in.readInt();
        long currentMs = System.currentTimeMillis();
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        streamLoadManager.init();
        for (int i = 0; i < size; i++) {
            StreamLoadTask loadTask = StreamLoadTask.read(in);
            loadTask.init();
            // discard expired task right away
            if (loadTask.checkNeedRemove(currentMs, false)) {
                LOG.info("discard expired task: {}", loadTask.getLabel());
                continue;
            }

            streamLoadManager.addLoadTask(loadTask);
        }
        return streamLoadManager;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 1 + idToStreamLoadTask.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.STREAM_LOAD_MGR, numJson);
        writer.writeInt(idToStreamLoadTask.size());
        for (StreamLoadTask streamLoadTask : idToStreamLoadTask.values()) {
            writer.writeJson(streamLoadTask);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        long currentMs = System.currentTimeMillis();
        int numJson = reader.readInt();
        for (int i = 0; i < numJson; ++i) {
            StreamLoadTask loadTask = reader.readJson(StreamLoadTask.class);
            loadTask.init();
            // discard expired task right away
            if (loadTask.checkNeedRemove(currentMs, false)) {
                LOG.info("discard expired task: {}", loadTask.getLabel());
                continue;
            }

            addLoadTask(loadTask);
        }
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
}