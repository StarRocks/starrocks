// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
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

public class StreamLoadManager {
    private static final Logger LOG = LogManager.getLogger(StreamLoadManager.class);

    private Map<String, StreamLoadTask> idToStreamLoadTask;
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

    public StreamLoadManager() {
        init();
    }

    public void init() {
        LOG.debug("begin to init stream load manager");
        idToStreamLoadTask = Maps.newConcurrentMap();
        dbToLabelToStreamLoadTask = Maps.newConcurrentMap();
        lock = new ReentrantReadWriteLock(true);
    }

    public void beginLoadTask(String dbName, String tableName, String label, long timeoutMillis,
                              int channelNum, int channelId, TransactionResult resp) throws UserException {
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
            task = createLoadTask(db, tableName, label, timeoutMillis, channelNum, channelId);
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

    public StreamLoadTask createLoadTask(Database db, String tableName, String label, long timeoutMillis,
                                         int channelNum, int channelId) throws UserException {
        Table table;
        db.readLock();
        try {
            unprotectedCheckMeta(db, tableName);
            table = db.getTable(tableName);
        } finally {
            db.readUnlock();
        }

        // init stream load task
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask streamLoadTask = new StreamLoadTask(id, db, (OlapTable) table,
                label, timeoutMillis, channelNum, channelId, System.currentTimeMillis());
        return streamLoadTask;
    }

    public void unprotectedCheckMeta(Database db, String tblName)
            throws UserException {
        Table table = db.getTable(tblName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
        }

        if (table instanceof MaterializedView) {
            throw new AnalysisException(String.format(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tblName, tblName));
        }

        if (!table.isOlapOrLakeTable()) {
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            LOG.warn("Database {} does not exist", dbName);
            throw new UserException("Database[" + dbName + "] does not exist");
        }
        return db;
    }

    // add load tasks and also add to to callback factory
    private void addLoadTask(StreamLoadTask task) {
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
        GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(task);
    }

    public TNetworkAddress executeLoadTask(String label, int channelId, HttpHeaders headers, TransactionResult resp)
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
    // Cancelled and Committed task will be remove after Configure.label_keep_max_second seconds
    public void cleanOldStreamLoadTasks() {
        LOG.debug("begin to clean old stream load tasks");
        writeLock();
        try {
            Iterator<Map.Entry<String, StreamLoadTask>> iterator = idToStreamLoadTask.entrySet().iterator();
            long currentMs = System.currentTimeMillis();
            while (iterator.hasNext()) {
                StreamLoadTask streamLoadTask = iterator.next().getValue();
                if (streamLoadTask.checkNeedRemove(currentMs)) {
                    unprotectedRemoveTaskFromDb(streamLoadTask);
                    iterator.remove();
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

        dbToLabelToStreamLoadTask.get(dbId).remove(label);
        if (dbToLabelToStreamLoadTask.get(dbId).isEmpty()) {
            dbToLabelToStreamLoadTask.remove(dbId);
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
                Database database = GlobalStateMgr.getCurrentState().getDb(dbFullName);
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

    // return all of stream load task named label in all of db
    public List<StreamLoadTask> getTaskByName(String label) {
        List<StreamLoadTask> result = Lists.newArrayList();
        readLock();
        try {
            for (Map<String, StreamLoadTask> labelToStreamLoadTask : dbToLabelToStreamLoadTask.values()) {
                if (labelToStreamLoadTask.containsKey(label)) {
                    result.add(labelToStreamLoadTask.get(label));
                }
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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/load/streamload/StreamLoadManager.java
    public static StreamLoadManager loadStreamLoadManager(DataInput in) throws IOException {
=======
    public long getStreamLoadTaskCount() {
        return idToStreamLoadTask.size();
    }

    // for ut
    public Map<String, StreamLoadTask> getIdToStreamLoadTask() {
        return idToStreamLoadTask;
    }

    public static StreamLoadMgr loadStreamLoadManager(DataInput in) throws IOException {
>>>>>>> 7299e5c95c ([Enhancement] Add FE memory related metrics (#28184)):fe/fe-core/src/main/java/com/starrocks/load/streamload/StreamLoadMgr.java
        int size = in.readInt();
        long currentMs = System.currentTimeMillis();
        StreamLoadManager streamLoadManager = new StreamLoadManager();
        streamLoadManager.init();
        for (int i = 0; i < size; i++) {
            StreamLoadTask loadTask = StreamLoadTask.read(in);
            loadTask.init();
            // discard expired task right away
            if (loadTask.checkNeedRemove(currentMs)) {
                LOG.info("discard expired task: {}", loadTask.getLabel());
                continue;
            }

            streamLoadManager.addLoadTask(loadTask);
        }
        return streamLoadManager;
    }
}