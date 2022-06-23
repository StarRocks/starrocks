// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/journal/bdbje/BDBJEJournal.java

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

package com.starrocks.journal.bdbje;

import com.google.common.annotations.VisibleForTesting;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.InsufficientLogException;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.Util;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/*
 * This is the bdb implementation of Journal interface.
 * First, we open() this journal, then read from or write to the bdb environment
 * We can also get journal id information by calling get***Id functions.
 * Finally, close this journal.
 * This class encapsulates the read, write APIs of bdbje
 */
public class BDBJEJournal implements Journal {
    public static final Logger LOG = LogManager.getLogger(BDBJEJournal.class);
    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;
    static int RETRY_TIME = 3;
    static int SLEEP_INTERVAL_SEC = 5;

    private String environmentPath = null;
    private String selfNodeName;
    private String selfNodeHostPort;

    private BDBEnvironment bdbEnvironment = null;
    private CloseSafeDatabase currentJournalDB;
    protected Transaction currentTrasaction = null;

    // store uncommitted kv, used for rebuilding txn on commit fails
    private List<Pair<DatabaseEntry, DatabaseEntry>> uncommitedDatas = new ArrayList<>();

    // only kept for profile test, will remove in the next PR
    @VisibleForTesting
    private AtomicLong nextJournalId = new AtomicLong(1);

    @VisibleForTesting
    public BDBJEJournal(BDBEnvironment bdbEnvironment, CloseSafeDatabase currentJournalDB) {
        this.bdbEnvironment = bdbEnvironment;
        this.currentJournalDB = currentJournalDB;
    }

    public BDBJEJournal(String nodeName) {
        initBDBEnv(nodeName);
    }

    /*
     * Initialize bdb environment.
     * node name is ip_port (the port is edit_log_port)
     */
    private void initBDBEnv(String nodeName) {
        environmentPath = GlobalStateMgr.getCurrentState().getBdbDir();
        try {
            Pair<String, Integer> selfNode = GlobalStateMgr.getCurrentState().getSelfNode();
            if (NetUtils.isPortUsing(selfNode.first, selfNode.second)) {
                LOG.error("edit_log_port {} is already in use. will exit.", selfNode.second);
                System.exit(-1);
            }
            selfNodeName = nodeName;
            selfNodeHostPort = selfNode.first + ":" + selfNode.second;
        } catch (IOException e) {
            LOG.error(e);
            System.exit(-1);
        }
    }

    /*
     * Database is named by its minimum journal id.
     * For example:
     * One database contains journal 100 to journal 200, its name is 100.
     * The next database's name is 201
     */
    @Override
    public synchronized void rollJournal(long newName) {
        // Doesn't need to roll if current database contains no journals
        if (currentJournalDB.getDb().count() == 0) {
            return;
        }

        String currentDbName = currentJournalDB.getDb().getDatabaseName();
        long currentName = Long.parseLong(currentDbName);
        long newNameVerify = currentName + currentJournalDB.getDb().count();
        if (newName == newNameVerify) {
            LOG.info("roll edit log. new db name is {}", newName);
            currentJournalDB = bdbEnvironment.openDatabase(Long.toString(newName));
        } else {
            String msg = String.format("roll journal error! journalId and db journal numbers is not match. "
                            + "journal id: %d, current db: %s, expected db count: %d",
                    newName, currentDbName, newNameVerify);
            LOG.error(msg);
            Util.stdoutWithTime(msg);
            System.exit(-1);
        }
    }

    /**
     * TODO remove this method in later PR
     * Now we only kept it for profile test
     */
    @VisibleForTesting
    @Override
    public synchronized void write(short op, Writable writable) {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(op);
        entity.setData(writable);

        // id is the key
        long id = nextJournalId.getAndIncrement();
        Long idLong = id;
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
        idBinding.objectToEntry(idLong, theKey);

        // entity is the value
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        try {
            entity.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        DatabaseEntry theData = new DatabaseEntry(buffer.getData());
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_SIZE_BYTES.increase((long) theData.getSize());
        }
        LOG.debug("opCode = {}, journal size = {}", op, theData.getSize());
        // Write the key value pair to bdb.
        boolean writeSuccessed = false;
        try {
            for (int i = 0; i < RETRY_TIME; i++) {
                try {
                    // Parameter null means auto commit
                    if (currentJournalDB.put(null, theKey, theData) == OperationStatus.SUCCESS) {
                        writeSuccessed = true;
                        LOG.debug("master write journal {} finished. db name {}, current time {}",
                                id, currentJournalDB.getDb().getDatabaseName(), System.currentTimeMillis());
                        break;
                    }
                } catch (DatabaseException e) {
                    LOG.error("catch an exception when writing to database. sleep and retry. journal id {}", id, e);
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        } finally {
            // If write failed, set nextJournalId to the origin value.
            if (!writeSuccessed) {
                nextJournalId.set(id);
            }
        }

        if (!writeSuccessed) {
            String msg = "write bdb failed. will exit. journalId: " + id + ", bdb database Name: " +
                    currentJournalDB.getDb().getDatabaseName();
            LOG.error(msg);
            Util.stdoutWithTime(msg);
            System.exit(-1);
        }
    }

    @Deprecated
    @Override
    public JournalEntity read(long journalId) {
        throw new RuntimeException("function not implemented");
    }

    @Override
    public JournalCursor read(long fromKey, long toKey) throws JournalException {
        return BDBJournalCursor.getJournalCursor(bdbEnvironment, fromKey, toKey);
    }

    @Override
    public long getMaxJournalId() {
        long ret = -1;
        if (bdbEnvironment == null) {
            return ret;
        }
        List<Long> dbNames = bdbEnvironment.getDatabaseNames();
        if (dbNames == null) {
            return ret;
        }
        if (dbNames.size() == 0) {
            return ret;
        }

        int index = dbNames.size() - 1;
        String dbName = dbNames.get(index).toString();
        long dbNumberName = dbNames.get(index);
        Database database = bdbEnvironment.openDatabase(dbName).getDb();
        ret = dbNumberName + database.count() - 1;

        return ret;
    }

    @Override
    public long getMinJournalId() {
        long ret = -1;
        if (bdbEnvironment == null) {
            return ret;
        }
        List<Long> dbNames = bdbEnvironment.getDatabaseNames();
        if (dbNames == null) {
            return ret;
        }
        if (dbNames.size() == 0) {
            return ret;
        }

        String dbName = dbNames.get(0).toString();
        Database database = bdbEnvironment.openDatabase(dbName).getDb();
        // The database is empty
        if (database.count() == 0) {
            return ret;
        }

        return dbNames.get(0);
    }

    @Override
    public void close() {
        bdbEnvironment.close();
        bdbEnvironment = null;
    }

    /*
     * open the bdbje environment, and get the current journal database
     */
    @Override
    public synchronized void open() {
        if (bdbEnvironment == null) {
            File dbEnv = new File(environmentPath);
            Pair<String, Integer> helperNode = GlobalStateMgr.getCurrentState().getHelperNode();
            String helperHostPort = helperNode.first + ":" + helperNode.second;
            bdbEnvironment = new BDBEnvironment(dbEnv, selfNodeName, selfNodeHostPort,
                    helperHostPort, GlobalStateMgr.getCurrentState().isElectable());
            try {
                bdbEnvironment.setup();
            } catch (Exception e) {
                LOG.error("catch an exception when setup bdb environment. will exit.", e);
                System.exit(-1);
            }
        }

        // Open a new journal database or get last existing one as current journal database
        List<Long> dbNames = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                // sleep for retry
                if (i > 0) {
                    Thread.sleep(3000L);
                }

                dbNames = bdbEnvironment.getDatabaseNames();
                if (dbNames == null) {
                    LOG.error("fail to get dbNames while open bdbje journal. will exit");
                    System.exit(-1);
                }

                String dbName = null;
                if (dbNames.size() == 0) {
                    /*
                     *  This is the very first time to open. Usually, we will open a new database named "1".
                     *  But when we start cluster with an image file copied from other cluster,
                     *  here we should open database with name image max journal id + 1.
                     *  (default Catalog.getCurrentCatalog().getReplayedJournalId() is 0)
                     */
                    dbName = Long.toString(GlobalStateMgr.getCurrentState().getReplayedJournalId() + 1);
                    LOG.info("the very first time to open bdb, dbname is {}", dbName);
                } else {
                    // get last database as current journal database
                    dbName = dbNames.get(dbNames.size() - 1).toString();
                }

                currentJournalDB = bdbEnvironment.openDatabase(dbName);
                if (currentJournalDB == null) {
                    LOG.warn("fail to open database {}. retried {} times", dbName, i);
                    continue;
                }
                return;
            } catch (InsufficientLogException insufficientLogEx) {
                LOG.warn("catch insufficient log exception. please restart", insufficientLogEx);
                // for InsufficientLogException we should refresh the log and
                // then exit the process because we may have read dirty data.
                bdbEnvironment.refreshLog(insufficientLogEx);
                System.exit(-1);
            } catch (Throwable t) {
                LOG.warn("catch exception, retried: {} ", i, t);
            }
        }

        // TODO: covert all System.exit() in BDBJE to a proper exception
        //       This is really horribly ugly.
        LOG.error("Fail to open() after {} times!", RETRY_TIME);
        System.exit(-1);
    }

    @Override
    public void deleteJournals(long deleteToJournalId) {
        List<Long> dbNames = bdbEnvironment.getDatabaseNames();
        if (dbNames == null) {
            LOG.info("delete database names is null.");
            return;
        }

        String msg = "existing database names: ";
        for (long name : dbNames) {
            msg += name + " ";
        }
        msg += ", deleteToJournalId is " + deleteToJournalId;
        LOG.info(msg);

        for (int i = 1; i < dbNames.size(); i++) {
            if (deleteToJournalId >= dbNames.get(i)) {
                long name = dbNames.get(i - 1);
                String stringName = Long.toString(name);
                LOG.info("delete database name {}", stringName);
                bdbEnvironment.removeDatabase(stringName);
            } else {
                LOG.info("database name {} is larger than deleteToJournalId {}, not delete",
                        dbNames.get(i), deleteToJournalId);
                break;
            }
        }
    }

    @Override
    public long getFinalizedJournalId() {
        List<Long> dbNames = bdbEnvironment.getDatabaseNames();
        if (dbNames == null) {
            LOG.error("database name is null.");
            return 0;
        }

        String msg = "database names: ";
        for (long name : dbNames) {
            msg += name + " ";
        }
        LOG.info(msg);

        if (dbNames.size() < 2) {
            return 0;
        }

        return dbNames.get(dbNames.size() - 1) - 1;
    }

    @Override
    public List<Long> getDatabaseNames() {
        if (bdbEnvironment == null) {
            return null;
        }

        return bdbEnvironment.getDatabaseNames();
    }

    public BDBEnvironment getBdbEnvironment() {
        return bdbEnvironment;
    }

    /**
     * start batch write
     * for BDB: start transaction.
     */
    @Override
    public void batchWriteBegin() throws InterruptedException, JournalException {
        if (currentTrasaction != null) {
            throw new JournalException(String.format(
                    "failed to begin batch write because has running txn = %s", currentTrasaction));
        }

        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                // sleep before retry
                if (i != 0) {
                    Thread.sleep(SLEEP_INTERVAL_SEC * 1000);
                }

                currentTrasaction = currentJournalDB.getDb().getEnvironment().beginTransaction(
                        null, bdbEnvironment.getTxnConfig());
                return;
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to begin txn after retried %d times! db = %s",
                        i + 1, currentJournalDB);
                LOG.error(errMsg, e);
                exception = new JournalException(errMsg);
                exception.initCause(e);
            }
        }
        // failed after retried
        throw exception;
    }

    /**
     * append buffer to current batch
     * for bdb: write to transaction, no commit
     */
    @Override
    public void batchWriteAppend(long journalId, DataOutputBuffer buffer) throws InterruptedException, JournalException {
        if (currentTrasaction == null) {
            throw new JournalException("failed to append because no running txn!");
        }
        // id is the key
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
        idBinding.objectToEntry(journalId, theKey);
        // entity is the value
        DatabaseEntry theData = new DatabaseEntry(buffer.getData(), 0, buffer.getLength());

        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                // sleep before retry
                if (i != 0) {
                    Thread.sleep(SLEEP_INTERVAL_SEC * 1000);
                }

                OperationStatus status = currentJournalDB.put(currentTrasaction, theKey, theData);
                if (status != OperationStatus.SUCCESS) {
                    throw new JournalException(String.format(
                            "failed to append journal after retried %d times! status[%s] db[%s] key[%s] data[%s]",
                            i + 1, status, currentJournalDB, theKey, theData));
                }
                // success
                uncommitedDatas.add(Pair.create(theKey, theData));
                return;
            } catch (DatabaseException e) {
                String errMsg = String.format(
                        "failed to append journal after retried %d times! key[%s] value[%s] txn[%s] db[%s]",
                        i + 1, theKey, theData, currentTrasaction, currentJournalDB);
                LOG.error(errMsg, e);
                exception = new JournalException(errMsg);
                exception.initCause(e);
            } catch (JournalException e) {
                LOG.error("failed to write journal", e);
                exception = e;
            }
        }
        // failed after retried
        throw exception;
    }

    /**
     * persist current batch
     * for bdb: commit current transaction
     * notice that if commit fail, the transaction may not be valid.
     * we should rebuild the transaction and retry.
     */
    @Override
    public void batchWriteCommit() throws InterruptedException, JournalException {
        if (currentTrasaction == null) {
            throw new JournalException("failed to commit because no running txn!");
        }

        JournalException exception = null;
        try {
            for (int i = 0; i < RETRY_TIME; i++) {
                // retry cleanups
                if (i != 0) {
                    Thread.sleep(SLEEP_INTERVAL_SEC * 1000);

                    if (currentTrasaction == null || !currentTrasaction.isValid()) {
                        try {
                            rebuildCurrentTransaction();
                        } catch (JournalException e) {
                            // failed to rebuild txn, will continune to next attempt
                            LOG.warn("failed to commit journal after retried {} times! failed to rebuild txn",
                                    i + 1, e);
                            currentTrasaction = null;
                            exception = e;
                            continue;
                        }
                    }
                } // if i != 0

                // commit
                try {
                    currentTrasaction.commit();
                    return;
                } catch (DatabaseException e) {
                    String errMsg = String.format("failed to commit journal after retried %d times! txn[%s] db[%s]",
                            i + 1, currentTrasaction, currentJournalDB);
                    LOG.error(errMsg, e);
                    exception = new JournalException(errMsg);
                    exception.initCause(e);
                }
            }
            // failed after retried
            throw exception;
        } finally {
            // always reset current txn
            currentTrasaction = null;
            uncommitedDatas.clear();
        }
    }

    /**
     * txn can be invalid if commit fails on exception
     * in this case, we rebuild the current transaction with `uncommitedDatas`
     * there's no need to retry while we were rebuilding since we have retried outside this function
     */
    private void rebuildCurrentTransaction() throws JournalException {
        LOG.warn("transaction is invalid, rebuild the txn with {} kvs", uncommitedDatas.size());

        try {
            //  begin transaction
            currentTrasaction = currentJournalDB.getDb().getEnvironment().beginTransaction(
                    null, bdbEnvironment.getTxnConfig());
            // append
            for (Pair<DatabaseEntry, DatabaseEntry> kvPair : uncommitedDatas) {
                DatabaseEntry theKey = kvPair.first;
                DatabaseEntry theData = kvPair.second;
                OperationStatus status = currentJournalDB.put(currentTrasaction, theKey, theData);
                if (status != OperationStatus.SUCCESS) {
                    String msg = String.format(
                            "failed to append journal! status[%s] db[%s] key[%s] data[%s]",
                            status, currentJournalDB, theKey, theData);
                    LOG.warn(msg);
                    throw new JournalException(msg);
                }
            }
            LOG.info("rebuild txn succeed. new txn {}", currentTrasaction);
        } catch (DatabaseException e) {
            String errMsg = String.format("failed to rebuild txn! txn[%s] db[%s]", currentTrasaction, currentJournalDB);
            LOG.error(errMsg, e);
            JournalException exception = new JournalException(errMsg);
            exception.initCause(e);
            throw exception;
        }
    }

    /**
     * abort current transaction
     * for bdb: abort current transaction.
     */
    @Override
    public void batchWriteAbort() throws InterruptedException, JournalException {
        if (currentTrasaction == null) {
            LOG.warn("failed to abort transaction because no running transaction, will just ignore and return.");
            return;
        }
        try {
            currentTrasaction.abort();
        } catch (DatabaseException e) {
            JournalException exception = new JournalException(String.format(
                    "failed to abort batch write! txn[%s] db[%s]", currentTrasaction, currentJournalDB));
            exception.initCause(e);
            throw exception;
        } finally {
            currentTrasaction = null;
        }
    }

}