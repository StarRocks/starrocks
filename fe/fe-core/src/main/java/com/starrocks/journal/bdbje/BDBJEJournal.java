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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.Util;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.OperationType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
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
    private static final int RETRY_TIME = 3;

    private String environmentPath = null;
    private String selfNodeName;
    private String selfNodeHostPort;

    private BDBEnvironment bdbEnvironment = null;
    private CloseSafeDatabase currentJournalDB;
    // the next journal's id. start from 1.
    private AtomicLong nextJournalId = new AtomicLong(1);

    public BDBJEJournal(String nodeName) {
        initBDBEnv(nodeName);
    }

    /*
     * Initialize bdb environment.
     * node name is ip_port (the port is edit_log_port)
     */
    private void initBDBEnv(String nodeName) {
        environmentPath = Catalog.getCurrentCatalog().getBdbDir();
        try {
            Pair<String, Integer> selfNode = Catalog.getCurrentCatalog().getSelfNode();
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
    public synchronized void rollJournal() {
        // Doesn't need to roll if current database contains no journals
        if (currentJournalDB.getDb().count() == 0) {
            return;
        }

        long newName = nextJournalId.get();
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
                continue;
            }
        }

        if (!writeSuccessed) {
            if (op == OperationType.OP_TIMESTAMP) {
                /*
                 * Do not exit if the write operation is OP_TIMESTAMP.
                 * If all the followers exit except master, master should continue provide query service.
                 * To prevent master exit, we should exempt OP_TIMESTAMP write
                 */
                nextJournalId.set(id);
                LOG.warn("master can not achieve quorum. write timestamp fail. but will not exit.");
                return;
            }
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
    public JournalCursor read(long fromKey, long toKey) {
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
            bdbEnvironment = new BDBEnvironment();
            Pair<String, Integer> helperNode = Catalog.getCurrentCatalog().getHelperNode();
            String helperHostPort = helperNode.first + ":" + helperNode.second;
            try {
                bdbEnvironment.setup(dbEnv, selfNodeName, selfNodeHostPort,
                        helperHostPort, Catalog.getCurrentCatalog().isElectable());
            } catch (Exception e) {
                LOG.error("catch an exception when setup bdb environment. will exit.", e);
                System.exit(-1);
            }
        }

        // Open a new journal database or get last existing one as current journal database
        Pair<String, Integer> helperNode = Catalog.getCurrentCatalog().getHelperNode();
        List<Long> dbNames = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                dbNames = bdbEnvironment.getDatabaseNames();

                if (dbNames == null) {
                    LOG.error("fail to get dbNames while open bdbje journal. will exit");
                    System.exit(-1);
                }
                if (dbNames.size() == 0) {
                    /*
                     *  This is the very first time to open. Usually, we will open a new database named "1".
                     *  But when we start cluster with an image file copied from other cluster,
                     *  here we should open database with name image max journal id + 1.
                     *  (default Catalog.getCurrentCatalog().getReplayedJournalId() is 0)
                     */
                    String dbName = Long.toString(Catalog.getCurrentCatalog().getReplayedJournalId() + 1);
                    LOG.info("the very first time to open bdb, dbname is {}", dbName);
                    currentJournalDB = bdbEnvironment.openDatabase(dbName);
                } else {
                    // get last database as current journal database
                    currentJournalDB = bdbEnvironment.openDatabase(dbNames.get(dbNames.size() - 1).toString());
                }

                // set next journal id
                nextJournalId.set(getMaxJournalId() + 1);

                break;
            } catch (InsufficientLogException insufficientLogEx) {
                // Copy the missing log files from a member of the replication group who owns the files
                LOG.warn("catch insufficient log exception. will recover and try again.", insufficientLogEx);
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setRetainLogFiles(false);
                restore.execute(insufficientLogEx, config);
                if (!bdbEnvironment.close()) {
                    LOG.error("close bdb environment failed, will exit");
                    // NOTE: System.exit will trigger BDBEnvironment.close(),
                    // because BDBEnvironment.close() has been registered in shutdown hook,
                    // so in this case BDBEnvironment.close() will be called twice.
                    // But it is ok.
                    System.exit(-1);
                }
                bdbEnvironment.setup(new File(environmentPath), selfNodeName, selfNodeHostPort,
                        helperNode.first + ":" + helperNode.second, Catalog.getCurrentCatalog().isElectable());
            }
        }
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


}