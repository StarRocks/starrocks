// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal.bdbje;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.NetUtils;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalException;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BDBJEJournalTest {
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalTest.class);

    private File tempDir;
    private int restoredCheckpointIntervalSecond;

    @Before
    public void init() throws Exception{
        BDBJEJournal.RETRY_TIME = 3;
        BDBJEJournal.SLEEP_INTERVAL_SEC = 0;
        // avoid checkpoint
        restoredCheckpointIntervalSecond = FeConstants.checkpoint_interval_second;
        FeConstants.checkpoint_interval_second = 24 * 60 * 60;
        Path rootDir = Paths.get(System.getenv().getOrDefault("BDB_PUT_PROFILE_TEST_BDB_DIR_ROOT", "."));
        tempDir = Files.createTempDirectory(rootDir, "BDBJEJournalTest").toFile();
    }

    @After
    public void cleanup() throws Exception {
        FileUtils.deleteDirectory(tempDir);
        FeConstants.checkpoint_interval_second = restoredCheckpointIntervalSecond;
    }

    private BDBEnvironment initBDBEnv(String name) throws Exception {
        // try to find a port that is not bind
        String selfNodeHostPort = null;
        for (int port = 9000; port != 120000; port ++) {
            if(! NetUtils.isPortUsing("127.0.0.1", port)) {
                selfNodeHostPort = "127.0.0.1:" + String.valueOf(port);
                break;
            }
        }
        Assert.assertNotNull(selfNodeHostPort);
        BDBEnvironment environment = new BDBEnvironment(
                tempDir,
                name,
                selfNodeHostPort,
                selfNodeHostPort,
                true);
        environment.setup();
        return environment;
    }

    private boolean checkKeyExists(long key, Database db) throws Exception {
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, theKey);
        DatabaseEntry theData = new DatabaseEntry();
        OperationStatus operationStatus = db.get(null, theKey, theData, LockMode.READ_COMMITTED);
        if (operationStatus == OperationStatus.SUCCESS) {
            return true;
        } else if (operationStatus == OperationStatus.NOTFOUND) {
            return false;
        }
        Assert.fail();
        return false;
    }

    private String readDBStringValue(long key, Database db) throws IOException {
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, theKey);
        DatabaseEntry theData = new DatabaseEntry();
        OperationStatus operationStatus = db.get(null, theKey, theData, LockMode.READ_COMMITTED);
        Assert.assertEquals(OperationStatus.SUCCESS, operationStatus);
        byte[] retData = theData.getData();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
        return Text.readString(in);
    }

    /**
     * write() vs BatchWrite()
     * @throws Exception
     */
    @Ignore
    @Test
    public void writeProfile() throws Exception {
        long LOG_COUNT = Long.valueOf(System.getenv().getOrDefault(
                "BDB_PUT_PROFILE_TEST_LOG_COUNT", "1000"));
        int VALUE_SIZE = Integer.valueOf(System.getenv().getOrDefault(
                "BDB_PUT_PROFILE_TEST_VALUE_SIZE", "4096"));
        int BATCH_SIZE = Integer.valueOf(System.getenv().getOrDefault(
                "BDB_PUT_PROFILE_TEST_BATCH_SIZE", "4"));
        String data = StringUtils.repeat("x", VALUE_SIZE - 4);
        Writable writable = new Writable() {
            @Override
            public void write(DataOutput out) throws IOException {
                Text.writeString(out, data);
            }
        };

        BDBEnvironment notxnEnvironment = initBDBEnv("notxn");
        Database notxnDatabase = notxnEnvironment.openDatabase("testDb").getDb();
        Journal nonTxnJournal = new BDBJEJournal(notxnEnvironment, new CloseSafeDatabase(notxnDatabase));
        BDBEnvironment txnEnvironment = initBDBEnv("txn");
        Database txnDatabase = txnEnvironment.openDatabase("testDb").getDb();
        Journal txnJournal = new BDBJEJournal(txnEnvironment, new CloseSafeDatabase(txnDatabase));
        long startTime, endTime;

        LOG.info("================= TEST START ==========================");

        // 1. no txn
        startTime = System.currentTimeMillis();
        LOG.info("notxn: start to wrote {} lines, value size {}", LOG_COUNT, VALUE_SIZE);
        for (long i = 0; i != LOG_COUNT; ++i) {
            nonTxnJournal.write((short)(i % 128), writable);
        }
        endTime = System.currentTimeMillis();
        LOG.info("notxn: wrote {} lines, value size {} took {} seconds", LOG_COUNT, VALUE_SIZE, (endTime - startTime)/1000.0);

        // 2. with txn
        DataOutputBuffer buffer = new DataOutputBuffer();
        writable.write(buffer);
        startTime = System.currentTimeMillis();
        txnJournal.batchWriteBegin();
        LOG.info("txn: start to wrote {} lines, value size {}, BATCH size {}", LOG_COUNT, VALUE_SIZE, BATCH_SIZE);
        for (long i = 0; i != LOG_COUNT; ++ i) {
            txnJournal.batchWriteAppend(i, buffer);
            if (i % BATCH_SIZE == BATCH_SIZE - 1) {
                txnJournal.batchWriteCommit();
                txnJournal.batchWriteBegin();
            }
        }
        if (LOG_COUNT % BATCH_SIZE != 0){
            txnJournal.batchWriteCommit();
        } else {
            txnJournal.batchWriteAbort();
        }
        endTime = System.currentTimeMillis();
        LOG.info("txn: wrote {} lines, value size {}, batch size {}, took {} seconds",
                LOG_COUNT, VALUE_SIZE, BATCH_SIZE, (endTime - startTime)/1000.0);

        // 3. read and check
        notxnEnvironment.close();
        txnEnvironment.close();
        txnEnvironment = initBDBEnv("txn");
        txnDatabase = txnEnvironment.openDatabase("testDb").getDb();
        startTime = System.currentTimeMillis();
        LOG.info("txn: start to read {} lines, value size {}", LOG_COUNT, VALUE_SIZE);
        for (long i = 0; i != LOG_COUNT; ++ i) {
            String value = readDBStringValue(i, txnDatabase);
            Assert.assertEquals(data, value);
        }
        endTime = System.currentTimeMillis();
        LOG.info("txn: read {} lines, value size {}, took {} seconds",
                LOG_COUNT, VALUE_SIZE, (endTime - startTime)/1000);
    }

    @Test
    public void testWrieNoMock() throws Exception {
        BDBEnvironment environment = initBDBEnv("testWrieNormal");
        CloseSafeDatabase database = environment.openDatabase("testWrieNormal");
        BDBJEJournal journal = new BDBJEJournal(environment, database);

        String data = "petals on a wet black bough";
        Writable writable = new Writable() {
            @Override
            public void write(DataOutput out) throws IOException {
                Text.writeString(out, data);
            }
        };
        DataOutputBuffer buffer = new DataOutputBuffer();
        writable.write(buffer);

        // 1. initial check
        Assert.assertNull(journal.currentTrasaction);

        // 2. begin
        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);

        // 3. write 1
        journal.batchWriteAppend(1, buffer);

        // 4. commit 1
        journal.batchWriteCommit();
        Assert.assertNull(journal.currentTrasaction);

        // 3. write 2 logs & commit
        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);
        journal.batchWriteAppend(2, buffer);
        journal.batchWriteAppend(3, buffer);
        Assert.assertNotNull(journal.currentTrasaction);

        // 4. commmit
        journal.batchWriteCommit();
        Assert.assertNull(journal.currentTrasaction);

        // 5. check by read
        for (int i = 1; i != 4; i ++) {
            String value = readDBStringValue(i, database.getDb());
            Assert.assertEquals(data, value);
        }

        // 6. abort
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTrasaction);

        // 7. begin -> abort
        journal.batchWriteBegin();
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTrasaction);

        // 8. write log -> abort
        journal.batchWriteBegin();
        journal.batchWriteAppend(4, buffer);
        journal.batchWriteAbort();
        Assert.assertFalse(checkKeyExists(4, database.getDb()));
    }

    @Test
    public void testBatchWriteBeginRetry(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment,
            @Mocked Database rawDatabase,
            @Mocked Environment rawEnvironment,
            @Mocked Transaction txn) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);

        new Expectations(rawDatabase) {
            {
                rawDatabase.getEnvironment();
                minTimes = 0;
                result = rawEnvironment;
            }
        };
        new Expectations(rawEnvironment) {
            {
                rawEnvironment.beginTransaction(null, (TransactionConfig) any);
                times = 2;
                result = new DatabaseNotFoundException("mock mock");
                result = txn;
            }
        };
        journal.batchWriteBegin();
    }

    @Test
    public void testBatchWriteAppendRetry(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment,
            @Mocked Database rawDatabase,
            @Mocked Environment rawEnvironment,
            @Mocked Transaction txn) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);

        new Expectations(database) {
            {
                database.getDb();
                minTimes = 0;
                result = rawDatabase;

                database.put(txn, (DatabaseEntry) any, (DatabaseEntry) any);
                times = 3;
                result = new DatabaseNotFoundException("mock mock");
                result = OperationStatus.KEYEXIST;
                result = OperationStatus.SUCCESS;
            }
        };

        new Expectations(rawDatabase) {
            {
                rawDatabase.getEnvironment();
                minTimes = 0;
                result = rawEnvironment;

                rawDatabase.getDatabaseName();
                minTimes = 0;
                result = "fakeDb";
            }
        };
        new Expectations(rawEnvironment) {
            {
                rawEnvironment.beginTransaction(null, (TransactionConfig) any);
                minTimes = 1;
                result = txn;
            }
        };

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);
        journal.batchWriteAppend(1, buffer);
    }

    @Test
    public void testBatchWriteCommitRetry(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment,
            @Mocked Database rawDatabase,
            @Mocked Environment rawEnvironment,
            @Mocked Transaction txn) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);

        // write failed and retry, success
        new Expectations(database) {
            {
                database.getDb();
                minTimes = 0;
                result = rawDatabase;

                database.put((Transaction) any, (DatabaseEntry) any, (DatabaseEntry) any);
                times = 2;  // append & rebuild txn
                result = OperationStatus.SUCCESS;
            }
        };

        new Expectations(rawDatabase) {
            {
                rawDatabase.getEnvironment();
                minTimes = 0;
                result = rawEnvironment;

                rawDatabase.getDatabaseName();
                minTimes = 0;
                result = "fakeDb";
            }
        };

        new Expectations(rawEnvironment) {
            {
                rawEnvironment.beginTransaction(null, (TransactionConfig)any);
                times = 2;  // append & rebuild txn
            }
        };

        // commit fails 2 times
        // the first txn is valid, can continue commit
        // the second time txn is invalid, we have to rebuild txn
        new Expectations(txn) {
            {
                txn.commit();
                times = 3;
                result = new DatabaseNotFoundException("mock mock");
                result = new DatabaseNotFoundException("mock mock");
                result = null;

                txn.isValid();
                times = 2;
                result = true;
                result = false;
            }
        };

        journal.batchWriteBegin();
        journal.batchWriteAppend(1, buffer);
        journal.batchWriteCommit();
    }

    @Test(expected = JournalException.class)
    public void testAppendNoBegin(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);
        journal.batchWriteAppend(1, buffer);
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testCommitNoBegin(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);
        journal.batchWriteCommit();
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testBeginTwice(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);
        journal.batchWriteBegin();
        Assert.fail();
    }

    // you can count on me. -- abort
    @Test
    public void testAbort() throws Exception {
        BDBEnvironment environment = initBDBEnv("testWrieNormal");
        CloseSafeDatabase database = environment.openDatabase("testWrieNormal");
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);

        Assert.assertNull(journal.currentTrasaction);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTrasaction);

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTrasaction);

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);
        journal.batchWriteAppend(1, buffer);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTrasaction);

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTrasaction);
        journal.batchWriteAppend(2, buffer);
        journal.batchWriteCommit();
        Assert.assertNull(journal.currentTrasaction);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTrasaction);
    }

}
