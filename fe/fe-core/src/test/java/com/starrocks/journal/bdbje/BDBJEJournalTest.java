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
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

public class BDBJEJournalTest {
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalTest.class);

    private File tempDir;
    private int restoredCheckpointIntervalSecond;

    @Before
    public void init() throws Exception{
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
     * write() vs writeWithinTxn()
     * @throws Exception
     */
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
        Journal nonTxnJournal = new BDBJEJournal(notxnEnvironment, new CloseSafeDatabase(notxnDatabase), new AtomicLong(0));
        BDBEnvironment txnEnvironment = initBDBEnv("txn");
        Database txnDatabase = txnEnvironment.openDatabase("testDb").getDb();
        Journal txnJournal = new BDBJEJournal(txnEnvironment, new CloseSafeDatabase(txnDatabase), new AtomicLong(0));
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
        LOG.info("txn: start to wrote {} lines, value size {}, BATCH size {}", LOG_COUNT, VALUE_SIZE, BATCH_SIZE);
        for (long i = 0; i != LOG_COUNT; ++ i) {
            txnJournal.writeWithinTxn((short) (i % 128), buffer);
            if (i % BATCH_SIZE == BATCH_SIZE - 1) {
                txnJournal.commitTxn();
            }
        }
        if (LOG_COUNT % BATCH_SIZE != 0){
            txnJournal.commitTxn();
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
    public void testWrieNormal() throws Exception {
        BDBEnvironment environment = initBDBEnv("testWrieNormal");
        CloseSafeDatabase database = environment.openDatabase("testWrieNormal");
        BDBJEJournal journal = new BDBJEJournal(environment, database, new AtomicLong(111));

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
        Assert.assertEquals(111, journal.nextJournalId.get());
        Assert.assertNull(journal.currentTrasaction);

        // 2. write & commit
        journal.writeWithinTxn((short)-1, buffer);
        journal.commitTxn();

        Assert.assertEquals(112, journal.nextJournalId.get());

        // 3. write one log, not commit
        journal.writeWithinTxn((short)-1, buffer);

        Assert.assertNotNull(journal.currentTrasaction);
        Assert.assertEquals(112, journal.nextJournalId.get());
        Assert.assertEquals(112, journal.stagingStartJournalId);
        Assert.assertEquals(113, journal.staggingNextJournalId);

        // 4. write another log, not commit
        journal.writeWithinTxn((short)-1, buffer);

        Assert.assertNotNull(journal.currentTrasaction);
        Assert.assertEquals(112, journal.nextJournalId.get());
        Assert.assertEquals(112, journal.stagingStartJournalId);
        Assert.assertEquals(114, journal.staggingNextJournalId);

        // 5. finally, commit
        journal.commitTxn();
        Assert.assertNull(journal.currentTrasaction);
        Assert.assertEquals(114, journal.nextJournalId.get());

        // 6. check by read
        for (int i = 112; i != 114; i ++) {
            String value = readDBStringValue(i, database.getDb());
            Assert.assertEquals(data, value);
        }
    }

    @Test
    public void testWriteWithinTxnRetry(
            @Mocked CloseSafeDatabase database,
            @Mocked Database rawDatabase,
            @Mocked Environment rawEnvironment,
            @Mocked Transaction txn) throws Exception {
        BDBEnvironment environment = initBDBEnv("testWriteWithinTxnRetry");
        BDBJEJournal journal = new BDBJEJournal(environment, database, new AtomicLong(222));
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);

        // write failed and retry, success
        new Expectations() {
            {
                Thread.sleep(anyLong);
                times = 2;
            }
        };

        new Expectations(database) {
            {
                database.getDb();
                minTimes = 0;
                result = rawDatabase;

                database.put((Transaction) any, (DatabaseEntry) any, (DatabaseEntry) any);
                times = 2;
                result = new DatabaseNotFoundException("mock mock");
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
                result = "mock mock";
            }
        };
        new Expectations(rawEnvironment) {
            {
                rawEnvironment.beginTransaction(null, (TransactionConfig)any);
                times = 2;
                result = new DatabaseNotFoundException("mock mock");
                result = txn;
            }
        };
        new Expectations(txn) {
            {
                txn.getId();
                minTimes = 0;
                result = 999;
            }
        };
        Assert.assertNull(journal.currentTrasaction);
        Assert.assertEquals(222, journal.nextJournalId.get());
        journal.writeWithinTxn((short)-1, buffer);

        Assert.assertNotNull(journal.currentTrasaction);
        Assert.assertEquals(222, journal.nextJournalId.get());
        Assert.assertEquals(222, journal.stagingStartJournalId);
        Assert.assertEquals(223, journal.staggingNextJournalId);

    }
 }
