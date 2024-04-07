// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
import com.starrocks.journal.JournalException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BDBJEJournalTest {
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalTest.class);

    private File tempDir;
    private int restoredCheckpointIntervalSecond;

    @Before
    public void init() throws Exception {
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
        String selfNodeHostPort = "127.0.0.1:" + UtFrameUtils.findValidPort();
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


    @Test
    public void testWrieNoMock() throws Exception {
        BDBEnvironment environment = initBDBEnv("testWrieNormal");
        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();

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
        Assert.assertNull(journal.currentTransaction);

        // 2. begin
        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTransaction);

        // 3. write 1
        journal.batchWriteAppend(1, buffer);

        // 4. commit 1
        journal.batchWriteCommit();
        Assert.assertNull(journal.currentTransaction);

        // 3. write 2 logs & commit
        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTransaction);
        journal.batchWriteAppend(2, buffer);
        journal.batchWriteAppend(3, buffer);
        Assert.assertNotNull(journal.currentTransaction);

        // 4. commmit
        journal.batchWriteCommit();
        Assert.assertNull(journal.currentTransaction);

        // 5. check by read
        for (int i = 1; i != 4; i++) {
            String value = readDBStringValue(i, journal.currentJournalDB.getDb());
            Assert.assertEquals(data, value);
        }

        // 6. abort
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTransaction);

        // 7. begin -> abort
        journal.batchWriteBegin();
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTransaction);

        // 8. write log -> abort
        journal.batchWriteBegin();
        journal.batchWriteAppend(4, buffer);
        journal.batchWriteAbort();
        Assert.assertFalse(checkKeyExists(4, journal.currentJournalDB.getDb()));

        Assert.assertEquals(Arrays.asList(1L), journal.getDatabaseNames());
        Assert.assertEquals(3, journal.getMaxJournalId());
        journal.rollJournal(4);
        Assert.assertEquals(3, journal.getMaxJournalId());
        Assert.assertEquals(Arrays.asList(1L, 4L), journal.getDatabaseNames());
        journal.deleteJournals(4);
        Assert.assertEquals(Arrays.asList(4L), journal.getDatabaseNames());

        journal.close();
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
        Assert.assertNotNull(journal.currentTransaction);
        journal.batchWriteAppend(1, buffer);
    }


    @Test
    public void testBatchWriteCommitRetrySuccess(
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
                rawEnvironment.beginTransaction(null, (TransactionConfig) any);
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

    // retry 1: commit fails
    // retry 2: begin txn with exception
    // retry 3: put return error
    @Test(expected = JournalException.class)
    public void commitFailsRebuildFails(
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
                times = 4;  // append & rebuild txn
                result = OperationStatus.SUCCESS;
                result = OperationStatus.SUCCESS;
                result = OperationStatus.SUCCESS;
                result = OperationStatus.KEYEMPTY;
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
                times = 3;  // append & rebuild txn
                result = txn;
                result = new DatabaseNotFoundException("mock mock: begin txn");
                result = txn;
            }
        };

        new Expectations(txn) {
            {
                txn.commit();
                times = 1;
                result = new DatabaseNotFoundException("mock mock: commit");

                txn.isValid();
                times = 1;
                result = false;
            }
        };

        journal.batchWriteBegin();
        journal.batchWriteAppend(1, buffer);
        journal.batchWriteAppend(2, buffer);
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
        Assert.assertNotNull(journal.currentTransaction);
        journal.batchWriteBegin();
        Assert.fail();
    }

    // you can count on me. -- abort
    @Test
    public void testAbort() throws Exception {
        BDBEnvironment environment = initBDBEnv("testAbort");
        CloseSafeDatabase database = environment.openDatabase("testWrieNormal");
        BDBJEJournal journal = new BDBJEJournal(environment, database);
        String data = "petals on a wet black bough";
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, data);

        Assert.assertNull(journal.currentTransaction);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTransaction);

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTransaction);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTransaction);

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTransaction);
        journal.batchWriteAppend(1, buffer);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTransaction);

        journal.batchWriteBegin();
        Assert.assertNotNull(journal.currentTransaction);
        journal.batchWriteAppend(2, buffer);
        journal.batchWriteCommit();
        Assert.assertNull(journal.currentTransaction);
        journal.batchWriteAbort();
        Assert.assertNull(journal.currentTransaction);
    }


    // test open for the first time, will open a new db with replayedJournalId + 1
    @Test
    public void testOpenFirstTime(
            @Mocked CloseSafeDatabase database,
            @Mocked BDBEnvironment environment,
            @Mocked GlobalStateMgr globalStateMgr) throws Exception {

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                times = 1;
                result = globalStateMgr;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getReplayedJournalId();
                times = 1;
                result = 10;
            }
        };

        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = new ArrayList();

                environment.openDatabase("11");
                times = 1;
                result = database;
            }
        };

        new Expectations(database) {
            {
                database.close();
                times = 1;
            }
        };

        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();
        journal.close();
    }


    @Test
    public void testOpenNormal(@Mocked CloseSafeDatabase database,
                           @Mocked BDBEnvironment environment) throws Exception {

        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = Arrays.asList(3L, 23L, 45L);

                environment.openDatabase("45");
                times = 1;
                result = database;
            }
        };

        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();
    }

    @Test(expected = JournalException.class)
    public void testOpenGetNamesFails(@Mocked BDBEnvironment environment) throws Exception {
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = null;
            }
        };

        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testOpenFailManyTimes(@Mocked BDBEnvironment environment) throws Exception {
        new Expectations() {
            {
                Thread.sleep(anyLong);
                times = BDBJEJournal.RETRY_TIME - 1;
                result = null;
            }
        };
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = BDBJEJournal.RETRY_TIME;
                result = new DatabaseNotFoundException("mock mock");
            }
        };

        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();
        Assert.fail();
    }

    @Test
    public void testOpenDbFailRetrySucceed(@Mocked CloseSafeDatabase database,
                                           @Mocked BDBEnvironment environment)  throws Exception {
        new Expectations() {
            {
                Thread.sleep(anyLong);
                times = 1;
                result = null;
            }
        };
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 3;
                result = Arrays.asList(3L, 23L, 45L);

                environment.openDatabase("45");
                times = 3;
                result = null;
                result = new DatabaseNotFoundException("mock mock");
                result = database;
            }
        };


        new Expectations(database) {
            {
                database.close();
                times = 1;
            }
        };

        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();
        journal.close();
    }

    @Test
    public void testDeleteJournals(@Mocked BDBEnvironment environment)  throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment);

        // failed to get database names; do nothing
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = null;
            }
        };
        journal.deleteJournals(11);

        // 2. find journal and delete
        // current db (3, 23, 45) checkpoint is made on 44, should remove 3, 23
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = Arrays.asList(3L, 23L, 45L);

                environment.removeDatabase("3");
                times = 1;
                environment.removeDatabase("23");
                times = 1;
            }
        };
        journal.deleteJournals(45);
        journal.close();  // no db will closed
    }

    @Test
    public void testGetMaxJournalId(@Mocked CloseSafeDatabase closeSafeDatabase,
                                    @Mocked BDBEnvironment environment,
                                    @Mocked Database database) throws Exception {
        BDBJEJournal journal = new BDBJEJournal(environment);

        // failed to get database names; return -1
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = null;
            }
        };
        Assert.assertEquals(-1, journal.getMaxJournalId());

        // no databases; return -1
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = new ArrayList<>();
            }
        };
        Assert.assertEquals(-1, journal.getMaxJournalId());

        // db 3, 23, 45; open 45 get its size 10
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 1;
                result = Arrays.asList(3L, 23L, 45L);

                environment.openDatabase("45");
                times = 1;
                result = closeSafeDatabase;
            }
        };
        new Expectations(closeSafeDatabase) {
            {
                closeSafeDatabase.getDb();
                times = 1;
                result = database;
            }
        };
        new Expectations(database) {
            {
                database.count();
                times = 1;
                result = 10;

                database.close();
                times = 1;
            }
        };
        Assert.assertEquals(54, journal.getMaxJournalId());
        journal.close();  // no db will closed
    }

    @Test(expected = JournalException.class)
    public void testRollJournal(@Mocked CloseSafeDatabase closeSafeDatabase,
                                    @Mocked BDBEnvironment environment,
                                    @Mocked Database database) throws Exception {
        new Expectations(closeSafeDatabase) {
            {
                closeSafeDatabase.getDb();
                minTimes = 0;
                result = database;

                closeSafeDatabase.close();
                minTimes = 0;
            }
        };
        BDBJEJournal journal = new BDBJEJournal(environment, closeSafeDatabase);

        // 1. no data, do nothing and return
        new Expectations(database) {
            {
                database.count();
                times = 1;
                result = 0;
            }
        };
        journal.rollJournal(111);


        // 2. normal cases, current db is 9, has 10 logs, new log expect 19
        new Expectations(database) {
            {
                database.count();
                times = 2;
                result = 10;

                database.getDatabaseName();
                times = 1;
                result = 9;
            }
        };
        journal.rollJournal(19);

        // 3. exception  current db is 9, has 10 logs, new log expect 19
        new Expectations(database) {
            {
                database.count();
                times = 2;
                result = 10;

                database.getDatabaseName();
                times = 1;
                result = 9;
            }
        };
        journal.rollJournal(18);
        Assert.fail();
    }

    @Test
    public void testVerifyId() throws Exception {
        String data = "petals on a wet black bough";
        Writable writable = new Writable() {
            @Override
            public void write(DataOutput out) throws IOException {
                Text.writeString(out, data);
            }
        };
        DataOutputBuffer buffer = new DataOutputBuffer();
        writable.write(buffer);

        BDBEnvironment environment = initBDBEnv("testVerifyId");
        BDBJEJournal journal = new BDBJEJournal(environment);

        journal.open();
        journal.batchWriteBegin();
        journal.batchWriteAppend(1, buffer);
        journal.batchWriteAppend(2, buffer);
        journal.batchWriteCommit();

        journal.rollJournal(3);
        journal.open();
        journal.batchWriteBegin();
        journal.batchWriteAppend(3, buffer);
        journal.batchWriteAppend(4, buffer);
        journal.batchWriteCommit();

        Assert.assertEquals(2, journal.getFinalizedJournalId());
        Assert.assertEquals(4, journal.getMaxJournalId());
        journal.close();
    }

    @Test
    public void testJournalWithPrefix() throws Exception {
        String data = "petals on a wet black bough";
        Writable writable = new Writable() {
            @Override
            public void write(DataOutput out) throws IOException {
                Text.writeString(out, data);
            }
        };
        DataOutputBuffer buffer = new DataOutputBuffer();
        writable.write(buffer);

        BDBEnvironment environment = initBDBEnv("testJournalWithPrefix");

        new MockUp<StarMgrServer>() {
            @Mock
            public long getReplayId() {
                return 0;
            }
        };

        // test journal with prefix works fine
        {
            BDBJEJournal journalWithPrefix = new BDBJEJournal(environment, "aaa_");
            Assert.assertEquals("aaa_", journalWithPrefix.getPrefix());
            journalWithPrefix.open();
            journalWithPrefix.batchWriteBegin();
            journalWithPrefix.batchWriteAppend(1, buffer);
            journalWithPrefix.batchWriteAppend(2, buffer);
            journalWithPrefix.batchWriteCommit();

            journalWithPrefix.rollJournal(3);
            journalWithPrefix.open();
            journalWithPrefix.batchWriteBegin();
            journalWithPrefix.batchWriteAppend(3, buffer);
            journalWithPrefix.batchWriteAppend(4, buffer);
            journalWithPrefix.batchWriteCommit();

            List<Long> l = journalWithPrefix.getDatabaseNames();
            Assert.assertEquals(2, l.size());
            Assert.assertEquals((Long) 1L, l.get(0));
            Assert.assertEquals((Long) 3L, l.get(1));
        }

        // test journal without prefix works fine at the same time
        {
            BDBJEJournal journalWithoutPrefix = new BDBJEJournal(environment);
            journalWithoutPrefix.open();
            journalWithoutPrefix.batchWriteBegin();
            journalWithoutPrefix.batchWriteAppend(1, buffer);
            journalWithoutPrefix.batchWriteAppend(2, buffer);
            journalWithoutPrefix.batchWriteCommit();

            journalWithoutPrefix.rollJournal(3);
            journalWithoutPrefix.open();
            journalWithoutPrefix.batchWriteBegin();
            journalWithoutPrefix.batchWriteAppend(3, buffer);
            journalWithoutPrefix.batchWriteAppend(4, buffer);
            journalWithoutPrefix.batchWriteCommit();

            List<Long> l = journalWithoutPrefix.getDatabaseNames();
            Assert.assertEquals(2, l.size());
            Assert.assertEquals((Long) 1L, l.get(0));
            Assert.assertEquals((Long) 3L, l.get(1));
        }
    }
}
